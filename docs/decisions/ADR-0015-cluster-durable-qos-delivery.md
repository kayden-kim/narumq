# ADR-0015: Cluster QoS 1/2 durable delivery

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (cluster QoS 1/2 durable delivery scope; production storage·BP-03·N-1 제외) |
| 결정일 | 2026-09-27 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD CLU-05·CLU-06·CLU-07·CLU-09·CLU-12](../requirements/PRD.md#92-클러스터와-세션), [PRD TEN-08](../requirements/PRD.md#93-멀티테넌시) (tenant별 quota는 미충족·후속) |
| 관련 스펙·데이터 계약 | [Cluster §4.2·§11.11](../specs/cluster-session-and-delivery.md), [replicated-state-v1.proto](../contracts/replicated-state-v1.proto), [Cluster backup v3](../contracts/cluster-backup-v3.md) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md), [Broker core boundaries](../explainers/broker-core-boundaries.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

1. **범위와 복제 단위:** Cluster mode는 QoS 1/2 inbound publish 및 durable target 전달을 지원한다. 각 `(tenantId, clientId)` session마다 Raft 복제 `DeliverySessionState`를 두어 target별 durable queue와 inbound QoS 2 상태를 보관한다. 이는 공유 message log+cursor 모델이 아니라 session별 message copy 모델이다. 기존 `SessionCheckpoint` replace-all 모델은 대체한다. Production storage, BP-03 및 N-1 compatibility는 이 승인 범위에서 제외한다.
2. **상태:** `DeliverySessionState`의 key는 `(tenantId, clientId)`이고 `session_expires_at_unix_millis`, `next_sequence`(1부터 단조 증가), sequence 오름차순 `messages[]`, packet identifier 오름차순 `inbound[]`를 가진다. `DeliveryMessage`는 `sequence`, `topic`, `payload`, effective `qos`(1 또는 2), 대상별 `retain`(Retain-As-Published 반영 결과), `expires_at_unix_millis`, `packet_id`, `phase`를 가진다. `packet_id=0`과 `QUEUED`는 서로 동치다. `AWAIT_PUBACK`은 QoS 1 전용이며 `AWAIT_PUBREC`와 `AWAIT_PUBCOMP`는 QoS 2 전용이다.
3. **Publisher QoS 2 inbound와 tombstone:** `InboundMessage`는 publisher의 `packet_id`, `topic`, `payload`, `retain`, expiry, `released`, `released_index`를 보관한다. `released=false`는 PUBREL 대기 상태다. `released=true`는 RELEASE를 적용했지만 PUBCOMP 도달 여부를 모르는 tombstone이다. Payload를 비우고 topic은 보존한다. Tombstone은 같은 packet identifier의 새 RECEIVE, capacity eviction, ATTACH reset 또는 record 삭제 때만 사라진다. 이를 통해 PUBCOMP 유실 뒤 재연결된 publisher가 PUBREL을 재전송하면 unknown packet identifier 0x92가 아닌 성공 PUBCOMP reason 0x00을 받는다.
4. **Record lifecycle와 authoritative target 검증:** Record는 CONNECT의 ATTACH, subscription ADD apply(동일 key record가 없을 때), 또는 PUBLISH/RELEASE apply가 target 검증을 통과할 때 생성한다. 삭제는 DISCARD, ownership Release 또는 session-expiry purge뿐이다. 비어 있는 record도 `next_sequence`를 유지한다. PUBLISH/RELEASE command의 target은 ingress route projection이 만든 후보일 뿐이며 Apply가 committed state로 다시 판정한다. 같은 tenant ownership row와 현재 owner/epoch에 속한 matching committed subscription(QoS ≥ 1)이 있어야 한다. Shared filter는 `$share/<group>/` prefix를 제거해 topic match한다. 검증을 통과하지 못한 target은 `Skipped`다. 통과한 target이 record를 가지지 않으면 `next_sequence=1`로 생성한다. `DeliveryChange.publish_qos`는 PUBLISH와 RELEASE에서 1 또는 2이며 RELEASE에서는 반드시 2다. Apply는 이를 이용해 `min(publish_qos, matching subscription 중 최대 QoS)`를 계산한다. 다른 action에서 이 필드는 0이어야 한다. 대상별 retain은 ingress의 Retain-As-Published 결과를 그대로 쓴다. DISCARD는 같은 key의 committed subscription row도 삭제한다.
5. **한도:** Session당 durable message 64개 및 inbound 64개(tombstone 포함), 전체 tenant 합계 entries(messages+inbound) 4,096개와 16 MiB, payload 1,000,000 bytes, command당 target 1,024개로 제한한다. Byte 계산은 entry마다 `len(tenant)+len(client)+len(topic)+len(payload)`다. Tenant별 quota(TEN-08)는 제공하지 않는다.
6. **Expiry:** 모든 action apply 전에 proposer의 `proposed_at_unix_millis`로 purge한다. `0 < session_expires_at <= proposed_at`인 session record와 `phase=QUEUED`이며 `0 < expires_at <= proposed_at`인 message를 지운다. Packet identifier가 할당된 inflight와 inbound는 expiry purge로 제거하지 않는다.
7. **Action과 fencing:** `DeliveryChange.action`은 `PUBLISH`, `RECEIVE`, `RELEASE`, `SEND`, `ADVANCE`, `COMPLETE`, `ATTACH`, `DETACH`, `DISCARD`를 제공한다. PUBLISH는 QoS 1/2 ingress 또는 QoS≥1 Will의 delivery를 owner/epoch 없이 각 candidate에 검증하고 enqueue한다. 대상이 포화되면 그 target만 `Saturated`에 넣고 건너뛰며 Apply 결과는 `Applied, Found=true`다. QoS 2 RECEIVE는 publisher session owner/epoch로 fenced되며 중복된 unreleased packet은 no-op, tombstone packet 재사용은 새 RECEIVE로 대체한다. Inbound 64개일 때 가장 오래된 `released_index` tombstone을 하나 eviction할 수 있고, eviction 가능한 tombstone이 없거나 global capacity를 넘으면 `delivery_capacity_exceeded`다. RELEASE는 publisher owner/epoch로 fenced된다. Missing record/message는 `Found=false, Released=false`, tombstone 재수신은 `Found=false, Released=true`이며 enqueue하지 않는다. 첫 RELEASE는 tombstone을 남기고 만료되지 않은 payload를 target으로 publish한다. SEND는 session owner/epoch로 fenced되고 packet ID를 할당해 QoS phase를 시작한다. 동일한 할당은 no-op이며 다른 ID 충돌은 `delivery_packet_id_conflict`다. ADVANCE는 QoS 2를 `AWAIT_PUBREC`에서 `AWAIT_PUBCOMP`로 바꾸고 이미 진행된 경우 no-op이다. COMPLETE는 packet message를 삭제하며 없는 ID는 no-op이다. ATTACH는 record를 생성하고, `reset`이면 messages/inbound를 비우되 sequence high-water mark를 유지한다. `Found = !reset && (messages 또는 inbound가 있음)`이다. DETACH는 session expiry를 설정하고 DISCARD는 record와 subscription을 지운다. Fenced action은 현재 owner/epoch 권한 검사를 사용하며 실패는 `stale_epoch` conflict다.
8. **QoS 1 ACK 경계:** Publish use case는 retained state를 먼저 commit하고, effective QoS 1 이상 target을 포함하는 durable PUBLISH command를 commit한다. 이어 effective QoS 0 match를 기존 offline/forward/local lane으로 처리하고 마지막에 PUBACK을 보낸다. Durable target이 0개여도 PUBLISH command를 commit한다. Durable target은 primary route 후보만 사용하며 shared-subscription fallback은 없다.
9. **QoS 2 ACK 경계:** 전달 시점은 기존 domain Inbound 동작과 같은 PUBREL(method B)이다. QoS 2 PUBLISH 수신 뒤 RECEIVE를 commit하고 PUBREC을 보낸다. PUBREL 수신 시 retained state를 commit하고 route match 후 RELEASE를 commit하며 QoS 0 lane을 처리하고 PUBCOMP을 보낸다. RELEASE가 `Found=false`이면 QoS 0 lane을 건너뛰고 성공 PUBCOMP만 보낸다.
10. **QoS 0 lane:** QoS 1/2 publish의 match라도 effective QoS 0인 subscriber는 at-most-once다. PUBLISH/RELEASE commit 뒤 기존 경로를 탄다: detached session이면 offline ENQUEUE, remote owner면 `NMQFWRD2` forward, 그 밖에는 local queue다. PUBACK/PUBCOMP 뒤 이 lane의 실패나 ingress node crash는 QoS 0 전달을 유실시킬 수 있다. 이는 MQTT QoS 0과 ADR-0013 §1의 보장 범위 밖 동작에 부합한다.
11. **Outbound 전이:** Owner는 outbound PUBLISH 전 SEND, PUBREL 전 ADVANCE, packet identifier 해제 전 COMPLETE를 commit한다. 모든 wire effect보다 progress가 먼저 commit되므로 successor Acquire 뒤 새 owner가 현재 durable state를 이어간다. Separate final checkpoint는 필요하지 않으며, 이 결정은 drain 지원을 구현하지 않는다. Old owner 명령은 epoch fencing에서 `stale_epoch`가 된다.
12. **Reconnect와 takeover:** 새 owner는 ATTACH 뒤 committed inflight를 packet identifier 순서로 재전송한다. PUBLISH는 `DUP=1`, `AWAIT_PUBCOMP`는 PUBREL이다. 이어 QUEUED message를 sequence 순으로, CLAIM한 offline QoS 0 message를 그 뒤에 보낸다. Inbound QoS 2는 connection state로 복원한다. `released=false`는 AwaitRelease로 복원되어 duplicate PUBLISH에 PUBREC만 응답하고 PUBREL에서 RELEASE한다. Tombstone은 Complete로 복원되어 PUBREL에 성공 PUBCOMP만 응답한다. CONNACK SessionPresent는 `(offline CLAIM Found || durable ATTACH Found) && !CleanStart`다. ATTACH `Found`는 기존 delivery session record 존재 여부이며 entry가 없어도 true다. CleanStart=1 ATTACH reset은 delivery/inbound state와 committed persistent subscriptions를 지우고, 기존 record의 `next_sequence`는 단조 증가로 유지한다.
13. **실패 및 duplicate 의미:** Commit error/timeout(예: quorum 부재, stale epoch)은 성공 ACK 없이 연결을 닫는다. RECEIVE capacity 초과 또는 1 MiB command 제한으로 인한 `deliveryqueue.ErrFull`은 MQTT 5에서 PUBACK/PUBREC 0x97, MQTT 3.1.1에서 연결 종료다. PUBLISH/RELEASE 대상 capacity 초과는 대상별 `Saturated`이고 publish는 성공한다. Saturated session이 로컬 연결이면 기존 정책대로 연결을 닫고 reconnect 시 queue를 다시 적재한다. QoS 2 publisher inbound record는 connection/node를 넘어 duplicate를 막는다. QoS 1은 기존 connection inbound state로 동일 연결 내 duplicate를 막지만 reconnect 후 DUP publish는 새 command가 되어 중복 전달할 수 있으며 at-least-once 의미가 허용한다.
14. **비내구 예외와 한계:** SUBSCRIBE 중 QoS 1 이상 retained replay와 DETACH 실패로 남은 local QoS 0 message는 node-local이며 owner crash/takeover에서 복구하지 않는다. 후자는 reconnect 시 durable 뒤에 유지되고 capacity 초과분은 잘릴 수 있다. Connected owner node가 crash하고 client가 돌아오지 않으면 DETACH가 없어 session expiry가 갱신되지 않을 수 있다. Queue는 session당 64개까지 찬 뒤 target을 `Saturated`로 건너뛸 수 있고 global limit도 소진될 수 있다. 이는 후속 과제다.
15. **Schema와 배포:** Replicated-state schema는 `1.0`을 유지한다. `DeliveryChange`는 retained/offline과 동일한 exact member-profile gate를 통과한 경우만 제안한다. 업그레이드는 ADR-0013 §13과 같은 전체 cluster 재시작 cutover다. Backup v3(`NMQBKUP3`) format은 유지하되 required `inflight` kind의 count는 `Σ(len(messages)+len(inbound))`, digest 입력은 `ReplicatedStateSnapshot{delivery_sessions}`, tenant digest에는 delivery record tenant를 포함한다. 이전 archive의 checkpoint는 비어 있었으므로 empty-message digest는 동일하다.

## 2. 배경

ADR-0015를 결정할 당시 `-cluster-config` MQTT 경로는 QoS 1/2 client PUBLISH를 거절했고 QoS 1/2 Will도 durable publish 경로를 사용하지 않았다. `SessionCheckpoint` replicated kind는 schema·state machine·codec에만 있었고 runtime에서 사용되지 않아 ownership acquire와 planned-removal handoff를 막고 있었다. 이에 이 결정은 QoS 1/2 client publish와 Will, persistent delivery, publisher QoS 2 inbound state, ACK barrier 및 reconnect/takeover를 하나의 session별 replicated queue 흐름으로 연결하도록 범위를 정했다.

## 3. 결정 요인

1. 성공 QoS ACK 뒤 message gap을 막고, owner 및 publisher reconnect에서 protocol 상태를 재개한다.
2. Queue order, duplicate handling, expiry, capacity와 fencing을 committed replicated state에서 결정적으로 처리한다.
3. 기존 QoS 0 at-most-once 보장과 detached/offline 경로를 변경하지 않는다.
4. 대상 capacity 초과가 다른 subscriber와 전체 publish를 불필요하게 막지 않도록 한다.
5. 승인 범위를 production storage, BP-03 및 N-1 compatibility 증거와 분리한다.

## 4. 선택 근거

Session별 replicated queue는 owner가 복구할 대상과 sequence를 직접 식별하고 outbound QoS state와 inbound QoS 2 tombstone을 같은 session key로 관리한다. SEND/ADVANCE/COMPLETE가 wire effect보다 앞서 commit되므로 process 중단 뒤 새 owner가 protocol 단계를 이어갈 수 있다. Target은 stale route projection을 신뢰하지 않고 apply에서 committed ownership/subscription으로 재판정한다. 용량 한도는 결정적이고, 한 대상의 full queue는 그 target만 건너뛰며 다른 target은 계속 처리한다.

QoS 1/2 publish 안의 effective-QoS-0 subscriber를 기존 lane에 남기면 그 delivery는 계속 at-most-once이며 durable ACK 이후 유실 가능성이 있다. 이 구분은 QoS 0 contract를 실수로 durable 보장으로 확장하지 않는다. ACK는 durable command commit에만 묶이며 production storage/fsync crash acceptance, BP-03, N-1 compatibility는 별도 미완료 상태다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 공유 message log와 subscriber cursor | Publish를 한 번 저장하고 fan-out copy를 줄인다. | Cursor, replay, retention/GC, 개별 session QoS phase를 조정해야 하며 session 단위 recovery와 capacity 판정이 복잡해진다. | 승인된 per-session replicated durable queue 모델과 다르며 구현 경계가 더 넓다. |
| 용량이 찬 target이 있으면 publish 전체를 거절한다. | 모든 durable target을 함께 전달하거나 publisher에 실패를 보고한다. | 한 session의 backlog가 독립 subscriber 전달까지 막는다. | 승인된 동작은 포화 target을 건너뛰고 `Saturated`로 보고하는 것이다. 이는 PRD CLU-12의 “limit rather than drop”보다 약한 보장이라는 알려진 deviation이다. |
| QoS 1 inbound PUBLISH를 별도 durable inbound record로 보관한다. | Publisher-side retry identity를 connection 변화 뒤에도 보존할 수 있다. | QoS 2 inbound state와 별도 lifecycle·dedup 체계를 만든다. | QoS 1은 기존 connection inbound duplicate 상태를 유지하고 reconnect 뒤 at-least-once 중복을 허용한다. 별도 state는 승인 범위 밖이다. |
| `SessionCheckpoint` replace-all을 유지한다. | 기존 checkpoint DTO·handoff 흐름을 재사용한다. | 매 전이에 전체 session snapshot을 교체하며, 현 checkpoint는 runtime 미사용이고 acquire/removal을 차단한다. | 작은 fenced action, queue sequence와 inflight/tombstone lifecycle을 직접 복제하는 durable delivery state가 대체한다. |

## 6. 결과

### 긍정적 결과

- QoS 1/2 성공 ACK 뒤 durable target queue 및 publisher QoS 2 inbound state가 quorum committed state에 있다.
- 새 owner는 committed outbound phase와 inbound QoS 2 tombstone을 이용해 subscriber 및 publisher handshake를 재개한다.
- Apply 시 committed ownership/subscription 재검증으로 stale ingress candidate가 enqueue되지 않는다.
- Effective QoS 0 delivery는 기존 QoS 0 경로와 at-most-once 의미를 유지한다.

### 부정적 결과와 비용

- QoS 1/2 durable target별 Raft command 비용과 bounded global/session storage 사용량이 추가된다.
- Capacity 포화 시 saturated target은 전달을 건너뛰며 publisher 성공은 다른 대상의 성공을 의미할 수 있다. CLU-12와의 차이를 수용한다.
- QoS 0 lane과 두 node-local 예외는 durable ACK 뒤에도 유실될 수 있다.
- Connected owner crash 후 reconnect가 없으면 DETACH/expiry 갱신이 없어 session record가 남고 global capacity를 고갈시킬 수 있다.
- TEN-08 tenant별 quota, BP-03, production storage, N-1 compatibility는 검증·충족되지 않았다.

### 후속 작업

- [ ] Connected owner node crash 후 DETACH 부재로 남는 session queue와 expiry/capacity 영향을 평가한다.
- [ ] BP-03 보완 근거를 실행하고 결과를 기록한다.
- [ ] [ADR-0003 §9-2](ADR-0003-durable-ack-barrier.md#9-검증)의 WAL/fsync crash injection을 실행한다.
- [ ] N-1 compatibility 및 cutover/rollback 범위를 별도로 결정하고 검증한다.
- [ ] TEN-08 tenant별 quota를 정책·집행·시험으로 보완한다. 현재 global limit는 이를 대체하지 않는다.

## 7. 검증

| 결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Durable state, queue, publisher/subscriber protocol과 runtime 전이 | [Cluster §11.11](../specs/cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑)의 focused unit suite와 세 process acceptance를 실행했다. | Unit 시험은 state transition, ACK order, takeover, duplicate, QoS 2 Will QoS 보존, saturation과 quorum-loss 동작을 확인한다. Process acceptance는 detached QoS 1/2 session queue, owner takeover, backup/restore, ACK 뒤 process kill 및 quorum loss를 확인한다. | PASS: focused package suite 통과. 세 process acceptance PASS (31.414s); 상세 test path와 결과는 §11.11에 기록했다. |
| Production storage, BP-03, N-1 | WAL/fsync crash injection, production-like storage 및 N-1 compatibility acceptance | 해당 별도 기준과 증거를 만족한다. | 이 ADR 승인 범위 밖; 미실행. |

## 8. 재검토 조건

- PRD CLU-12의 capacity 동작 또는 saturation 시 publisher-visible contract 변경이 승인되면 capacity decision을 재검토한다.
- QoS 1 reconnect 중복, effective QoS 0 유실, tombstone retention 또는 session expiry 동작이 요구사항과 다르다는 증거가 나오면 state/ACK contract를 재검토한다.
- BP-03, WAL/fsync crash injection, production storage 또는 N-1 compatibility 결과가 나오면 이 ADR의 제외 범위와 검증 상태를 갱신한다.
- TEN-08 tenant quota 정책이 승인되면 global 한도와 분리된 tenant별 acceptance를 추가한다.
