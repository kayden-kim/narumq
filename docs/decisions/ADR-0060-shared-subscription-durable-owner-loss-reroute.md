# ADR-0060: 공유 구독의 durable 전달은 도달 불가 owner를 건너뛰고 session 종료 때 다른 멤버로 다시 넣는다

| 항목 | 내용 |
| --- | --- |
| 상태 | `Accepted` |
| 결정일 | 2026-10-15 |
| 결정자 | 사용자 결정("broker를 고치고 ADR-0015 §8을 개정한다", 2026-10-15) |
| 관련 요구사항 | [PRD](../requirements/PRD.md#91-mqtt-프로토콜) MQTT-07, [PRD](../requirements/PRD.md#92-클러스터와-세션) CLU-05·CLU-07, NFR-07 |
| 관련 스펙·계약 | [Cluster §3](../specs/cluster-session-and-delivery.md#3-routing)·[§4.2](../specs/cluster-session-and-delivery.md#42-qos-12-durable-delivery)·[§5](../specs/cluster-session-and-delivery.md#5-publish-commit과-delivery)·[§11.35](../specs/cluster-session-and-delivery.md#1135-공유-구독-durable-전달의-owner-손실-수용-매핑), [MQTT §4](../specs/mqtt-protocol-and-session.md), [Replicated-state v1](../contracts/replicated-state-v1.md), [Release compatibility v1 §4](../contracts/cluster-release-compatibility-v1.md#4-feature-activation), [Backup v3](../contracts/cluster-backup-v3.md) |
| 관련 아키텍처 문서 | [Cluster architecture](../architectures/cluster-architecture.md) |
| 관련 결정 | [ADR-0004](ADR-0004-session-ownership-and-fencing.md), [ADR-0025](ADR-0025-replicated-session-metadata-and-owner-loss-recovery.md)(timeout failure detector 거부), [ADR-0031](ADR-0031-replicated-state-schema-strict-evolution.md), [ADR-0032](ADR-0032-feature-activation-approval-policy.md), [ADR-0059](ADR-0059-dynamic-peer-tcp-liveness.md)(peer 연결 감지 시간) |
| 이 ADR이 대체하는 이전 ADR | [ADR-0015](ADR-0015-cluster-durable-qos-delivery.md) §8의 "shared-subscription fallback은 없다" 문장과 §12의 CleanStart reset 중 shared 출처 message 처리만 대체한다. |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

공유 구독(`$share/<group>/<filter>`)의 durable(QoS 1 이상) 전달에 두 가지 규칙을 더한다.

1. **도달 불가 owner 건너뛰기.** Ingress node는 group의 멤버를 고를 때 owner node가 지금 이 node에서 도달할 수 없는 멤버를 primary 선택에서 뺀다. 도달 가능 여부는 이 node의 peer transport 연결 상태다. Owner가 이 node이면 도달 가능이다. Transport가 모르는 node도 도달 가능으로 본다. 도달 가능한 멤버가 하나도 없으면 지금처럼 전체 멤버에서 고른다. 도달 불가 멤버는 `Fallbacks` 끝으로 보낸다.
   - 이것은 node-local routing 선호다. Ownership, epoch, fencing과 replicated state를 바꾸지 않는다. Timeout 기반 failure detector를 새로 만들지 않는다([ADR-0025](ADR-0025-replicated-session-metadata-and-owner-loss-recovery.md)).
   - Durable·QoS 0 후보에 같은 순서를 쓴다. 상태 복제나 feature 활성화가 필요 없다.
2. **공유 출처 message 재배분.** Durable message가 공유 구독 때문에 어떤 멤버의 queue에 들어갔다면, 그 멤버의 delivery record가 사라질 때 아직 전달되지 않은 message를 같은 group의 다른 멤버 queue로 옮긴다. 같은 apply 안에서 결정적으로 처리한다.
   - **출처 기록.** `DeliveryTarget`과 저장된 `DeliveryMessage`에 선택 field `shared_filter`를 둔다. 값은 group을 포함한 전체 filter 문자열(`$share/<group>/<filter>`)이다. Ingress는 대상이 공유 선택의 결과일 때만 채운다. 같은 client가 비공유 구독으로도 match하면 비운다. Apply는 대상 client가 현재 owner·epoch로 그 filter를 구독 중일 때만 message에 기록한다. 아니면 기록하지 않는다.
   - **재배분 시점.** CleanStart ATTACH reset, DISCARD, session expiry purge와 ownership Release가 delivery record를 지우기 직전이다.
   - **재배분 대상.** `shared_filter`가 있고 만료되지 않은 message 중 `QUEUED`인 것과 QoS 1 `AWAIT_PUBACK`인 것이다. QoS 2의 `AWAIT_PUBREC`·`AWAIT_PUBCOMP`는 이미 보내기 시작한 message이므로 다시 넣지 않는다(MQTT 5.0 §4.8.2, MQTT-4.8.2-5).
   - **목적지.** 같은 tenant에서 같은 `shared_filter`를 현재 owner·epoch로 구독 중이고 QoS가 1 이상인 다른 client다. 먼저 그 command를 제안한 node가 owner인 멤버를 고른다. 그 node는 방금 command를 보냈으므로 살아 있다. CleanStart 재접속이면 재접속한 node다. 만료 purge처럼 제안한 node를 알 수 없으면 이 단계를 건너뛴다. 각 단계에서 client ID 오름차순으로 정렬하고, 지워지는 client의 다음 ID부터 한 건씩 돌려 가며 고른다. Session당 64개, 전체·tenant 한도와 sequence 한도가 찬 멤버는 건너뛴다. 받을 멤버가 없으면 message를 버린다. 이것은 기존 reset 결과와 같다.
   - **새 message.** 새 sequence를 받고 `QUEUED`·packet ID 0이다. 만료 시각은 그대로다. QoS는 `min(원래 QoS, 목적지 구독 QoS)`다. RETAIN은 저장된 값이 true이고 목적지가 Retain As Published를 요청했을 때만 true다. `shared_filter`를 유지하므로 목적지가 다시 사라지면 다시 재배분한다.
   - Persistent session 멤버가 살아 있으면 queue는 그대로 남는다. 그 멤버가 돌아오면 받는다. MQTT 5.0 §4.8.2는 QoS 1 message를 선택된 멤버의 재접속까지 기다려도 된다고 허용한다.
3. **Feature.** 출처 기록과 재배분은 새 build feature `shared-subscription-reroute`가 활성화된 뒤에만 쓴다. 모든 build가 이 feature를 광고하지만 `FeatureActivation`이 commit되기 전에는 `shared_filter`를 제안하거나 저장하지 않는다. 활성화 전 동작과 wire bytes는 기존과 같다. Apply는 비활성 상태의 `shared_filter`를 `feature_inactive`로 거절한다.
4. **Message 크기 계산.** 저장한 `shared_filter`의 byte 길이를 그 message의 byte 수에 더해 전체 16 MiB와 tenant `storage_bytes`·`offline_queue_bytes`에 포함한다. 출처를 기록하지 않은 message의 계산은 바뀌지 않는다.

## 2. 배경

2026-10-15 BP-02 local capacity N-1 단계에서 공유 group의 clean session 멤버에 QoS 1 gap이 나왔다. 같은 현상을 host process 3-node에서 결정적으로 재현했다. 멤버 네 명(리더 node 둘, 생존 node 각 하나)을 QoS 1로 붙이고 리더를 `process.crash`로 죽인 뒤 생존 node에서 40건을 publish했다. 40건 모두 PUBACK을 받았지만 20건은 죽은 node의 멤버 두 명에게 배정되어 어떤 살아 있는 멤버도 받지 못했다. 죽은 node의 멤버가 `CleanStart=true`로 돌아오자 queue에는 message가 없었다.

원인은 세 가지가 겹친 것이다.

- 멤버 선택(`internal/domain/routing`)과 route projection은 owner node가 살아 있는지 보지 않는다.
- Durable 경로는 선택된 primary 하나만 쓴다. 이것이 ADR-0015 §8의 "shared-subscription fallback은 없다"다.
- CleanStart reset은 queue를 비운다(ADR-0015 §12).

이 결과는 [Cluster §3 6항](../specs/cluster-session-and-delivery.md#3-routing)("unavailable owner를 계속 선택해서는 안 된다"), [§5](../specs/cluster-session-and-delivery.md#5-publish-commit과-delivery)(durable ACK 뒤 gap 금지), PRD MQTT-07("subscriber 장애 시 다른 멤버가 이어받는다")과 충돌한다. 사용자는 broker를 고치고 ADR-0015 §8을 개정하기로 결정했다.

살아남은 node는 죽은 node의 소유권을 고치지 않는다. 시간 초과로 owner를 실패로 판정하는 detector를 ADR-0025가 거부했기 때문이다. 이 결정은 그 경계를 유지한다.

## 3. 결정 요인

1. 공유 group에 살아 있는 멤버가 있으면 durable ACK 뒤 message가 사라지지 않는다.
2. 소유권과 fencing을 바꾸지 않는다. Timeout detector를 만들지 않는다.
3. Replicated apply는 결정적이다. Node-local 연결 상태를 apply가 읽지 않는다.
4. 같은 message가 두 멤버에게 전달되지 않는다. 특히 QoS 2는 이미 보낸 message를 다른 멤버에게 보내지 않는다.
5. 기존 capacity·expiry·Saturated 의미와 N/N-1 rolling 절차를 유지한다.

## 4. 선택 근거

감지 전 구간과 감지 후 구간은 다른 수단이 필요하다.

- **감지 후(규칙 1).** 죽은 node와의 TCP 연결이 닫히면 ingress는 그 node의 멤버를 고르지 않는다. 재현에서 post-kill publish 40건이 모두 같은 규칙으로 살아 있는 멤버에게 갔다. [ADR-0059](ADR-0059-dynamic-peer-tcp-liveness.md)가 응답 없는 상대의 감지 시간을 10초 근처로 제한한다. 연결 상태는 node마다 다르고 거짓 양성이 있다. 그래서 선호로만 쓰고 소유권에는 쓰지 않는다.
- **감지 전(규칙 2).** 감지 전에 죽은 owner의 멤버 queue에 들어간 message는 규칙 1이 막을 수 없다. 그 멤버가 clean session이면 돌아올 때 reset으로 사라진다. 재배분은 연결 감지 시간과 무관하게 이 손실을 막는다. Apply가 committed state만 보므로 모든 replica가 같은 결과를 만든다.

**새 feature를 쓰는 이유.** 새 field는 command와 snapshot에 들어가고, apply 동작도 바뀐다. [ADR-0032](ADR-0032-feature-activation-approval-policy.md)에 따라 feature마다 rollback window가 따로 끝난다. `session-metadata`와 묶으면 Will 복구를 승인하려는 운영자가 공유 구독 동작 변경도 함께 승인하게 된다. `proposal-recovery`는 command 재제안 기능이라 관련이 없다. 기본 build feature로 두면 활성화만 하면 되고, 모든 member가 광고할 때만 commit된다.

**QoS 2 inflight를 제외하는 이유.** PUBLISH를 이미 보낸 QoS 2 message를 다른 멤버에게 보내면 중복 전달이다. MQTT 5.0은 session이 끝나면 그런 message를 다른 구독자에게 보내지 못하게 한다(MQTT-4.8.2-5). 아직 보내지 않은 `QUEUED` message는 보낸 적이 없으므로 QoS 2도 옮긴다. QoS 1 `AWAIT_PUBACK`은 중복이 허용되므로 옮긴다.

**지연이 남는 범위.** 목적지 멤버가 죽은 node 소속이면 그 message는 다시 그 멤버의 reset·expiry를 기다린다. Apply는 연결 상태를 읽지 않으므로 목적지의 도달 가능성을 모른다. 이 경우도 `shared_filter`가 남아 있어 마지막 멤버가 사라질 때까지 반복해 옮긴다. 도달 가능한 멤버가 없는 group에서는 message가 남아 있다가 그 멤버가 돌아올 때 전달되거나 session이 만료될 때 사라진다.

## 5. 대안

| 대안 | 장점 | 제외 이유 |
| --- | --- | --- |
| Durable command에 후보 목록(`Fallbacks`)을 싣고 apply가 고른다 | Publish 시점에 포화나 도달 불가를 한 번에 피한다. | Command가 group별 후보를 가져야 하고, 이미 queue에 들어간 message의 reset 손실을 막지 못한다. Apply가 도달 가능성을 알 수 없다. |
| 도달 불가 owner의 clean session 구독을 projection에서 제거한다 | Clean 멤버만 정확히 처리한다. | Timeout 기반 판정이라 ADR-0025의 거부 결정과 충돌한다. |
| 출처를 기록하지 않고 reset 때 구독을 보고 다시 유도한다 | Schema가 바뀌지 않는다. | 같은 client가 비공유 구독으로 받은 message와 구분하지 못해 중복 또는 오배분이 생긴다. |
| Apply가 peer 연결 상태로 목적지를 고른다 | 죽은 node의 멤버를 피한다. | Replica마다 상태가 달라 결과가 갈린다. Replicated apply는 committed state만 읽어야 한다. |
| 규칙 1만 적용한다 | Schema가 바뀌지 않는다. | 감지 전에 queue에 들어간 message의 clean session 손실이 남는다. |

## 6. 결과

### 긍정적 결과

- 리더 kill 뒤 살아 있는 멤버가 있는 group의 PUBACK된 QoS 1 message가 어떤 멤버에게는 전달된다. Persistent 멤버가 받은 queue는 그 멤버가 돌아올 때 전달된다.
- CleanStart reset·DISCARD·expiry purge·ownership Release로 사라지던 공유 출처 message가 살아 있는 멤버로 옮겨진다.
- 규칙 1은 활성화 없이 동작한다. 규칙 2는 활성화 전 기존 동작과 bytes를 유지한다.

### 부정적 결과와 비용

- Message마다 공유 filter 문자열이 저장된다. Message byte 계산에 포함해 한도가 같은 비율로 줄어든다.
- 감지 전 구간의 message는 그 멤버가 돌아오거나 session이 지워질 때까지 지연된다. 영구 손실은 막지만 즉시 전달은 아니다.
- 목적지의 QoS가 원래 message보다 낮으면 QoS가 내려간다. 높아져도 올라가지 않는다.
- QoS 2 inflight message는 reset·expiry 때 지금처럼 사라진다.
- 활성화한 cluster는 이 feature를 모르는 build로 rollback할 수 없다.
- 새 field와 apply 규칙은 N-1에 없다. 활성화 전에는 쓰지 않는다.

### 후속 작업

- 실제 EKS에서 BP-02 N-1 재측정은 [TODO](../TODO.md)에서 추적한다. 이 결정은 로컬 process 재현과 단위 시험까지 검증했다.

## 7. 검증

[Cluster §11.35](../specs/cluster-session-and-delivery.md#1135-공유-구독-durable-전달의-owner-손실-수용-매핑)에 연결한다.

1. 도달 불가 owner를 건너뛰는 선택과 모두 도달 불가일 때의 기존 선택을 도메인 시험으로 확인한다.
2. Reset·DISCARD·expiry purge·ownership Release에서의 재배분, 용량 포화, 받을 멤버 없음, QoS 2 inflight 제외와 feature gate를 apply 시험으로 확인한다.
3. Command·snapshot codec의 새 field와 backup 보존을 시험한다.
4. 리더 kill 뒤 clean 멤버의 CleanStart 재접속까지 포함한 3-node process 시험으로 PUBACK된 QoS 1 message가 모두 전달되는지 확인한다.

## 8. 재검토 조건

- 감지 전 구간의 지연이 운영 요구를 넘는다는 EKS 측정이 나온다.
- 목적지 선택이 죽은 node의 멤버에 반복해서 쏠린다는 증거가 나온다.
- QoS 2 inflight message의 손실을 허용할 수 없다는 요구가 나온다.
- Peer 연결 상태 대신 승인된 owner 실패 판정(fencing 근거 포함)이 생긴다.
