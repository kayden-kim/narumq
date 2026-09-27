# ADR-0013: Detached session QoS 0 offline queue 복제와 cluster backup v3

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (detached QoS 0 offline replication과 backup v3 범위; production storage·durable QoS 1/2 제외) |
| 결정일 | 2026-09-26 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD CLU-14](../requirements/PRD.md#92-클러스터와-세션), [PRD TEN-08](../requirements/PRD.md#93-멀티테넌시) (tenant별 offline quota는 미충족·후속) |
| 관련 스펙·데이터 계약 | [Cluster §4.1·§10·§11.9](../specs/cluster-session-and-delivery.md), [Cluster backup v3](../contracts/cluster-backup-v3.md), [replicated-state-v1.proto](../contracts/replicated-state-v1.proto) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md), [Broker core boundaries](../explainers/broker-core-boundaries.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | [ADR-0012](ADR-0012-retained-replication-and-cluster-backup-v2.md)의 archive format v2만 대체한다. Retained replication 결정은 유지한다. |

## 1. 결정

1. **범위와 연결된 전달:** Cluster mode에서 연결이 끊겼지만 유지되는 persistent session(이하 detached session)에 도착한 QoS 0 message만 Raft committed state에 저장한다. Connected session의 live delivery는 기존 node-local queue를 사용한다. QoS 1/2 offline entry, durable ACK([ADR-0003](ADR-0003-durable-ack-barrier.md)), cross-node live fan-out(Cluster §11의 별도 시나리오)과 N-1 compatibility는 이 결정의 범위가 아니다.
2. **복제 object:** 각 `(tenantId, clientId)` key에 `OfflineSessionState{tenantId, clientId, epoch, sessionExpiresAtUnixMillis, nextSequence, messages[]}`를 둔다. 각 `OfflineMessage`는 `sequence`, `topic`, `payload`, `qos(=0)`, `retain`, `expiresAtUnixMillis`를 가진다. Apply 시 session 안에서 1부터 증가하는 `sequence`가 안정된 message identity이며 FIFO 순서다.
3. **명령:** `OfflineChange`는 `DETACH`, `ENQUEUE`, `CLAIM` 세 action을 제공한다.
   - `DETACH`: owner가 persistent session 연결 종료 때 제안한다. `(ownerNodeId, epoch)`가 현재 ownership과 정확히 일치해야 한다. Record가 없으면 `nextSequence=1`로 만든다. 같은 epoch record가 이미 있으면 retry-safe no-op이다. 더 작은 epoch record는 기존 message를 보존하고 epoch만 올린 뒤 이어 붙인다. Local queue에서 가져온 FIFO message는 최대 64개, `topic+payload` 합계 최대 768 KiB까지 한 command에 담는다.
   - `ENQUEUE`: 어느 node든 publish 대상이 detached session이면 제안한다. `ownerNodeId=0`이며 message는 정확히 하나다. 같은 key의 record가 있고 epoch가 일치해야 한다. 아니면 `offline_session_missing` conflict다.
   - `CLAIM`: CONNECT가 새 ownership epoch를 commit한 뒤 새 owner가 제안한다. `(ownerNodeId, epoch)`가 현재 ownership과 정확히 일치해야 한다. Record를 삭제하고 messages를 apply 결과로 반환한다. Record가 없으면 `Found=false`다.
4. **전달 의미:** at-most-once다. `CLAIM` commit이 message record를 먼저 삭제하고 이후 local delivery queue에 넣는다. CLAIM 뒤 wire 전송 전 process가 중단되면 QoS 0 message가 유실될 수 있다. 자동 재시도는 하지 않는다. ENQUEUE commit 응답 timeout 뒤 늦게 commit된 message는 offline queue에 남는다. QoS 0 publisher에는 ACK가 없어 결과를 보장하지 않는다.
5. **결정적 한도:** Apply에서 session당 64개, 전체 tenant 합산 4,096개·16 MiB, payload당 1,000,000 bytes를 제한한다. DETACH command의 `topic+payload` 합계는 768 KiB 이하여야 한다. 전체 byte 계산은 각 message에 대해 `len(tenantId)+len(clientId)+len(topic)+len(payload)`를 합한다. 한도를 넘으면 command 전체를 `offline_capacity_exceeded`로 거절하고 기존 state를 변경하지 않는다(같은 apply에서 expiry purge한 결과는 유지한다). Offline session record 수는 ownership row 수를 넘지 않으므로 별도 상한은 두지 않는다. Tenant별 quota(TEN-08)는 제공하지 않는다.
6. **만료:** Offline command를 apply하기 전에 command의 `proposedAtUnixMillis` 기준으로 `0 < expiresAt <= proposedAt`인 message와 `0 < sessionExpiresAt <= proposedAt`인 session record를 제거한다. Apply는 replica local clock을 읽지 않는다. Session expiry는 disconnect 시각과 MQTT 5 Session Expiry Interval의 합이다. 만료하지 않는 persistent session(MQTT 3.1.1 또는 MQTT 5 `0xFFFFFFFF`)은 0으로 저장한다.
7. **Ownership 연동:** Ownership `Release`는 같은 key의 offline record도 제거한다. Transfer/acquire는 기존 record를 보존하고 새 owner의 CLAIM이 가져간다. Snapshot은 offline record마다 `record.epoch` 이상인 ownership epoch row를 가져야 한다.
8. **SessionPresent:** `CleanStart=0` CONNECT의 CLAIM이 `Found=true`이면 다른 node에 reconnect해도 CONNACK `SessionPresent=1`이며 message는 sequence FIFO로 전달한다. `CleanStart=1`이면 CLAIM으로 record를 지우고 message를 폐기한다.
9. **실패 처리:** DETACH가 quorum, compatibility 또는 stale epoch 이유로 실패하면 local queue를 유지한다. CLAIM 실패는 CONNECT를 ownership-unavailable reason으로 거절하고 local session 변경을 rollback한다. 기존 local session이 없었던 시도는 새 delivery queue 등록과 tenant route binding을 제거한다. 기존 local session이 있으면 CLAIM 성공 전까지 expiry/WILL timer를 유지하고, 실패 시 기존 timer·queue·route 상태를 보존한다. ENQUEUE 실패는 해당 delivery만 capacity일 때 `Saturated`, 나머지 실패일 때 `Unavailable`로 보고한다.
10. **한계:** Owner node가 연결 중에 crash하면 DETACH가 없으므로 이후 도착 message를 capture하지 않는다. Detach 전환 중 local queue 조회와 제거 사이에 해당 node로 도착한 QoS 0 message는 유실될 수 있다. ENQUEUE는 대상 session마다 Raft commit 한 번을 추가한다.
11. **Schema와 profile gate:** Replicated-state schema는 `1.0`을 유지하고 unknown-field rejection을 유지한다. Offline command proposal은 retained와 같은 exact member-profile gate 뒤에서만 허용한다. Gate는 fresh read barrier 뒤 모든 non-retired member의 release, protocol, schema가 정확히 일치하는지 확인한다.
12. **Archive v3:** `cluster-backup-v3` format `3.0`, magic `NMQBKUP3`를 사용한다. `BackupStateSummary`의 `offline` kind count는 detached session record 수와 queued message 수의 합이다. `not_replicated_kinds`는 제거한다. Tenant digest는 offline record의 tenant를 포함한다. `NMQBKUP1`과 `NMQBKUP2`는 `unsupported archive format N; capture a new backup with this release`로 거절한다. Restore 경계는 v2와 같이 같은 cluster, 정확히 세 bootstrap voter, learner 없음, 빈 directory다.
13. **Cutover:** Live migration이나 converter를 제공하지 않는다. 이전 build에는 Raft offline state가 없으므로 새 build는 빈 replicated offline state에서 시작한다. Exact profile gate 때문에 upgrade는 전체 cluster 중지 후 재시작하는 cutover다. 이전 process의 node-local queue는 종료와 함께 사라진다. 운영자는 upgrade 전 persistent subscriber를 재접속시켜 queue를 drain해야 하며, drain하지 않은 기존 queue의 손실 가능성을 수용해야 한다. v2 archive는 v2 지원 이전 build에서만 restore할 수 있다. 이번 변경에서 v2를 v3로 복원·변환하지 않는다.

## 2. 배경

ADR-0013 결정 당시 Cluster MQTT ingress는 QoS 0만 받았으며 live route와 bounded delivery queue는 node-local이었다. Persistent session이 끊긴 뒤 다른 node로 reconnect하면 이전 queue를 이어받지 못했다. Retained state는 ADR-0012에서 Raft와 backup v2에 연결됐지만 offline queue는 빠져 있어 detached delivery가 owner node crash, restore와 joiner catch-up을 지나 보존되지 않았다.

이 변경은 delivery queue 전체를 replicated authority로 바꾸지 않는다. Detach 시점에 남은 local FIFO를 캡처하고, 그 뒤 detached 대상에 매칭된 delivery만 ENQUEUE한다. CONNECT ownership acquire 뒤 CLAIM으로 새 owner가 가져간다. 당시 connected live delivery와 QoS 1/2 ACK·inflight state는 별도 작업이었으며, live QoS 0 forwarding은 ADR-0014, durable QoS 1/2는 후속 ADR-0015에서 정의했다.

TEN-08은 P0 요구사항이며 tenant별 offline queue, retained bytes와 저장 용량 quota를 요구한다. 이 결정의 승인 범위에서는 tenant별 quota를 제외하므로 TEN-08의 해당 수용 기준은 미충족 상태로 남는다. Global offline capacity는 tenant quota를 대체하지 않는다.

## 3. 결정 요인

1. Detached QoS 0 persistent session의 state를 restart·restore·catch-up 후 관찰 가능하게 복구한다.
2. Connected live publish 경로에 per-message Raft write를 추가하지 않는다.
3. Fencing, FIFO, duplicate suppression과 expiry 결과를 replica마다 결정적으로 유지한다.
4. 기존 unknown-field rejection과 retained의 exact profile gate를 재사용한다.
5. Offline v2 archive migration, TEN-08 tenant quota, production storage와 durable QoS 1/2를 검증 범위 밖이라고 명시한다.

## 4. 선택 근거

Detach 시점 capture는 기존 local queue의 FIFO message를 한 번만 이관하고, 이후 publish부터 ENQUEUE 경로로 보낸다. 따라서 connected session의 매 publish를 Raft에 쓰는 방식보다 live path 변경과 quorum cost가 작다. Application state machine이 epoch, expiry, sequence, capacity를 committed command만으로 판정하므로 stale owner나 replica local clock이 상태를 다르게 만들지 않는다.

CLAIM은 queue 제거와 delivery 사이에 작은 QoS 0 유실 창을 둔다. 이는 QoS 0의 at-most-once 성격과 맞고, CLAIM 뒤 재시도로 같은 message를 다시 전송하는 중복 위험을 피한다. Global capacity, session cap과 payload limit는 Raft apply에서 결정적으로 거절된다. Tenant별 quota는 이를 대신하지 않으며 TEN-08 미충족으로 기록한다.

Backup v3는 새 kind를 required summary와 tenant digest에 넣어 restore가 offline state를 검증하게 한다. 구형 archive에는 offline state가 없으므로 v2→v3 변환을 제공하지 않고, 운영자가 persistent queue를 drain한 뒤 cutover한다. Production EBS와 power-loss 보장은 이 작업에서 검증하지 않는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 모든 persistent delivery를 Raft로 보낸다. | Connected session과 detached session의 단일 delivery source를 갖는다. | 모든 message마다 write가 생기고 현재 live 경로를 바꾸며 §11 cross-node live fan-out과 겹친다. | 요청된 offline 범위보다 넓고 connected path의 성능·ACK 경계까지 변경한다. |
| v2→v3 live migration 또는 converter를 제공한다. | Archive format 전환 중 기존 queue를 옮길 수 있다. | 기존 offline queue는 memory-only였으므로 v2 archive에 원본이 없고, 과거 process를 읽을 converter도 없다. | Lossless conversion 근거가 없다. Drain 요구를 명시하고 v2를 거절한다. |
| CLAIM 전에 전송하고 이후 progress를 commit해 at-least-once로 만든다. | Commit 중단 뒤 재전송할 수 있다. | 전송 뒤 progress 전에 crash하면 QoS 0 message가 중복된다. | At-most-once 계약과 맞지 않는다. |
| Replicated-state schema를 `1.1`로 올린다. | Field 추가를 schema revision으로 표시한다. | 별도 schema migration 및 retained 경계와의 분기가 생긴다. | ADR-0012와 같은 이유로 schema `1.0` 및 exact profile gate를 유지한다. |
| Tenant별 offline quota를 이번 변경에 넣는다. | TEN-08의 quota 부분을 충족할 수 있다. | Tenant별 정책·집행·경보와 격리 시험이 별도 요구사항이며 승인 범위 밖이다. | 보류한다. TEN-08은 미충족 후속 항목으로 남기며 global limit가 대체하지 않는다. |

## 6. 결과

### 긍정적 결과

- Detached persistent session의 QoS 0 queue가 Raft state, snapshot, backup/restore와 learner catch-up을 통과한다.
- Epoch fencing, deterministic expiry/capacity와 monotonic sequence가 offline command에 적용된다.
- Connected live delivery는 기존 node-local 경로를 유지한다.
- Backup v3는 offline kind count/digest와 offline tenant를 검증하고, 구형 archive를 명시적으로 거절한다.

### 부정적 결과와 비용

- ENQUEUE는 대상 session마다 quorum-backed command 비용을 추가한다.
- At-most-once CLAIM은 commit 뒤 wire 전송 전 crash에서 QoS 0 message를 유실할 수 있다.
- Owner가 연결 중 crash하는 경우 및 detach transition의 짧은 capture race는 보호하지 않는다.
- Cutover 전에 drain하지 않은 node-local queue는 process 종료와 함께 사라질 수 있다. v2 archive migration과 rolling upgrade는 지원하지 않는다.
- TEN-08의 tenant별 offline queue·retained bytes·storage quota는 여전히 미충족이다. Global quota는 tenant 간 noisy-neighbor 격리를 보장하지 않는다.
- Linux/EBS power-loss, WAL torn-tail, volume loss/reseed, retention/RPO/RTO 및 durable QoS 1/2 ACK는 검증 범위 밖이다. 따라서 이 ADR과 process acceptance만으로 CLU-14 전체 완료나 production readiness를 선언하지 않는다.

### 후속 작업

- [ ] Production storage gate: [ADR-0009 §7](ADR-0009-raft-production-state-profile.md#7-검증)에서 Linux/EBS power-loss, WAL torn-tail, volume loss/reseed와 retention/RPO/RTO를 검증한다.
- [ ] QoS 1/2 offline delivery와 durable ACK: [ADR-0003 §9](ADR-0003-durable-ack-barrier.md#9-검증)에서 state, ACK barrier와 replica failure acceptance를 완료한다.
- [ ] Connected owner crash 중 capture와 detach transition race를 별도 acceptance로 평가한다.
- [ ] N-1 archive compatibility와 승인된 migration/rollback 정책을 결정한다.
- [ ] TEN-08의 tenant별 offline queue, retained bytes, storage quota와 초과 경보를 구현·검증한다. P0 요구사항이며 이 결정에서 충족하지 않는다.

## 7. 검증

| 결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Queue port가 만료분을 제외하고 FIFO deep copy를 제공한다. | `go test ./internal/adapters/memory/deliveryqueue/... -run TestListQueued -count=1` | Queue 상태를 바꾸지 않고 clone·미등록 오류·expiry를 확인한다. | PASS: `TestListQueuedReturnsLiveFIFOCopiesWithoutMutation` |
| Offline state가 deterministic FIFO, fencing, expiry, capacity와 snapshot validation을 보장한다. | `go test ./internal/application/membership/... ./internal/adapters/raft/records/... -count=1` | Sequence 1..N, stale epoch, session/global capacity, expiry와 codec validation을 확인한다. | PASS: membership 및 records package unit tests 전체 통과 |
| CONNECT와 publish의 offline 연결이 실패 결과와 local queue 정책을 지킨다. | connect, publish, subscribe, TCP와 runtime focused package tests | Claim rollback, detach fallback, SessionPresent, offline enqueue와 local fallback을 확인한다. | PASS: connect/publish/cmd focused suites 통과; route/timer rollback 수정 뒤 `go test ./internal/application/connect/... -count=1` 재실행 통과. |
| Backup v3가 offline kind와 tenant digest를 포함하고 v1/v2를 거절한다. | records archive 및 restore tests | Format 3, required offline summary, v1/v2 reject, reject 전 target 불변을 확인한다. | PASS: `backup_test.go` 및 `cluster_backup_restore_test.go` 통과 |
| 실제 cluster MQTT 경로가 restore·restart·joiner catch-up 뒤 FIFO를 보존한다. | `TestClusterBackupRestoreProcessCrashAndResync` | Owner crash 뒤 SessionPresent와 정확한 순서, v3 summary, restored/restarted/joiner delivery 및 동일 commit index summary를 확인한다. | PASS: process acceptance (17.62s); 상세는 [Cluster §11.9](../specs/cluster-session-and-delivery.md#119-offline-queue-replication과-backup-v3-수용-매핑) |
| Repository local gate가 통과한다. | `GOMAXPROCS=4 go run ./tools/verify local` | Vet, suite, race, fuzz, traceability, architecture, coverage와 mutation gate가 PASS한다. | PASS after route/timer rollback fixes: `local: PASS`; four fuzz targets와 all-package race, traceability, architecture, coverage PASS. Mutation: 17 killed/8 timed out, 42 killed/4 timed out, 0 lived/uncovered. Timeout은 kill로 계산하지 않았다. Windows unconstrained race run은 system commit limit 1455로 실패해 제한 실행했다. |
| Production storage와 tenant별 quota가 수용됐는가. | Linux/EBS production acceptance 및 TEN-08 quota acceptance | 실제 power-loss/volume 손실 경계와 tenant 별 quota·경보를 검증한다. | 범위 밖·미실행; 각각 ADR-0009 후속과 TEN-08 미충족으로 남김 |

## 8. 재검토 조건

- At-most-once QoS 0 claim 뒤 손실 허용이 PRD 또는 client contract와 맞지 않는다는 근거가 나오면 재검토한다.
- QoS 1/2 offline state를 추가하거나 delivery progress 의미를 바꾸면 ADR-0003과 이 결정을 함께 재검토한다.
- Tenant별 quota 정책과 경보가 확정되면 TEN-08 후속 설계를 추가하고 global limit과 분리된 acceptance를 만든다.
- v2 archive를 v3에서 복원해야 하는 요구가 생기면 archive에 없는 offline state를 추정하지 않고 운영 cutover/migration 결정을 다시 승인한다.
- Linux/EBS acceptance, retention/RPO/RTO 또는 N/N-1 compatibility가 확정되면 ADR-0009, CLU-14와 archive compatibility 범위를 갱신한다.
