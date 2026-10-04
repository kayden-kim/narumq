# ADR-0033: Connection rebalance operation

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-04 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD CLU-13](../requirements/PRD.md) |
| 관련 스펙·데이터 계약 | [Cluster §9·§11.31](../specs/cluster-session-and-delivery.md#9-rebalance), [Replicated-state v1](../contracts/replicated-state-v1.md), [Management API v1](../contracts/management-api-v1.md) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md), [Project structure](../architectures/project-structure.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

Rebalance는 연결이 많은 node에서 다른 node로 연결을 옮기는 수동 일회성 operation이다. 운영자가 `POST /api/v1/cluster/rebalance`로 시작한다. GET은 조회하고 DELETE는 취소한다. POST·DELETE는 `Operator`와 `Idempotency-Key`, GET은 `ReadOnly`를 요구한다.

1. 새 replicated `ConnectionRebalanceChange`와 feature token `connection-rebalance`를 둔다. 모든 non-retired member가 token을 광고한 뒤 activation을 commit해야 한다. Activation은 해당 feature의 rollback window를 끝낸다. 비활성 상태에서는 command와 snapshot record를 쓰지 않는다. Schema는 `1.0`을 유지한다. Command oneof 번호는 22, snapshot field 번호는 13이다. 이전 baseline descriptor는 유지한다.
2. `maxDeviationConnections`, `connectionRatePerSecond`, `loadBalancerPropagationDelayMs`, `timeoutMs`는 모두 필수다. 제품 기본값은 없다. 편차는 절대 연결 수 `1..1_000_000`이다. Rate·LB 대기·timeout·선택 `serverReference`의 검증은 node drain과 같다.
3. START 시점의 VOTER ID를 오름차순 participant로 고정한다. 최소 2개가 필요하다. Record는 cluster에 0개 또는 1개이며 새 START는 terminal record를 대체한다. Running rebalance, active drain, 진행 중 membership operation은 START를 거절한다. Running rebalance 중 drain START는 retryable `connection_rebalance_in_progress`다.
4. Phase는 `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`다. START·REPORT·FAIL·CANCEL은 replicated apply로 판정한다. 같은 operation의 이전 시각 REPORT와 비참여 node의 REPORT는 거절한다. CANCEL은 Running만 종료하며 Cancelled replay는 허용한다. Completed·Failed 취소는 `rebalance_not_cancellable`이다. Timeout은 `rebalance_timeout` FAILED다.
5. 각 voter는 CONNECT로 묶인 `tcp.Server.bySession`의 연결 수를 보고한다. CONNECT 전 socket과 ownership row는 세지 않는다. Report에는 node ID, 연결 수, shedding 여부, 이전한 session 수와 proposer 시각을 넣는다. 연결 수는 `0..4_294_967_295`다.
6. `total`은 모든 participant 연결 수의 합이다. `fairShare = ceil(total/n)`, `ceiling = fairShare + maxDeviationConnections`다. 모든 보고가 fresh이고 모든 연결 수가 ceiling 이하이며 shedding node가 없으면 REPORT apply가 COMPLETED로 바꾼다. 보고 시각 0은 미보고다. 나이가 15초를 넘으면 stale이며 미래 시각은 clock skew로 보고 fresh로 취급한다.
7. Node controller는 200ms마다 판정한다. 값이 바뀌면 즉시 보고하고 같은 값도 5초마다 다시 보고한다. Fresh 보고에서 local 연결 수가 ceiling을 초과하면 admission과 readiness를 닫고 목표를 fairShare로 고정한다. Readiness reason은 `connection rebalance shedding`이다. Management NodeState는 바꾸지 않는다.
8. Local 상태는 Idle → WaitingForLB → Transferring → Settling → Idle이다. LB 전파 대기를 마친 뒤 설정 rate로 tick당 TRANSFER 하나를 시도한다. Rate는 이전 시도 사이의 최소 간격을 제한하며 처리량을 보장하지 않는다. 승인한 200ms tick·tick당 한 번의 구조 때문에 실제 시도 속도는 최대 5회/초다. Rate가 5보다 커도 이 상한은 유지되며 commit 대기는 속도를 더 낮출 수 있다. 목표 이하이면 5초간 재접속을 기다린 뒤 admission을 연다. Terminal·취소·timeout이면 admission을 연다. Drain과 rebalance admission의 AND를 MQTT CONNECT에 적용한다.
9. 첫 TRANSFER 전에 fresh read barrier와 route projection 준비를 확인한다. 연결된 다른 participant 중 보고된 연결 수가 가장 작은 node로 옮긴다. 동률이면 작은 ID를 고른다. Local 연결 session ID 오름차순에서 local owner이며 epoch가 소진되지 않은 첫 후보를 고른다. 이번 shedding에서 시도한 session은 다시 고르지 않는다.
10. 기존 ownership `TRANSFER(epoch+1)`와 `CloseSessionForDrain`을 재사용한다. MQTT 5는 `DISCONNECT 0x9C`와 선택 Server Reference를 받고 MQTT 3.1.1은 연결이 닫힌다. 새 owner는 committed delivery state를 새 epoch ATTACH로 재개한다. [ADR-0028](ADR-0028-planned-handoff-will-and-reason-metric.md)의 epoch 비교가 계획 인계 Will을 분류하므로 drain 전용 분류를 추가하지 않는다.
11. 같은 Idempotency-Key와 같은 parameter 5개는 phase와 무관하게 replay한다. 같은 key의 다른 parameter는 `idempotency_conflict`다. 다른 key의 Running record는 retryable `connection_rebalance_in_progress`다. Feature 비활성 시작은 non-retryable `feature_inactive`다.
12. Snapshot은 record와 participant 깊은 복사본을 보존하며 record 하나를 objectCount 1개로 센다. Feature 없는 rebalance snapshot은 거절한다. Controller는 복구된 Running record에서 실행을 재개한다. 별도 checkpoint authority는 두지 않는다.

## 2. 배경

기존 node drain은 전체 node를 비우는 종료 절차다. CLU-13은 node 증설 뒤 연결 수만 정한 편차 안으로 줄여야 한다. Ownership row는 live 연결 수가 아니므로 TCP 연결을 직접 측정해야 한다.

## 3. 결정 요인

- Broker가 quorum-visible 보고로 수렴을 판정한다.
- 운영 기본값을 임의로 정하지 않는다.
- 기존 fencing·delivery 재개 경계를 유지한다.
- 연결을 끊는 작업을 명시적인 operation 안으로 제한한다.

## 4. 선택 근거

복제 operation은 조회 node와 재시작에 관계없이 같은 상태를 제공한다. 목표를 ceiling이 아닌 fairShare로 잡아야 저부하 node가 채워진다. Drain의 TRANSFER와 MQTT 종료 경로를 재사용하면 새 인계 프로토콜 없이 epoch fencing을 유지할 수 있다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Node-local shed API | 구현 범위가 작다. | 재시작하면 상태가 사라지고 운영자가 직접 수렴을 판정한다. | Cluster 전체 operation의 복제 상태가 필요하다. |
| 자동 상시 controller | 운영자 시작 없이 계속 조정한다. | 운영 기본값이 필요하며 오동작하면 연결을 계속 끊을 수 있다. | 먼저 수동 일회성 operation으로 범위를 제한한다. |

## 6. 결과

- 운영자는 parameter를 명시하고 수렴·실패·취소 결과를 조회한다.
- Participant는 START 때 고정된다. 실행 중 membership 변경은 막지 않는다. Participant가 죽거나 제거되면 보고가 stale해져 timeout FAILED가 될 수 있다.
- Client의 LB 재접속 경로는 broker가 통제하지 않는다. 재접속이 과부하 node에 몰리면 수렴하지 않을 수 있다.
- 내부 시간 상수는 운영 parameter의 기본값이 아니다. Production 부하·성능은 이 local 수용 범위 밖이다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| 수렴과 local 상태 전이 | connrebalance domain 시험 | Fresh·편차·shedding·LB 대기·settle 경계 일치 | [Cluster §11.31](../specs/cluster-session-and-delivery.md#1131-연결-rebalance-수용-매핑) |
| 복제·호환성·상호 배제 | Membership·records·DTO bridge 시험 | Apply 충돌, snapshot 재개, feature 없는 state 거절, baseline 유지 | 같은 수용 매핑 |
| 관리 계약과 실제 MQTT 연결 | HTTP·OpenAPI·3-node process 시험 | 취소 뒤 READY, 12/7/0에서 ceiling 이하 수렴, session 재개 | 같은 수용 매핑 |
| 기존 drain과 전체 검증 | Drain process 회귀·local gate | 기존 barrier·redirect·전체 gate 유지 | 같은 수용 매핑 |

## 8. 재검토 조건

- 운영 부하에서 내부 시간 상수 조정이 필요하면 측정 근거로 재검토한다.
- 자동 상시 조정이 필요하면 운영 기본값과 반복 종료 위험을 먼저 결정한다.
- Participant 변경이나 broker가 통제하는 재접속 배치가 필요하면 복제 계약을 먼저 정한다.
