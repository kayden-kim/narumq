# ADR-0035: Failover-aware commit lag와 미전송 proposal 회수

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-06 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD CLU-12·BP-06](../requirements/PRD.md) |
| 관련 스펙 | [Cluster §4.2](../specs/cluster-session-and-delivery.md#42-qos-12-durable-delivery), [§4.5](../specs/cluster-session-and-delivery.md#45-publish-backpressure-admission), [§11.28](../specs/cluster-session-and-delivery.md#1128-publish-backpressure-수용-매핑) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md), [Project structure](../architectures/project-structure.md) |
| 대체 범위 | [ADR-0030](ADR-0030-publish-backpressure-admission.md) §1 5항의 backlog 나이 정의와 `narumq_cluster_commit_backlog_oldest_seconds` 정의만 대체한다. |
| 대체한 새 결정 | [ADR-0036](ADR-0036-replicated-proposal-receipts-and-safe-reproposal.md)이 `proposal-recovery` 활성화 뒤 이미 전송한 application proposal의 재제안 금지만 대체한다. |

## 1. 결정

Generation은 같은 leader·term과 연결 상태에서 제출한 요청을 구분하는 번호다. Socket write는 peer 연결에 frame 쓰기를 시작하는 동작이다.

1. 도달 가능한 leader는 local Raft status의 `LeaderID != 0`이면서 다음 조건 가운데 하나를 만족하는 leader다.
   - `LeaderID == local NodeID`
   - Dynamic transport의 `Status(LeaderID).Connected == true`
2. 도달 가능한 leader가 없으면 `commit_lag`는 활성화되지 않는다.
3. Backlog의 `generation`은 다음 두 경우에 1 증가한다.
   - `(LeaderID, Term)`이 바뀐다.
   - 도달 가능한 leader가 도달 불가능해진다.
4. Commit을 Raft에 제출한 순간의 generation을 entry에 기록한다. 이 값이 현재 generation과 다르면 그 entry는 나이 계산에서 영구히 제외한다. 아직 제출하지 않은 entry는 계속 포함한다.
5. 포함된 entry 가운데 가장 오래된 entry의 나이는 `now - max(startedAt, reachableSince)`다. `reachableSince`는 현재 leader에 도달할 수 있게 된 시각이다.
6. 이 값이 `maxCommitLagMillis` 이상이면 거절한다. Metric `narumq_cluster_commit_backlog_oldest_seconds`도 같은 값을 출력한다.
7. Failover 지연은 기존 5초 `clusterDurableCommitTimeout`이 처리한다. Quorum 손실로 leader가 물러난 뒤에도 같다.
8. Socket write를 한 번도 시작하지 않은 raft-lane `MsgProp` frame만 회수할 수 있다. 회수 대상은 다음 두 가지다.
   - 연결이 없는 peer의 outbox ring에 남은 frame
   - Transport가 `ErrPeerBackpressure`로 받지 않은 frame
9. 회수한 frame은 adapter의 기존 forwarded FIFO에 넣는다. Write를 시작한 frame(`outCurrent`)은 기존처럼 남겨 두었다가 재연결 뒤 다시 보낸다.
10. Leader peer에 연결되어 있지 않으면 forwarded FIFO flush credit은 0이다.
11. 서로 다른 peer에 동시에 남은 proposal의 상대 순서와 FIFO overflow 때 남은 frame의 순서는 보장하지 않는다.
12. 결과가 확정되지 않은 commit의 timeout과 연결 종료는 Cluster §4.2와 [ADR-0015 §13](ADR-0015-cluster-durable-qos-delivery.md)을 유지한다.

## 2. 배경과 근거

진단 실행 `.artifacts/benchmark/bp06-diagnostics/bp06-trace-20261006T034857.756314900Z/`의 모든 kill round에서 follower가 이전 leader에게 보낸 commit 1건이 5초 동안 끝나지 않았다. 그동안 backlog 나이가 기본값 1초를 넘었다. 다른 command는 같은 시간에 약 20ms 안에 commit됐다.

Backpressure 구간은 node1 31.57–35.57, node2 46.85–50.85, node3 00.91–05.19였다. PUBLISH 109건이 `0x97`로 거절됐고 MQTT 3.1.1 연결은 닫혔다. Round 1 SEND의 socket write 시작 지연은 10.93초였다. 이 frame은 끊긴 peer의 outbox에 남았다가 늦게 apply됐다.

## 3. 결정 요인과 선택 근거

정상 leader에 도달할 수 있는 동안의 실제 저장 정체만 admission에 반영한다. 이전 leader에 묶인 요청의 결과를 새 요청의 거절로 확대하지 않는다. Socket write를 시작하지 않은 frame은 원격 수신 결과가 불확실한 요청과 구분할 수 있다. 이 경계에서만 기존 FIFO로 회수하므로 일반 command 중복 제거와 replicated schema를 추가하지 않는다.

## 4. 대안

| 대안 | 장점 | 채택하지 않은 이유 |
| --- | --- | --- |
| 고아 commit만 제외 | 기존 나이 계산 변경이 작다. | 선출이 1초를 넘는 round에서 거절이 남는다. |
| 기본값을 3000ms로 증가 | 구현 변경이 작다. | 연쇄 거절이 약 2초 남는다. 정상 저장 정체의 감지도 늦어진다. |
| OperationID 중복 제거 ledger를 둔 재제안 | 이미 전송한 요청도 다시 제안할 수 있다. | Cluster §4.2의 새 deduplication 없음 계약과 replicated schema를 바꿔야 한다. |

## 5. 결과와 한계

- Leader 변경과 연결 단절로 고아가 된 commit은 새 PUBLISH의 `commit_lag` 거절을 만들지 않는다.
- 같은 peer에서 회수한 frame은 queue 순서를 유지한다. Peer 사이 순서와 overflow 뒤 순서는 보장하지 않는다.
- `outCurrent`는 회수하지 않는다. 이전 leader가 받은 뒤 결과가 확정되지 않은 요청은 5초 timeout 뒤 성공 ACK 없이 연결을 닫을 수 있다.
- BP-06 판정과 comparator 예외는 바꾸지 않는다. 이 연결 종료만 남아 suite가 invalid여도 결과를 미수용으로 기록한다.
- Wire·replicated schema·apply 계약은 유지한다.

## 6. 검증

행동 시험과 실행 증거는 [Cluster §11.28](../specs/cluster-session-and-delivery.md#1128-publish-backpressure-수용-매핑)에 연결한다. 실제 compare·5회 suite의 거절, 회수 log와 연결 종료 분류는 [Performance §3.6](../specs/performance-and-verification.md#36-bp-06-failure-recovery-smoke-관찰-경계)에 기록한다. 이 결정의 채택은 시험 통과나 BP-06 수용을 뜻하지 않는다.

## 7. 재검토 조건

이미 전송한 요청도 재제안해야 하거나 peer 사이의 proposal 순서를 보장해야 하면 중복 제거·순서·schema 계약을 먼저 다시 결정한다.
