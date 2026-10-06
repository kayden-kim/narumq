# ADR-0036: 복제된 proposal receipt와 안전한 재제안

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-06 |
| 결정자 | NaruMQ 프로젝트 (사용자: 안전한 자동 재제안 도입) |
| 관련 요구사항 | [PRD CLU-12·BP-06](../requirements/PRD.md) |
| 관련 스펙 | [Cluster §4.2](../specs/cluster-session-and-delivery.md#42-qos-12-durable-delivery), [Performance §3.6](../specs/performance-and-verification.md#36-bp-06-failure-recovery-smoke-관찰-경계) |
| 관련 계약 | [Replicated-state v1](../contracts/replicated-state-v1.md) |
| 대체한 새 결정 | [ADR-0037](ADR-0037-proposal-request-identity.md)이 receipt key와 정렬을 별도 RequestID 기준으로 대체한다. [ADR-0038](ADR-0038-proposal-receipt-capacity-fences.md)이 count 한도 초과 요청의 fence와 결과 byte 예약을 구체화한다. |
| 대체 범위 | [ADR-0035](ADR-0035-failover-aware-commit-lag-and-unsent-proposal-reclaim.md)의 이미 전송한 application proposal 재제안 금지만 `proposal-recovery` 활성화 뒤 대체한다. 비활성 경로와 나머지 결정은 유지한다. |

## 1. 결정

Receipt는 command를 처음 적용한 결과를 보관한 복제 기록이다. Retirement floor는 다시 적용할 수 없는 오래된 요청의 경계다.

1. 새 build는 `proposal-recovery` feature를 광고한다. 운영자가 기존 feature activation 절차로 활성화해야 한다. 활성화 전 command와 snapshot bytes는 baseline 형식을 유지한다.
2. 활성화 뒤 application command에는 proposer NodeID, 최초 fresh ReadIndex의 index, retirement floor와 원래 command bytes의 SHA-256을 붙인다. OperationID와 payload는 재제안 때 바꾸지 않는다. ConfChangeV2는 이 경로에 넣지 않는다.
3. 각 origin은 node별 retirement floor와 OperationID별 receipt를 복제한다. Receipt는 최초 proof index, command digest와 최초 apply 결과를 담는다. 성공과 domain conflict를 모두 보관한다. 같은 OperationID의 다른 digest는 conflict다.
4. 같은 receipt의 재제안은 state mutation을 반복하지 않는다. 최초 결과를 반환한다. Runtime은 중복 apply에서 ownership close, Will, delivery notification 등 외부 효과를 반복하지 않는다.
5. Receipt를 없앤 뒤 늦게 도착한 요청도 다시 적용하지 않는다. 최초 proof index가 origin의 복제된 retirement floor 이하이면 stale proposal로 거절한다. Origin이 retired member이면 새 적용을 거절한다.
6. Proposer는 완료되지 않은 요청 가운데 가장 작은 최초 proof index를 보호한다. 새 command의 retirement floor는 그 index보다 작아야 한다. 보호할 요청이 없으면 현재 fresh proof index보다 작은 요청을 retire할 수 있다. 새 floor는 command apply와 함께 단조 증가한다. 재시작한 process는 이전 process의 waiter를 재사용하지 않는다.
7. Node별 receipt는 최대 1024개, cluster 전체 receipt는 최대 4096개다. 보관한 결과의 전체 bytes는 최대 32 MiB다. 새 origin 요청은 보호된 receipt와 pending 요청의 한도를 넘으면 아직 제안하지 않고 기다린다. 결과 보관 한도가 부족하면 mutation 전에 capacity conflict를 기록한다. Snapshot의 기존 64 MiB 직렬화 한도는 우선한다.
8. Leader·term 변경 뒤에는 새로운 nonce의 ReadIndex proof를 얻는다. Local apply와 projection이 그 proof까지 끝난 뒤 아직 완료되지 않은 요청만 원래 제출 순서로 재제안한다. Proof에 포함된 원래 command는 재제안 전에 완료한다. 같은 leader·term에서 일정 시간마다 같은 요청을 재제안하는 polling은 하지 않는다.
9. 재제안은 최초 caller deadline 안에서만 한다. 기존 5초 durable commit timeout은 늘리지 않는다. Timeout된 caller에게 늦은 성공 ACK를 보내지 않는다. 후속 command의 retirement floor가 닫기 전에는 원래 요청이 늦게 적용될 수 있다. 그 뒤에는 오래된 frame이 state를 바꾸지 않는다.
10. Origin 사이의 순서는 Raft commit 순서다. 같은 origin의 재제안 batch는 원래 제출 순서를 유지한다. 기존 transport가 보장하지 않는 서로 다른 peer의 frame 도착 순서를 새로 보장하지 않는다.
11. Snapshot·backup은 origin floor와 receipt를 함께 보존한다. Canonical origin 순서는 NodeID, receipt 순서는 OperationID다. 복구한 floor를 낮추지 않는다. `proposal-recovery` 활성화 뒤 지원하지 않는 build로 rollback하지 않는다.
12. BP-06은 NaruMQ에 이 feature를 활성화하고 기존 workload·timeout·판정 규칙을 유지한다. EMQX는 사용자 결정에 따라 pinned 5.8.8과 기존 crash-window 예외를 유지한다. EMQX replay inconsistency의 예외 밖 duplicate는 계속 invalid다.

## 2. 근거

이전 wire·WAL 진단의 OperationID `a8ffafbd-3fbd-4104-b842-51cda777e6d3`는 이전 leader가 socket에서 수신하고 WAL fsync까지 끝냈다. 진단에서 apply는 관측되지 않았다. 미전송 frame 회수만으로 이 요청을 복구할 수 없다. Receipt 없는 재제안은 원래 요청이 뒤늦게 commit될 때 두 번 적용할 수 있다.

최종 이전 suite `bp06-suite-20261006T133044.622538400Z-139472`는 NaruMQ의 계획 밖 disconnect 8건과 proposal deadline log를 같은 시각에 기록했다. 이 suite에는 per-operation wire trace가 없으므로 각 요청의 원격 수신 여부까지 확정하지 않는다. EMQX run 003·004는 초기 sequence 1·2의 재수신과 같은 시각의 `emqx_persistent_session_ds_replay_inconsistency`를 기록했다. 이는 기존 5000ms checkpoint 예외 밖이다.

## 3. 대안과 판단

| 대안 | 판단 |
| --- | --- |
| Timeout 증가·disconnect 예외 확대 | 내구성·복구 결함을 해결하지 않으므로 제외한다. |
| 결과가 불확실한 command를 새 OperationID로 재제안 | 중복 적용을 막을 수 없으므로 제외한다. |
| 시간 기반 receipt 만료 | Clock 차이와 늦은 frame 때문에 안전한 삭제 경계가 없어 제외한다. |
| 영구 OperationID ledger | 실행 시간이 길어질수록 state가 늘어나므로 제외한다. |
| Origin별 proof index floor와 bounded receipt | Clock 없이 오래된 frame을 차단하며 결과와 보관량을 제한할 수 있어 채택한다. |

## 4. 검증 범위

중복 성공·conflict, 다른 payload, retirement 뒤 늦은 frame, snapshot·backup 복구, inactive baseline, caller timeout과 실제 leader loss를 검증한다. 실행 증거와 시험 위치는 관련 스펙의 수용 표에 기록한다. 이 ADR의 채택만으로 BP-06 suite 수용을 뜻하지 않는다.
