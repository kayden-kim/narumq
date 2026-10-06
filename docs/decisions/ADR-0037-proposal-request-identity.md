# ADR-0037: Proposal 재시도와 논리 operation의 식별자 분리

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-06 |
| 결정자 | NaruMQ 프로젝트 (안전한 자동 재제안의 기존 호출 계약 유지) |
| 관련 결정 | [ADR-0036](ADR-0036-replicated-proposal-receipts-and-safe-reproposal.md) |
| 대체 범위 | ADR-0036 §1 2·3·11항의 receipt 식별·정렬 기준만 대체한다. |

## 결정

`CommandDTO.OperationID`는 논리 operation의 ID다. Membership의 STAGE_JOIN과 ADD_LEARNER처럼 서로 다른 command가 이 값을 공유한다. 따라서 receipt key로 사용할 수 없다.

1. Runtime은 최초 command 제출 때 별도의 canonical UUID `RequestID`를 만든다. 재제안은 같은 RequestID를 유지한다. 기존 OperationID와 payload는 변경하지 않는다.
2. Receipt key는 `(origin NodeID, RequestID)`다. 같은 key와 같은 digest는 최초 결과를 반환한다. 같은 key의 다른 digest 또는 최초 proof index는 conflict다.
3. 같은 OperationID의 서로 다른 RequestID는 서로 다른 command다. 기존 membership idempotency와 상태 전이는 그대로 적용한다.
4. Origin 안의 canonical receipt 순서는 RequestID다. Snapshot·backup은 RequestID를 보존한다.
5. RequestID는 기존 ReadIndex nonce와도 다르다. 재제안 proof마다 nonce는 바뀌지만 RequestID는 바뀌지 않는다.

기존 operation ID를 공유하는 서로 다른 membership 전이가 정상 적용되는지 검증한다. 같은 RequestID의 payload 변경과 실제 재제안의 중복 적용 차단도 별도로 검증한다.
