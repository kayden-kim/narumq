# ADR-0038: Proposal receipt의 용량 경계

| 항목 | 값 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-06 |
| 관련 결정 | [ADR-0036](ADR-0036-replicated-proposal-receipts-and-safe-reproposal.md), [ADR-0037](ADR-0037-proposal-request-identity.md) |
| 대체 범위 | ADR-0036의 receipt count 초과 요청과 결과 byte 예약 규칙을 구체화한다. |

## 배경

사용자는 NaruMQ의 안전한 자동 재제안을 선택했다. RequestID와 digest로 같은 요청을 구분한다. 결과를 무한히 저장하면 메모리가 늘어난다. 반대로 용량 초과 결과를 버리면 같은 요청이 나중에 성공할 수 있다.

## 결정

1. Origin마다 receipt를 최대 1024개 저장한다. 클러스터 전체에는 최대 4096개 저장한다. Runtime은 현재 receipt와 미완료 요청을 합산해 origin 한도 전에 기다린다. 기다리는 동안에도 원래 caller deadline은 유지한다.
2. Count 한도를 넘은 요청은 payload를 바꾸지 않는다. Origin의 `rejectedThrough`를 해당 요청의 최초 `ProofIndex`로 올린다. `rejectedIndex`에는 이 판정을 적용한 Raft index를 저장한다. 아직 receipt가 없는 요청의 proof가 이 경계 이하이면 `proposal_receipt_capacity_exceeded`를 반환한다. 이미 저장한 receipt는 이 판정보다 먼저 조회한다.
3. 이 capacity fence는 origin마다 두 uint64 값만 저장한다. Snapshot과 backup에도 두 값을 저장한다. 공간이 생겨도 count 한도로 거절한 요청을 나중에 성공시키지 않는다. 새 요청은 더 최근의 fresh proof를 받아 진행한다. 이미 GC된 요청은 retirement fence가 거절한다.
4. Count fence로 거절한 요청은 개별 receipt를 만들지 않는다. 응답 index는 capacity fence를 만든 index다. 정상적으로 수용한 요청과 byte 예산으로 거절한 요청은 최초 전체 결과를 receipt에 저장한다.
5. 결과의 논리적 byte 합은 최대 32 MiB다. 아직 쓰지 않은 receipt slot마다 512 byte를 남긴다. 이 공간은 작은 capacity conflict 결과를 기억하기 위한 공간이다. 가변 길이 결과는 mutation 전에 예약한다. 실제 결과가 예약보다 크면 driver를 fail-closed한다. 성공 응답으로 바꾸지 않는다.
6. PUBLISH와 RELEASE 결과는 대상 client ID와 enqueue sequence만 저장한다. Payload를 복제하는 결과는 ATTACH의 delivery session, offline CLAIM 메시지, Will 결과다. 이 결과의 topic·payload·principal 길이를 예약한다.
7. Result에 membership operation이 있으면 원래 `MemberID`와 `Phase`도 저장한다. Snapshot에서 복원할 때 action으로 이 값을 추정하지 않는다.

## 판단

Count 거절마다 receipt를 추가하는 방식은 count 한도를 지킬 수 없다. 작은 capacity fence는 개별 결과 대신 거절 경계를 보존한다. 용량 부족 때 성공 여부를 바꾸지 않는 것이 우선이다. 사용자에게 노출되는 정상 ACK와 원래 5초 deadline은 바꾸지 않는다.
