# ADR-0029: Replication factor와 write quorum은 3/2 profile만 지원한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (2026-10-04 사용자 결정) |
| 결정일 | 2026-10-04 |
| 결정자 | NaruMQ 프로젝트 사용자 |
| 관련 요구사항 | [PRD CLU-09](../requirements/PRD.md) |
| 관련 스펙·데이터 계약 | [Cluster §4.4·§11.27](../specs/cluster-session-and-delivery.md), [Raft membership v2](../contracts/raft-membership-v2.md), [Management API v1](../contracts/management-api-v1.md) |
| 관련 아키텍처 | [Project structure](../architectures/project-structure.md), [Cluster architecture](../architectures/cluster-architecture.md) |
| 관련 결정 | [ADR-0003 §5](ADR-0003-durable-ack-barrier.md#5-profile과-설정) (`Proposed` 유지), [ADR-0010](ADR-0010-dynamic-cluster-membership.md), [ADR-0009 §1.4](ADR-0009-raft-production-state-profile.md#14-consensus-group-partition-profile) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

1. Cluster config의 선택 객체 `replication`으로 복제 profile을 선언한다. 객체가 있으면 정수 key `factor`와 `writeQuorum`이 모두 필요하다. 생략하면 3/2다. 3/2 외의 값은 startup에서 거절한다.
2. 지원 profile은 3/2 하나다. 다른 값은 정확히 3 voter를 요구하는 bootstrap, incumbent 3 voter를 요구하는 seed admission, 3 voter restore의 확장과 함께 별도 결정으로 다룬다. 그 결정에는 cluster-wide committed profile과 N-1 capacity·latency gate가 필요하다. ADR-0003 §5의 설정 변경 quorum commit 조건은 이번 범위에 적용되지 않는다. 설정으로 선택할 값이 하나뿐이기 때문이다.
3. Replication factor는 유지할 최소 voter 수다. Write quorum은 durable write를 성공 처리하는 데 필요한 최소 영속 replica 수다. Bootstrap은 3 voter이고 removal guard는 voter를 3 미만으로 줄이지 않는다. 따라서 Raft 과반수는 항상 2 이상이다. Durable write는 기존 Raft 과반수 commit 뒤에만 성공한다. Raft 과반수보다 약한 ACK는 도입하지 않는다.
4. `membership.Service.StartPlannedRemoval`은 commit 전에 대상 외의 committed `VOTER` 수를 검사한다. 부족하면 non-retryable `ServiceErrorReplicationFactorUnsatisfied`를 반환하고 state를 바꾸지 않는다. HTTP adapter의 `writeMembershipServiceError`가 이를 `409 replication_factor_unsatisfied`로 변환한다. Leader의 `clusterRuntime.advanceMembership`도 `RemoveMember` ConfChange 제안 전에 현재 Raft voter 집합으로 재확인한다. 부족하면 session transfer와 ConfChange를 제안하지 않고 operation을 `REMOVING`에 둔다. `PlanRemovalHandoff`는 drain도 사용하므로 바꾸지 않는다. Replicated schema·wire·apply 규칙도 바꾸지 않는다. Apply 조건을 바꾸면 N-1과 N이 같은 committed BeginRemove를 다르게 적용할 수 있기 때문이다.
5. Management API 1.6.0의 `GET /api/v1/cluster`는 `replicationFactor`와 `writeQuorum`을 반환한다. Dynamic cluster는 3/2, 복제하지 않는 standalone은 1/1이다. 두 field는 계약의 optional 추가 항목이므로 N-1 응답도 허용한다.
6. 죽은 voter를 교체할 때는 새 voter를 먼저 추가하여 4 voter로 만든다. 그 뒤 이전 voter를 제거하여 3 voter로 돌아간다.
7. Mixed N-1/N에서 N-1 leader 또는 N-1 management node가 받은 removal은 보호되지 않는다. Service 확인과 commit 사이에 다른 removal이 끝나는 경합은 leader 재확인이 막는다. 다만 해당 operation은 `REMOVING`에 남아 이후 membership 변경을 `membership_change_in_progress`로 막는다. 자동 해제 절차는 없다.

## 2. 배경

CLU-09는 설정 가능한 replication factor와 write quorum을 요구한다. 기존 config에는 이 값이 없고 planned removal은 3 voter를 2 또는 1로 줄일 수 있다. 최소 voter 수를 지키면서 기존 durable ACK와 N-1 apply 호환성을 유지해야 한다.

## 3. 결정 요인

1. 설정한 write quorum보다 적은 replica로 durable ACK를 반환하지 않는다.
2. 기존 Raft commit·schema·apply 경계를 유지한다.
3. 검증하지 않은 복제 profile을 허용하지 않는다.
4. 정상 removal과 경합 뒤 removal 모두 최소 voter 수를 검사한다.

## 4. 선택 근거

3/2는 ADR-0003의 기준값이며 현재 bootstrap·seed admission·restore의 3-voter 규칙과 맞는다. Proposer에 보호를 두면 committed apply 의미를 바꾸지 않고 정상 3→2 removal을 거절할 수 있다. Raft voter 집합을 leader에서 다시 검사하면 사전 확인 이후의 경합도 막는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 임의 factor·quorum 허용 | 다양한 배포 지원 | Bootstrap·seed·restore와 committed profile 확장이 필요함 | 이번 승인 범위는 3/2 하나임 |
| Apply에서 BeginRemove 거절 | 모든 제안 경로 보호 | Mixed N-1/N에서 committed state가 갈라질 수 있음 | 기존 apply 의미를 보존해야 함 |
| Handoff planner에서 보호 | 검사 위치 감소 | 3-voter drain도 막힘 | Drain은 voter 제거가 아님 |

## 6. 결과

### 긍정적 결과

- Config와 API에서 복제 보장을 확인할 수 있다.
- 정상 removal은 최소 3 voter를 유지한다.
- 4→3 교체와 기존 quorum ACK 경계를 유지한다.

### 부정적 결과와 비용

- 지원 profile은 3/2로 제한된다.
- Mixed N-1/N removal 보호와 경합 시 `REMOVING` 대기는 §1 항목 7의 한계로 남는다.

### 후속 작업

- 다른 profile이나 경합 operation 해제가 필요하면 별도 결정과 수용 시험을 추가한다.
- Production Linux/EBS storage gate는 기존 미완료 범위로 유지한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Config·removal·API·ACK 경계 | [Cluster §11.27](../specs/cluster-session-and-delivery.md#1127-replication-factorwrite-quorum-profile-수용-매핑)의 unit·in-process·process·contract·local gate | 모든 매핑 PASS | 2026-10-04 모두 PASS; 명령과 결과는 §11.27 참조. 최초 local gate wall 305.38s; leader 재검사 회귀 보강 뒤 최종 local gate exit 0·wall 288.05s |

## 8. 재검토 조건

- 3/2 외의 복제 profile이 필요할 때 재검토한다.
- Mixed release removal 보호 또는 `REMOVING` operation 해제가 필요할 때 재검토한다.
