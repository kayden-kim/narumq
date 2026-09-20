# NaruMQ Architecture Decision Records

이 디렉터리는 NaruMQ의 중요한 기술·구조 선택과 근거를 기록한다. 제품 요구는 [PRD](../requirements/PRD.md), 외부 동작은 [시스템 스펙](../specs/README.md), 경계 형식은 [Contract](../contracts/README.md)를 기준으로 한다. 결정이 만든 목표 구조는 [Architecture Views](../architectures/README.md)에서 확인한다.

## 결정 목록

| ADR | 상태 | 결정 |
| --- | --- | --- |
| [ADR-0001](ADR-0001-core-implementation-language.md) | Accepted | Broker core를 Go로 구현한다. |
| [ADR-0002](ADR-0002-cluster-consensus-library.md) | Proposed | Cluster consensus core로 `etcd/raft`를 사용한다. |
| [ADR-0003](ADR-0003-durable-ack-barrier.md) | Proposed | 기준 3-node profile에서 quorum WAL 영속화와 commit 뒤 durable ACK한다. |
| [ADR-0004](ADR-0004-session-ownership-and-fencing.md) | Proposed | Session ownership을 quorum-committed epoch와 저장 경계 fencing으로 보장한다. |
| [ADR-0005](ADR-0005-management-api-contract-format.md) | Accepted | Phase 1 query profile의 기준 계약을 OpenAPI 3.1.2 JSON으로 관리한다. |
| [ADR-0006](ADR-0006-external-rollout-controller.md) | Proposed | Pod 삭제 전 drain을 승인하는 전용 rollout controller를 둔다. |
| [ADR-0007](ADR-0007-aws-production-deployment-platform.md) | Proposed | 초기 AWS production profile로 Amazon EKS on EC2를 사용한다. |

`Proposed` ADR은 검토 중인 제안이다. `Accepted` ADR은 연결된 기준 문서와 구현의
현재 범위 안에서 채택된 결정이다.

## 번호와 변경

ADR 번호는 생성 순서대로 증가한다. Accepted 결정의 결론을 바꿀 때는 기존 파일을 고치지 않고 새 ADR로 supersede한다.
