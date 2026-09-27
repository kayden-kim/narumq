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
| [ADR-0008](ADR-0008-raft-peer-transport.md) | Accepted | 고정 3-node Raft transport v1에 mTLS TCP와 bounded queues를 사용한다. |
| [ADR-0009](ADR-0009-raft-production-state-profile.md) | Accepted (design target only) | Raft version, file-backed storage, replicated-state encoding family와 단일 group target을 정하되 production readiness는 승인하지 않는다. |
| [ADR-0010](ADR-0010-dynamic-cluster-membership.md) | Accepted (membership 설계만) | Cluster membership은 staged learner 전환과 영구 node ID를 사용한다. |
| [ADR-0011](ADR-0011-cluster-backup-restore-v1.md) | Superseded (v1 archive only) | Commit-boundary archive와 offline voter restore를 사용했다. |
| [ADR-0012](ADR-0012-retained-replication-and-cluster-backup-v2.md) | Accepted (retained scope; archive v2는 ADR-0013이 대체) | Retained state를 Raft로 복제하고 backup v2에 포함한다. |
| [ADR-0013](ADR-0013-offline-queue-replication-and-cluster-backup-v3.md) | Accepted (offline QoS 0 + backup v3 scope only) | Detached persistent session의 QoS 0 offline message를 Raft로 복제하고 backup v3에 포함한다. |
| [ADR-0014](ADR-0014-cross-node-live-qos0-forwarding.md) | Accepted (cluster live QoS 0 forwarding only; QoS 1/2·durable ACK 제외) | Dynamic cluster의 cross-node live QoS 0 delivery를 별도 `NMQFWRD2` peer stream으로 전달한다. |
| [ADR-0015](ADR-0015-cluster-durable-qos-delivery.md) | Accepted (cluster QoS 1/2 durable delivery scope; production storage·BP-03·N-1 제외) | Cluster QoS 1/2 전달 queue와 inflight를 Raft에 복제하고 ACK보다 먼저 commit한다. |
`Proposed` ADR은 검토 중인 제안이다. `Accepted` ADR은 각 기록에 명시한 범위 안에서 채택된 결정이다. `design target only`는 production runtime이나 acceptance 검증을 뜻하지 않는다.

## 번호와 변경

ADR 번호는 생성 순서대로 증가한다. Accepted 결정의 결론을 바꿀 때는 기존 파일을 고치지 않고 새 ADR로 supersede한다.
