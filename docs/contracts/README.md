# NaruMQ Contract Catalog

이 디렉터리는 NaruMQ와 client, 운영 도구, cluster node 사이의 경계 형식과 호환성 약속을 관리한다. 제품 요구는 [PRD](../requirements/PRD.md), 외부 동작은 [시스템 스펙](../specs/README.md), 구조 view는 [Architecture](../architectures/README.md), 기술 선택은 [ADR](../decisions/README.md)을 기준으로 한다.

## Contract 목록

| Contract | 상태 | Producer → Consumer | 기준 artifact | 관련 요구사항 |
| --- | --- | --- | --- | --- |
| [Management API v1](management-api-v1.md) | Active | Broker → Dashboard·operator·monitoring query client | [OpenAPI JSON](management-api-v1.openapi.json) | API-02, API-04~API-09, SEC-10~SEC-11 |
| [Audit event v1](audit-event-v1.md) | Draft | Broker·controller → audit store·security operator | [JSON Schema](audit-event-v1.schema.json) | SEC-11, NFR-16~NFR-17 |
| [MQTT client compatibility v1](mqtt-client-compatibility-v1.md) | Draft | Broker ↔ MQTT client | 이 문서의 profile·matrix | MQTT-01~MQTT-13, CLIENT-01~CLIENT-09, DEP-06~DEP-12 |
| [Cluster release compatibility v1](cluster-release-compatibility-v1.md) | Draft | Release N node ↔ N-1 node | 이 문서의 compatibility matrix | CLU-11, NFR-15, DEP-18 |
| [Raft peer transport v1](raft-peer-transport-v1.md) | Active | NaruMQ Raft node ↔ NaruMQ Raft node | 이 문서의 hello·frame·queue·retry 계약 | CLU-01~CLU-02, CLU-09~CLU-10, SEC-09, NFR-07~NFR-10 |

`Active` contract는 기준 artifact, producer runtime과 자동 contract test가 같은 현재
동작을 확인한 계약이다. `Draft` contract는 구현과 검증 전 계약 후보다.
Producer·consumer contract test와 compatibility matrix를 통과한 뒤에만 `Active`로
전환한다.

## 아직 만들지 않는 Contract

| 후보 | 대기 이유 |
| --- | --- |
| Metrics catalog v1 | `SPEC-TBD-07`의 이름과 histogram bucket을 Phase 0 workload로 확정해야 한다. |
| Rollout custom resource v1 | [ADR-0006](../decisions/ADR-0006-external-rollout-controller.md) 승인과 controller resource model이 필요하다. |
| Replicated state encoding v1 | [ADR-0002](../decisions/ADR-0002-cluster-consensus-library.md)와 storage/RPC 후속 ADR 승인이 필요하다. |

SDK별 client version과 실행 profile은 독립 contract를 만들지 않고
[MQTT client compatibility v1](mqtt-client-compatibility-v1.md)의 checked-in
Phase 1 profile과 결과 schema에서 관리한다. Drain inventory와 migration evidence가
해결되기 전에는 해당 contract를 `Active`로 전환하지 않는다.

## Version 정책

- Contract 이름의 `v1`은 호환성 major를 뜻한다.
- 문서 metadata의 `1.x.y`는 contract revision을 뜻한다.
- 같은 major에서 backward-compatible 변경만 허용한다.
- Breaking change는 새 major contract와 migration 계획을 만든다.
- Deprecated contract는 Active consumer가 0이고 종료 조건을 검증하기 전까지 삭제하지 않는다.
