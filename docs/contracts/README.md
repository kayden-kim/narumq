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
| [Raft membership v2](raft-membership-v2.md) | Draft | ClusterAdmin · joining broker ↔ Cluster leader · authenticated peer transport | 이 문서와 [replicated-state-v1.proto](replicated-state-v1.proto) | CLU-01~CLU-02, CLU-04~CLU-11, SEC-09~SEC-11, NFR-07~NFR-10 |
| [Cluster backup v3](cluster-backup-v3.md) | Draft | ClusterAdmin backup producer → offline restore CLI | [Protobuf archive schema](cluster-backup-v3.proto) | CLU-14 |
| [Cluster backup v2](cluster-backup-v2.md) | Superseded | Historical producer → historical offline restore CLI | v2 source/generated binding은 제거됨 | CLU-14 |
| [Cluster backup v1](cluster-backup-v1.md) | Superseded | Historical producer → historical offline restore CLI | v1 source/generated binding은 제거됨 | CLU-14 |

`Active` contract는 기준 artifact, producer runtime과 자동 contract test가 같은 현재
동작을 확인한 계약이다. `Draft` contract는 구현이 존재할 수 있지만 경계 규칙,
compatibility matrix 또는 contract test mapping의 검토·승인이 아직 끝나지 않은 상태다.
Producer·consumer contract test와 compatibility matrix를 통과한 뒤에만 `Active`로
전환한다.

`Superseded` contract는 역사적 형식이나 결정을 보존하지만 현재 producer·consumer가
새 데이터를 만들거나 읽는 데 사용하지 않는다. Migration 대상과 종료 조건은 대체
contract에 명시한다.

## 아직 만들지 않는 Contract

| 후보 | 대기 이유 |
| --- | --- |
| Metrics catalog v1 | `SPEC-TBD-07`의 이름과 histogram bucket을 Phase 0 workload로 확정해야 한다. |
| Rollout custom resource v1 | [ADR-0006](../decisions/ADR-0006-external-rollout-controller.md) 승인과 controller resource model이 필요하다. |
| Replicated state encoding v1 | Draft schema [`replicated-state-v1.proto`](replicated-state-v1.proto)와 generated codec은 구현했지만, 공식 크기 제한·마이그레이션 규칙·릴리스 호환성 벡터를 포함한 검토된 contract는 아직 없다. |

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
