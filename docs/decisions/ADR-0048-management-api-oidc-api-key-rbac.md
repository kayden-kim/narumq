# ADR-0048: Management API OIDC, API key와 4역할 RBAC

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (SEC-10 적용 범위) |
| 결정일 | 2026-10-10 |
| 결정자 | 승인된 SEC-10 작업 계획 |
| 관련 요구사항 | [SEC-10, SEC-11](../requirements/PRD.md#94-인증과-보안), [API-05, API-08](../requirements/PRD.md#95-metric과-관리-api) |
| 관련 스펙·계약 | [Tenant와 security](../specs/tenancy-and-security.md), [Management API와 observability](../specs/management-api-and-observability.md), [Management API v1](../contracts/management-api-v1.md), [Audit event v1](../contracts/audit-event-v1.md), [Raft membership v2](../contracts/raft-membership-v2.md) |
| 관련 아키텍처 문서 | [Security boundaries](../architectures/security-boundaries.md), [Management HTTP module](../architectures/management-http-module.md), [Project structure](../architectures/project-structure.md) |
| 이 ADR이 대체하는 이전 ADR | ADR-0039(조회 API 금지 범위), ADR-0043(subject 목록) |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

1. Cluster Management API 인증은 이름 있는 API key와 OIDC JWT를 지원한다. 설정된 기존 cluster admin bearer token은 ClusterAdmin 권한의 break-glass credential로 유지한다. API key의 principal subject는 `apikey:<id>`, OIDC subject는 `oidc:<sub>`이며, bearer break-glass token은 `management-bearer-token`으로 기록한다. API key에는 설정한 역할을 부여하고 OIDC 권한은 토큰 claim에서 선택하지 않는다. 검증된 OIDC subject를 설정의 정적 `subject → roles` binding에 연결한다. 이는 MQTT TEN-01의 정적 identity binding 원칙을 따른다.
2. 네 역할에 순서 비교가 아닌 명시적 권한 표를 적용한다. `PermissionRead`는 ReadOnly, Operator, SecurityAdmin, ClusterAdmin에 허용한다. `PermissionOperate`는 Operator와 ClusterAdmin, `PermissionAuditRead`는 SecurityAdmin과 ClusterAdmin, `PermissionClusterAdmin`은 ClusterAdmin에만 허용한다. 한 principal에 여러 역할이 있으면 그중 하나라도 권한을 부여할 때 허용한다.
3. SecurityAdmin과 ClusterAdmin 전용 `GET /api/v1/nodes/{nodeId}/audit/events`를 제공한다. 요청을 처리하는 node의 저장된 audit hash-chain record를 최신 순서로 읽으며, 필터·cursor 기반 pagination을 지원한다. 읽기 전용이며 조회 성공을 감사 event로 다시 기록하지 않는다. 2026-10-11에 경로를 node-scoped route로 이동했다. 이전 `/api/v1/audit/events` 경로는 지원하지 않는다. ADR-0039의 조회 API를 추가하지 않는 결정은 이 node-local endpoint에 한해 대체한다. 보존과 SIEM 수집 책임은 ADR-0039대로 외부 collector/SIEM에 둔다.
4. Dynamic cluster에서 ClusterAdmin 전용 `GET /api/v1/nodes/{nodeId}/config`를 제공한다. Node-local route는 요청을 처리하는 node ID만 받으며 원격 node로 proxy하지 않는다. 응답에서 secret 원문·경로, password hash와 identity 목록을 제외한다.
5. Dashboard는 ReadOnly API key만 사용한다. Dashboard 사용자의 로그인과 인증은 Grafana가 맡고 mutation 화면을 제공하지 않는다. Grafana 사용자 인증 자체만으로 SEC-10을 충족하는 것은 아니다. [ADR-0045](ADR-0045-operations-dashboard-grafana.md)의 기존 Dashboard 결정은 이 인증 경계를 유지한다.
6. ADR-0043 §1의 principal subject 목록은 위의 break-glass, API key 및 OIDC subject 규칙으로 대체한다. Rate limit은 인증된 principal subject를 key로 계속 사용한다.

## 2. 배경

SEC-10은 Management API와 Dashboard에 인증과 역할 기반 접근 제어를 요구한다. 기존 관리 plane 인증은 공유 cluster admin token과 ReadOnly API key에 한정되며, 역할 간 권한 판단은 순서 비교에 의존한다. 이 구조로는 API key를 개별 주체에 귀속하거나 Operator와 SecurityAdmin의 책임을 분리할 수 없다. 또한 보안 관리자가 node-local 감사 기록을 확인할 조회 경로가 없다.

기존 결정은 서로 다른 범위를 다룬다. ADR-0039는 관리 감사 저장·수집 및 조회 API 부재를 정했고, ADR-0043은 rate-limit principal subject를 정했다. 이 ADR은 그중 조회 API 금지와 subject 목록만 명시한 범위에서 대체하며, 나머지 결정과 감사 retention/SIEM 책임은 유지한다.

## 3. 결정 요인

1. SEC-10의 네 관리 역할을 권한별로 명확하게 분리해야 한다.
2. 감사 actor를 개별 API key 또는 OIDC subject에 연결하고 OIDC claim에 의한 임의 권한 부여를 막아야 한다.
3. 기존 break-glass 운영 경로, MQTT TEN-01의 정적 binding 원칙, node-local 감사 저장 경계를 유지해야 한다.
4. Dashboard 접근을 조회 전용으로 제한하면서 Grafana의 사용자 인증과 API 인증 책임을 구분해야 한다.

## 4. 선택 근거

정적 subject binding은 인증된 OIDC identity와 운영자가 승인한 관리 역할을 분리하므로 JWT claim이 역할을 임의 선택하지 못한다. 이름 있는 key는 공유 ReadOnly key를 역할별·주체별로 구성할 수 있고, subject 규칙은 audit actor와 ADR-0043 rate-limit key를 일관되게 만든다. 명시적 permission matrix는 역할 순서에 암묵적으로 기대지 않고 각 route의 요구 권한을 드러낸다.

감사 조회를 node-local read-only endpoint로 한정하면 기존 hash-chain 저장 방식과 외부 SIEM 책임을 유지하면서 SecurityAdmin이 해당 node에 보존된 record를 확인할 수 있다. Dashboard에는 ReadOnly key만 연결하고 사용자 인증은 Grafana에 맡겨, Dashboard 경로가 mutation 권한을 획득하지 않도록 한다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| OIDC group claim으로 역할 매핑 | IdP의 group 관리와 역할 할당을 한 경로로 운영할 수 있다. | 토큰 claim에 권한 선택을 맡겨 정적 subject binding 원칙과 달라지고 claim 형식·동기화에 의존한다. | MQTT TEN-01과 동일하게 권한은 설정된 subject binding에서만 부여한다. |
| API key만 사용하고 OIDC를 도입하지 않음 | 구현·운영 구성이 단순하다. | 사용자 identity 연동이 없고 key 배포·회수에만 의존한다. | SEC-10의 OIDC 인증 요구와 승인된 설계를 충족하지 않는다. |
| 기존 순서 비교 방식의 역할 판단 유지 | 기존 코드 변경이 작다. | 역할 순서가 permission을 암묵적으로 나타내며 SecurityAdmin과 Operator 권한을 독립적으로 표현할 수 없다. | 역할별 권한을 명시하는 고정 permission matrix가 더 정확하고 검토 가능하다. |

## 6. 결과

### 긍정적 결과

- API key와 OIDC 주체가 별도 subject로 감사 및 rate-limit 기록에 연결된다.
- OIDC 권한은 정적 binding으로 제한되고, 네 역할의 route 권한이 permission 단위로 분명해진다.
- SecurityAdmin은 해당 node의 감사 hash-chain 기록을 조회할 수 있으며 저장·보존·SIEM 경계는 바뀌지 않는다.
- Dashboard는 ReadOnly API key만 사용하고 Grafana 사용자 인증을 계속 사용한다.

### 부정적 결과와 비용

- 운영자는 API key별 역할과 OIDC subject별 역할 binding을 구성·관리해야 한다.
- 감사 조회는 node-local이므로 다른 node의 기록은 해당 node에서 조회해야 한다.
- 기존 `readOnlyApiKeyFile` 설정과 기존 principal subject 이름은 호환 alias 없이 새 설정·subject 규칙으로 전환해야 한다.

### 후속 작업

- Management API 설정, API 계약, 보안 스펙과 아키텍처 설명을 이 결정과 일치시킨다.
- 설정·계약·3-node process acceptance 검증 결과를 추적 문서에 기록한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Cluster 설정이 API key와 OIDC verifier/identity binding을 정확히 검증·적재한다. | `go test ./cmd/narumq -count=1`의 `TestClusterManagementAccessConfiguration`, `TestLoadClusterManagementCredentials` 등 설정 시험 | 설정 오류, credential 로드와 OIDC verifier 구성 시험 통과 | `cmd/narumq/cluster_config_test.go` |
| 계약에 audit query의 응답·오류와 저장된 hash-chain record가 반영된다. | `go test ./tests/contracts/... -count=1`의 `TestAuditQueryHandlerResponsesMatchOpenAPI`, `TestAuditQueryReturnsStoredChainRecords` | OpenAPI와 일치하는 query 응답, invalid result 400, 저장 record chain 검증 통과 | `tests/contracts/management-api-v1`, `tests/contracts/audit-event-v1` |
| 네 역할, OIDC binding과 node-local audit query가 실제 cluster에서 동작한다. | `go test ./tests/integration -run '^TestDynamicClusterManagementRBACProcessAcceptance$' -count=1 -v -timeout 20m` | 실제 3-node process에서 허용·거절, audit filter/cursor와 actor roles 검증 통과 | `tests/integration/cluster_management_rbac_process_test.go` |

## 8. 재검토 조건

- 운영상 정적 OIDC subject binding으로 관리 identity를 안전하게 운용할 수 없거나 별도 승인된 group mapping 요구가 생기는 경우.
- 감사 조회를 node-local로 제공하는 대신 cluster-wide 집계가 요구되는 경우.
- 네 역할 permission matrix 또는 Dashboard의 ReadOnly API key 경계가 제품 요구와 맞지 않는 경우.
