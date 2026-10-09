# ADR-0043: Management API rate limit

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-09 |
| 결정자 | 사용자 |
| 관련 요구사항 | [API-05](../requirements/PRD.md#95-metric과-관리-api), [SEC-11](../requirements/PRD.md#94-인증과-보안) |
| 관련 스펙·계약 | [Management API와 observability](../specs/management-api-and-observability.md), [Management API v1](../contracts/management-api-v1.md), [Audit event v1](../contracts/audit-event-v1.md), [Raft membership v2](../contracts/raft-membership-v2.md) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md), [Broker core](../architectures/broker-core.md) |
| 이 ADR이 대체하는 이전 ADR | 없음. ADR-0005가 후속 범위로 남긴 API rate limiting을 이 ADR이 결정한다. |
| 이 ADR을 대체한 새 ADR | [ADR-0048](ADR-0048-management-api-oidc-api-key-rbac.md) (§1의 principal subject 목록만 대체) |

## 1. 결정

1. 각 broker process는 `/api/v1/**` 요청에 node-local, process-memory token bucket을 적용한다. Cluster 전체 합산 한도는 두지 않는다. 각 node의 Management listener를 replicated state나 peer RPC 없이 보호한다.
2. 인증에 성공한 요청은 `managementPrincipal.subject`를 key로 하는 principal bucket을 사용한다. 기본 rate와 burst는 초당 50건이다. Subject는 `management-bearer-token`, `management-api-key`, 또는 익명 local mode의 `local-unauthenticated`다.
3. 인증에 실패한 요청은 `RemoteAddr`에서 추출한 host를 key로 하는 auth-failure bucket을 사용한다. 기본 rate와 burst는 초당 5건이다. `X-Forwarded-For`는 신뢰하지 않는다.
4. Bucket capacity는 rate와 같고 새 bucket은 가득 찬 상태로 시작한다. 설정값은 초당 rate와 burst에 함께 적용하며 허용 범위는 1~1,000,000이다. Standalone flag는 `-http-request-rate`와 `-http-auth-failure-rate`, cluster configuration은 `management.requestRate`와 `management.authFailureRate`다. 생략한 cluster 값은 각각 50과 5다.
5. Limiter마다 key를 최대 4096개 보관한다. 새 key가 상한에 도달하면 모든 bucket을 먼저 refill하고 가득 찬 bucket을 제거한다. 그래도 상한이면 새 key 요청을 거절한다. 기존 key는 계속 처리한다.
6. 대상은 등록된 `/api/v1/**` route다. `/health/*`, `/metrics`, 등록되지 않은 path의 404 handler는 제한하지 않는다.
7. 처리 순서는 인증, 인증 실패 IP bucket 또는 인증 성공 principal bucket, role 검사, mutation audit admission이다. 따라서 `403` 요청도 principal token을 사용한다. `429` mutation은 handler를 실행하지 않으며 `AdmitMutation`도 호출하지 않는다.
8. 한도를 넘긴 요청은 `429 rate_limited`, message `management API rate limit exceeded`, `retryable=true`, `Retry-After: 1`을 반환한다. Rate가 1 이상이므로 token 하나가 생기는 데 최대 1초가 걸린다.
9. `429`는 `management.request` 감사 event로 기록한다. Target은 `management_endpoint`와 `r.Pattern`, result는 `DENIED`, reason은 `rate_limited`다. 인증 실패 IP 거절의 subject는 `unauthenticated`이며 roles를 생략한다. 성공 인증 거절은 인증 subject와 현재 role을 기록한다. 기존 node별 denial event 상한을 따른다.

## 2. 배경

Management API는 인증, RBAC, pagination과 filtering을 제공한다. PRD API-05가 요구하는 rate limit은 아직 구현되지 않았다. ADR-0005는 API rate limiting을 후속 범위로 남겼으며, 이 ADR이 이를 확정한다.

각 Management API listener는 node-local 자원이다. 요청 한도를 replicated state로 저장하거나 peer에 문의하면 보호 목적에 비해 복제와 RPC 비용이 생긴다. 인증 실패를 IP로 제한하고 인증 성공을 principal로 제한하면 공유 NAT 뒤 운영자 간섭을 줄이면서 credential별 부하를 나눌 수 있다.

## 3. 결정 요인

1. 각 node의 Management listener를 원격 의존성 없이 보호해야 한다.
2. 인증 주체 사이의 자원 경쟁을 제한하고 인증 실패 폭주를 억제해야 한다.
3. readiness probe와 metric scrape가 rate limit 영향을 받으면 안 된다.
4. 거절된 요청은 기존 감사 정책과 error contract에 연결해야 한다.
5. 임의 원격 주소 header나 무제한 bucket key로 제한 우회를 만들면 안 된다.

## 4. 선택 근거

Node-local bucket은 요청을 받은 process에서 인증 principal과 remote address를 이미 알고 있을 때 바로 판정한다. Cluster-wide 한도는 replicated state나 peer RPC를 요구하지만 node별 Management listener 보호에는 필요하지 않다.

Principal key는 동일 인증 주체의 요청만 함께 제한한다. 인증 실패는 신뢰할 수 있는 socket remote address의 host로 제한한다. `X-Forwarded-For`는 proxy 신뢰 계약이 없으므로 사용하지 않는다. Probe와 scrape는 요청 한도에서 제외해 health와 관측 경로를 보존한다.

가득 찬 bucket만 정리하면 만료되지 않은 token을 보유한 주체는 eviction으로 추가 burst를 얻지 못한다. 4096 key 상한 뒤에도 새 key를 거절하면 원격 주소나 subject가 계속 달라져도 map이 무한히 커지지 않는다.

기존 감사 service는 denial event를 node별 초당 20개로 제한한다. Rate-limit 거절을 기존 `management.request` 감사 경로에 보내되 별도 unbounded event queue를 만들지 않는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| IP 하나로 모든 요청을 제한 | 구현이 단순하다. | 공유 NAT 뒤의 운영자가 서로 한도를 소모한다. 인증 주체 간 분리가 없다. | 인증 성공 요청은 principal별로 제한한다. |
| Node 전체 단일 bucket | 상태 key 수가 없다. | 인증 실패 폭주가 정상 운영자 요청까지 제한한다. | 인증 실패와 성공을 별도 bucket으로 분리한다. |
| Cluster-wide 합산 한도 | 전체 cluster의 요청 rate를 하나로 제한한다. | Replicated state 또는 peer RPC가 필요하고 node-local listener 보호에 추가 비용을 만든다. | Node-local 자원 보호가 목적이므로 각 process에서 판정한다. |

## 6. 결과

### 긍정적 결과

- 인증 주체와 인증 실패 IP별로 burst와 지속 요청량을 제한한다.
- Probe와 `/metrics`는 management API 요청 폭주와 분리된다.
- 제한된 key map과 full-bucket eviction으로 key churn의 메모리 사용을 제한한다.
- 한도 초과는 공통 HTTP 오류와 감사 event로 관찰할 수 있다.

### 부정적 결과와 비용

- 여러 node에 요청을 분산하면 cluster 전체 허용량은 node 수에 따라 늘어난다.
- 같은 NAT 뒤의 인증 실패 요청은 하나의 IP bucket을 공유한다.
- Key 상한 뒤에 가득 찬 bucket이 없어도 새 principal 또는 IP는 거절된다.
- In-memory bucket은 process 재시작 때 초기화된다.

### 후속 작업

- API-05의 구현, contract와 process 검증을 Management API 스펙 수용 시험에 연결한다.
- Rate limit 설정을 standalone flag와 combined cluster configuration에 제공한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Principal·IP bucket과 refill 동작이 결정 규칙을 따른다. | `internal/adapters/http/management/rate_limit_test.go` | 분리된 key, burst, refill, clock 역행과 key 상한을 결정적 clock으로 검증한다. | 구현 검증 뒤 Management API 스펙 §9에 기록한다. |
| HTTP response와 모든 API operation의 429가 contract와 일치한다. | `tests/contracts/management-api-v1/contract_test.go::TestManagementRateLimitMatchesOpenAPI` | Principal 및 인증 실패 한도가 `429 rate_limited`와 `Retry-After: 1`을 반환하고 OpenAPI schema를 통과한다. | 구현 검증 뒤 Management API 스펙 §9에 기록한다. |
| Standalone·cluster 설정 경계와 감사 경로가 동작한다. | `cmd/narumq` 설정 시험 및 `tests/integration/cluster_audit_process_test.go::TestDynamicClusterManagementAuditProcessAcceptance` | 범위 밖 값은 거절되고 한도 초과 요청은 감사되며 mutation을 실행하지 않는다. | 구현 검증 뒤 Management API 스펙 §9에 기록한다. |

## 8. 재검토 조건

- 여러 node에 걸친 단일 global request budget이 요구사항이 되는 경우
- reverse proxy의 trusted forwarding contract가 채택되어 IP identity를 변경해야 하는 경우
- Bucket key 상한 또는 기본 rate가 실제 운영에서 listener 보호와 관리 작업을 동시에 충족하지 못하는 경우
