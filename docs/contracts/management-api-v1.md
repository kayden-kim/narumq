# Management API v1 Contract

이 문서는 NaruMQ Management API v1의 query와 승인된 mutation profile을 정의한다. 정확한
path, parameter, response schema와 media type은
[OpenAPI JSON](management-api-v1.openapi.json)이 기준이다.

| 항목 | 값 |
| --- | --- |
| 이름과 version | `narumq-management-api` 1.9.0 |
| 상태 | Active |
| Producer | NaruMQ broker management API |
| Consumer | Dashboard·operator·monitoring integration의 query client |
| 방향 | HTTP request/response |
| 기준 artifact | [management-api-v1.openapi.json](management-api-v1.openapi.json) |
| 기준 spec | [Management API와 observability](../specs/management-api-and-observability.md) |
| 관련 Architecture | [System overview](../architectures/system-overview.md), [Security boundaries](../architectures/security-boundaries.md) |
| 관련 requirement | API-02, API-04~API-09, SEC-10~SEC-11 |
| 관련 결정 | [ADR-0005](../decisions/ADR-0005-management-api-contract-format.md), [ADR-0018](../decisions/ADR-0018-release-compatibility-window.md), [ADR-0043](../decisions/ADR-0043-management-api-rate-limit.md) |
| 호환성 범위 | `/api/v1` query producer와 v1 query consumer |
| 검증 | `tests/contracts/management-api-v1`와 `tests/integration/management_api_test.go` |

## 1. Phase 1 operation profile

현재 runtime router가 제공하는 operation은 다음과 같다.

| Method | Path | 용도 |
| --- | --- | --- |
| GET | `/health/live` | liveness probe |
| GET | `/health/ready` | readiness probe |
| GET | `/health/startup` | startup probe |
| GET | `/health/drain` | drain-complete probe |
| GET | `/metrics` | Prometheus/OpenMetrics scrape |
| GET | `/api/v1/cluster` | cluster summary |
| POST | `/api/v1/cluster/backups` | quorum-fresh 경계의 checksummed backup archive 다운로드 |
| GET | `/api/v1/cluster/features` | committed active feature와 member build profile 조회 |
| POST | `/api/v1/cluster/features/{feature}` | 모든 non-retired member가 지원하는 feature 활성화 |
| GET | `/api/v1/nodes` | node list |
| GET | `/api/v1/nodes/{nodeId}` | node detail |
| POST | `/api/v1/nodes/{nodeId}/drain` | broker drain 시작 또는 기존 operation 재생 |
| GET | `/api/v1/nodes/{nodeId}/drain` | committed drain operation 조회 |
| DELETE | `/api/v1/nodes/{nodeId}/drain` | 취소 가능한 drain operation 취소 |
| POST | `/api/v1/cluster/rebalance` | cluster connection rebalance 시작 또는 replay |
| GET | `/api/v1/cluster/rebalance` | committed cluster connection rebalance 조회 |
| DELETE | `/api/v1/cluster/rebalance` | 실행 중인 rebalance 취소 |
| GET | `/api/v1/clients/{tenantId}/{clientId}` | client session detail |
| GET | `/api/v1/subscriptions` | subscription list |
| GET | `/api/v1/retained` | retained metadata list |
| GET | `/api/v1/tenants` | tenant list |

Version 1.5.0은 cluster feature 조회와 activation을 active contract에 추가한다. 조회는 `ReadOnly`, activation은 `ClusterAdmin`을 요구하며 dynamic cluster mode에서 제공된다. 이 catalog에 없는 path를 client가 호출할 수 있다는 계약은 없다.

Version 1.6.0은 `GET /api/v1/cluster` 응답에 optional `replicationFactor`, `writeQuorum`을 추가한다. Dynamic cluster는 3/2, standalone은 1/1을 반환한다.
Version 1.7.0은 `POST/GET/DELETE /api/v1/cluster/rebalance`를 추가한다. Operation은 Operator가 시작·취소하고 ReadOnly가 조회하며, 연결 수 보고와 수렴은 committed cluster operation으로 조정된다. Request의 `maxDeviationConnections`, `connectionRatePerSecond`, `loadBalancerPropagationDelayMs`, `timeoutMs`는 필수다.


## 2. Authentication과 authorization

- API resource(`/api/v1/**`)는 configured opaque `Authorization: Bearer <token>` 또는
  `X-API-Key` API key를 사용한다.
- Cluster mode에서 `management.readOnlyApiKeyFile`을 지정하면 파일의 앞뒤 공백을
  제거한 값이 `X-API-Key` credential이며 `ReadOnly` role을 가진다. 파일 형식과
  시작 시 검증은 [Raft membership v2 §1.4](raft-membership-v2.md#14-통합-클러스터-구성)를 따른다.
- 이 contract는 OIDC issuer discovery, JWT signature, claim semantics, token scope 또는
  API key 발급 형식을 약속하지 않는다. 입력 credential은 configured opaque value와
  비교된다.
- API query operation과 node drain GET, cluster feature GET은 `ReadOnly`; node drain POST/DELETE는 `Operator`를 요구한다. Tenant list, cluster backup과 cluster feature POST는 `ClusterAdmin` role을 요구한다.
- Health probe와 `/metrics`의 익명 허용 여부는 listener configuration이다. OpenAPI의
  security scheme은 보호된 배포를 기술하고, local development의 anonymous policy를
  제거하지 않는다.
- 모든 HTTP response는 `X-Request-Id`를 반환한다. 요청 header가 유효하면 같은 값을
  사용하고, 아니면 서버가 opaque 값을 생성한다.

- `/api/v1/**`는 node-local rate limit을 적용한다. 한도를 넘으면 retryable `429 rate_limited`와 `Retry-After: 1`을 반환한다. 세부 규칙은 [Management API spec §1.5](../specs/management-api-and-observability.md#15-rate-limit)를 따른다.

## 3. Query와 pagination

목록 endpoint의 공통 query parameter는 다음과 같다.

- `limit`: 양의 정수, default `50`, maximum `1000`.
- `cursor`: opaque pagination cursor. client는 내부 형식을 해석하지 않는다.
- 선언되지 않은 query key, 같은 key의 중복 값과 빈 값은 `400 invalid_request`로 거절한다.
- `limit`이 양의 정수가 아니거나 maximum `1000`을 초과하면 `400 invalid_request`로 거절한다.
- 성공 목록은 `{"data": [...], "page": {"nextCursor": ..., "hasMore": ...}}`다.
  `nextCursor`는 다음 페이지가 없으면 JSON `null`이다.

Endpoint별 filter allowlist:

| Endpoint | 허용 filter |
| --- | --- |
| `/api/v1/nodes` | `state`, `limit`, `cursor` |
| `/api/v1/clients` | `tenantId`, `clientIdPrefix`, `nodeId`, `protocol`, `limit`, `cursor` |
| `/api/v1/subscriptions` | required `tenantId`, optional `clientId`, `limit`, `cursor` |
| `/api/v1/retained` | required `tenantId`, optional `topicPrefix`, `limit`, `cursor` |
| `/api/v1/tenants` | `limit`, `cursor` |

`subscriptions`와 `retained`는 빈 문자열을 포함해 `tenantId`가 없으면 `400
invalid_request`다. 다른 endpoint도 OpenAPI에 선언된 filter 외 값을 받지 않는다.

## 4. Response와 오류

단일 resource는 `{"data": {...}}`, 목록은 위의 list envelope를 사용한다. `/metrics`는
요청 `Accept`에 `application/openmetrics-text`가 포함되면
`application/openmetrics-text; version=1.0.0; charset=utf-8`을, 그 외에는
`text/plain; version=0.0.4; charset=utf-8`을 반환한다.

`POST /api/v1/cluster/backups`는 JSON envelope 대신 `application/octet-stream` archive를
반환한다. `X-NaruMQ-Backup-Commit-Index`는 archive의 commit index, `X-NaruMQ-Backup-SHA256`은
archive framing을 검증하는 SHA-256 trailer digest다. Fresh quorum barrier가 불가능하면
retryable `503 cluster_backup_unavailable`을 반환한다.

JSON 오류는 다음 envelope다.

```json
{
  "error": {
    "code": "invalid_request",
    "message": "invalid request",
    "retryable": false,
    "details": null
  },
  "requestId": "req_example"
}
```

`404`는 존재하지 않는 node/client와 알 수 없는 router path에, `405`는 선언되지 않은
method에 사용한다. 내부 오류와 credential은 response에 노출하지 않는다.

## 5. Compatibility와 deferred scope

- `/api/v1`의 required response field 삭제·rename·type 변경은 breaking change다.
- Optional response field 추가는 contract suite와 consumer compatibility 확인 뒤에만
  허용한다.
- Version 1.2.0은 `NodeData`에 optional `replicationHealth`, `lastErrorCode`, `replicationQueue` field를 추가했다. Field schema와 enum은 OpenAPI artifact가 기준이며, contract suite가 response shape를 검증한다.
- Version 1.3.0은 `POST /api/v1/cluster/backups`를 추가했다. ClusterAdmin 전용이며 fresh quorum barrier에서 캡처한 typed snapshot과 committed tail을 binary archive로 반환한다.
- Version 1.4.0은 `/api/v1/nodes/{nodeId}/drain`의 POST/GET/DELETE를 추가한다. Drain request parameter bounds와 replay semantics는 [Drain spec](../specs/drain-and-rolling-update.md)을 따른다.
- Version 1.5.0은 `GET /api/v1/cluster/features`와 `POST /api/v1/cluster/features/{feature}`를 추가한다. 성공 response는 `activeFeatures`와 member별 `nodeId`, `releaseVersion`, `features`를 data envelope로 반환하며, 미지원 feature activation은 non-retryable `409 feature_not_supported_by_all_members`다.
- Version 1.7.0은 `POST/GET/DELETE /api/v1/cluster/rebalance`를 추가한다. Request parameter bounds와 operation response schema는 OpenAPI artifact가 기준이다.
- Version 1.8.0은 Dynamic cluster의 활성 mutation 8개와 관리 API의 인증·인가 거절에 node-local 감사 event를 기록한다. Audit publisher가 append할 수 없는 경우 해당 mutation은 실행되지 않고 retryable `503 audit_unavailable`을 반환한다.
- Active audit producer의 범위와 JSONL hash-chain 형식은 [Audit event v1 계약](audit-event-v1.md)과 [ADR-0039](../decisions/ADR-0039-management-audit-log.md)를 따른다. Future catalog의 다른 mutation endpoint, OIDC/JWT와 dashboard는 현재 Active contract가 아니다. 이 기능을 구현할 때는 별도 범위와 contract revision을 정한다.
- Version 1.9.0은 모든 `/api/v1` operation에 `429` response를 추가한다.
- Breaking change는 `/api/v2`와 별도 OpenAPI artifact로 제공한다.
