# Management API v1 Contract

이 문서는 NaruMQ Management API v1의 query와 승인된 mutation profile을 정의한다. 정확한
path, parameter, response schema와 media type은
[OpenAPI JSON](management-api-v1.openapi.json)이 기준이다.

| 항목 | 값 |
| --- | --- |
| 이름과 version | `narumq-management-api` 1.17.0 |
| 상태 | Active |
| Producer | NaruMQ broker management API |
| Consumer | Dashboard·operator·monitoring integration의 query client |
| 방향 | HTTP request/response |
| 기준 artifact | [management-api-v1.openapi.json](management-api-v1.openapi.json) |
| 기준 spec | [Management API와 observability](../specs/management-api-and-observability.md) |
| 관련 Architecture | [System overview](../architectures/system-overview.md), [Security boundaries](../architectures/security-boundaries.md) |
| 관련 requirement | API-02, API-04~API-09, MQTT-10, SEC-07, SEC-10~SEC-12 |
| 관련 결정 | [ADR-0005](../decisions/ADR-0005-management-api-contract-format.md), [ADR-0018](../decisions/ADR-0018-release-compatibility-window.md), [ADR-0043](../decisions/ADR-0043-management-api-rate-limit.md), [ADR-0048](../decisions/ADR-0048-management-api-oidc-api-key-rbac.md), [ADR-0049](../decisions/ADR-0049-mqtt-credential-revocation.md), [ADR-0050](../decisions/ADR-0050-mqtt-client-blocklist.md), [ADR-0057](../decisions/ADR-0057-mqtt-websocket-listener.md) |
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
| POST | `/api/v1/cluster/members` | member admission 시작 | ClusterAdmin |
| DELETE | `/api/v1/cluster/members/{memberId}` | cluster member removal 시작 | ClusterAdmin |
| GET | `/api/v1/cluster/membership-operations/{operationId}` | 마지막으로 관찰한 committed membership operation 조회 | ReadOnly |
| POST | `/api/v1/cluster/backups` | quorum-fresh 경계의 checksummed backup archive 다운로드 |
| GET | `/api/v1/cluster/features` | committed active feature와 member build profile 조회 |
| POST | `/api/v1/cluster/features/{feature}` | 모든 non-retired member가 지원하는 feature 활성화 |
| GET | `/api/v1/cluster/policy` | committed `ClusterPolicy`와 version 조회 | ReadOnly |
| PUT | `/api/v1/cluster/policy` | `mqttLimits`와 선택 `tenantQuotas`를 version 조건으로 commit | ClusterAdmin |
| GET | `/api/v1/nodes` | node list |
| GET | `/api/v1/nodes/{nodeId}` | node detail |
| GET | `/api/v1/nodes/{nodeId}/config` | 요청을 받은 node의 실제 적용 설정 조회 | ClusterAdmin |
| POST | `/api/v1/nodes/{nodeId}/drain` | broker drain 시작 또는 기존 operation 재생 |
| GET | `/api/v1/nodes/{nodeId}/drain` | committed drain operation 조회 |
| DELETE | `/api/v1/nodes/{nodeId}/drain` | 취소 가능한 drain operation 취소 |
| POST | `/api/v1/cluster/rebalance` | cluster connection rebalance 시작 또는 replay |
| GET | `/api/v1/cluster/rebalance` | committed cluster connection rebalance 조회 |
| DELETE | `/api/v1/cluster/rebalance` | 실행 중인 rebalance 취소 |
| GET | `/api/v1/clients` | filter와 pagination을 사용하는 client search | ReadOnly, tenant scope |
| GET | `/api/v1/clients/{tenantId}/{clientId}` | client session detail |
| GET | `/api/v1/subscriptions` | subscription list |
| GET | `/api/v1/retained` | retained metadata list |
| GET | `/api/v1/tenants` | tenant list |
| GET | `/api/v1/security/credential-revocations` | 폐기된 credential ID 목록 조회 (SecurityAdmin·ClusterAdmin) |
| POST | `/api/v1/security/credential-revocations/{credentialId}` | credential ID 폐기 (SecurityAdmin·ClusterAdmin) |
| DELETE | `/api/v1/security/credential-revocations/{credentialId}` | credential ID 폐기 복구 (SecurityAdmin·ClusterAdmin) |
| GET | `/api/v1/security/blocklist` | blocklist와 policy version 조회 (SecurityAdmin·ClusterAdmin) |
| POST | `/api/v1/security/blocklist/client-ids/{tenantId}/{clientId}` | tenant별 client ID 차단 (SecurityAdmin·ClusterAdmin) |
| DELETE | `/api/v1/security/blocklist/client-ids/{tenantId}/{clientId}` | tenant별 client ID 차단 해제 (SecurityAdmin·ClusterAdmin) |
| POST | `/api/v1/security/blocklist/usernames/{username}` | CONNECT Username 차단 (SecurityAdmin·ClusterAdmin) |
| DELETE | `/api/v1/security/blocklist/usernames/{username}` | CONNECT Username 차단 해제 (SecurityAdmin·ClusterAdmin) |
| POST | `/api/v1/security/blocklist/certificates/{certificateSha256}` | 검증된 client certificate fingerprint 차단 (SecurityAdmin·ClusterAdmin) |
| DELETE | `/api/v1/security/blocklist/certificates/{certificateSha256}` | certificate fingerprint 차단 해제 (SecurityAdmin·ClusterAdmin) |
| POST | `/api/v1/security/blocklist/ip-prefixes/{address}/{prefixLength}` | IP prefix 차단 (SecurityAdmin·ClusterAdmin) |
| DELETE | `/api/v1/security/blocklist/ip-prefixes/{address}/{prefixLength}` | IP prefix 차단 해제 (SecurityAdmin·ClusterAdmin) |
| GET | `/api/v1/nodes/{nodeId}/audit/events` | node-local audit hash-chain record 조회 (SecurityAdmin·ClusterAdmin) |

Node 고유 정보와 특정 node만 대상으로 하는 제어는 `/api/v1/nodes/{nodeId}/...` 경로를 사용한다. 단, Raft commit으로 cluster 전체에 적용되는 `/api/v1/nodes/{nodeId}/drain`은 어느 node에서 호출해도 된다. `config`와 `audit/events`에서 `nodeId`는 canonical positive decimal uint64여야 한다. 값이 잘못됐거나 canonical 형식이 아니면 `404 resource_not_found`를 반환한다. 유효한 ID가 요청을 받은 node를 가리키지 않으면 non-retryable `409 node_not_local`을 반환하고 `error.details.servingNodeId`에 serving node ID를 넣는다. API는 원격 node로 proxy하지 않으므로 대상 node에 직접 요청해야 한다.

`GET /api/v1/nodes/{nodeId}/config`는 Dynamic cluster에서만 제공하며 `ClusterAdmin` 권한을 요구한다. Query와 request body는 허용하지 않는다. 응답은 node의 실제 적용 설정, `mqttLimits`, `tenantQuotas`의 적용 출처·version과 `mqtt.webSocket` listener address·admission limits를 반환한다. `mqtt.webSocket`은 설정 object가 없으면 `null`이다. `security`에는 JWT 설정, 인증 identity 수, ACL 규칙과 tenant limit을 포함한다. ACL 규칙은 `tenantId`, `role`, `effect`, `action`, `topicFilter`를 보여 준다. 인증 identity 목록, secret 원문과 secret file 경로, password hash, token은 반환하지 않는다. OIDC·JWT의 JWKS URL에서는 userinfo를 제거한다. 전체 응답 schema는 OpenAPI `NodeConfigurationResponse`를 따른다.

`GET /api/v1/nodes/{nodeId}/audit/events`는 SecurityAdmin 또는 ClusterAdmin만 사용할 수 있다. 현재 node의 저장된 audit records를 `seq` 내림차순으로 반환한다. 허용 query는 `limit`, `cursor`, `actorSubject`, `action`, `targetType`, `targetId`, `requestId`, `result`, `from`, `to`이며 문자열 filter는 exact match다. `result`는 `ALLOWED`, `DENIED`, `FAILED`; `from` 포함·`to` 제외 RFC 3339 시각이다.

Blocklist는 Dynamic cluster에서 `client-blocklist` feature를 활성화한 뒤 사용할 수 있으며 standalone에는 제공하지 않는다. GET의 `data`는 `{policyVersion, entries[]}`이며 각 entry는 `kind`, 선택적 `tenantId`, `value`, `blockedAt`을 가진다. POST는 항목을 차단하고 DELETE는 해제하며, 성공 결과의 `data`에는 `blocked`와 `policyVersion`이 포함된다. 실제 entry 집합이 바뀐 command의 Raft index가 policy version이며, 중복 block·이미 해제된 항목의 unblock은 version을 바꾸지 않는다. 성공한 mutation 응답 뒤 5초 안에 모든 node가 새 차단을 적용한다. Feature 비활성 또는 4096개 capacity 초과는 `409`다. Path 값은 percent-encoding할 수 있다. 예를 들어 clientId `a/b`는 `a%2Fb`로 전달하며 서버는 이를 `a/b`로 해석한다. 이 endpoint들의 query parameter와 request body는 허용되지 않으며, 있으면 `400 invalid_request`다. Mutation은 `SecurityAdmin` 또는 `ClusterAdmin` 권한을 요구한다.
Cluster policy는 Dynamic cluster에서 `cluster-policy` feature 활성화 뒤 변경할 수 있으며 standalone에는 route가 없다. GET은 query와 body를 거절하고 commit 전에는 version `0`, policy `null`을 반환한다. GET·PUT은 `ETag: "<version>"`을 반환한다. PUT은 `If-Match`와 `Idempotency-Key`를 각각 하나씩 요구한다. `If-Match`가 현재 version과 다르면 non-retryable `409 cluster_policy_version_conflict`다. 성공 mutation은 `cluster.policy.update` audit event를 cluster ID target과 이전·새 version detail로 남긴다. 선택 `tenantQuotas`는 `cluster-policy-tenant-quotas` feature 활성화 뒤 사용할 수 있고, 네 quota 값과 tenant 목록 검증은 [ADR-0052 §1.1.3](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#113-clusterpolicy-tenant-quota-g7-b)를 따른다.
PUT는 query parameter를 허용하지 않으며 query가 있으면 `400 invalid_request`다. `If-Match`가 없거나 형식이 틀리면 `400 invalid_request`이며 message는 `one valid If-Match header is required`다. `mqttLimits` 값 범위 밖, 필수 quota field 누락, 잘못된 quota profile 또는 JSON body가 512 KiB보다 크면 `400 invalid_request`다. `tenantQuotas`를 생략하거나 `null`로 지정하면 ClusterPolicy는 quota profile을 관리하지 않는다. Feature 비활성은 `409 feature_inactive`, 최근 key를 다른 요청에 쓰면 `409 idempotency_conflict`, quorum 문제는 `503`이다.

Version 1.5.0은 cluster feature 조회와 activation을 active contract에 추가한다. 조회는 `ReadOnly`, activation은 `ClusterAdmin`을 요구하며 dynamic cluster mode에서 제공된다. 이 catalog에 없는 path를 client가 호출할 수 있다는 계약은 없다.

Version 1.6.0은 `GET /api/v1/cluster` 응답에 optional `replicationFactor`, `writeQuorum`을 추가한다. Dynamic cluster는 3/2, standalone은 1/1을 반환한다.
Standalone은 `GET /api/v1/cluster`의 `clusterId`에 `narumq-cluster-1`, `leaderNodeId`와 node 응답의 `nodeId`에 `narumq-node-1`을 반환한다. 이 값은 고정이며 바꿀 수 없다. `GET /api/v1/nodes/{nodeId}`는 `narumq-node-1`에만 node를 반환하고 다른 ID에는 `404`를 반환한다. Standalone node ID에는 Dynamic cluster의 canonical decimal 규칙을 적용하지 않는다. 결정은 [ADR-0052 §1.6](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#16-standalone과-console)을 따른다.

Version 1.7.0은 `POST/GET/DELETE /api/v1/cluster/rebalance`를 추가한다. Operation은 Operator가 시작·취소하고 ReadOnly가 조회하며, 연결 수 보고와 수렴은 committed cluster operation으로 조정된다. Request의 `maxDeviationConnections`, `connectionRatePerSecond`, `loadBalancerPropagationDelayMs`, `timeoutMs`는 필수다.


## 2. Authentication과 authorization

- Cluster mode의 break-glass `Authorization: Bearer <token>`은 `management.clusterAdminTokenFile`에서 읽은 opaque token이며 항상 `ClusterAdmin` 권한을 가진다.
- `management.apiKeys`는 이름 있는 API key `{id, keyFile, roles}` 항목의 배열이다. 값은 `X-API-Key`로 전달하고 해당 항목에 설정된 역할을 가진다. 각 키의 파일 내용은 앞뒤 공백을 제거한다.
- `management.oidc`를 설정하면 `Authorization: Bearer <JWT>`를 JWKS로 검증한다. JWT의 `iss`, `aud`, `sub`, 서명 및 시간 조건을 확인하고 권한은 claim에서 읽지 않는다. `sub`가 설정의 정적 `identities[{subject, roles}]` binding에 있어야 하며, binding 역할이 권한을 결정한다.
- Principal subject는 break-glass token의 경우 `management-bearer-token`, API key의 경우 `apikey:<id>`, OIDC의 경우 `oidc:<sub>`다. 인증을 끈 local mode는 `local-unauthenticated`이며, 인증 실패 요청의 audit actor subject는 `unauthenticated`다.
- 역할은 `ReadOnly`, `Operator`, `SecurityAdmin`, `ClusterAdmin`이다. Principal은 여러 역할을 가질 수 있으며, 부여된 역할 중 하나라도 해당 권한을 가지면 허용한다.

| Permission | ReadOnly | Operator | SecurityAdmin | ClusterAdmin |
| --- | --- | --- | --- | --- |
| `PermissionRead` — 조회 route, non-anonymous `/metrics`·probe | ✓ | ✓ | ✓ | ✓ |
| `PermissionOperate` — drain·rebalance mutation |  | ✓ |  | ✓ |
| `PermissionAuditRead` — `GET /api/v1/nodes/{nodeId}/audit/events` |  |  | ✓ | ✓ |
| `PermissionClusterAdmin` — tenant, membership, feature activation, backup, cluster policy update, node configuration view |  |  |  | ✓ |
| `PermissionCredentialAdmin` — credential revocation list, revoke, restore; blocklist list·block·unblock |  |  | ✓ | ✓ |

- `GET /api/v1/nodes/{nodeId}/audit/events`는 SecurityAdmin 또는 ClusterAdmin만 사용할 수 있다. Node-local query 규칙과 node mismatch 응답은 §1의 경로 규칙을 따른다.
- Audit cursor는 RawURL base64로 인코딩한 `seq:<decimal>`이고, cursor seq보다 작은 record부터 조회한다. 응답은 공통 list envelope이며 각 record는 `seq`, `prevHash`, `hash`, 저장된 `event`를 가진다. 조회는 감사 event를 생성하지 않으며 저장 event와 chain 값을 바꾸지 않는다.
- Health probe와 `/metrics`의 익명 허용 여부는 listener configuration이다. OpenAPI의 security scheme은 보호된 배포를 기술하고, local development의 anonymous policy를 제거하지 않는다.
- 모든 HTTP response는 `X-Request-Id`를 반환한다. 요청 header가 유효하면 같은 값을 사용하고, 아니면 서버가 opaque 값을 생성한다.

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
| `/api/v1/nodes/{nodeId}/audit/events` | `actorSubject`, `action`, `targetType`, `targetId`, `requestId`, `result`, `from`, `to`, `limit`, `cursor` |
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

`404`는 존재하지 않는 resource, 잘못됐거나 canonical 형식이 아닌 node ID, 알 수 없는 router path에 사용한다. `409 node_not_local`은 유효하지만 요청을 받은 node와 다른 ID를 node-local route에 지정했을 때 반환한다. 내부 오류와 credential은 response에 노출하지 않는다.

## 5. Compatibility와 deferred scope

- `/api/v1`의 required response field 삭제·rename·type 변경은 breaking change다.
- Optional response field 추가는 contract suite와 consumer compatibility 확인 뒤에만
  허용한다.
- Version 1.2.0은 `NodeData`에 optional `replicationHealth`, `lastErrorCode`, `replicationQueue` field를 추가했다. Field schema와 enum은 OpenAPI artifact가 기준이며, contract suite가 response shape를 검증한다.
- Version 1.3.0은 `POST /api/v1/cluster/backups`를 추가했다. ClusterAdmin 전용이며 fresh quorum barrier에서 캡처한 typed snapshot과 committed tail을 binary archive로 반환한다.
- Version 1.4.0은 `/api/v1/nodes/{nodeId}/drain`의 POST/GET/DELETE를 추가한다. Drain request parameter bounds와 replay semantics는 [Drain spec](../specs/drain-and-rolling-update.md)을 따른다.
- Version 1.5.0은 `GET /api/v1/cluster/features`와 `POST /api/v1/cluster/features/{feature}`를 추가한다. 성공 response는 `activeFeatures`와 member별 `nodeId`, `releaseVersion`, `features`를 data envelope로 반환하며, 미지원 feature activation은 non-retryable `409 feature_not_supported_by_all_members`다.
- Version 1.7.0은 `POST/GET/DELETE /api/v1/cluster/rebalance`를 추가한다. Request parameter bounds와 operation response schema는 OpenAPI artifact가 기준이다.
- Version 1.8.0은 Dynamic cluster의 활성 mutation 18개와 관리 API의 인증·인가 거절에 node-local 감사 event를 기록한다. Audit publisher가 append할 수 없는 경우 해당 mutation은 실행되지 않고 retryable `503 audit_unavailable`을 반환한다.
- Active audit producer의 범위와 JSONL hash-chain 형식은 [Audit event v1 계약](audit-event-v1.md)과 [ADR-0039](../decisions/ADR-0039-management-audit-log.md)를 따른다. `GET /api/v1/nodes/{nodeId}/audit/events`는 SecurityAdmin·ClusterAdmin 전용 node-local read-only query다. Dashboard는 ReadOnly API key만 사용하며 OIDC 사용자 로그인은 Grafana가 맡는다.
- Version 1.9.0은 모든 `/api/v1` operation에 `429` response를 추가한다.
- Version 1.10.0은 이름 있는 API key, 정적 subject binding을 사용하는 OIDC JWT와 4-role authorization을 추가하고 node-local audit chain 조회를 제공한다.
- Version 1.11.0은 Dynamic cluster 전용 credential revocation GET·POST·DELETE route와 `SecurityAdmin`·`ClusterAdmin` 권한을 추가한다. Replicated state의 기능은 `credential-revocation` feature activation 뒤에만 사용한다.
- Version 1.12.0은 Dynamic cluster 전용 `client-blocklist` feature를 사용하는 GET·POST·DELETE route를 추가한다. SecurityAdmin·ClusterAdmin만 조회·변경할 수 있으며, path 값은 percent-encoding을 허용하고 query와 body는 거절한다. Blocklist entry, canonical 값, 전파 목표와 policy version 의미는 [ADR-0050](../decisions/ADR-0050-mqtt-client-blocklist.md)를 따른다.
- Version 1.13.0은 Dynamic cluster 전용 `cluster-policy` feature를 사용하는 `GET/PUT /api/v1/cluster/policy`와 `mqttLimits` version update를 추가한다. GET은 ReadOnly, PUT은 ClusterAdmin을 요구하며 `If-Match`, `Idempotency-Key`, ETag와 audit detail을 사용한다.
- Version 1.14.0은 `loadBalancerPropagationDelayMs`, `timeoutMs`, `uptimeMs`를 각각 `loadBalancerPropagationDelayMillis`, `timeoutMillis`, `uptimeMillis`로 바꿨다. 이 breaking change는 JSON duration 이름을 정수 millisecond 표기로 맞추기 위한 1회 예외다. Consumer는 새 이름으로 갱신한 뒤 배포해야 한다. 이전 이름은 지원하지 않는다.
- Breaking change는 `/api/v2`와 별도 OpenAPI artifact로 제공한다.
- G7의 audit path 이동은 이 contract에서 명시한 breaking-change 규칙의 예외다. 이전 `/api/v1/audit/events` 경로는 지원하지 않는다.
- Version 1.15.0은 `ClusterPolicy` request·response에 선택적 `tenantQuotas`를 추가한다. 해당 field는 `cluster-policy-tenant-quotas` feature 활성화 뒤 사용할 수 있고, PUT body 한도는 512 KiB다.
- Version 1.15.0은 Dynamic cluster 전용 `GET /api/v1/nodes/{nodeId}/config`를 추가한다. ClusterAdmin 전용이며 적용 설정, policy 출처와 version을 반환하고 secret 원문·경로를 제외한다. Node-local route는 요청 node의 canonical decimal ID만 받으며 다른 ID에는 `409 node_not_local`을 반환한다.
- Version 1.15.0은 audit query를 `/api/v1/audit/events`에서 `/api/v1/nodes/{nodeId}/audit/events`로 이동한다. 이전 경로는 지원하지 않으며 client를 갱신해야 한다. Audit query도 local node만 대상으로 한다.
- Version 1.16.0은 drain POST의 `connectionRatePerSecond`, `loadBalancerPropagationDelayMillis`, `timeoutMillis`를 선택 field로 바꾼다. 생략하면 [ADR-0055](../decisions/ADR-0055-drain-and-reconnect-operating-defaults.md)의 기본값 `100`, `30000`, `600000`을 적용한다. 세 값을 모두 보내는 기존 요청은 그대로 유효하다. Rebalance request는 바뀌지 않는다.
- Version 1.17.0은 node configuration response의 `mqtt.webSocket` field를 추가한다. Field는 항상 포함하고, cluster config에 `mqtt.webSocket`이 없으면 `null`을 반환한다. 설정하면 WS/WSS 주소와 네 admission limit을 반환한다.
