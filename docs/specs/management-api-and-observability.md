# Management API와 observability 스펙

이 문서는 operator, GitOps controller, Dashboard와 monitoring system이 NaruMQ를 조회·변경하는 HTTP·Metric 계약의 전체 목표를 정의한다. 현재 active profile은 [Management API v1 contract](../contracts/management-api-v1.md)와 [OpenAPI 3.1 contract](../contracts/management-api-v1.openapi.json)으로 확정하고, 다른 미래 catalog는 명시적으로 deferred scope로 표시한다.

| 항목 | 값 |
| --- | --- |
| 상태 | Active (Phase 1 query profile, broker drain, cluster feature activation, management-plane audit, API rate limit, SEC-10 OIDC/API key RBAC, SEC-12 client blocklist, API-09 `mqttLimits`와 node configuration 조회, TEN-08 ClusterPolicy quota slice, MQTT-10 WS/WSS admission metric; 다른 ADR-0052 policy domain과 운영 mutation은 deferred) |
| 관련 요구사항 | API-01~API-09, MQTT-10, UI-01~UI-10, SEC-10~SEC-12 |
| 근거 | [PRD 9.5절, 9.6절과 14장](../requirements/PRD.md) |
| 관련 spec | [Tenant와 security](tenancy-and-security.md), [Drain](drain-and-rolling-update.md) |
| 관련 Architecture | [System overview](../architectures/system-overview.md), [Security boundaries](../architectures/security-boundaries.md) |
| 관련 결정 | [ADR-0005](../decisions/ADR-0005-management-api-contract-format.md), [ADR-0018](../decisions/ADR-0018-release-compatibility-window.md), [ADR-0043](../decisions/ADR-0043-management-api-rate-limit.md), [ADR-0045](../decisions/ADR-0045-operations-dashboard-grafana.md), [ADR-0048](../decisions/ADR-0048-management-api-oidc-api-key-rbac.md), [ADR-0050](../decisions/ADR-0050-mqtt-client-blocklist.md), [ADR-0052](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md) §1.1.1, [ADR-0057](../decisions/ADR-0057-mqtt-websocket-listener.md) |
| 관련 Contract | [Management API v1](../contracts/management-api-v1.md), [Management API OpenAPI](../contracts/management-api-v1.openapi.json), [Audit event v1](../contracts/audit-event-v1.md) |

## 1. HTTP 공통 계약

- Management API의 base path는 `/api/v1`이다. 사용자가 승인한 1.18.0 개발 계약 재정의는 현재 v1에서 직접 교체하며 별도 alias를 제공하지 않는다. 이 재정의 이후의 breaking change는 새 major path에서 제공한다.
- request와 response body는 `application/json` UTF-8을 사용한다.
- `/api/v1` 요청은 인증과 RBAC를 통과해야 한다. Health probe와 `/metrics`의 익명 노출 범위는 listener 정책으로 제한할 수 있다.
- 서버는 수신한 유효 request ID를 사용하거나 새 값을 생성해 `X-Request-Id`에 반환해야 한다.
- `cluster-policy`가 활성화된 Dynamic cluster는 `GET /api/v1/cluster/policy` 조회와 `PUT /api/v1/cluster/policy` mutation을 제공한다. GET은 feature 활성 여부와 관계없이 ReadOnly 권한으로 쓸 수 있고, PUT은 ClusterAdmin 권한과 `If-Match`, `Idempotency-Key`를 요구한다. 성공 응답은 새 `ETag` version을 반환한다. Standalone에는 route가 없다.

### 1.1 Management listener와 인증 정책

- `-http-listen`은 Management HTTP listener의 bind 주소다.
- `-http-auth-token-file`이 설정되면 파일에서 앞뒤 공백을 제거한 token을 읽고 유효한 `Authorization: Bearer <token>`을 요구하며, `/api/v1`에는 endpoint별 RBAC를 적용한다. 인증 실패는 `401`, 인증은 됐지만 권한이 부족하면 `403`이다.
- Cluster mode의 `management.apiKeys`에는 이름 있는 credential을 `{id, keyFile, roles}`로 등록한다. Key 파일은 읽은 뒤 앞뒤 공백을 제거하며 비어 있거나 cluster admin token 또는 다른 API key와 같으면 process 시작을 거절한다. API key principal subject는 `apikey:<id>`다.
- Cluster mode의 `management.oidc`는 JWT issuer·audience·algorithm allowlist와 JWKS 검증을 설정하고 `identities`의 정적 subject→roles binding으로 권한을 결정한다. JWT claims는 roles를 선택하지 않는다. OIDC principal subject는 `oidc:<sub>`다.
- Cluster admin bearer token은 ClusterAdmin 권한의 break-glass credential로 유지한다. Management API의 네 role과 permission mapping은 [Tenant와 security §6](tenancy-and-security.md#6-관리-plane-rbac) 및 [ADR-0048](../decisions/ADR-0048-management-api-oidc-api-key-rbac.md)을 따른다. Grafana Dashboard는 ReadOnly API key만 사용하며, 사용자 인증은 Grafana가 맡는다.
- 이 credential 형식은 cluster mode 전용이다. Standalone mode는 `-http-auth-token-file` 하나만 지원하며, 설정 시 ClusterAdmin bearer credential이다. `-http-auth-token-file`을 지정하지 않으면 local development용 익명 모드로 동작하지만, `-http-listen`은 literal loopback IP(`127.0.0.0/8` 또는 `::1`)만 허용한다. `0.0.0.0`, `:8080`, 외부 인터페이스 주소는 거부한다.
- 따라서 원격 interface에 Management API를 노출하려면 인증을 설정해야 한다. CLI에서 health probe와 `/metrics`는 listener 정책에 따라 익명 노출할 수 있다. 운영 환경에서는 probe listener와 firewall 범위를 별도로 제한해야 한다.

예:

```text
# local development: loopback-only anonymous mode
go run ./cmd/narumq -http-listen 127.0.0.1:8080

# remote/private interface: authentication required
go run ./cmd/narumq -http-listen 0.0.0.0:8080 -http-auth-token-file /etc/narumq/management.token
```

### 1.2 성공 envelope

단일 resource는 `data`, 목록은 `data`와 `page`를 반환한다.

```json
{
  "data": [],
  "page": {
    "nextCursor": "opaque-or-null",
    "hasMore": false
  }
}
```

cursor는 불투명 값이다. client는 cursor 내용을 해석해서는 안 된다. 정렬 기준은 endpoint OpenAPI에 고정해야 하며 같은 snapshot에서 안정적이어야 한다.
- Phase 1 목록 endpoint의 `limit` 기본값은 `50`, 최댓값은 `1000`이다. endpoint에 선언되지 않은 query name, 중복 query value, 빈 값 또는 범위를 벗어난 `limit`은 `400 invalid_request`로 거절한다.

### 1.3 오류 envelope

```json
{
  "error": {
    "code": "quorum_unavailable",
    "message": "요청을 완료할 수 없습니다.",
    "retryable": true,
    "details": {}
  },
  "requestId": "opaque-request-id"
}
```

| HTTP status | 사용 조건 |
| --- | --- |
| `400` | malformed JSON, invalid filter, invalid transition |
| `401` | 인증 없음 또는 실패 |
| `403` | 인증됐지만 resource/action 권한 없음 |
| `404` | resource 없음. 권한 밖 resource를 숨기는 데도 사용할 수 있다. |
| `409` | lifecycle·version·idempotency 충돌 |
| `422` | 형식은 유효하지만 quota·정책 제약을 충족하지 못함 |
| `429` | API rate limit 초과. 가능한 경우 `Retry-After` 포함 |
| `503` | quorum, dependency 또는 readiness 문제로 일시 처리 불가 |

오류 code 전체 목록과 각 endpoint의 response status는 [Management API v1 OpenAPI](../contracts/management-api-v1.openapi.json)에서 확정한다. 내부 stack, storage key와 secret을 오류에 포함해서는 안 된다. Active drain, feature activation, blocklist와 cluster policy mutation은 `409` 상태를 사용한다. Feature activation의 `feature_not_supported_by_all_members`는 non-retryable이며, `422`를 포함한 다른 mutation 동작은 deferred다. `429`는 §1.5를 따른다.

### 1.4 Deferred mutation과 idempotency

Active broker drain endpoint는 `Idempotency-Key`를 요구한다. 같은 node에 active operation이 있으면 새 operation을 만들지 않고 committed operation을 돌려준다. 같은 key에 다른 parameter를 보내면 `409 idempotency_conflict`다. Feature activation은 같은 active feature를 다시 요청하면 성공 no-op이므로 별도 idempotency key가 필요하지 않다. Dynamic cluster의 활성 management mutation 19개는 모두 감사 event를 기록하며, publisher를 쓸 수 없으면 handler를 실행하지 않고 retryable `503 audit_unavailable`을 반환한다. Blocklist block·unblock은 결과 상태가 정해진 set operation이므로 `Idempotency-Key`를 요구하지 않는다. Cluster policy update는 최근 한 건의 key만 replay한다. 같은 key를 같은 body·expected version으로 반복하면 같은 version을 반환한다. 그 외 mutation endpoint는 아직 deferred다.

향후 mutation은 `Idempotency-Key`를 받아야 한다. 같은 principal, endpoint와 key에 같은 payload를 재시도하면 최초 결과와 의미가 같아야 한다. 같은 key에 다른 payload가 오면 `409 idempotency_conflict`를 반환해야 한다. Resource 변경은 가능한 경우 version/ETag 기반 조건부 요청을 지원해야 한다.

### 1.5 Rate limit

각 node는 등록된 `/api/v1/**` route 요청에 독립된 process-local token bucket을 적용해야 한다. Cluster 전체 합산 한도는 없다. Health probe, `/metrics`와 등록되지 않은 path의 404 handler는 제한하지 않는다.

인증 성공 요청은 principal의 `subject`별 bucket을 사용해야 한다. 인증 실패 요청은 `RemoteAddr`에서 분리한 host별 bucket을 사용해야 한다. `X-Forwarded-For`를 client address로 신뢰해서는 안 된다. 익명 local mode에서 인증 성공 요청은 `local-unauthenticated` subject를 사용한다. 새 bucket은 가득 찬 상태로 시작하며 설정한 rate에 따라 token을 보충한다.

처리 순서는 인증, 인증 실패 IP bucket 또는 성공 principal bucket, role 검사, mutation audit admission이어야 한다. 인증 실패 bucket을 초과하면 `401` 대신 `429`를 반환해야 한다. 인증 성공 요청은 role 검사 전에 principal token을 소비하므로 `403`도 token을 소비한다. Rate limit에 걸린 mutation은 handler를 실행하거나 `AdmitMutation`을 호출해서는 안 된다.

한도를 넘긴 요청은 HTTP `429`, error code `rate_limited`, message `management API rate limit exceeded`, `retryable=true`와 `Retry-After: 1`을 반환해야 한다. 인증 실패 IP bucket 거절의 subject는 `unauthenticated`이며 roles를 생략한다. 성공 인증 거절은 인증 subject와 현재 role을 기록한다. 기존 node별 denial event 상한을 적용한다.
Audit producer가 구성된 node는 rate-limit 거절을 `management.request` event로 기록해야 한다. Target은 `management_endpoint`와 route pattern이고, result는 `DENIED`, reason은 `rate_limited`다. Audit event v1 §5에 따라 Dynamic cluster mode는 producer 범위에 포함하고 standalone mode는 포함하지 않는다.

Limiter는 각 bucket map에 최대 4096개 key를 보관해야 한다. 새 key를 저장할 때 상한에 도달하면 모든 bucket을 refill한 뒤 가득 찬 bucket을 제거해야 한다. 그래도 상한이면 새 key 요청을 거절해야 한다. 기존 key는 계속 처리한다.

Standalone mode는 `-http-request-rate`와 `-http-auth-failure-rate`를 사용한다. Dynamic cluster mode는 `management.requestRate`와 `management.authFailureRate`를 사용한다. 두 mode의 기본값, 허용 범위와 field 형식은 [Process configuration v1 §1.1, §1.3](../contracts/process-configuration-v1.md)에서 정한다. 설정한 값은 token 보충 rate와 burst 용량에 함께 적용한다. 세부 결정은 [ADR-0043](../decisions/ADR-0043-management-api-rate-limit.md)을 따른다.

## 2. Endpoint catalog

### 2.1 Phase 1 active profile

- OpenAPI에 정의된 24개 GET operation(health·metrics 포함)과 cluster backup POST, broker drain POST/GET/DELETE, membership, rebalance, feature, security와 cluster policy operation이 구현·계약 대상이다. 정확한 parameter allowlist, schema, response와 operation ID는 OpenAPI가 SSOT이며 `tests/contracts/management-api-v1`가 runtime handler와 일치하는지 검사한다.
Node configuration와 audit query는 요청을 받은 node에만 적용한다. `nodeId`는 canonical positive decimal uint64여야 한다. ID가 잘못됐거나 canonical 형식이 아니면 `404 resource_not_found`, 다른 유효한 node ID면 `409 node_not_local`을 반환하며 원격 proxy를 하지 않는다. `/api/v1/nodes/{nodeId}/drain`은 Raft commit으로 cluster 전체에 적용되므로 이 제한에서 제외한다.

| Method | Path | 성공 동작 | 권한 |
| --- | --- | --- | --- |
| GET | `/health/live` | process가 deadlock 없이 응답 가능하면 `200` | probe policy |
| GET | `/health/ready` | 신규 MQTT·관리 traffic 수용 가능하면 `200` | probe policy |
| GET | `/health/startup` | 초기 동기화가 완료되면 `200` | probe policy |
| GET | `/health/drain` | node가 `SAFE_TO_STOP`이면 `200` | probe policy |
| GET | `/metrics` | Prometheus 또는 OpenMetrics scrape | monitoring policy |
| GET | `/api/v1/metrics` | node 또는 cluster typed JSON metric snapshot 조회 | ReadOnly |
| GET | `/api/v1/cluster` | quorum과 cluster 요약 조회 | ReadOnly |
| POST | `/api/v1/cluster/members` | cluster member admission 시작 | ClusterAdmin |
| DELETE | `/api/v1/cluster/members/{memberId}` | planned removal 시작 또는 기존 operation replay | ClusterAdmin |
| POST | `/api/v1/cluster/backups` | quorum-fresh typed state backup archive 다운로드 | ClusterAdmin |
| GET | `/api/v1/nodes` | state filter와 pagination을 사용한 node 목록 조회 | ReadOnly |
| GET | `/api/v1/nodes/{nodeId}` | node detail 조회 | ReadOnly |
| GET | `/api/v1/nodes/{nodeId}/metrics` | 대상 node의 typed metric snapshot 조회 | ReadOnly |
| GET | `/api/v1/nodes/{nodeId}/config` | 해당 node의 실제 적용 설정·policy source 조회 | ClusterAdmin |
| GET | `/api/v1/cluster/features` | committed active feature와 member build profile 조회 | ReadOnly |
| POST | `/api/v1/cluster/features/{feature}` | 모든 non-retired member가 지원하는 feature 활성화; replay는 성공 no-op | ClusterAdmin |
| GET | `/api/v1/cluster/membership-operations/{operationId}` | 마지막으로 관찰한 committed membership phase 조회 | ReadOnly |
| GET | `/api/v1/cluster/policy` | committed version과 policy 조회 | ReadOnly |
| PUT | `/api/v1/cluster/policy` | version 조건으로 `mqttLimits`와 선택 `tenantQuotas`를 commit; stale version은 `409 cluster_policy_version_conflict` | ClusterAdmin |
| POST | `/api/v1/nodes/{nodeId}/drain` | committed voter의 broker drain 시작 또는 현재 operation 재생 | Operator |
| GET | `/api/v1/nodes/{nodeId}/drain` | committed drain operation 조회 | ReadOnly |
| DELETE | `/api/v1/nodes/{nodeId}/drain` | cancellable drain operation 취소 | Operator |
| GET | `/api/v1/cluster/rebalance` | committed connection rebalance 조회 | ReadOnly |
| GET | `/api/v1/clients` | tenant·client prefix·node·protocol filter로 client 검색 | ReadOnly, tenant scope |
| GET | `/api/v1/clients/{tenantId}/{clientId}` | session 상세 조회 | ReadOnly, tenant scope |
| GET | `/api/v1/subscriptions` | required `tenantId` 범위의 subscription 조회 | ReadOnly, tenant scope |
| GET | `/api/v1/retained` | required `tenantId` 범위의 retained metadata 조회 | ReadOnly, tenant scope |
| GET | `/api/v1/tenants` | tenant 목록 조회 | ClusterAdmin |
| GET | `/api/v1/security/credential-revocations` | 폐기된 credential ID 목록 조회 | SecurityAdmin, ClusterAdmin |
| GET | `/api/v1/security/blocklist` | committed blocklist와 policy version 조회 | SecurityAdmin, ClusterAdmin |
| GET | `/api/v1/nodes/{nodeId}/audit/events` | 요청을 받은 node의 저장 audit hash-chain record를 최신순으로 조회 | SecurityAdmin, ClusterAdmin |

### 2.2 Future/deferred catalog

아래 endpoint는 목표 catalog에 남겨 두지만 Phase 1 contract에는 포함하지 않는다. 구현·OpenAPI path·acceptance test가 추가되기 전에는 제품 동작으로 간주하지 않는다.

| Method | Path | Deferred 이유 |
| --- | --- | --- |
| POST | `/api/v1/tenants` | tenant mutation, idempotency와 audit 계약 미확정 |
| GET/PUT | `/api/v1/tenants/{tenantId}/quotas` | quota 저장·version 계약 미확정 |
| GET/PUT | `/api/v1/tenants/{tenantId}/policies` | ACL 저장·version 계약 미확정 |
| DELETE | `/api/v1/clients/{tenantId}/{clientId}` | disconnect mutation과 audit 계약 미확정 |
| POST | `/api/v1/clients/{tenantId}/{clientId}/redirect` | MQTT 5 redirect controller 계약 미확정 |
| DELETE | `/api/v1/retained/{topic}` | retained mutation과 tenant-safe topic encoding 미확정 |

Active drain endpoint의 body와 상태는 [drain spec](drain-and-rolling-update.md)을 기준으로 한다. Future MQTT redirect는 MQTT 5 `0x9C`와 Server Reference를 사용할 수 있지만 MQTT 3.1.1 client에 같은 의미를 약속해서는 안 된다.

## 3. Probe 의미

| Probe | 성공 조건 | 실패가 뜻하는 것 |
| --- | --- | --- |
| startup | process 초기화, local recovery, cluster sync와 필수 route 준비 완료 | 아직 재시작 판정에 liveness를 사용하면 안 됨 |
| liveness | process가 내부 health loop에 응답하고 진행 불능 상태가 아님 | kubelet 재시작 후보. terminal Raft `FAILED`는 503을 반환한다. |
| readiness | state=`READY`, quorum 확인, 필수 replica·route 준비, 신규 연결 허용 | load balancer에서 제외해야 함 |
| drain-complete | drain state=`SAFE_TO_STOP` | Pod 삭제 승인이 아님. rollout controller가 별도 판단해야 함 |

`DRAINING`과 `SAFE_TO_STOP` node의 readiness는 실패해야 한다. Startup이 아직 실패한다고 liveness도 실패하게 만들어 초기 sync 중 재시작 loop를 유발해서는 안 된다.

Cluster startup은 `JOINING`일 때 readiness와 같은 quorum·sync·route 준비 조건을 직접 확인한다. 조건을 충족하면 `READY`로 전환하고 성공한다. 외부 `/health/ready` 호출을 기다려서는 안 된다. Kubernetes는 startup 성공 전에 readiness probe를 실행하지 않기 때문이다. Typed Raft recovery 전에는 계속 `STARTING`이며, 이미 시작한 node의 drain 상태를 startup 요청으로 해제하지 않는다.

## 4. Cluster·node representation

Cluster 요약은 cluster identity·release compatibility·term·leader, membership/quorum 정보, lifecycle별 node 수, replication/storage 요약을 제공한다. `nodesByState`에는 `joining`, `ready`, `draining`, `safeToStop`, `unknown`을 반환한다. `unknown`은 retired member를 제외하고 실제 상태를 읽지 못한 member 수다. `nodeQueryComplete`와 `observedAt`은 cluster node query 완료 상태와 응답 생성 시각을 나타낸다.

Node 상세는 membership identity와 query 상태, 대상 node에서 직접 측정한 version, uptime, lifecycle, readiness 원인, role, applied index, connection·session 수, storage pressure와 local storage health를 제공한다. 측정값은 required nullable field다. `queryStatus=OK`이면 readiness가 false여도 실제 측정 결과다. 원격 조회 실패·미지원·퇴역·오류에서는 모든 측정값과 `observedAt`이 null이며 기본값이나 이전 값을 재사용하지 않는다. `replicationHealth`는 대상 node의 local storage health만 나타낸다. `replicationQueue`는 제거하고, peer별 replication lag는 `narumq_raft_peer_replication_lag_entries{peer}`로 본다.

`connectionCount`와 `sessionCount`는 non-retired node를 모두 성공적으로 읽었을 때만 합산한다. 일부 node가 unknown이면 두 합계는 null이다. `subscriptionCount`는 각 node의 복제된 route projection을 합산하지 않고 요청 node의 local committed projection 값으로 반환한다. `quorumAvailable`은 요청 node의 fresh proof 결과이며 remote query 성공 여부로 대체하지 않는다. `memberCount`, leader·quorum profile과 `underReplicated`의 의미는 유지한다.

Node 목록은 Dynamic node ID 오름차순으로 반환한다. `state` filter는 실제 state를 성공적으로 읽은 node만 대상으로 한다. Filter가 없으면 retired·staged member도 나타내며 staged member는 `UNREACHABLE`/`node_not_running`이다. Page 사이의 membership snapshot 원자성은 보장하지 않는다.

Metric 수집은 Management API와 `/metrics`가 같은 typed Registry snapshot을 사용한다. `/metrics?mode=node`는 기존 node-local fast path이며 RPC·readiness proof가 없다. `all_nodes_unaggregated`는 sample마다 `node_id`를 붙이고 각 node의 replicated projection도 별도 보존한다. `all_nodes_aggregated`는 catalog의 gauge `sum`, `max`, `per_node`만 적용한다. Counter와 histogram은 process restart 뒤 reset-safe rate/increase를 계산할 수 없으므로 node별 series를 유지한다. Histogram bucket·stage label과 누락 sample은 그대로 보존하며 없는 값을 0으로 만들지 않는다.

`/api/v1/nodes/{nodeId}/metrics`는 node snapshot과 `startedAt`을 반환한다. `GET /api/v1/metrics` 기본값은 `all_nodes_unaggregated`다. Unaggregated 응답은 부분 실패를 HTTP 200, `complete=false`, node별 상태 metadata로 나타낸다. Aggregated JSON과 cluster text mode는 non-retired node 하나라도 실패하면 retryable 503을 반환하며 partial text를 쓰지 않는다. `/metrics?mode=node`는 anonymous scrape policy를 유지한다. Cluster text mode와 모든 JSON metric route는 `ReadOnly` 인증이 필요하다. Public measurement/schema·mode·error 형식은 [Management API v1 contract](../contracts/management-api-v1.md)과 OpenAPI가 기준이다.

Dynamic Raft inbound의 count·byte 한도에는 payload를 읽기 위해 예약한 frame과 driver가 인수했지만 아직 `Complete`하지 않은 frame도 포함한다. 포화된 reader는 payload를 할당하지 않은 채 공간을 기다리며, 이 아직 예약하지 못한 frame은 queue count·byte에 더하지 않는다. 한 reader의 예약·대기·해제는 [v2 Raft 경계 계약](../contracts/raft-membership-v2.md)의 채택된 inbound 정책을 따른다. Forwarding lane과 fixed-voter queue의 의미는 바꾸지 않는다.

`cluster readiness rejected` 내부 로그의 `purpose=publish`는 PUBLISH의 local projection admission이다. 이 판정은 quorum proof가 아니므로 snapshot을 기록하더라도 `fresh_proof_confirmed=false`다. 실제 fresh read barrier를 통과한 readiness의 local snapshot 거절만 이 필드를 `true`로 기록한다. Snapshot 적용과 profile 조회 오류로 동기화가 해제될 때는 release-profile 로그의 고정 `stage`로 원인을 구분한다. Writer의 치명적인 delivery/control 오류는 socket을 닫기 전에 기록한다. Tenant/client ID·topic·payload·token을 추가하지 않는다.

## 5. Filtering과 masking

- 목록 endpoint는 cursor pagination과 endpoint별 allowlist filter만 허용해야 한다.
- client 검색 Phase 1 filter는 `tenantId`, `clientIdPrefix`, `nodeId`, `protocol`이다. arbitrary regular expression, IP filter와 wildcard topic Metric 등록을 허용해서는 안 된다.
- `subscriptions`와 `retained` 목록은 `tenantId`를 required query로 받아 tenant 범위를 명시해야 한다.
- client identity, IP, inflight payload와 offline payload는 role에 따라 마스킹하거나 제외해야 한다.
- Dynamic cluster에서 `session-metadata` feature를 활성화하면 `/api/v1/clients`와 `/api/v1/clients/{tenantId}/{clientId}`는 조회 node에 적용된 committed session metadata(합의로 저장된 연결·만료 정보)를 보여 준다. Owner node가 중단돼도 연결 상태·owner node·protocol·만료 예정 시각과 복제된 inflight/offline 개수를 조회할 수 있다. 장애를 timeout으로 추정하지 않으므로 `connected`는 owner 재시작 복구 또는 handoff가 commit될 때까지 이전 값을 유지한다. 활성화 전에 연결한 session은 다음 CONNECT 전까지 이 view에 표시되지 않는다. 비활성 cluster와 standalone은 기존 local session store를 사용한다([Cluster §4.3](cluster-session-and-delivery.md#43-session-metadata-복제와-owner-손실-복구)).
- retained 목록은 payload를 기본 반환하지 않고 metadata만 반환해야 한다. Payload 조회가 필요하면 별도 권한과 size limit을 가져야 한다.

## 6. GitOps와 runtime 변경

각 설정 필드는 source of truth를 `gitops`, `runtime` 또는 `derived`로 선언해야 한다.

- `gitops` 필드는 runtime API로 영구 변경해서는 안 된다. 이 rule은 deferred mutation API가 활성화될 때 적용한다.
- `runtime` 필드는 active mutation API와 감사 로그를 통해 바꿀 수 있다.
- reconciliation은 desired version, observed version, last result와 conflict를 조회 가능하게 해야 한다.
- controller가 값을 되돌렸을 때 silent success로 표시해서는 안 된다.

## 7. Metric 계약

`/metrics`는 listener 정책이 허용하는 경우 인증된 또는 익명 scrape 요청에 Prometheus/OpenMetrics 호환 text format을 반환해야 한다. 응답 header는 Prometheus 기본값 `text/plain; version=0.0.4; charset=utf-8`, OpenMetrics 요청값 `application/openmetrics-text; version=1.0.0; charset=utf-8`을 사용한다.

| Metric 종류 | 집계 규칙 |
| --- | --- |
| counter | cluster 값은 node counter의 rate/increase를 조회 계층에서 합산한다. restart를 고려한다. |
| gauge | 합계가 의미 있는 값만 합산한다. quorum, leader와 state는 합산하지 않고 명시적 gauge로 표시한다. |
| histogram | 같은 bucket 경계만 cluster에서 합산한다. quantile을 node별로 계산해 평균내서는 안 된다. |

Metric별 범위와 집계 방식은 [§7.1](#71-metric별-nodecluster-집계)을 따른다.


Dynamic cluster의 Raft WAL 동기화 세 series는 **WAL group fsync 한 번의 성공**을 관찰한다. [ADR-0020](../decisions/ADR-0020-asynchronous-raft-wal-group-commit.md)의 단일 worker는 이미 대기 중인 여러 Ready 저장 요청을 각각 CRC frame으로 기록하고 한 번 sync할 수 있다. Histogram 한 표본은 frame 수가 아니라 실제 성공한 `File.Sync` 한 번이다. Duration은 이 호출의 시간만, entries와 bytes는 그 sync가 함께 확정한 frame들의 합계를 나타낸다. Snapshot 파일·directory, metadata, restore 동기화 또는 fixed-voter 경로는 포함하지 않는다.

| Metric | Type | 값과 bucket |
| --- | --- | --- |
| `narumq_raft_wal_sync_duration_seconds` | histogram | 성공한 WAL group fsync의 초 단위 시간. finite bucket: `0.0001`, `0.00025`, `0.0005`, `0.001`, `0.0025`, `0.005`, `0.01`, `0.025`, `0.05`, `0.1`, `0.25`, `0.5`, `1`; 표준 `+Inf` bucket을 추가한다. |
| `narumq_raft_wal_sync_entries` | histogram | 해당 fsync로 확정한 모든 CRC frame의 Raft log entry 합계. Commit-only frame은 0 entries로 포함한다. finite bucket: `0`, `1`, `2`, `4`, `8`, `16`, `32`, `64`, `128`, `256`; 표준 `+Inf` bucket을 추가한다. |
| `narumq_raft_wal_written_bytes_total` | counter | 성공한 group fsync가 확정한 append bytes의 누적값. 각 frame의 8-byte header와 encoded payload를 합산한다. 실패한 write·sync의 bytes는 계측하지 않는다. |

이 세 series는 **label을 갖지 않는다**. 따라서 client ID, tenant, arbitrary topic 또는 node identity를 label에 추가해서는 안 된다. Registry는 모든 모드에서 series를 render하며 WAL observer가 연결되지 않은 standalone/fixed-voter 모드의 값은 0이다. Histogram을 cluster 차원으로 합산할 때는 node별 raw bucket count를 동일 경계에서 더하고, node별 quantile의 평균을 구하지 않는다.

`internal/adapters/raft`의 `WALSyncObserver`는 dynamic cluster의 successful WAL group `File.Sync` 뒤에 한 번만 불린다. 저장 값이 없어 frame·sync가 생략된 요청과 fsync 실패는 관찰하지 않으며, fixed-voter `raft_peer_config.go` 경로에는 observer를 연결하지 않는다.

Dynamic cluster는 `narumq_raft_stage_duration_seconds{stage}` histogram으로 driver의 처리·대기 시간을 기록한다. 고정 stage와 시작·종료 경계는 [BP-03 Raft 병목 진단](performance-and-verification.md#raft-병목-진단)을 따른다. Finite bucket은 `0.0001`, `0.00025`, `0.0005`, `0.001`, `0.0025`, `0.005`, `0.01`, `0.025`, `0.05`, `0.1`, `0.25`, `0.5`, `1`, `2`, `5`, `10`초이며 표준 `+Inf`를 추가한다. Registry는 모든 mode에서 render하지만 dynamic driver가 아닌 경로의 값은 0이다. 이 값에는 실패와 빈 Ready 처리도 포함되므로 WAL 성공 수나 MQTT 성공 ACK 수와 같은 분모로 취급하지 않는다.

Dynamic cluster의 readiness 거절은 `cluster readiness rejected` 구조화 로그로 진단한다. 고정 purpose·stage, 판정 시점 snapshot, 첫 불충족 조건, 집계·출력 제한과 민감정보 제외 규칙은 [BP-03 Readiness 진단 계약](performance-and-verification.md#readiness-진단-계약)을 따른다. Storage 검사나 fresh proof 단계에서 거절되면 아직 local snapshot을 판정하지 않았으므로 해당 flag와 index를 기록하지 않는다. 새 Metric label이나 API 응답 필드는 추가하지 않으며 기존 readiness·CONNACK 결과는 유지한다.


최소 범주는 connection/session/subscription, message·packet·ACK, latency, cluster·replication, storage, auth·ACL, drain과 process resource다. `narumq_node_drain_transitions_total{phase}`는 committed node drain 상태를 phase별로 집계하며, label은 8개 유효 drain phase만 허용한다. Dynamic cluster의 저장 quota는 `narumq_tenant_quota_rejections_total{tenant,dimension}` counter와 `narumq_tenant_quota_profile_committed`, `narumq_tenant_quota_profile_version`, `narumq_tenant_quota_config_mismatch` gauge를 제공한다. `tenant`는 처음 본 값 1,024개까지만 그대로 보존하고 나머지는 `_overflow`로 합친다. `dimension`은 `retained_bytes`, `offline_queue_messages`, `offline_queue_bytes`, `storage_bytes`만 허용한다. 기존 Metric 이름과 histogram bucket은 `SPEC-TBD-07`에서 확정한다. |

`narumq_cluster_policy_committed` gauge는 local node에 ClusterPolicy가 commit됐는지 나타낸다. `narumq_cluster_policy_version`은 적용한 version을, `narumq_cluster_policy_config_mismatch`는 local config와 commit 값이 다른지 나타낸다. 이 metric은 [ADR-0052 §1.1](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#11-cluster-정책의-권한과-reconciliation)을 따른다.

`narumq_tenant_limit_rejections_total{tenant,dimension}` counter는 tenant 연결·traffic
한도 거절마다 증가한다. `dimension`은 `connections`, `sessions`, `subscriptions`,
`message_rate`, `byte_rate`, `payload_size`만 허용한다. `tenant` label은 저장 quota
metric과 처음 본 tenant 1,024개의 집합을 공유한다. 나머지는 `_overflow`로 합친다.
Warn log `tenant limit rejected a request`는 tenant·dimension마다 10초에 한 번만 남긴다.
동작은 [Tenant §5.2](tenancy-and-security.md#52-연결-상태와-traffic-한도)를 따른다.

`narumq_will_outcomes_total{reason}` counter는 [Drain §5](drain-and-rolling-update.md#5-will-판정과-관측)의 6개 reason만 허용한다. 모든 mode에서 6개 series를 0부터 항상 같은 순서로 render한다. 알 수 없는 reason은 무시하며 임의 label을 추가하지 않는다.
`narumq_audit_events_total{outcome}`은 `published`, `publish_failed`, `invalid`, `denial_suppressed`, `mutation_rejected`, `dataplane_suppressed`, `dataplane_dropped`의 7개 outcome을 허용한다. 모든 format과 mode에서 7개 series를 0부터 고정 순서로 render한다. `dataplane_suppressed`는 초당 rate limit 초과를, `dataplane_dropped`는 queue 포화·종료 뒤 event를 나타낸다. 알 수 없는 outcome은 무시한다. `narumq_audit_publisher_ready` gauge는 node-local publisher가 append할 수 있으면 1, 아니면 0이다. 이 Metric에는 tenant, request ID, actor, action 또는 target label을 추가하지 않는다.

`narumq_mqtt_listener_admission_rejected_total{transport,reason}` counter는 CONNECT 전에
listener 한도로 닫은 socket을 센다. `transport`는 `tcp`, `tls`, `ws`, `wss`만 허용한다.
`reason`은 `max_connections`, `connection_rate`만 허용한다. 모든 mode에서 8개
series를 0부터 같은 순서로 render한다. Transport 순서는 `tcp`, `tls`, `ws`, `wss`이며 각
transport 안의 reason 순서는 `max_connections`, `connection_rate`다. 알 수 없는
값은 무시한다.

`narumq_mqtt_connect_rejected_total{protocol,reason}`은 broker가 CONNECT를 거절한 횟수를 protocol·고정 reason별로 센다. `protocol="3.1.0"`은 `unsupported_protocol_version` 한 series만 출력한다. `MQTT_3_1_1`과 `MQTT_5_0`은 각각 다음 8개 reason을 출력한다: `unsupported_protocol_version`, `identifier_rejected`, `bad_credentials`, `not_authorized`, `server_unavailable`, `quota_exceeded`, `banned`, `other`. Registry는 총 17개 series를 0부터 같은 순서로 출력하고 알 수 없는 protocol·reason 조합은 무시한다. MQTT 5 CONNACK reason code는 `0x84`→`unsupported_protocol_version`, `0x85`→`identifier_rejected`, `0x86`→`bad_credentials`, `0x87`→`not_authorized`, `0x88`·`0x89`→`server_unavailable`, `0x8A`→`banned`, `0x97`→`quota_exceeded`이며 나머지는 `other`다. MQTT 3.1.1 return code는 `1`→`unsupported_protocol_version`, `2`→`identifier_rejected`, `3`→`server_unavailable`, `4`·`5`→`not_authorized`이며 나머지는 `other`다. MQTT 5의 `banned`는 blocklist에 일치하는 CONNECT를 나타낸다.

`narumq_messages_dropped_total{reason}`은 routing 뒤 subscriber 한 명에게 전달할 메시지를 broker가 버린 횟수다. 고정 reason은 `queue_full`과 `packet_too_large`다. `queue_full`은 QoS 0 non-durable 후보가 모두 queue full로 끝난 경우에만 증가한다. `packet_too_large`는 subscriber의 Maximum Packet Size보다 큰 전달을 폐기할 때 증가한다. Message expiry는 publisher가 요청한 수명 만료이므로 이 counter에 넣지 않는다. Durable target 포화는 기존 `narumq_durable_delivery_saturated_targets_total`로 센다.

`narumq_node_drain_active`와 `narumq_node_drain_progress{quantity}`는 이 node가 적용한 drain operation의 committed progress를 나타낸다. `quantity`는 `connections_initial`, `connections_remaining`, `sessions_ready_for_takeover`, `sessions_taken_over`, `sessions_failed`, `pending_wal`, `under_replicated`다. Process 재시작 뒤에는 다음 local drain 명령 적용 전까지 모든 값이 0이다. Drain 속도와 ETA는 이 gauge의 Prometheus 추이에서 계산한다.

기본 label에 원문 `clientId`, username, IP, 임의 topic, request ID와 token claim을 넣어서는 안 된다. 제한된 exact topic Metric은 P1 opt-in resource로 제공하며 tenant, 최대 등록 수와 TTL을 적용해야 한다.

### 7.1 Metric별 node·cluster 집계

각 node의 `/metrics`는 해당 node 값만 출력한다. Prometheus는 scrape target의 `node_id` label로 node를 구분하고 아래 catalog에 따라 time-series를 집계한다. Management API snapshot mode도 같은 descriptor catalog를 사용하되 reset-safe stateless aggregation은 §7.2 규칙을 따른다. 새 Metric family를 추가하면 아래 표에도 행을 추가해야 한다.

| 값 | 구분 | 뜻 |
| --- | --- | --- |
| `node` | 범위 | 이 node가 직접 관찰하거나 처리한 값이다. 다른 node의 값과 겹치지 않는다. |
| `replicated` | 범위 | 복제된 committed 상태에서 만든 값이다. 모든 node가 같은 cluster 값을 보고한다. 적용이 늦은 node는 잠시 다른 값을 보고할 수 있다. |
| `leader` | 범위 | Raft leader만 series를 출력한다. Follower는 series를 출력하지 않는다. |
| `increase_sum` | 집계, counter | node별 `rate`·`increase`를 먼저 구한 뒤 합산한다. Process를 재시작해 값이 0이 되면 `rate`·`increase`가 이를 reset으로 처리한다. 예: `sum by (reason) (increase(narumq_will_outcomes_total[5m]))` |
| `sum` | 집계, gauge | node 값을 합산한다. 예: `sum(narumq_connections_active)` |
| `max` | 집계, gauge | 가장 큰 node 값을 쓴다. `replicated` 값은 합산하면 node 수만큼 커진다. 대기 시간은 최악 값을 봐야 한다. 예: `max(narumq_retained_messages)`, `max by (peer) (narumq_raft_peer_replication_lag_entries)` |
| `per_node` | 집계, gauge | 합산하지 않는다. Node별로 표시하거나 경보한다. 예: `narumq_audit_publisher_ready == 0` |
| `bucket_sum` | 집계, histogram | 같은 `le` bucket의 node별 `rate`를 합산한 뒤 quantile을 계산한다. Node별 quantile의 평균은 쓰지 않는다. 예: `histogram_quantile(0.99, sum by (le) (rate(narumq_publish_latency_seconds_bucket[5m])))` |

시험은 다음 조합 규칙을 강제한다.

- Counter 이름은 `_total`로 끝나야 한다. 범위는 `node`, 집계는 `increase_sum`이어야 한다.
- Histogram 범위는 `node`, 집계는 `bucket_sum`이어야 한다. 이름은 `_total`로 끝나면 안 된다. Label 열에 `le`를 적지 않는다.
- Gauge 집계는 `sum`, `max`, `per_node` 중 하나여야 한다. 이름은 `_total`로 끝나면 안 된다.
- `sum`은 범위가 `node`일 때만 쓴다.
- 범위가 `leader`이면 집계는 `max`여야 한다.

| Metric | Type | Label | 범위 | Cluster 집계 |
| --- | --- | --- | --- | --- |
| `narumq_connections_active` | `gauge` | - | `node` | `sum` |
| `narumq_connections_total` | `counter` | `protocol`, `transport`, `result` | `node` | `increase_sum` |
| `narumq_sessions_active` | `gauge` | `persistent` | `node` | `sum` |
| `narumq_subscriptions_active` | `gauge` | - | `replicated` | `max` |
| `narumq_retained_messages` | `gauge` | - | `replicated` | `max` |
| `narumq_retained_bytes` | `gauge` | - | `replicated` | `max` |
| `narumq_packets_received_total` | `counter` | `packet_type` | `node` | `increase_sum` |
| `narumq_packets_sent_total` | `counter` | `packet_type` | `node` | `increase_sum` |
| `narumq_auth_attempts_total` | `counter` | `method`, `result`, `reason` | `node` | `increase_sum` |
| `narumq_mqtt_connect_rejected_total` | `counter` | `protocol`, `reason` | `node` | `increase_sum` |
| `narumq_node_drain_transitions_total` | `counter` | `phase` | `node` | `increase_sum` |
| `narumq_will_outcomes_total` | `counter` | `reason` | `node` | `increase_sum` |
| `narumq_audit_events_total` | `counter` | `outcome` | `node` | `increase_sum` |
| `narumq_audit_publisher_ready` | `gauge` | - | `node` | `per_node` |
| `narumq_mqtt_listener_admission_rejected_total` | `counter` | `transport`, `reason` | `node` | `increase_sum` |
| `narumq_messages_dropped_total` | `counter` | `reason` | `node` | `increase_sum` |
| `narumq_node_drain_active` | `gauge` | - | `node` | `per_node` |
| `narumq_node_drain_progress` | `gauge` | `quantity` | `node` | `per_node` |
| `narumq_tenant_quota_profile_committed` | `gauge` | - | `replicated` | `per_node` |
| `narumq_tenant_quota_profile_version` | `gauge` | - | `replicated` | `per_node` |
| `narumq_tenant_quota_config_mismatch` | `gauge` | - | `node` | `per_node` |
| `narumq_cluster_policy_committed` | `gauge` | - | `replicated` | `per_node` |
| `narumq_cluster_policy_version` | `gauge` | - | `replicated` | `per_node` |
| `narumq_cluster_policy_config_mismatch` | `gauge` | - | `node` | `per_node` |
| `narumq_tenant_quota_rejections_total` | `counter` | `tenant`, `dimension` | `node` | `increase_sum` |
| `narumq_tenant_limit_rejections_total` | `counter` | `tenant`, `dimension` | `node` | `increase_sum` |
| `narumq_publish_backpressure_rejections_total` | `counter` | `reason` | `node` | `increase_sum` |
| `narumq_publish_backpressure_active` | `gauge` | `reason` | `node` | `per_node` |
| `narumq_storage_free_bytes` | `gauge` | - | `node` | `per_node` |
| `narumq_cluster_commit_backlog_oldest_seconds` | `gauge` | - | `node` | `max` |
| `narumq_durable_delivery_saturated_targets_total` | `counter` | - | `node` | `increase_sum` |
| `narumq_raft_peer_replication_lag_entries` | `gauge` | `peer` | `leader` | `max` |
| `narumq_publish_latency_seconds` | `histogram` | - | `node` | `bucket_sum` |
| `narumq_raft_wal_sync_duration_seconds` | `histogram` | - | `node` | `bucket_sum` |
| `narumq_raft_wal_sync_entries` | `histogram` | - | `node` | `bucket_sum` |
| `narumq_raft_wal_written_bytes_total` | `counter` | - | `node` | `increase_sum` |
| `narumq_raft_stage_duration_seconds` | `histogram` | `stage` | `node` | `bucket_sum` |

`narumq_subscriptions_active`는 scrape할 때 이 node의 route table에서 shared·non-shared 구독 수를 읽는다. Cluster mode의 route table은 committed 구독 전체의 projection이다. 그래서 다른 node의 SUBSCRIBE·UNSUBSCRIBE와 session 제거도 이 값에 반영된다.

Standalone mode는 node가 하나뿐이므로 `replicated`와 `node`의 결과가 같다.

### 7.2 Snapshot API와 cluster mode

`GET /api/v1/nodes/{nodeId}/metrics`는 대상 Registry의 typed snapshot을 반환한다. `NodeMetricsData`는 `nodeId`, `queryStatus`, nullable `queryErrorCode`, nullable `observedAt`, nullable `startedAt`, nullable `families`를 포함한다. `OK`에서 empty snapshot은 `[]`다. 다른 status에서는 두 시각과 `families`가 null이다. `startedAt`은 Registry instance 시작 경계이며 OS process 생성 시각이라고 보장하지 않는다.

`GET /api/v1/metrics`는 `mode=node`, `all_nodes_unaggregated`, `all_nodes_aggregated`를 받는다. 기본값은 `all_nodes_unaggregated`다. `MetricCollectionData`는 요청 mode, 전체 collection 완료 시각, `complete`, node별 metadata와 family를 반환한다. Empty arrays는 `[]`다. `node`는 local Registry fast path를 사용하며 다른 node에 RPC를 보내거나 readiness를 검사하지 않는다.

Node mode에서 metric label은 catalog 그대로 유지한다. Unaggregated cluster mode는 모든 sample에 `node_id` label을 추가하며 기존 label을 보존한다. `labelNames`에도 추가 label을 반영한다. Aggregated mode는 동일 원래 label identity별 gauge `sum` 또는 `max`를 적용하고 `per_node`는 `node_id`를 유지한다. Leader-scoped lag는 실제 반환 sample의 동일 peer별 max만 계산한다. Counter/`increase_sum`와 histogram/`bucket_sum`은 원시 누적값을 합치지 않고 `node_id`를 유지한다. Consumer는 node별 counter rate 또는 histogram bucket rate를 계산한 뒤 합산한다. Restart metadata가 rate 계산의 경계다.

Family와 sample은 이름·label 기준으로 정렬한다. Label 이름 중복, catalog와 다른 descriptor, 잘못된 family·bucket schema, non-finite sample, integer overflow, 서로 다른 descriptor는 aggregation failure다. Missing sample을 0으로 채우지 않는다. Snapshot에는 최대 64 family, 16,384 sample, sample당 8 label, label value당 1,024 UTF-8 byte를 허용한다. 각 node snapshot은 최대 8 MiB, 전체 JSON/text 결과는 최대 64 MiB다. 한도를 넘으면 결과를 자르지 않고 오류를 반환한다.

전체 collection은 시작 시 membership을 한 번 복사하고 최대 8 worker로 대상 node를 조회한다. 요청 전체 기한은 5초이며 caller deadline이 더 짧으면 이를 따른다. 자동 재시도와 주기 cache는 없다. 취소 시 진행 중 조회를 취소하고 worker를 모두 종료한다. Raw transport error·주소·인증 정보는 외부 오류에 노출하지 않는다.

| Query status 또는 오류 | 관찰 동작 |
| --- | --- |
| `OK` | 대상이 직접 측정한 snapshot과 `observedAt` 반환 |
| `UNREACHABLE` / `node_unreachable`, `node_query_timeout`, `node_not_running` | 측정값 없음; Node API는 상태 metadata를 반환 |
| `UNSUPPORTED` / `node_query_unsupported` | capability가 없는 peer; RPC 전송 안 함 |
| `RETIRED` | 측정값 없음; 오류 code 없음 |
| `ERROR` / `node_query_busy`, `node_query_invalid_response`, `node_snapshot_too_large`, `node_query_failed` | 정제한 실패 code와 함께 미확인 처리 |
| `metrics_collection_incomplete` | Aggregated JSON 또는 cluster text의 하나 이상 대상 실패; retryable 503 |
| `metrics_snapshot_too_large`, `metrics_aggregation_failed` | 64 MiB 한도 또는 schema·reduction 실패; retryable 503 |


## 8. Dashboard 계약

Dashboard는 [ADR-0045](../decisions/ADR-0045-operations-dashboard-grafana.md)의 Grafana OSS와 Prometheus를 사용해야 한다. 정의 파일은 `deploy/observability/grafana/dashboards/narumq-operations.json`이며 datasource UID는 `narumq-prometheus`와 `narumq-management`다. Prometheus scrape target은 `node_id` label에 Management API의 `nodeId`와 같은 문자열을 사용한다. `/metrics` 자체에는 node label을 넣지 않는다. 현재 query는 `cluster_id`로 데이터를 필터링하지 않으므로 Prometheus datasource 하나는 NaruMQ cluster 하나만 scrape해야 한다. Dashboard는 `node`(Prometheus `node_id`), `drain_node`(단일 node ID), `rebalance_max_deviation`(기본값 `100`) 변수를 제공해야 한다.
`node`는 multi-select이며 기본값은 All이고 값은 `label_values(narumq_connections_active, node_id)`에서 읽는다. `drain_node`는 단일 선택이며 `label_values(narumq_node_drain_active, node_id)`를 사용한다. `rebalance_max_deviation`은 기본값 `100`인 입력 변수다.

Dashboard는 외부 시계열 원본인 Prometheus와 snapshot용 Management API를 함께 읽는다. Infinity datasource query는 GET만 사용하고 `ReadOnly` API key를 `X-API-Key` header로 보낸다. Grafana database에는 Dashboard 정의만 둔다. Dashboard는 자체 시계열을 저장해서는 안 된다.

첫 화면의 `클러스터 상태` 행은 `quorumAvailable=false`를 빨간색 `위험`으로 표시해야 한다. Grafana Stat에는 boolean을 0/1로 변환한 뒤 색상 mapping을 적용한다. `underReplicated`가 1 이상이면 빨간색으로 표시해야 한다. Drain 기록이 없는 `/api/v1/nodes/{nodeId}/drain`은 API의 `404`를 표시한다.

| Panel ID | 행 | 제목 | PRD | 유형 | 데이터 원본과 query |
| ---: | --- | --- | --- | --- | --- |
| 1 | 클러스터 상태 | Quorum | UI-02 | stat | Management API `GET /api/v1/cluster`, `quorumAvailable`; Grafana에서 boolean을 0/1로 변환하고 `false`/0은 빨간색 `위험`, `true`/1은 초록색 `정상` |
| 2 | 클러스터 상태 | Replica 부족 | UI-02 | stat | 같은 API의 `underReplicated`; 0은 초록색, 1 이상은 빨간색 |
| 3 | 클러스터 상태 | Leader | UI-02 | stat | 같은 API의 decimal string `leaderNodeId`; Grafana에서 숫자로 표시하고 `null`은 빨간색 `없음` |
| 4 | 클러스터 상태 | Member·Quorum | UI-02 | stat | 같은 API의 `memberCount`, `quorumSize`, `replicationFactor`, `writeQuorum` |
| 5 | 클러스터 상태 | Node 상태 수 | UI-01 | stat | 같은 API의 `nodesByState.ready`, `joining`, `draining`, `safeToStop`, `unknown`, `nodeQueryComplete`; `nodeQueryComplete=true`는 초록 `완료`, false는 주황 `일부 미확인` |
| 6 | 클러스터 상태 | Node topology | UI-01 | table | Management API `GET /api/v1/nodes?limit=1000`; `nodeId`, `membershipPhase`, `queryStatus`, `queryErrorCode`, `observedAt`와 nullable 측정 field. Query status와 null을 아래 DASH-01 mapping으로 표시한다 |
| 7 | Traffic | 연결·session·subscription | UI-03 | timeseries | Prometheus: `sum(narumq_connections_active{node_id=~"$node"})`, `sum(narumq_sessions_active{node_id=~"$node"})`, `max(narumq_subscriptions_active{node_id=~"$node"})` |
| 8 | Traffic | Message rate | UI-03 | timeseries | Prometheus: 수신 `sum(rate(narumq_packets_received_total{packet_type="PUBLISH",node_id=~"$node"}[$__rate_interval]))`; 송신은 `narumq_packets_sent_total`로 같은 식 |
| 9 | Traffic | Protocol별 CONNECT 성공 | UI-03 | timeseries | Prometheus: `sum by (protocol) (rate(narumq_connections_total{result="success",node_id=~"$node"}[$__rate_interval]))` |
| 10 | Traffic | Protocol별 CONNECT 거절 | UI-03 | timeseries | Prometheus: `sum by (protocol, reason) (rate(narumq_mqtt_connect_rejected_total{node_id=~"$node"}[$__rate_interval]))` |
| 11 | Traffic | Drop·거절 | UI-03 | timeseries | Prometheus: `narumq_messages_dropped_total` by `reason`, `narumq_publish_backpressure_rejections_total` by `reason`, `narumq_tenant_limit_rejections_total` 및 `narumq_tenant_quota_rejections_total` by `dimension`, `narumq_durable_delivery_saturated_targets_total`, `narumq_mqtt_listener_admission_rejected_total` by `reason`; 모든 query는 `node_id=~"$node"`와 `[$__rate_interval]`을 사용 |
| 12 | Node 부하 | Node별 연결 편차 | UI-04 | table | Prometheus instant query는 `connections_active`, `deviation`, `rebalance_needed`라는 고정 `metric` label을 만든다. `deviation`은 node 연결 수에서 node 선택 범위의 평균을 올림한 값을 뺀다. `rebalance_needed`는 `deviation > bool $rebalance_max_deviation`이며 1은 빨간색 `필요`, 0은 초록색 `정상`이다. Grafana는 `node_id` 행과 metric별 열로 matrix를 만들고 deviation 내림차순으로 node를 정렬한다. |
| 13 | Node 부하 | Node별 연결 추이 | UI-04 | timeseries | Prometheus: `sum by (node_id) (narumq_connections_active)` |
| 14 | Drain | Drain 진행 | UI-05 | table | Prometheus instant query는 `drain_active`, `connections_initial`, `connections_remaining`, `sessions_taken_over`, `sessions_failed`, `connections_per_second`, `eta_seconds`, `takeovers_per_second` label을 만든다. 연결·session 값은 `narumq_node_drain_progress`에서 읽는다. 연결 감소 속도는 2분 `deriv`으로 계산하고 음수 결과는 0으로 제한한다. ETA는 연결 감소 속도가 양수일 때만 제공한다. Grafana는 `node_id` 행과 metric별 열로 matrix를 만든다. |
| 15 | Drain | Drain 상세 (${drain_node}) | UI-05 | table | Management API `GET /api/v1/nodes/${drain_node}/drain`; `phase`, `state`, `safeToStop`, `startedAt`, `deadlineAt`, `connections.remaining`, `sessions.takenOver`, `sessions.failed`, `failure.code`, `failure.blockingReason`. Drain 기록이 없으면 panel은 `No data`를 보이고 `Panel status`에서 `404 Not Found`를 확인할 수 있다. |

### DASH-01: Node topology and readiness

UI-01은 panel 5와 6에서 다룬다. 세 node가 정상 조회되면 실제 상태와 함께 Ready=3, Joining=0, Unknown=0, `nodeQueryComplete=true`를 표시한다. Raft quorum은 있지만 대상 readiness proof가 실패하면 `queryStatus=OK`, `ready=false`인 실제 not-ready 상태다. 조회 불가 시 해당 row는 `UNREACHABLE`과 null measurement를 표시하고 cluster `unknown` 수를 올리며 `nodeQueryComplete=false`를 표시한다. Dashboard는 null을 0 또는 `JOINING`으로 바꾸지 않는다.
Panel 6은 사용자가 readiness 원인을 먼저 볼 수 있도록 `Node`, `State`, `Ready`, `Readiness reason`, `Query status`, `Query error`, `Membership`, `Role`, `Replication` 순서로 표시한다. 나머지 snapshot field는 그 뒤에 둔다. Override matcher는 selector가 아니라 실제 column `State`, `Ready`, `Replication`, `Query status`를 사용한다. State `READY`는 초록, `JOINING`은 노랑, `DRAINING`은 주황, `SAFE_TO_STOP`은 파랑, null은 회색 `미확인`이다. Ready true는 초록 `READY`, false는 빨강 `NOT READY`, null은 회색 `미확인`이다. Replication `HEALTHY`는 초록, `DEGRADED`·`FAILED`는 빨강, null은 회색 `미확인`이다. Query status `OK`는 초록 `조회 성공`, `UNREACHABLE`은 주황 `조회 불가`, `UNSUPPORTED`는 회색 `조회 미지원`, `RETIRED`는 회색 `퇴역`, `ERROR`는 주황 `조회 오류`다. Null 숫자는 `—`로 표시하며 0으로 변환하지 않는다.

### DASH-02: Quorum and replication

UI-02는 panel 1–4에서 다룬다. 정상인 3-node cluster는 quorum 사용 가능과 `underReplicated=0`을 반환한다. Quorum이 false이거나 replica 부족 값이 0보다 크면 빨간색으로 표시한다.

### DASH-03: Traffic and protocol events

UI-03은 panel 7–11에서 다룬다. `node` 변수로 모든 target node 또는 특정 node를 선택한다. Prometheus series에서 PUBLISH rate, protocol별 CONNECT 성공·거절, 거절 사유와 drop rate를 표시한다.

### DASH-04: Node load balance

UI-04는 panel 12와 13에서 다룬다. Panel 12는 node마다 한 행을 표시하고 연결 편차가 큰 순서로 정렬한다. 설정한 rebalance 기준을 넘으면 필요 상태를 표시한다. Panel 13은 node별 연결 추이를 표시한다.

### DASH-05: Drain progress and safe stop

UI-05는 panel 14와 15에서 다룬다. Panel 14는 초기·잔여 연결 수, takeover 수, 속도와 조건을 만족할 때 ETA를 표시한다. Panel 15는 API가 반환한 drain 단계와 `safeToStop`을 표시한다.

### DASH-06: External time-series storage

UI-10은 모든 panel에 적용한다. Prometheus가 시계열을 저장한다. Grafana는 Dashboard 정의를 저장하며 시계열 저장소 역할을 하지 않는다.

Panel 14의 속도 단위는 연결/초와 takeover/초이며 ETA 단위는 초다. Panel 12의 `rebalance_max_deviation`은 운영자가 rebalance API의 `maxDeviationConnections`와 같은 값으로 설정한다. Client/session 상세, disconnect/redirect와 retained 조회 화면은 관련 API가 Active contract에 추가되기 전까지 제공하지 않는다. 실제 설정 순서는 [Operations Dashboard 운영 절차](../how-to/operations-dashboard.md)를 따른다.

## 9. 수용 시험

테스트 연결과 결과는 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/management-api-and-observability.md)에 있다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| MGT-SCN-001 | API-02 | OpenAPI 3.1 문서를 읽고 등록된 operation 전체를 runtime route와 handler 응답에 대조한다. | 문서가 유효하다. operation 집합이 runtime route와 일치한다. 성공 응답은 해당 operation의 response schema와 일치한다. |
| MGT-SCN-002 | API-05 | 목록 endpoint에 `limit` `1`·`1000`·`1001`, 중복·선언되지 않은 query, 빈 값을 보낸다. client 검색 filter(`tenantId`, `clientIdPrefix`, `nodeId`, `protocol`)를 쓰고, `tenantId` 없이 `subscriptions`와 `retained`를 요청한다. 없는 client 상세도 요청한다. | `1`과 `1000`은 성공한다. `1001`, 중복·알 수 없는 query, 빈 값, 범위를 벗어난 `limit`, 필수 `tenantId` 누락은 `400 invalid_request`다. 허용 filter는 성공한다. 없는 client 상세는 `404`다. |
| MGT-SCN-003 | API-05, SEC-10 | 익명, 잘못된 credential, Bearer token, ReadOnly API key로 `/api/v1`을 요청한다. ReadOnly credential로 ClusterAdmin 권한이 필요한 endpoint도 요청한다. | credential이 없거나 틀리면 `401`이다. Bearer token과 ReadOnly API key는 조회에 성공한다. 권한이 부족하면 `403`이다. Health probe는 listener 정책에 따라 익명으로 접근한다. |
| MGT-SCN-004 | API-02, SEC-10 | cluster feature를 조회하고 활성화한다. 미지원 feature, 잘못된 feature 이름, ReadOnly role의 POST도 요청한다. | GET 응답은 `data.activeFeatures`와 `data.members`를 포함한다. 각 member는 decimal string `nodeId`, canonical `releaseVersion`, 정렬된 `features`를 가진다. 빈 collection은 JSON `[]`다. 활성화 POST는 `200`, 미지원 feature는 non-retryable `409`, 잘못된 feature는 `400`, ReadOnly role의 POST는 `403`이다. |
| MGT-SCN-005 | API-05 | 서로 다른 principal과 같은 host의 인증 실패 요청이 각각 한도를 넘게 요청한다. 시간을 진행하거나 되돌린다. `RemoteAddr`에 host와 port를 함께 둔다. | principal마다, 인증 실패 host마다 독립된 bucket을 쓴다. 한도를 넘으면 거절하고 시간이 지나면 token을 보충한다. clock 역행은 token을 늘리지 않는다. bucket key는 `RemoteAddr`에서 port를 뺀 host다. |
| MGT-SCN-006 | API-05 | 한도를 소진한 뒤 health probe와 `/metrics`를 요청한다. | 이 요청들은 제한하지 않는다. |
| MGT-SCN-007 | API-05 | 권한이 부족한 인증 성공 요청으로 principal token을 소진한다. | `403` 요청도 principal token을 소비한다. 한도를 넘으면 `429`를 반환한다. |
| MGT-SCN-008 | API-05, SEC-11 | Audit producer가 구성된 node에서 한도를 넘긴 mutation 요청과 인증 실패 요청을 보낸다. | 거절된 mutation은 handler를 실행하지 않고 `AdmitMutation`을 호출하지 않는다. 거절은 `management.request` event(result `DENIED`, reason `rate_limited`)로 기록한다. 인증 실패 거절의 subject는 `unauthenticated`이며 roles를 생략한다. |
| MGT-SCN-009 | API-05 | 서로 다른 key 4096개를 채운 뒤 새 key와 기존 key를 요청한다. 시간을 진행한 뒤 새 key를 다시 요청한다. | 상한에서 새 key는 거절하고 기존 key는 계속 처리한다. 시간이 지나 가득 찬 bucket을 제거하면 새 key를 받는다. |
| MGT-SCN-010 | API-05 | OpenAPI에 정의된 operation의 `429` 응답을 contract 시험으로 확인한다. | 한도 초과는 `429`, error code `rate_limited`, message `management API rate limit exceeded`, `retryable=true`, `Retry-After: 1`이며 OpenAPI와 일치한다. |
| MGT-SCN-011 | API-05 | Standalone `-http-request-rate`·`-http-auth-failure-rate`와 cluster `management.requestRate`·`management.authFailureRate`에 경계값과 범위를 벗어난 값을 설정한다. | 경계값은 받는다. 범위를 벗어난 standalone 값은 exit 1과 `-http-request-rate and -http-auth-failure-rate must be between 1 and 1000000` 오류로 거절한다. |
| MGT-SCN-012 | SEC-11, API-08 | Dynamic cluster의 활성 management mutation(drain 성공, 실패, backup, staged member, panic)을 실행한다. 조회 요청도 보낸다. | 모든 mutation은 감사 event를 기록하고 결과 status를 포함한다. 조회는 mutation admission을 요청하지 않는다. 3-node process에서도 같은 기록을 확인한다. |
| MGT-SCN-013 | SEC-11, NFR-17 | 401·403으로 거절한 mutation과 audit publisher를 쓸 수 없는 상태의 mutation을 요청한다. | 401·403 거절은 audit event로 남는다. Publisher를 쓸 수 없으면 handler를 실행하지 않고 retryable `503 audit_unavailable`을 반환한다. |
| MGT-SCN-014 | SEC-11, NFR-17 | Audit 파일의 hash-chain 손상, 끝부분 손상, 중간 누락, rotation, 가장 오래된 rotation 삭제, directory 경계와 Windows rotation 충돌을 만든다. read-only 검증 명령을 실행한다. | 손상과 누락은 검출하고 publisher는 사용 불가가 된다. 삭제된 가장 오래된 rotation 뒤의 chain은 검증한다. 검증 명령은 파일을 변경하지 않는다. Windows에서 기존 대상 파일은 덮어쓰지 않는다. |
| MGT-SCN-015 | SEC-11 | 유효한 event, 잘못된 event, schema 경계값 event를 audit에 기록한다. | 유효한 event는 schema대로 저장하고 chain을 이어 간다. 잘못된 event는 발행하지 않는다. 저장한 event는 audit event v1 schema를 만족한다. |
| MGT-SCN-016 | SEC-10 | `management.apiKeys`와 `management.oidc` 설정을 정상값과 잘못된 값으로 읽는다. credential 파일을 읽는다. | 파일의 앞뒤 공백을 제거한다. 읽지 못하는 파일, 비어 있는 key, cluster admin token 또는 다른 API key와 같은 key는 process 시작을 거절한다. |
| MGT-SCN-017 | SEC-10 | break-glass token, API key, OIDC subject로 role별 permission을 요청한다. 정적 binding이 없는 OIDC subject도 요청한다. | Permission matrix가 네 role을 구분한다. Break-glass token은 ClusterAdmin이다. 정적 binding이 없는 OIDC subject는 `401`, 권한 밖 permission은 `403`이다. API key principal subject는 `apikey:<id>`이고 audit actor에 role을 기록한다. |
| MGT-SCN-018 | SEC-10 | OIDC JWT를 정상 claim, 잘못된 issuer·audience·algorithm·signature로 제출한다. | 검증한 claim으로 정적 subject→roles binding을 적용한다. JWT claim은 roles를 선택하지 않는다. 검증 실패는 `401`이다. |
| MGT-SCN-019 | SEC-10, API-08 | SecurityAdmin·ClusterAdmin이 `/api/v1/nodes/{nodeId}/audit/events`를 조회한다. 저장 record가 여러 rotation 파일에 걸쳐 있다. 권한이 없는 role과 audit reader가 없는 node도 요청한다. | 저장한 hash-chain record를 최신순으로 반환하고 cursor로 이어 읽는다. 권한이 없으면 거절한다. Reader가 없으면 route가 없다. 응답은 OpenAPI와 일치한다. |
| MGT-SCN-020 | API-04 | Node를 `JOINING`, `READY`, `DRAINING`, `SAFE_TO_STOP` 순서로 바꾸며 `/health/live`, `/health/ready`, `/health/startup`, `/health/drain`을 요청한다. Raft storage가 실패하거나 느린 상태도 만든다. | startup은 `JOINING`에서 실패하고 `READY`에서 성공한다. readiness는 `DRAINING`·`SAFE_TO_STOP`에서 실패한다. drain-complete는 `SAFE_TO_STOP`에서 성공한다. terminal storage 실패는 liveness를 내린다. 느린 storage는 lifecycle state를 바꾸지 않고 readiness만 실패시킨다. |
| MGT-SCN-021 | API-04 | Typed Raft recovery 전과 후에 startup을 요청한다. Recovery 뒤 `JOINING`인 3-node cluster를 외부 `/health/ready` 호출 없이 시작한다. | Recovery 전에는 `STARTING`이다. Recovery 뒤 `JOINING`이면 readiness 조건을 직접 확인하고 충족하면 `READY`로 전환해 startup이 성공한다. |
| MGT-SCN-022 | UI-01 | Cluster 요약과 node 목록을 조회한다. Quorum은 있지만 대상 readiness proof가 실패한 node, 상태를 읽지 못한 node, 퇴역한 node를 포함한다. | `nodesByState`는 `joining`, `ready`, `draining`, `safeToStop`, `unknown`을 반환한다. `queryStatus=OK`이면 `ready=false`도 실제 측정값으로 보존한다. 읽지 못하거나 퇴역한 node는 측정값이 null이고 `unknown`은 retired를 제외하고 센다. Node 목록은 ID 오름차순이다. |
| MGT-SCN-023 | UI-01, API-03 | `/api/v1/metrics`와 `/metrics`를 mode `node`, `all_nodes_unaggregated`, `all_nodes_aggregated`와 잘못된 mode로 요청한다. 일부 node가 실패한 상태, snapshot descriptor가 잘못된 상태, 정수 overflow를 포함한다. | 기본 mode는 `all_nodes_unaggregated`다. `node` mode는 local snapshot만 쓴다. 부분 실패한 unaggregated JSON은 `200`, `complete=false`다. Aggregated JSON과 cluster text는 retryable `503 metrics_collection_incomplete`다. 잘못된 mode는 `400`이다. 잘못된 descriptor와 overflow는 aggregation failure다. Cluster text mode는 `ReadOnly` 인증을 요구하고 `/metrics?mode=node`는 익명 scrape를 유지한다. |
| MGT-SCN-024 | UI-01 | 정상 3-node cluster를 조회한 뒤 한 node를 강제 종료하고 node 목록, cluster 요약, metric API를 다시 조회한다. | 정상일 때 세 node를 모두 읽는다. 종료한 node는 `UNREACHABLE`과 null 측정값이고 cluster `unknown=1`이다. Unaggregated JSON은 부분 결과를 반환한다. Aggregated JSON과 cluster text는 `503 metrics_collection_incomplete`다. |
| MGT-SCN-025 | API-01 | `/metrics`를 Prometheus와 OpenMetrics `Accept`로 요청한다. | `text/plain; version=0.0.4; charset=utf-8` 또는 `application/openmetrics-text; version=1.0.0; charset=utf-8`을 반환한다. OpenMetrics 본문은 `# EOF`로 끝나고 Prometheus 본문에는 없다. |
| MGT-SCN-026 | API-06 | 원문 client ID, IP, topic, token, request ID를 label 값으로 넣어 Metric을 기록한다. | 출력에 원문 값이 없다. 알 수 없는 protocol·transport·packet type은 `UNKNOWN`·`unknown`으로 정규화한다. |
| MGT-SCN-027 | API-03 | Registry가 출력한 Metric family를 §7.1 표와 비교한다. `_bucket` 표본이 없는 histogram도 만든다. | 모든 family의 이름·type·label·범위·집계 규칙이 표와 같다. `_bucket` 표본이 없는 histogram은 거절한다. |
| MGT-SCN-028 | API-03 | 3-node cluster에서 SUBSCRIBE 2개, UNSUBSCRIBE 1개, `SessionExpiryInterval=0`인 MQTT 5 연결 종료를 차례로 수행한다. Route table에서 shared·non-shared 구독을 교체·해지·제거한다. | 모든 node의 `narumq_subscriptions_active`가 `0→2→1→0`이다. MQTT 5 `UNSUBACK`은 packet ID, properties 길이와 성공 reason `0x00`을 포함한다. Route table의 `SubscriptionCount()`는 `Snapshot()` 길이와 같다. |
| MGT-SCN-029 | UI-03 | MQTT 3.1.0·3.1.1·5.0 CONNECT를 거절시키고 모든 CONNACK reason code와 return code를 Metric label로 변환한다. | `narumq_mqtt_connect_rejected_total{protocol,reason}`이 protocol·고정 reason별로 증가한다. 17개 series를 0부터 고정 순서로 render하고 알 수 없는 조합은 무시한다. CONNACK mapping은 §7과 같다. |
| MGT-SCN-030 | UI-03 | Routing 뒤 QoS 0 non-durable 후보가 모두 queue full로 끝나게 하고, subscriber의 Maximum Packet Size를 넘는 전달을 만든다. | `queue_full`과 `packet_too_large` reason으로 `narumq_messages_dropped_total`이 증가한다. `queue_full`은 전달에 모두 실패했을 때만 증가한다. 실제 broker process의 Metric으로도 관측한다. |
| MGT-SCN-031 | UI-05 | Node drain을 시작해 완료하고 `narumq_node_drain_active`, `narumq_node_drain_progress{quantity}`, `narumq_node_drain_transitions_total{phase}`를 scrape한다. | Gauge는 committed progress를 나타낸다. Phase label은 8개 유효 phase만 허용한다. Process 재시작 뒤 다음 drain 명령 전까지 값은 0이다. |
| MGT-SCN-032 | - | WAL group fsync를 성공·실패시키고 `narumq_raft_wal_sync_duration_seconds`, `narumq_raft_wal_sync_entries`, `narumq_raft_wal_written_bytes_total`을 scrape한다. | Histogram 표본은 성공한 `File.Sync` 한 번이다. 실패한 write·sync와 값이 없어 생략된 요청은 관찰하지 않는다. 세 series는 label이 없고 observer가 없는 mode에서는 0이다. |
| MGT-SCN-033 | - | `narumq_raft_stage_duration_seconds{stage}`에 여러 stage의 시간을 기록하고 10초를 넘는 값도 기록한다. | Stage별 count·sum·bucket이 따로 쌓인다. 10초를 넘는 값은 `+Inf` bucket에만 들어간다. 기록이 없는 stage도 0으로 render한다. |
| MGT-SCN-034 | SEC-11 | `narumq_audit_events_total{outcome}`과 `narumq_audit_publisher_ready`를 모든 format과 mode에서 render한다. 알 수 없는 outcome을 기록한다. | 7개 outcome series를 0부터 고정 순서로 render한다. 알 수 없는 outcome은 무시한다. Publisher가 append할 수 있으면 gauge는 1이고 아니면 0이다. |
| MGT-SCN-035 | DEP-07, DEP-12 | `narumq_will_outcomes_total{reason}`을 모든 format과 mode에서 render한다. 알 수 없는 reason을 기록한다. | 6개 reason series를 0부터 고정 순서로 render한다. 알 수 없는 reason은 무시한다. |
| MGT-SCN-036 | MQTT-10 | CONNECT 전에 listener 한도를 넘는 연결을 `tcp`, `tls`, `ws`, `wss`에서 만든다. | `narumq_mqtt_listener_admission_rejected_total{transport,reason}`의 8개 series를 0부터 고정 순서로 render한다. 알 수 없는 값은 무시한다. |
| MGT-SCN-037 | TEN-06, TEN-07 | Tenant 연결·traffic 한도 거절을 여러 tenant와 허용 목록 밖 dimension으로 만든다. | `narumq_tenant_limit_rejections_total{tenant,dimension}`은 6개 dimension만 허용한다. `tenant` label은 저장 quota Metric과 처음 본 1,024개 집합을 공유하고 나머지는 `_overflow`로 합친다. |
| MGT-SCN-038 | TEN-08 | Tenant 저장 quota 거절과 profile 상태를 기록하고, 1,024개를 넘는 tenant를 만든다. | `narumq_tenant_quota_rejections_total`은 4개 dimension만 허용한다. `narumq_tenant_quota_profile_committed`, `narumq_tenant_quota_profile_version`, `narumq_tenant_quota_config_mismatch`가 상태를 나타낸다. 처음 본 1,024개 뒤 tenant는 `_overflow`다. |
| MGT-SCN-039 | API-09, TEN-08 | ClusterPolicy를 commit하고 local 설정과 다르게 둔다. 값을 초기화한다. | `narumq_cluster_policy_committed`, `narumq_cluster_policy_version`, `narumq_cluster_policy_config_mismatch`가 local 적용 상태를 나타낸다. 초기화하면 gauge도 초기값으로 돌아간다. 불일치는 한 번만 기록한다. |
| MGT-SCN-040 | UI-01, UI-02, UI-03, UI-04, UI-05, UI-10 | Dashboard provisioning 정의를 계약 문서·OpenAPI·Metric catalog와 대조한다. | 모든 panel의 데이터 원본, API GET path와 Metric family가 기준 문서와 일치한다. |
| MGT-SCN-041 | UI-01, UI-02, UI-03, UI-04, UI-05, UI-10 | Docker로 3-node cluster, Prometheus, Grafana를 띄우고 panel query를 실행한다. | 세 process가 ready다. Prometheus target 3개와 datasource 2개가 정상이다. Panel query가 §8의 값을 frame으로 반환한다. Drain 완료 상세와 node별 Metric이 선택한 node 범위로 나온다. |
| MGT-SCN-042 | UI-01, SEC-10 | Dashboard용 ReadOnly API key로 `/metrics`와 `/api/v1/cluster`를 GET하고 backup POST를 요청한다. ClusterAdmin token으로도 backup POST를 요청한다. | ReadOnly GET은 `200`이다. ReadOnly backup POST는 `403`이며 backup을 만들지 않는다. ClusterAdmin의 backup POST는 `200`이다. |
| MGT-SCN-043 | UI-01, UI-02, UI-03, UI-04, UI-05, UI-10 | EKS `narumq-eks`에서 Prometheus target, Grafana datasource, topology와 시계열 panel을 확인한다. | Prometheus target이 `UP`이고 datasource가 `OK`다. 실제 topology와 panel 7 시계열 frame이 표시된다. |
| MGT-SCN-044 | API-03 | Snapshot이 64 family, 16,384 sample, sample당 8 label, label value당 1,024 byte, node당 8 MiB, 전체 64 MiB 한도를 넘거나 collection이 5초를 넘긴다. 요청을 취소한다. | 한도를 넘으면 결과를 자르지 않고 오류를 반환한다. Collection은 8 worker 이하로 조회하고 취소 시 모든 worker를 종료한다. |
| MGT-SCN-045 | API-04 | Dynamic cluster에서 PUBLISH local projection admission과 fresh read barrier를 통과한 readiness 거절을 만든다. | `cluster readiness rejected` 로그의 `purpose=publish`는 `fresh_proof_confirmed=false`다. 실제 fresh read barrier를 통과한 local snapshot 거절만 `true`다. Storage 검사나 fresh proof 단계의 거절은 snapshot flag와 index를 기록하지 않는다. |

API-09 `mqttLimits`와 node configuration 조회, TEN-08 ClusterPolicy quota의 시나리오는 [Configuration §6](configuration-and-runtime-options.md#6-수용-시험)의 `CFG-SCN-31`~`CFG-SCN-38`에 정의한다. 이 스펙은 같은 시나리오를 다시 정의하지 않는다.

Deferred mutation endpoint의 idempotency, audit와 dashboard aggregation·masking 시나리오는 해당 endpoint가 contract에 추가될 때 이 표에 추가한다.
