# Management API와 observability 스펙

이 문서는 operator, GitOps controller, Dashboard와 monitoring system이 NaruMQ를 조회·변경하는 HTTP·Metric 계약의 전체 목표를 정의한다. 현재 active profile은 [Management API v1 contract](../contracts/management-api-v1.md)와 [OpenAPI 3.1 contract](../contracts/management-api-v1.openapi.json)으로 확정하고, 다른 미래 catalog는 명시적으로 deferred scope로 표시한다.

| 항목 | 값 |
| --- | --- |
| 상태 | Active (Phase 1 query profile와 broker drain; 다른 mutation·OIDC·운영 자동화는 deferred) |
| 관련 요구사항 | API-01~API-09, UI-01~UI-10, SEC-10~SEC-11 |
| 근거 | [PRD 9.5절, 9.6절과 14장](../requirements/PRD.md) |
| 관련 spec | [Tenant와 security](tenancy-and-security.md), [Drain](drain-and-rolling-update.md) |
| 관련 Architecture | [System overview](../architectures/system-overview.md), [Security boundaries](../architectures/security-boundaries.md) |
| 관련 결정 | [ADR-0005](../decisions/ADR-0005-management-api-contract-format.md) |
| 관련 Contract | [Management API v1](../contracts/management-api-v1.md), [Management API OpenAPI](../contracts/management-api-v1.openapi.json), [Audit event v1](../contracts/audit-event-v1.md) |

## 1. HTTP 공통 계약

- 관리 API base path는 `/api/v1`이다. 호환되지 않는 변경은 새 major path에서 제공해야 한다.
- request와 response body는 `application/json` UTF-8을 사용한다.
- `/api/v1` 요청은 인증과 RBAC를 통과해야 한다. Health probe와 `/metrics`의 익명 노출 범위는 listener 정책으로 제한할 수 있다.
- 서버는 수신한 유효 request ID를 사용하거나 새 값을 생성해 `X-Request-Id`에 반환해야 한다.
- Phase 1의 일반 catalog은 조회 전용이며 broker node drain만 별도 active mutation으로 제공한다. Drain은 `Idempotency-Key`를 요구하고, 다른 mutation은 계속 deferred다.

### 1.1 Management listener와 인증 정책

- `-http-listen`은 Management HTTP listener의 bind 주소다.
- `-http-auth-token`이 설정되면 유효한 `Authorization: Bearer <token>`을 요구하고, `/api/v1`에는 endpoint별 RBAC를 적용한다. 인증 실패는 `401`, 인증은 됐지만 권한이 부족하면 `403`이다.
- `X-API-Key`는 machine-to-machine 조회 주체를 위한 opaque credential로 사용할 수 있으며, bearer token과 같은 RBAC 경계를 적용한다.
- bearer token과 API key는 opaque 값이다. Phase 1은 OIDC discovery, JWT claim 검증과 claim 기반 tenant scope를 약속하지 않는다.
- `-http-auth-token`이 비어 있으면 local development용 익명 모드로 동작하지만, `-http-listen`은 literal loopback IP(`127.0.0.0/8` 또는 `::1`)만 허용한다. `0.0.0.0`, `:8080`, 외부 인터페이스 주소는 거부한다.
- 따라서 원격 interface에 Management API를 노출하려면 반드시 `-http-auth-token` 또는 동등한 API key 정책을 설정해야 한다.
- CLI에서 health probe와 `/metrics`는 listener 정책에 따라 익명 노출할 수 있다. 운영 환경에서는 probe listener와 firewall 범위를 별도로 제한해야 한다.

예:

```text
# local development: loopback-only anonymous mode
go run ./cmd/narumq -http-listen 127.0.0.1:8080

# remote/private interface: authentication required
go run ./cmd/narumq -http-listen 0.0.0.0:8080 -http-auth-token "$NARUMQ_MANAGEMENT_TOKEN"
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

오류 code 전체 목록과 각 endpoint의 response status는 [Management API v1 OpenAPI](../contracts/management-api-v1.openapi.json)에서 확정한다. 내부 stack, storage key와 secret을 오류에 포함해서는 안 된다. Active drain은 `409` 상태를 사용하며, `422`와 `429`를 포함한 다른 mutation 동작은 deferred다.

### 1.4 Deferred mutation과 idempotency

Active broker drain endpoint는 `Idempotency-Key`를 요구한다. 같은 node에 active operation이 있으면 새 operation을 만들지 않고 committed operation을 돌려준다. 같은 key에 다른 parameter를 보내면 `409 idempotency_conflict`다. 다른 mutation endpoint는 아직 deferred다.

향후 mutation은 `Idempotency-Key`를 받아야 한다. 같은 principal, endpoint와 key에 같은 payload를 재시도하면 최초 결과와 의미가 같아야 한다. 같은 key에 다른 payload가 오면 `409 idempotency_conflict`를 반환해야 한다. Resource 변경은 가능한 경우 version/ETag 기반 조건부 요청을 지원해야 한다.

## 2. Endpoint catalog

### 2.1 Phase 1 active profile

현재 13개 GET operation과 broker drain POST/GET/DELETE가 구현·계약 대상이다. 정확한 parameter allowlist, schema, response와 operation ID는 OpenAPI가 SSOT이며 `tests/contracts/management-api-v1`가 runtime handler와 일치하는지 검사한다.

| Method | Path | 성공 동작 | 권한 |
| --- | --- | --- | --- |
| GET | `/health/live` | process가 deadlock 없이 응답 가능하면 `200` | probe policy |
| GET | `/health/ready` | 신규 MQTT·관리 traffic 수용 가능하면 `200` | probe policy |
| GET | `/health/startup` | 초기 동기화가 완료되면 `200` | probe policy |
| GET | `/health/drain` | node가 `SAFE_TO_STOP`이면 `200` | probe policy |
| GET | `/metrics` | Prometheus 또는 OpenMetrics scrape | monitoring policy |
| GET | `/api/v1/cluster` | quorum과 cluster 요약 조회 | ReadOnly |
| GET | `/api/v1/nodes` | state filter와 pagination을 사용한 node 목록 조회 | ReadOnly |
| POST | `/api/v1/nodes/{nodeId}/drain` | committed voter의 broker drain 시작 또는 현재 operation 재생 | Operator |
| GET | `/api/v1/nodes/{nodeId}/drain` | committed drain operation 조회 | ReadOnly |
| DELETE | `/api/v1/nodes/{nodeId}/drain` | cancellable drain operation 취소 | Operator |
| GET | `/api/v1/clients` | tenant·client prefix·node·protocol filter로 client 검색 | ReadOnly, tenant scope |
| GET | `/api/v1/clients/{tenantId}/{clientId}` | session 상세 조회 | ReadOnly, tenant scope |
| GET | `/api/v1/subscriptions` | required `tenantId` 범위의 subscription 조회 | ReadOnly, tenant scope |
| GET | `/api/v1/retained` | required `tenantId` 범위의 retained metadata 조회 | ReadOnly, tenant scope |
| GET | `/api/v1/tenants` | tenant 목록 조회 | ClusterAdmin |

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

## 4. Cluster·node representation

Cluster 요약은 최소 다음 정보를 제공해야 한다.

- cluster ID, release compatibility 범위와 current term/leader 식별 정보
- member 수, quorum 필요 수와 quorum 가능 여부
- READY/JOINING/DRAINING/SAFE_TO_STOP node 수
- replica 부족·replication lag와 storage pressure 요약
- 전체 connection, session, subscription과 message rate 요약

Node 상세는 `nodeId`, version, uptime, lifecycle state, readiness 원인, role, last applied index, resource pressure, connection 수와 drain operation을 포함해야 한다. Cluster mode에서 현재 node의 `replicationHealth`는 local Raft storage 상태를 나타내고, 원격 peer 행에서는 조회 node가 관찰한 peer 연결 상태를 나타내며 remote storage 상태를 주장하지 않는다. `lastErrorCode`는 원인별 안정된 code다. 원격 peer 행의 `replicationQueue`는 조회 node에서 해당 peer로 보내는 outbound message·byte·rejection 수를 제공한다. Local `DEGRADED`·`FAILED`는 lifecycle `state`를 변경하지 않고 readiness와 `storagePressure`에 반영하며, 내부 오류 원문을 노출해서는 안 된다. 정확한 field name과 schema는 OpenAPI에서 고정한다.

## 5. Filtering과 masking

- 목록 endpoint는 cursor pagination과 endpoint별 allowlist filter만 허용해야 한다.
- client 검색 Phase 1 filter는 `tenantId`, `clientIdPrefix`, `nodeId`, `protocol`이다. arbitrary regular expression, IP filter와 wildcard topic Metric 등록을 허용해서는 안 된다.
- `subscriptions`와 `retained` 목록은 `tenantId`를 required query로 받아 tenant 범위를 명시해야 한다.
- client identity, IP, inflight payload와 offline payload는 role에 따라 마스킹하거나 제외해야 한다.
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

최소 범주는 connection/session/subscription, message·packet·ACK, latency, cluster·replication, storage, auth·ACL, drain과 process resource다. `narumq_node_drain_transitions_total{phase}`는 committed node drain 상태를 phase별로 집계하며, label은 8개 유효 drain phase만 허용한다. 다른 Metric 이름과 histogram bucket은 `SPEC-TBD-07`에서 확정한다.

기본 label에 원문 `clientId`, username, IP, 임의 topic, request ID와 token claim을 넣어서는 안 된다. 제한된 exact topic Metric은 P1 opt-in resource로 제공하며 tenant, 최대 등록 수와 TTL을 적용해야 한다.

## 8. Dashboard 계약

Dashboard는 이 API와 외부 TSDB를 사용해야 한다. 장기 시계열 원본을 자체 저장해서는 안 된다.

첫 화면은 cluster topology, quorum 위험, replica 부족, node version/readiness/drain과 핵심 traffic 추이를 보여야 한다. client/session/subscription 화면은 Phase 1 query로 제공하고, redirect/disconnect와 retained·exact topic Metric 화면은 해당 API가 active contract에 추가된 뒤 제공한다.

UI mutation은 해당 API가 active contract에 추가될 때 같은 RBAC, 확인 절차, idempotency와 감사 계약을 따라야 한다.

## 9. 수용 시험

- `tests/contracts/management-api-v1`가 OpenAPI 문서 자체, 13개 operation set, representative success response와 Prometheus/OpenMetrics media type을 검증한다.
- 같은 contract suite가 pagination `1`/`1000` 경계, `1001` 초과, duplicate·unknown query와 subscriptions/retained의 required `tenantId`를 검증한다.
- 같은 contract suite가 anonymous, 잘못된 credential, ReadOnly bearer/API key 성공과 ClusterAdmin 권한 부족 `403`을 검증한다.
- startup, liveness, readiness와 drain-complete가 quorum·sync·drain 상태 조합을 정확히 반영하는지 시험한다.
- Cluster startup probe는 typed Raft recovery가 끝나기 전 `STARTING`, recovery 뒤 base lifecycle 상태를 반환해야 한다. `cmd/narumq/cluster_storage_health_test.go::TestClusterStartupWaitsForTypedRecovery`가 두 상태를 확인한다.
- Metric format, type, HELP/UNIT, histogram bucket과 금지 label을 자동 검사한다.
- deferred mutation endpoint의 idempotency, audit와 dashboard aggregation/masking 검증은 해당 endpoint가 contract에 추가될 때 이 목록에 연결한다.
