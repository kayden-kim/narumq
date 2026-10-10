# Management API와 observability 스펙

이 문서는 operator, GitOps controller, Dashboard와 monitoring system이 NaruMQ를 조회·변경하는 HTTP·Metric 계약의 전체 목표를 정의한다. 현재 active profile은 [Management API v1 contract](../contracts/management-api-v1.md)와 [OpenAPI 3.1 contract](../contracts/management-api-v1.openapi.json)으로 확정하고, 다른 미래 catalog는 명시적으로 deferred scope로 표시한다.

| 항목 | 값 |
| --- | --- |
| 상태 | Active (Phase 1 query profile, broker drain, cluster feature activation, management-plane audit, API rate limit, SEC-10 OIDC/API key RBAC and SEC-12 client blocklist; other mutation·운영 자동화는 deferred) |
| 관련 요구사항 | API-01~API-09, UI-01~UI-10, SEC-10~SEC-12 |
| 근거 | [PRD 9.5절, 9.6절과 14장](../requirements/PRD.md) |
| 관련 spec | [Tenant와 security](tenancy-and-security.md), [Drain](drain-and-rolling-update.md) |
| 관련 Architecture | [System overview](../architectures/system-overview.md), [Security boundaries](../architectures/security-boundaries.md) |
| 관련 결정 | [ADR-0005](../decisions/ADR-0005-management-api-contract-format.md), [ADR-0018](../decisions/ADR-0018-release-compatibility-window.md), [ADR-0043](../decisions/ADR-0043-management-api-rate-limit.md), [ADR-0045](../decisions/ADR-0045-operations-dashboard-grafana.md), [ADR-0048](../decisions/ADR-0048-management-api-oidc-api-key-rbac.md), [ADR-0050](../decisions/ADR-0050-mqtt-client-blocklist.md) |
| 관련 Contract | [Management API v1](../contracts/management-api-v1.md), [Management API OpenAPI](../contracts/management-api-v1.openapi.json), [Audit event v1](../contracts/audit-event-v1.md) |

## 1. HTTP 공통 계약

- 관리 API base path는 `/api/v1`이다. 호환되지 않는 변경은 새 major path에서 제공해야 한다.
- request와 response body는 `application/json` UTF-8을 사용한다.
- `/api/v1` 요청은 인증과 RBAC를 통과해야 한다. Health probe와 `/metrics`의 익명 노출 범위는 listener 정책으로 제한할 수 있다.
- 서버는 수신한 유효 request ID를 사용하거나 새 값을 생성해 `X-Request-Id`에 반환해야 한다.
- Phase 1 catalog는 조회와 broker node drain을 제공한다. Cluster feature GET은 `ReadOnly`, cluster feature activation은 `ClusterAdmin`의 active mutation이며, Dynamic cluster의 client blocklist는 SEC-12의 active SecurityAdmin·ClusterAdmin mutation이다. 그 밖의 mutation은 deferred다.

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

오류 code 전체 목록과 각 endpoint의 response status는 [Management API v1 OpenAPI](../contracts/management-api-v1.openapi.json)에서 확정한다. 내부 stack, storage key와 secret을 오류에 포함해서는 안 된다. Active drain, feature activation과 blocklist mutation은 `409` 상태를 사용한다. Feature activation의 `feature_not_supported_by_all_members`는 non-retryable이며, `422`를 포함한 다른 mutation 동작은 deferred다. `429`는 §1.5를 따른다.

### 1.4 Deferred mutation과 idempotency

Active broker drain endpoint는 `Idempotency-Key`를 요구한다. 같은 node에 active operation이 있으면 새 operation을 만들지 않고 committed operation을 돌려준다. 같은 key에 다른 parameter를 보내면 `409 idempotency_conflict`다. Feature activation은 같은 active feature를 다시 요청하면 성공 no-op이므로 별도 idempotency key가 필요하지 않다. Dynamic cluster의 활성 management mutation 18개는 모두 감사 event를 기록하며, publisher를 쓸 수 없으면 handler를 실행하지 않고 retryable `503 audit_unavailable`을 반환한다. Blocklist block·unblock은 결과 상태가 정해진 set operation이므로 `Idempotency-Key`를 요구하지 않는다. 그 밖의 mutation endpoint는 아직 deferred다.

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

- 현재 17개 GET operation과 cluster backup POST, broker drain POST/GET/DELETE, cluster feature POST가 구현·계약 대상이다. 정확한 parameter allowlist, schema, response와 operation ID는 OpenAPI가 SSOT이며 `tests/contracts/management-api-v1`가 runtime handler와 일치하는지 검사한다.

| Method | Path | 성공 동작 | 권한 |
| --- | --- | --- | --- |
| GET | `/health/live` | process가 deadlock 없이 응답 가능하면 `200` | probe policy |
| GET | `/health/ready` | 신규 MQTT·관리 traffic 수용 가능하면 `200` | probe policy |
| GET | `/health/startup` | 초기 동기화가 완료되면 `200` | probe policy |
| GET | `/health/drain` | node가 `SAFE_TO_STOP`이면 `200` | probe policy |
| GET | `/metrics` | Prometheus 또는 OpenMetrics scrape | monitoring policy |
| GET | `/api/v1/cluster` | quorum과 cluster 요약 조회 | ReadOnly |
| POST | `/api/v1/cluster/backups` | quorum-fresh typed state backup archive 다운로드 | ClusterAdmin |
| GET | `/api/v1/nodes` | state filter와 pagination을 사용한 node 목록 조회 | ReadOnly |
| GET | `/api/v1/nodes/{nodeId}` | node detail 조회 | ReadOnly |
| GET | `/api/v1/cluster/features` | committed active feature와 member build profile 조회 | ReadOnly |
| POST | `/api/v1/cluster/features/{feature}` | 모든 non-retired member가 지원하는 feature 활성화; replay는 성공 no-op | ClusterAdmin |
| POST | `/api/v1/nodes/{nodeId}/drain` | committed voter의 broker drain 시작 또는 현재 operation 재생 | Operator |
| GET | `/api/v1/nodes/{nodeId}/drain` | committed drain operation 조회 | ReadOnly |
| DELETE | `/api/v1/nodes/{nodeId}/drain` | cancellable drain operation 취소 | Operator |
| GET | `/api/v1/clients` | tenant·client prefix·node·protocol filter로 client 검색 | ReadOnly, tenant scope |
| GET | `/api/v1/clients/{tenantId}/{clientId}` | session 상세 조회 | ReadOnly, tenant scope |
| GET | `/api/v1/subscriptions` | required `tenantId` 범위의 subscription 조회 | ReadOnly, tenant scope |
| GET | `/api/v1/retained` | required `tenantId` 범위의 retained metadata 조회 | ReadOnly, tenant scope |
| GET | `/api/v1/tenants` | tenant 목록 조회 | ClusterAdmin |
| GET | `/api/v1/audit/events` | 이 node의 저장 audit hash-chain record를 최신순으로 조회 | SecurityAdmin, ClusterAdmin |

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

`internal/adapters/raft`의 `WALSyncObserver`는 dynamic cluster의 successful WAL group `File.Sync` 뒤에 한 번만 불린다. 저장 값이 없어 frame·sync가 생략된 요청과 fsync 실패는 관찰하지 않으며, fixed-voter `raft_peer_config.go` 경로에는 observer를 연결하지 않는다. 시험 위치: `internal/adapters/raft/wal_worker_test.go::TestWALWorkerGroupsQueuedFramesAndFailsClosed`, `internal/adapters/raft/node_test.go::TestWALSyncObserverReportsSuccessfulReadyFsyncs`, `internal/adapters/metrics/registry_test.go::TestRegistryRendersWALSyncMetrics`.

Dynamic cluster는 `narumq_raft_stage_duration_seconds{stage}` histogram으로 driver의 처리·대기 시간을 기록한다. 고정 stage와 시작·종료 경계는 [BP-03 Raft 병목 진단](performance-and-verification.md#raft-병목-진단)을 따른다. Finite bucket은 `0.0001`, `0.00025`, `0.0005`, `0.001`, `0.0025`, `0.005`, `0.01`, `0.025`, `0.05`, `0.1`, `0.25`, `0.5`, `1`, `2`, `5`, `10`초이며 표준 `+Inf`를 추가한다. Registry는 모든 mode에서 render하지만 dynamic driver가 아닌 경로의 값은 0이다. 이 값에는 실패와 빈 Ready 처리도 포함되므로 WAL 성공 수나 MQTT 성공 ACK 수와 같은 분모로 취급하지 않는다.

Dynamic cluster의 readiness 거절은 `cluster readiness rejected` 구조화 로그로 진단한다. 고정 purpose·stage, 판정 시점 snapshot, 첫 불충족 조건, 집계·출력 제한과 민감정보 제외 규칙은 [BP-03 Readiness 진단 계약](performance-and-verification.md#readiness-진단-계약)을 따른다. Storage 검사나 fresh proof 단계에서 거절되면 아직 local snapshot을 판정하지 않았으므로 해당 flag와 index를 기록하지 않는다. 새 Metric label이나 API 응답 필드는 추가하지 않으며 기존 readiness·CONNACK 결과는 유지한다.


최소 범주는 connection/session/subscription, message·packet·ACK, latency, cluster·replication, storage, auth·ACL, drain과 process resource다. `narumq_node_drain_transitions_total{phase}`는 committed node drain 상태를 phase별로 집계하며, label은 8개 유효 drain phase만 허용한다. Dynamic cluster의 저장 quota는 `narumq_tenant_quota_rejections_total{tenant,dimension}` counter와 `narumq_tenant_quota_profile_committed`, `narumq_tenant_quota_profile_version`, `narumq_tenant_quota_config_mismatch` gauge를 제공한다. `tenant`는 처음 본 값 1,024개까지만 그대로 보존하고 나머지는 `_overflow`로 합친다. `dimension`은 `retained_bytes`, `offline_queue_messages`, `offline_queue_bytes`, `storage_bytes`만 허용한다. 기존 Metric 이름과 histogram bucket은 `SPEC-TBD-07`에서 확정한다. |

`narumq_tenant_limit_rejections_total{tenant,dimension}` counter는 tenant 연결·traffic
한도 거절마다 증가한다. `dimension`은 `connections`, `sessions`, `subscriptions`,
`message_rate`, `byte_rate`, `payload_size`만 허용한다. `tenant` label은 저장 quota
metric과 처음 본 tenant 1,024개의 집합을 공유한다. 나머지는 `_overflow`로 합친다.
Warn log `tenant limit rejected a request`는 tenant·dimension마다 10초에 한 번만 남긴다.
동작은 [Tenant §5.2](tenancy-and-security.md#52-연결-상태와-traffic-한도)를 따른다.

`narumq_will_outcomes_total{reason}` counter는 [Drain §5](drain-and-rolling-update.md#5-will-판정과-관측)의 6개 reason만 허용한다. 모든 mode에서 6개 series를 0부터 항상 같은 순서로 render한다. 알 수 없는 reason은 무시하며 임의 label을 추가하지 않는다.
`narumq_audit_events_total{outcome}`은 `published`, `publish_failed`, `invalid`, `denial_suppressed`, `mutation_rejected`, `dataplane_suppressed`, `dataplane_dropped`의 7개 outcome을 허용한다. 모든 format과 mode에서 7개 series를 0부터 고정 순서로 render한다. `dataplane_suppressed`는 초당 rate limit 초과를, `dataplane_dropped`는 queue 포화·종료 뒤 event를 나타낸다. 알 수 없는 outcome은 무시한다. `narumq_audit_publisher_ready` gauge는 node-local publisher가 append할 수 있으면 1, 아니면 0이다. 이 Metric에는 tenant, request ID, actor, action 또는 target label을 추가하지 않는다.
Audit outcome series의 고정 순서, 0 기본값, 알 수 없는 label 무시와 readiness gauge는 `internal/adapters/metrics/registry_test.go::TestRegistryRendersAuditMetrics`에서 검증한다.

`narumq_mqtt_listener_admission_rejected_total{transport,reason}` counter는 CONNECT 전에
listener 한도로 닫은 socket을 센다. `transport`는 `tcp`, `tls`만 허용한다.
`reason`은 `max_connections`, `connection_rate`만 허용한다. 모든 mode에서 4개
series를 0부터 같은 순서로 render한다. Transport 순서는 `tcp`, `tls`이며 각
transport 안의 reason 순서는 `max_connections`, `connection_rate`다. 알 수 없는
값은 무시한다.

`narumq_mqtt_connect_rejected_total{protocol,reason}`은 broker가 CONNECT를 거절한 횟수를 protocol·고정 reason별로 센다. `protocol="3.1.0"`은 `unsupported_protocol_version` 한 series만 출력한다. `MQTT_3_1_1`과 `MQTT_5_0`은 각각 다음 8개 reason을 출력한다: `unsupported_protocol_version`, `identifier_rejected`, `bad_credentials`, `not_authorized`, `server_unavailable`, `quota_exceeded`, `banned`, `other`. Registry는 총 17개 series를 0부터 같은 순서로 출력하고 알 수 없는 protocol·reason 조합은 무시한다. MQTT 5 CONNACK reason code는 `0x84`→`unsupported_protocol_version`, `0x85`→`identifier_rejected`, `0x86`→`bad_credentials`, `0x87`→`not_authorized`, `0x88`·`0x89`→`server_unavailable`, `0x8A`→`banned`, `0x97`→`quota_exceeded`이며 나머지는 `other`다. MQTT 3.1.1 return code는 `1`→`unsupported_protocol_version`, `2`→`identifier_rejected`, `3`→`server_unavailable`, `4`·`5`→`not_authorized`이며 나머지는 `other`다. MQTT 5의 `banned`는 blocklist에 일치하는 CONNECT를 나타낸다.

`narumq_messages_dropped_total{reason}`은 routing 뒤 subscriber 한 명에게 전달할 메시지를 broker가 버린 횟수다. 고정 reason은 `queue_full`과 `packet_too_large`다. `queue_full`은 QoS 0 non-durable 후보가 모두 queue full로 끝난 경우에만 증가한다. `packet_too_large`는 subscriber의 Maximum Packet Size보다 큰 전달을 폐기할 때 증가한다. Message expiry는 publisher가 요청한 수명 만료이므로 이 counter에 넣지 않는다. Durable target 포화는 기존 `narumq_durable_delivery_saturated_targets_total`로 센다.

`narumq_node_drain_active`와 `narumq_node_drain_progress{quantity}`는 이 node가 적용한 drain operation의 committed progress를 나타낸다. `quantity`는 `connections_initial`, `connections_remaining`, `sessions_ready_for_takeover`, `sessions_taken_over`, `sessions_failed`, `pending_wal`, `under_replicated`다. Process 재시작 뒤에는 다음 local drain 명령 적용 전까지 모든 값이 0이다. Drain 속도와 ETA는 이 gauge의 Prometheus 추이에서 계산한다.

시험 위치: `internal/adapters/metrics/registry_test.go::TestRegistryRendersConnectRejectionsByProtocolAndReason`, `TestRegistryRendersMessageDrops`, `TestRegistryRendersNodeDrainProgress`; `internal/adapters/metrics/registry_aggregation_test.go::TestRegistryFamiliesMatchAggregationCatalog`.

기본 label에 원문 `clientId`, username, IP, 임의 topic, request ID와 token claim을 넣어서는 안 된다. 제한된 exact topic Metric은 P1 opt-in resource로 제공하며 tenant, 최대 등록 수와 TTL을 적용해야 한다.

### 7.1 Metric별 node·cluster 집계

각 node의 `/metrics`는 해당 node의 값만 출력한다. Cluster 값은 Prometheus 같은 조회 계층이 아래 규칙으로 계산한다. 새 Metric family를 추가하면 아래 family 표에도 행을 추가해야 한다. `internal/adapters/metrics/registry_aggregation_test.go::TestRegistryFamiliesMatchAggregationCatalog`는 이 표를 읽어 `/metrics` 출력과 비교한다.

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
| 5 | 클러스터 상태 | Node 상태 수 | UI-01 | stat | 같은 API의 `nodesByState.ready`, `joining`, `draining`, `safeToStop` |
| 6 | 클러스터 상태 | Node topology | UI-01 | table | Management API `GET /api/v1/nodes?limit=1000`; `nodeId`, `role`, `version`, `uptimeMs`, `state`, `ready`, `readinessReason`, `replicationHealth`, `storagePressure`, `connectionCount`, `lastAppliedIndex`. `READY`는 초록색, `JOINING`은 노란색, `DRAINING`은 주황색, `SAFE_TO_STOP`은 파란색이며 `ready=false` 또는 replication health `DEGRADED`·`FAILED`·`UNREACHABLE`은 빨간색 |
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

UI-01은 panel 5와 6에서 다룬다. 수용 시험은 세 broker process 각각의 `/health/ready`가 `200`을 반환하는지 확인한다. `GET /api/v1/nodes`는 요청을 받은 node의 readiness를 제공한다. 원격 node는 `JOINING`과 `remote readiness is not available from this node`를 반환한다. Dashboard는 이 응답을 그대로 표시하고 원격 readiness를 추정하지 않는다.

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

- API-05 rate-limit 수용 시험은 인증 성공 principal과 인증 실패 IP의 독립 bucket, token 보충·clock 역행, probe·metrics 제외, 403 token 소비, mutation 거절 감사, key 상한·eviction, `RemoteAddr` host 해석, OpenAPI `429`, standalone·cluster 설정 범위를 확인한다: `internal/adapters/http/management/rate_limit_test.go`, `tests/contracts/management-api-v1/contract_test.go::TestManagementRateLimitMatchesOpenAPI`, `cmd/narumq/cmd_config_coverage_test.go`.
- API-05 집중 시험 결과(2026-10-09): adapter rate-limit 시험 7개, management OpenAPI contract suite, cluster·standalone 설정 시험이 모두 통과했다. 실행 파일 smoke 시험은 인증된 요청에서 `200, 200, 429`, 잘못된 자격 증명에서 `401, 429`를 반환했다. Liveness probe 5회는 모두 `200`이었다. 두 `429` 응답은 `Retry-After: 1`을 포함했다. 범위를 벗어난 standalone rate는 exit 1과 `-http-request-rate and -http-auth-failure-rate must be between 1 and 1000000` 오류를 반환했다.
- API-05 전체 검증 결과(2026-10-09): `go run ./tools/verify traceability`가 P0 81건 중 60건 매핑, 예외 21건으로 통과했다. `go run ./tools/verify local`도 153.41초 만에 통과했다. Cluster process fixture는 API 제한이 다른 process acceptance 시험을 막지 않도록 두 rate를 최댓값으로 설정한다. 64개 동시 fresh-read 요청 시험은 5.50초 만에 통과했다.
- Feature 조회 response는 `data.activeFeatures`와 `data.members`를 포함한다. 각 member는 decimal string `nodeId`, canonical `releaseVersion`, 정렬된 `features`를 가진다. 빈 collection은 JSON `[]`로 반환한다.
- Contract suite는 cluster feature GET response shape, 활성화 POST의 200, 미지원 feature의 non-retryable 409, invalid feature의 400과 ReadOnly role POST의 403을 검사한다.
- 같은 contract suite가 pagination `1`/`1000` 경계, `1001` 초과, duplicate·unknown query와 subscriptions/retained의 required `tenantId`를 검증한다.
- 같은 contract suite가 anonymous, 잘못된 credential, ReadOnly API key 성공과 ClusterAdmin 권한 부족 `403`을 검증한다.
- startup, liveness, readiness와 drain-complete가 quorum·sync·drain 상태 조합을 정확히 반영하는지 시험한다.
- SEC-11 / API-08 / NFR-17 수용 시험은 활성 management mutation 8개, 401·403 거절, fail-closed admission, hash-chain 손상, schema 검증, directory 경계와 Windows rotation 충돌을 확인한다: `internal/application/audit/service_test.go`, `internal/adapters/http/management/audit_test.go`, `cmd/narumq/cluster_config_test.go`, `cmd/narumq/main_test.go` (`TestAuditVerifyCommandVerifiesAndLeavesFilesReadOnly`), `tests/contracts/audit-event-v1/contract_test.go`, `tests/integration/cluster_audit_process_test.go::TestDynamicClusterManagementAuditProcessAcceptance`, `internal/adapters/auditlog/file_publisher_test.go`, `internal/adapters/auditlog/persist_windows_test.go`. Process acceptance는 2026-10-09에 통과했다(시험 5.82초).
- Cluster startup probe는 typed Raft recovery가 끝나기 전 `STARTING`, recovery 뒤 base lifecycle 상태를 반환해야 한다. `cmd/narumq/cluster_storage_health_test.go::TestClusterStartupWaitsForTypedRecovery`가 두 상태를 확인한다.
- Metric format, type, HELP/UNIT, histogram bucket과 금지 label을 자동 검사한다.
- UI-03 CONNECT 거절 label과 CONNACK mapping: `internal/adapters/tcp/server_connect_reject_test.go::TestConnectRejectReasonMapsEveryCONNACKCode`, `internal/adapters/metrics/registry_test.go::TestRegistryRendersConnectRejectionsByProtocolAndReason`, `tests/integration/management_api_test.go::TestMQTTConnectRejectionAndDropAreObservable`.
- UI-03 routing 뒤 QoS 0 queue-full drop: `internal/application/publish/publish_test.go::TestPublishCountsQoS0QueueFullDrops`.
- UI-05 node-local drain gauge: `internal/adapters/metrics/registry_test.go::TestRegistryRendersNodeDrainProgress`, `tests/integration/cluster_drain_process_test.go::TestDynamicClusterNodeDrainProcessAcceptance`.
- SEC-10 설정·credential, 4-role permission matrix, OIDC 정적 subject binding과 audit query: `cmd/narumq/cluster_config_test.go`, `internal/adapters/authentication/jwt_verifier_test.go`, `internal/adapters/http/management/auth_test.go`, `internal/application/audit/query_test.go`, `internal/adapters/auditlog/query_test.go`, `internal/adapters/http/management/audit_query_handlers_test.go`, `tests/contracts/management-api-v1/contract_test.go::TestAuditQueryHandlerResponsesMatchOpenAPI`, `tests/contracts/audit-event-v1/contract_test.go::TestAuditQueryReturnsStoredChainRecords`, `tests/integration/cluster_management_rbac_process_test.go::TestDynamicClusterManagementRBACProcessAcceptance`.
- UI-01~UI-05·UI-10 Dashboard contract와 process acceptance: `tests/integration/dashboard_stack_test.go::TestDashboardProvisioningMatchesContracts`, `tests/integration/dashboard_stack_test.go::TestDashboardStackProcessAcceptance`.
- Dashboard 수용 시험 결과(2026-10-09): `TestDashboardProvisioningMatchesContracts`와 opt-in Docker E2E `TestDashboardStackProcessAcceptance`가 통과했다. E2E 실행은 `NARUMQ_DASHBOARD_E2E=1`, `NARUMQ_DASHBOARD_E2E_HOLD=90s` 조건에서 test 229.14초, package 230.254초였다. 세 process의 `/health/ready` `200`, Prometheus target 3개, datasource health 2개, ReadOnly cluster GET `200`, backup·drain POST `403`, cluster traffic·protocol Metric과 완료된 drain 결과를 확인했다. Grafana `/api/ds/query`가 panel 2, 4, 5, 6, 15의 frame을 반환했다. Replica 부족은 `0`, Member·Quorum은 `3/2/3/2`, node 상태 수는 `Ready=1`·`Joining=2`, node topology는 local `READY`와 원격 node의 `JOINING`·정확한 readiness reason, drain 상세는 `COMPLETED`·`SAFE_TO_STOP`·`safeToStop=true`·remaining `0`·takeover `1`을 표시했다. Node별 query에서는 node 1 connection·received PUBLISH rate와 MQTT 3.1.1 성공, node 2 sent PUBLISH rate와 MQTT 5.0 성공·같은 series의 `not_authorized` 거절을 확인했다. 성공 protocol label은 선택한 node 밖에서 나오지 않았다.
- Dashboard named API key regression (2026-10-10): opt-in Docker E2E `TestDashboardStackProcessAcceptance`가 새 `management.apiKeys` 설정에서 통과했다. `NARUMQ_DASHBOARD_E2E=1`, `NARUMQ_DASHBOARD_E2E_HOLD=90s` 실행에서 test는 228.46초, package는 229.503초였다. ReadOnly credential로 cluster GET을 통과하고 backup·drain POST를 거절하는 acceptance가 통과했다.
- Dashboard visual 결과(2026-10-09): browser에서 Quorum `정상`, replica 부족 `0`, Leader `1`, Members `3`, quorum size `2`, replication factor `3`, write quorum `2`를 확인했다. Node topology는 node 1 `READY`, node 2 `JOINING`과 `remote readiness is not available from this node`, node 3 `SAFE_TO_STOP`을 표시했다. `Node=2` 선택 화면에서 Connections, Sent PUBLISH, MQTT 5.0 CONNECT 성공과 CONNECT 거절 panel을 확인했다. Node load에는 세 node가 보였다. Drain panel에는 node 3 `connections_initial=1`, `connections_remaining=0`, `sessions_taken_over=1`, 상세 단계 `COMPLETED`, 상태 `SAFE_TO_STOP`, `safeToStop=true`가 표시됐다. Browser error와 console error는 없었다. CI 실행 링크와 production deployment 검증은 없다.
- P0 traceability gate (2026-10-09): `go run ./tools/verify traceability` PASS. UI-01~UI-05·UI-10이 test에 연결됐고, 전체 P0는 66/81건 매핑·15건 예외다.
- SEC-10 traceability gate (2026-10-10): `go run ./tools/verify traceability` PASS. P0 요구사항 81개 중 68개가 시험에 연결됐고 예외 13개가 남았다. SEC-10은 16개 시험에 연결됐으며 예외에 없다.
- SEC-10 full local gate (2026-10-10): `go run ./tools/verify local` PASS, 205.10초. Traceability는 P0 68/81개 매핑·예외 13개, architecture·branch instrumentation·5개 fuzz target·전체 package/integration·race·vet·coverage가 통과했다. Integration package는 125.359초였다.
- Repository local gate (2026-10-09): `go run ./tools/verify local` PASS in 149.04 seconds. Branch instrumentation, fuzz, architecture, package tests, coverage, and traceability checks passed.
- API-03 catalog test `internal/adapters/metrics/registry_aggregation_test.go::TestRegistryFamiliesMatchAggregationCatalog`는 Prometheus/OpenMetrics 출력의 metric 이름·type·label, 범위와 집계 규칙을 문서 표와 비교한다. Histogram에 `_bucket` 표본이 없는 경우와 `_count` label 조합에 bucket 계열이 없는 경우도 거절한다.
- `internal/adapters/metrics/registry_test.go::TestRegistryReadsSubscriptionCountAtScrape`는 scrape마다 현재 구독 수를 읽는지 확인한다. `internal/domain/routing/routing_test.go::TestTableSubscriptionCountMatchesSnapshot`는 nil·빈 table, shared·non-shared 구독, 교체, 해지, session 제거와 projection 교체 뒤 `SubscriptionCount()`가 `Snapshot()` 길이와 같은지 확인한다.
- `tests/integration/cluster_metrics_process_test.go::TestDynamicClusterSubscriptionGaugeProcessAcceptance`는 3-node cluster에서 초기 gauge 0, SUBSCRIBE 2개 뒤 2, UNSUBSCRIBE 뒤 1을 확인한다. `UNSUBACK`의 packet ID, properties 길이와 성공 reason `0x00`도 확인한다. `SessionExpiryInterval=0`인 MQTT 5 연결을 종료하면 모든 node의 gauge가 0으로 돌아오는지도 확인한다.
- API-03 process 실행 증거(2026-10-09): 수정 전 `go test ./tests/integration -run '^TestDynamicClusterSubscriptionGaugeProcessAcceptance$' -count=1 -v`는 96.22초 뒤 실패했다. 두 SUBSCRIBE를 commit한 뒤 node 1에서 gauge 2를 확인하지 못했다. 수정 후 `GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterSubscriptionGaugeProcessAcceptance$' -count=1 -v -timeout 20m`는 PASS했다(시험 7.80초, package 8.799초). 모든 node의 gauge `0→2→1→0`과 MQTT 5 `UNSUBACK` body `00 03 00 00`을 확인했다.
- deferred mutation endpoint의 idempotency, audit와 dashboard aggregation/masking 검증은 해당 endpoint가 contract에 추가될 때 이 목록에 연결한다.
