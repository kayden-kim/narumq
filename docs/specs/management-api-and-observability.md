# Management API와 observability 스펙

이 문서는 operator, GitOps controller, Dashboard와 monitoring system이 NaruMQ를 조회·변경하는 HTTP·Metric 계약을 정의한다. Resource별 전체 schema는 이 문서를 따르는 OpenAPI에서 확정한다.

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 관련 요구사항 | API-01~API-09, UI-01~UI-10, SEC-10~SEC-11 |
| 근거 | [PRD 9.5절, 9.6절과 14장](../requirements/PRD.md) |
| 관련 spec | [Tenant와 security](tenancy-and-security.md), [Drain](drain-and-rolling-update.md) |
| 관련 Architecture | [System overview](../architectures/system-overview.md), [Security boundaries](../architectures/security-boundaries.md) |
| 관련 결정 | [ADR-0005](../decisions/ADR-0005-management-api-contract-format.md) |
| 관련 Contract | [Management API v1](../contracts/management-api-v1.md), [Audit event v1](../contracts/audit-event-v1.md) |

## 1. HTTP 공통 계약

- 관리 API base path는 `/api/v1`이다. 호환되지 않는 변경은 새 major path에서 제공해야 한다.
- request와 response body는 `application/json` UTF-8을 사용한다.
- 모든 요청은 인증과 RBAC를 통과해야 한다. Health probe의 노출 범위는 listener 정책으로 제한할 수 있다.
- 서버는 수신한 유효 request ID를 사용하거나 새 값을 생성해 `X-Request-Id`에 반환해야 한다.
- mutation API는 결과를 감사 로그에 기록해야 한다.

### 1.1 성공 envelope

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

### 1.2 오류 envelope

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

오류 code 전체 목록은 OpenAPI에서 확정한다. 내부 stack, storage key와 secret을 오류에 포함해서는 안 된다.

### 1.3 Mutation과 idempotency

P1 mutation endpoint는 `Idempotency-Key`를 받아야 한다. 같은 principal, endpoint와 key에 같은 payload를 재시도하면 최초 결과와 의미가 같아야 한다. 같은 key에 다른 payload가 오면 `409 idempotency_conflict`를 반환해야 한다. 보존 시간은 OpenAPI/configuration에 명시한다.

resource 변경은 가능한 경우 version/ETag 기반 조건부 요청을 지원해야 한다. stale version으로 상태를 덮어써서는 안 된다.

## 2. Endpoint catalog

| Method | Path | 성공 동작 | 권한 |
| --- | --- | --- | --- |
| GET | `/health/live` | process가 deadlock 없이 응답 가능하면 `200` | probe policy |
| GET | `/health/ready` | 신규 MQTT·관리 traffic 수용 가능하면 `200` | probe policy |
| GET | `/health/startup` | 초기 동기화가 완료되면 `200` | probe policy |
| GET | `/health/drain` | node가 `SAFE_TO_STOP`이면 `200` | probe policy |
| GET | `/api/v1/cluster` | quorum과 cluster 요약 조회 | ReadOnly |
| GET | `/api/v1/nodes` | node 목록 조회 | ReadOnly |
| GET | `/api/v1/nodes/{nodeId}` | node 상세 조회 | ReadOnly |
| POST | `/api/v1/nodes/{nodeId}/drain` | drain 시작 또는 현재 operation 반환 | Operator |
| GET | `/api/v1/nodes/{nodeId}/drain` | drain operation 조회 | ReadOnly |
| DELETE | `/api/v1/nodes/{nodeId}/drain` | 취소 가능 단계의 drain 취소 | Operator |
| GET/POST | `/api/v1/tenants` | tenant 목록·생성 | ClusterAdmin |
| GET/PUT | `/api/v1/tenants/{tenantId}/quotas` | quota 조회·교체 | SecurityAdmin 또는 scoped admin |
| GET/PUT | `/api/v1/tenants/{tenantId}/policies` | ACL 조회·교체 | SecurityAdmin 또는 scoped admin |
| GET | `/api/v1/clients` | filter로 client 검색 | ReadOnly, tenant scope |
| GET | `/api/v1/clients/{tenantId}/{clientId}` | session 상세 조회 | ReadOnly, tenant scope |
| DELETE | `/api/v1/clients/{tenantId}/{clientId}` | client disconnect | Operator, tenant scope |
| POST | `/api/v1/clients/{tenantId}/{clientId}/redirect` | MQTT 5 client에 임시 redirect 요청 | Operator, tenant scope |
| GET | `/api/v1/subscriptions` | subscription 조회 | ReadOnly, tenant scope |
| GET | `/api/v1/retained` | retained metadata 조회 | ReadOnly, tenant scope |
| DELETE | `/api/v1/retained/{topic}` | exact retained topic 삭제 | Operator, tenant scope |
| GET | `/metrics` | OpenMetrics scrape | monitoring principal |

Drain endpoint의 body와 상태는 [drain spec](drain-and-rolling-update.md)을 기준으로 한다. MQTT redirect는 MQTT 5 `0x9C`와 Server Reference를 사용하며 MQTT 3.1.1 client에 같은 의미를 약속해서는 안 된다.

## 3. Probe 의미

| Probe | 성공 조건 | 실패가 뜻하는 것 |
| --- | --- | --- |
| startup | process 초기화, local recovery, cluster sync와 필수 route 준비 완료 | 아직 재시작 판정에 liveness를 사용하면 안 됨 |
| liveness | process가 내부 health loop에 응답하고 진행 불능 상태가 아님 | kubelet 재시작 후보 |
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

Node 상세는 `nodeId`, version, uptime, lifecycle state, readiness 원인, role, last applied index, resource pressure, connection 수와 drain operation을 포함해야 한다. 정확한 field name은 OpenAPI에서 고정한다.

## 5. Filtering과 masking

- 목록 endpoint는 cursor pagination과 endpoint별 allowlist filter만 허용해야 한다.
- client 검색은 tenant, exact/prefix clientId, node, IP와 protocol filter를 P1에서 제공한다.
- arbitrary regular expression이나 wildcard topic Metric 등록을 허용해서는 안 된다.
- client identity, IP, inflight payload와 offline payload는 role에 따라 마스킹하거나 제외해야 한다.
- retained 목록은 payload를 기본 반환하지 않고 metadata만 반환해야 한다. Payload 조회가 필요하면 별도 권한과 size limit을 가져야 한다.

## 6. GitOps와 runtime 변경

각 설정 필드는 source of truth를 `gitops`, `runtime` 또는 `derived`로 선언해야 한다.

- `gitops` 필드는 runtime API로 영구 변경해서는 안 된다. 시도하면 `409 managed_by_gitops`를 반환해야 한다.
- `runtime` 필드는 API mutation과 감사 로그를 통해 바꿀 수 있다.
- reconciliation은 desired version, observed version, last result와 conflict를 조회 가능하게 해야 한다.
- controller가 값을 되돌렸을 때 silent success로 표시해서는 안 된다.

## 7. Metric 계약

`/metrics`는 인증된 scrape 요청에 Prometheus/OpenMetrics 호환 text format을 반환해야 한다.

| Metric 종류 | 집계 규칙 |
| --- | --- |
| counter | cluster 값은 node counter의 rate/increase를 조회 계층에서 합산한다. restart를 고려한다. |
| gauge | 합계가 의미 있는 값만 합산한다. quorum, leader와 state는 합산하지 않고 명시적 gauge로 표시한다. |
| histogram | 같은 bucket 경계만 cluster에서 합산한다. quantile을 node별로 계산해 평균내서는 안 된다. |

최소 범주는 connection/session/subscription, message·packet·ACK, latency, cluster·replication, storage, auth·ACL, drain과 process resource다. Metric 전체 이름과 bucket은 `SPEC-TBD-07`에서 확정한다.

기본 label에 원문 `clientId`, username, IP, 임의 topic, request ID와 token claim을 넣어서는 안 된다. 제한된 exact topic Metric은 P1 opt-in resource로 제공하며 tenant, 최대 등록 수와 TTL을 적용해야 한다.

## 8. Dashboard 계약

Dashboard는 이 API와 외부 TSDB를 사용해야 한다. 장기 시계열 원본을 자체 저장해서는 안 된다.

첫 화면은 cluster topology, quorum 위험, replica 부족, node version/readiness/drain과 핵심 traffic 추이를 보여야 한다. 운영 화면은 node 부하 편차, drain progress·ETA·실패 이유를 표시해야 한다. P1 화면은 권한 범위 안에서 client/session/subscription, redirect/disconnect와 retained·exact topic Metric을 제공한다.

UI mutation은 API와 같은 RBAC, 확인 절차, idempotency와 감사 계약을 따라야 한다.

## 9. 수용 시험

- OpenAPI request/response example과 실제 handler를 contract test로 비교한다.
- 모든 목록 endpoint에 pagination 경계, filter allowlist와 tenant scope를 시험한다.
- 각 role의 endpoint·resource matrix와 IDOR를 시험한다.
- startup, liveness, readiness와 drain-complete가 quorum·sync·drain 상태 조합을 정확히 반영하는지 시험한다.
- Metric format, type, HELP/UNIT, histogram bucket과 금지 label을 자동 검사한다.
- 같은 idempotency key 재시도와 payload 충돌을 시험한다.
- Dashboard query가 node/cluster aggregation 규칙과 masking을 지키는지 검증한다.
