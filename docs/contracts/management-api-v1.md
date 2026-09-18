# Management API v1 Contract

이 문서는 NaruMQ Management API v1의 소유권, versioning과 호환성 규칙을 정의한다. 정확한 path, parameter와 schema는 [OpenAPI JSON](management-api-v1.openapi.json)이 기준이다.

| 항목 | 값 |
| --- | --- |
| 이름과 version | `narumq-management-api` 1.0.0 |
| 상태 | Draft |
| Producer | NaruMQ broker management API |
| Consumer | Dashboard, operator, GitOps·rollout controller, monitoring integration |
| 방향 | HTTP request/response |
| 기준 artifact | [management-api-v1.openapi.json](management-api-v1.openapi.json) |
| 기준 spec | [Management API와 observability](../specs/management-api-and-observability.md), [Drain과 rolling update](../specs/drain-and-rolling-update.md) |
| 관련 Architecture | [System overview](../architectures/system-overview.md), [Security boundaries](../architectures/security-boundaries.md) |
| 관련 requirement | API-02, API-04~API-09, SEC-10~SEC-11, DEP-02~DEP-03 |
| 관련 결정 | [ADR-0005](../decisions/ADR-0005-management-api-contract-format.md) |
| 호환성 범위 | `/api/v1` producer와 v1 consumer |
| 검증 | `tests/contracts/management-api-v1` 예정. 현재 미구현 |

## 1. Protocol

- HTTPS에서 JSON UTF-8 body를 사용한다.
- Management resource의 base path는 `/api/v1`이다.
- Health probe와 `/metrics`는 version path 밖에 둔다.
- Timestamp는 UTC RFC 3339 문자열이다.
- Duration은 음수가 아닌 millisecond 정수다.
- 모든 management response는 `X-Request-Id` header를 반환한다.
- OIDC bearer token 또는 관리 API key를 사용한다. Endpoint별 RBAC는 spec과 OpenAPI description을 따른다.

## 2. Request와 response 호환성

- Server는 request body에서 schema에 없는 field를 `400 invalid_request`로 거절한다.
- Consumer는 response에 추가된 모르는 field를 무시해야 한다.
- Consumer는 모르는 enum 값을 `UNKNOWN` local 상태로 보존하고 mutation 판단에 사용해서는 안 된다.
- Required response field 추가, 기존 field 삭제·rename·type 변경은 breaking change다.
- Optional response field 추가는 기존 consumer contract test가 통과할 때만 호환 변경이다.
- Request field의 새로운 제약은 기존에 유효했던 request를 거절하면 breaking change다.

## 3. Resource와 pagination

단일 resource는 `{"data": {...}}`, 목록은 `{"data": [...], "page": {...}}` envelope를 사용한다. Cursor는 불투명 문자열이다. 빈 다음 cursor는 JSON `null`로 표현한다.

목록 consumer는 `hasMore=true`일 때만 `nextCursor`를 사용해야 한다. Server는 page size가 contract maximum을 넘으면 `400 invalid_page_size`를 반환한다. 기본·최대 page size의 production 값은 `SPEC-TBD-06`이므로 OpenAPI는 범위만 고정하고 기본값을 선언하지 않는다.

Quota의 모든 수치는 음수가 아닌 정수다. `0`은 해당 resource 생성이나 traffic을 허용하지 않는다는 뜻이다. 무제한을 뜻하지 않는다. V1은 무제한 quota 값을 제공하지 않는다.

## 4. Mutation과 idempotency

- Mutation은 `Idempotency-Key`를 받을 수 있다. Drain start에는 반드시 사용하도록 controller contract test에서 강제한다.
- 같은 principal, method, path, key와 같은 body는 최초 결과와 같은 operation을 반환한다.
- 같은 key에 다른 body를 사용하면 `409 idempotency_conflict`다.
- Resource version 충돌은 `409 version_conflict`다.
- Mutation 결과는 [Audit event v1](audit-event-v1.md)을 생성해야 한다.

Idempotency record 보존 시간은 configuration contract에서 확정한다. 확정 전에는 Management API를 Active로 전환할 수 없다.

## 5. 오류

오류 body는 `error.code`, `error.message`, `error.retryable`, `error.details`와 `requestId`를 가진다. `code`는 machine-readable stable identifier다. `message`는 운영자용이며 client 분기 기준으로 사용하지 않는다.

OpenAPI에 정의되지 않은 내부 오류는 `500 internal_error`로 변환한다. Stack, storage key, token과 credential을 반환해서는 안 된다.

## 6. Breaking change

Breaking change는 `/api/v2`와 별도 OpenAPI artifact로 제공한다. V1 deprecation에는 다음을 기록한다.

- 대체 endpoint와 migration guide
- 마지막 지원 release
- Dashboard·controller consumer 전환 상태
- 제거 조건과 rollback 가능 기간

## 7. Draft blocker

- `SPEC-TBD-06`: pagination 기본·최대값과 idempotency 보존 시간
- Handler·Dashboard·controller contract test 구현
- OIDC issuer/API key header의 배포 profile 확정
- OpenAPI lint와 breaking-change CI tool 선정
