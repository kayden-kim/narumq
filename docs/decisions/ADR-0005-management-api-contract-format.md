# ADR-0005: Management API contract는 OpenAPI 3.1.2 JSON으로 관리한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Proposed |
| 제안일 | 2026-08-16 |
| 결정일 | 미정 |
| 결정자 | 미정 |
| 관련 요구사항 | API-02, API-05, API-08~API-09, SEC-10~SEC-11 |
| 관련 스펙 | [Management API와 observability](../specs/management-api-and-observability.md) |
| 관련 Architecture | [System overview](../architectures/system-overview.md), [Security boundaries](../architectures/security-boundaries.md) |
| 관련 Contract | [Management API v1](../contracts/management-api-v1.md), [OpenAPI 원본](../contracts/management-api-v1.openapi.json) |
| 관련 해설 | [OpenAPI, controller와 error mapping](../explainers/openapi-controller-and-error-mapping.md) |

## 1. 결정

Management HTTP API의 기준 artifact로 OpenAPI Specification 3.1.2 JSON 문서를 사용한다.

- Entry document는 `docs/contracts/management-api-v1.openapi.json`이다.
- API major version은 path `/api/v1`에 포함한다.
- OpenAPI `info.version`은 contract revision을 나타낸다.
- Schema dialect는 JSON Schema Draft 2020-12를 사용한다.
- Server handler, client, 문서와 contract test는 이 artifact에서 생성하거나 검증한다.
- Markdown contract는 ownership, compatibility와 변경 정책만 설명하고 schema를 복제하지 않는다.

HTTP API의 breaking change는 `/api/v2`와 새 contract로 제공한다. 같은 `/api/v1` 안에서는 승인된 backward-compatible 변경만 허용한다.

## 2. 배경

PRD는 versioned JSON API와 OpenAPI 문서의 일치를 요구한다. Spec은 resource, 상태와 공통 envelope를 정의하지만 exact field, required 여부와 오류 response는 machine-readable contract가 필요하다.

OpenAPI 3.1 계열은 JSON Schema Draft 2020-12와 정렬되어 request·response를 하나의 artifact로 검증할 수 있다. JSON 표현은 YAML parser 차이와 implicit scalar 변환을 피한다.

## 3. 결정 요인

1. Go handler와 Dashboard client를 같은 contract로 검증해야 한다.
2. Required, nullability, enum과 error response를 기계적으로 검사해야 한다.
3. API version과 contract revision을 구분해야 한다.
4. Code generation tool 하나에 source of truth를 종속시키면 안 된다.
5. Breaking change를 release 전에 탐지해야 한다.

## 4. Compatibility 정책

- Response consumer는 모르는 field를 무시해야 한다.
- Server는 request body의 모르는 field를 거절한다.
- Optional response field 추가는 contract test가 기존 consumer를 통과할 때 호환 변경이다.
- Required field 추가, field 삭제·이름 변경·type 변경과 enum 값 의미 변경은 breaking change다.
- Enum 값 추가는 consumer의 unknown-enum 처리 검증 없이는 호환으로 보지 않는다.
- Error `code`는 안정된 식별자다. 기존 code의 의미를 바꾸지 않는다.
- Cursor는 불투명 값이며 format을 public contract로 만들지 않는다.

## 5. 대안

| 대안 | 장점 | 제외 이유 |
| --- | --- | --- |
| OpenAPI 3.2.0 | 최신 OAS 기능 사용 | 초기 tooling 호환 범위를 Phase 0에서 증명하지 못했다. 3.1.2로 시작하고 재검토한다. |
| OpenAPI 3.0.x | Tooling이 넓다. | JSON Schema 표현과 nullability가 3.1보다 제한된다. |
| YAML OpenAPI | 사람이 읽기 쉽다. | YAML implicit type과 parser 차이를 줄이기 위해 JSON을 기준으로 선택한다. 생성 YAML은 파생 artifact로 허용한다. |
| Go code annotation 기준 | Handler 가까이에서 관리 | 언어·framework와 contract가 결합하고 consumer-first review가 어렵다. |
| Markdown 표만 사용 | 작성이 빠르다. | Required, schema와 compatibility를 자동 검증할 수 없다. |

## 6. 결과

- API review와 implementation diff를 분리해 확인할 수 있다.
- OpenAPI lint, schema validation과 breaking-change 검사가 CI 필수가 된다.
- Handler와 Dashboard type을 생성할 수 있지만 생성 code는 기준 artifact가 아니다.
- OAS 3.1.2를 지원하는 toolchain을 Phase 0에서 고정해야 한다.

## 7. 검증

1. OpenAPI 3.1 validator가 artifact를 통과한다.
2. 모든 example이 schema validation을 통과한다.
3. Go handler의 route, status와 body가 contract test와 일치한다.
4. Dashboard client가 generated type과 response compatibility test를 통과한다.
5. 이전 Active contract와 breaking-change diff를 CI에서 검사한다.
6. Secret과 무제한 cardinality 식별자가 example에 없는지 검사한다.

## 8. 재검토 조건

- OAS 3.2가 NaruMQ toolchain에서 안정적으로 지원되고 필요한 기능이 확인된다.
- JSON 기준이 review 또는 generation 비용을 크게 높인다.
- HTTP 외 protocol이 management plane의 기준 interface가 된다.

## 9. 참고 자료

- [OpenAPI Specification 3.1.2](https://spec.openapis.org/oas/v3.1.2.html)
- [JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12)
