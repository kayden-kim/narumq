# {{Contract 이름}} v{{major}} Contract

{{교환 목적과 이 문서가 관리하는 호환성 범위를 설명한다. 정확한 형식은 기준 artifact를 연결한다.}}

| 항목 | 값 |
| --- | --- |
| 이름과 version | `{{contract-name}}` 1.0.0 |
| 상태 | Draft |
| 소유자 | {{team/owner}} |
| Producer | {{생성 주체}} |
| Consumer | {{소비 주체}} |
| 방향 | {{request/response, publish/subscribe 등}} |
| 기준 artifact | {{OpenAPI/AsyncAPI/Proto/JSON Schema 링크}} |
| 기준 spec | {{링크}} |
| 관련 requirement | {{ID}} |
| 호환성 범위 | {{지원 조합}} |
| 검증 | {{contract test/schema validation 링크}} |

## 1. 형식과 의미

| Field·message | Type | Required·nullable | 의미·제약 |
| --- | --- | --- | --- |
| `{{field}}` | {{type}} | {{규칙}} | {{단위, 범위, 기본값}} |

## 2. 오류

| Code | 조건 | 재시도 가능 | 상태 변경 |
| --- | --- | --- | --- |
| `{{stable_code}}` | {{조건}} | {{yes/no}} | {{결과}} |

## 3. 호환성 규칙

- Consumer는 {{unknown field/enum 처리}}해야 한다.
- 호환 변경: {{규칙}}
- Breaking change: {{규칙}}

## 4. Version과 lifecycle

| 상태 | 조건 | 지원 종료·대체 |
| --- | --- | --- |
| Active | {{승인 조건}} | {{정책}} |

## 5. 예제와 검증

{{기준 artifact로 자동 검증되는 최소 예제를 연결한다.}}

## 6. Draft blocker

- {{확정되지 않은 값, test 또는 승인}}

