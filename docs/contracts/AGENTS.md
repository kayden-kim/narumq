# Contract 작성 규칙

[상위 규칙](../AGENTS.md)을 따르며 다음을 추가한다.

- 이름과 version, 상태, producer, consumer, 방향과 소유자를 명시한다.
- 검증 가능한 schema가 있으면 machine-readable artifact를 기준으로 삼는다.
- Markdown에 schema 전체를 복제하지 않고 목적, 범위와 호환성 규칙을 설명한다.
- type, required, nullability, enum, 범위, 단위, 식별자와 unknown 값 처리를 명시한다.
- 호환 변경과 breaking change를 구분한다.
- Breaking change는 새 major version, migration과 rollback 계획을 만든다.
- producer와 consumer 양쪽의 contract test를 연결한다.
- secret이나 실제 credential을 예시에 넣지 않는다.

## 추가 작성 지침

- **상태 정의:** 상태는 `Draft`, `Active`, `Deprecated`, `Retired`를 명시한다.
- **Schema 세부 규칙:** 시간은 timezone과 정밀도, duration과 크기는 단위를 명시한다. 식별자의 scope, 대소문자 구분, encoding 정규화 규칙을 정의한다. 오류는 안정된 code, 재시도 가능 여부, HTTP·protocol status 대응을 명시한다.
- **호환성 및 Breaking Change:** Field 삭제·이름 변경·type 변경, 의미 변경 및 기존 값 금지는 breaking change로 취급한다. Optional field나 enum 추가는 consumer의 unknown 필드/값 처리 규칙을 확인한 뒤에만 호환 변경으로 분류한다.
