# Convention 문서 작성 규칙

[상위 규칙](../AGENTS.md)을 따르며 다음을 추가한다.

- 대상 language, source와 artifact 범위를 명시한다.
- 공식 specification, formatter와 language convention을 프로젝트 규칙보다 우선한다.
- 상태는 `Draft`, `Active`, `Deprecated`, `Retired`를 사용한다.
- 허용·금지 예시와 판단 기준을 함께 쓴다.
- formatter, lint, test 또는 review로 판정할 수 있게 쓴다.
- generated code와 외부 schema가 강제하는 이름의 예외를 명시한다.
- Architecture가 정한 경계를 바꾸거나 제품 동작을 새로 정의하지 않는다.

## 추가 작성 지침

- **규칙 우선순위:** 충돌 시 1. Compiler/Language spec → 2. `gofmt` 등 공식 formatter → 3. 공식 language convention → 4. NaruMQ convention → 5. Package-local convention 순으로 적용한다.
- **예제 규칙:** 예제는 NaruMQ의 실제 package와 domain 용어를 사용하고, Go code는 `gofmt` 결과 형태로 작성한다. 아직 구현되지 않은 type이나 package는 Target convention임을 밝힌다.
