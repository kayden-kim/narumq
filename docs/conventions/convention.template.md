# {{언어 또는 영역}} conventions

{{규칙이 해결하는 일관성 문제와 핵심 원칙을 한 문단으로 쓴다.}}

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 적용 대상 | {{human-written source/test/config 등}} |
| 기준 version | {{language/tool version과 링크}} |
| 관련 Architecture | {{링크}} |
| 관련 ADR | {{링크}} |
| 적용 시작일 | YYYY-MM-DD |
| Enforcement | {{formatter/lint/test/review}} |

## 1. 규칙 우선순위

1. Compiler와 공식 specification
2. 공식 formatter와 tooling
3. 공식 convention
4. 이 프로젝트 convention
5. Package-local convention

## 2. 규칙

| 대상 | 해야 할 것 | 피해야 할 것 | 판정 방법 |
| --- | --- | --- | --- |
| {{대상}} | `{{good}}` | `{{bad}}` | {{lint/review 기준}} |

## 3. 예제

```text
좋음: {{예제}}
나쁨: {{예제}}
이유: {{판단 기준}}
```

## 4. 예외와 generated artifact

{{외부 contract, generator와 불가피한 예외의 기록 위치·만료 조건}}

## 5. 검증 gate

```shell
{{복사해 실행 가능한 검증 명령}}
```

## 6. Migration

{{기존 source 적용 범위, 호환성 영향과 단계적 전환 방법}}

