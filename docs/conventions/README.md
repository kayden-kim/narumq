# NaruMQ Conventions

이 디렉터리는 NaruMQ source와 repository artifact의 일관된 작성 규칙을 관리한다. 구조와 dependency는 [Architecture Views](../architectures/README.md), 기술 선택은 [ADR catalog](../decisions/README.md), 제품 동작은 [시스템 스펙](../specs/README.md)을 기준으로 한다.

## 문서 목록

| 문서 | 상태 | 적용 범위 | 답하는 질문 |
| --- | --- | --- | --- |
| [Go coding conventions](go-coding-conventions.md) | Active | 새로 작성하거나 변경하는 human-written Go source | Package, type, Prefix·Postfix, DTO, error와 test를 어떻게 이름 짓고 작성하는가? |

## 적용 원칙

- Active convention은 새 source와 변경 code에 적용한다.
- 기존 code가 규칙과 다르면 관련 변경에서 안전하게 고친다. 무관한 대규모 rename을 끼워 넣지 않는다.
- Generated code와 외부 protocol 이름은 generator·contract를 우선하고 human-written wrapper에서 project convention으로 변환한다.
- Convention 위반을 허용하려면 이유, 범위, owner와 제거 조건을 lint configuration 또는 검토 기록에 남긴다.
