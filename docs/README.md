# NaruMQ 문서

이 디렉터리는 프로젝트의 기준 문서를 질문의 종류에 따라 분리한다.

| 디렉터리 | 답하는 질문 | 시작 템플릿 |
| --- | --- | --- |
| `requirements/` | 무엇이 필요하며 왜 필요한가? | `PRD.template.md` |
| `specs/` | 시스템은 외부에서 어떻게 동작해야 하는가? | `system-spec.template.md` |
| `contracts/` | 경계에서 무엇을 어떤 형식으로 주고받는가? | `contract.template.md` |
| `architectures/` | 구성요소와 경계는 어떻게 연결되는가? | `architecture-view.template.md` |
| `decisions/` | 왜 이 선택을 했는가? | `ADR-0000-template.md` |
| `explainers/` | 핵심 기술과 원리는 어떻게 작동하는가? | `explainer.template.md` |
| `conventions/` | 소스와 저장소 산출물을 어떤 규칙으로 작성하는가? | `convention.template.md` |
| `how-to/` | 특정 작업을 안전하게 어떻게 수행하는가? | `how-to.template.md` |

## 문서 추가 절차

1. 독자가 답을 얻으려는 주된 질문을 하나 정한다.
2. 위 표에서 디렉터리를 선택한다.
3. 해당 템플릿을 복제하고 자리표시자를 채운다.
4. 요구사항 ID와 관련 기준 문서를 상대 링크로 연결한다.
5. 디렉터리의 `README.md` 카탈로그를 갱신한다.
6. 링크, 상태, 검증 증거와 미결정 사항을 점검한다.
