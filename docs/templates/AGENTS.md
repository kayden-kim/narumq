# 템플릿 원본 관리 규칙

[상위 규칙](../AGENTS.md)을 따른다. 이 디렉터리는 시작 템플릿의 원본만 모아 둔다. 작성한 프로젝트 문서는 해당 문서 유형의 디렉터리에 둔다. 템플릿은 구현 기준으로 채택된 문서가 아니며 README 문서 목록에 등록하지 않는다.

## 템플릿과 대상 작성 규칙

원본을 수정할 때도 해당 문서 유형의 작성 규칙을 읽고 적용한다. 새 프로젝트 문서를 작성할 때는 대상 디렉터리의 `AGENTS.md`에서 필요한 시작 템플릿을 선택한다.

| 원본 파일 | 실제 문서의 대상 디렉터리·작성 규칙 |
| --- | --- |
| `PRD.template.md` | [requirements](../requirements/AGENTS.md) |
| `system-spec.template.md`, `screen-spec.template.md` | [specs](../specs/AGENTS.md) |
| `contract.template.md` | [contracts](../contracts/AGENTS.md) |
| `code-map.template.md`, `module-design.template.md`, `architecture-view.template.md` | [architectures](../architectures/AGENTS.md) |
| `ADR-0000-template.md` | [decisions](../decisions/AGENTS.md) |
| `explainer.template.md` | [explainers](../explainers/AGENTS.md) |
| `convention.template.md`, `DESIGN.template.md`, `frontend.template.md` | [conventions](../conventions/AGENTS.md) |
| `how-to.template.md` | [how-to](../how-to/AGENTS.md) |
| `incident.template.md` | [incidents](../incidents/AGENTS.md) |

## 편집과 복제

- 템플릿 원본은 이 디렉터리 한 곳에서 관리한다. 유형별 디렉터리에 원본 사본을 남기지 않는다.
- 템플릿을 복제한 실제 문서는 대상 유형의 디렉터리에 의미 있는 이름으로 작성한다. 필요한 자리표시자를 채우고 사용하지 않는 예시 행과 작성 안내를 제거한다.
- 템플릿의 상대 링크는 원본 위치에서 유효해야 한다. 복제한 뒤에도 대상 문서 위치에서 링크를 확인한다. 실제 문서의 내용 링크는 시작 템플릿 대신 작성한 기준 문서로 바꾼다.
- 템플릿을 추가·이동·삭제하면 이 파일의 유형 대응과 해당 유형의 `AGENTS.md` 안내를 함께 갱신한다. README 문서 목록에는 작성한 실제 문서만 등록한다.
- 템플릿 수정은 제품의 요구사항·설계·코드를 자동으로 바꾸지 않는다. 실제 프로젝트에 적용할 때 채택 범위와 기존 내용을 확인한다.
- 공통 템플릿은 특정 프레임워크나 라이브러리를 전제로 쓰지 않는다. 기술 선택과 그에 따른 설정은 실제 프로젝트 문서에 채택 근거와 함께 기록한다.
