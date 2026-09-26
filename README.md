# NaruMQ

NaruMQ는 Kubernetes 환경에 최적화된 고가용성 분산 MQTT 브로커 클러스터입니다.

## 문서 체계

저장소의 모든 설계 및 규범 문서는 `docs/` 아래에서 SSOT(Single Source of Truth) 원칙에 따라 관리됩니다.

- [문서 전체 인덱스](docs/README.md)
- [저장소 작업 지침](AGENTS.md)
- [문서 작성 규칙](docs/AGENTS.md)

### 디렉터리 구성

| 디렉터리 | 역할 및 질문 |
| --- | --- |
| `docs/requirements/` | 무엇이 필요하며 왜 필요한가? ([PRD](docs/requirements/PRD.md)) |
| `docs/specs/` | 시스템 밖에서 관찰할 수 있는 동작은 무엇인가? ([시스템 스펙](docs/specs/README.md), 복잡한 화면 동작은 화면 스펙) |
| `docs/contracts/` | 경계에서 무엇을 어떤 형식으로 주고받는가? |
| `docs/architectures/` | 코드는 어디에 있고 구성요소·모듈은 어떻게 연결되는가? ([Project structure의 코드 지도 절](docs/architectures/project-structure.md), [모듈 설계 템플릿](docs/architectures/module-design.template.md)) |
| `docs/decisions/` | 왜 이 기술·구조를 선택했는가? (ADR) |
| `docs/explainers/` | 핵심 기술과 원리는 어떻게 작동하는가? |
| `docs/conventions/` | 소스와 저장소 산출물을 어떤 규칙으로 작성하는가? |
| `docs/how-to/` | 특정 작업을 안전하게 어떻게 수행하는가? |

### 처음 읽는 순서

1. [PRD](docs/requirements/PRD.md)에서 무엇을 왜 만드는지 확인한다.
2. [Project structure](docs/architectures/project-structure.md)의 코드 지도 절에서 현재 코드 위치와 진입 흐름을 확인한다. 별도의 `Target repository tree`는 목표 구조로 구분한다.
3. [시스템 스펙](docs/specs/README.md)에서 변경할 기능의 외부 동작과 시험 기준을 찾는다.
4. 관련 contract·architecture·ADR을 읽고, 필요하면 [기술 해설](docs/explainers/README.md)에서 배경과 실제 코드를 따라간다.

### 기본 문서 연결 흐름

```text
Requirement → Spec → Contract
Requirement / Spec / Contract / ADR → Architecture View
ADR / Architecture View → Convention
Spec / Contract → ADR → Explainer
Spec / Contract / ADR → How-to
```
