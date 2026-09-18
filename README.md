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
| `docs/specs/` | 시스템은 외부에서 어떻게 동작해야 하는가? |
| `docs/contracts/` | 경계에서 무엇을 어떤 형식으로 주고받는가? |
| `docs/architectures/` | 구성요소와 경계는 어떻게 연결되는가? |
| `docs/decisions/` | 왜 이 기술·구조를 선택했는가? (ADR) |
| `docs/explainers/` | 핵심 기술과 원리는 어떻게 작동하는가? |
| `docs/conventions/` | 소스와 저장소 산출물을 어떤 규칙으로 작성하는가? |
| `docs/how-to/` | 특정 작업을 안전하게 어떻게 수행하는가? |

### 기본 문서 연결 흐름

```text
Requirement → Spec → Contract
Requirement / Spec / Contract / ADR → Architecture View
ADR / Architecture View → Convention
Spec / Contract → ADR → Explainer
Spec / Contract / ADR → How-to
```
