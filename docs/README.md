# NaruMQ 문서

이 디렉터리는 프로젝트의 기준 문서를 질문의 종류에 따라 분리한다.

[PRD 구현 잔여 작업](TODO.md)은 PRD 전반에서 구현·검증·선행 결정이 남은 작업을 요구사항 ID 기준으로 추적한다. 기존 cluster 수용 시험 잔여와 완료 증거도 보존한다.

| 디렉터리 | 답하는 질문 | 시작 템플릿 |
| --- | --- | --- |
| [PRD(제품 요구사항 정의서)](requirements/README.md) | 무엇이 필요하며 왜 필요한가? | [PRD.template.md](templates/PRD.template.md) |
| [스펙(동작 명세)](specs/README.md) | 시스템은 외부에서 어떻게 동작해야 하는가? | [system-spec.template.md](templates/system-spec.template.md), [screen-spec.template.md](templates/screen-spec.template.md) |
| [인터페이스 계약(Contract)](contracts/README.md) | 경계에서 무엇을 어떤 형식으로 주고받는가? | [contract.template.md](templates/contract.template.md) |
| [아키텍처(구성요소의 책임과 연결 관계)](architectures/README.md) | 코드는 어디에 있고 구성요소와 모듈은 어떻게 연결되는가? | [code-map.template.md](templates/code-map.template.md), [module-design.template.md](templates/module-design.template.md), [architecture-view.template.md](templates/architecture-view.template.md) |
| [ADR(기술 결정 기록)](decisions/README.md) | 왜 이 선택을 했는가? | [ADR-0000-template.md](templates/ADR-0000-template.md) |
| [해설](explainers/README.md) | 핵심 기술과 원리는 어떻게 작동하는가? | [explainer.template.md](templates/explainer.template.md) |
| [시각 자료](artifacts/README.md) | 구현과 스펙을 시각 자료로 어떻게 이해하는가? | 요청별 HTML artifact |
| [코딩 컨벤션](conventions/README.md) | 소스와 저장소 산출물을 어떤 규칙으로 작성하는가? | [convention.template.md](templates/convention.template.md), [DESIGN.template.md](templates/DESIGN.template.md), [frontend.template.md](templates/frontend.template.md) |
| [실행 절차](how-to/README.md) | 특정 작업을 안전하게 어떻게 수행하는가? | [how-to.template.md](templates/how-to.template.md) |
| [사고 기록](incidents/README.md) | 사고의 영향·원인·대응·후속 조치는 무엇인가? | [incident.template.md](templates/incident.template.md) |
| [사용자 매뉴얼](manual/index.md) | 브로커를 어떻게 실행하고 클라이언트를 어떻게 연동하는가? | GitHub Pages (VitePress) |

## 템플릿과 스킬

시작 템플릿은 모두 [docs/templates](templates/AGENTS.md)에서 관리한다. 유형별 사용 기준은 해당 디렉터리의 `AGENTS.md`를 따른다. 템플릿을 실제 문서 목록에 등록하지 않는다.

- [frontend-workflow 스킬](../.agents/skills/frontend-workflow/SKILL.md): 프런트엔드 디자인 탐색·구현·검토
- [explain 스킬](../.agents/skills/explain/SKILL.md): 프로젝트 구조와 코드 흐름 설명
- [artifact-design 스킬](../.agents/skills/artifact-design/SKILL.md): 글·시각 자료·상호작용을 결합한 단일 HTML 아티팩트 제작

## 처음 읽는 순서

1. [PRD](requirements/README.md)에서 제품의 문제·범위와 요구사항을 확인한다.
2. [Project structure의 코드 지도 절](architectures/project-structure.md)에서 주요 package와 현재 구현 위치를 찾는다. `Target repository tree`는 별도의 목표 구조이므로 구현된 경로와 구분한다.
3. [시스템 스펙](specs/README.md)에서 변경할 외부 동작과 연결된 시험 기준을 확인한다.
4. 해당 모듈의 architecture view와 contract를 읽고, 설계 배경이 필요하면 ADR·해설을 따른다.

## 프로젝트 작업 안내

작업 전 아래 표의 기준 문서를 확인한다. 명령과 규칙의 본문은 해당 문서 한 곳에서 관리하고 여기에는 복제하지 않는다.

| 확인할 내용 | 프로젝트의 기준 문서 |
| --- | --- |
| 변경 대상의 구조와 코드 위치·진입점 | [Project structure의 코드 지도 절](architectures/project-structure.md), [브로커 코어 아키텍처](architectures/broker-core.md) |
| 적용할 코드 작성 규칙 | [Go 코딩 컨벤션](conventions/go-coding-conventions.md) |
| 개발 환경 준비와 실행 방법 | [저장소 README](../README.md) (Go 1.26.5 기준) |
| Operations Dashboard 구성·검증 | [NaruMQ Operations Dashboard 구성](how-to/operations-dashboard.md) |
| 변경 범위별 테스트와 전체 완료 검증 | [성능 및 검증](specs/performance-and-verification.md) (`go run ./tools/verify local`, `ci`, 수동 `mutation`) |

검증 절차에는 실행 위치, 필요한 도구·환경·버전, 변경 범위별 명령, 전체 완료 명령과 성공 조건을 기록한다. 수동 mutation 검증은 필요할 때 별도로 실행한다.

작업 순서는 루트 [작업 지침](../AGENTS.md#작업-순서와-필수-참조), 문서 채택 여부와 충돌 판단은 [기준 문서 적용과 충돌 처리](AGENTS.md#기준-문서-적용과-충돌-처리)를 따른다.

## 변경에 따른 문서 선택

모든 변경에 모든 유형의 문서를 만들지 않는다. 기존 문서로 다룰 수 있으면 그 문서를 갱신하고, 독립적으로 관리할 질문이 생겼을 때만 필요한 문서를 추가한다. 여러 조건에 해당하면 관련 유형을 함께 확인한다.

| 변경 상황 | 우선 확인하고 필요한 경우 갱신할 문서 |
| --- | --- |
| 사용자에게 보이는 동작을 추가하거나 변경 | 요구사항·시스템 스펙. 화면이 관련되면 [화면 스펙 선택 기준](specs/AGENTS.md)에 따라 단순한 화면은 스펙의 화면 구성 절에, 기준에 해당하는 화면은 별도 화면 스펙에만 쓴다. 관련 문서에는 화면 스펙 경로와 시나리오 ID만 연결한다. |
| 기존 요구사항과 다른 동작을 고치는 버그 수정 | 기존 동작 명세와 해당 시나리오의 시험·증거. 요구사항 자체가 달라지지 않으면 새로 만들지 않는다. |
| API·메시지·파일의 교환 형식이나 호환성 변경 | 경계 계약과 관련 동작 명세 |
| 구성요소의 책임·의존 관계·배포 구조 변경 | 코드 지도와 관련 architecture view |
| 모듈 내부 책임·데이터 모델·처리 순서·초기화 시점 변경 | 코드 지도와 해당 모듈 설계 |
| 중요한 대안을 비교하고 설계 방향 선택 | 결정 기록과 결정이 반영될 기준 문서 |
| 이름 짓기·서식·코드 작성 방식 변경 | 작성 규칙 |
| 반복할 개발·검증·운영 작업 정리 | 실행 절차 |
| 이해하기 어려운 원리나 구현 설명 | 해설 |

작은 기능은 관련 요구사항과 동작 명세부터 시작하고, 교환 형식이나 중요한 설계 선택이 생기면 해당 문서를 연결한다. 영향이 없는 문서 유형은 빈 문서를 만들어 채우지 않는다.

## 문서 추가 절차

1. 독자가 답을 얻으려는 주된 질문을 하나 정한다.
2. 위 선택 기준과 표에서 디렉터리를 선택하고, 같은 질문을 다루는 기존 문서가 있는지 먼저 확인한다.
3. 해당 템플릿을 복제하고 자리표시자를 채운다.
4. 요구사항 ID와 관련 기준 문서를 상대 링크로 연결한다.
5. 디렉터리의 `README.md` 문서 목록을 갱신한다.
6. 링크, 채택 상태와 적용 범위, 검증 증거와 미결정 사항을 점검한다. 시험 추적의 기준은 해당 동작 명세의 수용 시험 표로 두고, 다른 문서에서는 같은 결과를 복제하지 않고 참조한다.

