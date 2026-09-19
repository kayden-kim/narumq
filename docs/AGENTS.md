# 문서 작성 규칙

이 파일은 `docs/`와 모든 하위 디렉터리에 적용한다. 더 가까운 하위 `AGENTS.md`가 구체적인 규칙을 추가하며, 충돌하면 하위 규칙을 우선한다.

## 문서 배치

| 디렉터리 | 포함할 내용 | 포함하지 않을 내용 |
| --- | --- | --- |
| `requirements/` | 문제, 목표, 범위, 제약, 우선순위, 수용 기준 | 구현 선택, 상세 형식, 실행 절차 |
| `specs/` | 외부 동작, 상태 전이, 오류, protocol flow | 선택의 역사, 장문의 배경 |
| `contracts/` | producer·consumer, schema, version, 호환성 | 제품 배경, 전체 상태 전이, 운영 절차 |
| `architectures/` | component, dependency, data·control flow, trust boundary | 선택의 역사, 정확한 schema, 실행 절차 |
| `decisions/` | 배경, 결정, 대안, 결과, 재검토 조건 | 요구사항 복제, 사용 안내 |
| `explainers/` | 개념, 원리, mental model, trade-off | 규범 요구, 작업 절차 |
| `conventions/` | naming, source·test·formatting 규칙과 gate | 제품 동작, 구조 결정, 운영 절차 |
| `how-to/` | 전제 조건, 단계, 검증, 복구 | 결정 근거의 반복 설명 |

## 연결 원칙

- Requirement는 관련 spec과 안정된 요구사항 ID를 연결한다.
- Spec은 requirement를 연결하고, 정확한 형식은 contract로 분리한다.
- Contract는 기준 spec, producer, consumer와 호환성 범위를 연결한다.
- Architecture는 requirement, spec, contract와 ADR의 결론을 구조 view로 조합한다.
- ADR은 영향을 받는 기준 문서를 연결한다.
- Convention은 관련 ADR과 architecture view를 연결한다.
- Explainer와 how-to는 기준 문서를 바꾸지 않고 링크한다.
- 같은 사실의 기준 문서는 하나만 둔다.

```text
Requirement → Spec → Contract
Requirement / Spec / Contract / ADR → Architecture View
ADR / Architecture View → Convention
Spec / Contract → ADR → Explainer
Spec / Contract / ADR → How-to
```

- Architecture는 `Current`와 `Target`을 구분하고 구현 증거가 없으면 `Verified`로 표시하지 않는다.

## 작성 원칙

- 제목과 첫 문단만으로 목적과 범위를 알 수 있게 쓴다.
- 한 문서에는 하나의 주된 독자와 목적을 둔다.
- 수용 기준과 규범 문장은 관찰하거나 시험할 수 있게 쓴다.
- 미확정 값을 임의의 기본값으로 만들지 말고 책임자와 확정 위치를 기록한다.
- 결론을 뒤집는 결정은 기존 ADR을 덮어쓰지 않고 새 ADR로 대체한다.
- 문서 내용은 MECE(상호 배타적·전체 포괄) 원칙에 따라 구성한다.
- 프로젝트 문서는 한국어를 기본으로 작성한다.
- 공식 protocol 용어, 제품명, API path, field, Metric name와 source identifier는 원문을 유지하고 필요한 경우 backtick으로 표시한다.
- 일반론보다 NaruMQ에 필요한 정보, 제약과 판단을 우선하며 배경 지식이 길어지면 `docs/explainers/`로 분리한다.
- 독자가 결과와 문제를 먼저 이해한 뒤 실제 입력이 코드와 상태를 거쳐 결과가 되는 과정을 따라갈 수 있는 설명 문서를 작성하거나 고칠 때는 `explain` 스킬을 사용한다. 배경 개념, 구조와 호출 관계, 주요 결정의 이유, 핵심 코드 흐름, 실패 조건과 구현 경계를 실제 파일·문서·테스트에 연결해 설명하고, 확인한 사실과 추정을 구분한다.

## 변경과 이동

- 기존 내용을 복제해 새 문서를 만들지 않고 기준 문서를 이동하거나 분리한다.
- 문서를 이동하거나 이름을 바꾸면 저장소 전체의 상대 링크를 함께 수정한다.
- 호환용 stub은 외부 링크를 유지해야 할 명확한 이유가 있을 때만 남긴다.
- 구현이나 아키텍처가 바뀌면 관련 requirement, spec, contract, architecture view, convention, ADR와 how-to를 같은 변경에서 동기화한다.

## 완료 점검

1. 문서가 올바른 디렉터리에 있는가?
2. 서로 다른 문서 유형의 관심사가 섞이지 않았는가?
3. 요구사항 ID와 기준 문서 링크가 유효한가?
4. 상태, 미결정 사항과 검증 증거가 사실과 일치하는가?
5. 내부 상대 링크가 모두 유효한가?
6. Architecture view가 Current와 Target, Verified와 Draft를 구분하는가?
7. Source 예제와 이름이 Active convention을 따르는가?
