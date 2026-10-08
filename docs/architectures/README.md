# NaruMQ Architecture Views

이 디렉터리는 NaruMQ 코드 지도와 system, component, cluster, deployment, security 관점의 구조를 보여준다. Architecture view는 기준 문서의 결론을 연결한 구조 snapshot이다. 제품 요구, 외부 동작, 정확한 경계 형식이나 선택 근거를 새로 정의하지 않는다.

## 상태 표기

| 관점 상태 | 의미 |
| --- | --- |
| `Current` | 구현 또는 배포 증거와 대조한 현재 구조 |
| `Target` | Requirement와 ADR이 지향하는 목표 구조 |
| `Transitional` | Current에서 Target으로 이동하는 임시 구조 |
| `Historical` | 대체되어 참고용으로만 남긴 구조 |

문서 상태 `Draft`는 아직 구현 증거로 검증하지 않았음을 뜻한다. `Verified`는 연결된 source·manifest·test artifact와 view가 일치할 때만 사용한다.

관점 상태(`Current`/`Target`/`Transitional`/`Historical`)는 문서가 설명하는 구조 시점을 뜻하고, 문서 상태(`Draft`/`Active`/`Verified`)는 채택·증거 수준을 뜻한다. `Target` view를 `Verified`로 표시해도 목표 설계가 코드에 구현됐다는 의미는 아니다. 코드 지도는 확인한 코드 위치만 `Current`로 기록하며, Project structure 전체의 상태를 그 절에 일괄 적용하지 않는다.

## 문서 목록

| 문서 | 답하는 질문 | 관점 상태 | 주요 기준 문서 |
| --- | --- | --- | --- |
| [System overview](system-overview.md) | 외부 actor와 NaruMQ의 주요 실행 단위는 무엇인가? | Target / Draft | PRD, 전체 system spec |
| [Broker core architecture](broker-core.md) | Broker 내부 책임과 source dependency는 어떻게 나뉘는가? | Target / Draft | ADR-0001, NFR-11~15 |
| [Project structure (코드 지도 절 + 구조 view)](project-structure.md) | 실제 진입 경로와 package 배치, 현재 구현 사실과 목표 tree는 어떻게 구분되는가? | Current code map section + Target tree / Active | ADR-0001, NFR-11~15, TEST-01~17 |
| [Cluster architecture](cluster-architecture.md) | Quorum, ownership, routing과 durable state는 어떻게 연결되는가? | Target / Draft | Cluster spec, ADR-0002~0004 |
| [EKS on EC2 deployment architecture](eks-ec2-deployment.md) | Logical broker node는 NLB·EKS·EC2·EBS에 어떻게 대응하는가? | Target / Draft | DEP-19~23, ADR-0006~0007 |
| [Security boundaries](security-boundaries.md) | Client·node·관리 plane의 trust boundary와 enforcement point는 어디인가? | Target / Draft | Tenant와 security spec, Management API·Audit event contract |

## 권장 읽기 순서

1. Project structure의 코드 지도 절
2. Broker core architecture
3. System overview
4. Cluster architecture
5. EKS on EC2 deployment architecture
6. Security boundaries

구체적인 선택 이유가 필요하면 [ADR catalog](../decisions/README.md), source 작성 규칙은 [Convention catalog](../conventions/README.md), protocol과 분산 원리는 [기술 해설](../explainers/README.md)을 읽는다. 외부 동작과 정확한 형식의 기준은 각각 [시스템 스펙](../specs/README.md)과 [Contract catalog](../contracts/README.md)이다.

## 현재 한계

System overview, broker core, cluster, deployment, security view는 현재 `Target / Draft`다. [Project structure](project-structure.md)는 `Active`이며 구현된 package facts와 명시적인 `Target repository tree`를 함께 담는다. `Target` 경로만으로 구현 완료나 production 지원을 주장하지 않으며, 관점 상태와 구현 증거는 각 문서 metadata 및 연결된 source·test evidence로 판단한다.
