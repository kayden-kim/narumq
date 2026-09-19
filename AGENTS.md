# marumq 작업 지침

- 문서를 SSOT(Single Source of Truth)로 삼는다.
- 모든 코드는 관련 문서에 명시된 요구사항과 설계를 근거로 작성한다.
- 모든 설명과 문서는 도메인 및 전문 기술 지식이 없는 신입 주니어 개발자를 대상으로 작성하며, 전문 용어는 쉽게 풀어 설명하고 필요한 배경 지식을 함께 제공한다.
- 전문 용어 대신 구체적인 동작을 먼저 말한다. 정확한 이름을 배우는 것이 유용하면 쉬운 설명 뒤에 용어를 붙이고, 이후에도 뜻을 떠올릴 수 있게 쓴다. 예를 들어 "같은 요청이 여러 번 와도 결제가 한 번만 되게 하는 성질을 멱등성이라고 합니다"처럼 설명한다.
- 외래어가 아닌 표현은 쉬운 한국어로 작성한다. 불필요하게 영어 표현을 섞지 않으며, 꼭 필요한 외래어·기술 용어·코드 식별자는 사용하되 처음 나올 때 쉬운 한국어 뜻과 현재 설명에서의 역할을 함께 설명한다.
- 변경 전에 관련 문서와 유사한 기존 구현을 확인하고, 프로젝트의 구조([Project structure](docs/architectures/project-structure.md)), 역할 분담, 이름 짓기, 코드 작성 방식 등 기존 패턴과 규칙을 우선 준수하며, Go 코드 작성 시 [Go coding conventions](docs/conventions/go-coding-conventions.md)을 따른다.
- 클린 아키텍처(Clean Architecture / Ports and Adapters) 원칙을 준수한다: 소스 코드 의존성은 항상 바깥쪽(`internal/adapters`)에서 안쪽 정책(`internal/application`, `internal/domain`)으로만 향해야 하며, `domain`은 외부 프레임워크, 라이브러리, I/O 패키지를 일절 import하지 않는다. 인터페이스(Port)는 이를 사용하는 `application` 계층이 소유하고, 외부 데이터 타입은 어댑터 경계에서 DTO 또는 도메인 값으로 변환하며, 구체 구현체 조립은 `cmd/narumq`(Composition Root)에서만 수행한다([Broker core architecture](docs/architectures/broker-core.md), [Broker core의 계층 경계](docs/explainers/broker-core-boundaries.md) 참조).
- 모든 스펙([시스템 스펙](docs/specs/README.md))의 시나리오는 테스트 케이스로 100% 추적 및 커버되어야 한다. 단위 테스트가 가능한 도메인 규칙과 상태 머신 로직은 외부 의존성 없는 유닛 테스트로 100% 검증함을 기본으로 하고, 노드 drain이나 인프라 장애 등 시스템 시나리오는 통합·E2E·카오스 테스트 계층에 누락 없이 매핑하여 검증하며, 테스트와 기준 검증이 없는 코드는 완료로 간주하지 않는다([Performance와 verification](docs/specs/performance-and-verification.md) 참조).
- 기존 방식으로 요구사항을 충족할 수 있으면 해당 방식을 재사용한다. 확장이 필요한 경우 기존 구조와 패턴에 어울리는 최소한의 변경으로 구현하고, 유사한 기능에도 일관되게 적용할 수 있는 규약으로 확장한다.
- 새로운 패턴이나 규칙이 꼭 필요한 경우 도입 이유와 기존 방식과의 관계를 관련 문서에 명시하고, 변경 범위 안에서 일관되게 적용한다. 요청과 무관한 구조 변경이나 광범위한 재작성은 피한다.

