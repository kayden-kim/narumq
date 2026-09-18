# Architecture 문서 작성 규칙

[상위 규칙](../AGENTS.md)을 따르며 다음을 추가한다.

- 한 문서는 system, component, deployment, security 등 하나의 구조 질문에 답한다.
- 첫 부분에 관점 상태, 문서 상태, 구현 증거, 기준 문서와 최종 확인일을 쓴다.
- Current와 Target을 한 diagram에 섞지 않는다.
- Component 책임, dependency 방향과 경계를 함께 설명한다.
- Diagram의 모든 화살표가 dependency, runtime call, data flow 또는 control flow 중 무엇인지 밝힌다.
- 논리 component와 process, host, container, cloud resource 같은 물리 자원을 구분한다.
- Architecture는 기준 문서의 결론을 조합하며 새 요구사항이나 결정을 만들지 않는다.

## 추가 작성 지침

- **View 상태 및 메타데이터:** 관점 상태(`Current`, `Target`, `Transitional`, `Historical`), 문서 상태(`Draft`, `Verified`), 구현 증거(Source tree, manifest, test artifact 등)를 명시한다. `Verified`는 실제 구현 증거를 확인했을 때만 부여한다.
- **Dependency 및 Port 경계:** Source dependency는 안쪽 policy를 향해야 하며, Domain은 framework·storage·network를 직접 참조하지 않는다. Port interface는 사용하는 application 측이 소유하며 dependency cycle은 허용하지 않는다.
- **Deployment 및 Security 관점:** Deployment view는 process, Pod, host, 가용 영역, volume과 논리 `nodeId`를 구분한다. Security view는 network zone보다 identity와 신뢰 근거를 우선해 boundary를 설명한다.
- **동기화:** Requirement, spec, contract 또는 Accepted ADR로 구조가 바뀌면 관련 architecture view를 같은 변경에서 갱신한다.
