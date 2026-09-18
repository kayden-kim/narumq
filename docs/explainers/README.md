# NaruMQ 기술 해설

이 디렉터리는 NaruMQ의 요구사항과 아키텍처 결정을 이해하는 데 필요한 기술 배경을 설명한다. 여기의 설명은 제품 계약을 새로 정의하지 않는다. 제품 요구는 [PRD](../requirements/PRD.md), 외부 동작은 [시스템 스펙](../specs/README.md), 구조 view는 [Architecture](../architectures/README.md), 기술 결정은 [ADR](../decisions/)을 기준으로 한다.

## 문서 목록

| 순번 | 문서 | 답하는 질문 | 주요 관련 항목 |
| --- | --- | --- | --- |
| 1 | [MQTT의 역사와 버전 진화: MQIsdp부터 MQTT 5.0까지](mqtt-history-and-version-evolution.md) | MQTT는 어떻게 탄생했고 MQIsdp, 3.1.1, 5.0은 무엇이 다르며 NaruMQ는 왜 특정 버전만 지원하는가? | MQTT-01, MQTT-13, CLIENT-01 |
| 2 | [MQTT 전달 보장과 세션](mqtt-delivery-guarantees.md) | QoS와 세션 설정은 메시지 보장을 어떻게 바꾸는가? | MQTT-02~05, CLU-06~09, CLIENT-01~07 |
| 3 | [MQTT QoS 0·1·2와 Will protocol 흐름](mqtt-qos-and-will-protocol-flows.md) | 각 QoS handshake와 Will 등록·발행은 packet 수준에서 어떻게 진행되는가? | MQTT-02~05, CLU-06~09, CLIENT-01~09 |
| 4 | [MQTT topic routing, retained message와 shared subscription](mqtt-topic-routing-retained-and-shared-subscriptions.md) | Topic name, filter, wildcard와 저장·공유 구독 상태는 delivery 대상을 어떻게 바꾸는가? | MQTT-06~10 |
| 5 | [Broker core의 계층 경계](broker-core-boundaries.md) | domain, application, port와 adapter의 의존성은 어떻게 나뉘는가? | NFR-11~15, ADR-0001 |
| 6 | [Boundary model과 mapping](boundary-models-and-mapping.md) | Domain entity, use case DTO, wire type과 persistence record는 무엇이 다르며 어디에서 변환하는가? | NFR-11~14 |
| 7 | [OpenAPI, controller와 error mapping](openapi-controller-and-error-mapping.md) | OpenAPI wire type은 controller, use case DTO와 안정된 API error로 어떻게 변환되는가? | API-01~09, ADR-0005 |
| 8 | [인증, tenant binding과 ACL](authentication-tenant-binding-and-acl.md) | TLS·mTLS·JWT/JWKS identity가 principal, tenant와 publish·subscribe 권한으로 어떻게 이어지는가? | SEC-01~12 |
| 9 | [세션 소유권과 fencing](session-ownership-and-fencing.md) | 여러 노드가 같은 세션을 동시에 수정하지 못하게 하려면 무엇이 필요한가? | CLU-04~10, DEP-08~10, NFR-07~10 |
| 10 | [WAL, snapshot과 crash recovery](wal-snapshot-and-crash-recovery.md) | WAL append, stable 저장, snapshot과 replay 중 어떤 상태가 crash 뒤에도 남는가? | CLU-06~12, NFR-07~10 |
| 11 | [Raft quorum과 durable ACK](raft-quorum-and-durable-ack.md) | Replicated, persisted, committed와 applied는 어떻게 다르며 성공 ACK는 언제 가능한가? | CLU-01~12, ADR-0002~0003 |
| 12 | [Go 동시성과 backpressure](go-concurrency-and-backpressure.md) | 많은 장기 연결을 다룰 때 goroutine·queue·GC를 어떻게 이해해야 하는가? | MQTT-11~12, CLU-12, NFR-01~06, ADR-0001 |
| 13 | [Go buffer ownership와 zero-copy](go-buffer-ownership-and-zero-copy.md) | Packet buffer의 owner와 lifetime을 어떻게 정하고 언제 copy·transfer해야 하는가? | NFR-01~06 |
| 14 | [결정적 test와 failure injection](deterministic-testing-and-failure-injection.md) | Clock, random, network, storage와 consensus를 어떻게 대체해 장애를 재현하는가? | TEST-01~17 |
| 15 | [Metric, log와 audit event](metrics-logs-and-audit.md) | 세 관측 기록은 무엇이 다르며 cardinality와 민감정보를 어떻게 통제하는가? | API-07~09, SEC-11, NFR-16~17 |
| 16 | [EKS 고가용성과 장애 도메인](eks-ha-failure-domains.md) | 세 broker를 어떤 장애 도메인으로 나눠야 실제 3-node HA가 되는가? | DEP-19~23, CLU-01~14, ADR-0007 |
| 17 | [안전한 노드 drain과 Will](safe-node-drain-and-will.md) | Kubernetes 재배포 중 세션을 어떻게 인계하고 Will을 어떻게 처리하는가? | DEP-01~23, CLIENT-03~09 |
| 18 | [AWS NLB와 MQTT 연결 lifecycle](aws-nlb-mqtt-connection-lifecycle.md) | NLB target 제외와 MQTT session 인계는 어떤 순서로 진행되는가? | DEP-03~07, DEP-12, DEP-22, ADR-0007 |

## 읽는 순서

처음 읽는다면 프로토콜 기초부터 브로커 내부 구현, 분산 시스템, 클라우드 운영 순서로 학습하는 것을 권장한다.

### 1단계: MQTT 프로토콜 기초와 메시지 모델
1. [MQTT의 역사와 버전 진화: MQIsdp부터 MQTT 5.0까지](mqtt-history-and-version-evolution.md)
2. [MQTT 전달 보장과 세션](mqtt-delivery-guarantees.md)
3. [MQTT QoS 0·1·2와 Will protocol 흐름](mqtt-qos-and-will-protocol-flows.md)
4. [MQTT topic routing, retained message와 shared subscription](mqtt-topic-routing-retained-and-shared-subscriptions.md)

### 2단계: 브로커 코어 아키텍처와 경계 모델
5. [Broker core의 계층 경계](broker-core-boundaries.md)
6. [Boundary model과 mapping](boundary-models-and-mapping.md)
7. [OpenAPI, controller와 error mapping](openapi-controller-and-error-mapping.md)

### 3단계: 멀티테넌시와 보안 격리
8. [인증, tenant binding과 ACL](authentication-tenant-binding-and-acl.md)

### 4단계: 분산 합의와 스토리지 영속성
9. [세션 소유권과 fencing](session-ownership-and-fencing.md)
10. [WAL, snapshot과 crash recovery](wal-snapshot-and-crash-recovery.md)
11. [Raft quorum과 durable ACK](raft-quorum-and-durable-ack.md)

### 5단계: Go 런타임 및 동시성 최적화
12. [Go 동시성과 backpressure](go-concurrency-and-backpressure.md)
13. [Go buffer ownership와 zero-copy](go-buffer-ownership-and-zero-copy.md)

### 6단계: 결정적 검증과 관측성
14. [결정적 test와 failure injection](deterministic-testing-and-failure-injection.md)
15. [Metric, log와 audit event](metrics-logs-and-audit.md)

### 7단계: Kubernetes 배포와 인프라 라이프사이클
16. [EKS 고가용성과 장애 도메인](eks-ha-failure-domains.md)
17. [안전한 노드 drain과 Will](safe-node-drain-and-will.md)
18. [AWS NLB와 MQTT 연결 lifecycle](aws-nlb-mqtt-connection-lifecycle.md)
