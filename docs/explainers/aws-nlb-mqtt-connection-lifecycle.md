# AWS NLB와 MQTT 연결 lifecycle

Amazon Network Load Balancer(NLB)에서 target을 제외해도 이미 연결된 MQTT 전송 제어 프로토콜(Transmission Control Protocol, TCP) connection이 다른 broker로 이동하지는 않는다. 안전한 rolling update에는 신규 연결 제외, NLB 설정 전파 대기, quorum에 복제된 session state, 의도적인 client reconnect와 `SAFE_TO_STOP` 검증이 필요하다. QoS 전이는 각각 wire effect보다 먼저 commit되며 별도 전체 session checkpoint는 없다. Planned drain과 `SAFE_TO_STOP`은 아직 구현되지 않았으므로 이 문서는 목표 설계 절차다.

| 항목 | 값 |
| --- | --- |
| 기준 독자 | AWS 로드밸런서 기초는 알지만 NLB의 장기 TCP 커넥션 관리와 EKS graceful drain 절차가 생소한 주니어 개발자 |
| 선수 문서 | [안전한 노드 drain과 Will](safe-node-drain-and-will.md) |
| 관련 기준 문서 | [PRD](../requirements/PRD.md) (DEP-03~07, DEP-12, DEP-22), [Drain과 rolling update 스펙](../specs/drain-and-rolling-update.md), [ADR-0007: AWS production deployment platform](../decisions/ADR-0007-aws-production-deployment-platform.md) (Proposed) |
| 대상 version·확인일 | `EKS_EC2_HA` Profile, 2026-08-16 |

## 읽고 나면 답할 수 있는 질문

- Kubernetes readiness, NLB target state, broker drain state와 `SAFE_TO_STOP`을 구분할 수 있다.
- Target deregistration이 기존 MQTT connection을 다른 broker로 옮기지 못하는 이유를 설명할 수 있다.
- 계획된 target 제외와 health check 실패가 기존 connection에 미치는 차이를 설명할 수 있다.
- NLB idle timeout과 MQTT Keep Alive의 역할을 구분할 수 있다.

## 문제 상황: target은 빠졌는데 connection은 남아 있다

Draining broker가 readiness를 `false`로 바꿨고 NLB target도 `draining` 상태가 됐다고 하자. 새 connection은 줄지만 기존 MQTT TCP connection은 그대로 남을 수 있다. NLB는 그 connection 안의 Client ID, subscription, inflight packet과 owner epoch를 알지 못한다.

따라서 “target 제외 완료”와 “broker 종료 안전”은 같은 사건이 아니다. Network 경로와 MQTT session 경로를 따로 확인해야 한다.

## 필수 용어: 네 가지 서로 다른 상태

| 상태 | 답하는 질문 | 안전한 종료를 단독으로 보장하는가? |
| --- | --- | --- |
| Kubernetes readiness | 이 Pod를 새 traffic 후보로 볼 수 있는가? | 아니오. Controller·target 반영이 지연될 수 있다. |
| NLB target health·lifecycle | NLB가 이 target에 새 TCP 연결을 보낼 수 있는가? | 아니오. 기존 connection과 MQTT session을 모른다. |
| Broker drain state | Session·inflight·replication을 인계했는가? | 아니오. Client 재접속과 target 제외가 함께 필요하다. |
| `SAFE_TO_STOP` | Process를 종료해도 되도록 모든 barrier가 확인됐는가? | 예. 외부 rollout controller가 이 상태 뒤에만 Pod 삭제를 요청한다. |

Readiness 실패를 `SAFE_TO_STOP`과 같게 취급하면 안 된다. NLB deregistration delay가 끝났다는 사실도 broker의 active connection, owner epoch 또는 replication backlog가 0이라는 뜻이 아니다.

## 정상 흐름: 계획된 target 제외

아래 화살표는 readiness 변경부터 NLB target 제외, client 재접속과 session takeover까지의 runtime control flow다.

```mermaid
sequenceDiagram
    participant R as Rollout Controller
    participant B as Draining Broker
    participant K as Kubernetes/AWS Controller
    participant L as NLB Target Group
    participant C as MQTT Client
    participant Q as Quorum
    participant N as Remaining Broker

    R->>B: drain 시작
    B->>Q: DRAINING commit
    B-->>K: readiness=false
    K->>L: endpoint/target 제외 반영
    Note over K,L: controller와 NLB 전파 지연
    L-->>B: 신규 connection 유입 감소·중단
    Note over O,Q: Session과 QoS 진행 상태는 각 전이에서 이미 commit됨; 별도 전체 checkpoint 없음
    B-->>C: MQTT 5 redirect 또는 계획된 연결 종료
    C->>L: 공통 endpoint로 재접속
    L->>N: 새 TCP connection
    N->>Q: 더 큰 owner epoch와 session 복구
    Q-->>N: takeover commit
    N-->>B: 새 owner 확인
    B-->>R: SAFE_TO_STOP
    R->>K: Pod 삭제 요청
```

Target 제외 요청 직후에도 전파 중 새 연결이 들어올 수 있다. Broker는 `DRAINING` 상태에서 이를 정상 READY connection처럼 오래 유지하지 말고, admission 정책에 따라 거절하거나 reconnect 대상으로 유도해야 한다. 전파 대기는 측정된 최악값을 기준으로 하며 고정된 임의 sleep만으로 완료를 판정하지 않는다.

## 자주 하는 오해와 trade-off: deregistering과 unhealthy는 같은 경로가 아니다

NLB target에는 계획된 **deregistration**과 health check 실패로 인한 **unhealthy** 경로가 있다.

- Deregistering target에는 새 connection을 보내지 않으며, 기존 connection은 deregistration delay 동안 유지될 수 있다.
- Deregistration delay가 끝난 뒤 connection을 종료할지는 target group의 connection termination 속성에 영향을 받는다.
- Unhealthy target의 established connection 종료에는 별도 target health 속성이 적용된다.

Readiness 변경이 실제 환경에서 target deregistration으로 이어지는지, health check 실패로 먼저 관찰되는지는 AWS Load Balancer Controller version, target type와 health check 설정에 따라 검증해야 한다. Unhealthy 경로가 established connection을 예상보다 일찍 끊으면 새 node가 committed session state를 복구하기 전에 RST가 발생할 수 있다.

따라서 release profile은 “readiness를 내린다”만 기록하지 않고 다음을 증거로 남긴다.

1. Kubernetes endpoint 제외 시각
2. AWS controller의 target 변경 시각
3. NLB target state 전이와 마지막 신규 connection 시각
4. Established connection 종료 시각과 원인
5. Broker의 `SAFE_TO_STOP` barrier 완료 시각

## NLB가 이전하지 않는 것

NLB는 새 TCP connection의 target을 고른다. 다음 상태를 broker 사이에 이전하지 않는다.

- MQTT Client ID와 authentication context
- Subscription과 Session Expiry
- QoS 1·2 inflight packet identifier와 handshake
- Offline queue와 retained state
- Will과 Will Delay timer
- NaruMQ session owner epoch

Client는 공통 NLB endpoint에 새 TCP connection을 만들고 같은 session key로 CONNECT해야 한다. 새 broker가 quorum에서 더 큰 owner epoch를 얻고 session을 복구한 뒤에만 takeover가 완료된다.

## MQTT version에 따른 연결 종료

| Client | Broker가 할 수 있는 일 | Will에 대한 주의 |
| --- | --- | --- |
| MQTT 5, Server Reference 지원 | `DISCONNECT 0x9C`와 Server Reference로 임시 이동을 알린다. | Server DISCONNECT 자체가 Will을 무조건 제거하지 않는다. 충분한 Will Delay 안에 같은 session으로 재접속해야 억제할 수 있다. |
| MQTT 5, Server Reference 미지원 | 공통 endpoint로 재접속하도록 SDK 계약을 적용하고 연결을 닫는다. | Will Delay, reconnect backoff와 Session Expiry를 함께 검증한다. |
| MQTT 3.1.1 | TCP connection을 닫거나 client가 먼저 정상 DISCONNECT하도록 협력한다. | Server가 연결을 닫으면 설정된 Will이 표준상 발행될 수 있다. |

NLB가 FIN 또는 RST로 TCP를 끝냈다는 이유만으로 broker가 이를 “계획된 종료이므로 Will 없음”으로 재분류하면 안 된다. Broker가 protocol connection의 종료 원인과 Will timer를 소유해야 한다. 계획 drain의 Will 기대값은 [안전한 노드 drain과 Will](safe-node-drain-and-will.md)을 따른다.

## Release profile에 고정할 NLB 설정

| 설정 | 결정해야 할 질문 | 잘못 설정했을 때의 위험 |
| --- | --- | --- |
| Target type (`ip` 또는 `instance`) | NLB target이 Pod IP인가 worker와 NodePort인가? | Target 제외 시점, source IP와 health 범위를 잘못 해석한다. |
| Health check protocol·interval·threshold | Readiness와 target health가 언제 연결되는가? | 신규 connection 제외가 늦거나 transient failure로 connection이 끊긴다. |
| Deregistration delay | 기존 connection을 얼마 동안 유지할 수 있는가? | 너무 짧으면 강제 종료, 너무 길면 rollout 정체가 생긴다. |
| Deregistration connection termination | Delay 뒤 남은 TCP connection을 종료할 것인가? | Broker barrier 전에 connection을 강제로 끊을 수 있다. |
| Unhealthy connection termination | Unhealthy 판정 때 established connection을 즉시 끝낼 것인가? | Planned readiness 변경이 reconnect storm과 예상 Will을 만들 수 있다. |
| TCP idle timeout | 유휴 connection을 NLB가 언제 종료하는가? | MQTT Keep Alive·network timeout보다 짧으면 불필요한 reconnect가 발생한다. |
| Client IP preservation | Broker가 원본 client IP를 어디서 얻는가? | ACL·audit·rate limit identity가 달라질 수 있다. |
| Proxy Protocol v2 | Listener가 MQTT packet 앞의 proxy header를 해석하는가? | 지원하지 않는 listener에 켜면 첫 MQTT packet parsing이 실패한다. |
| Cross-zone load balancing | 연결이 AZ 사이에 어떻게 분산되는가? | AZ별 target·client 편차와 inter-AZ traffic 비용이 달라진다. |

표의 값은 독립적으로 정하지 않는다. 예를 들어 deregistration delay가 broker의 최대 drain 시간보다 짧아도 connection termination을 비활성화했다고 안전하다고 단정할 수 없다. Unhealthy 경로, client timeout과 Pod termination grace period가 별도로 connection을 끝낼 수 있다.

## Idle timeout과 MQTT Keep Alive

NLB의 TCP idle timeout과 MQTT Keep Alive는 서로 다른 timer다.

- NLB idle timeout은 TCP flow에 traffic이 없는 시간을 본다.
- MQTT Keep Alive는 client가 protocol packet을 보내지 않는 최대 간격과 broker의 liveness 판정을 정한다.
- MQTT `PINGREQ`와 `PINGRESP`는 protocol traffic이므로 정상적인 keep-alive 교환은 idle flow를 활성 상태로 유지한다.

하지만 client Keep Alive가 NLB idle timeout보다 길거나 0이고 application traffic도 없으면 NLB가 먼저 연결을 제거할 수 있다. 지원 SDK matrix는 Keep Alive 범위를 고정하고 NLB idle timeout보다 충분히 짧은 heartbeat를 검증해야 한다.

## 실패 흐름을 구분해 관측하기

| 관측 결과 | 가능한 원인 | 필요한 판단 |
| --- | --- | --- |
| Readiness 뒤에도 신규 connection 유입 | Endpoint·target 전파 지연 | 전파 대기와 마지막 신규 connection 시각을 확인한다. |
| 기존 connection이 즉시 RST | Unhealthy termination, Pod 종료 또는 network failure | Planned deregistration 경로와 분리해 조사한다. |
| Reconnect가 같은 draining broker로 돌아옴 | Target 제외 미완료 또는 다른 endpoint 사용 | Client endpoint와 NLB target state를 확인한다. |
| Client가 새 broker에 연결됐지만 session 없음 | Client ID·tenant·expiry 불일치 또는 committed session state 복구 실패 | NLB 문제가 아니라 MQTT session 계약을 확인한다. |
| 예상 밖 Will 대량 발행 | Will Delay 부족, MQTT 3.1.1 server close, 강제 connection 종료 | Protocol version과 종료 주체별 metric을 분리한다. |
| Target은 drained지만 Pod 삭제 불가 | Active owner·replication backlog·quorum 위험 | `SAFE_TO_STOP` barrier 실패를 정상적으로 차단한 상태다. |

Target metric과 broker metric을 한 timeline에 놓아야 “NLB에서 사라졌다”와 “session 인계가 끝났다” 사이의 간격을 확인할 수 있다.

## NaruMQ에서 찾을 위치

규범적 요구는 [PRD](../requirements/PRD.md)의 DEP-03~DEP-07, DEP-12와 DEP-22를 따른다. Resource와 control path는 [EKS on EC2 deployment architecture](../architectures/eks-ec2-deployment.md), 정확한 drain state와 barrier는 [Drain과 rolling update 스펙](../specs/drain-and-rolling-update.md), deployment profile 선택은 [ADR-0007: AWS production deployment platform](../decisions/ADR-0007-aws-production-deployment-platform.md) (Proposed), 전체 Kubernetes 종료 흐름은 [안전한 노드 drain과 Will](safe-node-drain-and-will.md)을 참고한다. NLB는 [EKS 고가용성과 장애 도메인](eks-ha-failure-domains.md)의 storage·quorum 독립성을 대신하지 않는다.

## 확인 질문

1. NLB target이 `draining`이어도 broker의 active connection이 0이 아닐 수 있는 이유는 무엇인가?
2. Readiness 변경부터 `SAFE_TO_STOP`까지 NLB와 session takeover가 각각 확인해야 할 사건을 순서대로 말해 보라.
3. Unhealthy connection termination과 planned deregistration를 같은 metric으로 보면 어떤 장애 원인을 놓치는가?
4. MQTT Keep Alive가 NLB idle timeout보다 길 때 idle connection에 어떤 일이 생길 수 있는가?

## 참고 자료

- [Amazon EKS에서 Network Load Balancer 사용](https://docs.aws.amazon.com/eks/latest/userguide/network-load-balancing.html)
- [Network Load Balancer target group 속성](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/edit-target-group-attributes.html)
- [Network Load Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html)
- [AWS Load Balancer Controller annotations](https://kubernetes-sigs.github.io/aws-load-balancer-controller/latest/guide/service/annotations/)
- [OASIS MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [OASIS MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
