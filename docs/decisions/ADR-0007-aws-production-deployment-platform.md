# ADR-0007: 초기 AWS production profile은 Amazon EKS on EC2를 사용한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 제안일 | 2026-08-16 |
| 결정일 | 2026-10-13 |
| 결정자 | 사용자 |
| 관련 요구사항 | DEP-01~DEP-24, CLU-01~CLU-14, NFR-07~NFR-10 |
| 관련 스펙 | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md), [Drain과 rolling update](../specs/drain-and-rolling-update.md), [Performance와 verification](../specs/performance-and-verification.md) |
| 관련 Architecture | [EKS on EC2 deployment](../architectures/eks-ec2-deployment.md), [System overview](../architectures/system-overview.md) |
| 관련 결정 | [ADR-0003](ADR-0003-durable-ack-barrier.md), [ADR-0004](ADR-0004-session-ownership-and-fencing.md), [ADR-0006](ADR-0006-external-rollout-controller.md) |
| 관련 Contract | [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md), [Management API v1](../contracts/management-api-v1.md), [MQTT client compatibility v1](../contracts/mqtt-client-compatibility-v1.md) |
| 관련 해설 | [EKS 고가용성과 장애 도메인](../explainers/eks-ha-failure-domains.md), [AWS NLB와 MQTT 연결 lifecycle](../explainers/aws-nlb-mqtt-connection-lifecycle.md) |

## 1. 결정

NaruMQ의 초기 AWS production deployment profile인 `EKS_EC2_HA`는 Amazon EKS의 EC2 worker를 사용한다.

기준 profile은 다음 조합으로 한정한다.

- 서로 다른 세 가용 영역에 quorum broker Pod를 하나씩 배치한다.
- 각 broker는 다른 broker와 공유하지 않는 Amazon EBS CSI persistent volume을 사용한다.
- MQTT TCP/TLS listener는 Amazon Network Load Balancer로 노출한다.
- 계획된 교체는 [ADR-0006](ADR-0006-external-rollout-controller.md)의 전용 rollout controller와 `SAFE_TO_STOP` gate를 사용한다.
- 지원하는 EKS·Kubernetes version, EC2 instance family, EBS 설정과 NLB target 속성은 release artifact에 고정한다.

Amazon ECS on EC2는 별도 lifecycle adapter가 같은 배포·장애 시험을 통과할 때까지 `Experimental`로 둔다. Amazon EKS Fargate와 Docker Compose는 초기 production HA profile로 지원하지 않는다.

Qualification과 BP-02 release comparison에 사용할 profile을 다음처럼 선택한다.

- EKS Kubernetes 1.37과 EKS-optimized Amazon Linux 2023 ARM64를 사용한다. 2026-10-12 기준으로 1.37은 AWS EKS의 최신 standard-support version이며 지원 종료일은 2027-12-01이다.
- `m7g.large` worker 3대를 서로 다른 AZ에 하나씩 둔다. 각 노드에는 broker Pod 하나를 배치한다.
- Broker마다 20 GiB `gp3` EBS volume을 둔다. 기본 성능은 3,000 IOPS와 125 MiB/s이며 추가 성능은 provision하지 않는다.
- `m7g.large`는 2 vCPU와 8 GiB memory를 제공한다. Broker Pod의 CPU·memory requests와 limits는 각각 `1 CPU`, `4 GiB`로 맞춰 NaruMQ와 EMQX에 같은 자원을 준다. 나머지 nominal capacity는 OS와 Kubernetes add-on에 남긴다. 실제 allocatable 자원과 N-1 capacity는 시험한다.
- NLB는 `IP` target과 TCP listener를 사용한다. TLS는 broker에서 종료해 mTLS client certificate identity를 보존한다.
- AWS Load Balancer Controller는 2.7.2 이상을 사용한다. 실제 controller·AMI·add-on version과 image digest는 run artifact에 고정한다.

이 값은 qualification profile의 선택이다. 실제 AWS/EBS 장애 시험, production workload 수집과 성능 gate를 통과하기 전에는 production 지원 선언이 아니다. Broker image 공급 경로, EBS 암호화·snapshot 정책, NLB health check·deregistration·idle timeout과 운영용 controller 설정은 별도 고정이 필요하다.

## 2. 배경

NaruMQ는 단순한 stateless HTTP service가 아니다. 장기 TCP 연결, quorum membership, session ownership, durable ACK, per-node stable storage와 drain 완료 여부가 서로 연결된다. Runtime만 컨테이너를 실행할 수 있다고 해서 같은 고가용성과 데이터 보장을 제공하지는 않는다.

현재 요구사항과 스펙은 다음 Kubernetes primitive를 전제로 구체화돼 있다.

- readiness와 Pod lifecycle
- required anti-affinity와 topology spread
- PodDisruptionBudget
- EBS CSI persistent volume과 volume topology
- Pod 삭제 전 drain을 승인하는 전용 controller

초기 release에서 여러 orchestration model을 동시에 지원하면 node identity, storage attachment, target 제외, 강제 종료와 rollout gate를 각각 다시 설계하고 검증해야 한다. 먼저 하나의 기준 profile을 선택해 제품 보장과 시험 조건을 고정할 필요가 있다.

## 3. 결정 요인

1. 3-node quorum을 EC2 instance와 가용 영역 장애로부터 독립적으로 배치할 수 있어야 한다.
2. Broker별 stable storage의 가용 영역 결합과 재연결 과정을 명시할 수 있어야 한다.
3. 장기 MQTT 연결을 신규 target에서 제외하고 application drain을 완료한 뒤 process를 종료할 수 있어야 한다.
4. 기존 Kubernetes 기반 요구사항과 rollout controller 설계를 재사용할 수 있어야 한다.
5. 단일 노드·단일 가용 영역 장애, rolling update와 rollback을 자동 검증할 수 있어야 한다.
6. 초기 production 지원 범위를 작게 유지하면서 다른 AWS runtime으로 확장할 경계를 남겨야 한다.

## 4. 선택 근거

### 4.1 장애 도메인

EKS on EC2는 Pod, EC2 instance와 가용 영역을 scheduler constraint로 분리할 수 있다. 기준 profile은 세 broker를 세 가용 영역에 하나씩 배치해 한 EC2 instance 또는 한 가용 영역의 손실이 두 quorum member를 동시에 제거하지 않도록 한다.

이 배치만으로 안전성이 완성되지는 않는다. Pod 이름, IP와 EC2 instance ID는 cluster `nodeId`가 아니며, session owner epoch와 저장 경계 fencing은 [ADR-0004](ADR-0004-session-ownership-and-fencing.md)를 따라야 한다.

### 4.2 Stable storage

Amazon EBS volume은 해당 가용 영역 안에서 복제되고 같은 가용 영역의 EC2 instance에 연결된다. 따라서 각 broker의 volume은 process·instance 수명과 분리된 stable storage를 제공하지만, 다른 가용 영역의 application replica를 대신하지 않는다.

NaruMQ의 durable ACK는 [ADR-0003](ADR-0003-durable-ack-barrier.md)에 따라 서로 다른 broker의 stable storage에 quorum write가 완료돼야 한다. “EBS가 내부 복제된다”와 “NaruMQ quorum이 두 독립 copy를 commit했다”를 같은 보장으로 취급하지 않는다.

### 4.3 연결 lifecycle

NLB는 Layer 4에서 MQTT TCP/TLS 연결을 분산한다. Target 제외는 새 연결의 유입을 제어하지만 이미 만들어진 TCP 연결이나 MQTT session을 다른 broker로 이전하지 않는다. Rollout controller, broker drain, NLB target lifecycle과 client reconnect가 함께 동작해야 한다.

기존 Kubernetes controller와 `SAFE_TO_STOP` barrier를 그대로 사용할 수 있다는 점이 초기 profile의 핵심 이점이다. 세부 흐름은 [AWS NLB와 MQTT 연결 lifecycle](../explainers/aws-nlb-mqtt-connection-lifecycle.md)에서 설명한다.

### 4.4 검증 표면

하나의 기준 profile은 다음 값을 release artifact와 자동 시험에 고정할 수 있게 한다.

- Kubernetes·EKS version과 AWS Load Balancer Controller version
- Worker CPU architecture, EC2 instance family, AMI와 image digest
- Production broker image build·publish 경로, registry와 immutable image digest
- Pod·EC2·가용 영역 placement
- StorageClass, EBS encryption·volume type·IOPS와 snapshot 정책
- NLB target type, health check, deregistration, connection termination와 idle timeout 정책
- `terminationGracePeriodSeconds`, load balancer 전파 대기와 drain deadline

값을 고정하지 않은 “EKS 호환” 선언은 재현 가능한 production 보장이 아니다.

## 5. 대안

| 대안 | 장점 | 제외 또는 보류 이유 |
| --- | --- | --- |
| Amazon ECS on EC2 | Kubernetes control plane 없이 EC2 container runtime과 NLB를 사용할 수 있다. | Task Protection, target 제외, drain 승인, stable `nodeId`, persistent volume과 stop timeout을 연결하는 별도 adapter가 필요하다. 동일 수용 시험 전에는 `Experimental`이다. |
| Amazon EKS Fargate | Worker 운영 부담을 줄일 수 있다. | EBS volume을 Fargate Pod에 mount할 수 없어 현재 per-node stable storage 전제와 맞지 않는다. Platform patch에 따른 Pod 교체도 별도 종료 검증이 필요하다. |
| Docker Compose | 로컬 개발과 protocol 시험이 단순하다. | 단일 host 장애가 모든 container와 volume을 함께 제거한다. 가용 영역, scheduler, PDB와 cloud load balancer lifecycle을 검증할 수 없다. |
| Self-managed Kubernetes on EC2 | Kubernetes primitive와 설정 자유도가 높다. | Control plane과 worker lifecycle 운영 범위가 커지며 초기 제품 보장에 직접 기여하지 않는다. |
| 여러 profile 동시 production 지원 | 배포 선택지가 넓다. | Runtime별 lifecycle·storage·identity 실패 조합 때문에 release gate가 분산된다. 초기 검증 비용과 보장 불확실성이 커진다. |

## 6. 결과

### 긍정적 결과

- 요구사항, Kubernetes 스펙과 rollout controller의 기준 환경이 하나로 정렬된다.
- EC2 instance, 가용 영역, EBS와 NLB 장애를 재현 가능한 profile로 시험할 수 있다.
- ECS 지원은 broker core를 바꾸지 않고 lifecycle adapter 경계로 확장할 수 있다.
- Production 지원과 local·experimental profile의 보장 차이가 명확해진다.

### 부정적 결과

- EKS control plane, EC2 node group, EBS CSI driver와 AWS Load Balancer Controller를 함께 운영해야 한다.
- 세 가용 영역 간 통신, NLB와 EBS 비용이 발생한다.
- EBS의 가용 영역 결합 때문에 AZ loss 뒤 volume 자체를 다른 AZ에 즉시 attach할 수 없다. 남은 quorum이 서비스를 유지하고 replacement node의 재동기화를 수행해야 한다.
- 초기 release에서 ECS, Fargate와 Compose 사용자는 production HA 지원을 받을 수 없다.
- AWS controller와 Kubernetes version 차이가 target 제외 순서에 영향을 줄 수 있어 version별 통합 시험이 필요하다.

## 7. 검증

1. 세 broker Pod가 서로 다른 EC2 instance와 가용 영역에 배치되고 각자 독립 EBS volume을 사용하는지 검사한다.
2. Broker process, Pod, EC2 instance와 단일 가용 영역을 차례로 잃어도 quorum과 durable ACK 보장이 요구사항대로 유지되는지 확인한다.
3. Durable ACK 직후 한 broker와 volume 접근을 제거해 message gap이 0인지 검증한다.
4. NLB target 제외 전파 중 새 연결이 drain node에 도착해도 `SAFE_TO_STOP`을 잘못 승인하지 않는지 확인한다.
5. Established connection의 종료 방식과 client reconnect가 MQTT version·Will 계약을 지키는지 검증한다.
6. 연속 100회 N-1↔N rolling update와 rollback에서 강제 종료가 0인지 확인한다.
7. Release artifact가 실제 EKS, controller, EC2, EBS와 NLB 설정을 기록하고 미지원 profile 결과를 분리하는지 확인한다.

## 8. 재검토 조건

- ECS lifecycle adapter가 같은 failure matrix와 release gate를 통과하고 운영 수요가 확인된다.
- EKS Fargate가 NaruMQ의 stable storage와 종료 lifecycle을 검증 가능한 방식으로 제공한다.
- EBS 외 storage가 RPO·latency·failure domain 조건을 더 낮은 운영 비용으로 충족한다.
- EKS 운영 비용 또는 가용성 측정값이 제품 SLO를 지속해서 충족하지 못한다.
- NaruMQ의 quorum·storage 또는 rollout 모델이 Kubernetes primitive를 전제로 하지 않게 바뀐다.

## 9. 참고 자료

- [Amazon EKS Kubernetes versions](https://docs.aws.amazon.com/eks/latest/userguide/kubernetes-versions.html)
- [Amazon EKS-optimized Arm Amazon Linux AMIs](https://docs.aws.amazon.com/eks/latest/userguide/eks-optimized-ami.html)
- [Amazon EKS EBS CSI driver](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html)
- [Amazon EKS에서 Network Load Balancer 사용](https://docs.aws.amazon.com/eks/latest/userguide/network-load-balancing.html)
- [Amazon EBS volume과 가용 영역](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-volumes.html)
- [Amazon EKS Fargate](https://docs.aws.amazon.com/eks/latest/userguide/fargate.html)
- [Amazon ECS Task Scale-in Protection](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection.html)
- [Amazon EBS gp3 volume performance](https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html)
- [Amazon EC2 M7g instances](https://aws.amazon.com/ec2/instance-types/m7g/)
- [EMQX Enterprise 6.3.1 release notes](https://docs.emqx.com/en/emqx/latest/changes/changes-ee-v6.html)
- [EMQX Enterprise 6.3.1 Docker image metadata](https://hub.docker.com/v2/repositories/emqx/emqx-enterprise/tags/6.3.1)
