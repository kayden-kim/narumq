# Drain과 rolling update 스펙

이 문서는 node를 계획적으로 교체할 때 신규 연결을 차단하고 session을 인계한 뒤, 외부 controller가 Pod 삭제를 승인할 수 있는 조건을 정의한다. Kubernetes `preStop`은 최후 방어선이며 승인 gate가 아니다.

| 항목 | 값 |
| --- | --- |
| 상태 | Active (broker-side drain/`SAFE_TO_STOP` 범위만; external rollout과 전체 호환성 gate는 Draft) |
| 관련 요구사항 | DEP-01~DEP-24, CLIENT-01~CLIENT-09, CLU-11 |
| 근거 | [PRD 9.7절과 10장](../requirements/PRD.md), [Kubernetes Container Lifecycle Hooks](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/), [OASIS MQTT 5.0 Server redirection](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html#_Toc3901255) |
| 관련 Architecture | [Cluster architecture](../architectures/cluster-architecture.md), [EKS on EC2 deployment](../architectures/eks-ec2-deployment.md) |
| 관련 결정 | [ADR-0004](../decisions/ADR-0004-session-ownership-and-fencing.md), [ADR-0006](../decisions/ADR-0006-external-rollout-controller.md), [ADR-0007](../decisions/ADR-0007-aws-production-deployment-platform.md), [ADR-0016](../decisions/ADR-0016-node-drain-and-safe-to-stop.md) (broker-side 범위만), [ADR-0051](../decisions/ADR-0051-cluster-update-strategy.md), [ADR-0055](../decisions/ADR-0055-drain-and-reconnect-operating-defaults.md) |
| 관련 Contract | [Management API v1](../contracts/management-api-v1.md), [MQTT client compatibility v1](../contracts/mqtt-client-compatibility-v1.md), [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md) |
| 해설 | [안전한 노드 drain과 Will](../explainers/safe-node-drain-and-will.md), [EKS 고가용성과 장애 도메인](../explainers/eks-ha-failure-domains.md), [AWS NLB와 MQTT 연결 lifecycle](../explainers/aws-nlb-mqtt-connection-lifecycle.md) |

## 1. Node lifecycle

```mermaid
stateDiagram-v2
    [*] --> JOINING
    JOINING --> READY: "sync·quorum·committed subscription route projection rebuild"
    READY --> DRAINING: "drain start"
    DRAINING --> READY: "취소 가능 + 안전한 rollback"
    DRAINING --> SAFE_TO_STOP: "barrier 충족"
    SAFE_TO_STOP --> READY: "허용하지 않음"
    SAFE_TO_STOP --> [*]: "process 종료"
```

| State | 신규 MQTT 연결 | 기존 연결 | Readiness | 종료 허용 |
| --- | --- | --- | --- | --- |
| `JOINING` | 거절 | 없음 | 실패 | 아니오 |
| `READY` | 허용 | 처리 | 성공 | 아니오 |
| `DRAINING` | 거절 | rate 제한 아래 인계 | 실패 | 아니오 |
| `SAFE_TO_STOP` | 거절 | 0 | 실패 | 외부 gate가 승인할 수 있음 |

정의하지 않은 상태 전이는 `409 invalid_node_transition`으로 거절하고 현재 state를 반환해야 한다. 모든 전이는 Metric과 구조화 로그에 남겨야 한다.

`-cluster-config` runtime은 committed voter를 대상으로 drain operation을 복제하고, 신규 MQTT CONNECT를 차단하며, 설정된 LB 전파 대기 뒤 rate 제한 ownership 인계를 수행한다. 모든 barrier를 통과하면 `SAFE_TO_STOP`을 replicated state에 commit하고 readiness는 실패로 유지한다. 이 broker 동작은 standalone mode에 제공되지 않는다. QoS 1/2 progress는 protocol wire effect보다 먼저 개별 replicated transition으로 commit되며 별도 replace-all checkpoint는 없다. External rollout controller, Kubernetes/EKS `preStop`, BP-07 및 N-1↔N은 별도 scope다.

Recovering 또는 joining node는 committed persistent subscription에서 node-local route projection을 rebuild하고 local sync·quorum 조건을 충족한 뒤에만 `READY`를 광고할 수 있다.

## 2. Drain operation resource

### 2.1 시작

`POST /api/v1/nodes/{nodeId}/drain`

```json
{
  "serverReference": "mqtt.example.internal:8883",
  "connectionRatePerSecond": 100,
  "loadBalancerPropagationDelayMillis": 15000,
  "timeoutMillis": 600000
}
```

`connectionRatePerSecond`, `loadBalancerPropagationDelayMillis`, `timeoutMillis`는 선택 field다. 생략하면 [ADR-0055](../decisions/ADR-0055-drain-and-reconnect-operating-defaults.md)의 기본값 `100`, `30000`, `600000`을 적용한다. 허용 범위는 각각 `1..10000`, `0..3600000`, `1000..86400000`이며 기본값 적용 뒤 timeout은 LB 전파 대기보다 커야 한다. Idempotency 비교는 기본값을 채운 뒤의 값으로 한다. `serverReference`는 선택이고 최대 256 UTF-8 bytes이며 제어 문자를 허용하지 않는다. 누락·범위 위반·잘못된 reference는 `400 invalid_request`다.

POST와 DELETE는 `Operator`, GET은 `ReadOnly` role을 요구하며 POST와 DELETE는 `Idempotency-Key`를 요구한다. 요청 대상은 cluster에 committed voter여야 한다. Active drain은 같은 node에서 새 operation을 만들지 않고 현재 committed operation을 반환한다. 최초 POST는 `202 Accepted`, 현재 operation replay는 `200 OK`다. 같은 key에 다른 parameter를 쓰면 `409 idempotency_conflict`; 다른 node drain이 이미 active이면 `409 node_drain_in_progress`다. 없는 node는 `404`, quorum을 확보할 수 없으면 `503`이다.

Rebalance operation이 `RUNNING`이면 drain 시작은 `409 connection_rebalance_in_progress`로 거절해야 한다. 이 오류는 retryable이다.

### 2.2 조회

`GET /api/v1/nodes/{nodeId}/drain`
요청은 fresh quorum read barrier를 통과한 committed 상태를 반환한다. Barrier를 확보하지 못하면 `503`이다.

```json
{
  "data": {
    "operationId": "550e8400-e29b-41d4-a716-446655440000",
    "nodeId": "3",
    "state": "DRAINING",
    "phase": "TRANSFERRING_SESSIONS",
    "startedAt": "2026-08-16T00:00:00Z",
    "deadlineAt": "2026-08-16T00:10:00Z",
    "connections": { "initial": 12000, "remaining": 420 },
    "sessions": { "stateReadyForTakeover": 11580, "takenOver": 11560, "failed": 0 },
    "replication": { "pendingWal": 0, "underReplicated": 0 },
    "safeToStop": false,
    "failure": null
  }
}
```

`phase`는 최소 `EXCLUDING_TRAFFIC`, `WAITING_FOR_LB`, `VERIFYING_SESSION_STATE`, `TRANSFERRING_SESSIONS`, `VERIFYING_BARRIER`, `COMPLETED`, `FAILED`, `CANCELLED`를 구분해야 한다. `failure`는 정상 진행 중 `null`이며 실패 시 `{ "code": string, "blockingReason": string | null }`이다. `safeToStop`은 `COMPLETED`에서만 true다.

### 2.3 취소

`DELETE /api/v1/nodes/{nodeId}/drain`은 `Operator`와 `Idempotency-Key`를 요구하며 operation을 안전하게 취소할 수 있을 때 `CANCELLED`로 commit해야 한다. 성공은 `200 OK`다. `SAFE_TO_STOP` 이후에는 `409 drain_not_cancellable`과 `details.state=SAFE_TO_STOP`을 반환해야 한다. 취소가 시작된 session takeover를 되돌리거나 epoch를 낮춰서는 안 된다. 새 readiness는 route와 replica 준비를 다시 확인한 뒤에만 성공해야 한다.

`FAILED` drain을 취소할 때는 phase를 `CANCELLED`로 바꾸고 `failure`의 code와 blocking reason을 함께 지운다. 이 취소 명령을 WAL에서 재생한 뒤에도 같은 상태여야 한다. Failure 정보는 `FAILED` phase에만 유효하다. 취소한 기록에 failure 정보가 남으면 typed snapshot 검증이 실패해 node가 재시작하지 못한다.

## 3. Drain 실행 순서

1. node state를 quorum에 `DRAINING`으로 commit한다.
2. 신규 connection admission을 닫고 readiness를 실패로 바꾼다.
3. configured load balancer propagation delay 동안 기존 connection은 유지한다.
4. connection rate limit 안에서 client reconnect와 session state takeover를 유도한다.
5. Successor의 더 큰 epoch `Acquire`가 commit되기 전에 committed persistent subscription과 QoS delivery state가 successor에서 복구 가능한지 확인한다. QoS state는 각 protocol transition에서 이미 commit됐으므로 별도의 최종 checkpoint mutation을 제안하지 않는다.
6. Successor의 더 큰 epoch session takeover commit을 확인한다.
7. safe-to-stop barrier를 평가한다.
8. barrier가 성공하면 `SAFE_TO_STOP`을 commit하고 external rollout gate에 노출한다.

Successor `Acquire` 뒤 그보다 늦게 committed ordering된 old owner의 route-affecting subscription 또는 inflight command는 exact owner/epoch check에서 거절되고 authoritative aggregate와 route projection을 바꾸지 않는다. Command 도착 시각이나 locally cached route는 이 결과를 우회할 수 없다.

Timeout은 operation을 `FAILED`로 만들 뿐 Pod 삭제를 승인하지 않는다. 실패한 node는 readiness가 내려간 DRAINING에 남아야 하며 operator가 원인을 해결하거나 명시적으로 취소해야 한다.

## 4. Client connection 인계

| Client | Server 동작 | Client 의무 | Will 기대 |
| --- | --- | --- | --- |
| MQTT 5, Server Reference 지원 | session state가 successor에서 복구 가능한 뒤 `DISCONNECT 0x9C`와 Server Reference 전송 | 같은 tenant·clientId로 임시 endpoint 또는 공통 LB에 재접속 | 충분한 Will Delay 안에 재접속하면 억제 |
| MQTT 5, Server Reference 미지원 | `0x9C`와 configured 공통 endpoint를 제공하거나 연결 종료 | 공통 LB 재접속, backoff+jitter | Will Delay 계약에 따름 |
| MQTT 3.1.1 | committed session state를 보존한 뒤 connection 종료 | 공통 LB에 같은 session key로 재접속 | Will이 설정돼 있으면 서버 종료로 발행한다(`expected_mqtt311_server_disconnect`) |
| 협력형 client | 서버 신호 뒤 client가 정상 DISCONNECT하도록 별도 application 협력 | 정상 DISCONNECT 후 재접속 | 정상 DISCONNECT면 억제 |

`0x9C`는 임시 이동이다. 영구 endpoint 변경을 뜻하는 `0x9D`로 바꿔 보내서는 안 된다. Server Reference 형식은 지원 SDK compatibility matrix에서 고정해야 한다.

공식 지원 client는 persistent session, 충분한 Session Expiry, exponential backoff+jitter와 동일 session key를 사용해야 한다. PUBACK/PUBCOMP 전 outbound publish는 local outbox에 보관하고 QoS 1 consumer는 중복을 견뎌야 한다.

## 5. Will 판정과 관측

`narumq_will_outcomes_total{reason}`은 Will의 최종 결과를 아래 6개 사유로 집계한다. [ADR-0028](../decisions/ADR-0028-planned-handoff-will-and-reason-metric.md)을 따른다.

| 상황 | 집계 node | reason |
| --- | --- | --- |
| 정상 `DISCONNECT 0x00`, pending Will 있음, metadata commit 성공 또는 local-only | owner | `suppressed_normal_disconnect` |
| 같은 node 재접속이 예약 Will 취소, superseded 정보 없음 | 재접속 node | `suppressed_reconnected_within_delay` |
| Acquire가 예약 Will 또는 계획 인계 MQTT 5 delay>0 persistent Will을 대체 | 재접속 node | `suppressed_reconnected_within_delay` |
| 계획 인계 MQTT 3.1.1 즉시 발행 | 후계 또는 재접속 node | `expected_mqtt311_server_disconnect` |
| 계획 인계 MQTT 5 delay 0 또는 retention `IMMEDIATE` 즉시 발행 | 후계 또는 재접속 node | `expected_zero_or_expired_will_delay` |
| 계획 인계 예약 Will을 timer·expiry로 발행 | 예약 node | `expected_zero_or_expired_will_delay` |
| 계획 인계 MQTT 5 delay>0·retention≠`IMMEDIATE`인데 즉시 발행 | 후계 node | `unexpected_supported_client` |
| 비정상 close·keepalive·owner crash recovery 또는 복원한 예약 Will 발행 | 발행 node | `unplanned_failure` |

계획 인계는 서버가 시작한 비정상 종료다. MQTT 3.1.1, Will Delay 0 또는 retention `IMMEDIATE`이면 즉시 claim·발행한다. 나머지는 후계 recovery 시각부터 Will Delay로 예약하며 session expiry로 제한한다. 이전 owner는 `DisconnectTakeover`로 local Will을 버리고 집계하지 않는다. Recovery 전 재접속도 committed ownership apply의 대체 결과로 같은 규칙을 적용한다.

Will 하나는 결과를 확정한 node에서 정확히 한 번 집계한다. Client `DISCONNECT 0x04`, takeover close, ACL 거절·metadata commit 실패로 발행하지 않은 Will은 집계하지 않는다. Feature 비활성 drain은 기존처럼 Will을 버리고 집계하지 않는다. 계획/비계획 출처는 복제하지 않으므로 재시작·detached 이전 뒤 복원한 예약 Will은 `unplanned_failure`다. Mixed N-1/N의 N-1 후계는 기존 억제를 유지한다.

Release gate는 `unexpected_supported_client`가 0이어야 한다. 이 사유는 위 표의 broker 규칙 위반만 탐지한다. Client 쪽 증거는 SDK drain suite로 확인한다. 예상 Will을 성공률에 섞어 숨기거나 제품 결함으로 오분류해서는 안 된다.

## 6. Safe-to-stop barrier

다음 조건을 모두 충족해야 `SAFE_TO_STOP`이 된다.

- 신규 connection admission이 닫혀 있고 readiness가 실패한다.
- 활성 MQTT connection이 0이다.
- 이 node가 현재 epoch owner인 session이 0이다.
- Persistent subscription과 QoS delivery transition은 commit된 상태이며, successor takeover 뒤 old owner의 추가 mutation이 epoch fencing으로 거절된다.
- committed but unapplied WAL과 필수 replication backlog가 허용 기준 안이다.
- 필요한 replica 수와 cluster quorum이 node 종료 뒤에도 유지된다.
- 진행 중인 local mutation이 없고 새 owner takeover 확인이 끝났다.
- state와 barrier evidence가 node-local memory가 아니라 복구 가능한 위치에 기록됐다.

Barrier 항목 하나라도 `unknown`이면 실패로 판정해야 한다. `unknown`을 0이나 성공으로 취급해서는 안 된다.
구현은 barrier 조건을 다음 안정된 blocker code로 노출한다.

| 확인 조건 | 실패 blocker |
| --- | --- |
| 신규 admission이 닫히지 않음 | `admission_open` |
| readiness가 이미 실패 상태가 아님 | `readiness_up` |
| 활성 connection 수가 0이 아님 또는 알 수 없음 | `connections_remaining` |
| local ownership session 수가 0이 아님 | `sessions_owned` |
| local persistent subscription 수가 0이 아님 | `orphan_subscription` |
| committed index와 applied index 사이에 WAL이 남음 | `pending_wal` |
| voting replica 또는 connected successor 수가 quorum에 부족 | `insufficient_replicas` |
| local node가 Raft leader임 | `local_leader` |
| local proposal이 진행 중임 | `pending_local_commit` |
| pending Will timer가 남음 | `pending_will` |
| successor voter가 아직 없거나 route projection이 준비되지 않음 | `no_successor_voter`, `route_projection_not_ready` |
| read barrier 또는 quorum을 확보하지 못함 | `quorum_unavailable` |

다음 barrier 재평가 전까지 blocker가 남으면 operation은 계속 대기한다. Deadline에 도달하면 `FAILED`와 `drain_timeout`을 commit하고 마지막 blocker를 기록한다.

## 7. External rollout gate와 Kubernetes/EKS

Rollout controller는 Pod 삭제 전에 drain을 시작하고 `SAFE_TO_STOP`을 확인해야 한다. 실패·timeout이면 현재 Pod와 다음 Pod의 교체를 중단해야 한다. 기준 3-AZ profile에서는 계획 중단을 한 번에 한 node로 제한해야 한다.

`preStop` hook은 다음만 수행한다.

1. 같은 node의 drain을 idempotent하게 시작하거나 현재 상태를 조회한다.
2. 남은 grace period 안에서 가능한 범위까지 기다린다.
3. 결과와 failure reason을 기록한다.

Kubernetes의 termination grace period는 `preStop` 실행 전에 계산이 시작된다. hook 실패나 장기 실행이 Pod 삭제를 영구 차단한다고 가정해서는 안 된다. `terminationGracePeriodSeconds`는 LB 대기, drain 최악 시간, hook overhead와 SIGTERM 뒤 정상 종료 시간을 합친 값 이상이어야 하며, 검증되지 않은 짧은 값은 배포 admission에서 거절해야 한다.

Kubernetes는 Pod 삭제뿐 아니라 startup·liveness probe 실패로 container를 종료할 때도 `preStop`을 실행한다. 따라서 startup 예산(`failureThreshold` × `periodSeconds`)을 다 쓴 container는 종료 직전에 drain을 시작한다. 이 drain은 계획된 교체가 아니다. 결과와 복구 규칙은 [§8.1](#81-startup-probe-종료와-drain-기록-재시작-순환)을 따른다.

`terminationGracePeriodSeconds ≥ ceil(timeoutMillis/1000) + 30 + 30`이어야 한다. 기본 drain 값으로는 660초다. 두 30초 값은 측정 전 예산이며 실측은 [TODO §1.4](../TODO.md#14-drain-정책관측과-production-배포)에서 추적한다.

배포 template은 PodDisruptionBudget, required anti-affinity, topology spread와 max unavailable 1을 제공해야 한다. 기준 3-AZ profile은 quorum node를 AZ마다 하나씩 배치해야 한다.

### 7.1 AWS deployment profile

초기 production 기준인 `EKS_EC2_HA`는 다음 조건을 충족해야 한다.

- 지원 EKS와 Kubernetes minor version을 release artifact에 고정한다.
- EC2 기반 node group의 서로 다른 세 가용 영역에 quorum Pod를 하나씩 배치한다.
- 각 quorum Pod는 다른 Pod와 공유하지 않는 Amazon EBS CSI persistent volume을 사용한다.
- EBS encryption, AZ binding, snapshot·restore와 disk pressure 실패 결과를 검증한다.
- Amazon NLB TCP/TLS target은 readiness 실패 뒤 신규 connection을 제외해야 한다.
- NLB deregistration delay, connection termination, TCP idle timeout, client IP preservation 또는 Proxy Protocol 정책을 명시한다.
- NLB 설정 전파 중 유입될 수 있는 신규 connection을 drain의 load balancer propagation 단계가 흡수해야 한다.
- Worker architecture와 AMI를 release artifact에 고정한다. Production broker image를 선택한 architecture로 빌드·게시하고 immutable digest를 고정한다. Broker, controller와 필수 add-on image는 선택한 architecture를 지원해야 한다.

`EKS_FARGATE`, `ECS_EC2`, `COMPOSE_LOCAL`과 단일 host의 다중 container 결과는 `EKS_EC2_HA` 수용 시험을 대신해서는 안 된다.

배포 profile의 선택 근거는 [ADR-0007](../decisions/ADR-0007-aws-production-deployment-platform.md)을 따른다. Pod·EC2·가용 영역·EBS의 장애 범위는 [EKS 고가용성과 장애 도메인](../explainers/eks-ha-failure-domains.md), NLB target과 established connection의 서로 다른 lifecycle은 [AWS NLB와 MQTT 연결 lifecycle](../explainers/aws-nlb-mqtt-connection-lifecycle.md)을 참고한다.

Amazon ECS on EC2를 지원하려면 Kubernetes rollout controller와 별개인 lifecycle adapter가 필요하다. Adapter는 ECS Task Scale-in Protection을 설정한 상태에서 NLB target 제외와 NaruMQ drain을 시작하고, `SAFE_TO_STOP` 뒤에만 protection을 해제해야 한다. Task ARN, IP와 container name을 영구 `nodeId`로 사용해서는 안 된다. 이 흐름과 storage·discovery·강제 종료 시험을 모두 통과하기 전 ECS profile은 `Experimental`이어야 한다.

### 7.2 업데이트 방식과 환경별 선택

계획 업데이트는 [ADR-0051](../decisions/ADR-0051-cluster-update-strategy.md)에 따라 한 번에 한 node씩 진행해야 한다. 새 클러스터로 전환하는 Blue/Green은 지원하지 않는다.

#### In-place 교체

1. 대상 node의 drain을 시작한다.
2. `SAFE_TO_STOP`을 확인한다.
3. 프로세스를 종료한다.
4. 같은 `nodeId`, cluster config, 데이터 디렉터리로 새 release를 시작한다.
5. `/health/ready` 성공을 확인한 뒤 다음 node로 진행한다.

재시작 node는 READY 전에 WAL·snapshot replay → log catch-up → `MemberProfileChange` commit → route projection 재구축 → drain `CLEAR` 순서로 진행한다([ADR-0018](../decisions/ADR-0018-release-compatibility-window.md)).

#### 교체형 교체

1. Committed voter가 정확히 3개인지 확인한다.
2. 새 release node를 새 `nodeId`와 빈 데이터 디렉터리로 stage·join·promotion한다. [동적 멤버십 절차](../how-to/dynamic-cluster-membership.md)를 따른다.
3. 기존 node의 drain을 시작하고 `SAFE_TO_STOP`을 확인한다.
4. 기존 node를 planned removal로 제거한다.
5. Voter가 3개로 돌아오고 모든 node가 READY인지 확인한 뒤 다음 node로 진행한다.

교체형 조합은 세 차례 모두 node를 바꾸는 local 3-node process 수용 시험으로 확인했다. 각 batch는 QoS 1·2 topic마다 3개 메시지를 보낸다. 이 증거는 production storage, Kubernetes·ECS 배포 profile과 N-1↔N 혼합 교체를 대신하지 않는다([§10](#10-broker-drain-수용-시험-매핑)).

| 환경 | 데이터 디렉터리 재연결 | 사용할 방식 |
| --- | --- | --- |
| VM(같은 디스크 유지) | 가능 | In-place. Host 교체 시 교체형 |
| Docker Compose named volume(개발·시험 전용, production 미지원) | 가능 | In-place |
| EKS StatefulSet PVC(`EKS_EC2_HA`) | 가능 | In-place. Node·AZ·volume 교체 시 교체형 |
| ECS 서비스 관리 EBS | 불가(task마다 새 volume, task 종료 시 삭제) | 교체형만 |
| ECS on EC2 호스트 디스크 | 인스턴스를 유지하면 가능 | In-place. 인스턴스 교체 시 교체형 |

두 방식 모두 N/N-1 release window 안에서만 혼합 운영한다. 새 feature activation은 모든 node를 교체한 뒤에만 한다. Activation 전 rollback은 같은 방식으로 N-1 release를 다시 배포한다.

## 8. 비정상 종료

Process crash 또는 host loss에서는 planned drain과 barrier가 실행되지 않을 수 있다. 남은 node는 cluster spec에 따라 새 epoch를 획득하고 persistent session을 복구해야 한다. 이를 계획 drain 성공으로 기록해서는 안 되며 `unplanned_failure`로 관측해야 한다. `COMPLETED` record를 재생한 node는 admission을 닫은 채 유지하고 bounded readiness preflight를 통과한 뒤 replicated `CLEAR`를 commit해야 한다. Preflight 또는 commit에 실패하면 재시도하며, `CLEAR` apply 뒤에만 admission을 다시 연다.

### 8.1 Startup probe 종료와 drain 기록 재시작 순환

Kubelet은 startup probe가 성공하기 전에는 readiness·liveness probe를 실행하지 않는다. 그래서 cluster runtime의 `/health/startup`은 `JOINING`일 때 readiness 조건을 스스로 평가해야 한다. 외부의 `/health/ready` 요청을 기다려서는 안 된다. `DRAINING`·`SAFE_TO_STOP`은 startup을 통과한다. 이때 readiness는 계속 실패한다.

Container가 startup 예산을 다 써서 종료되면 `preStop`이 drain을 시작하고 drain은 보통 `COMPLETED`가 된다. 재시작한 container는 이 기록을 §8 규칙대로 `CLEAR`한 뒤 readiness를 다시 평가한다. 기록이 사라지면 사유는 `node drain cancelled; readiness recheck pending`으로 보일 수 있다. 이 문구는 취소 요청이 없었어도 `CLEAR` 뒤에 나온다. 취소 기록과 구분하려면 `GET /api/v1/nodes/{nodeId}/drain`의 phase와 `cleared completed node drain record` 로그를 본다.

복구 규칙은 다음과 같다.

| 재시작 때 기록 | 동작 | 사람이 할 일 |
| --- | --- | --- |
| 없음(전원 손실·`SIGKILL`) | `preStop`이 없으므로 drain 기록도 없다. Startup이 readiness를 평가해 `READY`가 된다. | 없음 |
| `COMPLETED` | 자동 `CLEAR` 뒤 `READY`가 된다. | 없음. Startup이 멈추면 원인을 찾는다. |
| `FAILED` | `DRAINING`과 readiness 실패를 유지한다. | 원인을 해결한 뒤 `DELETE /api/v1/nodes/{nodeId}/drain`으로 취소한다. |
| `CANCELLED` | 기록은 무해하다. Startup이 readiness를 평가한다. | 없음 |

전원 손실 시험 도구는 장애 뒤 대상 node가 `DRAINING`이면 먼저 Pod의 `restartCount`와 직전 container 종료 사유를 확인한다. 재시작이 있으면 drain 기록의 phase를 읽는다. `/health/ready`를 호출해 startup을 대신 통과시켜서는 안 된다. 그 호출은 결함을 가린다. 2026-10-15 전원 손실 실패가 이 경로였다([Cluster spec](cluster-session-and-delivery.md#2026-10-15-격리-eks-전원-손실-후속-관찰-failedunresolved)의 2026-10-16 원인 분석).

## 9. 수용 시험

- 최대 예상 traffic에서 연속 100회 rolling update를 수행한다.
- MQTT 3.1.1/5.0, QoS 0/1/2, retained, Will과 persistent session을 섞어 시험한다.
- 매 payload의 단조 증가 ID로 gap, duplicate와 reorder를 분리 측정한다.
- drain timeout·failure 때 controller가 Pod 삭제와 다음 node 진행을 요청하지 않는지 검증한다.
- preStop 중복 호출, hook 실패와 grace period 부족을 시험한다.
- 새 owner takeover 뒤 이전 owner의 stale write가 거절되는지 확인한다.
- 지원 client의 `unexpected_supported_client` Will이 0인지 검증한다.
- N-1↔N upgrade와 rollback에서 같은 barrier를 통과하는지 확인한다.
- `EKS_EC2_HA`에서 Pod·EC2 node·단일 AZ failure, EBS detach·reattach와 NLB deregistration을 시험한다.
- 배포 artifact가 profile 상태를 표시하고 미지원 profile의 결과를 production 출시 근거에서 제외하는지 검증한다.
- ECS adapter가 도입되면 protected task 때문에 막힌 deployment, protection 만료, direct task stop과 host loss를 별도 실패로 시험한다.

## 10. Broker drain 수용 시험 매핑

아래 evidence는 dynamic cluster의 broker-side drain과 `SAFE_TO_STOP`, 시험 전용 SDK drain 호환성, 그리고 local Kind controller lifecycle acceptance를 검증한다. Release-wide rollout이나 production storage 승인으로 확대 해석하지 않는다.

| Scenario | Acceptance test | 실행 증거 |
| --- | --- | --- |
| Local real-controller rollout (DEP-11/DEP-18/SEC-11) | `TestRolloutControllerKindLifecycle`: controller가 실제 Go broker 3-process `OnDelete` StatefulSet을 Management API committed-voter profile로 preflight하고 OCI manifest/config labels, version/features, UID/RV, persisted complete drain barrier, durable audit를 검증한다. | **2026-10-15 PASS**: `NARUMQ_ROLLOUT_KIND_SMOKE=1 NARUMQ_ROLLOUT_REGISTRY=172.20.0.3:5000 NARUMQ_ROLLOUT_HOST_REGISTRY=http://127.0.0.1:5001 NARUMQ_ROLLOUT_OLD_DIGEST=sha256:dd44acdabea4188939ef08e84a5537ed7b7c1812f62a7ccc33935d16c26e0c2b NARUMQ_ROLLOUT_TARGET_DIGEST=sha256:cc6e480086f74a0233ffc52f88c62796accc346a03843bec6f8c78e25f4a195c NARUMQ_ROLLOUT_CONTROLLER_IMAGE=narumq-rollout-controller:local GOMAXPROCS=4 go test ./tests/integration -run '^TestRolloutControllerKindLifecycle$' -count=1 -v -timeout 15m`; test **243.53s**, package **248.487s**. Controller was restarted during active drain; the same broker `operationId` was observed afterward. Three broker Pod UIDs changed to target image IDs; the audit PVC contained a `rollout.pod.delete_approved` event with controller Pod UID producer identity; caller version `0.0.2` mismatch caused no Pod UID change. This `0.0.0`→`0.0.1` TEST-ONLY upgrade does not qualify published N/N-1, production B*, MQTT-13 90 days, independent assessor, EKS HA or NLB acceptance. |
| MQTT 5 persistent QoS 1과 MQTT 3.1.1 live/detached session이 있는 3-node drain | `TestDynamicClusterNodeDrainProcessAcceptance`: readiness·admission 차단, 기존 connection 유지, idempotent replay, `DISCONNECT 0x9C`/Server Reference, rate-limited transfer, `SAFE_TO_STOP`, session resume와 restart `CLEAR`를 확인한다. | `go test ./tests/integration -run '^TestDynamicClusterNodeDrain' -count=1 -v -timeout 20m` PASS; 이 case 9.75s. |
| Cancel, crash/restart, completed-record clear와 missing-replica timeout | `TestDynamicClusterNodeDrainCancelTimeoutAndResumeProcessAcceptance`: cancel 뒤 READY, 같은 operation 재개, missing successor에서 `drain_timeout/insufficient_replicas`, 취소 뒤 readiness를 확인한다. | 같은 process command PASS; 이 case 16.03s. Combined package 25.916s. |
| Domain·application·wire·HTTP·metrics와 OpenAPI contract | `internal/domain/nodedrain`, `internal/application/membership`, `internal/adapters/tcp`, `internal/adapters/http/management`, `internal/adapters/metrics`, `tests/contracts/management-api-v1` 등 focused unit/contract suite. | `go test ./internal/domain/nodedrain ./internal/domain/mqtt ./internal/application/membership ./internal/application/connect ./internal/adapters/raft/records ./internal/adapters/tcp ./internal/adapters/http/management ./internal/adapters/metrics ./tests/contracts/management-api-v1 ./cmd/narumq ./tools/verify -count=1` PASS. |
| Existing ownership, subscription replay와 removal regressions | `TestDynamicClusterMQTTConnectOwnershipTakeoverProcessAcceptance`, `TestDynamicClusterPersistentSubscriptionReplayProcessAcceptance`, `TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum`. | Targeted integration command PASS; package 38.257s. |
| Prior full local verification record (historical; separate from the 2026-10-15 attempt below) | `GOMAXPROCS=4 go run ./tools/verify local` | PASS; 204.42s. This is the previously recorded run only, not the current attempt. |
| Full local verification attempt after rollout adapter/API changes (2026-10-15) | `GOMAXPROCS=4 go run ./tools/verify local` | **FAIL; 499.46s; not a full-local PASS.** Architecture and traceability passed (P0 71/81, 10 exceptions). Test output reported TEN-09 victim p99 ratio `1.517 > 1.5` and `internal/adapters/raft/TestNodeCloseFlushesHeldAppendOrReportsFailure/unconsumed-prior-error` failed in setup with Windows `compact WAL: replace WAL: Access is denied`. Targeted follow-ups: `GOMAXPROCS=4 go test ./internal/adapters/raft -run '^TestNodeCloseFlushesHeldAppendOrReportsFailure$/^unconsumed-prior-error$' -count=1 -v` PASS (subtest 0.03s, package 0.901s); `GOMAXPROCS=4 go test -race ./internal/adapters/raft -run '^TestNodeCloseFlushesHeldAppendOrReportsFailure$/^unconsumed-prior-error$' -count=1 -v` PASS (2.070s); `GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterTenantNoisyNeighborProcessAcceptance$' -count=1 -v -timeout 10m` PASS (package 47.090s, p99 ratio 0.732). These isolated results support, but do not prove, load sensitivity/intermittency. The subtest PASS does not explain or resolve the aggregate race-child exit 1; its diagnostics remain unavailable. The full verifier remains FAIL. |
| NodeDrain core coverage ratchet | `go run ./tools/verify coverage --profile .artifacts/verification/coverage.out --policy tests/verification/coverage-policy.json --report .artifacts/verification/coverage-summary.json` | PASS; 100/102 executable lines (98.04%) and 13/13 functions (100%). |
| NodeDrain mutation gate | `go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-nodedrain.json internal/domain/nodedrain` | PASS; KILLED=64, LIVED=0, NOT_COVERED=0, efficacy=100%, mutant coverage=100%; 76s. |
| SDK callback adapter의 redirect·session resume·Will 억제 | 필수 `tests/compatibility/mqtt-clients-v1 --suite drain`: Paho Go MQTT 5 `v0.23.0`과 Python `2.1.0`이 test-only readiness LB의 node3에서 node2로 이동한다. 후속 QoS 1 전달, Will Delay 뒤 no-Will과 drain `SAFE_TO_STOP`을 확인한다. | `drain-paho-go-python-will-acl-final`의 cluster case 2/2 PASS. 재접속 Go 89ms, Python 약 99.071ms. 기존 혼합 suite의 9/10 FAIL 기록은 보존한다. Artifact는 `.artifacts/compatibility/mqtt-clients-v1/drain-paho-go-python-will-acl-final/`에 있다. |
| 선택적 SDK 진단: malformed server DISCONNECT 4종과 configured endpoint fallback | `--suite sdk-diagnostics`의 SDK별 truncated·disallowed·duplicate·invalid-reference case. NaruMQ 대신 fake server를 사용하므로 제품 필수 합격 조건에서 제외한다. 진단은 Main CONNECT 두 번, vector 한 번과 trap 접속 부재를 기대한다. | 기존 기록은 Python 4/4 PASS, Go 3/4 PASS. `go-mqtt5-malformed-duplicate-server-reference`는 `serverReferenceAccepted=true`, `trap_accept=1`로 FAIL이며 이 결과는 유지한다. 2026-10-16 사용자 결정으로 SDK 진단 실패와 정상 drain 승인을 분리했다. 판정 규칙은 [Client contract §6.3](../contracts/mqtt-client-compatibility-v1.md#63-선택적-sdk-진단)에 있다. |
| SDK drain 구현 뒤 전체 local 회귀 | `GOMAXPROCS=4 go run ./tools/verify local`, nested Go module의 `go vet ./... && go build ./...`, Phase 1 Docker suite | Local PASS(280.08s; integration 234.376s). Traceability·architecture·4개 fuzz target 각각 10,000회·전체 unit/integration·production package race·coverage PASS. Nested vet/build PASS. `phase1-drain-adapters-final` 26/26 PASS(132.67s). Local gate는 SDK Docker suite를 대신하지 않는다. |
| 필수 drain과 선택적 SDK 진단의 판정 분리 | `TestDrainSuiteVerdictsSeparateSDKDiagnostics`와 실제 `--suite drain`·`--suite sdk-diagnostics` 실행. 필수 Will 실패·누락 결과는 계속 차단하며 SDK 진단 실패는 별도 suite에 남긴다. | 2026-10-16 회귀·vet와 필수 Docker drain PASS. 선택적 SDK 진단은 기존 중복 수락을 FAIL로 유지했다. 최초 fixture 실패와 수정·실행 명령·artifact는 [Client contract §6.4](../contracts/mqtt-client-compatibility-v1.md#64-검증-범위-분리-실행-기록-2026-10-16)에 있다. |
| 계획 인계 domain·대체 Will·집계 규칙 | `TestLifecycle_TakeoverAndPlannedHandoffWillRules`, `TestApplyOwnershipReportsSupersededWill`, `TestWillOutcomeClassifiers`, `TestDisconnectRecordsWillOutcome`, `TestConnectSupersededWillOutcome`, `TestRecoverOwnedPreviousEpochAppliesPlannedHandoffWillRules`, `TestRegistryRendersWillOutcomes`, runtime recovery 및 기존 TCP 시험 | 2026-10-04 PASS: `go build ./...`를 각 구현 단계 뒤 통과했다. `go test ./internal/domain/session ./internal/application/membership ./internal/application/connect ./internal/adapters/metrics ./internal/adapters/tcp ./cmd/narumq -count=1` PASS(각 0.600/0.731/0.649/0.583/11.352/15.383s). |
| Session metadata 활성 3-node 연속 rolling의 표준 Will·사유 | `TestDynamicClusterDrainWillReasonProcessAcceptance`: node 3→1→2 drain, reference 재접속, session resume, observer 수신과 node restart를 확인한다. | 2026-10-04 PASS: `GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterDrainWillReasonProcessAcceptance$' -count=1 -v -timeout 20m`; test 49.06s, package 49.253s. Observer는 v311·zero·expire payload만 받았다. Safe delay 8초 뒤와 normal Will 무수신을 확인했다. 아래 delta 표 참조. |
| ADR-0028 영향 process 회귀 | Drain·cancel/timeout/resume, session metadata owner crash, removal handoff quorum, QoS 2 Will, CONNECT ownership takeover | 2026-10-04 PASS: 아래 회귀 명령의 6개 시험 모두 통과했다. Package 39.569s; 각 drain 13.54s, cancel 24.11s, metadata 26.79s, removal 32.66s, QoS 2 Will 12.60s, takeover 9.49s. |
| ADR-0028 전체 local gate | `GOMAXPROCS=4 go run ./tools/verify local` | 2026-10-04 PASS; wall 298.08s, integration 243.252s. Gofmt·vet·traceability·architecture·전체 unit/integration·production race·4개 fuzz 각 10,000회·coverage PASS. Production line 86.07%, function 97.51% (`.artifacts/verification/coverage-summary.json`). 첫 실행은 새 domain 시험의 MQTT requirement metadata 누락으로 중단했으며 comment·compliance mapping을 고친 뒤 통과했다. |
| Recovery 전 재접속의 이전 Will ACL 재검사 | `TestConnectSupersededWillAuthorization`: 새 CONNECT에는 Will을 넣지 않는다. 이전 principal·Will topic이 허용되면 한 번 발행·집계한다. 이전 topic이 거절되거나 principal이 없으면 새 CONNECT는 성공하지만 이전 Will 발행·집계는 모두 없다. | 2026-10-04 PASS: `go test ./internal/application/connect -run '^TestConnectSupersededWillAuthorization$' -count=1 -v`의 3개 case 모두 PASS. 최초 package 0.422s; 새 CONNECT의 Will을 제거한 뒤에도 0.406s PASS. |
| ACL 경계 시험 추가 뒤 local gate | 위 focused 명령 뒤 `GOMAXPROCS=4 go run ./tools/verify local` | 2026-10-04 PASS; 두 명령 합계 wall 286.02s, integration 246.431s. Gofmt·vet·traceability·architecture·전체 unit/integration·production race·4개 fuzz 각 10,000회·coverage PASS. 이 실행의 production line 86.20%, function 97.51%. 아래 시험 입력·요구사항 연결 정리 전 실행이다. |
| ACL 시험 입력과 요구사항 연결 정리 | 새 CONNECT의 Will을 제거해 이전 Will의 ACL 검사만 확인한다. Traceability를 `SEC-06`, `MQTT-01/TEST-04`, `MQTT-3.1.2-8`에 연결하고 compliance matrix에 시험을 추가한다. | 2026-10-04 PASS: 위 focused 명령 뒤 `go run ./tools/verify traceability`; 3개 case와 traceability PASS, 두 명령 합계 wall 2.99s. 제품 코드는 바꾸지 않았으므로 전체 local gate는 다시 실행하지 않았다. |
| ADR-0051 교체형 3회 연속 교체 | `TestDynamicClusterReplacementRollingUpdateProcessAcceptance`: node 4·5·6 추가, node 1·2·3 drain·`SAFE_TO_STOP`·planned removal, MQTT 5/3.1.1 live와 3.1.1 offline session 복구, QoS 1/2 gap 0, QoS 2 duplicate 0, 최종 voter `{4,5,6}`을 확인한다. 각 batch는 topic별 3개 메시지를 보낸다. | **2026-10-10 PASS**. 처음 사용한 topic별 batch당 5개 workload는 offline session당 QoS 1·2 총 90개를 보내 `MaxDeliveryMessagesPerSession=64`를 넘었다. offline client는 각 topic에서 seq 33–45를 받지 못했다. 코드 대조 결과, `membership.StateMachine.enqueueDeliveryTargetsLocked`는 한도를 넘은 target을 `Saturated`로 반환한다. `publish.UseCase.completePublication`은 이를 publication error로 바꾸지 않는다. `tcp.connection.completeInboundPublish`는 publisher에 성공 ACK를 보낸 뒤 saturated target 연결을 닫는다. 따라서 첫 gap은 한도를 넘은 test workload와 일치하며, 교체 과정의 session 손실 증거는 아니다. 사용자는 batch당 topic별 3개로 줄이는 변경을 승인했다. 최종 workload는 offline session당 54개(각 topic 27개)이며 64개 한도 안에 있다. 단일 실행 `GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterReplacementRollingUpdateProcessAcceptance$' -count=1 -v -timeout 20m` PASS; case 36.00s. 반복 `GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterReplacementRollingUpdateProcessAcceptance$' -count=3 -v -timeout 45m` PASS; 세 case 36.08s, 32.12s, 31.74s. 각 실행에서 세 round가 로그에 나타났고, MQTT 5 live·MQTT 3.1.1 live·offline receiver의 QoS 1 duplicate는 모두 0이었다. 최종 offline session은 `SessionPresent=true`였고 gap은 없었다. 최종 voter는 `{4,5,6}`이었다. |
| 연속 100회 in-place rolling의 local workload 상한 | `TestDynamicClusterContinuousRollingDrainProcessAcceptance`: 실제 3-node에서 node 3→1→2 순서로 drain·`SAFE_TO_STOP`·재시작을 100회 수행한다. Round당 QoS 1/2 PUBLISH 24건(4 batch×topic당 3건), retained PUBLISH 1건, MQTT 5 safe Will과 MQTT 3.1.1 Will을 사용한다. | **2026-10-11 PASS**: `NARUMQ_ROLLING_100=1 GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterContinuousRollingDrainProcessAcceptance$' -count=1 -v -timeout 3h`; test 1715.09s, package 1716.407s. QoS 1·2 topic별 1,200건, gap 0, QoS 1 duplicate 100, QoS 2 duplicate 0, reorder 0; retained 집합은 매 round 기대값과 일치했다. Will delta는 `suppressed_reconnected_within_delay=100`, `expected_mqtt311_server_disconnect=100`, `unexpected_supported_client=0`, `unplanned_failure=0`이었다. QoS 1 duplicate와 reorder는 기록 전용이다. 이 test-defined local workload는 production 최대 traffic이 아니다. QoS 0, controller 중단·실패, `preStop`, stale write, N-1↔N, `EKS_EC2_HA`는 이번 실행 범위에 없으며 TODO §1.2는 계속 `미검증`이다. |
| Fixture 확장과 repository gate | `TestDynamicClusterNodeConfigurationProcessAcceptance`, `GOMAXPROCS=4 go run ./tools/verify local`, `go run ./tools/verify markdown-links` | **2026-10-11 PASS**: `cluster_node_configuration_process_test.go`의 ACL 규칙 수 기대값 58은 새 fixture ACL 3개 뒤 61로 바꿨다. 집중 시험 PASS(6.97s). 첫 local 실행은 기존 58개 규칙 기대값 때문에 실패했다. 수정 뒤 local PASS(244.26s), traceability P0 71/81·exceptions 10. Markdown links PASS(156 files, 2,278 links). Windows/amd64, Go 1.26.5. |
| Acceptance harness와 repository gate | Fixture·PUBLISH decoder·node-drain regression, `go vet`, gofmt, traceability, Markdown link 검사, 전체 local verification | **2026-10-10 PASS**: fixture `TestDynamicClusterNodeDrainProcessAcceptance` 13.36s; `TestDynamicClusterCrossNodeDeliveryMatrixProcessAcceptance`와 `TestDynamicClusterNodeDrainProcessAcceptance` 회귀 PASS; `go vet ./tests/integration` PASS; `gofmt -l tests/integration` 출력 없음; traceability PASS(P0 69/81, exceptions 12); markdown-links PASS(152 files, 2,135 links); `GOMAXPROCS=4 go run ./tools/verify local` PASS(`local: PASS`, 190.49s). Windows/amd64, Go 1.26.5. 대상 commit `322cfe3a416a2435a17991855df4aa06d92ac24f`; 실행에 사용한 코드와 기록은 미커밋이었다. 작업 트리에는 `tests/integration/cluster_membership_process_test.go`, `cluster_delivery_matrix_process_test.go`, `cluster_replacement_update_process_test.go`, 이 문서, [ADR-0051](../decisions/ADR-0051-cluster-update-strategy.md), [TODO](../TODO.md)의 변경이 있었고 기존 사용자 변경 `docs/artifacts/mqtt-protocol-spec-code-explanation.html`도 남아 있었다. |
| ADR-0055 drain 기본값 | `TestDefaultParametersAreValid`, `TestNodeDrainHTTPAppliesProductDefaultsToOmittedFields`, `TestNodeDrainRequestDefaultsMatchDomain` | 2026-10-11 PASS: `go test ./internal/domain/nodedrain ./internal/adapters/http/management ./internal/application/membership ./tests/contracts/management-api-v1 -count=1` PASS(0.633s, 1.228s, 1.028s, 2.389s); 세 시험 전용 `-run` 실행 PASS(0.392s, 0.651s, 0.878s); `go build ./...` PASS. `GOMAXPROCS=4 go test ./tests/integration -run '^TestZZDrainDefaultsSmoke$' -count=1 -v -timeout 10m` PASS(7.72s). `GOMAXPROCS=4 go test ./tests/integration -run '^(TestDynamicClusterNodeDrainProcessAcceptance|TestDynamicClusterNodeDrainCancelTimeoutAndResumeProcessAcceptance)$' -count=1 -v -timeout 20m` PASS(13.61s, 18.91s). 첫 Bash local gate는 `TestDynamicClusterConnectionRebalanceProcessAcceptance`에서 Windows WAL replace `Access is denied`로 실패했다. 이 시험 단독 재실행 PASS(16.24s), Bash local 재실행 PASS(`local: PASS`, 263.74s). 계획에 따른 PowerShell `pwsh -NoProfile -Command '$env:GOMAXPROCS = "4"; Get-Date -Format yyyy-MM-dd; go run ./tools/verify local'` PASS(`local: PASS`, 270.95s; traceability P0 71/81, exceptions 10). PowerShell `pwsh -NoProfile -Command 'go run ./tools/verify markdown-links'` PASS(155 files, 2,263 local links). SDK Docker drain suite **미실행**: 계획상 이 suite는 test-only 값만 확인하므로 이번 변경 뒤 재실행하지 않았다. `tests/compatibility`에서 `session-metadata`를 찾지 못했고, feature 비활성 drain의 기존 Will 억제 동작은 바뀌지 않았다. |
| EKS 라이브 NLB 라이프사이클 및 전파 실측 (DEP-22, DEP-03, SPEC-TBD-04) | 라이브 클러스터(`narumq-eks`)에서 Node 3 drain 시작 후 readiness, EndpointSlice 및 AWS NLB TargetGroup 상태 전파 실측 | **2026-10-14 관측**: T0 drain 시작 `2026-10-14T02:22:30.437331Z`; `/health/ready`는 +0.079s에 HTTP 503. EndpointSlice `ready: false` T1 `02:22:41.957972Z`(+11.520641s), AWS TargetGroup `draining` T2 `02:22:43.085056Z`(+12.647725s); T2는 30초 전파 예산 이내였다. `nlbRoutingIsolated=false`: NLB 경유 20개 MQTT probe는 모두 unauthenticated였고 하나도 accepted되지 않아 healthy target에만 성공적으로 라우팅됨을 증명하지 않는다. 별도 direct drain-target probe는 `CONNACK 0x03`(Server Unavailable) 거절을 관측했다. 이 결과는 accepted-traffic 또는 active-connection 안전성을 증명하지 않으며, authenticated routing 및 established-connection drain도 시험하지 않았다. 아티팩트: `.artifacts/verification/nlb-lifecycle-verification-report.json`. |
| EKS 라이브 preStop 훅 시맨틱 계약 및 재진입 실측 (DEP-13, DEP-14) | `tests/integration/eks_deployment_test.go` 시맨틱 계약 검증 및 라이브 파드(`narumq-2`) 내 preStop 훅 2회 연속 실행 | **2026-10-14 PASS**: preStop 스크립트 시맨틱 계약(GET 사전확인, 404 시 POST, Bearer 인증, Idempotency-Key, 600s 데드라인, COMPLETED/safeToStop 루프) 검증 `TestEKSDeployment` 통과. 라이브 파드 내 실행: Run 1은 최초 404를 감지해 POST 후 COMPLETED 대기하여 36.532s에 exit 0 완료. Run 2는 기존 COMPLETED 상태(HTTP 200)를 감지하여 중복 POST 없이 1.515s만에 exit 0 완료. 동일 operationId(`cb584088-d8f6-444c-b9f3-3332239b8b03`) 재사용 확인. 아티팩트: `.artifacts/verification/prestop-hook-verification-report.json`. |
| EKS 파드 재생성 후 동일 EBS 볼륨 재연결 관측 (DEP-21, DEP-17, NFR-07) | 파드(`narumq-1`) 재생성 후 동일 PVC/EBS 재연결, 쿼럼 및 적용 index 관측 | **2026-10-14 관측**: 정상 Pod 재생성에서 동일 PVC(`data-narumq-1`)와 EBS 볼륨(`vol-09c3a0e11277903d9`, gp3 20Gi 암호화)이 재연결되었고 쿼럼을 유지했다. 65.064s에 Ready로 복구되며 applied index 93→100이었다. 이는 durable ACK payload 검증이나 WAL/snapshot 무손실 증명은 아니다. EBS 손실, 다른 worker로의 재부착, crash fault는 시험하지 않았다. AWS TargetGroup 3/3 healthy 복구 92.851s는 보고서에 기록되어 있다. 아티팩트: `.artifacts/verification/ebs-storage-az-recovery-report.json`. |
| 격리 EKS controller 교체·재시작 (2026-10-15 재개) | 전용 `narumq-ha-acceptance-20261015`, broker `0.0.0`→합성 `0.0.1`, ARM64 platform digest, 실제 Management API·EBS 감사 PVC | **PASS, 제한 범위**: 세 broker UID 교체·Ready, active-drain controller 재시작 뒤 같은 operation/key 복구, 전체 교체 156.16초. 수정 target의 startup은 수동 readiness 호출 없이 통과했다. `.artifacts/verification/eks-resume-20261015T000337Z-21df3b/rollout-lifecycle-result.json`; `rollout-active-drain-restart.json`. 게시 release 호환성·최대 부하·나머지 restart 경계는 미검증. |
| 격리 EKS API 실패·target 독점 (2026-10-15 재개) | Controller 전용 hostAlias fault proxy는 실제 cluster/features를 전달하고 drain에 HTTP 503을 반환한다. 별도 CR은 같은 target Lease를 요구한다. | **PASS, 관찰 범위**: API 실패와 `target_owned`에서 세 broker UID 불변. `.artifacts/verification/eks-resume-20261015T000337Z-21df3b/rollout-api-unavailable.json`; `rollout-exclusive-target.json`. Proxy를 Service selector로 연결한 첫 설정은 preflight에서 거절됐고 복구 중 한 ordinal이 교체됐다. 이 실패한 설정은 no-mutation 성공 증거에서 제외했다(`fault-setup-recovery.json`). |
| 실패 drain 취소 뒤 재시작 | `TestStateMachineFailedDrainCancellationClearsFailure`, `TestDynamicClusterNodeDrainCancelTimeoutAndResumeProcessAcceptance`의 취소 후 재시작 | **수정 전 EKS·local FAIL → 수정 후 PASS**. 취소 전이의 failure fields를 지웠다. Membership drain 시험 PASS(0.795초), startup·drain 재시작 process 시험 PASS(28.180초). EKS에서 실패한 container의 image만 바꿔 같은 Pod UID·WAL·EBS를 복구했고, 후속 동일 EC2 비정상 재시작도 235.05초에 3-voter Ready와 기존 marker 읽기를 통과했다. 증거: `.artifacts/verification/eks-resume-20261015T000337Z-21df3b/drainfix-same-volume-recovery.json`, `powerloss-afterfix-result.json`. 이전 실패 기록은 보존한다. |
| Startup probe 종료 뒤 drain 기록 재시작 (2026-10-16 원인 분석) | `TestDynamicClusterRestartStartupOnlyAfterProbeKillAndCrashProcessAcceptance`: 3-node process에서 node 3에 `preStop` 같은 drain을 `COMPLETED`까지 실행하고 `SIGKILL`한다. 재시작한 node는 `/health/startup`만 호출한다. 이어 drain 없이 한 번 더 `SIGKILL`하고 같은 방식으로 확인한다. | **수정 전 FAIL → 수정 후 PASS**. 수정 전 tree(`9ddd24d`)에서는 시험 제한 시간 동안 `/health/startup`이 계속 `503`, `node is joining`이었다(시험 98.64초). 수정 후 tree에서는 16.63초에 통과했다. `.artifacts/analysis/clu14-powerloss-0210-rca.md`. |
| 격리 EKS Argo CD health 소비자 (DEP-18) | 실제 Argo CD Git sync 뒤 동일 `NaruMQRollout`의 status를 주입하고 리소스·Application health를 함께 확인한다. | **2026-10-15 PASS, 15개 case**. 명령·환경·실패한 초기 설정·증거·broker 미배포 경계는 [아래 수용 기록](#argo-cd-health-소비자-수용-2026-10-15)을 따른다. |
| 격리 EKS rollout controller 결함 matrix와 Argo CD 적용 교체 (DEP-11·DEP-18) | Fault proxy·admission webhook·controller restart·non-leader process·status CAS 경쟁으로 [Rollout controller v1 §3](../contracts/rollout-controller-v1.md#3-acceptance-matrix)의 남은 행을 실행한다. Argo CD가 Git에서 적용한 실제 broker 교체도 실행한다. | **2026-10-16 부분 검증**. 결함 matrix A~G는 PASS다(판정 수정 포함). Argo CD 적용 교체는 교체·health가 PASS, sync가 FAIL이다. 고친 controller에서는 Pod 교체 없는 sync만 PASS다. 결함 3개를 찾았다. 2개는 고친 controller로 다시 시험했고 1개는 단위 시험으로만 확인했다. `SAFE_TO_STOP` 뒤 retry 의미는 미결정이다. [아래 수용 기록](#external-rollout-controller-eks-결함-matrix-수용-2026-10-16)을 따른다. |
| 계획된 cross-worker 동일 EBS 복구 (DEP-17·DEP-21, NFR-07 일부) | `SAFE_TO_STOP` 뒤 exact UID·resourceVersion으로 Pod를 삭제한다. 같은 AZ의 다른 worker에서 기존 PVC·PV·EBS와 MQTT 상태를 확인한다. | **2026-10-15 PASS, 계획된 이동만**. 파일·명령·실측과 장애 수용의 남은 경계는 [아래 기록](#계획된-cross-worker-ebs-복구-수용-2026-10-15)을 따른다. |
| 단일 EC2 비정상 종료 뒤 동일 EBS 복구 (DEP-17·DEP-21, NFR-07 일부) | 단일 follower EC2의 OS shutdown을 생략해 종료한다. 종료 확인 전 operator storage 복구 변경은 금지하고, 새 worker의 동일 PVC·PV·EBS와 기존 MQTT 상태를 확인한다. | **2026-10-15 초기 observer FAIL / 같은 장애의 물리·MQTT 후속 관찰 PASS**. 전체 자동 수용은 미완료다. 실패·관찰 공백·명령과 증거는 [아래 기록](#비정상-단일-ec2-worker-복구-관찰-2026-10-15)을 따른다. |
| 단일 leader EC2 비정상 종료 (DEP-20·DEP-17, NFR-07) | 현재 leader의 EC2를 OS 종료 없이 종료한다. 생존 follower로 관찰하며 새 leader 선출, 30초 이하 단일 불가용 구간, 같은 PVC·PV·EBS 복구와 durable stream RPO 0을 확인한다. | **2026-10-16 PASS(1회)**. 수정 image `sha256:edcff675…`, leader node 1(`ap-northeast-2b`)의 EC2를 `terminate-instances --skip-os-shutdown`으로 종료했다. 장애 요청 9.2초 뒤 node 3이 leader로 관찰됐다. 장애 뒤 관리 API 26/26·MQTT 37/37 표본이 성공해 표본 기준 불가용 구간은 없었다. 장애 요청 61.3초 뒤 같은 AZ 여분 worker에서 같은 PVC·PV·EBS로 Ready가 됐다. Durable stream은 acked 25개(22개는 장애 요청 뒤 전송)를 `SessionPresent=1`로 모두 받았다. 표본 사이 연속 가용성과 cross-host 시계 오차는 확인하지 않았다. [기준](#dep-20-leader-ec2-손실) |
| 단일 AZ 전원 중단 모델 (DEP-20, NFR-07) | Follower 한 개가 있는 AZ의 모든 worker를 OS 종료 없이 정지하고 대체 용량 없이 유지한 뒤 다시 시작한다. 중단 중 남은 두 AZ의 quorum·MQTT·session 인계와 재시작 뒤 3-voter 복구, durable stream RPO 0, 통제 해제 뒤 180초 안정 상태를 확인한다. | **2026-10-16 PASS(모델 범위, 1회)**. 2차 실행(수정 image `sha256:edcff675…`, `ap-northeast-2a`, node 3): 480초 정지 동안 관리 API 74/74·MQTT 254/254 표본 성공, 중단 중 생존 node에서 `SessionPresent=1`과 acked 60/60 수신, 시작 요청 뒤 74.0초에 대상 Pod Ready(같은 PVC·PV·EBS), 재개 뒤 180초 동안 ASG activity·instance·Pod UID 변화 0. 1차 실행(이전 image `sha256:1d780c51…`)도 중단·회복 조건은 통과했으나 재개 직후 ASG 교체가 판정 밖에서 일어나 기준을 보완했다. 표본 관찰이며 표본 사이 연속 가용성, network·EBS 서비스·control plane 장애는 확인하지 않았다. [기준과 기록](#dep-20-단일-az-전원-중단-모델) |
| EBS disk pressure·ENOSPC (DEP-21, NFR-07) | 실제 broker EBS filesystem에 ballast를 써서 follower admission 압박(`0x97`), follower ENOSPC, 두 follower ENOSPC를 차례로 만든다. 공간 회복과 재시작 뒤 durable stream RPO 0을 확인한다. | **2026-10-16 PASS(1회, 3번째 실행)**. 수정 image `sha256:407129c5…`, leader node 3(관찰), 대상 node 4·2, 부하 128 KiB × 2 topic. **D1**: 여유 공간 512 MiB에서 첫 `0x97`이 0.15초에 왔고(15회 모두 `0x97`), 관찰 표본 25/25 성공, ballast 축소 뒤 0.77초에 대상 경로 publish가 성공했다. **D2**: 대상 filesystem을 여유 0 byte까지 채운 4초 뒤 부하를 시작했다. 부하 시작 뒤 관찰을 시작하자 첫 표본에서 대상 자신의 관리 API에 `storage_no_space`·`storagePressure=true`·`replicationHealth=FAILED`가 보였다. ENOSPC가 첫 부하 publish보다 먼저 일어났는지는 확인하지 않았다. Pod는 6.3초에 NotReady가 되었다. 생존 경로 관리 API 54/54·MQTT 55/55 성공. Ballast 제거 45.0초 뒤 같은 Pod UID로 3개 Ready(container 재시작 1회). Durable 29/29, 누락·중복 0. **D3**: 두 대상 모두 자신의 API에서 storage 실패가 보인 10:04:18부터 회복까지 보낸 3개는 모두 결과 미상(`connection_lost`)이고 `acked`는 없었다. 회복 54.5초. Durable PUBACK 3개 모두 수신, 누락 0, 결과 미상 3개 미전달. 두 시나리오 모두 retained 부하 정리가 완료되었다(topic마다 PUBACK reason 0). 앞선 실행: 1번째는 D1 PASS. 2번째(storage-2)는 D2·D3 도구 결함(원격 행으로 판정, 관찰 tunnel 끊김)으로 판정 불가였고, 남은 1 MiB retained 부하가 CLU-14 재시드의 snapshot 결함을 드러냈다([Cluster §7](cluster-session-and-delivery.md#7-failure-recovery) 7항). 증거: `.artifacts/verification/eks-matrix-20261016T065350Z-24adc2/storage-3/eks-matrix-20261016T065350Z-24adc2/summary.json`. 표본 관찰이며 반복 성능이 아니다. |
검증 환경은 Windows/amd64, Go 1.26.5다. 검증 기준 HEAD는 `ee01d3aeeb7a6637b1bca6dfa529561f3257d3f0`이며, PowerShell gate 실행 당시 아래 16개 변경 파일은 모두 미커밋이었다: `docs/TODO.md`, `docs/contracts/management-api-v1.md`, `docs/contracts/management-api-v1.openapi.json`, `docs/contracts/mqtt-client-compatibility-v1.md`, `docs/decisions/ADR-0016-node-drain-and-safe-to-stop.md`, `docs/decisions/ADR-0051-cluster-update-strategy.md`, `docs/decisions/README.md`, `docs/decisions/ADR-0055-drain-and-reconnect-operating-defaults.md`, `docs/explainers/safe-node-drain-and-will.md`, `docs/specs/README.md`, `docs/specs/drain-and-rolling-update.md`, `internal/adapters/http/management/drain_handlers.go`, `internal/adapters/http/management/drain_handlers_test.go`, `internal/domain/nodedrain/drain.go`, `internal/domain/nodedrain/drain_test.go`, `tests/contracts/management-api-v1/contract_test.go`. PowerShell gate 뒤에는 이 문서와 ADR-0055의 검증 기록만 갱신했다. Go 코드와 시험은 바꾸지 않았다. Markdown 링크 검사는 최종 기록 뒤 다시 실행했다.



Broker process test는 Server Reference packet을 확인한다. SDK drain suite는 callback adapter의 1회 임시 redirect·session resume·Will 억제를 두 MQTT 5 SDK에서 확인했다. Go의 중복 Server Reference 처리는 실패했으므로 전체 SDK gate는 열려 있다. 표준 계획 인계 Will과 DEP-12 metric은 ADR-0028 수용 범위다.

EKS 실측(2026-10-14)은 `preStop` 훅 시맨틱·실행, EndpointSlice/NLB 관측 전파 시각, 별도 direct drain-target의 `CONNACK 0x03` 거절, 그리고 정상 Pod 재생성 후 동일 PVC/EBS 재연결·쿼럼 유지·applied index 93→100을 기록했다. 20개 unauthenticated MQTT probe 중 accepted된 것은 없고 `nlbRoutingIsolated=false`이므로 healthy target로의 성공적 독점 라우팅이나 accepted traffic/active connection 안전성은 입증되지 않았다. EBS 시험은 durable ACK payload, WAL/snapshot 무손실, EBS 손실, cross-worker 재부착 또는 crash fault를 검증하지 않았다. Local rollout controller의 실제 broker/Kind lifecycle acceptance는 위 §10에 기록했지만 이 증거는 전체 DEP 수용을 뜻하지 않는다. Production EKS controller rollout, 물리 HW 전원·단일 AZ 손실 카오스, 연속 100회 rolling·BP-07, N-1↔N, 및 MQTT 3.1.0 migration은 열린 gate로 유지한다.

### ADR-0028 연속 rolling 관측

아래 값은 각 round의 전후 모든 node counter 합 차이다. Node restart로 counter가 0으로 돌아가기 전에 차이를 기록했다.

| reason | Round 1 (node 3) | Round 2 (node 1) | Round 3 (node 2) | 합계 |
| --- | --- | --- | --- | --- |
| `suppressed_normal_disconnect` | 1 | 0 | 0 | 1 |
| `suppressed_reconnected_within_delay` | 1 | 1 | 1 | 3 |
| `expected_mqtt311_server_disconnect` | 1 | 0 | 0 | 1 |
| `expected_zero_or_expired_will_delay` | 2 | 0 | 0 | 2 |
| `unexpected_supported_client` | 0 | 0 | 0 | 0 |
| `unplanned_failure` | 0 | 0 | 0 | 0 |

회귀 명령은 저장소 root에서 실행했다.

```sh
GOMAXPROCS=4 go test ./tests/integration -run '^(TestDynamicClusterNodeDrainProcessAcceptance|TestDynamicClusterNodeDrainCancelTimeoutAndResumeProcessAcceptance|TestDynamicClusterSessionMetadataProcessAcceptance|TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum|TestDynamicClusterDurableQoS2WillProcessAcceptance|TestDynamicClusterMQTTConnectOwnershipTakeoverProcessAcceptance)$' -count=1 -v -timeout 30m
```

기존 SDK 9/10 결과와 production EKS rollout 증거 미완료 상태를 유지한다. §10의 local controller smoke는 synthetic TEST-ONLY image transition이며 published-release compatibility, production B*, MQTT-13 ≥90일 관측 또는 independent assessor를 대체하지 않는다.

### Argo CD health 소비자 수용 (2026-10-15)

[Rollout controller v1 §3](../contracts/rollout-controller-v1.md#3-acceptance-matrix)의 `Argo CD health` 행과 ADR-0006 §7 검증 6을 확인했다. 대상은 HEAD `8096f30a05735084bfafd6eb42b2763c2db31f79` 작업 트리의 기존 `argocd-health.lua`와 `crd.yaml`이다. 두 파일은 이번 시험에서 수정하지 않았다. 환경은 별도 local state·VPC로 만든 EKS 1.37, ARM64 `m7g.large` worker 3대, Argo CD core `v3.5.5`, kubectl `v1.37.1`이다. Argo CD·Redis·시험용 Git 서버는 ARM64 manifest digest로 고정했다. 버전·hash·실제 Pod imageID는 [설치 출처](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/argo-install-provenance.json)와 [실행 출처](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/argo-runtime-provenance.json), [최종 실행 설정](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/argo-final-runtime.json)에 있다.

실제 Git repository를 내부 ClusterIP로 제공했다. Argo CD가 고정 revision에서 시험용 `NaruMQRollout`을 동기화했다. 이후 Kubernetes status subresource에 상태를 주입했다. 각 case는 Git sync가 `Synced`이고 ComparisonError가 없으며 리소스와 Application의 health가 모두 기대값일 때만 통과했다. 명령은 전용 임시 observer의 `python -B observe-health.py`다. 정확한 입력·기대값과 관찰 시각·resourceVersion·message는 [입력 matrix](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/health-matrix-inputs.json)와 [15개 case 결과](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/health-matrix-result.json)에 있다.

실행한 observer 원문은 [검증 아티팩트](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/observe-health.py)에 보존했다. 보존본의 SHA-256은 실행 당시 기록한 `observerSha256`과 같다. 실행 당시에는 전용 private 디렉터리의 `kubectl.exe`·`kubeconfig`와 같은 위치에서 실행했다. 이 입력과 AWS fixture는 시험 후 제거했으므로 보존본 자체를 현재 환경에 실행하는 재시험 명령으로 사용하면 안 된다.

완료 후 실패, 취소 후 알 수 없는 phase, 완료 후 status 삭제 전이도 통과했다. 이전 `Healthy`가 남지 않았다. `failureReason`의 표시와 빈 phase도 확인했다. 같은 CR UID와 spec을 유지했으며 broker Pod는 0개였다. 이는 health 소비자 수용이다. NaruMQ controller의 상태 생성, 실제 broker drain·교체, GitOps 전체 배포, 게시 release·최대 부하 수용은 검증하지 않았다.

초기 설정 실패는 보존했다. Argo Application을 repo-server 준비 전에 만들었으므로 첫 비교에 연결 거절이 기록됐다. 첫 observer는 자체 90초 제한으로 종료됐다. 준비 완료 뒤 hard refresh를 요청하고 observer의 실행 제한을 내부 wait보다 길게 맞췄다. Argo CD의 기본 `appTree` 저장 방식으로는 리소스별 health가 Application CR에 없으므로, 공식 `ARGOCD_APPLICATION_CONTROLLER_PERSIST_RESOURCE_HEALTH=true` 옵션을 켜서 두 health를 함께 관찰했다. 이 옵션은 health 판정 코드를 바꾸지 않는다. [초기 실패](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/initial-sync-failure.json)와 [관찰 설정](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/observer-configuration.json)을 참조한다. Lua 연결 절차는 [개발 가이드](../how-to/development-guide.md#argo-cd-health-mapping-연결과-확인)에 있다.

시험 뒤 두 전용 namespace와 Terraform 관리 자원 41개를 삭제했다. 직접 AWS 조회에서 새 root EBS 3개, 전용 EKS·VPC·EIP·ENI·IAM·ASG·CloudWatch log group 부재와 NAT `deleted`를 확인했다. 최종 state의 관리 자원은 0개다. 보호 snapshot 3개와 기존 공유 image는 그대로다. 새 application PVC·NLB·ECR image는 만들지 않았다. 전체 AWS 비용이 0이라는 뜻은 아니다. [직접 정리 확인](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/direct-cleanup-verification.json)과 [최종 실행 기록](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/final-summary.json)을 따른다. 제품 코드 변경이 없어 전체 local suite는 실행하지 않았으며 과거 full-local 실패 상태도 바꾸지 않았다.

### External rollout controller EKS 결함 matrix 수용 (2026-10-16)

결과는 **남은 결함 matrix A~G 통과와 Argo CD 적용 교체의 부분 검증**이다. A~G의 마지막 결과 파일은 모두 `PASS`다. Argo CD 적용 교체 결과 파일(`rollout-argo-upgrade.json`)은 sync 판정이 `FAIL`이다. 고친 controller에서 Pod 교체 없이 sync만 확인한 결과(`rollout-argo-sync-r3.json`)는 `PASS`다. 시험 중 controller 결함 3개를 찾았다. 2개는 고친 image로 다시 시험했다. 나머지 1개는 시험 환경을 지운 뒤 단위 시험으로만 확인했다. 과거 실패 기록과 처음 판정 결과는 그대로 보존했다.

환경은 run `eks-rollout-20261016T122800Z-a3f1b2`다. 별도 private Terraform state와 VPC(`10.39.0.0/16`)로 EKS cluster `narumq-rollout-20261016-a3f1b2`를 만들었다. 생성 계획은 새 자원 41개만 만들었다([계획 검토](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/create-plan-review.json)). 변경 명령은 모두 kubeconfig context, cluster endpoint·tag, namespace label이 같은 run을 가리킬 때만 실행했다([fixture identity](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/fixture-identity.json)). Broker는 합성 데이터만 쓰는 3-voter `OnDelete` StatefulSet이다. Broker image는 같은 소스에서 version label만 다른 arm64 1.2.0·1.3.0·1.4.0이다([image 출처](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/image-provenance.json)).

도구는 [main.py](../../tests/chaos/eks-rollout-faults/main.py)다. CLI 진입점은 없고, 운영자 Python 세션에서 시나리오 함수를 차례로 호출했다. [Fault proxy](../../tests/chaos/eks-rollout-faults/proxy.py)는 controller와 broker Management Service 사이에서 실제 broker 응답을 바꿔 drain GET 장애를 만든다. [Admission webhook](../../tests/chaos/eks-rollout-faults/delete_delay_webhook.py)은 Pod DELETE 응답을 늦춰 결과 불명을 만든다. Pod DELETE는 EKS Kubernetes API audit log(CloudWatch)로 교차 확인했다. 도구 자체는 `python -B tests/test_eks_rollout_faults.py`로 시험한다(15개 PASS, AWS·kubectl 호출은 fake).

Controller image는 세 가지다. 처음 image는 `sha256:bfd26fd0cac446668b22e80e4b576fcd628b1b809b5784002f195ae1d2a4e790`이다. r2(`sha256:89dca1673f5e9c23b96fc894cc931cd0738740c86f2f1e9777f90d54d55b79c8`)는 아래 결함 1을 고쳤다. r3(`sha256:ba7bee40edfb7af8997e501d83af0395ccd75f3ea3b06f0c964bc2757fdac9b7`)는 r2에 결함 2의 수정만 더했다. r2와 r3의 차이는 `internal/adapters/rollout/reconciler.go`의 finalizer write뿐이다.

| 시나리오 | Controller | 결과 | 증거 |
| --- | --- | --- | --- |
| A. 실제 broker drain `FAILED` | r2 | PASS(판정 수정) | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-drain-failed.json), [처음 판정](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-drain-failed.initial-check-fail.json) |
| B. Drain GET incomplete·malformed·503·응답 지연 뒤 deadline | r2 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-get-faults-deadline.json) |
| C. 실제 PVC audit append 장애 | r2 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-audit-sink-failure.json) |
| D. Pod DELETE 결과 불명, 그 중 controller restart, Pod resourceVersion 변경 | r2 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-delete-unknown.json), [중단된 두 번째 시도](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-delete-unknown.attempt2-incomplete.json) |
| E. Drain POST 결과 불명 중 restart, non-leader process | r2 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-post-unknown-non-leader.json) |
| F. Status CAS 충돌 | r2 | PASS(판정 수정) | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-cas-conflict.json), [처음 판정](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-cas-conflict.initial-check-fail.json) |
| G. 실제 audit 파일의 approval·denial·failure | r2 | PASS(판정 수정) | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-audit-events.json), [처음 판정](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-audit-events.initial-check-fail.json) |
| A~G 구간의 Kubernetes audit DELETE | 처음·r2 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-kubernetes-audit-deletes.json), [구간](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/scenario-windows.json) |
| Argo CD 적용 교체 1.3.0→1.4.0 | r2 | **FAIL**(sync 1건, 결함 2) | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-upgrade.json), [처음 판정](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-upgrade.initial-check-fail.json), [Git 내용](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-git.json) |
| Argo CD 단계의 Kubernetes audit DELETE | r2 | PASS(판정 수정) | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-kubernetes-audit.json), [처음 판정(같은 기록으로 재계산)](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-kubernetes-audit.initial-check-fail.json) |
| Argo CD finalizer sync 재확인 | r3 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-sync-r3.json) |

처음 image의 A·B·C·D 결과는 `*.controller-bfd26fd0.json`으로 보존했다. 처음 image의 D는 blocker 표시 확인에서 실패했다.

주요 실측은 다음과 같다.

- B: 각 장애 구간에서 실제 broker drain은 `DRAINING`이었다. Rollout은 `Draining`에 머물렀고 delete와 다른 node drain 요청은 0건이었다. GET 간격 중앙값은 incomplete 5.23초, malformed 5.2초, 503 5.19초, 응답 지연 10.17초였다. 이어 저장된 deadline에서 `drain_timeout`으로 실패했다.
- C: Audit 파일을 변경 불가로 만들자 실제 `SAFE_TO_STOP` 뒤에도 `audit_unavailable`로 45초 동안 차단했다. 승인 저장, 파일 변경, API server에 도달한 DELETE는 0건이었다.
- D: Rollout은 `delete_unconfirmed`를 계속 표시했고 같은 UID/RV precondition으로만 재시도했다. Restart 뒤 새 leader도 같은 precondition을 썼다. Pod resourceVersion이 바뀌자 fresh 증거로 다시 승인했다. 이전 precondition의 DELETE는 API server가 409로 거절했고, 다음 DELETE가 성공했다. deletionTimestamp 뒤 중복 DELETE는 0건이었다.
- E: 새 leader는 저장된 idempotency key로 POST를 replay하고, broker가 첫 POST로 만든 operation을 채택했다. Non-leader process는 Management API를 호출하지 않았고 Lease를 가져가지 않았다.
- F: Controller log에 CAS 충돌 4건이 있었다. 2건은 `Draining`, 2건은 `DrainSafe` 승인 write에서 났다. 충돌 동안 delete intent 저장과 Pod 삭제는 0건이었다. 승인은 충돌 뒤 다시 조정해 성공했다.
- Argo CD: Argo CD core `v3.5.5`가 시험용 Git 서버의 revision `08dc1c3e5552a33df87011ca83d858ed2f4b2046`에서 StatefulSet template(1.4.0)과 `NaruMQRollout`을 동기화했다. 15:46:15~15:49:30 UTC(195초)에 r2 controller가 세 broker를 하나씩 교체했다. 각 Pod에서 controller DELETE는 1건이었다. Argo CD의 DELETE는 0건이었다. Kubelet의 마무리 DELETE는 같은 UID precondition으로 Pod object를 지우는 Kubernetes 동작이다. Application은 교체 중 `Progressing`, 완료 뒤 `Healthy`였다. 그러나 완료 뒤 Application은 `OutOfSync`였으므로 이 결과 파일의 sync 판정은 `FAIL`이다(결함 2).
- Argo CD finalizer sync 재확인: r3에서 이전 CR을 지우자 controller가 finalizer를 patch로 해제했다. 새 revision `aee757cb1d73671247c52a8a2bdd40bbccc2c2e0`의 CR은 모든 Pod가 이미 1.4.0이므로 Pod 교체 없이 완료됐다. 완료 뒤 60초 이상 Application과 CR이 `Synced`·`Healthy`였다. Live CR에는 `spec.abort: false`와 finalizer가 함께 있었다. 이 확인은 finalizer write만 검증한다. 고친 controller로 실제 교체와 sync를 함께 확인하지는 않았다.

찾은 결함과 처리는 다음과 같다.

1. **Status 반복 write와 blocker 깜박임(처음 image)**: Controller는 broker의 밀리초 시각을 초 단위로 저장된 시각과 비교했다. 그래서 매 조회가 "증거 변경"이 되어 status를 다시 썼다. 다시 쓸 때마다 `failureCode`가 지워지고 Ready=True가 됐다. 매 write는 resourceVersion을 바꿔 watch event를 일으켰고, poll 간격 없이 바로 다시 조정했다. 14:29:44~14:35:16에 controller DELETE가 946건(500 945건, 409 1건), GET이 초당 약 6건이었다. r2는 시각을 저장 정밀도로 줄여 비교하고, 바뀐 내용이 없으면 status를 쓰지 않는다([계약 §2.6](../contracts/rollout-controller-v1.md#2-reconciliation-contract)). `TestUnknownDeleteRetryKeepsBlockerWithoutRewritingUnchangedStatus`는 수정 전 실패(`writes=10 want=8`), 수정 뒤 통과했다.
2. **Finalizer 설치가 Git spec을 지움(r2)**: Controller가 CR 전체를 `Update`로 다시 써서 `spec.abort: false`가 사라졌다. CRD의 `abort`에는 기본값이 없다. 그래서 Argo CD가 CR을 계속 `OutOfSync`로 판단했다. Sync 작업 자체는 `Succeeded`였다. r3는 `metadata.finalizers`만 바꾸는 merge patch를 resourceVersion 잠금과 함께 쓴다([계약 §2.11](../contracts/rollout-controller-v1.md#2-reconciliation-contract)). `TestReconcilerMissingSecretsAndNormalReconcileCAS`는 수정 전 실패(patch 0건), 수정 뒤 통과했다.
3. **Deadline 뒤 승인 단계가 끝나지 않음(처음 image에서 관찰)**: `DrainSafe`·`DeleteApproved`에서 저장된 deadline이 지나면 controller가 `barrier_unknown`으로 계속 차단했다. Delete는 0건이었지만 `Failed`와 operator action이 표시되지 않았다. 원인은 `requireSafeDrain`이 deadline 경과를 일반 오류로 돌려준 것이다. 시험 뒤 deadline 경과를 `Failed`/`drain_timeout`으로 끝내도록 고쳤다(§2.9). 이때 broker drain은 이미 `SAFE_TO_STOP`이라 취소할 수 없다. 그래서 status와 `rollout.failed` 감사 event의 operator action은 `spec.abort=true`만 안내한다. `TestApprovalAfterPersistedDeadlineFailsWithoutDelete` 4개 case가 수정 전 모두 실패, 수정 뒤 통과했다. 이 수정은 EKS에서 다시 실행하지 않았다.

미결정 사항이 하나 있다. Broker는 `SAFE_TO_STOP` drain을 취소하지 않는다(§2.3). 그래서 `SAFE_TO_STOP` 뒤 실패한 rollout에서 새 `retryToken`은 `retry_cancel_unconfirmed`로 차단된다. `spec.abort=true`는 delete 없이 `Aborted`로 끝났다([abort 결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-abort-after-safe-to-stop.json), 운영자 세션에서 직접 판정). 이 경우의 retry 의미는 [TODO §1.2](../TODO.md#12-release와-rollout-gate)에서 정한다.

판정 수정은 관찰 데이터가 처음 기대와 달랐을 때만 했다. 각 결과 파일의 `checkRevision`에 이유가 있다.

- A: 저장된 deadline을 초 단위로 줄였으므로 controller deadline이 broker deadline보다 먼저 온다. 그래서 controller가 broker `FAILED`보다 먼저 `drain_timeout`으로 실패한다. 판정은 실제 broker GET이 같은 operation의 `FAILED`를 보고할 때까지 기다리도록 바꿨다.
- F: Status adapter가 UID·resourceVersion 변경을 스스로 감지하므로 API server 409는 생기지 않는다. 승인 write 충돌은 3건이 아니라 2건이었다.
- G: 이전 시도 CR의 approval까지 세었으므로 rollout UID로 범위를 좁혔다.
- Argo CD health: 첫 표본(15:46:32)은 CR이 Argo CD resource tree에 나타나기 전이었다. 이 `Healthy`는 Argo CD 자체 집계다. 판정은 CR이 tree에 있는 표본만 본다. Sync 판정 FAIL은 그대로 둔다.
- Argo CD audit DELETE: Kubelet의 마무리 DELETE를 controller DELETE의 완료로 인정했다. 처음 결과 파일은 두 번째 실행이 덮어썼으므로, 같은 audit 기록에 원래 판정을 다시 계산해 보존했다.

범위의 한계는 다음과 같다. 합성 fixture에서 한 번 실행했다. A~G는 r2, finalizer sync는 r3, 결함 3 수정은 단위 시험으로만 확인했다. 다른 OCI/version preflight 거절 조합, 승인 직전·replacement 대기 중 restart는 EKS에서 주입하지 않았다. 게시 release N-1↔N과 rollback은 `CLU-11`, 최대 부하와 연속 100회는 `DEP-01~12` TODO 항목으로 넘겼다.

시험 뒤 정리했다. Rollout CR, Argo CD Application·AppProject, `narumq`·`argocd` namespace, PVC·PV 4개, CRD 4개, ClusterRole·ClusterRoleBinding 2개씩을 지웠다. `terraform destroy`가 41개를 삭제했다. EKS log group과 이번 run의 ECR tag 6개를 지웠다. AWS를 직접 조회해 EKS·VPC·NAT·EIP·EC2·ENI·EBS·IAM role·policy·OIDC·log group·run ECR tag의 부재를 확인했다. 보호 snapshot 3개와 공유 ECR tag 2개는 그대로다([정리 확인](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/cleanup-verification.json)). Private 파일은 DPAPI로 암호화했다.

### 계획된 cross-worker EBS 복구 수용 (2026-10-15)

대상은 commit `9c96670`과 기존 제품 코드다. 코드·배포 template은 바꾸지 않았다. 별도 local state·VPC의 EKS 1.37에 `m7g.large` 3대와 빈 gp3 PVC 3개로 합성 3-voter cluster를 만들었다. 초기 startup은 수동 readiness GET 없이 Kubernetes probe만으로 통과했다. 기존 합성 fixture의 인증 설정만 재사용했고 새 cluster ID와 PVC를 사용했다. 원본 archive나 보호 snapshot은 복원하지 않았다. 이동 대상 EBS와 같은 `ap-northeast-2c`에 임시 ARM64 worker 하나를 추가했다. [배포 기록](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/fixture-deployment.json)과 [검증한 이미지](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/task-image-verified.json), [worker·AMI·volume 소유 기록](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/cleanup-ownership.json)을 참조한다.

시험 파일은 [MQTT oracle](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/mqtt-oracle.py)과 [worker 이동 observer](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/relocate-worker.py)다. Python 3.14의 전용 임시 venv에 Paho MQTT `2.1.0`을 설치했다. MQTT 5 password 인증과 session expiry 3,600초를 사용했다. 파일에는 비밀 값을 넣지 않았다. `--private` 위치의 전용 kubeconfig·kubectl 1.37.1·인증 파일과 같은 run 디렉터리의 `relocation-plan.json`이 선행 입력이다. Survivor Pod의 management/MQTT는 localhost `23880`/`23883`, 대상 Pod는 `28880`/`28883`으로 전달했다. 아래는 실제 실행 순서다. 각 `<...>`는 당시 전용 경로다. 환경을 폐기했으므로 현재 그대로 실행하는 재시험 명령은 아니다.

```text
<venv-python> -B <run-directory>/mqtt-oracle.py prepare --private <private-directory>
<venv-python> -B <run-directory>/relocate-worker.py --private <private-directory>
<venv-python> -B <run-directory>/mqtt-oracle.py verify --private <private-directory>
```

첫 명령은 persistent subscription을 만든 뒤 연결을 끊었다. 이어 retained marker 하나와 오프라인 QoS 1 메시지 세 개의 성공 PUBACK을 확인했다. 이동 observer는 follower node 2의 기존 worker를 cordon하고 실제 drain API의 `COMPLETED/SAFE_TO_STOP`을 확인했다. 연결 잔여 0, pending WAL 0, under-replicated 0과 실패 없음도 확인했다. 이후에만 UID·resourceVersion precondition을 사용해 Pod를 삭제했다. [삭제 전 barrier 증거](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/safe-before-delete.json)가 순서를 입증한다.

관찰 시작 후 64.008초에 다른 EC2 worker의 새 Pod를 포함한 broker 3개가 Ready인 것을 확인했다. 이 시간에는 cordon과 drain이 포함된다. AWS attachment 확인과 복구 후 표본 수집까지 포함한 observer 전체 실행은 73.746초였다. PVC UID·PV 이름·EBS volume ID가 같았고 AWS attachment와 CSI VolumeAttachment는 새 worker를 가리켰다. Survivor 경로의 36개 표본은 모두 quorum available과 성공 PUBACK을 기록했다. 이는 표본 관찰이며 표본 사이의 모든 순간이나 production SLO를 증명하지 않는다. 세 번째 명령은 이동한 broker에 재연결해 `sessionPresent=true`, 기대한 오프라인 QoS 1 payload hash 세 개와 retained hash를 확인했다. 재발행은 하지 않았고 이 실행의 QoS 1 중복은 0개였다. [물리 복구 결과](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/physical-relocation-result.json), [연속 관찰](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/continuity-observations.json), [복구 후 MQTT 결과](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/mqtt-after-relocation.json), [최종 broker·CSI 기록](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/recovered-broker-and-csi.json)을 따른다.

결과는 [계획된 cross-worker 수용 PASS](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/acceptance-result.json)다. 살아 있는 다른 voter의 복제도 사용하므로 EBS만을 원본으로 한 독립 복구를 입증하지 않는다. EC2 전원 손실·종료, 강제 EBS detach, 단일 AZ 손실, volume loss/reseed, 전체 voter durable commit과 자연 발생 WAL torn-tail, production 최대 부하는 시험하지 않았다. 기존 `CLU-14 FAILED/UNRESOLVED`를 바꾸지 않는다. 초기 운영 명령 `kubectl rollout status`는 `OnDelete`에서 지원되지 않아 실패했으며 `kubectl wait --for=condition=Ready pod -l app=narumq`로 startup을 관찰했다. 제품 결함이나 통과 증거로 이 명령 실패를 사용하지 않는다.

수용 증거를 닫은 뒤 임시 node group 하나와 Terraform 관리 자원 41개를 삭제했다. 직접 AWS 조회에서 새 root EBS 4개·broker EBS 3개, 전용 EKS·VPC·EIP·ENI·IAM·ASG·CloudWatch log group, 작업 image tag·digest의 부재와 NAT `deleted`를 확인했다. 최종 state의 관리 자원은 0개다. 보호 snapshot 3개와 공유 image는 유지했다. 합성 데이터를 폐기하는 정리 단계에서만 StatefulSet을 orphan 삭제한 뒤 Pod를 grace 0으로 제거했다. 이 강제 정리는 앞선 exact UID·`SAFE_TO_STOP` 수용 경로와 별개이며 장애 수용 증거로 쓰지 않는다. [정리 경계](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/namespace-cleanup.json), [직접 AWS 확인](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/direct-cleanup-verification.json), [최종 실행 기록](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/final-summary.json)을 따른다. 인증 자료·배포 입력·plan·state backup은 DPAPI `CurrentUser`로 보존했다. 비밀 값이 없는 두 observer 소스는 검증 아티팩트로 남겼다. 제품 코드 변경이 없어 전체 local suite는 실행하지 않았고 과거 실패 상태도 유지했다.

### 비정상 단일 EC2 worker 복구 관찰 (2026-10-15)

결과는 **초기 observer 실패와 같은 장애의 복구 후속 관찰을 구분**한다. 초기 실행을 통과로 바꾸지 않았고 장애도 다시 주입하지 않았다. [결과 기록](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/acceptance-result.json)의 상태는 `LIMITED_RECOVERY_OBSERVED_WITH_FAILED_INITIAL_OBSERVER`다. 기존 `CLU-14 FAILED/UNRESOLVED`, 전체 EC2·AZ 장애 matrix, 연속 가용성과 자동 observer 완료 gate는 열린 상태로 유지한다.

대상 HEAD는 `0c2e527`이다. 제품 코드가 `9c96670`과 같음을 확인한 뒤 이전 합성 ARM64 broker image를 작업 전용 tag로 다시 게시했다. 공유 image는 바꾸지 않았다. 별도 local state·새 VPC의 EKS 1.37, `m7g.large` 3-AZ worker, 새 gp3 PVC 3개를 사용했다. 기존 합성 fixture에서는 인증 설정만 재사용했다. 초기 startup은 수동 readiness GET 없이 Kubernetes probe로 통과했다. 같은 AZ의 별도 Ready worker 하나와 대상 follower EBS의 완료된 전용 snapshot을 먼저 확보했다. 실제 `/data` filesystem은 ext4였고 data volume의 `DeleteOnTermination=false`도 확인했다. [배포 입력 경계](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/fixture-deployment.json), [fault plan](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/fault-plan.json), [사전 점검](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/fault-preflight.json), [filesystem](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/target-filesystem.json)을 참조한다.

시험 소스는 [MQTT oracle](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/mqtt-oracle.py), [최초 abrupt worker observer](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/abrupt-worker.py), [읽기 전용 후속 observer](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/observe-recovery.py)다. 전용 임시 venv의 Python 3.14·Paho MQTT 2.1.0과 kubectl 1.37.1을 사용했다. `<private-directory>`에는 해당 fixture의 kubeconfig·kubectl·인증 파일이 필요하다. 같은 run 디렉터리의 plan도 선행 입력이다. 아래는 당시 실행 순서이며 폐기한 환경에 현재 그대로 실행하는 재시험 명령이 아니다.

```text
<venv-python> -B <run-directory>/mqtt-oracle.py prepare --private <private-directory>
<venv-python> -B <run-directory>/abrupt-worker.py --private <private-directory>
# 위 observer의 CAS 실패와 기존 Node의 자동 삭제를 확인한 뒤, 같은 장애만 계속 관찰했다.
<venv-python> -B <run-directory>/observe-recovery.py --private <private-directory>
<venv-python> -B <run-directory>/mqtt-oracle.py verify --private <private-directory>
```

첫 명령은 persistent subscription을 만든 뒤 연결을 끊고 retained marker와 오프라인 QoS 1 메시지 세 개의 성공 PUBACK을 확인했다. 두 번째 명령은 active drain 기록이 없는 follower node 3의 EC2에 `TerminateInstances`의 `SkipOsShutdown=true`를 요청했다. Fault 전에 drain·Pod delete를 호출하지 않았다. `terminated` 상태는 요청 후 19.405초에 확인했다. [종료 요청](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/fault-request.json)과 [종료 확인](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/terminated-fencing-proof.json)이 이 경계를 기록한다. Operator의 out-of-service 변경은 이 확인 뒤에만 허용했다([Kubernetes non-graceful shutdown 절차](https://kubernetes.io/docs/concepts/cluster-administration/node-shutdown/#non-graceful-node-shutdown)).

최초 observer의 out-of-service taint JSON Patch 요청은 HTTP 422로 거절돼 종료됐다. Patch에는 UID·resourceVersion 검사가 있었다. 기대한 resourceVersion은 `2217`이었고, Kubernetes audit에는 그 요청 0.300초 전에 다른 client의 성공 patch가 resourceVersion을 `3175`로 바꾼 기록이 있다. 오래된 resourceVersion 검사에서 거절됐을 가능성이 높지만, 응답에 실패한 연산이 없어 원인을 확정하지 않는다. 거절된 taint는 적용되지 않았다. 이후 AWS cloud controller의 기존 Node DELETE가 HTTP 200으로 완료됐다. 수동 Pod delete나 EC2 `Force`·EBS force-detach 요청은 사용하지 않았다. [실패 원문](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/abrupt-recovery-failure.json), [patch 거절 audit](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/node-patch-rejection-audit.json), [Node lifecycle audit](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/old-node-lifecycle-audit.json)을 따른다.

초기 관리 API probe는 첫 표본만 quorum·PUBACK 성공을 기록했고 이후 세 표본은 client timeout이었다. Client timeout 5초는 [Peer observability §3](../contracts/peer-observability-v1.md#3-한도와-시간)의 전체 query 예산과 같다. 이 표본 수집기는 관리 조회 뒤 MQTT publish를 수행하므로 timeout된 세 조회 뒤에는 MQTT probe를 보내지 못했다. 이를 MQTT 전송 실패나 서버 계약 위반의 증거로 쓰지 않는다. 후속 observer는 client timeout 10초로 읽기만 수행했고 69개 조회 모두 quorum available을 반환했다. 실패 뒤 관찰 공백이 있으므로 전체 구간의 연속 가용성은 입증하지 않는다. [초기 표본](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/continuity-progress.json)과 [observer 한계](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/observer-limitations.json)에 기록했다.

같은 장애의 후속 관찰에서는 종료한 EC2와 다른 같은-AZ EC2가 동일 PVC UID·PV·EBS를 연결했다. AWS attachment와 CSI VolumeAttachment도 새 worker를 가리켰고 나머지 두 broker Pod UID는 그대로였다. Broker 3개의 Ready와 quorum을 확인한 시점은 fault 요청 후 472.829초다. 이 값은 관찰 공백을 포함한 상한이며 정확한 RTO가 아니다. 마지막 명령은 복구한 broker에서 `sessionPresent=true`, 오프라인 QoS 1 payload hash 세 개와 retained hash를 재발행 없이 확인했다. 이 수신 구간의 QoS 1 중복 관찰값은 0개다. [물리 복구 후속 결과](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/physical-recovery-followup.json)와 [복구 후 MQTT 결과](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/mqtt-after-recovery.json)를 따른다. 살아 있는 quorum의 복제를 사용하므로 EBS만을 원본으로 한 독립 복구, 전체 voter 전원 손실, 자연 발생 WAL torn-tail, volume loss/reseed 또는 production SLO의 수용으로 확대하지 않는다.

복구 후속 관찰을 마친 뒤 전용 namespace·임시 node group 하나·Terraform 관리 자원 41개를 삭제했다. 직접 AWS 조회에서 최초 root 4개와 ASG 대체 root 1개, broker EBS 3개, 이번 전용 snapshot 1개, 전용 EKS·VPC·EIP·ENI·IAM·ASG·CloudWatch log group과 작업 image tag·digest의 부재를 확인했다. NAT는 `deleted`이고 최종 state의 관리 자원은 0개다. 역사적 보호 snapshot 3개와 공유 image는 유지했다. 정리 단계의 합성 Pod grace 0 제거는 관찰 종료 뒤에만 수행했으며 fault·복구 증거로 쓰지 않는다. [직접 정리 확인](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/direct-cleanup-verification.json)과 [최종 기록](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/final-summary.json)을 따른다. 인증·배포 입력·plan·state backup은 DPAPI `CurrentUser`로 보존했고 비밀 값이 없는 세 observer 소스는 검증 아티팩트로 남겼다. 제품 코드 변경이 없어 전체 local suite는 실행하지 않았고 이전 실패 상태도 바꾸지 않았다.

### EKS worker observer 보완 기준

검증 도구를 저장소에서 관리하도록 보완했다. 제품의 복구 정책·API 예산·drain gate는 바꾸지 않았다. 적용 범위는 전용 합성 fixture의 관찰과 operator fencing 판정이다. 로컬 검증 뒤 실제 EKS 재시험을 한 번 실행했다. 결과와 한계는 [생존 Pod 고정 observer의 EKS 재시험](#생존-pod-고정-observer의-eks-재시험-2026-10-15)을 따른다.

- 관리 API와 MQTT는 독립된 작업에서 각각 표본을 기록한다. API 조회 실패·지연 때문에 MQTT 표본을 생략하지 않는다. API client 제한 시간은 서버의 전체 5초 query 예산보다 긴 10초다.
- Fault mode는 plan에 지정한 생존 Pod의 UID·worker·provider instance를 확인한다. 대상 Pod·worker·instance와 같거나 Ready가 아니면 fault를 주입하지 않는다. 도구가 직접 그 Pod 하나에 관리 API·MQTT port-forward를 열고 두 port의 준비와 Pod identity를 재확인한다. 기존 port를 재사용하거나 service/LB로 연결하지 않는다. Tunnel이 중단되면 재연결·다른 Pod 전환 없이 실패로 남긴다. Fault 직전 identity·tunnel 생존을 다시 확인하고, 관찰과 MQTT 종료 후 tunnel도 종료한다.
- 각 표본은 시작·종료 시각과 결과를 기록한다. 오류가 있어도 표본을 남기고, 실패·관찰 공백을 성공으로 바꾸지 않는다. 인증 값과 오류의 비밀 원문은 기록하지 않는다.
- Operator Node 변경은 AWS에서 대상 instance가 `terminated`임을 확인한 뒤에만 허용한다. Patch 직전에도 종료 상태를 확인한다. 기존 Node UID·provider instance가 다르면 해당 Node를 변경하지 않는다. 기존 taint를 보존하고 `node.kubernetes.io/out-of-service:NoExecute`를 추가한다. 같은 key의 `NoSchedule`만으로는 이 조건을 충족하지 않는다.
- Node patch 명령이 실패하면 fresh GET으로 현재 Node를 읽는다. 삭제·UID 교체·resourceVersion 변경·같은 상태를 구분해 기록한다. 실패한 JSON Patch 연산이나 실제 적용 여부를 응답 없이 추정하지 않으며 patch를 자동 반복하지 않는다. 이후에는 읽기 전용으로 대체 Pod 복구를 관찰한다. 상태 변화가 없는 실패나 변경 후 taint 미확인은 전체 결과를 실패로 남긴다.
- Fencing 결과가 `out_of_service` 또는 `already_out_of_service`이면 기존 대상 Pod 하나를 Pod UID precondition과 `gracePeriodSeconds=0`으로 한 번 삭제한다. 삭제 직전에 EC2 `terminated`, 기존 Node UID·provider instance·out-of-service `NoExecute` taint, Pod UID·`spec.nodeName`을 다시 읽는다. 하나라도 다르거나 Node가 없으면 삭제하지 않고 이유를 기록한다. 삭제 명령이 실패하면 fresh GET으로 Pod를 읽고 자동 반복하지 않는다. 같은 UID가 남아 있는 결과(`failed_unexplained`·`delete_unconfirmed`)는 전체 결과를 실패로 남긴다. 이유: EC2 terminate에서는 cloud controller가 Node와 taint를 먼저 지울 수 있다. 그러면 Kubernetes가 taint를 근거로 하는 Pod 강제 삭제와 즉시 detach를 하지 못하고 기본 6분 force detach 대기에 들어간다고 본다 [INFERENCE]. 이 삭제는 PodGC가 NotReady·out-of-service Node에서 하는 강제 삭제를 Node가 사라지기 전에 앞당긴다. 기존 EC2가 `terminated`일 때만 허용하므로 이전 instance가 같은 EBS에 쓰는 경로는 없다. EBS force detach와 VolumeAttachment 삭제는 하지 않는다. 근거는 [EBS detach 대기 분석](../../.artifacts/analysis/ebs-detach-wait-analysis.md)이다.
- 복구 표본마다 대상 PV의 VolumeAttachment(`nodeName`·`attached`·삭제 시각·detach 오류 시각)와 EBS attachment(instance·state)를 함께 기록한다. 이 조회가 실패해도 오류 종류만 남기고 복구 관찰은 계속한다.
- 관찰 작업은 fault와 복구 판정 동안 계속 실행한다. 종료 시 진행 중인 probe가 끝나고 표본 파일이 닫혔는지 확인한다. 기록 실패나 작업 종료 실패가 있으면 전체 관찰을 통과로 표시하지 않는다.
- 로컬 회귀·실제 MQTT와 Kubernetes API smoke는 observer의 동작만 검증한다. 실제 EC2 장애·EBS 재연결 판정은 EKS 실행 기록만 근거로 쓴다. 연속 운영 가용성과 전체 장애 matrix는 계속 열린 gate다.
- 진단용 `nodes` 표본은 fault mode에서만 약 4초 간격으로 생존 Pod의 `GET /api/v1/nodes` 첫 페이지를 읽어 노드별 7개 필드만 `nodes-samples.jsonl`에 남긴다. 이 스트림은 관찰 전용 부하이며 PASS/FAIL 판정에 쓰지 않는다. 다만 표본 기록 실패와 작업 종료 실패는 전체 관찰을 실패로 만든다.
- Fault 요청 전에 `nodes` 첫 표본이 끝나기를 최대 20초 기다린다. 결과는 `monitor.diagnostics.nodes.baseline`에 남긴다. `available`은 첫 완료 표본이 성공했을 때만 참이다. 한도까지 표본이 없거나 실패해도 fault는 진행하고 PASS/FAIL 판정은 바뀌지 않는다. `faultRequestedAt`은 AWS 요청 직전에 기록한다.
- Cycle 1에서는 `monitor.diagnostics.nodes.baseline`이 fault 전에 available이었다. 세 node 모두 약 0.03초에 `OK`였다. [Cycle 1 결과](../../.artifacts/verification/eks-unknown-retest-20261015T135242Z/fault/cycle-1/eks-unknown-retest-20261015T135242Z-2b7e10/summary.json)를 따른다.

#### 구현과 로컬 검증 (2026-10-15)

진입점은 [main.py](../../tests/chaos/eks-worker-recovery/main.py)의 `main`이다. `monitor`는 읽기 전용 관리 조회와 합성 MQTT QoS 1 publish를 실행한다. `IndependentMonitor`는 [observer.py](../../tests/chaos/eks-worker-recovery/observer.py)에서 두 작업의 JSONL 표본·오류·종료를 관리한다. 예를 들어 API 응답이 6초 걸려도 MQTT 작업은 기다리지 않고 PUBACK을 기록한다. 표본 사이의 가용성은 추정하지 않는다.

`fault`는 명시한 `--execute`와 별도 plan이 있어야 실행한다. `preflight`에서 전용 namespace·account·cluster·Pod/Node/EC2·PVC/PV/EBS·같은-AZ 여분 worker·완료 snapshot을 확인한다. 이후 `observe_fault`가 단일 follower EC2를 종료하고 `fence_node`의 fresh 관찰 뒤 `release_fenced_pod`로 기존 대상 Pod를 한 번 삭제한다. 결과는 `pod-release-observation.json`과 summary의 `recovery.podRelease`에 남는다. 이후 대체 Pod·volume attachment를 읽고, `recovery-observations.json`의 각 표본에 Pod 삭제 시각, 기존 Node의 존재·taint·Ready, VolumeAttachment와 EBS attachment 상태를 기록한다. Persistent session·오프라인 payload·retained oracle은 별도 시나리오다. 명령의 PASS를 그 시나리오나 전체 DEP gate의 PASS로 쓰지 않는다. 준비와 실행 방법은 [개발 가이드](../how-to/development-guide.md#eks-worker-observer-실행)를 따른다.

로컬 회귀 파일은 [test_eks_worker_recovery.py](../../tests/test_eks_worker_recovery.py)다. `python -B -W error::ResourceWarning tests/test_eks_worker_recovery.py`로 48개 시험을 통과했다(2026-10-15, 기존 Pod 해제 시험 9개 추가). CI에도 별도 단계로 연결했다. `go run ./tools/verify local`은 이 Python 시험을 실행하지 않는다.

| 보완 기준 | 회귀 시험 이름 | 실제 표면 확인 |
| --- | --- | --- |
| API 지연 중 MQTT 독립 진행 | `MonitorTests.test_mqtt_progresses_while_management_is_blocked` | 실제 3-voter broker 앞에 6초 지연 proxy를 두고 CLI 실행. API 2개·MQTT 6개 성공. 첫 API 대기 구간 안에서 PUBACK 3개 완료 |
| 실패 표본 유지·비밀 오류 원문 제외·false 결과 판정 | `MonitorTests.test_timeout_is_recorded_without_stopping_later_probes_or_leaking_error_text`, `test_negative_quorum_or_puback_cannot_pass` | 11초 지연에서 client 10초 timeout 1개를 보존. MQTT 6개는 성공. CLI는 예상대로 exit 1·FAIL |
| 기록·종료 실패는 PASS 금지 | `MonitorTests.test_journal_failure_cannot_be_reported_as_pass`, `test_shutdown_timeout_remains_a_failure_after_the_worker_finishes` | 회귀에서 실제 파일 닫기와 대기 중인 thread로 확인. CLI smoke 두 실행의 worker 종료·journal 닫기도 확인 |
| 종료·UID·provider identity 경계 | `FenceTests.test_nonterminated_instance_never_allows_node_mutation`, `test_state_change_before_patch_blocks_mutation`, `test_absent_or_replaced_node_is_never_mutated`, `test_provider_change_with_same_uid_does_not_fence_another_instance` | 실제 Kind API의 합성 Node를 사용. `running` 입력에서는 patch 0회 |
| CAS·삭제·교체·응답 유실 뒤 새 조회, 재시도 금지 | `FenceTests.test_stale_resource_version_is_refreshed_without_retry_or_cause_claim`, `test_delete_during_patch_resumes_read_only_observation`, `test_uid_replacement_during_patch_does_not_mutate_the_replacement`, `test_unchanged_rejection_is_not_classified_as_a_known_cas_failure`, `test_response_loss_does_not_claim_our_mutation_was_applied` | 실제 API에서 RV 갱신·UID 교체·Node 삭제를 patch 직전에 수행. 각각 patch 1회만 시도하고 현재 상태를 기록. kubectl 1ms deadline 실패도 fresh GET 뒤 실패 원인·적용 여부를 미확정으로 유지 |
| 기존 taint 보존·NoExecute 확인 | `FenceTests.test_patch_preserves_taints_and_requires_both_identity_preconditions`, `test_already_fenced_node_is_not_patched_again`, `test_noschedule_alone_does_not_establish_the_required_noexecute_fencing` | 실제 API의 정상 추가와 기존 NoSchedule만 있는 Node에서 확인. 기존 maintenance taint와 NoSchedule을 보존 |
| 기존 Pod 해제: 종료·fencing·Node identity 경계 | `PodReleaseTests.test_nonterminated_instance_never_allows_pod_deletion`, `test_unfenced_outcomes_never_delete`, `test_node_must_still_exist_with_the_same_identity_and_taint` | Kind 1.31.2 API에서 taint 제거 Node와 삭제된 Node에 대해 삭제 0회 |
| 기존 Pod 해제: UID 조건 1회 삭제·대체 Pod 보존·재시도 금지 | `PodReleaseTests.test_fenced_terminated_node_releases_the_old_pod_once_with_its_uid`, `test_replacement_or_moved_pod_is_never_deleted`, `test_replacement_created_during_delete_is_kept_and_attempt_is_not_retried`, `test_rejected_delete_with_old_pod_remaining_is_unexplained_without_retry`, `test_accepted_delete_that_leaves_the_old_pod_is_unconfirmed` | Kind API에서 kubelet 없는 Node의 Terminating Pod를 `delete --raw` grace 0으로 제거. 다른 UID 조건은 API가 거절하고 Pod를 유지. 같은 이름의 새 UID Pod는 삭제하지 않음 |
| 해제 실패의 판정·attachment 표본 | `DiagnosticBaselineWiringTests.test_old_pod_that_survives_the_release_attempt_fails_the_run` | Kind API의 VolumeAttachment 2개 중 대상 PV 1개만 기록. EBS 조회 실패는 오류 종류만 남김. EBS 응답은 통제된 입력 |

기존 Pod 해제의 실제 API 확인은 [Kind 기록](../../.artifacts/verification/pod-release-local-20261015T1625Z/kind-release-proof.json)을 따른다. Kind에는 attach-detach controller가 EBS를 붙이는 경로가 없다. 따라서 이 기록은 삭제 조건만 확인하며 detach 단축의 증거가 아니다. Kind cluster는 시험 뒤 삭제했다.

기록은 [최종 결과](../../.artifacts/verification/worker-observer-local-20261015T071513Z/final-summary.json), [6초 지연](../../.artifacts/verification/worker-observer-local-20261015T071513Z/delayed-api-proof.json), [10초 deadline 실패 보존](../../.artifacts/verification/worker-observer-local-20261015T071513Z/deadline-api-proof.json), [Kubernetes 7개 경계](../../.artifacts/verification/worker-observer-local-20261015T071513Z/kubernetes-fencing-proof.json)를 따른다. Python 3.14·Paho MQTT 2.1.0, Docker의 실제 broker 3개와 Kind Kubernetes 1.31.2를 사용했다. Broker는 기존 `9c96670` 합성 image를 사용했다. 로컬 broker의 준비 상태는 시험용 GET으로 확인했다. 이를 EKS startup probe 증거로 쓰지 않는다.

Kubernetes 시험의 EC2 상태는 통제된 입력이다. AWS 종료·EBS 재연결 증거가 아니다. 사용한 broker container 3개·network·Kind cluster·새 private directory는 삭제했다. AWS 자원은 생성하지 않았다. 기존 암호화 입력과 공유 image는 보존했다. 제품 Go 코드·의존 흐름은 바꾸지 않아 전체 local suite는 실행하지 않았다. 원격 CI도 이 작업에서는 실행하지 않았다. Workflow YAML 구문 확인과 문서 링크 검사는 통과했다. 과거 local suite 실패·최초 EKS observer 실패·`CLU-14 FAILED/UNRESOLVED`는 유지한다.

이 절의 로컬 통과는 EKS gate를 닫지 않았다. 이후 실제 EKS 재시험은 아래 절에 기록한다. 코드 지도에는 검증 도구의 진입점만 추가했으며 제품 모듈 설계는 변경하지 않았다.

#### 생존 Pod의 관찰 경로 고정 (2026-10-15)

기존 fault observer는 외부에서 연 두 port의 대상 identity를 확인하지 않았다. Cluster summary만으로는 요청을 받은 Pod를 특정할 수 없다. 대상 worker로 잘못 연결하면 fault와 함께 관찰도 끊길 위험이 있다.

`main`은 fault plan의 `observerPod`·`observerPodUID`·`observerWorkerName`·`observerInstanceId`로 `SurvivorTunnel`을 시작한다. 이 class는 실제 Pod·Node·provider와 EC2 running 상태를 확인한 뒤 kubectl을 직접 실행한다. `23880:8080`과 `23883:1883`을 지정 Pod 하나에 연결하고 두 port의 준비 뒤 identity를 다시 읽는다. Fault 직전에도 identity·process를 확인한다. `observer-endpoint.json`과 summary에 확인한 identity를 기록한다. 각 관찰 작업은 process 종료를 실패로 기록한다. 관찰과 MQTT 종료 뒤 소유한 tunnel·출력 thread를 닫는다. `monitor` mode의 외부 tunnel은 이 검증 범위가 아니다.

기준은 위 observer 계약이며 제품의 복구 정책이나 API 형식은 바꾸지 않았다. 변경 파일은 `main.py`의 tunnel lifecycle, `tests/test_eks_worker_recovery.py`의 안전 경계, 실행 절차와 코드 지도다. 예를 들어 plan의 생존 Pod UID가 연결 준비 중 바뀌면 tunnel을 닫고 fault를 주입하지 않는다.

| 기준 | 시험·확인 방법 | 결과 |
| --- | --- | --- |
| 대상 Pod·worker·instance 관찰 거절 | `SurvivorTests.test_target_pod_worker_or_instance_cannot_be_used_as_survivor` | PASS. 세 identity의 충돌을 각각 거절 |
| 교체·NotReady·삭제 중·종료 instance 거절 | `SurvivorTests.test_changed_identity_notready_or_terminating_survivor_is_rejected` | PASS. Pod UID·worker·provider 불일치와 readiness·deletion·running 경계 확인 |
| Fault CLI의 거절 기록 | `SurvivorTests.test_fault_cli_persists_refusal_before_mqtt_or_aws_mutation` | PASS. exit 1·summary FAIL, MQTT 시작·AWS mutation 없음 |
| 실제 두 port의 정확한 Pod 연결 | Kind 1.31.2·kubectl 1.34.1, 실제 Python HTTP/TCP Pod와 `SurvivorTunnel.start` | PASS. 두 port에서 같은 합성 Pod 표식을 확인하고 UID 기록 |
| 기존 port를 재사용하지 않음 | Host의 23880을 점유한 뒤 실제 kubectl 실행 | PASS. 준비 실패 뒤 소유 process·thread 종료 |
| 연결 준비 중 UID 교체 | 두 번째 identity GET 전에 실제 Pod 삭제·재생성 | PASS. 다른 UID를 거절하고 tunnel 종료 |
| Process 종료 뒤 자동 재연결 없음 | 실제 tunnel process 종료 뒤 `check` | PASS. 오류 반환, 같은 종료 process 유지 |
| 유휴 tunnel의 Pod 삭제 감지 한계 | 실제 Pod 삭제 뒤 kubectl 종료를 25초 기다림 | **예상한 process 종료 대기 실패를 보존**. 유휴 process가 남았다. Process 생존은 Pod 생존의 증거가 아니며 즉시 Pod 손실 감지를 보장하지 않는다. Fault 직전 fresh identity 검사는 별도로 유지한다. |

`python -B tests/test_eks_worker_recovery.py -v`로 기존 시험을 포함한 20개가 통과했다. 검증 대상은 `ef6e561` 이후 미커밋 observer·시험·문서 변경이며 이전 staging/TODO 변경도 작업 트리에 있었다. 실제 로컬 실행은 [runtime 기록](../../.artifacts/verification/survivor-tunnel-20261015T085807Z/runtime-proof.json)과 [최종 기록](../../.artifacts/verification/survivor-tunnel-20261015T085807Z/final-summary.json)을 따른다. EC2 running은 통제된 입력이고 endpoint는 합성 HTTP/TCP다. 이 로컬 실행은 새 MQTT protocol·NaruMQ·AWS/EKS 장애 수용을 통과했다고 주장하지 않는다. 임시 Kind cluster·kubeconfig와 소유 tunnel process·thread는 제거했다. AWS 자원은 생성하지 않았다. 전체 Go local gate와 원격 CI는 실행하지 않았다. 과거 실패는 유지한다.

#### 생존 Pod 고정 observer의 EKS 재시험 (2026-10-15)

결과는 **단일 follower EC2 비정상 종료에 한정한 통과**다. 상태는 `PASS_SINGLE_FOLLOWER_ABRUPT_EC2_OBSERVER_AND_DATA_RECOVERY_LIMITED`다. [결과 기록](../../.artifacts/verification/eks-observer-acceptance-20261015T091838Z-617088/acceptance-result.json)과 [최종 기록](../../.artifacts/verification/eks-observer-acceptance-20261015T091838Z-617088/final-summary.json)을 따른다. 기존 `CLU-14 FAILED/UNRESOLVED`, 과거 최초 observer 실패, 전체 EC2·AZ 장애 matrix와 production 수용은 바꾸지 않는다.

대상 HEAD는 `ef6e561`이고 observer·시험·문서의 미커밋 변경을 포함했다. 제품 코드는 `9c96670`과 같음을 확인했다. 이전 합성 ARM64 broker image를 작업 전용 tag로 다시 게시했고 공유 image는 바꾸지 않았다. 별도 local state·새 VPC의 EKS 1.37, 서로 다른 AZ의 `m7g.large` worker 3대와 새 gp3 PVC 3개를 사용했다. Cluster ID는 run ID다. Startup은 수동 readiness GET 없이 Kubernetes probe로 통과했다. 도구는 Python 3.14, Paho MQTT 2.1.0, kubectl 1.37.1이다. kubectl은 공식 SHA-256과 일치했다.

준비 단계에서 leader는 node 3이었다. 대상은 follower `narumq-1`(node 2, AZ b), 관찰 경로는 다른 worker·instance의 `narumq-0`(AZ a)이다. 대상과 같은 AZ의 Ready worker 한 대를 추가하고, run 전용 snapshot이 `completed`임을 확인했다. 대상 data volume은 암호화돼 있고 `DeleteOnTermination=false`였다. 서로 다른 두 생존 Pod에서 pre-fault drain 기록 조회가 같은 404를 반환했다([조회 기록](../../.artifacts/verification/eks-observer-acceptance-20261015T091838Z-617088/drain-record-read-path.json)). 장애 전 persistent session, retained marker 1개와 오프라인 QoS 1 메시지 3개의 PUBACK 4개를 확인했다. 모두 Reason Code 0이었다.

실행 순서는 다음과 같다. `<private-directory>`와 plan에 필요한 값은 [개발 가이드](../how-to/development-guide.md#eks-worker-observer-실행)를 따른다.

```text
<venv-python> -B tests/chaos/eks-worker-recovery/payload_oracle.py prepare --private <private-directory> --run-id <run-id> --output <result-directory> --target-port <대상 Pod tunnel port> --publisher-port <생존 Pod tunnel port>
<venv-python> -B tests/chaos/eks-worker-recovery/main.py fault --private <private-directory> --output <run-directory>/<run-id> --plan <fault-plan.json> --execute
<venv-python> -B tests/chaos/eks-worker-recovery/payload_oracle.py verify --private <private-directory> --run-id <run-id> --output <result-directory> --target-port <복구된 대상 Pod tunnel port>
```

`prepare`의 tunnel은 `fault` 시작 전에 닫아야 한다. `fault`가 `23880`·`23883`을 직접 소유하기 때문이다.

| 관찰 항목 | 실제 결과 |
| --- | --- |
| Fault 요청·종료 확인·fencing | `SkipOsShutdown=true` 요청 후 17.1초에 `terminated`, 약 19.0초에 out-of-service `NoExecute` taint 적용. Patch 실패와 CAS 거절은 없었다. 기존 Node는 `NodeNotReady` 뒤 cloud controller가 2초 안에 삭제했다. 수동 Pod 삭제와 EBS force detach는 사용하지 않았다. |
| API·MQTT 관찰 | 도구가 소유한 `narumq-0` tunnel에서 관리 API 203개·MQTT 238개 표본이 모두 성공했다. Fault 요청 뒤 표본은 각각 200개·235개다. 표본 시작 시각 기준 최대 간격은 관리 API 7.065초, MQTT 2.056초다. 관리 API 표본 13개(11:39:02Z~11:40:32Z)는 각각 약 5.03초 걸렸고 모두 `quorumAvailable=true`를 반환했다. 최대 소요는 5.061초다. 이전 표본 종료부터 다음 표본 시작까지의 idle gap은 관리 API 최대 2.016초다. 두 값은 다르다. 종료 때 worker와 journal을 닫았다. |
| 대체 Pod와 volume | 대체 Pod는 fault 후 95.6초 관측 표본에 나타났다. 같은 AZ의 다른 worker에서 같은 PVC·PV·EBS·VolumeAttachment를 사용했다. 나머지 두 broker Pod UID는 바뀌지 않았다. Broker 3개 Ready와 quorum은 fault 후 471.06초에 관찰했다. |
| 데이터 oracle | 복구한 broker에서 `sessionPresent=true`, 오프라인 QoS 1 payload hash 3개를 발행 없이 받았다. 중복은 0개, retained hash는 같았다. |

471.06초는 표본으로 관찰한 상한이며 정확한 RTO나 production SLO가 아니다. 관리 API 표본은 장애 구간에서 약 7초 간격까지 벌어졌다. 이전 후속 관찰의 상한 472.829초와 비슷하다. 대체 Pod가 생성된 뒤 `FailedAttachVolume`(`waiting for detach`)이 11:40:35Z부터 11:46:40Z까지 약 365초 이어졌다. 이 대기가 복구 시간의 대부분이다. 이 시험은 제품 코드나 Kubernetes 설정을 바꿔 그 시간을 줄이지 않았다.

세 EKS 실행의 event를 비교하면 detach 대기는 모두 6분 3~5초였다. Out-of-service taint 적용이 실패한 실행과 성공한 두 실행의 대기가 같았다. Node 삭제 시각이 기록된 두 실행에서 cloud controller는 Node가 NotReady가 된 뒤 1~2초 안에 Node를 삭제했고, 대체 Pod는 Node 삭제 45~48초 뒤 생성됐다. Kubernetes `v1.37.0` source에서 PodGC는 NotReady이면서 taint가 있는 Node의 종료 중 Pod만 강제 삭제한다(20초 주기). Attach-detach controller는 Pod object가 삭제되거나 종료 상태가 보고돼야 volume을 detach 대상으로 본다. 그 뒤 Node object에 taint가 있으면 바로 detach하고, 그렇지 않으면 `maxWaitForUnmountDuration`(기본 6분) 뒤 force detach한다. [INFERENCE] EC2 terminate에서는 Node 삭제와 함께 taint도 사라진다. 그래서 taint가 detach를 앞당기지 못하고 6분 기본 대기로 복구된다. EKS patch 차이, controller-manager 로그와 실제 설정값은 확인하지 않았다. 이 분석에 따라 관찰 도구는 taint 확인 직후 기존 Pod를 UID 조건으로 삭제하도록 바뀌었다([EKS worker observer 보완 기준](#eks-worker-observer-보완-기준)). 효과는 [기존 Pod 해제의 EKS 재시험](#기존-pod-해제의-eks-재시험-2026-10-15) 1회에서 확인했다. 근거는 [EBS detach 대기 분석](../../.artifacts/analysis/ebs-detach-wait-analysis.md)에 있다.

함께 보존할 결과와 한계는 다음과 같다.

- 첫 `prepare` 시도는 Windows 예약 TCP 범위(`28814~29013`)에서 `28883` bind에 실패했다. 이 실패는 장애 주입 전이었고 해당 시도는 AWS를 변경하지 않았다. 기존 observer가 대상 Pod의 `28880` tunnel을 가정하던 drain 기록 조회도 같은 범위와 충돌했다. 이를 소유한 생존 Pod tunnel의 cluster-wide 조회로 바꾸고 `DrainRecordTests` 2개를 추가했다. 이후 회귀는 22개가 통과했다. 이 변경은 fault 실행 전에 적용했다.
- 이번 장애는 follower 하나이며 생존 voter 둘이 quorum을 유지했다. EBS 데이터도 quorum 복제와 함께 쓰이므로 EBS만으로 독립 복구했다는 증거가 아니다. 전체 voter 전원 손실, volume loss/reseed, 단일 AZ 손실, 자연 발생 WAL torn-tail, production 최대 부하와 SLO는 시험하지 않았다.
- 표본이 모두 성공해도 표본 사이의 연속 가용성은 입증하지 않는다. 관리 API의 7초 간격 동안과 모든 표본 사이의 상태는 알 수 없다. `kubectl port-forward` process 생존은 Pod 생존의 증거가 아니다. 이번 결과는 한 Pod의 tunnel에서 관찰한 표본이다.
- 앞선 EKS 실행에서는 `nodesByState`가 `ready 1·unknown 2`에서 `ready 2·unknown 1`로 바뀌었다. 생존 node가 `unknown`이 된 원인은 당시 `[INFERENCE]`였다. 그 실행의 로컬 Docker 재현에서는 target 하나만 응답하지 않게 했고 leader의 `unknown`은 나오지 않았다. 추가 S1–S4 실험 8회에서도 재현하지 못했다. 이는 이후 cycle 1 및 DNS 실험과 별개다. [이전 실행 기록](../../.artifacts/verification/eks-observer-acceptance-20261015T091838Z-617088/unknown-repro-local-load.json).
- 2026-10-15 EKS cycle 1에서는 healthy leader node 2가 quorum을 유지하면서 `unknown`으로 집계됐다. `GET /api/v1/nodes`에서 node 2와 target node 3이 14:18:17.677Z, 14:18:26.711Z, 14:18:35.747Z 시작 표본에서 `UNREACHABLE/node_query_timeout`(5.02–5.03초)이었다. Node 2는 14:18:44.794Z 표본에서 `OK`였다. 비정상으로 관측된 구간은 표본 경계로 23.1–36.2초다. `GET /api/v1/cluster`는 14:18:15.7–14:18:36.9에 `ready 1·unknown 2`였고 이후 `ready 2·unknown 1`이 됐다. Leader 2와 quorum은 203개 표본 전체에서 유지됐다. MQTT QoS 1 PUBACK은 성공했다(9–23ms, 한 건 0.245초). Node 3은 대체 Pod Ready까지 비정상이었다(+473.383초). Oracle 검증은 PASS였다. Node 1·2 broker log에는 이 창의 오류가 없었다. Node 2의 첫 `dynamic peer backpressure` 로그는 14:21:15Z였다. 종료 전 target log는 수집하지 않았다. 두 CoreDNS replica는 종료 worker에 배치돼 있었다. 대체 container는 14:18:40Z에 시작했다. 사후 dump에 남은 Deployment `Available` condition의 전환 시각은 14:18:41Z다. 이 값은 대체 Pod Ready나 DNS 복구 시각이 아니다. DNS probe Pod의 정상 상태 조회 876회는 실패 없이 완료됐다(중앙값 0.002초, 최대 0.012초). Fault 중 DNS 관측은 없었다. EndpointSlice의 구 endpoint 제거 시각과 대체 Pod Ready 시각은 복원할 수 없다. [EKS cycle 1 표본](../../.artifacts/verification/eks-unknown-retest-20261015T135242Z/fault/cycle-1/eks-unknown-retest-20261015T135242Z-2b7e10/nodes-samples.jsonl), [CoreDNS timeline](../../.artifacts/verification/eks-unknown-retest-20261015T135242Z/fault/cycle-1/post-fault/cycle1-coredns-timeline.json). EKS fixture는 시험 뒤 삭제했다. Terraform 41개를 destroy했고 직접 조회 32개 항목이 모두 통과했다. 두 번째 fault cycle은 준비만 하고 실행하지 않았다. [직접 정리 확인](../../.artifacts/verification/eks-unknown-retest-20261015T135242Z/direct-cleanup-verification.json).
- 로컬 Docker DNS 실험은 8회 수행했다. D1 resolver pause와 UDP/TCP DROP에서 건강한 node 2와 node 3은 먼저 약 5초 `node_query_timeout`, 이어 빠른 `node_unreachable`이 됐다. `unknown=2`, leader 2, quorum은 유지됐고 MQTT QoS 1은 475/475 성공했다. D2는 DNS가 정상인 채 node 3을 pause했으며 node 3만 비정상이었다. D3은 DNS를 약 25초 차단한 동안 node 3도 pause했다. DNS 복구 후 node 2는 `OK`가 됐고 node 3은 peer 복구까지 비정상이었다. Broker 로그는 각 run에서 0줄이었다. 코드상 `QueryObservability`는 hostname으로 매 RPC dial하고 app-level address cache는 없다. Image는 `CGO_ENABLED=0`이며 lookup deadline은 `node_query_timeout`으로 매핑된다. [로컬 DNS 실험](../../.artifacts/analysis/dns-stall/experiment.json), [코드 분석](../../.artifacts/analysis/unknown-leader-timeout-analysis.md).
- 당시의 해석은 다음과 같았다. [INFERENCE] EKS worker 종료로 CoreDNS 두 replica가 함께 사라져 14:18:13.043Z부터 대체 container가 시작된 14:18:40Z까지 약 27초간 새 DNS 조회가 지연되고, 매번 fresh dial하는 healthy peer 관측 RPC도 timeout됐을 가능성이 있다. 그 실행은 fault 중 DNS를 측정하지 않았다. Nodes 경로에는 ReadIndex barrier가 없고 dead peer 송신 queue는 block하지 않는다. A(barrier), B(rejection), C(deadline race)는 자료의 지지를 받지 않는다. D(dead-peer queue)는 이 구간의 원인으로 기각됐다. 2026-10-16 EKS 재현에서 DNS를 함께 측정해 원인을 확인했다. 사용자는 이 동작을 계약 동작으로 수용했다. [CoreDNS 배치와 healthy node `unknown`의 EKS 재현](#coredns-배치와-healthy-node-unknown의-eks-재현-2026-10-16)을 따른다.
- EKS 실행은 이전 artifact의 oracle 사본을 사용했다. 저장소의 [payload_oracle.py](../../tests/chaos/eks-worker-recovery/payload_oracle.py)는 같은 검증 논리에 `--run-id`·`--output` 인자를 붙여 옮긴 것이다. 실제 로컬 Docker 3-voter broker에서 `prepare`와 `verify`를 모두 통과했다([로컬 검증](../../.artifacts/verification/eks-observer-acceptance-20261015T091838Z-617088/tracked-oracle-local-proof.json)). 이 로컬 검증은 EC2 장애나 broker 재시작을 포함하지 않고 broker가 `joining` 요약 상태일 때 실행했다. 따라서 이 파일 자체를 EKS 장애 복구의 증거로 쓰지 않는다.

시험 뒤 합성 StatefulSet·PVC·namespace, EBS 3개, run 전용 snapshot 1개와 추가 node group을 삭제했다. 검토한 delete-only plan으로 Terraform 관리 자원 41개를 삭제했고 state의 관리 자원은 0개다. 직접 AWS 조회에서 EKS·VPC·ENI·security group·load balancer·EIP·IAM 역할/정책·ASG·CloudWatch log group의 부재와 NAT `deleted`, worker instance 5개 `terminated`를 확인했다. 작업 image tag·digest도 삭제했다. 보호 snapshot 3개와 공유 image는 유지했다. 합성 입력·plan·state는 Windows DPAPI `CurrentUser`로 암호화해 보존했다. 정리 단계의 StatefulSet orphan 삭제와 Pod grace 0 제거는 관찰 뒤 작업이며 복구 증거가 아니다. 로컬 oracle 검증을 위해 Docker Desktop을 시작했다. 전체 AWS 비용이 0이라고 주장하지 않는다. [직접 정리 확인](../../.artifacts/verification/eks-observer-acceptance-20261015T091838Z-617088/direct-cleanup-verification.json)을 따른다. 전체 Go local gate와 원격 CI는 실행하지 않았다.

#### 기존 Pod 해제의 EKS 재시험 (2026-10-15)

결과는 **1회 실행에서 6분 detach 대기가 사라졌다**는 것이다. 대체 Pod의 `FailedAttachVolume`은 0건이었고, 기존 Pod 삭제 요청 약 5.5초 뒤 같은 EBS가 여분 worker에 attach됐다. Broker 3개 Ready 관찰은 fault 요청 뒤 193.995초다. 이전 세 실행의 471~473초보다 짧다. Tool 결과와 데이터 oracle은 모두 PASS다. [최종 기록](../../.artifacts/verification/eks-detach-release-20261015T162550Z-23b31b/final-summary.json)과 [tool summary](../../.artifacts/verification/eks-detach-release-20261015T162550Z-23b31b/fault/eks-detach-release-20261015T162550Z-23b31b/summary.json)를 따른다.

Fixture는 이전과 같은 Terraform root(EKS `v1.37.0-eks-b9510e3`, 새 VPC, `m7g.large` worker 3대)와 같은 broker image digest `sha256:140a99b2…`를 사용했다. Image는 작업 전용 tag로 다시 게시했다. Leader는 `narumq-1`(node 2, AZ a)이었다. 대상은 follower `narumq-2`(node 3, AZ b), 관찰 경로는 `narumq-0`(AZ c)이다. 대상과 같은 AZ b에 여분 worker 한 대를 추가했다. 두 CoreDNS replica는 대상이 아닌 worker에 있었다. EBS CSI controller replica 하나가 대상 worker에 있었고 attacher·provisioner lease를 갖고 있었다. Attach 시간에 controller 장애 조치가 섞이지 않도록 대상 worker를 잠시 cordon한 뒤 그 replica를 삭제했다. 새 replica가 Ready가 되고 lease가 대상이 아닌 replica로 옮겨진 뒤 uncordon했다([배치 기록](../../.artifacts/verification/eks-detach-release-20261015T162550Z-23b31b/pre-fault-placement.json)). 이 조정은 fault 전 작업이다.

| 시각(UTC) | 관찰 |
| --- | --- |
| 16:48:23.762 | Fault 요청(`SkipOsShutdown`) |
| 16:48:43.548 | EC2 `terminated` 확인 |
| 16:48:47.014 | Out-of-service taint 적용(`out_of_service`) |
| 16:48:51 | `TaintManagerEviction` event |
| 16:48:54.458 | 기존 Pod UID 조건 grace 0 삭제 요청. 2.1초 뒤 fresh GET에서 새 UID Pod 확인(`deleted`) |
| 16:48:57 | 대체 Pod가 여분 worker에 scheduling |
| 16:48:58.899 | 첫 복구 표본. 기존 Node는 남아 있고 taint가 있었다. VolumeAttachment는 여분 worker의 것 1개뿐이고 `attached=true`였다. EBS attach 시각은 16:48:59 |
| 16:49:00 | `SuccessfulAttachVolume` event |
| 16:49:02~03 | Container 시작 |
| 16:49:21.372 | 기존 Node가 없는 첫 표본 |
| 16:49:13~16:51:23 | Startup probe 503 14회 |
| 16:51:31.344 | 관리 API 표본에서 `ready 3` 확인(+187.6초) |

Fault 요청 뒤 관리 API 85개·MQTT 99개 표본이 모두 성공했다. 표본 시작 간격은 최대 7.042초·2.054초다. 데이터 oracle은 복구한 broker에서 `sessionPresent=true`, 오프라인 QoS 1 payload 3개와 retained를 재발행 없이 확인했다. 첫 표본 전에 기존 VolumeAttachment가 사라졌으므로 정확한 제거 시각은 16:48:54.458과 16:48:58.899 사이로만 안다. 이 결과는 가설과 맞는다. 다만 한 번의 실행이며 controller-manager 로그는 읽지 않았다.

새로 드러난 대기는 attach 뒤 약 130초의 startup probe 실패다. 대체 node 3은 16:50:02Z부터 peer 1·2를 `connected=false`, `outbound queue full`로 기록했다. Leader `narumq-1`은 16:51:14Z부터 peer 3을 `connected=true`, `outbound queue full`로, 16:51:18Z부터 `peer write failed`로 기록했다. 16:51:23.161Z에 `connected=false`가 됐고, node 3은 16:51:23.172Z에 release profile 동기화를 기록했다. [INFERENCE] Leader가 종료된 이전 Pod 주소로 맺은 peer 연결을 쓰기 실패가 날 때까지 유지했고, 그동안 대체 node가 leader 메시지를 받지 못했다. Socket 상태, node 3의 송신이 연결되지 않은 이유와 transport의 write deadline은 확인하지 않았다. 이전 실행에서는 attach가 6분 늦어 이 대기가 가려졌을 수 있다 [INFERENCE]. 이 항목은 TODO §1.4에서 추적한다. [Broker log](../../.artifacts/verification/eks-detach-release-20261015T162550Z-23b31b/fault/post-fault/logs/narumq-1.log)를 따른다.

후속 로컬 재현에서 같은 순서를 확인했다. 연결 방향상 leader만 node 3을 dial한다. Dynamic peer stream은 Go 기본 keepalive만 쓰고 감지 시간을 제한하는 설정이 없었다. 이전 IP를 응답 없이 버리자 leader는 응용 송신 대기열이 차고 write deadline이 실패할 때까지 연결을 유지했다. [ADR-0059](../decisions/ADR-0059-dynamic-peer-tcp-liveness.md)의 TCP 생존 확인을 적용하자 leader의 연결 종료가 fault 뒤 49.5초에서 12.7초 이내로 당겨졌다. 대체 node 준비 시간은 28.74초에서 시작 직후로 줄었다([Cluster §11.34](cluster-session-and-delivery.md#1134-dynamic-peer-tcp-생존-확인-수용-매핑)). 2026-10-16 EKS 재측정 결과는 [Cluster §11.34 EKS 무응답 종료 재측정](cluster-session-and-delivery.md#eks-무응답-종료-재측정-2026-10-16)에 있다.

시험 뒤 합성 namespace와 broker EBS 3개, 여분 node group, run 전용 snapshot, 작업 image tag·digest를 삭제했다. 검토한 delete-only plan으로 Terraform 관리 자원 41개를 삭제했다. 직접 AWS 조회 27개가 모두 통과했다. 보호 snapshot 3개와 공유 image tag `observability-20261014`는 유지했다. 증거 파일에서 관리 token과 MQTT 비밀번호는 발견되지 않았다. 합성 입력·plan·state·kubeconfig는 Windows DPAPI `CurrentUser`로 암호화해 보존했다. [직접 정리 확인](../../.artifacts/verification/eks-detach-release-20261015T162550Z-23b31b/direct-cleanup-verification.json)을 따른다. 제품 코드는 바꾸지 않아 전체 Go local gate와 원격 CI는 실행하지 않았다. `CLU-14 FAILED/UNRESOLVED`와 과거 실패 기록은 유지한다.

#### CoreDNS 배치와 healthy node `unknown`의 EKS 재현 (2026-10-16)

결과는 **원인 확인**이다. Worker 하나를 잃으면 그 worker의 CoreDNS endpoint로 간 DNS 조회가 timeout된다. 관측 RPC는 query마다 peer의 DNS 이름으로 새로 연결한다(`internal/adapters/tcp/peer_observability_transport.go`). 그래서 DNS 조회가 5초 query 예산을 다 쓰면 healthy node도 `UNREACHABLE/node_query_timeout`이 된다. CoreDNS를 서로 다른 worker·AZ에 두는 배포 전제는 이 구간을 줄인다. 그러나 없애지는 못한다. 2026-10-16 사용자는 이 동작을 계약 동작으로 수용했다. 실패하거나 timeout된 query는 [peer observability §3](../contracts/peer-observability-v1.md#3-한도와-시간)과 [Management API v1](../contracts/management-api-v1.md)에 따라 `UNREACHABLE`·`unknown`으로 보고한다.

같은 fixture에서 두 번 실행했다. 실행 방법·image·socket 결과는 [Cluster §11.34 EKS 무응답 종료 재측정](cluster-session-and-delivery.md#eks-무응답-종료-재측정-2026-10-16)과 같다. 관찰 도구가 연결한 node 1의 worker에 DNS probe Pod를 두었다. 이 Pod는 node 2·3의 headless DNS 이름을 UDP로 0.5초마다(제한 3초), `getaddrinfo`로 1초마다 조회했다([probe](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/scripts/dns_probe.py)). CoreDNS Pod와 EndpointSlice 상태는 1초마다 기록했다.

시각은 fault 요청 뒤의 초다.

| 관찰 | 재현 실행 | 전제 실행 |
| --- | --- | --- |
| CoreDNS 배치 | 두 replica 모두 대상 worker | Terraform 전제 적용. 대상 worker(AZ c)와 leader worker(AZ b)에 하나씩 |
| Observer worker의 UDP DNS timeout | 600회 중 170회. 6.22~48.22초. 10~45초에는 모두 실패 | 508회 중 54회. 5.39~27.89초 |
| 죽은 CoreDNS endpoint가 routing에서 빠짐 | 새 replica 준비 42.7초·45.7초 | 23.8초에 not ready |
| Healthy leader node 2의 node 표본 | 3.2~39.4초에 시작한 5개 표본 모두 `node_query_timeout`. 48.5초에 `OK` | 12개 표본 중 22.3초에 시작한 1개만 `node_query_timeout` |
| Cluster 요약 `unknown=2` | 3.3~45.6초 | 2.3초·9.3초·16.4초 표본. 23.4초부터 `unknown=1` |
| Leader·quorum·MQTT·oracle | leader 2 유지, 표본 모두 성공, PASS | leader 2 유지, 표본 모두 성공, PASS |

재현 실행은 2026-10-15 cycle 1과 같은 모습이다. DNS가 모두 실패한 구간과 node 2의 timeout 구간이 겹친다. DNS가 돌아온 직후 node 2도 `OK`가 됐다. 전제 실행에서는 살아 있는 replica가 응답했다. 그러나 죽은 replica의 endpoint가 23.8초까지 routing에 남아 일부 조회가 timeout됐다. 그동안 cluster 요약과 node 표본이 간헐적으로 healthy node를 미확인으로 보였다.

운영자는 worker 하나를 잃은 뒤 수십 초 동안 healthy node가 `unknown`으로 보일 수 있다고 예상해야 한다. 이 구간의 `unknown` 하나만으로 그 node가 죽었다고 판단하지 않는다. Leader·quorum 상태를 함께 보고, 다음 표본에서 다시 확인한다. 배치 전제는 [EKS architecture의 배치 제약](../architectures/eks-ec2-deployment.md#placement와-availability-constraint)을 따른다.

한계는 다음과 같다. 실행은 배치마다 한 번이다. 죽은 endpoint가 빠지는 시각은 Node 상태와 fencing 시각에 따라 달라진다. 이번 두 실행은 관찰 도구의 fencing과 기존 Pod 해제를 포함한다. Fencing이 없으면 이 구간이 더 길 수 있다 [INFERENCE]. NodeLocal DNSCache 같은 다른 DNS 구성은 시험하지 않았다. [결과](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/result.json), [분석](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/analysis.json), [재현 실행 DNS 표본](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/cycle-b/post-fault/dns-probe.jsonl), [전제 실행 DNS 표본](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/cycle-a/post-fault/dns-probe.jsonl), [CoreDNS 배치 기록](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/cycle-a-coredns-placement.json)을 따른다.

시험 뒤 합성 namespace 두 개와 broker EBS 3개, AWS CLI로 만든 여분 node group, run 전용 snapshot, 작업 image, CloudWatch log group을 삭제했다. 검토한 delete-only plan으로 Terraform 관리 자원 41개를 삭제했고 state는 비었다. 직접 AWS 조회 22개 항목이 모두 통과했다. 보호 snapshot 3개와 공유 image tag `observability-20261014`의 digest는 그대로다. 공개 증거 폴더와 `docs/`·`deploy/terraform`에서 관리 token, MQTT 비밀번호, fixture Secret의 base64 값 10개와 PEM private key 머리글을 검색했고 발견되지 않았다. 합성 입력·plan·state·kubeconfig·도구 출력은 Windows DPAPI `CurrentUser`로 암호화해 보존했다. 각 파일은 복호화 뒤 원본과 같은지 확인한 다음 평문을 지웠다. [직접 정리 확인](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/direct-cleanup-verification.json)을 따른다. `CLU-14 FAILED/UNRESOLVED`와 기존 전체 local gate 실패는 유지한다.

### EKS 장애 matrix 보완 기준 (2026-10-16)

이 절은 DEP-20·DEP-21의 남은 EKS 장애 시나리오와 판정 기준을 실행 전에 정한다. 대상은 전용 합성 fixture다. 각 시나리오는 한 번 실행하며, 한 번의 결과를 반복 성능으로 해석하지 않는다. 제품 동작 기준은 [Cluster §6·§7](cluster-session-and-delivery.md#7-failure-recovery), [ADR-0003](../decisions/ADR-0003-durable-ack-barrier.md), [Cluster §11.28 publish backpressure](cluster-session-and-delivery.md#1128-publish-backpressure-수용-매핑)와 [PRD §4.2](../requirements/PRD.md#42-초기-성공-지표)의 비정상 노드 장애 RTO 30초다.

#### 공통 durable stream oracle

Durable stream oracle은 PUBACK을 받은 메시지가 장애 뒤에도 사라지지 않는지(RPO 0) 확인한다.

1. 장애 전에 subscriber `durable-stream-subscriber`가 장애 대상 Pod에서 persistent session(Session Expiry 3600초)을 만든다. 시나리오 topic을 QoS 1로 구독한 뒤 정상 종료한다.
2. Publisher `durable-stream-publisher`는 생존 Pod에 연결해 장애 직전부터 순번 payload를 MQTT 5 QoS 1로 보낸다. 기본 간격은 3초이고 시나리오당 최대 60개다. 분리된 session의 durable queue는 session당 64개까지 담는다. 그 뒤의 대상은 `Saturated`로 건너뛰지만 publish는 성공할 수 있다([Cluster §4.2](cluster-session-and-delivery.md#42-qos-12-durable-delivery)의 승인된 결정). 그래서 60개를 넘겨 보내면 RPO가 아니라 이 한도를 재게 된다. 메시지마다 결과를 기록한다.
   - `acked`: reason code가 `0x80` 미만인 PUBACK을 받았다.
   - `rejected`: reason code가 `0x80` 이상인 PUBACK을 받았다.
   - `unknown`: 15초 안에 PUBACK이 없거나 연결이 끊겼다.
3. 확인 단계에서 subscriber는 지정한 Pod에 Clean Start 0으로 다시 연결한다. `SessionPresent=1`이어야 한다. 10초 동안 새 메시지가 없을 때까지 받는다.
4. 통과 조건은 두 가지다. `acked` 메시지는 모두 한 번 이상 받아야 한다(누락 0). `rejected` 메시지는 받으면 안 된다. `unknown` 메시지의 전달 여부와 QoS 1 중복 수는 기록만 하고 판정에 쓰지 않는다.

2026-10-16 첫 EKS 실행은 이 상한 없이 0.2초 간격으로 보냈다. D3 subscriber session은 PUBACK을 받은 1,515개 중 seq 65 이후를 받지 못했다. 이는 위 session당 64개 한도와 일치하며 oracle 설계 오류로 판정한다. 그 실행의 RPO 판정은 쓰지 않는다. 증거는 `.artifacts/verification/eks-matrix-20261016T065350Z-24adc2/storage/`에 보존한다.

#### DEP-20 leader EC2 손실

- 사전 조건은 기존 follower 시험과 같다. 차이는 다음과 같다. 대상은 현재 leader여야 한다. 관찰 Pod는 다른 AZ의 follower여야 한다.
- 장애 주입, fencing, 기존 Pod 해제, 같은 AZ 여분 worker의 같은 PVC·PV·EBS 복구 판정은 [EKS worker observer 보완 기준](#eks-worker-observer-보완-기준)을 따른다.
- 관리 API 표본에서 생존 member 하나가 새 leader로 보여야 한다.
- 관리 API와 MQTT 표본의 실패는 장애 요청 뒤 하나의 연속 구간에만 허용한다. 그 구간은 첫 실패 표본 시작부터 다음 성공 표본 종료까지 30초 이하여야 한다. 구간 밖의 실패가 있으면 실패다.
- Durable stream 확인은 복구 뒤 생존 Pod에서 한다.

#### DEP-20 단일 AZ 전원 중단 모델

이 시나리오는 한 AZ의 compute 전원을 잃은 상태를 모델로 삼는다. AZ의 network·EBS 서비스·EKS control plane 장애는 모델에 포함하지 않는다. 실제 AZ 장애를 완전히 재현한 것으로 표현하지 않는다.

- 대상 AZ에는 follower 한 개가 있어야 한다. Leader와 관찰 Pod는 다른 AZ에 있어야 한다. 대상 AZ에는 여분 worker가 없어야 한다.
- 장애 전에 대상 AZ를 포함한 node group ASG의 `Launch`, `Terminate`, `ReplaceUnhealthy`, `AZRebalance`, `HealthCheck` process를 멈춘다. 이 조치는 정지한 instance가 교체되지 않게 하는 시험 통제다. 시험 전부터 멈춰 있던 process는 건드리지 않는다. 시험이 끝나면 도구가 멈춘 process만 다시 켠다.
- Process를 다시 켜기 전에 시작한 instance의 ASG health를 확인한다. Instance가 `running`이고 Kubernetes Node가 Ready인데 ASG health가 `Unhealthy`이면 `set-instance-health --health-status Healthy`로 되돌리고 기록한다. 120초 안에 조건을 만족하지 못한 instance는 바꾸지 않고 기록한다.
- 대상 AZ의 모든 worker EC2를 `stop-instances --skip-os-shutdown`으로 정지한다. 480초 동안 유지한 뒤 같은 instance를 시작한다.
- 중단 동안 관찰 Pod의 관리 API 표본은 모두 `quorumAvailable=true`여야 하고 leader는 대상 AZ 밖에 있어야 한다. MQTT 표본은 모두 성공해야 한다.
- 중단 동안 subscriber가 관찰 Pod로 다시 연결해 durable stream을 1차 확인한다. 대상 Pod가 소유했던 session을 생존 node가 넘겨받는지 보는 단계다.
- 시작 뒤 900초 안에 대상 slot의 Pod가 Ready가 되어야 한다. 같은 PVC·PV·EBS를 써야 하고, 3개 broker Ready와 quorum이 회복되어야 한다. 이후 durable stream을 2차 확인한다.
- Process를 다시 켠 뒤 `postResumeSeconds`(기본 180초) 동안 10초 간격으로 관찰한다. 이 동안 ASG scaling activity가 없어야 하고, ASG instance 집합과 broker Pod UID가 회복 시점과 같아야 한다. 이 조건은 모델이 만든 상태가 시험 뒤에도 그대로 유지되는지 확인한다. 모델 통제를 푼 직후의 교체는 실제 AZ 장애가 아니라 시험 통제의 부작용이므로 별도 실패로 판정한다.

2026-10-16 첫 실행(`HealthCheck` 미중단, 사후 관찰 없음)은 위 중단·회복 조건을 모두 통과했다. 그러나 도구가 process를 다시 켠 6초 뒤 ASG가 다시 시작한 instance를 "EC2 health check가 stopped를 보고함" 이유로 종료하고 새 instance를 띄웠다. Data volume은 같은 PVC로 새 instance에 붙었다. 이 교체는 도구 판정 뒤에 일어나 판정에 들어가지 않았다. 그래서 `HealthCheck` 중단과 사후 관찰 조건을 추가했다.

#### DEP-21 EBS disk pressure·ENOSPC

Ballast는 대상 broker Pod에 추가한 ephemeral container가 같은 PVC filesystem에 쓰는 파일이다. Broker container와 제품 설정은 바꾸지 않는다. 대상은 follower다. 기본 `minFreeBytes`는 1 GiB다([Process configuration v1](../contracts/process-configuration-v1.md)).

| 단계 | 조치 | 통과 조건 |
| --- | --- | --- |
| D1 admission 압박 | 대상 filesystem의 여유 공간을 1 GiB 미만, 0보다 크게 만든다. | 대상 Pod로 보낸 QoS 1 PUBLISH가 10초 안에 PUBACK `0x97`을 받는다. 생존 Pod 경로의 publish는 성공한다. Ballast를 줄여 여유 공간을 1 GiB+128 MiB 이상으로 만들면 10초 안에 대상 경로 publish가 다시 성공한다. |
| D2 follower ENOSPC | 대상 filesystem을 가득 채우고, 관찰 Pod로 retained QoS 1 쓰기 부하를 보낸다. | 부하 시작 60초 안에 대상 node가 Ready가 아니게 되고, **대상 node 자신의** 관리 API에 `lastErrorCode=storage_no_space` 또는 `FAILED`와 `storagePressure=true`가 보인다. 다른 node의 원격 행은 연결 상태만 담고 storage 상태를 담지 않으므로 판정에 쓰지 않는다([Cluster §7](cluster-session-and-delivery.md#7-failure-recovery), [§11.6](cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)). Liveness 503으로 Kubernetes가 container를 다시 시작하면 공간이 회복될 때까지 시작이 실패할 수 있다. 이는 fail-closed 동작이다. 생존 Pod의 quorum과 MQTT 표본은 모두 성공한다. Ballast를 지우면 900초 안에 같은 PVC로 3개 broker가 Ready가 된다. Durable stream 확인이 통과한다. 관찰 tunnel이 끊기면 결과를 판정 불가(FAIL)로 기록한다. |
| D3 두 follower ENOSPC | 두 follower filesystem을 모두 가득 채우고, leader인 관찰 Pod로 retained QoS 1 쓰기 부하를 보낸다. | 두 node 모두 각자의 관리 API에서 storage 실패가 보이거나 Ready가 아니게 된 때부터 한 node의 공간이 회복될 때까지 보낸 메시지에 `acked`가 없어야 한다. 공간 회복과 재시작 뒤 900초 안에 3개 broker가 Ready가 된다. Durable stream 확인이 통과한다. |

D2·D3 부하가 남기는 retained 상태는 256 KiB 이하로 유지해야 한다. 기본값은 128 KiB payload를 2개 topic에 번갈아 쓰는 것이다(`loadPayloadBytes`·`loadTopics`). WAL은 publish마다 payload만큼 커지므로 topic 수를 줄여도 filesystem을 채우는 데는 영향이 없다. 직렬화한 snapshot이 1 MiB를 넘으면 다시 시작한 follower가 snapshot으로 따라잡을 수 없다([Cluster §7](cluster-session-and-delivery.md#7-failure-recovery) 7항). 그러면 이 시험은 disk pressure가 아니라 알려진 snapshot 전송 한계를 측정하게 된다. 256 KiB는 1 MiB 한도에 여유를 두는 시험 설계값이다. 다른 복제 상태와 직렬화 overhead까지 포함한 실제 snapshot 크기는 측정하지 않았다. 부하를 시작한 시나리오는 결과와 관계없이 부하 topic의 retained message 삭제를 시도하고 완료 여부를 기록한다. 완료되지 않았으면 다음 시험 전에 지워야 한다.

2026-10-16 두 번째 실행(storage-2)은 8개 topic(1 MiB)을 썼다. D2 관찰 tunnel이 끊겨 retained 정리를 하지 못했다. 이 상태가 다음 CLU-14 재시드에서 1 MiB를 넘는 snapshot(1,055,832 byte)의 대부분을 차지한 것으로 본다 [INFERENCE].
