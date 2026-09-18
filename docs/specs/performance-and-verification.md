# Performance와 verification 스펙

이 문서는 NaruMQ의 성능·신뢰성·구조 요구를 어떤 조건과 증거물로 판정하는지 정의한다. 아직 합의하지 않은 수치는 임의로 채우지 않으며, 수치가 없으면 해당 release gate를 닫지 않는다.

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 관련 요구사항 | DEP-19~DEP-23, NFR-01~NFR-06, NFR-11~NFR-15, PERF-01~PERF-12, TEST-01~TEST-17 |
| 근거 | [PRD 11장과 15장](../requirements/PRD.md) |
| 관련 Architecture | [Broker core](../architectures/broker-core.md), [Cluster architecture](../architectures/cluster-architecture.md), [EKS on EC2 deployment](../architectures/eks-ec2-deployment.md) |
| 관련 Convention | [Go coding conventions](../conventions/go-coding-conventions.md) |
| 관련 결정 | [ADR-0001](../decisions/ADR-0001-core-implementation-language.md), [ADR-0007](../decisions/ADR-0007-aws-production-deployment-platform.md) |
| 관련 Contract | [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md), [MQTT client compatibility v1](../contracts/mqtt-client-compatibility-v1.md) |
| 해설 | [Go 동시성과 backpressure](../explainers/go-concurrency-and-backpressure.md), [Broker core의 계층 경계](../explainers/broker-core-boundaries.md), [EKS 고가용성과 장애 도메인](../explainers/eks-ha-failure-domains.md), [AWS NLB와 MQTT 연결 lifecycle](../explainers/aws-nlb-mqtt-connection-lifecycle.md) |

## 1. Workload 기준값

Phase 0 결과는 다음 기호를 숫자와 단위로 고정해야 한다.

| 기호 | 의미 | 필수 분해 |
| --- | --- | --- |
| `B_conn` | 최대 동시 연결 | protocol, TLS, tenant, keepalive |
| `B_pub` | publish rate | QoS, payload, retained, durability |
| `B_sub` | delivery rate | QoS, fan-out, shared 여부 |
| `B_msg_size` | payload 분포 | p50/p95/p99/max |
| `B_topic` | topic·filter 규모 | exact, wildcard, overlap, fan-out |

여기에 persistent session 비율, offline 비율·시간, Session Expiry, reconnect ramp, 인증 backend latency, tenant 부하 편차와 AZ network 특성을 기록해야 한다. 평균값만으로 profile을 만들면 안 된다.

## 2. SLI와 판정

| SLI | 측정 경계 |
| --- | --- |
| connect success | TCP/TLS 시도부터 성공 CONNACK까지. 인증·protocol 거절은 사유별 분리 |
| publish ACK latency | client PUBLISH 송신부터 PUBACK 또는 PUBREC 수신까지 |
| publish-to-delivery latency | publisher 송신 timestamp부터 subscriber PUBLISH 수신까지 |
| throughput | 측정 구간 안의 성공 publish와 delivery 수/초 |
| message gap | 성공 durable ACK 범위에서 기대 ID 중 수신되지 않은 수 |
| duplicate | 같은 test message ID를 보장 수준보다 더 받은 수 |
| failover RTO | failure injection부터 새 connection·session에서 성공 처리가 회복될 때까지 |
| drain time | drain operation 시작부터 SAFE_TO_STOP까지 |
| resource efficiency | connection/session/subscription당 memory와 message당 CPU·network·disk |

Latency는 p50, p95, p99와 p99.9를 함께 보고한다. 오류 sample을 latency 통계에서 제거했다면 오류율과 제거 규칙을 같은 결과에 기록해야 한다. Coordinated omission을 피할 수 있는 load model을 사용하고 client·broker clock 오차 또는 단일 clock 측정 방식을 기록해야 한다.

## 3. Benchmark 공통 계약

동일 harness가 target만 바꿔 NaruMQ와 실제 EMQX를 실행해야 한다.

1. broker node 수, CPU/memory limit, storage class, volume, network와 AZ 배치를 같게 한다.
2. MQTT version, TLS, 인증, ACL, QoS, durability, session, topic, payload와 subscription 분포를 기능적으로 같게 한다.
3. 같게 만들 수 없는 기능은 결과에 `non_equivalent`로 표시하고 직접 우열 판정에서 제외한다.
4. load generator는 broker와 분리하고 같은 client implementation, seed, ramp와 message stream을 사용한다.
5. 각 profile은 고정 warm-up과 측정 구간으로 최소 5회 독립 실행한다.
6. 중앙값과 편차를 함께 보고한다. 실행 순서가 영향을 줄 수 있으면 순서를 교차하거나 무작위화하고 기록한다.
7. broker 오류뿐 아니라 load generator saturation과 client 오류도 수집한다.

고정 EMQX version·edition과 NaruMQ 대비 합격 비율은 `SPEC-TBD-08`이다.

## 4. 필수 benchmark profile

| ID | 고정 조건 | 필수 결과 |
| --- | --- | --- |
| BP-01 Protocol Baseline | MQTT 5, QoS 0, non-persistent, 고정 payload | protocol/network 기본 throughput·latency·CPU |
| BP-02 Production Mix | `B_*`와 실제 TLS·인증·tenant 분포 | 대체 가능성, N/N-1 SLO |
| BP-03 Durable Delivery | QoS 1/2, persistent, offline queue, 동등 replication | ACK latency, gap, disk·network 비용 |
| BP-04 Subscription Stress | wildcard, overlap, shared, high fan-out | route propagation, delivery throughput, tail latency |
| BP-05 Connection Scale | ramp, idle, keepalive, reconnect subset | success rate, connection memory, reconnect rate |
| BP-06 Failure Recovery | node kill, rejoin, resync | RTO, gap·duplicate, recovery resource spike |
| BP-07 Rolling Drain | 동일 connection에서 순차 교체 | drain time, reconnect 분포, Will·message 결과 |

BP-03은 durable cluster가 있는 phase, BP-07은 drain이 있는 phase에 통과 판정을 내린다. 기능이 없는 Phase 0 결과를 전체 profile 통과로 표시해서는 안 된다.

## 5. Artifact manifest

각 실행은 다음 artifact를 하나의 run ID로 연결해야 한다.

- Git commit, NaruMQ version, Go patch version
- EMQX version, image digest, edition/license와 전체 설정
- Deployment profile과 지원 상태, Kubernetes/EKS manifest, resource limit, topology와 storage 정보
- AWS profile이면 EKS·EC2·EBS CSI·NLB version과 resolved 설정
- load generator source/version, client SDK, seed와 실행 command
- workload profile JSON과 모든 resolved 값
- warm-up·측정 구간, 반복 번호, 시작·종료 시각
- raw latency·throughput sample, broker/client log와 Metric snapshot
- 오류·drop·gap·duplicate 목록과 aggregate report

비밀정보는 artifact에 포함해서는 안 된다. 재실행에 필요한 secret reference만 기록한다.

## 6. Regression gate

release마다 이전 NaruMQ 안정 version과 고정 EMQX 기준 version을 다시 측정해야 한다. 이전 안정 version보다 throughput 또는 p99 latency가 5% 이상 악화되면 release를 차단한다. 측정 편차가 5%보다 크면 즉시 합격·불합격을 판정하지 않고 원인을 확인한 뒤 반복 횟수를 늘려야 한다.

Phase 0에서 profile별 최소 throughput, 최대 p99·p99.9, connection당 memory, N-1 SLO와 recovery RTO를 숫자로 확정해야 한다. 하나라도 비어 있으면 성능 gate는 실패다.

## 7. Verification 계층

| 계층 | 검증 대상 | 외부 의존성 |
| --- | --- | --- |
| unit | 정상·오류·경계 분기, 상태 머신, ACL, quota | 없음. clock/random/network/storage/consensus/auth는 test double |
| property/fuzz | parser, encoder, topic/filter, session transition 불변식 | 없음 |
| mutation | protocol, delivery, ownership, ACL, drain critical state machine | 없음 또는 결정적 harness |
| integration | 실제 listener, storage adapter, consensus transport와 auth adapter | 격리된 test dependency |
| conformance | MQTT 3.1.1/5.0 normative statement와 wire behavior | MQTT test client |
| end-to-end | Kubernetes, Dashboard/API, client compatibility와 rolling update | production-like cluster |
| chaos | process/host loss, packet loss·latency·partition, disk full·slow I/O | production-like cluster |
| benchmark | 성능·용량·회귀와 EMQX 비교 | 격리된 benchmark environment |

Feature와 bug fix는 실패를 재현하는 test를 포함해야 한다. Flaky test 격리는 임시 조치이며 owner와 해소 기한을 기록해야 한다.

## 8. Coverage와 test quality gate

- 전체 production code는 line/function 90% 이상, branch 85% 이상이어야 한다.
- protocol, session, routing, delivery, tenancy, security와 cluster lifecycle은 line/function 95% 이상, branch 90% 이상이어야 한다.
- PR 변경 code는 line/branch diff coverage 95% 이상이어야 한다.
- coverage는 ratchet이다. 더 높은 기준에 도달한 release 뒤에는 낮출 수 없다.
- generated/vendor code와 단순 선언만 근거를 남겨 제외할 수 있다.
- critical state machine mutation score는 80% 이상이어야 한다.
- concurrency code는 race detector 또는 동등한 검사를 통과해야 한다.

Coverage 수치만으로 합격할 수 없다. P0 요구사항 또는 지원 MQTT normative statement에 연결된 assertion이 없으면 CI가 merge를 차단해야 한다.
- 시스템 스펙([시스템 스펙](README.md))에 명시된 모든 시나리오(정상, 예외, 경계 조건, 상태 전이)는 테스트 케이스로 100% 추적 및 커버되어야 한다. 단위 테스트가 가능한 도메인 규칙과 상태 머신 로직은 외부 의존성 없는 유닛 테스트로 100% 검증함을 기본으로 하고, 분산 합의·노드 드레인·인프라 장애 등 시스템 수준 시나리오는 통합·E2E·카오스 테스트 계층에 누락 없이 매핑하여 검증한다.

## 9. Architecture verification

[ADR-0001](../decisions/ADR-0001-core-implementation-language.md)의 경계를 CI architecture test로 확인해야 한다.

- domain이 storage, Kubernetes, HTTP, UI, Metric, application과 adapter package를 import하지 않는다.
- application은 domain과 자신이 소유한 port에만 의존한다.
- adapter의 framework·RPC·storage type이 application DTO 또는 domain value 경계를 넘지 않는다.
- composition root 외부에서 concrete adapter를 직접 조립하지 않는다.
- session, routing, delivery, tenancy, security와 cluster lifecycle package 간 금지 dependency를 명시한다.
- N-1↔N wire·persisted state·snapshot compatibility test를 release matrix에 포함한다.

## 10. Release evidence와 P0 gate

P0 traffic 승인은 다음 증거가 모두 있을 때만 가능하다.

1. 모든 P0 요구사항에 unit test와 자동 acceptance test가 연결돼 있다.
2. MQTT compliance matrix에 지원 normative statement가 빠짐없이 연결돼 있다.
3. coverage, mutation, fuzz와 race gate가 통과했다.
4. BP-01~BP-07을 각 기능이 완성된 phase에서 실행했고 raw artifact가 있다.
5. 정상 3-node와 N-1에서 확정한 production profile SLO를 충족했다.
6. 단일 node kill에서 durable ACK message gap이 0이다.
7. `EKS_EC2_HA`에서 세 quorum Pod의 EC2·가용 영역·EBS 분리, 단일 가용 영역 장애와 NLB deregistration 결과를 검증했다.
8. 연속 100회 rolling update에서 persistent session 손실과 지원 client의 unexpected Will이 0이다.
9. tenant isolation과 관리 API RBAC penetration test를 통과했다.
10. backup/restore, rollback, Metric·alert·Dashboard를 production-like 환경에서 검증했다.
11. MQTT 3.1.0 inventory와 migration/격리 증거가 있다.

증거가 없으면 완료가 아니다.
