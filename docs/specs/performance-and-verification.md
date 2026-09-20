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

고정 EMQX version·edition과 NaruMQ 대비 합격 비율은 `SPEC-TBD-08`이다. BP-01의 역사적 OSS 비교 대상은 `emqx/emqx:5.8.8@sha256:4593b6e30196754779fa14483332cb0be56c233f1c8a387b1308bec4337c0144`로 고정하지만, 현재 지원 release 기준선과 합격 수치는 여전히 미확정이다.

### 3.1 BP-01 관찰 경계

BP-01 공통 하네스의 smoke 입력과 비교 경계는 다음으로 고정한다.

- MQTT 5.0, QoS 0, 비영속 workload, `Clean Start=true`, `Session Expiry Interval=0`
- exact topic 하나, publisher 1개와 subscriber 1개, profile에 지정한 고정 payload 크기
- publisher가 subscriber의 SUBACK 성공 뒤 시작하며, publisher 송신 시각부터 subscriber 수신까지를 같은 runner의 monotonic clock으로 측정
- plaintext loopback network namespace만 사용하며, runner는 broker container의 network namespace에서 `127.0.0.1:1883`에 연결
- NaruMQ와 EMQX는 같은 CPU·memory limit와 read-only root filesystem 조건으로 순차 실행하고, EMQX는 Open Source Edition 5.8.8 역사적 비교 대상으로 기록
- EMQX 5.8.8은 CONNACK에 `Receive Maximum` 같은 MQTT 5 property를 보낼 수 있지만, `internal/domain/mqtt`의 현재 property-free subset 디코더는 이를 의도적으로 거부한다. 따라서 BP-01 runner는 CONNECT 응답에 한해 `readWireFrame`·`readConnAck`로 property section의 길이 일관성만 검증하고, 나머지 workload packet은 기존 domain codec을 재사용한다.

warm-up sample은 집계에서 제외한다. 측정 구간의 `sent == delivered`이고 duplicate·missing·protocol error·generator overload가 모두 0일 때만 target run을 성공으로 판정한다. 이 smoke 결과는 하네스와 실행 경로의 동작 증거이며 release-gating 또는 production 대체 가능성의 근거가 아니다.

profile의 duration은 이 문서의 공통 규칙대로 millisecond 정수로 표현한다. 실제 Phase 0 수치형 workload와 SLO는 `SPEC-TBD-08`을 닫기 전까지 임의로 정하지 않는다.

### 3.2 BP-05 제한된 Connection Scale smoke 관찰 경계

BP-05 smoke는 전체 연결 규모나 운영 용량을 정하는 시험이 아니라, 같은 MQTT 5 연결 ramp를 두 target에 적용하는 실행 경로 증거다. 현재 고정 profile은 `tests/benchmark/bp05/testdata/smoke.json`의 다음 값만 사용한다.

- `connectionCount=64`, `rampConnectionsPerSecond=32`, `baselineMillis=3000`, `idleMillis=4000`
- `reconnectCount=16`, `reconnectRatePerSecond=16`, `keepAliveSeconds=2`, `pingIntervalMillis=500`
- broker `1 CPU / 512 MiB`, generator `1 CPU / 256 MiB`
- MQTT 5, `Clean Start=true`, `Session Expiry Interval=0`, plaintext loopback `127.0.0.1:1883`

runner는 `bp05-000000`부터 고정 폭 Client ID를 사용한다. initial ramp 뒤 각 성공 연결에서 첫 `PINGREQ`를 ping interval 안에 분산하고, 이후 고정 간격으로 `PINGRESP`를 정확히 하나씩 확인한다. idle 종료 후 가장 낮은 Client ID 16개를 정상 `DISCONNECT`하고 같은 Client ID로 reconnect한다. 연결 시도와 phase 시작·종료는 각각 `connections.jsonl`과 `phases.jsonl`에 즉시 기록한다.

broker memory는 runner 시작 전 표본의 중앙값을 `baselineMemoryBytes`, `idle` phase 표본의 중앙값을 `plateauMemoryBytes`로 계산한다. 표본이 짝수이면 정렬된 가운데 두 값에 대해 `lower + (upper-lower)/2`를 사용한다.

```text
incrementalMemoryBytes = plateauMemoryBytes - baselineMemoryBytes
memoryBytesPerIdleConnection =
    incrementalMemoryBytes / idleSurvivors
```

차이는 signed 값으로 보존하며 음수로 clamp하지 않는다. 각 구간 표본이 2개 미만이면 target resource 결과를 invalid로 표시한다. `workload-summary.json`과 `resource-summary.json`이 모두 valid이고 두 target의 final ping까지 성공한 경우에만 합성 `summary.json`을 valid로 표시한다.

이 smoke는 전체 BP-05 통과, `B_conn`, connection당 production SLO, release 합격 비율 또는 gate 8을 확정하지 않는다. 반복 실행에 따른 편차와 실제 production profile은 별도 결정인 `SPEC-TBD-08`이 닫힌 뒤 정한다.

### 3.3 PERF-07 반복 suite 측정 계약

BP-01과 BP-05의 `compare`는 한 번의 독립 실행을 기록하고, 반복 측정은 각 benchmark package의 `suite` 명령이 소유한다. suite는 다음 명령 형식을 사용한다.

```text
go run ./tests/benchmark/bp01 suite --profile <path> [--output <root>] [--repetitions <n>]
go run ./tests/benchmark/bp05 suite --profile <path> [--output <root>] [--repetitions <n>]
```

`--repetitions`의 기본값은 5이며 5보다 작으면 artifact를 만들지 않고 거부한다. 각 회차는 같은 profile과 한 번 준비한 immutable NaruMQ·runner image를 사용하지만, target별 새 container와 새 회차 directory를 사용하므로 이전 회차 상태를 공유하지 않는 독립 실행이다. suite는 회차 시작 전에 NaruMQ와 EMQX를 다음 순서로 교차한다.

```text
1: narumq, emqx
2: emqx, narumq
3: narumq, emqx
...
```

이 순서와 실제 시작·종료 시각, `runId`, 성공 여부와 실패 이유는 parent `suite-manifest.json`에 기록한다. 무작위 순서를 사용하지 않으며, suite가 받은 원래 명령은 각 child manifest의 `command`에도 그대로 보존한다.

suite metric의 중앙값과 편차는 다음처럼 계산한다. 입력 sample은 `repetition`, `runId`, `value`를 가지며, sample의 순서를 바꾸지 않고 중앙값 계산용 값만 오름차순으로 복사한다.

```text
median = sorted values의 가운데 값
       = 짝수 개이면 가운데 두 값의 산술평균
MAD    = median(|value - median|)
```

suite directory는 `.artifacts/benchmark/<benchmark>/<suite-id>/`이며 `profile.json`, `suite-manifest.json`, `suite-summary.json`과 `runs/001/`부터 고정 폭으로 증가하는 child artifact를 보존한다. child artifact는 기존 단일 `compare`의 `manifest.json`, `comparison.json`, target raw artifact와 동일한 schema를 사용한다. parent summary는 BP별 안정된 rate·latency·resource 필드와 NaruMQ runtime 필드만 선택해 `MetricSummary`로 집계하며 target 간 비율, 승자, 합격 threshold는 계산하지 않는다.

image build/pull 또는 suite manifest 갱신처럼 실행 추적 자체가 불가능한 준비·기록 실패는 즉시 suite를 끝내고 빈 target `metrics`와 해당 이유를 기록한다. 회차의 target 실행, comparison, 필수 metric sample 또는 runtime applicability 실패는 추적 가능한 범위에서 후속 회차를 계속 보존하되 suite 전체를 invalid로 표시한다. 이 경우 parent summary의 모든 target `metrics`는 비우고 실패 이유와 child raw artifact만 남긴다. 단, NaruMQ runtime의 GC CPU metric이 한 회차라도 unavailable이면 이는 유효한 unavailable 상태로 기록하며 suite invalid 사유로 삼지 않고, 세 CPU metric만 생략한다. 모든 suite artifact와 smoke 반복 결과는 `releaseGating=false`를 유지하며 production profile, Gate 8 통과, NaruMQ 우위를 판정하지 않는다.

반복 순서와 중앙값·MAD 계산은 `tests/benchmark/internal/suiteartifact`가 소유한다. BP-01과 BP-05 package는 metric 선택, parent·child artifact schema와 Docker 실행을 각각 소유한다.

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

BP-01 실행은 다음 파일을 target artifact로 남긴다.

- `profile.json`과 원문 SHA-256, resolved profile
- `samples.jsonl`의 sequence·phase·scheduled/send/receive UTC timestamp·schedule lag·latency nanoseconds
- `phases.jsonl`의 `warmup`·`measure`·`drain` 각 `start`/`end` UTC 시각
- NaruMQ target의 `runtime-metrics.jsonl`과 `runtime-summary.json` (Go runtime/metrics 원시 표본과 측정 구간 요약)
- `summary.json`의 offered/sent/delivered, publish/delivery rate, p50/p95/p99/p999, schedule lag, duplicate/missing/error와 `releaseGating`
- `broker-stats.jsonl`의 CPU·memory·network·block I/O·PID 표본(초기 표본과 runner 종료 직후 최종 표본 포함)
- broker stdout/stderr, 실제 image ID/digest와 resource-limit inspect
- Docker build base의 planned digest, 실제 immutable resolved digest와 substitution 사유

BP-05 제한된 smoke 실행은 `.artifacts/benchmark/bp05/<run-id>/`에 다음 target artifact를 남긴다.

- `profile.json`과 원문 SHA-256, resolved profile
- `connections.jsonl`의 client ID·phase·scheduled/started/CONNACK UTC timestamp·schedule lag·latency·success·error
- `phases.jsonl`의 `ramp`·`idle`·`reconnect`·`final_check`·`cleanup` 시작/종료 시각과 active connection 수
- `workload-summary.json`의 initial/reconnect requested·attempted·succeeded·failed, 실제 rate, latency percentiles, idle survivor, PING, protocol/transport error와 generator overload
- `resource-summary.json`의 baseline/plateau 표본 수·중앙 memory, signed incremental memory와 idle connection당 계산값
- 합성 `summary.json`, broker stats/log, runner log, container inspect, 실제 image ID/digest와 동일 resource/network/read-only 조건
- NaruMQ target의 `runtime-metrics.jsonl`과 `runtime-summary.json` (Go runtime/metrics 원시 표본과 `idle` 구간 요약)
- EMQX target의 `runtime-summary.json` (`not_applicable` 상태와 Go 프로세스에만 적용된다는 이유)

이 artifact는 하네스 동작과 제한된 Phase 0 gate 6 증거를 보존한다. 전체 BP-05 용량, `B_conn`, production SLO와 release gating 결과를 대신하지 않으며 `releaseGating=false`를 유지한다.

반복 suite는 위 target artifact를 다음 parent 파일로 함께 보존한다.

- `suite-manifest.json`: `suiteId`, benchmark ID, profile SHA-256과 resolved profile, 요청·완료 회차 수, `orderStrategy=alternating`, host·Docker·image metadata, `runs[]`, `valid`, `failureReason`과 `releaseGating=false`
- `suite-summary.json`: target별 `metrics`, `runtimeApplicability`, 성공 회차 수, 중앙값·MAD·최솟값·최댓값과 `releaseGating=false`
- `runs/001/`부터 `runs/<n>/`: 회차별 `manifest.json`, `comparison.json`, raw sample·log·runtime artifact

회차 하나가 실패하면 후속 회차를 실행할 수 있지만 parent `valid=false`이고 모든 target의 parent `metrics`를 비운다. parent의 중앙값·MAD는 모든 요청 회차가 성공하고 필수 metric sample과 applicability 검증을 통과한 경우에만 기록한다. 따라서 일부 성공 회차의 숫자로 전체 반복 suite를 합격으로 보이게 하지 않는다.

EMQX `5.8.8`은 Open Source Edition의 EOL historical comparator로 기록하며, 결과에는 EOL `2026-02-28`과 `releaseGating=false`를 함께 남긴다. `.artifacts/benchmark/bp01/<run-id>/`는 로컬 evidence 경로이고 source commit에 raw 결과를 섞지 않는다.

비밀정보는 artifact에 포함해서는 안 된다. 재실행에 필요한 secret reference만 기록한다.

### 5.1 Gate 7 런타임 측정 artifact

NaruMQ 프로세스는 `runtime/metrics`의 누적 표본을 `runtime-metrics.jsonl`에 기록한다. `phases.jsonl`의 요청 phase에 정확히 하나의 `start`와 `end`가 있어야 하며, 두 시각 안에 들어오는 표본만 요약에 사용한다. 표본은 최소 2개이고 첫 표본과 마지막 표본의 시각이 달라야 한다.

요약은 첫 표본과 마지막 표본의 누적값 차이를 실제 표본 시각 차이로 나눈다.

```text
elapsedSeconds = lastCollectedAt - firstCollectedAt
allocationBytesPerSecond =
    (last.heapAllocBytesTotal - first.heapAllocBytesTotal) / elapsedSeconds
allocationsPerSecond =
    (last.heapAllocObjectsTotal - first.heapAllocObjectsTotal) / elapsedSeconds
gcCPUFraction =
    (last.gcCPUSecondsTotal - first.gcCPUSecondsTotal) /
    (last.runtimeCPUSecondsTotal - first.runtimeCPUSecondsTotal)
```

GC pause 백분위수는 누적 histogram count의 차이에 nearest-rank를 적용하고, 선택된 bucket의 exclusive upper bound를 초 단위 값으로 기록한다. pause 표본이 없으면 count와 백분위수는 0이다. 무한대 bucket이 선택되거나 누적 counter·bucket 경계가 감소·변경되면 요약은 invalid다. 측정 구간에 GC cycle이 한 번도 실행되지 않으면 현재 Go runtime에서 `/cpu/classes` 누적값이 증가하지 않을 수 있다. 이때 `cpuMetricsAvailable=false`와 `cpuMetricsReason`을 기록하고 CPU 관련 값은 추정하지 않는다. GC CPU counter까지 증가했는데 runtime CPU delta가 0이면 artifact는 invalid로 처리한다.

이 값은 NaruMQ Go 프로세스 내부의 진단값이다. `/cpu/classes`의 GC CPU는 Docker CPU 사용량과 같은 값이 아니며, EMQX의 Erlang/BEAM 런타임 값으로 변환하거나 직접 비교하지 않는다. 따라서 EMQX는 숫자 0이 아니라 `applicability=not_applicable`과 이유를 기록한다. `releaseGating=false`이며 smoke 표본은 production SLO가 아니다. 수치 기준과 반복 실행 편차가 확정되지 않았으므로 이 artifact만으로 Gate 7 또는 Gate 8을 닫지 않는다.

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

Phase 0의 MQTT normative statement 연결표는
[`tests/conformance/mqtt-compliance-v1.json`](../../tests/conformance/mqtt-compliance-v1.json)이다.
이 파일은 지원 범위라고 선언한 statement, requirement ID와 자동 test function을 함께
기록한다. `go run ./tools/verify traceability`는 source comment와 matrix를 양방향으로
비교한다. 따라서 matrix에만 있는 test, source에만 있는 statement, requirement union이
다른 경우를 모두 실패로 처리한다.

## 8. Coverage와 test quality gate

- 전체 production code는 line/function 90% 이상, branch 85% 이상이어야 한다.
- protocol, session, routing, delivery, tenancy, security와 cluster lifecycle은 line/function 95% 이상, branch 90% 이상이어야 한다.
- PR 변경 code는 line/branch diff coverage 95% 이상이어야 한다.
- coverage는 ratchet이다. 더 높은 기준에 도달한 release 뒤에는 낮출 수 없다.
- generated/vendor code와 단순 선언만 근거를 남겨 제외할 수 있다.
- critical state machine mutation score는 80% 이상이어야 한다.
- concurrency code는 race detector 또는 동등한 검사를 통과해야 한다.

현재 Phase 0의 자동 검증은 다음 명령을 기준으로 한다.

```text
go run ./tools/verify local
go run ./tools/verify ci --base <merge-base>
```

`local`은 `go vet`, 전체 test, race test, traceability, architecture와
coverage profile 생성을 순서대로 실행한다. `ci`는 같은 검증에 더해 `base...HEAD`
의 production 변경 line/function diff coverage를 확인한다. 두 명령은 첫 실패에서
멈추고, coverage 원본과 요약을 `.artifacts/verification/`에 남긴다.

Coverage 계산은 Go atomic coverprofile의 executable block을 합쳐 다음처럼 만든다.

```text
line coverage     = 실행된 source line 수 / 전체 실행 source line 수
function coverage = 실행된 function 수 / 전체 function 수
diff coverage     = base...HEAD에 추가된 실행 line/function 중 실행된 수 / 전체 수
```

변경된 실행 line이 없으면 diff coverage는 100%로 기록한다. Branch coverage는
승인된 branch instrumentation 도구가 repository에 고정되기 전까지 `not_evaluated`
와 이유를 artifact에 기록하며, line coverage를 branch coverage로 대신하지 않는다.
현재 기준과 ratchet 값은 [`tests/verification/coverage-policy.json`](../../tests/verification/coverage-policy.json)에 둔다.

Ratchet 값은 release evidence가 안정된 측정치를 확인한 뒤에만 올린다. 동일한
runner를 여러 번 실행해 coverage count가 고정됐는지 확인하고, 확인한 요약 수치를
정책에 반영한다. 측정치가 baseline보다 높다는 이유만으로 자동 변경하지 않으며,
이후 측정치가 baseline 아래로 내려가면 gate가 실패한다.

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

`go run ./tools/verify architecture`는 `go list -json ./cmd/... ./internal/...`의 실제
production import graph를 읽어 domain → application → adapter 경계를 검사한다.
`tests`와 `tools`는 production package가 import하지 못하며, adapter끼리의 직접 의존과
composition root 밖의 concrete adapter 조립도 실패시킨다. 이 검사는 source tree를
복사한 예시가 아니라 현재 package graph를 대상으로 한다.

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
