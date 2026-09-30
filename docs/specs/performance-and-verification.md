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

BP-01, BP-03과 BP-05의 `compare`는 각각 한 번의 독립 실행을 기록하고, 반복 측정은 각 benchmark package의 `suite` 명령이 소유한다. suite는 다음 명령 형식을 사용한다.

```text
go run ./tests/benchmark/bp01 suite --profile <path> [--output <root>] [--repetitions <n>]
go run ./tests/benchmark/bp03 suite --profile <path> [--output <root>] [--repetitions <n>]
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

반복 순서와 중앙값·MAD 계산은 `tests/benchmark/internal/suiteartifact`가 소유한다. BP-01, BP-03과 BP-05 package는 metric 선택, parent·child artifact schema와 Docker 실행을 각각 소유한다.

### 3.4 BP-03 Durable Delivery smoke 관찰 경계

BP-03은 QoS 1/2 persistent session의 durable delivery 경로에서 ACK, 수신 누락·중복, WAL 동기화 분포와 자원 사용을 기록하는 고정 smoke workload다. `compare` 단일 실행은 기본적으로 NaruMQ를 먼저 실행하고, `suite`는 target 순서를 번갈아 적용한다. 이 benchmark는 smoke workload의 재현성과 수집 경로를 검증하며 production SLO, 우열 또는 release 통과를 판정하지 않는다. 모든 BP-03 artifact는 `releaseGating=false`다.

#### Topology와 비교 경계

- NaruMQ는 `narumq-bp03` dynamic cluster의 3개 broker node를 사용한다. QoS 1/2 ACK는 quorum에 복제된 Raft WAL Ready frame의 fsync 뒤에 관찰한다. 세 node는 Docker named volume을 사용한다.
- EMQX는 pinned EMQX 5.8.8 Open Source Edition의 3-node static cluster다. `durable_sessions.enable=false`인 일반 persistent session state는 node-local memory에 있고 replicated durable log나 fsync를 사용하지 않는다.
- 두 target은 profile에 지정된 broker CPU/memory limit, runner CPU/memory limit, payload, stream, rate와 phase를 공유한다. Broker는 read-only root filesystem에서 실행하고, runner는 별도 container를 사용한다. 각 broker container는 자체 network namespace owner와 Docker bridge network를 공유한다.
- NaruMQ fixture는 MQTT CONNECT에서 JWKS를 통해 RS256 JWT를 검증하고 peer 간에는 mTLS를 사용한다. Broker별 node config, private peer key, management token은 해당 broker에만 mount한다. Runner credential은 별도로 제공하며, network owner에는 `jwks.json`만 포함된 read-only directory를 mount한다.
- EMQX target에는 authenticator를 설정하지 않는다. Runner가 credential을 보내더라도 EMQX는 이를 JWT로 검증하지 않으므로 인증 경계가 동등하지 않다.
- `replication` 차이는 **EMQX OSS 5.8.8 regular session은 node-local memory를 사용하고 `builtin_raft`는 Enterprise 전용**이라는 점이다. `durability` 차이는 **EMQX PUBACK이 fsync보다 먼저 반환**된다는 점이다. `authentication` 차이는 **NaruMQ는 CONNECT에서 JWT를 검증하지만 EMQX에는 authenticator가 없다**는 점이다. 비교 산출물은 세 차원을 `nonEquivalent`로 기록하고 ACK latency를 직접적인 우열 비교에서 제외한다.
- Storage는 로컬 Docker named volume을 사용한다. Linux/EBS production filesystem, 실제 전원 손실, production volume loss/reseed 또는 승인된 RPO/RTO를 입증하지 않는다.

#### 고정 smoke profile

`tests/benchmark/bp03/testdata/smoke.json`은 아래 profile을 strict decode한다. 숫자 수용 범위는 smoke 하네스의 안전 한계이며 production capacity나 SLO가 아니다.

| 필드 | Smoke 값 | 의미와 유효 범위 |
| --- | --- | --- |
| `profileId` | `bp03-smoke-v1` | 비어 있지 않은 실행 이름 |
| `purpose` | `smoke` | 유일하게 허용하는 목적 |
| `seed` | `3` | 재현 가능한 payload seed |
| `pairsPerQoS` | `2` | QoS 1과 QoS 2 각각 publisher/subscriber 2개 stream |
| `payloadBytes` | `256` | 16–65,536 bytes |
| `sessionExpirySeconds` | `300` | 1–86,400 seconds |
| `maxInflightPerPublisher` | `8` | 1–32개 미응답 publish |
| `ackTimeoutMillis` | `10000` | 1,000–60,000 ms |
| `ratesPerStreamPerSecond` | `[5, 10, 25]` | 1–8개의 오름차순 rate, 각각 1–1,000 message/s |
| `warmupMillis` | `2000` | 각 rate의 warm-up 시간; 통계에서 제외 |
| `measureMillis` | `12000` | 각 rate의 측정 시간; 최소 3,000 ms |
| `drainMillis` | `5000` | ACK된 publish의 수신을 기다리는 최대 시간 |
| `offlineSettleMillis` | `1000` | subscriber disconnect 뒤 기다리는 시간 |
| `offlineMessagesPerStream` | `32` | QoS별 두 stream이 offline 중 보낼 메시지 수; session당 64보다 작음 |
| `reconnectTimeoutMillis` | `20000` | session 재접속·offline 전달 대기 한도 |
| `maxMessages` | `4096` | runner collector가 보유할 수 있는 총 message 상한 |
| `brokerCPUs` / `brokerMemoryBytes` | `1` / `1073741824` | 각 broker container의 CPU와 byte 단위 memory limit |
| `generatorCPUs` / `generatorMemoryBytes` | `2` / `536870912` | runner container의 CPU와 byte 단위 memory limit |

각 QoS의 publisher와 subscriber는 서로 다른 cluster node를 사용한다. persistent subscriber가 연결을 끊은 동안 stream별 32개 메시지를 publish하고, 다른 node로 `CleanStart=false` 재접속해 `SessionPresent=true`와 모든 offline 메시지의 전달을 확인한다. `compare` 한 번은 NaruMQ와 EMQX를 각각 실행하고, `suite` 기본값은 독립 실행 5회다.

#### 관찰 순서와 유효성

Runner는 다음 phase를 순서대로 기록한다.

1. `readiness`: NaruMQ 세 node의 health, cluster ID와 같은 leader를 확인한다. 두 target 모두 6개 node 쌍의 cross-node QoS 1 probe publish와 delivery를 확인한다.
2. `setup`: persistent subscriber를 구독시키고 publisher를 연결한다. Subscriber는 `CleanStart=true`와 profile Session Expiry, publisher는 `CleanStart=true`와 Session Expiry 0을 사용한다.
3. 각 rate에서 `step-<ss>-warmup` 뒤 `step-<ss>-measure`를 실행한다. Runner는 절대 시각 schedule과 최대 inflight publish 수를 적용한다. QoS 1은 PUBACK, QoS 2는 PUBCOMP 수신 뒤 inflight slot을 해제한다. 측정 전 warm-up 교환이 완료되거나 timeout될 때까지 기다린다. NaruMQ는 각 measure 경계에서 `/metrics`를 scrape하며, end scrape 전에 publisher 교환이 완료되거나 timeout되도록 기다리고 해당 window의 ACK 분모를 그 시점에 고정한다.
4. `drain`: 새 publish를 멈추고 drain 한도 안에 ACK된 measure 메시지가 수신될 때까지 기다린다.
5. `offline`: subscriber를 정상 disconnect하고 settle 시간 뒤 stream마다 QoS 1/2 메시지 32개를 publish한다. End scrape 전에 publisher 교환이 완료되거나 timeout될 때까지 기다린다. NaruMQ는 이 구간의 WAL metric도 scrape한다.
6. `reconnect`: 하나의 deadline 안에 각 persistent subscriber를 다른 node로 재접속하고 offline delivery를 기다린다. 연결 시도도 같은 deadline에 포함한다. `SessionPresent=false`는 실패이며, 예상한 모든 offline receipt와 publisher ACK, QoS 2 PUBCOMP가 끝나기 전에 연결을 정리하지 않는다.
7. `cleanup`: reader를 멱등하게 중지하고 DISCONNECT를 보낸 뒤 socket을 닫으며, reader가 종료된 후 raw message/phase 기록과 요약을 저장한다.

`workload-summary.json`은 각 measure step 및 offline QoS별 offered/sent/acked/delivered/gap/duplicate, ACK 및 publish-to-delivery latency와 rate를 기록한다. `gap`은 성공 ACK를 받았지만 drain 기한까지 수신하지 못한 메시지 수다. QoS 1 duplicate는 보고하지만 at-least-once 규칙상 무효화하지 않는다. QoS 2 duplicate, protocol error, session 미복원, generator overload, timeout·rejected ACK, 불완전 교환 또는 gap은 workload를 invalid로 만든다. QoS 2 교환은 PUBCOMP에서 완료된다. PUBREC 뒤 발생한 timeout도 timeout으로 집계하며 해당 교환은 미완료 상태로 남는다. `generatorOverload`는 기록한 publish의 `schedule lag >= stream interval`일 때 설정되며 CPU utilization 지표가 아니다. 이 값만으로 runner CPU 포화와 broker/ACK backpressure를 구분하거나 quorum failure의 원인을 증명할 수 없다.

`resource-summary.json`은 runner phase의 시작·종료 시각으로 broker 및 network-owner Docker stats를 자른다. 각 measure/offline window와 node에서 broker CPU median, memory 최대값, block read/write delta 및 owner network Rx/Tx delta를 기록하고 cluster total과 ACK당 network/block write 값을 계산한다. Window에서 source/node 표본이 두 개 미만이면 resource summary는 invalid다. Block write가 0인 표본은 유효한 값이다. Owner의 network delta가 모두 0이면 `networkStatsAvailable=false`로 기록하고 suite network metric을 생략하지만 workload를 무효화하지 않는다. 시간 경계가 서로 다른 clock을 사용하므로 `clockSource`는 `stats=host clock, phases=runner container clock`이다.

NaruMQ `wal-summary.json`은 `narumq_raft_wal_sync_duration_seconds`, `narumq_raft_wal_sync_entries`, `narumq_raft_wal_written_bytes_total`의 window 시작/종료 scrape 차이를 node별·cluster 합계로 기록한다. 각 window의 전체 WAL series가 관측되고 성공한 Ready WAL fsync가 하나 이상이어야 valid다. `walBytesPerAckedMessage`는 해당 window publisher 교환이 완료되거나 timeout된 뒤 고정한 ACK 수를 분모로 한다. EMQX는 WAL 수치를 0으로 가장하지 않고 `applicability=not_applicable`, reason `EMQX regular sessions keep state in memory and write no replicated durable log`를 기록한다.

Rate sweep `[5, 10, 25]`에 대해 각 NaruMQ measure window의 WAL sync count/rate, entries-per-sync 분포와 mean, fsync duration p50/p99/p999, WAL bytes 및 ACK당 byte를 함께 보존한다. 관찰된 entries-per-sync 분포는 이 benchmark에서의 group commit 크기를 설명한다. 이 benchmark는 group-commit 설정 knob를 추가하지 않으며, smoke sweep으로 설정값이나 production threshold를 결정하지 않는다. Suite parent는 ACK latency p99/p999, delivery p99, ACK rate, ACK당 network/block-write 비용, offline reconnect drain 최대값, NaruMQ WAL sync rate/entries-per-sync/ACK당 byte의 중앙값과 MAD를 보고한다. NaruMQ와 EMQX ACK latency는 durability·replication·authentication 경계가 다르므로 직접 비교하지 않는다.

Runner unit tests는 collector 경계와 WAL histogram delta를 검증한다. `go test ./tests/benchmark/bp03 -count=1`은 profile, fixture, workload, resource/WAL parser, comparison 및 suite 계약을 확인한다. 격리된 Docker 실행 증거는 다음 명령으로 만든다.

```text
go run ./tests/benchmark/bp03 compare --profile tests/benchmark/bp03/testdata/smoke.json
go run ./tests/benchmark/bp03 suite --profile tests/benchmark/bp03/testdata/smoke.json
```

이 명령의 artifact는 수집 경계가 동작했다는 local smoke 증거다. Docker named volume 결과는 Linux/EBS production durability, 승인되지 않은 SPEC-TBD-08 수치 또는 release gate를 대신하지 않는다.

#### Local execution evidence

- `bp03-20260930T061549.390008900Z-28772`와 `bp03-20260930T063854.898438300Z-34520`에서 고정 smoke profile로 `compare`를 실행했다. 두 결과 모두 top-level `valid=false`; EMQX workload/resource는 valid, NaruMQ target은 invalid였다.
- 첫 실행은 NaruMQ step 1 schedule lag가 6.358초로 `generatorOverload=true`였고, node 3 broker log에 `dispatch MQTT PUBLISH: quorum unavailable`가 남았다. 두 번째는 schedule lag 8.965초, `generatorOverload=true`였으며 node 3 log에 `dispatch MQTT PUBREL: quorum unavailable`가 남았다. 각각의 runner가 q2-01 connection reset으로 종료됐다.
- 이 두 artifact는 실패 원인 진단만을 위한 local 실행 기록이다. 유효한 NaruMQ workload가 없어 ACK/WAL 비용을 비교 결과로 채택하지 않았으며, BP-03 smoke acceptance를 충족하지 않는다.
- 5회 suite `bp03-suite-20260930T064519.153992500Z-35844`는 고정 smoke profile로 요청된 5회 실행을 모두 마쳤지만, parent 결과는 `valid=false`, 성공 반복은 `0/5`, 집계 metric은 비어 있었다. NaruMQ는 다섯 회 모두 workload가 invalid였다. 각 회차의 step 1에서 `generatorOverload=true`와 protocol error 1건이 기록됐고, 최대 schedule lag는 2.914–5.913초였다. Runner는 네 회차에서 TCP reset, 한 회차에서 EOF로 종료됐으며, NaruMQ broker log에는 네 회차에서 `quorum unavailable`가 기록됐다. EMQX는 4/5회 유효했고, 첫 회차는 QoS 1 message invariant 실패로 invalid였다. 유효한 5회 suite가 없어 median/MAD와 target 비교 결과는 채택하지 않았다.
- `bp03-20260930T085025.956335900Z-23820`은 같은 고정 smoke profile (`profile SHA-256=0b8a88abac04fcfffeb72152cb86ae6aab4682808df7e4d43c9ee3df8661c92a`)로 실행했다. 시작 전 Docker에 실행 중인 container가 없었다. EMQX pinned image `emqx/emqx:5.8.8@sha256:4593b6e30196754779fa14483332cb0be56c233f1c8a387b1308bec4337c0144`는 runner가 CONNECT username `jwt`와 JWT credential을 전송한 상태에서 workload/resource가 valid였다. EMQX에는 authenticator를 설정하지 않았으며, 인증 경계 차이는 그대로다.
- 같은 run에서 NaruMQ workload는 invalid였다. Runner는 `subscriber q1-02 read: EOF`를 기록했고 node 3 broker log에는 `dispatch MQTT PUBLISH: quorum unavailable`가 남았다. 첫 measure step은 최대 schedule lag 8.307초와 `generatorOverload=true`를 기록했으며, QoS 1은 120 offered 중 109 ACK·71 delivery·38 gap, QoS 2는 120 offered 중 60 ACK·50 delivery·10 gap이었다. WAL summary에는 metric window가 없었다. 이 관찰만으로 두 log/event 사이의 인과관계를 주장하지 않는다.
- Top-level compare는 `valid=false`이며, 이 실행은 실패 진단 기록일 뿐 BP-03 수용 증거나 ACK/WAL 비교 결과로 채택하지 않았다. Single compare가 invalid였으므로 5회 suite를 실행하지 않았다.
- 소스 흐름으로 실패 경로는 좁힐 수 있지만 실패 단계는 구분할 수 없다. TCP `dispatchPublish` → durable `publish.UseCase.Publish`/`ReceiveQoS2` → `CommitDelivery` (5초 제한) → `clusterRuntime.Commit` 순서이며, `Commit`은 proposal을 보내기 전에 2초 제한의 fresh read barrier를 수행한다. `classifyClusterPortError`는 `ErrNotLeader`가 아닌 error를 모두 `quorum unavailable`로 바꾸고, `submitProposal`도 driver 중단이나 context 종료 때 같은 sentinel을 반환한다.
- Runner log에는 q1-02 EOF 시각이 없고, NaruMQ metric 파일은 step-01 시작 scrape뿐이다. Node 3 broker warning은 08:51:16.432에 기록되어 measure 종료 08:51:16.271보다 161 ms 늦다. Artifact만으로는 어느 `Commit` 단계가 실패했는지, EOF와 warning이 인과 관계인지 판별할 수 없다. Read-barrier timeout은 시간상 가능한 추정이지만 확인되지는 않았다. 이 관찰로 quorum 동작을 바꾸는 broker 수정은 정당화되지 않는다. 진단 계측은 `storage_failed`, `read_barrier`, `proposal`, `proposal_apply` 단계를 error 원인과 함께 기록하고, Raft leader/role/term/index 및 voter·learner와 transport peer 상태, storage 상태와 driver 종료 상태를 남긴다. Runner는 client별 최초 reader error에 UTC RFC3339Nano 시각을 붙인다. 이 계측은 proposal·read-barrier 동작이나 error 반환 계약을 바꾸지 않는다.
- 진단 계측 후 동일 smoke profile (`SHA-256=0b8a88abac04fcfffeb72152cb86ae6aab4682808df7e4d43c9ee3df8661c92a`)로 실행한 `bp03-20260930T103830.508843700Z-27552`도 top-level `valid=false`였다. 실행 전 `docker ps` 결과는 비어 있었다. EMQX workload/resource는 valid였고 NaruMQ는 invalid였다. `comparison.json`의 `equivalent=true`는 workload·resource·runner image·topology 일치만 뜻하며 durability, replication, authentication 경계가 같다는 뜻은 아니다. 이 compare가 invalid여서 5회 suite는 실행하지 않았다.
- NaruMQ step 1 measure(10:39:05.013–10:39:32.863 UTC)는 최대 schedule lag 14.100초와 `generatorOverload=true`를 기록했다. QoS 1/2 각각 120개가 전송·ACK·전달됐고 gap은 0이었다. Runner는 다음 warmup step 02의 settle 중 `2026-09-30T10:39:41.60414586Z`에 q1-02의 TCP reset으로 종료됐다. Step 2/3 measure는 시작하지 못했고 offline 요약은 QoS 1/2 모두 64 offered, 0 sent·delivered, sessionPresent 0/4였다. Resource/WAL 관찰도 step 1만 있으므로 이 실행의 ACK/WAL 비용은 채택하지 않는다.
- Node 3은 `10:39:38.5113769Z`와 `10:39:41.603979495Z`에 `command_kind=durable_delivery`, `commit_stage=read_barrier`, `error=context deadline exceeded`를 기록했다. 첫 경고 97.6 µs 뒤에는 `dispatch MQTT PUBACK: quorum unavailable`, 두 번째 경고 166 µs 뒤에는 runner의 q1-02 reader reset이 기록됐다. 두 node 3 snapshot 모두 leader 1, follower role 0, term 2였고 commit/applied index는 각각 1392와 1517, voter set은 1/2/3이었다. Peer 1/2 모두 connected였고 transport error는 없었으며, inbound/outbound 각 peer queue는 1 message 이하, storage는 HEALTHY, `driver_failed=false`, `driver_stopped=false`였다. 계측은 즉시 실패 지점을 2초 fresh read barrier timeout, proposal 이전으로 좁혔지만 barrier가 제한 시간 안에 끝나지 않은 원인은 증명하지 못한다. Generator overload와 invalid workload가 있었으므로 이 결과만으로 정상 부하의 broker 결함을 결론 내리거나 Raft/read-barrier 동작을 바꾸지 않는다.
- Smoke profile의 2-pair stream mapping에서는 q2-01 publisher와 q1-02 subscriber가 모두 node 3에 연결되지만 서로 다른 client ID·TCP connection이다. Node 3의 `dispatch MQTT PUBLISH` warning과 runner의 q1-02 reader EOF/reset은 같은 connection으로 입증되지 않았으며, node-level log에는 client identity가 없다. 이후 `read_barrier` warning과 q1-02 reset도 시간 순서만 확인됐고 인과관계는 입증되지 않았다.
- `cluster command commit failed` 진단은 failure stage·class·command kind별로 초당 최대 1건을 출력한다. `failure_count`, `failures_since_previous_log`, `first_failure_at`, `last_failure_at`로 반복을 요약한다. Proposal apply 원인은 `submitProposal` 경계에서 한 번 기록하며 `Commit`의 중복 warning은 제거했다. 로그에는 tenant/client ID나 command payload를 넣지 않는다.
- `bp03-20260930T123200.653059500Z-32492`은 profile SHA-256 `0b8a88abac04fcfffeb72152cb86ae6aab4682808df7e4d43c9ee3df8661c92a`로 실행했다. EMQX workload/resource는 valid였지만 NaruMQ workload는 invalid여서 top-level compare도 `valid=false`였다. NaruMQ step 1은 QoS 1/2 각각 120개 offered·sent·ACKed·delivered, gap/duplicate 0, max schedule lag 1.135 ms로 통과했다. Step 2는 max schedule lag 1.536 s와 `generatorOverload=true`였고 QoS 1은 240 offered/130 sent/126 ACK/123 delivered/gap 3, QoS 2는 240/114/109/101/gap 8이었다. Runner는 `2026-09-30T12:32:50.159113472Z`에 subscriber `q2-02`의 TCP reset으로 종료했다.
- 완주한 NaruMQ WAL window는 step 1의 12.018665 s뿐이다. `narumq_raft_wal_sync_duration_seconds_sum` delta/window로 계산한 node별 fsync occupancy는 node 1 40.56%, node 2 41.46%, node 3 42.01%였다. `meanEntriesPerSync`는 각각 0.505802, 0.505003, 0.505535 (cluster 0.505446)였고 sync count는 1,896/1,899/1,897이었다. Step 2 start scrape는 있으나 runner가 종료되어 end scrape가 없어 step 2 WAL metric은 계산할 수 없다.
- 세 broker log에는 workload 시작 전 `read_barrier`/`deadline` warning이 node 1 `12:32:23.624588685Z`, node 2 `12:32:24.419975580Z`, node 3 `12:32:25.214385020Z`에 하나씩 기록됐다. 이들은 `q2-02` reset보다 약 25초 앞서며 reset 시각에 대응하는 barrier warning은 artifact에서 관찰되지 않았다. 두 현상의 인과관계는 확인되지 않았다.
- NaruMQ workload가 invalid여서 conditional 5회 suite는 실행하지 않았다. 이 compare는 failure diagnosis 기록이며 BP-03 ACK/WAL 비교 증거가 아니다. Profile, workload, WAL scrape, logs는 위 run artifact에 보존했다.
- `ControlQueueCapacity: 256`과 `deliveryqueue: 256`, `deliveryRetryInterval: 100ms`, `writeDeliveries` 내 `nextControl()` 제어 패킷 플러시 적용 뒤 동일 smoke profile로 실행한 `bp03-20260930T145505.056290600Z-31628`은 Setup부터 Step 1, Step 2, Step 3, Offline, Reconnect, Cleanup까지 전체 워크로드를 끝까지 완주했다.
- Offline 구간은 QoS 1 64/64, QoS 2 64/64, sessionPresent 4/4(100%), gap/duplicate 0으로 통과했다.
- Step 1은 QoS 1/2 각각 120개 offered·sent·ACKed·delivered, gap 0으로 통과했다.
- Step 2는 480/480 전송·ACK·전달 완료(QoS 1/2 모두 gap 0)하여 메시지 전달 불변식을 만족했으나, Follower node 3에 연결된 `q2-01` publisher의 높은 ACK 지연(avg 447.1ms, max 942.6ms)으로 인해 max schedule lag 3.336s가 발생하여 `generatorOverload=true`였다 (node 1/2에 연결된 `q1-01`, `q1-02`, `q2-02`는 max schedule lag 1.1~1.2ms, avg 0.5~0.6ms 유지).
- Step 3는 1200/1200 전송·ACK 완료했고, QoS 1은 600/600 전달(gap 0)이었으나, `bp03-sub-q2-02`의 `durable delivery targets saturated` 경고가 28건 기록되며 QoS 2 572/600 전달(gap 28), max schedule lag 22.164s로 `generatorOverload=true`였다.
- 진단 메트릭과 로그 분석: `writeDeliveries` 내 `nextControl()` 소진 후 실행한 run `145505`에서는 Step 2의 QoS 2 gap이 0건으로 관측되었고 포화 경고도 발생하지 않았다. 회귀 시험 `server_qos_delivery_test.go::TestDeliveryWriterRetainsStateOnQueueAndWireErrors/queued_control_PUBREL_is_serviced_before_remaining_delivery_backlog`는 전달 백로그보다 앞서 대기 중인 제어 패킷(PUBREL)이 소켓으로 처리됨을 결정론적으로 검증한다. 이후 진행한 추가 진단 시도 `bp03-20260930T150424.465458500Z-37680`에서 락 해제 실험을 시도했으나 PacketID 경합 및 복구 불변식 안전성을 위해 이전의 안전한 락 구조로 완전히 되돌렸다. 복구 불변식을 위반하는 commit-only Ready fsync 생략 시도 역시 되돌렸으며, SSOT 규격에 따른 proposal 전 fresh read barrier는 그대로 유지한다.
- NaruMQ workload가 invalid여서 conditional 5회 suite는 실행하지 않았으며, BP-03은 미달성 상태다. 이 결과들은 `.artifacts/benchmark/bp03/bp03-20260930T145505.056290600Z-31628/` 및 `bp03-20260930T150424.465458500Z-37680/`에 보존했다.


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

BP-03 single-run artifact는 `.artifacts/benchmark/bp03/<run-id>/`에 보존한다.

- Root: `profile.json`, host/image/topology metadata가 있는 `manifest.json`, `comparison.json`, per-target validity와 `workload-summary.json`·`resource-summary.json`·`wal-summary.json`을 포함하는 `summary.json`.
- 각 `narumq/`, `emqx/` target은 `phases.jsonl`, `messages.jsonl`, workload/resource/WAL summary, runner log, broker log, secret 환경 값을 redaction한 Docker inspect JSON, broker/network-owner `*-stats.jsonl`을 보존한다.
- NaruMQ만 `metrics/nodeN-step-<ss>-{start,end}.prom`, `metrics/nodeN-offline-{start,end}.prom` 원시 scrape를 보존한다. EMQX WAL artifact는 `applicability=not_applicable`을 기록한다.
- Fixture JWT, management token, private key와 certificate는 run directory 밖의 임시 directory에만 두고 두 target 실행 뒤 제거한다. Network owner에는 공개 JWKS만 전달한다. Artifact에는 secret 값을 저장하지 않는다.

BP-03 local run은 Docker named-volume WAL 관측 증거만 제공한다. 이 artifact를 production filesystem/EBS, physical power-loss 또는 production RPO/RTO 증거로 표현해서는 안 된다.

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

### 7.1 Phase 1 MQTT client compatibility evidence

Phase 1 SDK interoperability는 일반 unit test와 분리된 Docker/E2E evidence gate다. 저장소 root에서 Go `1.26.5`와 Linux/amd64 container를 실행할 수 있는 Docker daemon을 준비한 뒤 다음 command를 사용한다.

```text
go run ./tests/compatibility/mqtt-clients-v1 \
  --run-id phase1-paho-go-python \
  --output .artifacts/compatibility/mqtt-clients-v1/phase1-paho-go-python
```

Command는 checked-in `tests/compatibility/mqtt-clients-v1/profile.json`을 strict decode하고 Eclipse Paho Go MQTT 3.1.1 `v1.5.1`, Eclipse Paho Go MQTT 5 `v0.23.0`, Eclipse Paho Python `paho-mqtt 2.1.0` image를 amd64로 build한다. Docker가 없거나 image architecture가 amd64가 아니면 case를 시작하지 않고 environment manifest와 실패 artifact를 남긴다.

성공 artifact는 `.artifacts/compatibility/mqtt-clients-v1/<run-id>/` 아래 `manifest.json`, `summary.json`, `cases.jsonl`, build log, image inspect metadata를 보존한다. 각 case는 broker log와 inspect, client stdout/stderr와 result JSON을 보존하고, QoS retransmission case는 proxy trace를 추가한다. Artifact에는 payload body, credential, raw certificate, 외부에서 주어진 Client ID와 secret을 기록해서는 안 된다.

Phase 1 compatibility acceptance는 Go 10개, Python 10개, cross-SDK QoS 1 4개와 MQTT 3.1.0 거부 2개, 총 26개 expected case를 검사한다. 이 Docker 명령은 `MQTT5_DRAIN_SAFE`, malformed server DISCONNECT 또는 Server Reference 자동 추종을 실행하지 않는다. Broker의 `DISCONNECT 0x9C`/Server Reference process acceptance는 [Drain spec §10](drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)에 별도로 기록하며, supported SDK 전체의 redirect/Will 호환성을 증명하지 않는다. 따라서 DEP-06/07/12와 `SPEC-TBD-04`는 release blocker로 남는다.

이 Docker/E2E gate는 외부 dependency가 없어야 하는 `go test ./...`와 `go run ./tools/verify local`에 포함되지 않는다. 후자의 검증은 nested SDK module을 다운로드하거나 Docker를 시작하지 않아야 하며, compatibility command는 release evidence가 필요한 환경에서 명시적으로 실행한다.

### 7.2 Phase 0 mutation·fuzz gate

현재 mutation gate는 critical state machine인 `internal/domain/delivery/state.go`,
`internal/domain/session/state.go`, `internal/domain/nodedrain`을 package별로
실행한다. 한 package의 결과로 다른 package의 점수를 상쇄하지 않는다.

`ci` 실행에서만 세 package mutation command를 수행한다. `local`은 전체 unit·integration
suite와 bounded fuzz를 실행해 빠른 개발 피드백을 제공하고, CI는 여기에 mutation gate를 더한다.
정책은 [`tests/verification/gremlins.yaml`](../../tests/verification/gremlins.yaml)에
둔다. 이 정책은 `workers=1`, `test-cpu=1`, `efficacy=80`, `mutant-coverage=80`을
고정하고 source·file·mutant exclusion을 사용하지 않는다. 각 package의 worker는
서로 격리된 임시 작업 디렉터리에서 독립 mutant를 검사한다. 각 package는 다음
command와 JSON artifact를 별도로 만든다.

```text
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-delivery.json internal/domain/delivery
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-nodedrain.json internal/domain/nodedrain
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-session.json internal/domain/session
```

Package별 판정식은 다음과 같다. `KILLED`는 test가 mutant를 잡은 수,
`LIVED`는 mutant가 살아남은 수, `NOT_COVERED`는 실행되지 않은 mutant 수다.

```text
test efficacy =
    KILLED / (KILLED + LIVED) × 100

mutant coverage =
    (KILLED + LIVED) /
    (KILLED + LIVED + NOT_COVERED) × 100
```

두 수치가 각 package에서 모두 80 이상이어야 한다. 한 package의 높은 점수로 다른
package의 실패를 상쇄하지 않는다. Equivalent mutant를 별도로 제외하지 않으며,
관찰 가능한 계약이 누락되어 `LIVED`가 발생하면 해당 package의 기존 state test에
정상·오류·경계 전이 assertion을 보강한다.

TEST-13의 표준 fuzz campaign은 다음 네 target을 각각 별도 process로 실행한다.
Local/CI runner는 네 process를 동시에 시작하되 각 process의 `-parallel=1`과
`-fuzztime=10000x` 고정 quota를 유지한다.

```text
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzDecoder$ -fuzztime=10000x -parallel=1
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzCodecRoundTrip$ -fuzztime=10000x -parallel=1
go test ./internal/domain/routing -run=^$ -fuzz=^FuzzTopicMatching$ -fuzztime=10000x -parallel=1
go test ./internal/domain/session -run=^$ -fuzz=^FuzzLifecycleTransitions$ -fuzztime=10000x -parallel=1
```

Go fuzz가 생성하는 입력의 발견 순서는 deterministic하지 않다. 따라서 gate의 반복
계약은 입력 순서가 아니라 네 target의 고정 quota와 실패 입력의 보존이다. 실패가
발견되면 해당 package의 `testdata/fuzz/<Target>/` corpus로 승격해 ordinary test와
후속 fuzz 실행에서 재현한다.

Feature와 bug fix는 실패를 재현하는 test를 포함해야 한다. Flaky test 격리는 임시 조치이며 owner와 해소 기한을 기록해야 한다.

Phase 0의 MQTT normative statement 연결표는
[`tests/conformance/mqtt-compliance-v1.json`](../../tests/conformance/mqtt-compliance-v1.json)이다.
이 파일은 지원 범위라고 선언한 statement, requirement ID와 자동 test function을 함께
기록한다. `go run ./tools/verify traceability`는 source comment와 matrix를 양방향으로
비교한다. 따라서 matrix에만 있는 test, source에만 있는 statement, requirement union이
다른 경우를 모두 실패로 처리한다.

### 7.3 Cluster failpoint build

Membership crash-boundary and durable-ACK acceptance tests use the
`narumq_failpoint` build tag. Only that build reads `NARUMQ_FAILPOINT` and
`NARUMQ_FAILPOINT_CONTROL`; the default build ignores both environment variables
and never exits or injects peer isolation because of them. A matching crash point
writes `narumq failpoint reached: <point>` to stderr and exits with code `86`.
Accepted values are:

```text
add-learner-before-persist
add-learner-after-persist
add-learner-after-apply
promote-voter-before-persist
promote-voter-after-persist
promote-voter-after-apply
remove-member-before-persist
remove-member-after-persist
remove-member-after-apply
snapshot-install-before-persist
snapshot-install-after-persist
begin-remove-after-apply
route-projection-rebuild-error
ownership-acquire-after-commit
delivery-wal-before-write
delivery-wal-before-sync
delivery-wal-after-sync
delivery-commit-before-apply
delivery-commit-after-apply
```

This build tag provides process-crash seams at membership, snapshot, ownership,
and durable-delivery boundaries; timing-based kills cannot identify those points
reliably. `NARUMQ_FAILPOINT_CONTROL` names a JSON file read synchronously while
the tagged runtime is active:

```json
{"crash":"delivery-wal-before-sync","isolatePeers":[2],"holdCommands":["subscription"]}
```

`crash` selects one registered point; `isolatePeers` lists peer node IDs to
isolate in both outbound sends and inbound peer-message handling. Removing the
file or making it empty disarms the control. Invalid JSON, crash/fault names, or
hold kinds are logged and disable the control for that read. `holdCommands`
accepts `ownership`, `subscription`, and `delivery`. A matching command pauses
after its read barrier and any applicable exact-profile check, before command encoding and
proposal submission. The tagged process writes
`narumq failpoint holding: <label>`, polls the control file until the kind is
removed or the file is cleared, and warns then proceeds after 60 seconds. On
release, the proposal uses a 10-second context derived from
`context.WithoutCancel`, so cancellation of the original caller while held does
not cancel the proposal. The result marker is
`narumq failpoint held result: <label> outcome=applied`,
`... outcome=conflict conflict=<code>`, or
`... outcome=error error=<message>`. This command hold is available only in
the failpoint build.

The Raft adapter exposes `PersistenceBoundaryHook`
for persistence tests and a separate `WALBoundaryHook` for non-empty normal
entries at WAL-before-write, before-sync and after-sync. That hook runs with the
node lock held and its entry bytes are borrowed only for the callback. The
composition root decodes those entries and limits these crash points to durable
`DeliveryActionPublish`/`DeliveryActionReceive`; adapter code remains independent
of command configuration. Existing `filePersistenceOps` and `onReadyEvent` hooks
remain in-package adapter seams. `ownership-acquire-after-commit` remains a
composition-root boundary after quorum commit and before CONNECT local activation.

### 7.4 Cluster storage chaos evidence

실제 ENOSPC, 느린 storage I/O와 sync/power-loss fault acceptance는 별도 Docker chaos gate다. Go `1.26.5`, Linux/amd64 Docker daemon, `/dev/fuse`, `CAP_SYS_ADMIN`과 image build에 필요한 module network access를 준비한 뒤 repository root에서 실행한다. 전원 차단은 journal로 sync되지 않은 FUSE 상태를 되돌리는 local simulation이며 물리 power loss나 EBS 검증이 아니다.

```text
go run ./tests/chaos/cluster-storage-v1 \
  --run-id storage-faults-20260925 \
  --output .artifacts/chaos/cluster-storage-v1/storage-faults-20260925
```

Runner는 새 Docker network와 세 broker process에서 여섯 scenario를 순서대로 실행한다: `follower-disk-full`, `quorum-disk-full`, `follower-slow-io`, `follower-sync-error`, `follower-torn-power-loss`, `cluster-power-loss`. Persistent `chaos-durable-sub` subscriber로 QoS 1/2 ACK와 sentinel delivery를 확인한다. Follower storage fault는 healthy quorum의 ACK/recovery를, quorum loss는 no-ACK와 QoS 2 same-packet resume·QoS 1 at-most-once를 확인한다. Sync-error는 follower fsync failure와 fail-closed reopen을 확인한다. Torn-power-loss는 held WAL fsync와 torn-tail rollback 뒤 delivery를 확인한다. Cluster-power-loss는 in-flight QoS 1 publisher 중 세 node cut을 동시에 시작하고 full restart 뒤 READY, leader/index convergence, persistent `SessionPresent` 및 ACKed delivery를 확인한다. `scenarioEvidence.durableAck`는 `ackedBeforeFault`, `ackedDuringFault`, `unackedDuringFault`, `ackedAfterRecovery`, `missingAcked`, `duplicates`, `recoverySessionPresent`를 기록한다. 이전 `storage-fault-20260925d`는 MQTT admission만 검사했으며 durable ACK 결과를 대체하지 않는다.

성공 결과는 `.artifacts/chaos/cluster-storage-v1/<run-id>/`의 `manifest.json`, `summary.json`, `docker-build.log`, `image-inspect.json`, scenario별 `events.jsonl`과 `node<i>.log`에 남긴다. Token·JWT·private key·certificate 원문은 artifact에 기록하지 않는다.

이 Docker/FUSE chaos gate는 외부 dependency가 없어야 하는 `go test ./...`와 `go run ./tools/verify local`에서 제외된다. 두 검증의 PASS만으로 실제 disk-full·slow-I/O 증거를 대체하지 않으며 chaos command를 별도로 실행해야 한다.

Durable ACK extension unit test:

```text
go test ./tests/chaos/cluster-storage-v1 -count=1
```

2026-09-27 Docker durable ACK run:

```text
GOMAXPROCS=2 go run ./tests/chaos/cluster-storage-v1 --run-id storage-fault-durable-20260927d --output .artifacts/chaos/cluster-storage-v1/storage-fault-durable-20260927d
```

PASS, exit code 0; `summary.json` reports all three scenarios passed. Follower disk-full and slow-I/O ACKed all six leader/healthy-follower QoS 1/2 topics during the fault; recovery delivered each payload once with no missing ACKed messages or duplicates. Quorum disk-full kept fault-time QoS 1/2 unacknowledged; recovery resumed persistent QoS 2 with `SessionPresent=true`, while QoS 1 remained at-most-once. Artifacts: `.artifacts/chaos/cluster-storage-v1/storage-fault-durable-20260927d/`. Earlier run attempts exposed Docker startup, node startup, and fault-handling ordering defects; the passing run occurred after those fixes. BP-03 and Linux/EBS/physical power-loss gates remain open.

2026-09-30 최종 여섯 scenario Docker gate:

```text
GOMAXPROCS=2 go run ./tests/chaos/cluster-storage-v1 --run-id storage-power-loss-20260930g --output .artifacts/chaos/cluster-storage-v1/storage-power-loss-20260930g
```

PASS, 종료 코드 0; 여섯 scenario 모두 통과했다. Sync-error 시나리오에서 `fail-fsync`를 유지한 채 broker를 재시작하자 process가 exit code 1로 fail-closed됐다. Normal mode 전환·restart 뒤 ACK된 QoS 1/2 일곱 topic이 전달됐다. Torn-WAL cut은 held fsync 1회, 실제 `node/wal.bin`의 unsynced 16 bytes 중 8 bytes를 보존했다. Cluster 시나리오에서는 세 broker 모두 exit 137, index 39→42 수렴, `SessionPresent=true`였다. 네 during-cut QoS 1 ACK와 cut 중 실제 PUBLISH write 후 `awaitAck` 전에 막아 둔 `qos1-05`의 `unknown` PUBACK outcome을 기록했다. `AckOutcomeUnknownDuringFault`에는 이 topic만 들고, known timeout/closed outcomes는 unacked로 남겼다. Cut 전·중 ACK된 여섯 topic에 누락·중복이 없었다. Unknown outcome은 commit되지 않았다는 뜻이 아니며 payload는 at-most-once로 검증했다. Artifact: `.artifacts/chaos/cluster-storage-v1/storage-power-loss-20260930g/summary.json`. Docker/FUSE simulation은 physical power-loss나 production EBS 증거가 아니다.

Mounted FUSE acceptance는 `/dev/fuse`, `CAP_SYS_ADMIN`, `apparmor=unconfined`를 설정한 pinned `golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd` container에서 실행했다. `tests/chaos/cluster-storage-v1/node`에서 `NARUMQ_CHAOSFS_MOUNT_TEST=1 go test ./... -count=1`가 통과했다. 이 시험은 file/directory durability, held WAL fsync·torn rollback, cut 시작 뒤 늦게 도착한 WAL fsync의 거부, file/directory sync failure를 확인한다. 같은 suite의 `TestChaosControllerRejectsHeldFsyncResumingAfterRemount`도 release→remount→late-resume에서 stale callback이 `EIO`로 거부되는지 확인했다.

## 8. Coverage와 test quality gate

- 전체 production code는 line 85% 이상, function 90% 이상, branch 85% 이상이어야 한다.
- protocol, session, routing, delivery, tenancy, security, cluster lifecycle과 node drain domain은 line/function 95% 이상, branch 90% 이상이어야 한다.
- PR 변경 code는 line/branch diff coverage 95% 이상이어야 한다.
- 전체 production line coverage 기준은 85%, function coverage 기준은 90%다. Core와 diff는 각각 별도 higher threshold를 유지한다.
- Generated source is excluded only when it is under `cmd/` or `internal/`, ends in `.pb.go`, and begins with the standard `// Code generated … DO NOT EDIT.` marker. Other unknown files in a coverage profile fail verification.
- `internal/domain/delivery`, `internal/domain/session`, `internal/domain/nodedrain`은 각각 test efficacy와 mutant coverage 80% 이상이어야 한다. 두 수치와 package별 판정은 합산하지 않는다.
- concurrency code는 race detector 또는 동등한 검사를 통과해야 한다.

현재 Phase 0의 자동 검증 표준 진입점은 다음 명령이다.

```text
go run ./tools/verify local
go run ./tools/verify ci --base <merge-base>
```

두 runner는 gofmt 사전 검사 뒤 `local` 10개, `ci` 13개 command를 실행한다. CI에만
delivery, nodedrain, session mutation command가 추가된다. Coverage profile을 사용하는 마지막
coverage 판정만 병렬 단계가 끝난 뒤 실행한다. 나머지 독립 command는 동시에 실행하며,
하나가 실패하면 나머지 process를 취소한다. 전체 test suite와 atomic coverage profile은
두 runner 모두 한 번의 `go test ./...`로 수행하며, test parallel limit은 2다. 명시적으로 병렬
실행하는 다섯 개 process acceptance test도 동시에 최대 두 개만 실행한다. race detector는 실제 production package
test (`./cmd/...`, `./internal/...`)에 적용한다. integration suite는 별도 child broker
process를 빌드해 실행하므로 test binary에만 적용되는 race detector가 해당 broker process를
검사하지 않는다.

```text
go vet ./...
go run ./tools/verify traceability
go test ./... -parallel=2 -count=1 -covermode=atomic -coverpkg=./cmd/...,./internal/... -coverprofile=.artifacts/verification/coverage.out
go test -race ./cmd/... ./internal/... -count=1
4 bounded fuzz campaigns (concurrent, each -parallel=1)
CI only: delivery + nodedrain + session mutation (concurrent, each workers=1, test-cpu=1)
go run ./tools/verify coverage ...
```


`ci`는 coverage command에만 `--base <merge-base>`를 정확히 한 번 전달한다. Mutation
command에는 diff base를 전달하지 않는다. coverage 원본과 요약은
`.artifacts/verification/coverage.out` 및
`.artifacts/verification/mutation-delivery.json`,
`.artifacts/verification/mutation-nodedrain.json`, `.artifacts/verification/mutation-session.json`에 남긴다. `ci`는 같은 검증에 더해
`base...HEAD`의 production 변경 line/function diff coverage를 확인한다.


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

`ratchet` 값은 명시적 policy baseline이며 실행 측정치로 자동 변경하지 않는다. 현재 전체 production line baseline은 85%, function baseline은 90%다. Core와 diff의 더 높은 기준은 별도로 유지한다. 기준 변경에는 반복 실행으로 안정성을 확인한 증거와 관련 요구사항·policy의 동시 갱신이 필요하다.

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
3. Coverage와 race gate, 네 bounded fuzz campaign, 그리고 `internal/domain/delivery`·`internal/domain/nodedrain`·`internal/domain/session`의 package별 mutation gate가 모두 통과했다. 각 mutation JSON은 `.artifacts/verification/`에 보존하고 두 점수가 각각 80% 이상이어야 한다.
4. BP-01~BP-07을 각 기능이 완성된 phase에서 실행했고 raw artifact가 있다.
5. 정상 3-node와 N-1에서 확정한 production profile SLO를 충족했다.
6. 단일 node kill에서 durable ACK message gap이 0이다.
7. `EKS_EC2_HA`에서 세 quorum Pod의 EC2·가용 영역·EBS 분리, 단일 가용 영역 장애와 NLB deregistration 결과를 검증했다.
8. 연속 100회 rolling update에서 persistent session 손실과 지원 client의 unexpected Will이 0이다.
9. tenant isolation과 관리 API RBAC penetration test를 통과했다.
10. backup/restore, rollback, Metric·alert·Dashboard를 production-like 환경에서 검증했다.
11. MQTT 3.1.0 inventory와 migration/격리 증거가 있다.

표준 runner가 생성한 `.artifacts/verification/coverage.out`,
`.artifacts/verification/coverage-summary.json`,
`.artifacts/verification/mutation-delivery.json`과
`.artifacts/verification/mutation-nodedrain.json`,
`.artifacts/verification/mutation-session.json`이 release evidence의 검증 원본이다.
Fuzz failure가 발생하면 해당 package의 `testdata/fuzz/<Target>/` corpus도 함께
보존해야 하며, quota를 채웠다는 사실만으로 보존된 failure를 삭제하거나
입력 순서를 deterministic하다고 간주해서는 안 된다.

증거가 없으면 완료가 아니다.
