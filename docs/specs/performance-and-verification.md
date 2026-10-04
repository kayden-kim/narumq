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
- 2026-10-01 사용자 요청으로 BP-03 comparator만 pinned EMQX 5.8.8 Enterprise의 3-node static cluster로 전환한다. Image index digest는 `sha256:d166efa91eae706ffbfca3f47e36fae0fd441507a8a57bda622da09e4a019c14`다. `durable_sessions.enable=true`, `durable_storage.messages.backend=builtin_raft`, `n_sites=3`, `replication_factor=3`, `n_shards=1`을 고정한다. 하나의 message shard는 NaruMQ의 단일 Raft group에 가깝게 맞춘 선택이며 session metadata의 복제 방식까지 동일하다는 뜻은 아니다. BP-01/BP-05의 기존 OSS comparator는 변경하지 않는다. 기본 evaluation license는 최대 25 concurrent sessions, 비운영 평가용이며 commercial license·운영 사용 승인을 뜻하지 않는다.
- 두 target은 profile에 지정된 broker CPU/memory limit, runner CPU/memory limit, payload, stream, rate와 phase를 공유한다. Broker는 read-only root filesystem에서 실행하고, runner는 별도 container를 사용한다. 각 broker container는 자체 network namespace owner와 Docker bridge network를 공유한다.
- NaruMQ fixture는 MQTT CONNECT에서 JWKS를 통해 RS256 JWT를 검증하고 peer 간에는 mTLS를 사용한다. Broker별 node config, private peer key, management token은 해당 broker에만 mount한다. Runner credential은 별도로 제공하며, network owner에는 `jwks.json`만 포함된 read-only directory를 mount한다.
- EMQX도 동일한 runner JWT를 JWKS로 검증하고 issuer·audience·subject를 검사한다. File ACL은 `bp03/#` publish/subscribe만 허용한다. Broker에는 별도 EMQX config directory만 mount해 NaruMQ private key·admin token을 노출하지 않는다.
- 복제된 durable message storage는 양쪽 모두 3-node로 맞추지만 EMQX의 메시지 batch·session metadata 저장, ACK와 fsync의 정확한 순서, NaruMQ의 tenant/client-ID binding과 peer mTLS는 같은 계약으로 검증되지 않았다. 비교 산출물은 남은 차이를 `nonEquivalent`로 기록하고 ACK latency를 직접적인 우열 비교에서 제외한다. `durable_sessions.enable`만 켠 것을 quorum 영속화 동등성의 증거로 취급하지 않는다.
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

1. `readiness`: NaruMQ 세 node의 health, cluster ID와 같은 leader를 확인한다. EMQX는 최초 부팅 로그만으로 준비 완료를 판단하지 않는다. Runner 시작 전에 pinned 5.8.8의 `emqx eval`로 세 node 모두 `emqx:is_running()`, 기대한 세 node의 `emqx:running_nodes()`, 설치된 authorization hook을 조회하는 `emqx_authz:lookup()`을 확인한다. 추가로 실제 Durable Storage가 `builtin_raft`이고 하나의 message shard에 세 site의 replica가 배치됐으며 replica transition이 없는지 확인한다. 유효 JWT authenticator도 설치돼 있어야 한다. 초기화가 끝나지 않으면 기존 startup deadline 안에서 기다리며 일반 memory session으로 fallback하지 않는다. 확인 결과를 node별 artifact에 저장한다. 두 target 모두 이후 6개 node 쌍의 cross-node QoS 1 probe publish와 delivery를 확인한다.
2. `setup`: persistent subscriber를 구독시키고 publisher를 연결한다. Subscriber는 `CleanStart=true`와 profile Session Expiry, publisher는 `CleanStart=true`와 Session Expiry 0을 사용한다.
3. 각 rate에서 `step-<ss>-warmup` 뒤 `step-<ss>-measure`를 실행한다. Runner는 절대 시각 schedule과 최대 inflight publish 수를 적용한다. QoS 1은 PUBACK, QoS 2는 PUBCOMP 수신 뒤 inflight slot을 해제한다. 측정 전 warm-up 교환이 완료되거나 timeout될 때까지 기다린다. NaruMQ는 각 measure 경계에서 `/metrics`를 scrape하며, end scrape 전에 publisher 교환이 완료되거나 timeout되도록 기다리고 해당 window의 ACK 분모를 그 시점에 고정한다.
4. `drain`: 새 publish를 멈추고 drain 한도 안에 ACK된 measure 메시지가 수신될 때까지 기다린다.
5. `offline`: subscriber를 정상 disconnect하고 settle 시간 뒤 stream마다 QoS 1/2 메시지 32개를 publish한다. End scrape 전에 publisher 교환이 완료되거나 timeout될 때까지 기다린다. NaruMQ는 이 구간의 WAL metric도 scrape한다.
6. `reconnect`: 하나의 deadline 안에 각 persistent subscriber를 다른 node로 재접속하고 offline delivery를 기다린다. 연결 시도도 같은 deadline에 포함한다. `SessionPresent=false`는 실패이며, 예상한 모든 offline receipt와 publisher ACK, QoS 2 PUBCOMP가 끝나기 전에 연결을 정리하지 않는다. Subscriber의 QoS 2 수신 상태는 socket이 아니라 persistent session에 속한다. Disconnect 때 reader 종료를 기다린 뒤 미완료 PUBLISH와 완료된 PUBREL의 Packet ID 상태를 새 연결에 넘긴다. 따라서 세션 재개 직후 받은 기존 PUBREL에는 PUBCOMP를 반환하되 delivery를 다시 집계하지 않는다. 알 수 없는 Packet ID와 잘못된 packet은 기존대로 protocol error다.
7. `cleanup`: reader를 멱등하게 중지하고 DISCONNECT를 보낸 뒤 socket을 닫으며, reader가 종료된 후 raw message/phase 기록과 요약을 저장한다.

`workload-summary.json`은 각 measure step 및 offline QoS별 offered/sent/acked/delivered/gap/duplicate, ACK 및 publish-to-delivery latency와 rate를 기록한다. `gap`은 성공 ACK를 받았지만 drain 기한까지 수신하지 못한 메시지 수다. QoS 1 duplicate는 보고하지만 at-least-once 규칙상 무효화하지 않는다. QoS 2 duplicate, protocol error, session 미복원, generator overload, timeout·rejected ACK, 불완전 교환 또는 gap은 workload를 invalid로 만든다. QoS 2 교환은 PUBCOMP에서 완료된다. PUBREC 뒤 발생한 timeout도 timeout으로 집계하며 해당 교환은 미완료 상태로 남는다. `generatorOverload`는 기록한 publish의 `schedule lag >= stream interval`일 때 설정되며 CPU utilization 지표가 아니다. 이 값만으로 runner CPU 포화와 broker/ACK backpressure를 구분하거나 quorum failure의 원인을 증명할 수 없다.

Reader 실패가 workload context를 취소하면 최종 오류에 최초 reader 원인도 보존한다. 뒤따른 DNS·dial 취소만으로 원인을 판단하지 않는다. `tests/benchmark/bp03.TestSubscriberReaderResumesQoS2Release`는 실제 TCP 연결을 바꾼 뒤 미완료 교환과 PUBCOMP 재전송의 Packet ID 처리 및 exactly-once 집계를 확인한다. 수정 전 두 경우 모두 PUBCOMP를 받지 못해 FAIL했다. 이 결함 재현은 기존 BP-03 reconnect 실패의 최초 packet을 확인한 증거와 구분한다.

`resource-summary.json`은 runner phase의 시작·종료 시각으로 broker 및 network-owner Docker stats를 자른다. 각 measure/offline window와 node에서 broker CPU median, memory 최대값, block read/write delta 및 owner network Rx/Tx delta를 기록하고 cluster total과 ACK당 network/block write 값을 계산한다. Window에서 source/node 표본이 두 개 미만이면 resource summary는 invalid다. Block write가 0인 표본은 유효한 값이다. Owner의 network delta가 모두 0이면 `networkStatsAvailable=false`로 기록하고 suite network metric을 생략하지만 workload를 무효화하지 않는다. 시간 경계가 서로 다른 clock을 사용하므로 `clockSource`는 `stats=host clock, phases=runner container clock`이다.

NaruMQ `wal-summary.json`은 `narumq_raft_wal_sync_duration_seconds`, `narumq_raft_wal_sync_entries`, `narumq_raft_wal_written_bytes_total`의 window 시작/종료 scrape 차이를 node별·cluster 합계로 기록한다. 각 window의 전체 WAL series가 관측되고 성공한 Ready WAL fsync가 하나 이상이어야 valid다. `walBytesPerAckedMessage`는 해당 window publisher 교환이 완료되거나 timeout된 뒤 고정한 ACK 수를 분모로 한다. EMQX는 replicated Durable Storage를 사용하지만 같은 NaruMQ WAL metric을 제공하지 않으므로 WAL 수치를 0으로 가장하지 않고 `applicability=not_applicable`, reason `EMQX Durable Storage does not expose the NaruMQ Ready WAL metrics collected by this harness`를 기록한다. 이는 EMQX가 WAL을 쓰지 않는다는 뜻이 아니다.

Rate sweep `[5, 10, 25]`에 대해 각 NaruMQ measure window의 WAL sync count/rate, entries-per-sync 분포와 mean, fsync duration p50/p99/p999, WAL bytes 및 ACK당 byte를 함께 보존한다. 관찰된 entries-per-sync 분포는 이 benchmark에서의 group commit 크기를 설명한다. 이 benchmark는 group-commit 설정 knob를 추가하지 않으며, smoke sweep으로 설정값이나 production threshold를 결정하지 않는다. Suite parent는 ACK latency p99/p999, delivery p99, ACK rate, ACK당 network/block-write 비용, offline reconnect drain 최대값, NaruMQ WAL sync rate/entries-per-sync/ACK당 byte의 중앙값과 MAD를 보고한다. NaruMQ와 EMQX ACK latency는 durability·replication·authentication 경계가 다르므로 직접 비교하지 않는다.

##### Raft 병목 진단

BP-03 개선은 peer queue 포화와 WAL 지연 사이의 원인을 분리한 뒤 진행한다. 높은 `fsync` 점유율만으로 queue 포화의 원인을 확정하지 않는다. 고정 workload·자원·ACK 경계는 그대로 유지한다.

- NaruMQ는 `narumq_raft_stage_duration_seconds{stage}`에 `inbound_queue`(인증된 frame enqueue부터 driver 인수), `commit_queue`(commit 제출 시도부터 발행 제어를 통과한 driver request 처리 시작), `ready`(동기식 Ready 처리), `apply`(application 적용), `send`(transport enqueue), `read_index`(driver ReadIndex 시작부터 로컬 apply/projection 확인 또는 실패), `proposal`(proposal 시작부터 application 적용·projection 완료 응답 또는 driver 실패)의 시간을 초 단위 histogram으로 기록한다.
- Stage label은 위 고정 목록만 사용한다. Client·tenant·payload를 label에 넣지 않는다. 완료된 작업만 duration에 포함하며 실패도 포함한다. Proposal의 client timeout과 이후 실제 apply는 서로 다른 사건이므로 stage histogram만으로 성공 ACK를 판정하지 않는다. `commit_queue`에는 channel 제출 대기와 인수한 FIFO head의 발행 제어 대기도 포함한다.
- Runner는 readiness 뒤부터 정리 직전까지 1초 간격으로 node별 `/metrics` 원문과 요청 시작·종료 UTC 시각을 보존한다. 실패 종료에도 최종 scrape를 시도하고 scrape 실패를 기록한다. 이 진단 artifact는 measure window의 WAL summary와 분리하며, 실패 run을 유효한 성능 결과로 바꾸지 않는다.
- 원인 판단은 같은 시간 구간의 stage histogram 증가량, WAL sync 시간, broker의 peer 포화·deadline 로그를 대조한다. 계측 결과가 지목한 경로만 개선하며 fresh proof, commit-only HardState sync, Packet ID·epoch fencing은 생략하지 않는다.

계측으로 WAL 대기와 다수의 peer frame이 확인되면 이미 fresh proof를 완료한 FIFO command를 최대 64개·64 KiB 수집 예산으로 하나의 Raft `MsgProp`에 묶을 수 있다. 각 command는 독립 log entry와 apply 결과를 유지한다. 64 KiB를 넘는 단일 command는 기존 단건 경로를 사용하며, 여러 command를 담는 forwarded frame은 adapter의 1 MiB wire 한도를 넘지 않는다. 새 proof의 공유·cache, ACK 선반환, command schema 변경, HardState sync 생략은 이 개선에 포함하지 않는다. Batch 거절은 모든 소속 command에 오류를 전달하며 자동 재제안하지 않는다.

시험 연결: `internal/adapters/metrics/registry_test.go::TestRaftStageHistogramsKeepStagesIsolatedAndBoundOverflow`는 stage 분리와 bucket overflow를, `internal/adapters/raft/node_dynamic_test.go::TestDynamicFollowerForwardsApplicationProposal`는 batch의 입력 copy·전체 거절·entry 순서와 WAL 재개방 복구를, `cmd/narumq/cluster_runtime_test.go::TestClusterCommitReadProofPreservesSubmissionOrderAndCancellation`은 미완료 proof·projection·취소가 batch 제출을 막는 조건을 확인한다. 실제 scrape 보존과 workload 결과는 아래 실행 증거에 연결한다.

2026-10-02 사용자 지시 `개선 진행해`로 dynamic driver의 fresh ReadIndex 발행 제어를 채택한다. Peer별 outbound count·byte 여유에서 control 진행용 두 frame·512 byte를 제외하고, read 한 건의 heartbeat·proof response 두 frame 비용과 같은 Ready 전에 이미 발행한 read를 계산한다. 여러 node가 동시에 발행하면 leader의 같은 FIFO를 쓰므로 현재 voter·learner 수로 예산을 나눈다. 분모는 1–15로 제한해 큰 cluster에서도 최소 한 요청의 진행을 허용하며, 이는 전체 incoming frame의 무손실 보장이 아니다. 빈 queue의 3-voter cluster에서 node별 발행 중인 local proof는 최대 5개다. Follower는 현재 leader의 여유를 사용하고, leader는 local vote와 함께 quorum을 구성할 만큼의 voter가 수용할 수 있는 예산을 사용한다. 느린 minority peer 하나가 healthy quorum 전체를 막지 않는다. 초과 요청은 기존 bounded request channel과 driver가 인수한 단일 FIFO head에서 기다린다. Head의 원래 context·deadline을 유지하고 취소·driver 종료 때 결과를 반환한다.

Driver는 outbound write 완료의 합쳐진 알림에서 예산을 다시 계산하며, 기다리는 동안 inbound 응답과 tick·Ready·apply를 계속 처리한다. Fan-out을 만드는 inbound ReadIndex request 뒤에는 추가 inbound staging을 멈추고 먼저 Ready를 처리한다. Replication·heartbeat response는 기존 64개 공용 staging 예산에서 모아 WAL 기록을 불필요하게 나누지 않으며 timed proposal 수집은 control에서 끝낸다. Ready 전송 뒤에는 Go scheduler에 실행 기회를 양보해 transport writer가 queue를 비울 수 있게 한다. 이는 특정 peer 완료를 기다리거나 timeout을 늘리는 동작이 아니다. Queue 상한·outbound full 거절·peer FIFO·command별 fresh proof·HardState sync·MQTT ACK·readiness 계약은 그대로다. 예산은 새 local 요청의 생성 제어이며 transport 예약이나 모든 Raft frame의 무손실 보장이 아니다. Incoming forwarded ReadIndex와 replication이 만드는 전송량은 실제 runtime·BP-03의 포화·stage/WAL 시간과 함께 검증한다. `AsyncStorageWrites`는 이번 범위가 아니며 flag 전환으로 구현하지 않는다.

수용 시험: `cmd/narumq/cluster_read_admission_test.go::TestClusterFollowerReadBurstCompletesAfterEgressDrain`은 실제 3-node Raft·mTLS TCP와 정지한 초기 writer에서 follower 요청 64개의 독립 fresh proof 완료와 outbound 상한·거절 부재를 확인한다. 수정 전 32/64 완료·32개 미완료로 FAIL했다. 이 재현은 제한된 egress에서의 결함 증거이며 기존 BP-03 실패의 인과나 성능 향상을 증명하지 않는다. 변경 후 실행 결과와 고정 compare·조건부 반복 suite·안전성 검증은 아래 실행 증거에 연결한다.

추가 시험 연결: `TestClusterLeaderReadBurstPreservesHealthyQuorumWithBlockedMinority`는 세 번째 peer의 writer를 시작하지 않은 실제 TCP 조건에서 leader의 64개 fresh proof가 healthy quorum으로 완료되는지 확인한다. `TestClusterUnissuedReadReturnsCancellationAndDriverFailure`는 count·byte 부족에서 아직 발행하지 않은 head의 취소·driver 종료 결과와 소유권 해제를, `internal/adapters/tcp.TestDynamicOutboundCapacityRemainsChargedUntilWireCompletion`은 진행 중인 socket write의 count·byte 유지와 wire 완료 후 wake·회수를 확인한다. `tests/integration.TestDynamicClusterConcurrentFreshReadProcessAcceptance`는 실제 세 broker process에서 follower의 concurrent summary 64개가 각 fresh quorum 결과를 반환하는지 확인한다.

##### 2026-10-02 fresh ReadIndex 발행 제어 — 부분 실행 증거

수정 전 실제 TCP follower burst는 독립 proof 64개 중 32개만 완료하고 32개가 deadline까지 남아 FAIL했다. 발행 제어와 응답 비용·writer scheduling을 반영한 뒤 follower burst, blocked minority, 미발행 head의 cancellation/driver failure, in-progress outbound 회수 시험은 PASS했다. 실제 세 broker process에서 follower summary 64개는 각 요청의 fresh quorum proof를 확인하고 18.5784ms에 모두 완료했다(`TestDynamicClusterConcurrentFreshReadProcessAcceptance`, package 11.675s). 이는 해당 bounded burst 수용 증거이며 고정 BP-03를 대신하지 않는다.

같은 고정 smoke profile로 실행한 두 compare는 EMQX valid / NaruMQ invalid였다.

| Run ID | 관찰 결과 | 판정 경계 |
| --- | --- | --- |
| `bp03-20261002T125831.714550600Z-9640` | Step 1 QoS별 120 sent/ACK/delivery 일치. Step 2는 QoS 1 77/69/36, QoS 2 104/103/65로 중단했고 generator overload가 기록됐다. | Workload 중 outbound 포화 로그는 없었다. MQTT PUBLISH 연결 종료의 첫 원인을 확인하지 못했으므로 driver 수집 방식과의 인과를 주장하지 않는다. |
| `bp03-20261002T131133.804863100Z-35944` | ReadIndex request만 inbound 수집을 끝내도록 좁힌 뒤 Step 1·2·3 QoS별 120/240/600 sent/ACK/delivery 일치, gap·duplicate 0, overload 없음. Offline QoS 1 64/64/64, QoS 2 64/64/0, sessionPresent 3/4, protocolErrors 1. | 마지막 오류는 `q2-02` 재접속 DNS lookup의 context 취소다. Workload 중 peer 포화·readiness 거절·deadline 로그는 없었으며 DNS나 CONNACK 거절을 근본 원인으로 확정하지 않는다. |

두 실행의 artifact는 `.artifacts/benchmark/bp03/<run-id>/`에 보존했다. 두 번째 run의 Step 3 ACK p99는 QoS 1 103.370073ms, QoS 2 108.084611ms였지만 invalid run의 부분 관찰이다. Conditional 5회 suite나 BP-03 합격 근거로 채택하지 않는다.

이후 runner의 QoS 2 수신 상태가 socket마다 초기화되는 결함을 실제 재접속 fixture로 재현했다. 미완료 PUBREL과 완료 교환의 재전송 두 경우 모두 PUBCOMP 대기 timeout으로 FAIL했고, session 상태 보존 뒤 PASS했다. 이 수정은 unknown Packet ID 거절·exactly-once 집계와 profile·ACK 경계를 유지한다. 최초 reader 취소 원인도 최종 오류에 보존한다. 전체 focused package 실행 `go test ./cmd/narumq ./internal/adapters/raft ./internal/adapters/tcp ./tests/benchmark/bp03 -count=1`은 PASS했다(14.069/2.044/10.279/0.509s).

Runner session 수정 뒤 compare `bp03-20261002T132456.532540300Z-2904`도 EMQX valid / NaruMQ invalid였다(203.66s wall). Step 1·2의 count invariant는 유지됐지만 Step 3 QoS 1 sent/ACK/delivery는 257/249/123(gap 126), QoS 2는 373/371/241(gap 130)이었고 offline·reconnect는 수행하지 못했다. Generator overload·protocol error는 없었다. 13:26:01.832 UTC의 leader node 1→2 `MsgReadIndexResp` outbound 32-message 포화 뒤 durable target saturation이 기록됐고, 최초 reader 오류는 13:26:06.804 UTC `q2-02` subscriber EOF로 보존됐다. Local origin당 15개 제한만으로는 여러 origin의 fan-out을 충분히 제어하지 못했으므로 voter·learner 수에 예산을 나누는 위 규칙으로 보강한다. 이는 session 수정으로 BP-03가 해결됐다는 증거가 아니다.

`TestClusterConcurrentOriginsReadBurstCompletesAfterEgressDrain`는 실제 세 TCP node가 동시에 64개씩 발행할 때 독립 proof 192개 완료와 healthy-peer 무거절·queue 상한을 확인한다. 기존 15개 제한에서도 이 unrestricted-CPU fixture는 PASS(0.19s)했으므로 BP-03의 1-CPU 응답 포화를 재현한 시험이라고 주장하지 않는다. 부하 profile·자원·proof·ACK 계약은 변경하지 않는다.

Origin 수에 따른 예산 분할 적용 뒤 실제 TCP follower·blocked minority·동시 origin burst와 취소/driver 종료 시험은 PASS했다(각 0.20/0.20/0.20/0.26s). 동시 origin의 192개 독립 proof를 기존 2초 deadline 안에 완료했고 healthy peer의 거절은 없었다. 실제 세 broker process의 follower summary 64개도 fresh quorum proof와 함께 17.314ms에 완료했다(`tests/integration` 6.117s). `GOMAXPROCS=4`로 두 package의 해당 시험을 실행했으며 이 결과는 제한된 container CPU의 BP-03 수용과 구분한다.

예산 분할 뒤 고정 compare `bp03-20261002T133437.979734800Z-36656`는 전체 workload를 완주했다(211.31s wall). Step 1·2·3 QoS별 sent/ACK/delivery는 각각 120/240/600으로 일치했고, offline QoS 1/2도 각각 64/64/64였다. Gap·duplicate·protocol error는 0, `SessionPresent=4/4`, reconnect drain 최대 475.524832ms였다. Workload 중 peer 포화·readiness 거절·deadline 로그는 없었다. 그러나 Step 3 최대 schedule lag는 3.809644755s로 `generatorOverload=true`였고 전체 NaruMQ workload는 **invalid**였다. EMQX는 valid였다. 해당 run의 유일한 workload failure reason은 `step 03 generator overload`이며, 유효 run을 고르는 재실행이나 조건부 5회 suite는 하지 않는다.

Step 3 ACK p99는 QoS 1 880.258472ms, QoS 2 116.766664ms였다. 같은 measure 경계 scrape 증가량에서 node 1/2/3 Ready 시간은 12.600190/12.763175/12.722284초, WAL fsync는 12.179688/12.362633/12.315901초였다. Fsync가 Ready 시간의 96.66/96.86/96.81%를 차지했다. 완료된 commit queue 표본의 평균은 4.579/56.919/8.678ms였고 admission 제어로 보류한 시간도 포함한다. 이 합계는 병렬 요청의 관찰값이므로 서로 더해 wall time이나 단일 ACK의 지연으로 해석하지 않는다. Queue 포화 없는 완주는 확인됐으나 schedule lag와 synchronous WAL 점유의 인과·개선량은 확정하지 않았다. `AsyncStorageWrites` adapter 재설계나 추가 성능 정책은 이번 채택 범위 밖이다.

최신 코드의 matched-target durable ACK fault process suite는 PASS(`narumq/tests/integration` 158.343s; 159.65s wall)였다. Replica kill 여섯 조합 31.34s, WAL/commit crash 20개 조합 100.74s, leader/follower partition 두 scenario 26.11s에서 ACK·복구 경계를 유지했다. 이어진 Docker/FUSE `storage-readcredit-20261002`는 exit 0, 여섯 scenario 모두 PASS(248.43s wall)였다. Slow-I/O에서 healthy quorum progress 55/55, recovery 12.198s, applied index 373 수렴과 최종 ACKed delivery를 확인했다. 모든 `missingAcked`·`duplicates`는 비어 있고 quorum disk-full 중 성공 ACK는 없었다. 상세 evidence는 [Cluster §11.6·§11.13](cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)에 연결한다. 이는 physical power-loss나 유효한 BP-03 성능 수용의 증거가 아니다.

최종 `GOMAXPROCS=4 go run ./tools/verify local` PASS(299.58s wall; integration 262.907s)였다. Traceability, 네 fuzz target 각각 10,000회, architecture, package·integration 시험, package race와 coverage를 통과했다. Runner의 재접속 PUBREL/PUBCOMP 회귀와 unknown Packet ID의 protocol-error 종료도 포함한다. 최종 문서 증거는 이 gate 결과와 별도로 연결을 검증한다. BP-03 invalid·조건부 5회 suite 미실행·production storage 미수용 상태는 바꾸지 않는다.



Runner unit tests는 collector 경계와 WAL histogram delta를 검증한다. `go test ./tests/benchmark/bp03 -count=1`은 profile, fixture, workload, resource/WAL parser, comparison 및 suite 계약을 확인한다. 격리된 Docker 실행 증거는 다음 명령으로 만든다.

```text
go run ./tests/benchmark/bp03 compare --profile tests/benchmark/bp03/testdata/smoke.json
go run ./tests/benchmark/bp03 suite --profile tests/benchmark/bp03/testdata/smoke.json
```

이 명령의 artifact는 수집 경계가 동작했다는 local smoke 증거다. Docker named volume 결과는 Linux/EBS production durability, 승인되지 않은 SPEC-TBD-08 수치 또는 release gate를 대신하지 않는다.

#### Local execution evidence

##### 2026-10-02 계측과 proposal batching — 수용 미완료

같은 고정 smoke profile (`SHA-256=0b8a88abac04fcfffeb72152cb86ae6aab4682808df7e4d43c9ee3df8661c92a`)로 아래 두 `compare`를 실행했다. 두 run 모두 EMQX target은 valid, NaruMQ target은 invalid다. ACK 경계는 계속 직접 비교할 수 없다. 아래 수치는 실패 run의 부분 관찰이며 성능 개선의 확정 근거, 유효 benchmark 또는 BP-03 완료 증거로 채택하지 않는다.

| 실행 | 관찰 | 진단 artifact |
| --- | --- | --- |
| `bp03-20261002T004857.831093300Z-6908` — 계측 baseline | Step 1·2는 QoS별 sent/ACK/delivery 120/120/120 및 240/240/240, gap·duplicate 0, generator overload 없음. Step 3은 warmup에서 종료해 measure sent 0. | `narumq/diagnostics.jsonl` 108건과 node별 원문 scrape; scrape error 0. |
| `bp03-20261002T005736.207420300Z-19400` — proposal batching | Step 1·2의 같은 count invariant는 유지했으나 Step 3은 invalid. QoS 1 offered/sent/ACK/delivery = 600/518/510/419, gap 91; QoS 2 = 600/600/600/507, gap 93. 두 QoS duplicate 0, generator overload 없음. Offline·reconnect 수용은 완료하지 못함. | `narumq/diagnostics.jsonl` 138건과 node별 원문 scrape; scrape error 0. |

Baseline Step 2의 node별 첫·마지막 진단 scrape(00:49:44.160–00:49:55.167 UTC, 약 11초) 증가량에서 Ready 처리 합계는 9.026/9.598/9.723초, WAL fsync 합계는 8.693/9.270/9.405초였다. WAL fsync는 Ready 처리 시간의 96.32/96.59/96.73%를 차지했다. Application apply는 0.141/0.142/0.136초, transport enqueue는 0.057/0.051/0.051초였다. 이는 동기식 WAL 대기가 driver 시간의 대부분이라는 관찰이지 queue 포화의 단일 원인을 증명하지 않는다. Stage 합계는 여러 요청의 시간이 겹칠 수 있으므로 서로 더해 wall time으로 해석하지 않는다.

Batching run은 Step 3 warmup 뒤 measure를 시작했지만 node 1 outbound 포화(00:58:37.073), inbound 포화로 peer 2 연결 종료(00:58:44.255), durable delivery 포화와 read/proposal deadline, subscriber EOF 뒤 중단됐다. 부분 Step 2의 QoS 1 ACK p99는 82.662→73.308 ms, delivery p99는 160.159→129.527 ms였고 QoS 2는 각각 79.266→77.731 ms, 188.248→159.472 ms였다. 각 변경당 한 번의 invalid 실행이므로 이 차이를 재현 가능한 개선율로 주장하지 않는다.

Artifact root는 `.artifacts/benchmark/bp03/<run-id>/`다. 실제 실패와 timing 증거는 `comparison.json`, `narumq/workload-summary.json`, `narumq/diagnostics.jsonl`, `narumq/metrics/`, node별 log에 보존했다. 두 compare가 invalid여서 조건부 5회 suite는 실행하지 않았다. **BP-03와 ADR-0003 성능 수용은 미완료**로 유지한다. 이 두 실행 당시 inbound 포화는 연결을 닫는 기존 정책이었다. 이후 사용자가 `제한된 수신 대기 채택`을 명시적으로 선택해 [v2 경계 계약](../contracts/raft-membership-v2.md)의 적용 범위를 dynamic Raft inbound에 한정해 갱신했다. 새 정책의 실행 결과는 별도 후속 증거로 구분한다.

계측·batching의 focused package 시험은 `go test ./internal/adapters/raft ./cmd/narumq ./internal/adapters/metrics ./tests/benchmark/bp03 -count=1` PASS였다(각 1.943/12.501/0.350/0.439s). Dynamic follower batch의 입력 copy·전체 거절·순서·WAL 복구와 FIFO proof/cancellation 회귀를 포함한다. 실제 durable ACK fault 시험 `GOMAXPROCS=4 NARUMQ_DURABLE_ACK_FAULTS=1 go test ./tests/integration -run 'TestDynamicClusterDurableAck.*ProcessAcceptance' -count=1 -v -timeout 30m`도 PASS(159.235s)했다. 세 replica × QoS 1/2 ACK 뒤 kill, 20개 WAL/commit crash 조합, leader/follower partition의 시험·결과는 [Cluster §11.13](cluster-session-and-delivery.md#1113-adr-0003-durable-ack-fault-수용-매핑)에 연결한다.

Batching 뒤 storage fault 실행 `GOMAXPROCS=2 go run ./tests/chaos/cluster-storage-v1 --run-id storage-proposals-20261002 --output .artifacts/chaos/cluster-storage-v1/storage-proposals-20261002`는 exit 0, 여섯 scenario 모두 PASS였다(259.07s wall). `summary.json`의 모든 `missingAcked`·`duplicates`는 비어 있고 quorum disk-full 중 성공 ACK는 없었다. Scenario별 ACK·복구 증거는 [Cluster §11.6](cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)에 연결한다. Docker/FUSE 결과는 production physical power-loss 증거가 아니다.

최종 `GOMAXPROCS=4 go run ./tools/verify local` PASS(301.06s wall): traceability, 네 fuzz target 각각 10,000회, architecture, package·integration 시험, race, coverage를 통과했다. Integration package는 261.972s였다. 이 안전성 검증과 부분 실행 관찰만으로 BP-03 수용 상태를 바꾸지 않는다.

##### 명시적 채택 뒤 bounded 수신 대기 — 수용 미완료

사용자의 `제한된 수신 대기 채택` 선택 뒤 dynamic Raft lane에만 공간 예약과 TCP 읽기 중단을 적용했다. 32개·4 MiB 한도, 유효 prefix 뒤의 5초 payload deadline, idle-prefix 동작, command별 fresh proof·독립 entry·quorum-before-ACK은 유지했다. Fixed-voter와 delivery-forwarding lane은 바꾸지 않았다.

첫 후속 compare `bp03-20261002T021302.960231500Z-30036`도 EMQX valid / NaruMQ invalid였다(205.86s wall). Step 1·2는 QoS별 sent/ACK/delivery 120/120/120 및 240/240/240, gap·duplicate 0이었다. Step 3 QoS 1 offered/sent/ACK/delivery는 600/594/586/571(gap 15), QoS 2는 600/600/600/583(gap 17)이었다. Duplicate 0, generator overload 없음, 최대 schedule lag 1.877 ms였다. Offline·reconnect는 수행하지 못했다. `diagnostics.jsonl`은 종료 scrape를 포함한 147건, scrape error 0이었다. 단일 invalid run의 gap 감소는 재현 가능한 개선 근거로 채택하지 않는다.

해당 node log에는 inbound queue 포화 연결 종료가 없었다. Node 2→1 outbound queue 포화는 02:14:13.710 UTC에 32 messages·613 bytes, 연결 유지 상태로 기록됐고, 02:14:18.705 이후 durable read-barrier·proposal deadline과 MQTT EOF가 발생했다. 시간 순서는 관찰됐지만 거절된 frame의 종류와 특정 timeout 사이 인과는 확인되지 않았다. 기존 outbound `ErrPeerBackpressure`·unreachable 보고는 이번 inbound 정책의 변경 범위 밖이며 유지했다.

별도 종료 경계 회귀에서 payload를 모두 읽은 직후 context를 취소하면 frame을 stage하는 결함을 재현했다: `TestDynamicRaftInboundCancellationBeforeStagingReturnsReservation`이 `cancellation after payload read: <nil>`로 FAIL했다. Staging 직전 cancellation fence를 추가한 뒤 `go test ./internal/adapters/tcp ./cmd/narumq ./internal/adapters/raft ./tests/benchmark/bp03 -count=1` PASS(9.611/12.325/1.821/0.450s; 15.44s wall)했다. Count·byte cap, FIFO 재개, lease·reservation, 취소·identity·shutdown·연결 교체·writer failure·timeout·truncated payload의 시험은 [v2 계약](../contracts/raft-membership-v2.md)에 연결한다.

Cancellation fence 뒤 최종 compare `bp03-20261002T021958.747822300Z-10548`도 EMQX valid / NaruMQ invalid였다(211.02s wall). 고정 profile SHA-256은 위 실행들과 같았다. 이번 실행에서는 Step 1·2·3의 QoS별 sent/ACK/delivery가 각각 120/120/120, 240/240/240, 600/600/600으로 일치했고 gap·duplicate 0, generator overload 없음이었다. 그러나 offline QoS 1은 64/64/64, QoS 2는 sent/ACK/delivery 64/64/32(gap 32)였으며 `SessionPresent`는 3/4였다. 네 번째 subscriber `q2-02`의 재접속이 실패한 CONNACK은 reason `0x88`(Server unavailable)이었다. 이는 session state 유실의 증거가 아니라 재접속 수용 실패이며 BP-03 전체는 invalid다.

최종 run은 `diagnostics.jsonl` 162건·scrape error 0을 보존했다. Node log에 inbound queue 포화 연결 종료는 없었지만 02:21:17.769–17.796 UTC에 outbound 32-message 포화가 기록됐고, node 3에서 02:21:19.936의 read-barrier deadline과 02:21:22.766의 durable read-barrier deadline·PUBACK 처리 실패가 이어졌다. 거절된 frame과 재접속 실패의 인과는 아직 미확정이다. 두 후속 compare 모두 invalid여서 조건부 5회 suite는 실행하지 않았으며, 반복 성능 개선·BP-03·ADR-0003 수용 완료를 주장하지 않는다.

최종 bounded 수신 코드의 `NARUMQ_DURABLE_ACK_FAULTS=1` matched-target process suite는 PASS(159.613s)였고, 뒤이어 실행한 `GOMAXPROCS=4 go run ./tools/verify local`도 traceability·네 fuzz target·architecture·package/integration·race·coverage PASS였다(integration package 257.151s; 두 명령 합계 460.37s wall).

반면 Docker/FUSE `storage-flow-20261002`는 exit 1(255.22s wall), **5/6 PASS**였다. `follower-slow-io`는 follower가 HEALTHY·READY로 복구되고 baseline MQTT 재접속이 수락된 뒤 leader의 durable sentinel CONNECT가 `0003`(Server unavailable)으로 거절되어 최종 ACKed delivery 확인을 완료하지 못했다. 다른 다섯 scenario는 통과했다. 실패 scenario의 `missingAcked`·`duplicates` 배열이 비어 있어도 sentinel 검증 전에 중단됐으므로 무누락·무중복 통과 증거가 아니다. 상세 결과는 [Cluster §11.6](cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)에 연결한다.

CONNECT/readiness의 projection lag fail-closed 정책은 이번 승인 범위 밖이므로 유지했다. Fresh proof 뒤 Ready 중간 상태에서 projection checkpoint를 기다리도록 바꾸는 선택은 별도의 미승인 후보다. 그 새 동작을 요구한 실험 시험 두 개는 기존 정책에서 즉시 거절되어 FAIL한 뒤 되돌렸으며, 실제 chaos의 거절 원인이라는 증거로 채택하지 않는다. BP-03와 storage-fault 수용은 미완료로 남긴다.

Inbound 범위의 마지막 종료 보강에서는 `ResetPeer`가 socket만 닫고 capacity waiter를 깨우지 않는 누락을 확인했다. Prefix만 쓴 시험은 reader가 아직 공간 대기에 들어가기 전에 reset될 수 있어 통과를 증거로 쓰지 않았다. `TestDynamicRaftResetUnblocksActiveCapacityWait`를 실제 대기 select 진입과 동기화한 뒤에는 `reset socket left the capacity waiter alive`로 FAIL했다. `ResetPeer`의 connection 무효화와 같은 mutex 안에서 capacity wake를 signal한 뒤 TCP·runtime package 시험 PASS(10.039/10.840s; 13.79s wall)했다. 이 보강은 malformed-peer reset의 종료 경계이며 앞선 BP-03 재접속·slow-I/O sentinel 실패를 해결했다는 증거가 아니다. 위 compare·ACK fault·chaos 실행은 reset wake 보강 전의 증거로 구분한다.

Reset wake 보강과 미승인 readiness 시험 원복 뒤 최종 `GOMAXPROCS=4 go run ./tools/verify local` PASS(305.88s wall; integration 271.163s)했다. Traceability, 네 fuzz target 각각 10,000회, architecture, package/integration 시험, race와 coverage를 통과했다. 이 결과는 malformed-peer reset 회귀와 기존 readiness 계약을 포함한 repository gate의 증거이며, 앞선 invalid BP-03와 5/6 chaos 결과를 합격으로 바꾸지 않는다.

##### 2026-10-02 readiness 거절 진단 — 기존 계약 유지

이후 사용자는 `기존 계약 유지·진단 진행`을 선택했다. Readiness의 storage 검사·fresh quorum proof·local snapshot 판정 순서와 반환 오류는 바꾸지 않는다. `cluster readiness rejected` 로그는 실제 거절에 사용한 storage view와 local snapshot을 기록한다. `purpose`는 `mqtt`, `health`, `summary`, `backup`, `drain`으로 호출 경로를 구분하고, `readiness_stage`는 `readiness_storage`, `readiness_fresh_read`, `readiness_local_state`만 사용한다. Management summary의 quorum 조회를 MQTT admission으로 오인하지 않도록 `hasFreshProof`는 `summary`로 구분한다. Fresh proof 실패에는 오류 분류를, local 상태 실패에는 첫 불충족 조건과 모든 판정 flag·proof/applied/projection index를 남긴다. 여러 조건이 동시에 실패할 수 있으므로 첫 조건만으로 다른 조건이 정상이었다고 해석하지 않는다.

Local snapshot 앞뒤의 `ready_sequence_before`·`ready_sequence_after`도 함께 기록한다. 홀수는 Ready의 WAL·apply·projection 처리 구간이며, 두 값이 다르면 snapshot 수집 중 처리 구간이 바뀌었다는 뜻이다. 이는 기다리거나 재판정할 근거로 사용하지 않는다. Storage 값은 기존 첫 검사 시점의 값이며 이후 상태로 다시 샘플링하지 않는다. 같은 purpose·stage·거절 분류는 기존 commit 실패 로그 집계 방식으로 첫 건과 1초 간격 표본만 출력하고 건수를 누적한다. 로그는 해당 표본의 판정 증거이지 그 사이의 개별 CONNECT 기록이 아니다. Client/tenant ID, payload, topic, token은 추가하지 않으며, retry·checkpoint 대기·proof 재사용·ACK 경계 변경은 포함하지 않는다.

실행: `go test ./cmd/narumq -count=1` PASS(11.448s; 13.85s wall), summary 호출 분리 뒤 `GOMAXPROCS=4 go test ./cmd/narumq -count=1` PASS(10.916s; 13.71s wall). 기존 storage fail-closed·projection admission·fresh-proof/FIFO 시험을 유지했으며 checkpoint 대기를 요구하는 시험은 추가하지 않았다.

고정 profile SHA-256과 resource/workload 조건을 유지한 진단 compare 두 건은 다음과 같다. 둘 다 EMQX valid / NaruMQ invalid이고 `ackLatencyDirectlyComparable=false`다.

| 실행 | 실제 결과 | readiness 진단 |
| --- | --- | --- |
| `bp03-20261002T030945.504296100Z-15728` — 206.84s wall; summary purpose 분리 전 | Step 1·2·3 모두 QoS별 sent/ACK/delivery 120/240/600 일치, gap·duplicate 0, overload 없음. Offline QoS 1은 64/64/64, QoS 2는 64/64/0, `SessionPresent=3/4`. Runner는 `q2-02` DNS lookup의 context 취소로 중단. | Node 2·3의 startup health 거절 표본 3건은 `release_profile_unsynced`; proof/applied/projection index 모두 5, Ready sequence는 같은 짝수. Workload 중 readiness 거절 표본은 없었다. Diagnostic scrape 156건·error 0. |
| `bp03-20261002T031550.460725700Z-39164` — 194.24s wall; summary purpose 분리 뒤 | Step 1·2는 QoS별 120/240 sent/ACK/delivery 일치. Step 3 도중 QoS 1 sent/ACK/delivery 255/247/121(gap 126), QoS 2 371/370/241(gap 129), duplicate 0·overload 없음. `q1-01` subscriber EOF로 중단되어 offline·reconnect는 미실행. | Startup health 표본 3건은 같은 `release_profile_unsynced`, index 5/5/5·안정된 짝수 sequence였다. Workload 중 readiness 거절 표본은 없었다. Diagnostic scrape 120건·error 0. |

첫 run의 node 3은 03:11:08.873 UTC에 durable read-barrier deadline과 `dispatch MQTT PUBACK: quorum unavailable`를 기록했다. Runner의 reconnect reader는 오류 시 상위 context를 취소하므로 마지막 DNS 취소만으로 DNS 장애나 CONNACK 거절을 주장하지 않는다. 두 번째 run은 03:16:51.197 UTC에 node 1→2 outbound 32-message 포화, 03:16:53.607부터 durable target saturation, 03:16:56.178에 node 2 durable read-barrier deadline·PUBACK 실패·subscriber EOF, 03:16:56.206에 proposal deadline·PUBLISH 실패가 관찰됐다. 거절된 peer frame이 timeout을 일으켰다는 인과는 미확정이다. 두 run은 이전 offline CONNACK 거절을 재현하지 않았으며 projection lag가 그 원인이라고 결론내릴 근거도 없다. Invalid compare이므로 조건부 5회 suite는 실행하지 않았다.

Docker/FUSE `storage-readiness-20261002`는 exit 0, **6/6 PASS**(249.71s wall)였다. Fault 중 follower disk-full·sync-error의 MQTT 거절은 `readiness_storage`, health `FAILED`, code `storage_no_space`·`storage_io_error`였다. Slow-I/O 거절은 같은 stage에서 `DEGRADED`·`storage_slow`였다. Quorum disk-full의 leader는 local storage `HEALTHY` 상태에서도 `readiness_fresh_read`·`deadline`·`fresh_proof_confirmed=false`로 거절했다. 판정 단계가 달라 아직 읽지 않은 local flag를 허위로 기록하지 않았다. Startup health의 local 거절은 quota profile 미커밋 또는 release profile 미동기화였다. 두 compare와 이 chaos에서 기록한 local snapshot 27건은 모두 projection index가 applied index 이상이었으며, 이는 기록한 표본의 관찰이지 모든 요청의 무경합 보장은 아니다.

Slow-I/O는 leader progress 55/55, recovery 12.213s, applied index 373 수렴과 최종 sentinel·ACKed delivery 검증을 통과했다. 여섯 scenario의 missing/duplicate 배열이 모두 비어 있고 해당 검증도 실제 완료됐다. 이전 `storage-flow-20261002`의 복구 후 sentinel CONNACK 실패는 재현되지 않았으므로 readiness 정책 변경으로 해결했다고 주장하지 않는다. 상세 chaos 수용은 [Cluster §11.6](cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)에 연결한다.

같은 판정 계측의 matched-target durable ACK process suite PASS(157.322s: replica kill 31.79s, 20 WAL/commit crash 99.60s, partition 25.78s), 이어진 `GOMAXPROCS=4 go run ./tools/verify local` PASS(integration 245.901s; 두 명령 합계 435.04s wall)였다. Traceability·네 fuzz target 각 10,000회·architecture·package/integration·race·coverage를 통과했다. 이 외부 실행과 gate는 마지막 로그 attr 구성의 allocation 보강 전 증거로 구분한다.

마지막 로그 구성은 typed `slog.Attr`와 고정 buffer로 바꿔 숫자 boxing·slice 확장 복사를 피했다. Disabled WARN과 rate-limit 표본 제외 건은 attr를 구성하지 않는다. `GOMAXPROCS=4 go test ./cmd/narumq -gcflags=-m -count=1` PASS(13.168s; 16.47s wall)였으며 compiler escape analysis에서 local snapshot과 두 append는 `does not escape`였다. 판정 입력·로그 필드·반환 오류는 그대로 유지한다.

최종 typed attr 코드로 Docker/FUSE `storage-readinessattrs-20261002`도 **6/6 PASS**했다(scenario 실행 247.896s). MQTT 표본은 다시 storage 거절 3건과 quorum-loss fresh-proof deadline 9건으로 분리됐고, numeric/boolean 필드는 원문 JSON에서 해당 타입으로 보존됐다. Local snapshot에는 projection/applied gap이 없었다. Slow-I/O는 progress 55/55, recovery 12.195s, index 373 수렴과 sentinel·ACKed delivery를 통과했으며 모든 scenario의 missing/duplicate가 없었다. 뒤이어 최종 `GOMAXPROCS=4 go run ./tools/verify local`도 traceability·네 fuzz 각 10,000회·architecture·package/integration·race·coverage PASS(integration 279.091s; chaos와 gate 합계 558.26s wall)했다. 이 gate는 마지막 source 변경 뒤의 증거다. BP-03 compare 두 건은 attr 구성 보강 전의 workload 증거이며 invalid 상태·반복 suite 미실행·ADR-0003 미완료는 그대로다.


아래 `bp03-20261001T133103.344202600Z-10712`까지의 실행은 Enterprise 전환 전 OSS·memory-session comparator의 과거 증거다. Enterprise 구성의 실행 증거는 별도 run ID로 구분하며 같은 내구성 조건의 측정으로 섞지 않는다.

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
- 2026-10-01 추가 진단 compare `bp03-20261001T131030.071121600Z-2212`, `bp03-20261001T131403.928045500Z-37288`도 `valid=false`였다. 두 run 모두 Step 1의 QoS별 120/120, Step 2의 QoS별 240/240 전송·ACK·전달과 gap/duplicate 0을 확인했고 해당 step의 generator overload는 없었다. 첫 run의 최대 schedule lag는 1.256463/1.144711ms, 둘째는 1.216113/1.556861ms였다. 이후 subscriber EOF로 Step 3 warmup 중에 중단되어 offline/reconnect는 실행되지 않았다. 둘째 run에서 13:15:03 UTC outbound queue 32개 포화, 13:15:08 UTC node 2·3의 durable `read_barrier` deadline과 PUBREC/PUBLISH quorum-unavailable, `q2-02` EOF가 관측됐다. 이 관측만으로 WAL 지연과 timeout의 인과를 확정하지 않는다. 두 run은 최종 suffix 예약 안전성 수정 전 진단이며 유효한 비교 결과로 채택하지 않는다.

#### 2026-10-01 최종 재검증

최종 코드의 고정 compare `bp03-20261001T133103.344202600Z-10712`는 exit 1, top-level `valid=false`였다. EMQX workload/resource는 valid이고 NaruMQ는 invalid였다. 실행 전 Docker container가 없었으며 다른 검증 작업과 겹치지 않았다. Profile은 `tests/benchmark/bp03/testdata/smoke.json`, SHA-256 `0b8a88abac04fcfffeb72152cb86ae6aab4682808df7e4d43c9ee3df8661c92a`로 변경하지 않았다.

- Step 1은 QoS 1/2 각각 120/120, Step 2는 각각 240/240 전송·ACK·전달을 완료했다. 두 step 모두 gap/duplicate 0, `generatorOverload=false`; max schedule lag는 1.148269/1.194068ms였다.
- Step 3 warmup에서 13:32:02.561 UTC node 2의 peer 1 inbound queue가 32개로 포화되고 연결이 끊긴 상태를 기록했다. Node 1에서도 inbound 포화가 이어졌다. 13:32:07.528 UTC node 2의 durable proposal deadline과 `dispatch MQTT PUBACK: quorum unavailable` 뒤, 13:32:07.529 UTC subscriber `q1-01` EOF가 기록됐다. 13:32:07.570 UTC durable `read_barrier` deadline과 PUBLISH quorum-unavailable도 기록됐다. Warmup을 끝까지 완료하지 못해 Step 3 측정·offline·reconnect는 실행되지 않았다. Summary의 이 구간 0 값은 성공 또는 message gap 부재를 뜻하지 않는다.
- 시작 시각 13:31:29–31 UTC의 세 `read_barrier` warning은 command kind가 없는 초기 조회이며, 뒤의 durable timeout과 구분한다. 포화·timeout 순서는 관측 사실이지만 WAL 비용이 직접 원인이라는 인과는 확인하지 않았다.

아래는 완료된 WAL scrape window의 **진단 값**이다. Fsync occupancy는 node별 sync duration sum 증가량을 해당 WAL window 시간으로 나눈 비율이다. Node 간 시간을 합산하거나 완료된 두 step만으로 전체 run을 유효하다고 판단하지 않는다.

| WAL window | 관측 시간 | Node 1/2/3 fsync occupancy | Node 1/2/3 meanEntriesPerSync |
| --- | --- | --- | --- |
| Step 1 | 12.041271s | 39.42% / 41.08% / 41.65% | 0.588091 / 0.584860 / 0.585828 |
| Step 2 | 12.081434s | 79.54% / 84.64% / 85.39% | 0.859767 / 0.821490 / 0.832250 |

NaruMQ invalid 분기에 따라 이번 최종 코드의 conditional 5회 suite는 실행하지 않았다. 유효한 ACK/WAL 비교, 반복 suite 및 ADR-0003 §9-5 acceptance로 채택하지 않으며 BP-03은 미완료다. 승인된 계획의 invalid 분기에 따라 추가 성능 변경 없이 진단 결과를 기록했다. Commit-only HardState sync와 command별 fresh proof는 유지한다. Artifact는 `.artifacts/benchmark/bp03/bp03-20261001T133103.344202600Z-10712/`에 보존했다.

기능·안전성 검증은 별도로 통과했다:

- Targeted `-race` 실행의 package별 결과는 cmd runtime 12.766s, Raft 3.567s, chaos runner 1.739s로 PASS였다. 같은 실행의 TCP package는 추가 시험의 잘못된 DTO 필드 참조로 compile에 실패했으며 이를 수정했다. 이후 `GOMAXPROCS=4 go test ./internal/adapters/tcp -count=1 -race` PASS (13.691s). Batch retry 시험은 앞선 commit의 응답만 오류로 돌아오는 경우와 모든 suffix ID 보존, local/durable queue의 wire topic·ID 순서를 확인한다.
- `GOMAXPROCS=4 NARUMQ_DURABLE_ACK_FAULTS=1 go test ./tests/integration -run '^TestDynamicClusterDurableAck(ReplicaKill|CrashPoint|Partition)ProcessAcceptance$' -count=1 -v -timeout 30m` PASS (161.165s): replica kill 6개, WAL/commit crash 20개, partition 2개 시나리오.
- Docker/FUSE run `storage-batch-20261001`의 여섯 scenario 모두 PASS (263.29s). 상세 ACK·cut·recovery 증거는 [Cluster §11.6](cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)에 기록했다. 이 결과는 실제 전원 손실 또는 production Linux/EBS 증거가 아니다.
- 최종 compare 이후 `GOMAXPROCS=4 go run ./tools/verify local` PASS (306.35s): package/integration/race, 네 fuzz target, traceability, architecture, coverage. 앞선 local run에서 발견한 TCP test fixture data race는 위 초기화 순서 수정으로 해소됐으며 최종 race suite에서 재발하지 않았다.

#### 2026-10-03 공식 async WAL·group commit 실행

[ADR-0020](../decisions/ADR-0020-asynchronous-raft-wal-group-commit.md)을 채택했다. Disk 전용 단일 worker에 bounded append 요청을 넘기고 이미 대기 중인 CRC frame들을 한 sync로 기록한다. Driver의 8ms timed collection은 제거하지만 command별 fresh ReadIndex와 commit-only HardState sync는 유지한다. Local WAL 완료 전 entry-bearing replication도 보류한다. `CompleteApply`는 runtime state·projection 적용 뒤 호출한다. Ready histogram은 이제 fsync를 포함하지 않는 poll 시간이므로 이전 Ready/WAL 시간 비율과 직접 비교하지 않는다.

동일 고정 smoke profile SHA-256 `0b8a88abac04fcfffeb72152cb86ae6aab4682808df7e4d43c9ee3df8661c92a`의 compare `bp03-20261002T165307.722011400Z-40216`는 exit 0, 두 target workload/resource가 valid였다. Profile·resource·deadline·queue를 늘리지 않았다.

| NaruMQ 측정 | Step 1 | Step 2 | Step 3 |
| --- | --- | --- | --- |
| QoS별 offered·sent·ACKed·delivered | 120 각각 | 240 각각 | 600 각각 |
| 최대 schedule lag | 1.131ms | 1.231ms | 2.569ms |
| Generator overload·gap·duplicate | false·0·0 | false·0·0 | false·0·0 |
| QoS 1 ACK p99 | 23.657ms | 48.877ms | 59.531ms |
| QoS 2 ACK p99 | 28.350ms | 44.711ms | 60.689ms |

Offline QoS별 64개 전송·ACK·전달, sessionPresent 4/4, protocol error 0이었다. Step 3 cluster sync 6,625회·14,381 entries, mean 2.171 entries/sync, WAL window 12.071719s였다. 이전 invalid 진단 compare `bp03-20261002T133437.979734800Z-36656`의 Step 3은 sync 8,100회·14,204 entries, mean 1.754, schedule lag 3.809645s였다. 이번 관측은 단일 고정 workload 개선이며 production 성능이나 실패 원인 전체를 입증하지 않는다. 두 window 길이도 같지 않다. Initial compare metadata의 `fsync per Ready frame` 표현은 이 cutover 뒤 오래된 설명이며 suite부터 async single-writer group commit으로 수정했다.

단일 compare가 유효해 조건부 독립 5회 suite를 실행했다. `bp03-suite-20261002T165724.420235500Z-25996`는 exit 1, **4/5 성공·parent valid=false**였다. 회차 1·2·4·5의 양쪽 target은 valid였고 NaruMQ Step 3 schedule lag는 1.179–1.318ms였다. 3회차는 Step 1·2 count를 통과한 뒤 Step 3 warmup에서 publisher `q1-02` EOF (`2026-10-02T17:04:43.022186277Z`)로 중단했다. Node 2는 직전 `dispatch MQTT PUBLISH: tcp connection closed`, 약 4.91초 뒤 durable proposal deadline을 기록했다. 원인은 확인되지 않았으며 뒤의 timeout을 EOF 원인으로 단정하지 않는다. Step 3·offline의 0 값은 성공이 아니라 미실행이다. Parent metric은 비었고 median/MAD를 채택하지 않는다. 실패를 지우기 위한 추가 suite는 실행하지 않았다.

Artifact는 `.artifacts/benchmark/bp03/<위 run ID>/`에 있다. 동일 workload/resource의 `equivalent=true`는 정확한 ACK/storage·security 계약 등가를 뜻하지 않으며 `releaseGating=false`다. **BP-03 반복 수용과 ADR-0003은 미완료**다.

안전성은 [Cluster §11.18](cluster-session-and-delivery.md#1118-비동기-walapplication-완료-경계-수용-매핑)에 연결한다. Raft/runtime race PASS, 실제 MQTT takeover·backup/restore·drain·peer smoke PASS, matched-target ACK fault suite PASS(159.50s wall), Docker/FUSE 최종 6/6 PASS(251.16s wall)였다. 초기 leader before-write 조기 ACK와 첫 chaos의 in-flight projection readiness 실패는 수정·재검증했으며 기록을 숨기지 않는다. Production Linux/EBS·물리 전원 손실은 미실행이다.

최종 `GOMAXPROCS=4 go run ./tools/verify local` PASS (301.74s wall; integration 264.667s). Traceability·architecture·네 fuzz target 각 10,000회·전체 package/integration·production package race·coverage를 확인했다. 이 gate는 위 단일 compare·5회 suite와 최종 readiness 코드 뒤 실행했으며 BP-03 parent invalid를 대체하지 않는다.

후속 보완은 [관측성 스펙](management-api-and-observability.md)의 WAL histogram과 metric HELP를 실제 성공한 group sync당 한 표본·여러 frame의 entries/bytes 합계로 일치시켰다. Registry focused suite·traceability PASS. 보관 append 종료 저장·오류 반환, 보류 복제 bytes 소유와 held-sync heartbeat 회귀는 Raft race suite PASS였다. 이후 ACK fault suite PASS (160.56s wall), Docker/FUSE `storage-asyncclose-20261003` 6/6 PASS (242.19s wall), local gate PASS (292.24s wall). 마지막 consumed-error 종료 보고 보강 뒤 최종 local gate도 PASS (289.24s wall)다. 위 BP-03 artifact는 후속 보완 전 실행이며, 4/5 invalid suite를 최신 코드의 반복 성능 통과로 재해석하지 않는다.

##### 2026-10-03 EOF 재현과 release-profile 재확인 wake

사용자의 실패 원인 조사 요청으로 연결 종료 진단을 보강했다. Async PUBLISH의 local admission 거절이 `ErrConnectionClosed`로 잘못 분류되던 경로는 `ErrClusterNotReady`로 수정했다. `TestDurablePublishProjectionDenialIsNotSocketFailure`는 기존 오류로 FAIL을 재현한 뒤 QoS 1/2에서 성공 ACK·inbound mutation·durable 제출 없이 올바른 오류를 반환함을 확인한다. Outer phase의 fail-closed 종료 정책은 유지한다. Writer는 실제 delivery/control 치명 오류를 종료 전에 기록하며, throwaway smoke에서 `write pipe: i/o timeout` 로그 뒤 peer EOF를 확인했다.

진단 실행을 성공 결과만 골라 수용하지 않는다:

| 실행 | 결과·관찰 |
| --- | --- |
| `bp03-20261002T181557.406064300Z-5612` / `bp03-suite-20261002T181947.588338600Z-18508` | Compare 양쪽 valid, suite 5/5 valid. 원인 수정 전에도 성공한 표본이므로 EOF 해소 증거가 아니다. |
| `bp03-20261002T184957.134456000Z-41612` / `bp03-suite-20261002T185324.743416400Z-2264` | Compare valid, suite 4/5 invalid. 3회차 node 2에서 applied/projection 4017/4017, storage `HEALTHY`, `release_profile_synced=false`의 PUBLISH 거절 뒤 publisher EOF를 관찰했다. |
| `bp03-suite-20261002T191223.997944900Z-35300` | Release 전이 진단을 추가한 suite도 4/5 invalid. 1회차 node 3에서 `19:13:20.191710238Z` snapshot restore invalidation → `19:13:20.221105598Z` PUBLISH의 `release_profile_unsynced` 거절 → `19:13:20.223362862Z` publisher `q2-01` EOF를 확인했다. 거절 snapshot은 applied/projection 4043/4043, storage `HEALTHY`였다. |

이 마지막 재현에서 최초 종료를 유발한 것은 restore 뒤 재확인되지 않은 release-profile gate다. Controller는 기존에 다음 500ms periodic reconcile까지 기다릴 수 있었다. 뒤늦은 proposal timeout이나 teardown의 quorum 오류를 이 EOF 원인으로 취급하지 않는다. 초기 `bp03-suite-20261002T165724.420235500Z-25996`에는 전이 진단이 없으므로 같은 원인이었다고 확정하지 않는다.

개선은 scheduling에 한정한다. Snapshot restore·committed local profile 변경은 **계속 즉시 gate를 닫고**, 대기 알림 최대 한 개로 controller를 깨운다. 재확인은 기존 `Members`·`ActiveFeatures`의 두 fresh read barrier와 revision fence를 유지한다. 성공 재확인 전에는 gate를 열지 않으며 오류·unsupported feature는 fail closed다. PUBLISH retry·admission 대기 추가·proof cache·queue/deadline/resource 증가는 없다. `TestClusterReleaseProfileSnapshotWakeReconfirmsAfterStaleRead`는 기존 코드의 periodic 대기로 FAIL했고 보완 뒤 controller race suite가 PASS했다. 같은 시험에서 restore 직후·이전 revision 완료 뒤·새 두 barrier 완료 전의 닫힌 gate를 확인한다. 기존 compatible-snapshot invalidation 시험은 변경하지 않았다.

보완 뒤 최종 compare `bp03-20261002T193345.142241000Z-38956` (206.24s wall)는 양쪽 workload/resource valid였다. 같은 고정 profile SHA-256 `0b8a88abac04fcfffeb72152cb86ae6aab4682808df7e4d43c9ee3df8661c92a`와 resource 제한으로 독립 5회 suite `bp03-suite-20261002T193711.351279300Z-4364` (927.13s wall)는 **5/5·parent valid=true**였다. 각 회차 Step 3 QoS별 offered·sent·ACKed·delivered 600개, offline QoS별 64개, sessionPresent 4/4, gap·duplicate 0이었다. Step 3 max schedule lag는 1.177–1.641ms이고 overload는 없었다.

| Suite Step 3 NaruMQ metric | 중앙값 | MAD | 최소–최대 |
| --- | --- | --- | --- |
| QoS 1 ACK p99 | 54.895ms | 1.657ms | 53.238–57.203ms |
| QoS 2 ACK p99 | 56.611ms | 3.149ms | 53.387–62.605ms |
| Mean entries/sync | 2.053607 | 0.003409 | 2.036770–2.071090 |

최종 5회에는 `snapshot_restored` 진단 표본이 없었다. 이 suite는 고정 local smoke의 반복 수용 근거이지, snapshot 직후 모든 PUBLISH가 성공하거나 모든 향후 EOF가 제거됐다는 증명이 아니다. 실제 설치 snapshot은 별도 `NARUMQ_RELEASE_COMPAT=1` process acceptance에서 N/N-1 양방향·두 N node와 MQTT/durable session을 확인했다(PASS, 85.55s wall). Gate가 닫힌 동안의 거절은 의도된 계약으로 남는다. `releaseGating=false`·`ackLatencyDirectlyComparable=false`와 storage/security 계약의 비등가도 유지하며 ADR-0003 전체 또는 production 수용을 완료하지 않는다.

안전성 재검증: `GOMAXPROCS=4 NARUMQ_DURABLE_ACK_FAULTS=1 go test ./tests/integration -run '^TestDynamicClusterDurableAck(ReplicaKill|CrashPoint|Partition)ProcessAcceptance$' -count=1 -v -timeout 30m` PASS (160.53s wall; replica/QoS 6개·crash 20개·partition 2개). `GOMAXPROCS=2 go run ./tests/chaos/cluster-storage-v1 --run-id storage-releasewake-20261003 --output .artifacts/chaos/cluster-storage-v1/storage-releasewake-20261003`도 6/6 PASS (241.47s wall), 모든 scenario의 `missingAcked`·`duplicates`는 비었다.

위 외부 실행 뒤 로그의 `fresh_proof_confirmed`를 명시적인 판정 입력으로 정정했다. Local PUBLISH snapshot은 새 quorum proof가 아니므로 `false`, fresh readiness barrier 뒤 snapshot은 `true`다. 초기 진단 artifact의 PUBLISH `fresh_proof_confirmed=true,proof_index=0`은 잘못된 표시였으며 fresh proof 근거로 쓰지 않는다. 두 JSON 값의 구분은 throwaway smoke로 관찰했다. Scheduling·readiness·ACK 동작은 이 로그 정정에서 바꾸지 않았다.

Wake·proof 진단 코드와 회귀를 정리한 단계의 `GOMAXPROCS=4 go run ./tools/verify local`은 PASS (293.03s wall; integration 264.789s)였다. Traceability·architecture·네 fuzz target 각 10,000회·전체 package/integration·production package race·coverage를 확인했다. Wake 회귀는 별도 `-count=10 -race`도 PASS (7.29s wall)였다. 실행 결과를 덧붙인 문서 변경은 gate 뒤에 했으며 편집 결과를 육안 확인했다. 임시 diagnostic smoke 파일은 제거했다. Production Linux/EBS·physical power-loss 검증과 group 크기별 production 성능은 미실행이며 이 로컬 검증으로 대체하지 않는다.

마지막 writer 보완은 `NewServer`가 non-nil logger를 보장하는 불변식을 그대로 사용한다. 생성자를 거치지 않은 timeout 시험 fixture에 logger를 제공하고 production writer의 중복 nil 검사는 제거했다. Focused writer 시험에서 실제 `write pipe: i/o timeout` warning과 연결 종료를 확인했다. 이 코드·fixture 및 앞선 문서 정정 뒤 full local gate도 PASS (278.34s wall; integration 245.843s)였다. 이 마지막 실행의 결과를 기록한 문서 덧붙임은 다시 gate 뒤에 했으며 편집 결과를 육안 확인했다. Benchmark·ACK fault·chaos artifact는 앞서 설명한 wake 코드의 외부 실행이며 이 fixture 정리로 재측정한 결과로 표시하지 않는다.



#### 2026-10-01 Enterprise comparator 구성 검증

`go run ./tests/benchmark/bp03 compare --profile tests/benchmark/bp03/testdata/smoke.json`을 실행했다. Run ID는 `bp03-20261001T142245.697829200Z-22092`, profile SHA-256은 기존과 같은 `0b8a88abac04fcfffeb72152cb86ae6aab4682808df7e4d43c9ee3df8661c92a`다. 이번 실행부터 EMQX는 위에 고정한 Enterprise image와 Durable Sessions·JWT 설정을 사용한다.

- **실제 구성:** 세 node의 `emqx-durability.txt`에서 `durable_sessions.enable=true`, message backend `builtin_raft`, `n_sites=3`, `replication_factor=3`, `n_shards=1`을 확인했다. Shard `0`의 replica set에는 같은 세 site가 있었고 transition은 비어 있었다. JWT authenticator는 활성 상태이며 JWKS와 `iss`·`aud`·`sub` 검증 설정이 기록됐다. 설정만 선언하고 memory backend로 실행하는 fallback은 없다.
- **EMQX PASS:** workload와 resource summary가 모두 valid였다. QoS 1과 QoS 2 각각 Step 1에서 120/120, Step 2에서 240/240, Step 3에서 600/600 ACK·delivery를 완료했다. 각 step의 max schedule lag는 2.008ms, 1.435ms, 1.682ms였고 `generatorOverload=false`였다. Offline에서는 QoS별 64/64 ACK·delivery, reconnect의 `SessionPresent=4/4`를 확인했다. 누락·중복·ACK 거절·timeout은 없었다.
- **전체 compare FAIL:** NaruMQ는 Step 1·2의 QoS별 120/120·240/240 ACK·delivery를 완료했지만 Step 3 warmup에서 중단됐다. 14:23:48 UTC부터 node 1과 node 3에 inbound queue 포화가 기록됐고, 14:23:53에 node 3 durable proposal deadline, `dispatch MQTT PUBACK: quorum unavailable`, subscriber `q1-02` EOF가 이어졌다. Step 3 측정·offline·reconnect는 실행하지 못했다. 전체 결과는 `valid=false`이며 유효한 NaruMQ/EMQX 성능 비교나 BP-03 acceptance가 아니다.
- **검증 범위:** 이번 smoke는 replicated Durable Sessions 구성과 실제 MQTT workload·offline/reconnect 경로의 증거다. EMQX restart/power-loss, quorum `fsync` 이전 ACK 여부, NaruMQ와 같은 QoS state 영속 시점 및 peer mTLS는 검증하지 않았다. `comparison.json`의 durability·authentication `nonEquivalent`와 `ackLatencyDirectlyComparable=false`를 유지한다. EMQX WAL summary의 `not_applicable`은 수집 지표의 차이이며 WAL 부재를 뜻하지 않는다.
- **Package 검증:** `GOMAXPROCS=4 go test ./tests/benchmark/bp03 ./tests/benchmark/bp01 ./tests/benchmark/bp05 ./tests/benchmark/internal/environment -count=1` PASS. BP-01/BP-05의 OSS image는 유지하며 image pull API의 변경된 호출 지점을 함께 검증했다.
- **최종 local 검증:** Enterprise 전환과 위 실행 증거 반영 후 `GOMAXPROCS=4 go run ./tools/verify local` PASS (316.76s). Package·integration·race, 네 fuzz target, traceability, architecture, coverage를 통과했다. 이 검증은 invalid인 BP-03 workload를 유효하게 바꾸거나 미실행 durability 경계를 대신하지 않는다.

Artifact는 `.artifacts/benchmark/bp03/bp03-20261001T142245.697829200Z-22092/`에 보존했다. Enterprise 평가판의 25 concurrent session·non-production 제한을 기록하며 production license와 장기 comparator 선택을 대신하지 않는다.


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
- NaruMQ만 `metrics/nodeN-step-<ss>-{start,end}.prom`, `metrics/nodeN-offline-{start,end}.prom`과 `metrics/nodeN-diagnostic-<sequence>.prom` 원시 scrape를 보존한다. `diagnostics.jsonl`은 node별 요청 시작·종료 UTC 시각, 원시 파일 상대 경로 또는 scrape 오류를 기록한다. EMQX WAL artifact는 `applicability=not_applicable`을 기록한다.
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

### 7.1 MQTT client compatibility evidence

Phase 1 SDK interoperability는 일반 unit test와 분리된 Docker/E2E evidence gate다. 저장소 root에서 Go `1.26.5`와 Linux/amd64 container를 실행할 수 있는 Docker daemon을 준비한 뒤 다음 command를 사용한다.

```text
go run ./tests/compatibility/mqtt-clients-v1 \
  --run-id phase1-paho-go-python \
  --output .artifacts/compatibility/mqtt-clients-v1/phase1-paho-go-python
```

Command는 checked-in `tests/compatibility/mqtt-clients-v1/profile.json`을 strict decode하고 Eclipse Paho Go MQTT 3.1.1 `v1.5.1`, Eclipse Paho Go MQTT 5 `v0.23.0`, Eclipse Paho Python `paho-mqtt 2.1.0` image를 amd64로 build한다. Docker가 없거나 image architecture가 amd64가 아니면 case를 시작하지 않고 environment manifest와 실패 artifact를 남긴다.

성공 artifact는 `.artifacts/compatibility/mqtt-clients-v1/<run-id>/` 아래 `manifest.json`, `summary.json`, `cases.jsonl`, build log, image inspect metadata를 보존한다. 각 case는 broker log와 inspect, client stdout/stderr와 result JSON을 보존하고, QoS retransmission case는 proxy trace를 추가한다. Artifact에는 payload body, credential, raw certificate, 외부에서 주어진 Client ID와 secret을 기록해서는 안 된다.

Phase 1 compatibility acceptance는 Go 10개, Python 10개, cross-SDK QoS 1 4개와 MQTT 3.1.0 거부 2개, 총 26개 expected case를 검사한다. `--suite phase1`이 기본값이다. 이 suite는 `MQTT5_DRAIN_SAFE`나 malformed server DISCONNECT를 실행하지 않는다. Broker의 Server Reference packet과 SDK callback adapter의 redirect·Will 결과는 [Drain spec §10](drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)에 따로 연결한다.

이 Docker/E2E gate는 외부 dependency가 없어야 하는 `go test ./...`와 `go run ./tools/verify local`에 포함되지 않는다. 후자의 검증은 nested SDK module을 다운로드하거나 Docker를 시작하지 않아야 하며, compatibility command는 release evidence가 필요한 환경에서 명시적으로 실행한다.

Drain compatibility는 같은 실행 환경에서 별도로 실행한다. Run ID와 output은 기존 artifact와 겹치지 않아야 한다.

```text
go run ./tests/compatibility/mqtt-clients-v1 --suite drain \
  --run-id drain-paho-go-python-will-acl-final \
  --output .artifacts/compatibility/mqtt-clients-v1/drain-paho-go-python-will-acl-final
```

`drain-profile.json`은 두 SDK의 cluster drain 2개와 malformed DISCONNECT 8개를 고정한다. Cluster case는 새 3-node broker, JWT/JWKS 인증과 시험 전용 readiness LB를 사용한다. Malformed case는 fake server와 trap listener를 사용한다. Operation 45s, Will Delay 5s, 재접속 2s, LB 전파 대기 1000ms, rate 10/s와 drain timeout 60s는 test-only 값이다.

Artifact는 같은 run directory의 `manifest.json`, `summary.json`, `cases.jsonl`, build log와 image inspect에 기록한다. Cluster case는 `client/result.json`, client log, `lb/lb-trace.jsonl`, `drain-operation.json`, node별 log·inspect를 추가한다. Malformed case는 `fakeserver/fakeserver-trace.jsonl`을 추가한다. Credential과 private key는 임시 fixture에만 저장하고 case 종료 뒤 지운다.

최종 drain run은 9/10이다. 두 redirect case와 Python malformed 4개는 통과했다. Go의 중복 Server Reference case만 trap 접속으로 실패했다. 이 결과는 §7.1의 Docker 검증 범위이며 전체 SDK 지원 승인이 아니다. 상세 판정은 [Client contract §6.2](../contracts/mqtt-client-compatibility-v1.md#62-drain-compatibility-suite), 열린 작업은 [Cluster 잔여 작업 §1.2](../tasks.md#12-release와-rollout-gate)를 따른다.

`--suite drain`도 `go test ./...`와 `tools/verify local`에 포함하지 않는다. Root module의 순수 profile·result·trace unit test는 local gate에 포함한다. Nested SDK module의 build·vet와 실제 Docker suite는 별도로 실행해야 한다.

이 구현 뒤 실행한 전체 local gate는 PASS(280.08s)다. Compatibility root package unit test를 포함한다. Nested Go module의 `go vet ./... && go build ./...`도 PASS했다. Phase 1의 최종 adapter 회귀 `phase1-drain-adapters-final`은 26/26 PASS(132.67s)이며 같은 compatibility artifact root 아래에 보존했다. SDK drain의 9/10은 local gate 실패가 아니라 별도 호환성 gate의 실패다.

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
offline-detach-before-commit
offline-detach-after-commit
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
`offline-detach-before-commit`과 `offline-detach-after-commit`은 `DetachSession`의 offline DETACH proposal 직전과 `Applied` 확인 직후의 composition-root 경계다.

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

#### Local ext4 filesystem gate

사용자가 선택한 `local-ext4-v1`은 FUSE를 거치지 않는 실제 ext4 검증 profile이다. Linux/amd64 Docker daemon, privileged container와 kernel의 ext4·loop device 지원, image build용 module network access가 필요하다. 각 broker의 시험 전용 Docker volume에 256 MiB image를 만들고 `data=ordered`로 mount한다. Recovery-boundary fixture는 별도 1 GiB image에서 실행한다. 시험 자원을 제외한 host block device나 기존 volume을 지정하지 않는다.

저장소 root에서 실행한다. `--profile`을 생략하면 기존 `local-fuse-v1`의 여섯 scenario를 그대로 실행한다.

```text
go run ./tests/chaos/cluster-storage-v1 \
  --profile local-ext4-v1 \
  --run-id ext4-local \
  --output .artifacts/chaos/cluster-storage-v1/ext4-local
```

네 scenario는 follower ENOSPC, quorum ENOSPC, 전체 broker process SIGKILL/restart, filesystem recovery boundaries다. 마지막 scenario는 `TMPDIR`를 ext4 위에 두고 기존 WAL·snapshot·backup·voter-volume 수용 시험을 실행한다. 필수 test와 package의 완료를 모두 확인하며 nested test의 skip·fail도 실패다. Manifest schema `2`와 summary에는 `profile`, `evidenceBoundary`를 기록한다. 실제 kernel·mount 옵션·loop device·image 크기·broker SHA-256은 scenario의 `filesystems`와 원시 event에 보존한다. 세 process의 SIGKILL exit code와 ACK/session/delivery는 별도 증거이며 readiness 200만으로 복구를 판정하지 않는다.

정상 종료·실패·취소 때 시험 소유 자원을 정리한다. Loop device는 mount 뒤 autoclear로 표시해 container가 강제 종료돼도 마지막 mount가 사라지면 해제한다. Unmount·detach 확인이 없거나 Docker 정리가 실패하면 run도 실패한다. Privileged 권한은 host의 device 접근 범위를 넓히므로 폐기 가능한 개발/시험 환경에서만 실행한다.

이 gate는 host/kernel을 공유하는 로컬 ext4의 process-crash·인위적 recovery 입력 증거다. 자연 발생 torn write, filesystem cache를 잃는 물리 전원 손실, 독립 host/AZ·EBS, backup retention·DR RPO/RTO 또는 production 지원 선언으로 사용하지 않는다. 실제 결과와 미완료 gate는 [Cluster §11.23](cluster-session-and-delivery.md#1123-local-ext4-filesystem-수용-매핑)에서 관리한다. `go run ./tools/verify local`과 별도로 실행해야 한다.


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
두 runner 모두 한 번의 `go test ./...`로 수행한다. Test parallel limit은 `local` 8,
`ci` 2다. Local은 fixture가 격리된 cluster process 시험의 대기 시간을 겹쳐 실행한다.
CI는 mutation command도 함께 실행하므로 기존 상한을 유지한다. 기본 gate의
cluster process acceptance test는 `t.Parallel()`로 각 runner의 상한을 함께 사용한다.
각 시험의 data directory·인증서·포트·failpoint control은 독립적으로 유지한다.
장애 관찰 시간과 assertion은 줄이지 않는다. Opt-in 장애·release·WAL lifecycle 시험의
실행 조건과 순서는 유지한다.

Integration test process는 일반 broker와 `narumq_failpoint` broker를 build tag별로 한 번만
빌드한다. 공유 실행 파일은 `TestMain`이 만든 임시 directory에 두며, 모든 시험과 child
process 정리가 끝난 뒤 삭제한다. 다른 release의 `ldflags`나 historical source를 쓰는
빌드는 공유하지 않는다. 다음 `go test` 실행에서는 새로 빌드해 이전 실행 파일을 재사용하지 않는다.

Race detector는 실제 production package test (`./cmd/...`, `./internal/...`)에 적용한다.
Integration suite는 별도 child broker process를 빌드해 실행하므로 test binary에만 적용되는
race detector가 해당 broker process를 검사하지 않는다.

```text
go vet ./...
go run ./tools/verify traceability
go test ./... -parallel=8 -count=1 -covermode=atomic -coverpkg=./cmd/...,./internal/... -coverprofile=.artifacts/verification/coverage.out
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

### 8.1 2026-10-03 검증 게이트 실행 시간 개선

기본 cluster process 시험의 순차 실행과 같은 broker의 반복 빌드를 줄였다.
Broker runtime, 시험의 장애 관찰 시간, assertion, coverage 기준과 fuzz quota는 바꾸지 않았다.

Windows/amd64·Go 1.26.5에서 아래 통합 시험 명령을 변경 전후 각각 실행했다.
두 실행 모두 `GOMAXPROCS`는 설정하지 않았다.

```text
go test ./tests/integration -parallel=2 -count=1 -json -covermode=atomic -coverpkg=./cmd/...,./internal/...
```

| 확인 항목 | 변경 전 | 변경 후 |
| --- | --- | --- |
| 통합 시험 package 시간 | 372.966s | 209.303s |
| 명령 전체 시간 | 379.14s | 212.98s |
| 성공한 최상위 시험 | 51개 | 같은 51개 |
| Skip 목록 | 기존 opt-in 목록 | 같은 목록 |

Package 시간은 163.663s, 약 43.9% 줄었다. 변경 후 JSON event에서 동시에 실행한
최상위 시험은 최대 두 개였다. 공유 broker build 로그는 일반·failpoint 각각 한 건이었다.
원시 출력은 `.artifacts/verification/gate-speed-before.jsonl`과 `gate-speed-after.jsonl`에 보존했다.

공유 실행 파일의 수명과 fixture 격리는 실제 process 시험으로 확인했다.
Live QoS 0 forwarding, concurrent fresh read, concurrent CONNECT, stale epoch fencing
시험을 `-parallel=2 -count=2 -race`로 실행해 8/8 PASS였다(33.96s wall).
빌드는 build tag별 한 번이었고 `TestMain` 종료 뒤 두 실행 파일이 삭제된 것을 확인했다.
출력은 `.artifacts/verification/gate-speed-smoke.jsonl`에 보존했다.
이 race 실행은 test process를 검사하며 child broker의 race 검사를 대신하지 않는다.

전체 `local` gate는 `GOMAXPROCS=4`를 설정한 환경에서 PASS였다(241.324s wall,
integration 207.182s). Gofmt, vet, traceability, architecture, 전체 package/integration,
production package race, 네 fuzz target 각각 10,000회와 coverage를 확인했다.
Production line coverage는 85.98%, function coverage는 97.41%였다.
출력은 `.artifacts/verification/gate-speed-local.log`와 기존 coverage artifact에 보존했다.
CI 전용 mutation과 opt-in 외부 수용 시험은 이번 실행에서 수행하지 않았다.
위 실행 기록을 덧붙인 문서는 gate 종료 뒤 편집했으며 상대 링크를 별도로 확인했다.

### 8.2 2026-10-04 local 시험 동시 실행 상한 조정

`verify local`의 최상위 병렬 test 상한을 2에서 4로 늘린다. 시험 선택·횟수·장애 관찰 시간과 assertion은 바꾸지 않는다. 전체 suite의 `-count=1`, atomic coverage, production package race, 네 fuzz target의 quota와 coverage policy를 유지한다. CI의 추가 mutation과 resource 경합을 피하려고 `verify ci`는 기존 상한 2를 유지한다.

변경 전 CLU-13 local gate는 315.85s였으며 integration package는 261.824s였다. 이는 다른 검증 command를 이미 동시에 실행해도 integration이 전체 시간을 제한한다는 근거다. Broker binary는 이미 build tag별 한 번만 빌드하므로 새 cache는 추가하지 않는다. 시험의 data directory·인증서·포트 lease·failpoint control 격리를 재사용한다.

실행 시간 비교는 같은 Windows/amd64·Go 1.26.5·`GOMAXPROCS=4`에서 integration 전체를 `-parallel=2`와 `-parallel=4`로 실행한다. 성공·skip 목록과 공유 binary build 횟수를 함께 대조한다. 이후 실제 `verify local` PASS를 확인한다. 이 조정은 CPU·storage 자원이 다른 모든 host에서 같은 개선율을 보장하지 않는다.

다음 명령을 순서대로 실행했으며 두 실행 모두 PASS였다.

```text
go test ./tests/integration -parallel=2 -count=1 -json -covermode=atomic -coverpkg=./cmd/...,./internal/...
go test ./tests/integration -parallel=4 -count=1 -json -covermode=atomic -coverpkg=./cmd/...,./internal/...
```

| 확인 항목 | 변경 전: 상한 2 | 변경 후: 상한 4 |
| --- | --- | --- |
| Integration package 시간 | 254.498s | 147.265s |
| 명령 전체 시간 | 260.33s | 149.54s |
| 성공한 최상위 시험 | 55개 | 같은 55개 |
| 성공한 시험·하위 시험 목록 | 106개 | 같은 106개 |
| Skip 목록 | 8개 | 같은 8개 |
| 관측한 최상위 시험 동시 실행 수 | 최대 2개 | 최대 4개 |
| 공유 broker build | 일반·failpoint 총 2건 | 같은 2건 |

Integration package 시간은 107.233s, 약 42.1% 줄었다. JSON event의 성공·skip 이름 목록을 대조했다. 원시 출력은 `.artifacts/verification/local-speed-20261004-before.log`와 `local-speed-20261004-after.log`에 남겼다. 목록·시간·동시 실행 수는 `local-speed-20261004-comparison.json`에 보존했다. 각 상한을 한 번씩 비교한 관측값이며 모든 host의 개선율로 일반화하지 않는다.

상한 4 runner의 `cmd /c "set GOMAXPROCS=4&& go run ./tools/verify local"`도 exit 0, `local: PASS`였다. 전체 시간은 208.06s, integration package는 166.484s였다. 직전 CLU-13 gate의 315.85s와 비교하면 107.79s, 약 34.1% 줄었다. 전체 gate의 기준 실행은 앞서 수행한 [Cluster §11.31](cluster-session-and-delivery.md#1131-연결-rebalance-수용-매핑)이며, integration 비교처럼 연속 실행한 별도 기준 측정은 아니다.

Gofmt·vet·traceability·architecture·전체 package/integration·production package race·네 fuzz target 각 10,000회·coverage가 통과했다. Production line coverage는 85.66%, function coverage는 97.31%였다. `go build ./...`와 `go test ./tools/verify -count=1`도 PASS였다. Coverage policy는 변경하지 않았다. CI 전용 mutation과 기존 opt-in 외부 수용 시험은 실행하지 않았다. Branch coverage는 기존 도구 미설정 사유로 `not_evaluated`를 유지한다.

### 8.3 2026-10-04 local 상한 8 적용

사용자 결정으로 `verify local`의 동시 시험 상한을 4에서 8로 늘린다. CI는 2를 유지한다. 검증 command 수·시험 선택·횟수·timeout·장애 관찰 시간·assertion·coverage policy·race·fuzz quota는 바꾸지 않는다.

같은 Windows/amd64·Go 1.26.5·`GOMAXPROCS=4`에서 상한 8의 integration 전체를 먼저 실행했다. Package 97.517s, wall 99.64s, PASS였다. 상한 4의 147.265s보다 49.748s, 약 33.8% 짧았다. 성공한 시험·하위 시험 106개와 skip 8개 이름 목록은 같았다. 최상위 동시 실행 수는 최대 8개였고 공유 broker build는 2건이었다. 증거는 `.artifacts/verification/local-speed-20261004-parallel8.log`와 `local-speed-20261004-parallel8.json`이다.

이 실행은 한 번의 integration 관측이다. Crash/recovery 시험 하나는 상한 4의 53.06s보다 긴 63.61s였으므로 개별 시험의 속도가 모두 좋아진다고 판단하지 않는다. 기본값 적용 검증에서는 상한 8 integration을 반복하고, 실제 `verify local`의 다른 command와 함께 실행한 결과도 확인한다.

기본값 적용 뒤 `GOMAXPROCS=4`에서 `go test ./tests/integration -parallel=8 -count=2 -json -covermode=atomic -coverpkg=./cmd/...,./internal/...`를 실행했다. Package 189.973s, wall 191.99s, PASS였다. 성공한 시험·하위 시험 212개는 기존 106개를 각각 두 번 통과한 목록과 같았다. Skip 16개도 기존 8개를 두 번 실행한 목록과 같았다. 실패는 없었고 동시 실행 수는 최대 8개였다. 공유 broker build는 command 전체에서 2건이었다. Crash/recovery 시험은 두 회 각각 56.15s·57.92s였다.

반복 실행 원문과 목록 대조 결과는 `.artifacts/verification/local-speed-20261004-parallel8-repeat.log`와 `local-speed-20261004-parallel8-repeat.json`에 남겼다. `-count=2`는 한 test process에서 반복하며 각 fixture의 상태는 새로 만든다. 반복 안정성 증거로 쓰되 서로 다른 binary build를 쓰는 독립 성능 반복 측정으로 해석하지 않는다.

상한 8을 적용한 `cmd /c "set GOMAXPROCS=4&& go run ./tools/verify local"`도 exit 0, `local: PASS`였다. 전체 시간은 164.66s, integration package는 126.118s였다. 앞선 상한 4 gate의 208.06s보다 43.40s, 약 20.9% 짧았다. 두 전체 gate는 각각 한 번의 실행이며 다른 host에서 같은 개선율을 보장하지 않는다.

Gofmt·vet·traceability·architecture·전체 package/integration·production package race·네 fuzz target 각각 10,000회·coverage를 통과했다. Production line coverage는 85.80%, function coverage는 97.37%였다. `go build ./...`와 `go test ./tools/verify -count=1`도 PASS였다. CI 전용 mutation과 기존 opt-in 외부 수용 시험은 실행하지 않았으며 기준값은 유지했다.

전체 gate 출력은 `.artifacts/verification/local-speed-20261004-parallel8-local.log`에, 이 실행의 coverage 요약은 `local-speed-20261004-parallel8-coverage.json`에 보존했다. 기본 coverage profile과 요약은 기존 경로를 사용한다. 실행 시간과 반복 목록 대조 결과는 `local-speed-20261004-parallel8-repeat.json`에도 기록했다.


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
