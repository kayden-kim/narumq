# Performance와 verification 스펙

이 문서는 NaruMQ의 성능·신뢰성 요구, benchmark 판정 경계와 검증 증거를 정의한다. 수치가 확정되지 않은 release gate는 닫지 않는다.

이 문서의 수용 테스트 표는 시나리오 ID, 조건과 기대 결과만 둔다. 테스트 연결과 결과는 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인하고, 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md)에 둔다.

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 관련 요구사항 | DEP-19~DEP-23, NFR-01~NFR-06, NFR-11~NFR-15, PERF-01~PERF-12, TEST-01~TEST-17 |
| 근거 | [PRD 11장과 15장](../requirements/PRD.md) |
| 관련 구조·규칙 | [Broker core](../architectures/broker-core.md), [Cluster architecture](../architectures/cluster-architecture.md), [EKS on EC2 deployment](../architectures/eks-ec2-deployment.md), [Go coding conventions](../conventions/go-coding-conventions.md) |
| 관련 결정·계약 | [ADR-0001](../decisions/ADR-0001-core-implementation-language.md), [ADR-0007](../decisions/ADR-0007-aws-production-deployment-platform.md), [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md), [MQTT client compatibility v1](../contracts/mqtt-client-compatibility-v1.md) |

## 1. Workload 기준값

Phase 0은 workload를 다음 값과 단위로 확정해야 한다. 평균만으로 profile을 만들지 않는다. BP smoke 입력은 production workload의 대체값으로 사용하지 않는다. 2026-10-15 사용자는 production EMQX traffic 대신 `EKS_EC2_HA` 선택 profile에서 잰 수용 용량을 `B_*`로 쓰기로 했다. 측정 방법은 [§3.9](#39-bp-02-eks-용량-측정-계약)를 따른다.

| 기호 | 의미 | 필수 분해 요소 |
| --- | --- | --- |
| `B_conn` | 최대 동시 연결 수 | 프로토콜 버전, TLS 여부, 테넌트, keepalive |
| `B_pub` | 발행 속도(초당) | QoS, payload 크기, retained 여부, 영속화 수준 |
| `B_sub` | 수신 속도(초당) | QoS, fan-out 배수, shared 구독 여부 |
| `B_msg_size` | payload 크기 분포 | p50, p95, p99, 최대값 |
| `B_topic` | 토픽·필터 규모 | exact 매칭, wildcard, overlap, fan-out |

또한 persistent session 비율, offline 비율·시간, Session Expiry, reconnect ramp, 인증 backend latency, tenant 부하 편차와 AZ network 특성을 기록한다. §6에는 EMQX 대비 성능 비율 제안을 기록한다. 실제 `B_*` 값은 §3.9 측정 뒤 기록한다. Production SLO와 profile별 절대 수치 기준은 그 측정 결과를 근거로 확정한다.

## 2. SLI와 판정

| SLI | 측정 경계 |
| --- | --- |
| 연결 성공률 | TCP/TLS 시도부터 성공 CONNACK까지 (인증·프로토콜 거절은 사유별 분리) |
| 발행 ACK 지연시간 | PUBLISH 송신부터 PUBACK 또는 PUBREC 수신까지 |
| 종단 간 전달 지연시간 | publisher 송신 시각부터 subscriber PUBLISH 수신까지 |
| 처리량 | 측정 구간 안의 성공 발행 및 전달 건수/초 |
| 메시지 유실(gap) | 성공한 durable ACK 메시지 중 기한 안에 수신되지 않은 건수 |
| 중복 수신 | 동일한 테스트 메시지 ID의 허용 기준 초과 수신 건수 |
| 장애 복구 RTO | 장애 주입부터 새 연결·세션의 성공 처리 회복까지 |
| 드레인 소요 시간 | 드레인 시작부터 SAFE_TO_STOP 관찰까지 |
| 리소스 효율성 | 연결/세션/구독당 메모리 사용량 및 메시지당 CPU·네트워크·디스크 사용량 |

Latency는 p50, p95, p99, p99.9를 보고한다. 오류 sample을 제외하면 오류율과 제외 규칙도 기록한다. Coordinated omission을 피하는 load model을 쓰며 clock 오차 또는 단일 clock 측정 경계를 남긴다.

## 3. Benchmark 공통 계약

비교 harness는 동일한 generator와 고정 workload를 사용하고 target만 바꾼다. 다음 조건을 지킨다.

1. Node 수, CPU/memory limit, storage class·volume, network와 AZ 배치를 맞춘다.
2. MQTT version, TLS, 인증, ACL, QoS, durability, session, topic, payload와 subscription 분포를 기능적으로 맞춘다.
3. 맞출 수 없는 차이는 `nonEquivalent`로 남기고 직접 우열 판정에서 제외한다.
4. Generator는 broker와 분리하고 client 구현, seed, ramp와 message stream을 동일하게 한다.
5. 고정 warm-up·측정 구간을 두고 최소 5회 독립 실행한다. Suite는 target 순서를 교차한다.
6. 중앙값과 MAD를 함께 보고하고 broker·generator 오류와 saturation을 모두 수집한다.

고정 production comparator는 EMQX Enterprise 6.3.1이며 exact edition·image digest는 [PRD §18 질문 15](../requirements/PRD.md#18-미결정-사항)에 있다. 기존 BP-01/BP-02/BP-04/BP-05 local smoke의 OSS 5.8.8과 BP-03/BP-06/BP-07 local smoke의 Enterprise 5.8.8은 과거 smoke 기준이다. 과거 artifact와 결과는 수정하지 않는다. 2026-10-16 사용자 결정에 따라 상용 EMQX Enterprise 라이선스를 요구하는 상대 비교는 필수 출시 차단 게이트에서 제외하고 선택적 외부 벤치마크로 운영한다. 필수 릴리스 게이트는 NaruMQ 자체의 5% 회귀 차단 규칙과 `EKS_EC2_HA` qualification profile에서 측정한 자립형 수용 용량(`B_*`)으로 판정한다.

Qualification과 BP-02 release comparison profile은 [ADR-0007](../decisions/ADR-0007-aws-production-deployment-platform.md)의 ARM64 EKS 선택값을 따른다. 이 선택만으로 production gate를 통과하지 않는다. Production gate에는 실제 `B_*`, native ARM64 broker·generator image, full profile digest와 AWS/EBS 수용 증거가 필요하다. [`tests/benchmark/Dockerfile`](../../tests/benchmark/Dockerfile)은 `TARGETOS`/`TARGETARCH`(`amd64` 기본값, `arm64` 지원)로 broker와 runner 바이너리를 빌드한다. 실제 EKS 실행에는 ARM64 이미지를 빌드·게시하고 immutable digest를 고정해야 한다. 조건을 충족하지 않은 artifact는 `releaseGating=false`로 둔다.

Profile SHA-256은 저장소에 커밋된 LF bytes로 계산한다. `.gitattributes`의 `tests/benchmark/*/testdata/*.json text eol=lf` 규칙으로 `core.autocrlf=true`인 Windows checkout에서도 같은 bytes를 쓴다. BP-06 profile의 LF digest는 `ca5e28b04623e708d9521ab34ce5ed3b9dd617d4ac57c4571243bdd6061f6a15`다. 과거 artifact의 값은 실행 당시 사실이므로 고치지 않는다. 2026-10-08 이전 BP-06 artifact의 digest는 [검증 기록](../verification/performance-and-verification.md#3-benchmark-공통-계약-기록)에 있다.

| BP | 프로파일 | 스펙 위치 |
| --- | --- | --- |
| BP-01 | Protocol Baseline | §3.1 |
| BP-02 | Production Mix | §3.8, EKS 용량 측정 §3.9 |
| BP-03 | Durable Delivery | §3.4 |
| BP-04 | Subscription Stress | §3.7 |
| BP-05 | Connection Scale | §3.2 |
| BP-06 | Failure Recovery | §3.6 |
| BP-07 | Rolling Drain | §3.5 |

### 3.1 BP-01 Protocol Baseline 관찰 경계

BP-01은 MQTT 5.0, QoS 0, 비영속 session(`Clean Start=true`, expiry 0), exact topic, publisher 1개와 subscriber 1개, 고정 payload를 사용한다. Runner는 broker container의 network namespace에서 `127.0.0.1:1883`로 연결한다. NaruMQ와 OSS EMQX 5.8.8은 같은 CPU/memory limit과 read-only root filesystem으로 순차 실행한다.

측정은 subscriber SUBACK 성공 뒤 시작한다. Runner의 monotonic clock에서 publisher 송신부터 subscriber 수신까지 잰다. Warm-up은 통계에서 제외한다. Measure의 `sent == delivered`이고 duplicate·missing·protocol error·generator overload가 모두 0일 때 target valid다. EMQX CONNACK에 추가 MQTT 5 properties가 있으면 BP-01 전용 `readWireFrame`·`readConnAck`가 property 길이 일관성만 확인한다. 다른 workload packet은 domain codec을 쓴다.

이 smoke는 하네스와 실행 경로만 입증한다. Phase 0 workload, SLO, release gate를 정하지 않는다. 필수 raw sample과 phase, runtime metric은 §5에 정의한다.

#### 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 아래 시나리오의 시험은 `Valid`에 송신 schedule lag 한도가 들어가는 in-process 부하 실행이다. 기본 gate는 이 시험을 실행하지 않고 `Acceptance: NARUMQ_LOAD_QUALITY`로 표시했다. BP-05의 in-process 실행(`PRF-SCN-123`)도 같다. 두 시험은 `NARUMQ_LOAD_QUALITY=1 go run ./tools/verify acceptance --run TestRunAgainstInProcessNaruMQ`로 함께 실행한다([ADR-0064](../decisions/ADR-0064-ci-gate-logic-only.md), [§7.2](#72-fuzz-게이트와-수동-mutation-검증)).

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-122 | - | BP-01 runner를 in-process NaruMQ에 smoke profile(초당 20건, warm-up 25ms, measure 250ms, drain 250ms)로 실행한다. | `Run`이 성공하고 summary가 valid다. 송신 수가 0보다 크고 수신 수와 같으며 protocol error가 0이다. `samples.jsonl`이 비어 있지 않다. `phases.jsonl`은 warm-up, measure, drain의 start·end를 이 순서로 담는다. |

### 3.2 BP-05 제한된 Connection Scale smoke 관찰 경계

고정 profile은 `tests/benchmark/bp05/testdata/smoke.json`이다.

| 필드 | 값 |
| --- | --- |
| `connectionCount`, `rampConnectionsPerSecond` | 64, 32 |
| `baselineMillis`, `idleMillis` | 3000, 4000 |
| `reconnectCount`, `reconnectRatePerSecond` | 16, 16 |
| `keepAliveSeconds`, `pingIntervalMillis` | 2, 500 |
| 브로커 / 제너레이터 리소스 제한 | 1 CPU / 512 MiB; 1 CPU / 256 MiB |
| 프로토콜 / 전송 / 세션 | MQTT 5.0, 루프백 평문, Clean Start, 만료 0초 |

Client ID는 `bp05-000000`부터 고정 폭으로 쓴다. Initial ramp 뒤 각 성공 연결에서 PINGREQ를 분산해 보내고, 이후 정해진 간격에 PINGRESP 하나를 확인한다. Idle 뒤 가장 낮은 ID 16개를 정상 종료하고 같은 ID로 reconnect한다. 연결과 phase는 각각 `connections.jsonl`, `phases.jsonl`에 즉시 기록한다.

Broker memory baseline은 runner 시작 전 sample의 median, plateau는 `idle` phase sample median이다. 짝수 표본 median은 `lower + (upper-lower)/2`로 계산한다.

```text
incrementalMemoryBytes = plateauMemoryBytes - baselineMemoryBytes
memoryBytesPerIdleConnection = incrementalMemoryBytes / idleSurvivors
```

차이는 signed 값이며 음수로 clamp하지 않는다. 각 구간 sample이 2개 미만이면 resource invalid다. `summary.json`은 workload·resource가 모두 valid이고 양쪽 target의 마지막 ping도 성공해야 valid다. Smoke는 전체 BP-05, `B_conn`, production memory SLO, release 비율 또는 Gate 8을 확정하지 않는다.

#### 수용 테스트

이 절의 in-process 실행도 [§3.1](#31-bp-01-protocol-baseline-관찰-경계)과 같이 gate 밖의 `Acceptance: NARUMQ_LOAD_QUALITY` 시험이다. 시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-123 | - | BP-05 runner를 in-process NaruMQ에 연결 8개, 초당 ramp 8개, baseline 2000ms, idle 2000ms, reconnect 2개, keep-alive 2초, ping 간격 400ms로 실행한다. | `Run`이 성공하고 summary가 valid다. 초기 연결 8개가 성공하고 idle 생존이 8개이며 reconnect 2개가 성공한다. Ping·protocol·transport·마지막 ping 오류와 generator overload가 0이다. `connections.jsonl`, `phases.jsonl`, `workload-summary.json`이 비어 있지 않고 phase는 ramp, idle, reconnect, final_check, cleanup의 start·end 순서다. |

### 3.3 PERF-07 반복 suite 측정 계약

단일 `compare`는 독립 실행 1회다. 각 package의 `suite`가 반복 측정을 소유한다. 저장소 root에서 다음 명령 형식을 쓴다.

```text
go run ./tests/benchmark/bp01 suite --profile <path> [--output <root>] [--repetitions <n>]
go run ./tests/benchmark/bp02 suite --profile <path> [--output <root>] [--repetitions <n>]
go run ./tests/benchmark/bp03 suite --profile <path> [--output <root>] [--repetitions <n>]
go run ./tests/benchmark/bp04 suite --profile <path> [--output <root>] [--repetitions <n>]
go run ./tests/benchmark/bp05 suite --profile <path> [--output <root>] [--repetitions <n>]
go run ./tests/benchmark/bp06 suite --profile <path> [--output <root>] [--repetitions <n>]
go run ./tests/benchmark/bp07 suite --profile <path> [--output <root>] [--repetitions <n>]
```

기본 repetitions는 5이며 5 미만은 artifact 생성 전에 거부한다. 각 회차는 같은 profile과 immutable NaruMQ·runner image를 쓰지만 새 target container와 run directory를 사용한다. 순서는 `NaruMQ, EMQX`; `EMQX, NaruMQ`를 교차한다. 시작·종료 시각, `runId`, 성공 여부와 실패 이유를 parent `suite-manifest.json`에 기록한다. 원래 명령은 child manifest에도 보존한다.

```text
median = 정렬한 값의 가운데 값 (짝수이면 가운데 두 값의 평균)
MAD = median(|value - median|)
```

정렬은 metric 계산용 복사본에만 적용한다. Suite root는 `.artifacts/benchmark/<benchmark>/<suite-id>/`이며 profile, manifest, summary, `runs/001/`부터 child artifact를 보존한다. Parent는 BP별 metric과 NaruMQ runtime metric만 집계하며 target 비율·승자·합격 threshold를 계산하지 않는다.

준비 단계·manifest 기록처럼 추적 불가능한 실패는 즉시 중단하고 이유를 남긴다. 추적 가능한 child 실패는 후속 회차 artifact를 보존하되 suite를 invalid로 하고 parent metric을 비운다. NaruMQ GC CPU metric의 명시된 `unavailable` applicability는 suite 실패가 아니며 해당 CPU metric만 생략한다. 모든 smoke artifact는 `releaseGating=false`다.

#### 반복 실행 불변 조건

재시도는 정해진 수용 경계를 완화하거나 결과를 고르는 데 쓰지 않는다. 한 회차가 invalid면 parent suite도 invalid이며 성공한 일부 회차의 median/MAD를 합격 근거로 표시하지 않는다. BP-06에서 정의한 comparator crash 예외 외에는 invalid 결과를 사후에 valid로 바꾸지 않는다.

### 3.4 BP-03 Durable Delivery smoke 관찰 경계

BP-03의 runner completion·ACK latency 경계는 QoS 1의 PUBACK, QoS 2의 최종 PUBCOMP다. 이는 표의 공통 SLI인 첫 ACK 경계(PUBACK/PUBREC)와 다르므로 결과에서 구분한다.

#### Topology와 비교 경계

- NaruMQ는 `narumq-bp03` dynamic cluster 3 node와 Docker named volume을 쓴다. Durable ACK는 quorum-replicated Raft WAL 처리를 포함한다.
- EMQX는 Enterprise 5.8.8, image index digest `sha256:d166efa91eae706ffbfca3f47e36fae0fd441507a8a57bda622da09e4a019c14`의 3-node static cluster다. `durable_sessions.enable=true`, `durable_storage.messages.backend=builtin_raft`, `n_sites=3`, `replication_factor=3`, `n_shards=1`을 고정한다. 이 구성은 session metadata 저장과 ACK/fsync 경계가 NaruMQ와 같다는 뜻이 아니다. Evaluation license는 최대 25 concurrent sessions이며 운영 license나 운영 승인이 아니다.
- 두 target은 같은 profile·CPU/memory·runner·payload·stream·rate·phase를 쓴다. Broker는 read-only root filesystem, runner는 별도 container다.
- NaruMQ JWT/JWKS 인증과 peer mTLS, EMQX JWT 및 `bp03/#` file ACL을 쓴다. Secret은 artifact에 두지 않는다.
- 세 node replicated message storage가 있어도 batch·session metadata·ACK와 fsync 순서는 동등하지 않다. `nonEquivalent`에 차이를 기록하고 ACK latency를 직접적인 우열 비교에서 제외한다. `durable_sessions.enable`만으로 quorum durability를 주장하지 않는다.
- Docker named volume은 Linux/EBS, 실제 전원 손실, production volume loss/reseed, 승인된 RPO/RTO를 입증하지 않는다.

#### 고정 smoke profile

`tests/benchmark/bp03/testdata/smoke.json`은 strict decode한다. 범위는 안전한 smoke 한도이며 production SLO가 아니다.

| 필드 | 값 | 허용 범위 |
| --- | --- | --- |
| `profileId` / `purpose` / `seed` | `bp03-smoke-v1` / `smoke` / `3` | 공백 불가 / `smoke` 전용 / `uint64` |
| `pairsPerQoS` / `payloadBytes` | 2 / 256 | payload 16–65,536 bytes |
| `sessionExpirySeconds` | 300 | 1–86,400초 |
| `maxInflightPerPublisher` | 8 | 1–32 |
| `ackTimeoutMillis` | 10,000 | 1,000–60,000 ms |
| `ratesPerStreamPerSecond` | `[5,10,25]` | 1–8개 엄격 오름차순, 1–1,000 |
| `warmupMillis` / `measureMillis` / `drainMillis` | 2,000 / 12,000 / 5,000 | warmup 1,000–60,000 ms; measure 3,000–120,000 ms; drain 1,000–30,000 ms |
| `offlineSettleMillis` / `offlineMessagesPerStream` | 1,000 / 32 | 100–10,000 ms / 1–64 |
| `reconnectTimeoutMillis` / `maxMessages` | 20,000 / 4,096 | 1,000–120,000 ms / 1–1,000,000 |
| 브로커 CPU/메모리 | 1 / 1,073,741,824 | 유한한 양수 CPU / 양수 바이트 |
| 제너레이터 CPU/메모리 | 2 / 536,870,912 | 유한한 양수 CPU / 양수 바이트 |

`maxMessages`는 runner가 보유하는 총량 제한이다. Publisher·subscriber는 서로 다른 cluster node를 사용한다. Persistent subscriber가 끊긴 동안 QoS stream별 32개를 발행하고 다른 node로 `CleanStart=false` 재접속한다. `SessionPresent=true`와 전체 offline 전달을 확인한다.

#### Phase, validity와 WAL

Phase 순서는 `readiness`, `setup`, rate별 warmup/measure/drain, offline, reconnect, cleanup이다. Readiness는 NaruMQ health·cluster ID·공통 leader를 확인하고 두 target 모두 6개 cross-node pair QoS 1 probe를 수행한다. EMQX는 실제 pinned 5.8.8 process의 `emqx:is_running()`, `running_nodes()`, authorization/JWT hook과 `builtin_raft`의 3-site replica 구성·비전이 상태를 확인한다. Initialization이 끝나지 않으면 startup deadline 안에서 기다리며 memory-session fallback을 허용하지 않는다.

Subscriber는 persistent expiry, `CleanStart=true`; publisher는 clean session과 expiry 0을 쓴다. Absolute schedule과 inflight cap을 적용한다. QoS 1 완료는 PUBACK, QoS 2 완료는 PUBCOMP다. Measure 시작 전 warmup exchange를 완료하거나 timeout 처리한다. Measure 경계와 offline 구간에서 publisher ACK가 완료되거나 timeout된 뒤 scrape한다. Drain은 새 publish를 멈추고 ACKed message delivery를 제한 시간까지 기다린다.

Offline disconnect 뒤 settle하고 각 QoS stream에서 32개를 발행한다. Reconnect의 단일 deadline에는 연결 시도도 포함한다. SessionPresent false는 실패다. QoS 2의 pending/completed Packet ID 상태는 socket이 아니라 persistent session에 속한다. 재접속 뒤 이미 완료한 PUBREL에는 PUBCOMP를 보내되 delivery를 재집계하지 않는다. Unknown Packet ID나 잘못된 packet은 protocol error다.

Workload valid 조건:

- 모든 measure·offline ACKed 교환 완료, QoS 2 `PUBCOMP`, gap 0.
- QoS 2 duplicate, protocol error, transport error, rejected ACK, ACK timeout, overload가 0.
- 모든 persistent session 복원, 예상한 offline receipt 수와 `SessionPresent=4/4`.
- 모든 phase 완료. Reader 실패 시 최초 원인을 보존하며 후속 DNS/dial 취소를 최초 원인으로 오인하지 않는다.
- `schedule lag >= stream interval`이면 overload다. 이 값만으로 runner 포화, broker backpressure 또는 quorum failure의 원인을 단정하지 않는다.

NaruMQ `wal-summary.json`은 `narumq_raft_wal_sync_duration_seconds`, `narumq_raft_wal_sync_entries`, `narumq_raft_wal_written_bytes_total`을 node별·cluster별 기록한다. 각 window에 전체 series와 성공한 Ready WAL fsync가 하나 이상 있어야 valid다. ACK 분모는 해당 window publisher exchange 완료/timeout 후 고정한다. EMQX에는 NaruMQ Ready WAL metric이 없으므로 `not_applicable`과 이유를 기록하며 0으로 가장하지 않는다. 이는 EMQX에 WAL이 없다는 뜻이 아니다.

현재 NaruMQ는 [ADR-0020](../decisions/ADR-0020-asynchronous-raft-wal-group-commit.md)의 async WAL single-writer group commit을 사용한다. Driver의 8ms timed collection은 제거됐고, command별 fresh ReadIndex 및 commit-only HardState sync는 유지한다. Local WAL 완료 전에 entry-bearing replication을 내보내지 않는다. `CompleteApply`는 runtime state와 projection 적용 뒤 호출한다. Ready histogram은 fsync를 포함하지 않는 poll 시간이므로 예전 Ready/fsync 비율과 직접 비교하지 않는다. Group commit histogram은 성공한 group sync당 표본으로, 그 sync에 포함된 frame의 entries/bytes 합계를 보고한다.

#### Raft 병목 진단

`narumq_raft_stage_duration_seconds{stage}`는 다음 7개 고정 stage를 기록한다. Stage label은 고정 목록만 허용하며 client, tenant, payload 값을 넣지 않는다.

| Stage | 측정 경계 |
| --- | --- |
| `inbound_queue` | 인증된 frame enqueue부터 Raft driver가 인수할 때까지 |
| `commit_queue` | commit 제출 시도부터 driver가 제출 request 처리를 시작할 때까지. Channel 대기와 인수한 FIFO head의 publish-control 대기를 포함 |
| `ready` | 동기식 Raft Ready poll/처리 시간. Async WAL에서는 fsync 대기 시간을 포함하지 않는다 |
| `apply` | application 상태 적용 |
| `send` | transport enqueue |
| `read_index` | driver ReadIndex 시작부터 local apply/projection 확인 또는 실패까지 |
| `proposal` | proposal 시작부터 application apply/projection 완료 응답 또는 driver 실패까지 |

완료된 작업만 histogram에 기록하며 실패도 포함한다. Client timeout 뒤 실제 apply는 별도 사건이므로 stage histogram만으로 성공 ACK를 판정하지 않는다. Histogram stage 합계는 동시 작업이 겹칠 수 있어 wall time이나 한 ACK의 지연으로 더하지 않는다.

Runner는 readiness 완료 뒤부터 cleanup 직전까지 node별 `/metrics` 원문을 1초 간격으로 수집한다. 요청 시작·종료 UTC 시각, 원문 경로 또는 scrape 오류를 `diagnostics.jsonl`에 기록한다. 실패 종료에도 마지막 scrape를 시도하며 오류를 기록한다. 이 진단 자료는 measure window의 WAL summary와 분리하고 workload validity를 대신하지 않는다.

원인 분석은 같은 구간의 stage histogram delta, WAL sync 시간, peer 포화 log를 대조한다. 높은 fsync 비율만으로 queue 포화의 원인을 확정하지 않는다. 계측이 지목한 경로만 조사하며 command별 fresh proof, commit-only HardState sync, Packet ID와 epoch fencing을 생략하지 않는다.

Raft stage histogram, proposal 전달과 fresh read 경로의 시나리오는 아래 BP-03 수용 테스트의 PRF-SCN-008~PRF-SCN-015다.

#### Readiness 진단 계약

Readiness는 storage 검사, fresh quorum proof, local snapshot 판정을 기존 순서로 수행하며 기존 fail-closed 반환 오류를 유지한다. 진단은 결과를 기다리거나 재판정하지 않는다.

- `purpose`: `mqtt`, `health`, `summary`, `backup`, `drain`만 사용한다. Management summary의 quorum 조회는 MQTT admission으로 분류하지 않는다.
- `readiness_stage`: `readiness_storage`, `readiness_fresh_read`, `readiness_local_state`만 사용한다. 단계별로 실제로 평가한 값만 기록하고 미실행 판정값을 채우지 않는다.
- Fresh proof 거절은 오류 분류를 기록한다. Local-state 거절은 첫 불충족 조건, 모든 판정 flag, proof/applied/projection index를 기록한다. 동시 실패 조건은 따로 유추하지 않는다.
- Local snapshot 앞뒤 `ready_sequence_before`·`ready_sequence_after`를 기록한다. 홀수는 Ready WAL/apply/projection 구간이며, 값이 달라지면 snapshot 수집 중 해당 구간이 변했다는 뜻이다. 이 사실만으로 기다리거나 재판정하지 않는다.
- Storage 값은 원래 검사 시점의 값이다. 이후 값으로 다시 sample하지 않는다. 동일 purpose·stage·거절 분류는 최초 건과 1초 간격 표본만 출력하고 건수를 누적한다. 표본 log는 개별 CONNECT 기록이 아니다.
- Client/tenant ID, payload, topic, token을 기록하지 않는다. Retry, checkpoint 대기, proof 재사용, ACK 경계 변경을 하지 않는다.

Readiness 진단의 시나리오는 PRF-SCN-016과 PRF-SCN-017이다. 수집 경계와 log rate limit은 이 절의 계약이며 stage histogram 시험과 구분한다.

Target은 `phases.jsonl`, `messages.jsonl`, `workload-summary.json`, `resource-summary.json`, `wal-summary.json`, runner/broker log, redacted inspect와 broker/network-owner stats를 보존한다. NaruMQ의 raw metric은 `metrics/` 아래에 둔다. `diagnostics.jsonl`은 node·요청 시각·raw 경로 또는 scrape 오류를 잇는다. Failure에도 최종 scrape를 시도한다. Resource window에서 source/node 표본이 2개 미만이면 invalid다. Block write가 0인 표본은 유효하다. Network owner delta가 모두 0이면 `networkStatsAvailable=false`로 기록하고 workload를 invalid로 하지 않으며, suite에서 해당 network metric만 생략한다. Stats는 host clock, phase는 runner clock이므로 `clockSource`에 둘 다 기록한다.

Suite metric key는 `stepNN_qQ_ack_latency_p99_ns`, `stepNN_qQ_ack_latency_p999_ns`, `stepNN_qQ_delivery_latency_p99_ns`, `stepNN_qQ_ack_rate_per_second`와 `stepNN_block_write_bytes_per_acked_message`다. Network 표본이 있으면 `stepNN_network_tx_bytes_per_acked_message`를 추가한다. NaruMQ 전용 key는 `stepNN_wal_syncs_per_second`, `stepNN_wal_mean_entries_per_sync`, `stepNN_wal_bytes_per_acked_message`다. `offline_reconnect_drain_millis_max`는 양 target metric이다. EMQX의 NaruMQ WAL key는 `not_applicable`이며 metric으로 0을 만들지 않는다.

#### BP-03 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#34-bp-03-durable-delivery-기록)에 있다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-001 | - | BP-03 profile에 누락·알 수 없는 field, trailing JSON, overflow와 모든 범위 밖 값을 입력한다. profile 원문을 읽는다. | Profile을 strict decode하고 message 상한을 계산한다. 누락·알 수 없는 field, trailing JSON, overflow와 범위 밖 값은 거절한다. 원문 bytes와 SHA-256을 그대로 반환한다. |
| PRF-SCN-002 | - | Stream을 node에 배치하고 발행자 reader 실패와 ACK timeout을 입력한다. | Stream마다 노드를 교차 배치한다. Reader 실패는 slot 대기가 반환한다. ACK timeout이 난 Packet ID는 격리한다. |
| PRF-SCN-003 | - | Subscriber reader가 QoS 2 PUBREL 흐름을 이어서 처리한다. | QoS 2 release 흐름을 재개한다. |
| PRF-SCN-004 | - | ACK됐지만 수신하지 못한 message, QoS 1·2 duplicate, 스케줄 지연, drain snapshot, QoS 2 PUBCOMP timeout과 offline 완료 경계를 입력한다. | ACK됐지만 받지 못한 message는 gap이며 invalid다. QoS 2 duplicate는 invalid다. QoS 1 duplicate는 보고하되 허용한다. 스케줄 지연이 간격과 같으면 overload다. Snapshot은 이후 ACK를 관찰하지 않는다. PUBCOMP timeout은 미완료 교환으로 센다. Offline 완료는 QoS 2 PUBCOMP와 기대한 모든 수신을 기다리며 기한을 놓치면 실패한다. |
| PRF-SCN-005 | - | WAL histogram의 bucket delta, overflow, 누락된 family와 EMQX target을 입력한다. | Bucket delta와 overflow를 반영해 WAL summary를 만든다. 누락된 family는 invalid summary로 남긴다. EMQX는 `not_applicable`이다. |
| PRF-SCN-006 | - | Resource window의 표본 부족, 0 byte block write와 network owner 표본 없음을 입력한다. | 표본이 부족하면 invalid다. 0 byte block write는 유효하다. Network 표본이 없어도 workload는 invalid가 아니다. |
| PRF-SCN-007 | - | 두 target의 compare와 suite를 만든다. 동등성 증거, EMQX WAL 적용성, network 표본이 없는 실행과 실패한 하위 실행을 입력한다. | 동등성과 `nonEquivalent` dimension을 판정한다. EMQX WAL 적용성은 요구하지 않는다. 필수 key와 단위를 만든다. 한 실행이라도 network 표본이 없으면 network metric만 생략한다. 하위 실행이 하나라도 실패하면 모든 상위 metric을 비운다. |
| PRF-SCN-008 | - | 7개 고정 Raft stage와 허용되지 않은 label, histogram overflow를 기록한다. | 고정 stage만 허용한다. Stage별 histogram이 서로 격리되고 overflow는 상한 안에 둔다. |
| PRF-SCN-009 | - | 동적 cluster의 follower가 application proposal을 받는다. | Follower가 proposal을 leader로 전달한다. |
| PRF-SCN-010 | - | Commit read proof를 여러 개 제출하고 일부를 취소한다. | 제출 순서를 보존한다. 취소를 올바르게 처리한다. |
| PRF-SCN-011 | - | Follower에서 fresh read를 한꺼번에 요청한다. | Egress가 drain된 뒤 모든 read가 완료된다. |
| PRF-SCN-012 | - | Leader에서 fresh read를 한꺼번에 요청하고 소수 node의 응답을 막는다. | 정상 quorum을 유지한 채 모든 read가 완료된다. |
| PRF-SCN-013 | - | 아직 발행하지 않은 read를 취소하고 driver 실패를 일으킨다. | 취소와 driver 실패를 각각 호출자에게 반환한다. |
| PRF-SCN-014 | - | TCP outbound capacity를 점유한 frame이 wire에 완전히 나가기 전 상태를 만든다. | Wire 전송이 끝날 때까지 capacity가 계속 차감된다. |
| PRF-SCN-015 | - | 실제 process cluster에서 fresh read를 동시에 요청한다. | 모든 read가 인수 시험 기준대로 완료된다. |
| PRF-SCN-016 | - | Readiness의 storage, fresh quorum proof와 local snapshot 판정이 각각 실패한다. | 단계별 거절 사유를 보고하고 기존 fail-closed 반환 오류를 유지한다. |
| PRF-SCN-017 | - | 같은 purpose·stage·거절 분류의 readiness 실패가 반복된다. | 최초 건과 1초 간격 표본만 출력하고 건수를 누적한다. Diagnostic field에 Client/tenant ID, payload, topic, token을 넣지 않는다. |

저장소 root에서 실행한다.

```text
go test ./tests/benchmark/bp03 -count=1
go run ./tests/benchmark/bp03 compare --profile tests/benchmark/bp03/testdata/smoke.json
go run ./tests/benchmark/bp03 suite --profile tests/benchmark/bp03/testdata/smoke.json
```

#### Smoke 결과의 해석 경계

Suite는 고정 local smoke의 반복 증거이며 production SLO, direct target superiority, ADR-0003 전체 또는 production acceptance가 아니다. `releaseGating=false`, `ackLatencyDirectlyComparable=false`를 유지한다.

### 3.5 BP-07 Rolling Drain smoke 관찰 경계

BP-07은 같은 연결 수를 유지한 3-node cluster에서 node 하나씩 drain하고 같은 volume으로 재시작한다. Retained workload와 최대 traffic에서 연속 100회 rolling은 별도 범위다. 모든 artifact `releaseGating=false`; `SPEC-TBD-08`의 comparator 수치 제안은 미채택이며 실제 workload와 profile별 cutoff도 미정이다.

#### Topology와 비교 경계

- Docker bridge topology에서 node마다 network-owner와 named volume을 둔다. DNS는 `node{1,2,3}.bp07.test`다.
- NaruMQ는 `narumq-bp07` dynamic cluster, tenant·subject `bp07`·`bp07-client`, ACL `bp07/#`를 쓴다. EMQX는 BP-03과 같은 pinned Enterprise 5.8.8 image를 사용한다.
- 두 target은 workload, resource limit, client, seed와 schedule을 공유한다.
- Controller는 atomic JSON marker를 `.tmp` 파일 작성 뒤 rename한다. Windows에서 permission/sharing 오류가 나면 같은 rename만 10ms 간격으로 최대 1초 재시도한다. 다른 오류는 바로 실패한다. Round 번호는 감소하지 않는다.
- NaruMQ는 drain API에서 `SAFE_TO_STOP`, `COMPLETED`, `safeToStop=true`를 모두 확인한 뒤 같은 container를 재시작한다. EMQX는 evacuation을 시작하고 `prohibiting` 상태를 확인한 뒤 재시작한다. Cluster readiness 뒤 rebalance를 중지하고 `disabled` 상태를 확인한다.
- EMQX 5.8.8의 rebalance start/stop은 종료 코드 0 또는 1을 허용하되 성공 문구와 실제 상태를 모두 확인한다. Eval은 종료 코드 0이어야 한다.
- Drain 시간은 host monotonic clock으로 시작 호출 직전부터 완료 최초 관찰까지 잰다. Restart-ready 시간은 재시작 시작부터 준비 관찰까지 잰다.

Drain barrier는 두 제품에서 다르므로 drain metric으로 우열을 판정하지 않는다. `comparison.json`은 `drainTimeDirectlyComparable=false`를 기록한다. NaruMQ Will은 지원 계약으로 판정하고 EMQX Will은 관찰값만 기록한다.

| `nonEquivalent` dimension | 비교 경계 |
| --- | --- |
| `drainBarrier` | NaruMQ는 replicated `SAFE_TO_STOP` barrier에서 완료한다. EMQX는 고정 takeover 대기 뒤 `prohibiting` 상태에 도달한다. |
| `durability` | EMQX Durable Storage batch/session persistence와 NaruMQ quorum WAL fsync-before-ACK 동등성은 검증되지 않았다. |
| `authentication` | 둘 다 JWKS JWT를 검증하지만 EMQX fixture는 NaruMQ tenant/client-ID binding과 peer mTLS를 재현하지 않는다. |
| `willContract` | NaruMQ는 ADR-0028에 따라 Will 결과를 검증한다. EMQX는 Will delivery를 기록만 한다. |

#### Smoke profile과 workload

`tests/benchmark/bp07/testdata/smoke.json`은 모든 필드를 요구하며 모르는 필드와 뒤따르는 JSON을 거절한다. 수용 범위는 `tests/benchmark/bp07/profile.go::Profile.Validate`를 따른다.

| 필드 | Smoke 값 | 허용 범위·조건 |
| --- | --- | --- |
| `profileId` / `purpose` / `seed` | `bp07-smoke-v1` / `smoke` / 7 | 공백 불가 / `smoke`만 / `uint64` |
| `payloadBytes` / `publishRatePerStreamPerSecond` | 128 / 10 | 16–65,536 / 1–100 |
| `maxInflightPerPublisher` / `ackTimeoutMillis` | 8 / 10,000 | 1–32 / 1,000–60,000 ms |
| `sessionExpirySeconds` / `keepAliveSeconds` | 300 / 30 | 60–86,400 / 5–300 s |
| `idleClients` / `willDelaySeconds` | 7 / 60 | 0–8 / 1–3,600 s |
| `warmupMillis` | 5,000 | 1,000–600,000 ms |
| `replacementOrder` / `cycles` | `[3,1,2]` / 1 | node 1,2,3의 순열 / 1–10 |
| `connectionRatePerSecond` / `lbPropagationMillis` | 10 / 2,000 | 1–10,000 / 1,000–3,600,000 ms; LB 값은 1,000 ms의 배수 |
| `drainTimeoutMillis` / `emqxWaitTakeoverSeconds` | 120,000 / 5 | 1,000–86,400,000 ms, > LB 전파 / 1–600 s |
| `brokerStopTimeoutSeconds` / `readyTimeoutMillis` | 10 / 180,000 | 1–60 s / 1,000–600,000 ms |
| `interRoundSettleMillis` / `willSettleMillis` | 3,000 / 2,000 | 0–600,000 / 0–60,000 ms |
| `reconnectTimeoutMillis` / `healthPollMillis` | 30,000 / 250 | 1,000–600,000 / 50–5,000 ms |
| `settleMillis` / `maxMessages` | 15,000 / 16,384 | 1,000–600,000 ms / 1–1,000,000 |
| `brokerCPUs` / `brokerMemoryBytes` / `generatorCPUs` / `generatorMemoryBytes` | 1 / 1,073,741,824 / 2 / 536,870,912 | 각각 유한한 양수 CPU, 양수 byte |

교차 조건: `drainTimeoutMillis > lbPropagationMillis`; Will delay(ms) > reconnect timeout; session expiry > Will delay; `2*healthPollMillis <= lbPropagationMillis`; `maxMessages >= 4*publishRatePerStreamPerSecond*warmupMillis/1000`. Round 수는 `len(replacementOrder)*cycles`다. 이 수치는 smoke profile이며 production SLO가 아니다.

기본 workload는 18 client다. MQTT 5 publisher/subscriber는 QoS 0/1/2 stream을 쓰며 MQTT 3.1.1은 QoS 1 stream을 쓴다. 여기에 persistent idle client와 Will client가 포함된다. Payload는 seed·stream·sequence로 결정한다.

Publisher는 절대 시각 schedule을 따른다. Disconnect 동안 tick은 `skippedDisconnected`, inflight가 찬 동안은 `skippedInflightFull`로 기록하며 overload가 아니다. 연결되고 inflight slot이 있는데 lag가 interval 이상이면 overload다. QoS1은 PUBACK, QoS2는 PUBCOMP에서 완료한다. Disconnect 때 미완료 exchange는 `interrupted`로 기록하고 gap에서 제외한다. Persistent reconnect는 `SessionPresent=true`여야 하며 QoS2 packet-ID 상태를 session에 보존한다. Backoff는 100ms부터 두 배씩 증가해 2,000ms에서 멈춘다. `(seed, clientIndex)` PCG full-jitter를 쓰며 reconnect deadline 초과는 실패다.

#### Phase와 validity

Phase 순서는 `readiness`, `setup`, `warmup`, `rolling`, `settle`, `final_check`, `cleanup`이다. Runner와 controller는 `control/` marker를 써 stale round marker가 상태를 역행하지 않게 한다. 각 round는 세 node ready를 확인한 뒤 target node를 drain 또는 evacuation하고 중지·재시작·재준비한다. 모든 round는 deadline 안에 끝나야 한다.

Workload가 valid하려면 다음을 모두 만족한다.

- 모든 phase와 rolling completion 성공.
- 계획 밖 disconnect, protocol error, ACK timeout, overload가 0이며 message capacity를 넘지 않음.
- 모든 reconnect 완료, persistent `SessionPresent=true`, QoS1/2 ACKed gap 0, QoS2 duplicate 0.
- `final_check`에서 모든 client 연결 유지.
- QoS1 duplicate, QoS0 lost/reorder는 기록 전용. NaruMQ Will counter의 고정 6 series를 모두 읽고 `unexpected_supported_client`·`unplanned_failure` delta가 0이어야 한다. NaruMQ v5-safe Will은 0, MQTT 3.1.1 고유 Will 수는 해당 server-close 수와 같아야 한다. EMQX Will은 validity 판정에서 제외한다.
- Resource rolling sample은 node마다 최소 2개다. CPU는 median, memory는 maximum이다. Reset-aware block write를 기록한다. Owner network delta가 모두 0이면 `networkStatsAvailable=false`이며 workload 실패는 아니다.

Round metric은 `roundNN_drain_millis`, `roundNN_restart_ready_millis`, `roundNN_reconnect_latency_max_millis`; aggregate는 `reconnects_total`, `qos1_duplicates_total`, `qos0_lost_total`이다. Drain contract가 달라 target 우열로 해석하지 않는다.

`SPEC-TBD-08`의 comparator 수치 제안은 미채택이다. Profile별 수치 기준, Linux/EBS power loss, 실제 NLB·rollout controller와 최대 traffic 연속 100회 rolling은 미완료다.

#### 명령과 수용 테스트

```text
go run ./tests/benchmark/bp07 compare --profile tests/benchmark/bp07/testdata/smoke.json
go run ./tests/benchmark/bp07 suite --profile tests/benchmark/bp07/testdata/smoke.json
```

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#35-bp-07-rolling-drain-기록)에 있다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-018 | - | BP-07 smoke profile에 알 수 없는 field와 범위 밖 값을 입력한다. | 모두 거절한다. |
| PRF-SCN-019 | - | Dialer와 session이 MQTT 연결, 구독과 QoS 흐름을 실행한다. | 연결·재연결과 QoS 흐름이 규칙대로 동작한다. |
| PRF-SCN-020 | - | QoS 1/2 gap·duplicate, 계획 밖 disconnect, marker round 귀속과 Will counter를 입력해 집계한다. | Gap·duplicate와 계획 밖 disconnect를 이 절의 validity 규칙대로 판정한다. Marker는 올바른 round에 귀속하고 stale round marker는 상태를 되돌리지 않는다. NaruMQ v5-safe Will은 0이고 MQTT 3.1.1 고유 Will 수는 server-close 수와 같으며 EMQX Will은 판정에서 제외한다. |
| PRF-SCN-021 | - | Rolling round의 drain state와 EMQX CLI 결과를 입력한다. | Round마다 세 node ready를 확인하고 drain·evacuation 결과를 판정한다. 모든 round가 deadline 안에 끝나야 통과한다. |
| PRF-SCN-022 | - | Rolling 구간의 리소스 표본을 입력한다. | Node마다 최소 2개 표본으로 CPU 중앙값과 memory 최댓값을 계산한다. Owner network delta가 모두 0이면 `networkStatsAvailable=false`이며 workload는 실패가 아니다. |
| PRF-SCN-023 | - | Suite 지표와 하위 실행 실패를 입력한다. | 지표를 만들고 하위 실행이 invalid이면 상위 지표를 비운다. |
| PRF-SCN-024 | - | MQTT packet 경계 입력을 client에 넣는다. | Packet 경계를 규칙대로 해석한다. |
| PRF-SCN-025 | - | 3-node 환경의 published owner port, restart와 stats를 확인한다. | Owner port, restart와 stats를 올바르게 보고한다. |
| PRF-SCN-026 | - | 리더가 없는 상태, 리더십 이전, 여러 origin의 동시 요청과 다시 발행한 요청에서 broker가 fresh read를 처리한다. | 리더가 돌아오면 leaderless read가 완료된다. 발행된 read는 리더십 이전을 넘어 복구된다. 여러 origin의 read가 충돌하지 않는다. 다시 발행한 read는 이전 quorum proof를 거절한다. |

Local gate PASS는 invalid workload나 미완료 production gate를 대체하지 않는다.

### 3.6 BP-06 Failure Recovery smoke 관찰 경계

BP-06은 3-node cluster에서 broker 하나를 drain 없이 `SIGKILL`해 N-1을 유지한 뒤 같은 container·volume으로 재시작한다. Artifact는 모두 `releaseGating=false`; `SPEC-TBD-08` 기준과 run-derived profile cutoff가 미정이다.

#### Topology와 비교 경계

- BP-07 Docker bridge/network-owner topology와 named volume을 쓴다. DNS는 `node{1,2,3}.bp06.test`, cluster ID `narumq-bp06`, tenant `bp06`, subject `bp06-client`, ACL `bp06/#`다.
- EMQX Enterprise 5.8.8 pinned digest는 `sha256:d166efa91eae706ffbfca3f47e36fae0fd441507a8a57bda622da09e4a019c14`다. Durable Sessions/JWT를 쓴다. Evaluation license는 동시 session 25개로 제한되므로 workload client는 최대 19개다.
- 두 target은 limit·client·seed·payload·schedule이 같다. NaruMQ readiness는 bearer token을 쓰며 EMQX availability endpoint는 인증 없이 HTTP 200을 요구한다. Readiness dialer는 ready node를 round-robin으로 선택한다.
- Will은 필수 판정에서 제외한다. Local Docker volume은 production Linux/EBS, 물리 power loss, NLB, 승인 RPO/RTO를 입증하지 않는다.

`rtoDirectlyComparable=false`다. `durability`, `authentication`, `resyncCondition`, `sessionRecovery`, `crashDurability`를 `nonEquivalent`로 기록한다. NaruMQ는 replicated persistent session/ACK semantics를 쓴다. EMQX 5.8.8 `store_batch`는 `sync => false`; durable session state는 `durable_sessions.heartbeat_interval=5000ms`마다 저장된다. 아래 ACKed gap과 QoS2 재전송 예외는 EMQX에만 적용한다.

#### 고정 profile과 workload

`tests/benchmark/bp06/testdata/smoke.json`은 unknown field와 trailing JSON을 거절한다. 수치는 test safety bounds다.

| JSON field | Smoke 값 | 허용 범위·조건 |
| --- | --- | --- |
| `profileId` / `purpose` / `seed` | `bp06-smoke-v1` / `smoke` / 6 | 공백 불가 / `smoke`만 / `uint64` |
| `payloadBytes` / `publishRatePerStreamPerSecond` | 128 / 10 | 16–65,536 / 1–100 |
| `maxInflightPerPublisher` / `ackTimeoutMillis` | 8 / 10,000 | 1–32 / 1,000–60,000 ms |
| `sessionExpirySeconds` / `keepAliveSeconds` | 300 / 30 | 60–86,400초, expiry ms > reconnect+down / 5–300초 |
| `idleClients` | 7 | 0–11; `8+idleClients <= 19` |
| `warmupMillis` / `steadyMillis` | 5,000 / 5,000 | 1,000–600,000 / 2,000–600,000 ms |
| `killOrder` / `cycles` | `[3,1,2]` / 1 | node 1,2,3 순열 / 1–10 |
| `markerAckTimeoutMillis` / `killTimeoutMillis` / `downMillis` | 5,000 / 10,000 / 10,000 ms | 1,000–60,000 / 1,000–60,000 / 1,000–600,000 ms |
| `readyTimeoutMillis` / `resyncTimeoutMillis` | 180,000 / 180,000 ms | 1,000–600,000 ms; resync ≥ ready |
| `settleMillis` / `interRoundSettleMillis` | 15,000 / 3,000 ms | 1,000–600,000 / 2,000–600,000 ms |
| `reconnectTimeoutMillis` / `recoveryTimeoutMillis` | 30,000 / 30,000 ms | 1,000–600,000 ms; recovery ≥ reconnect |
| `healthPollMillis` / `maxMessages` | 250 / 16,384 | 50–5,000 ms / 1–1,000,000, `4*publishRatePerStreamPerSecond*(warmupMillis+steadyMillis)/1000` 이상 |
| `brokerCPUs` / `brokerMemoryBytes` / `generatorCPUs` / `generatorMemoryBytes` | 1 / 1,073,741,824 / 2 / 536,870,912 | 각각 유한한 양수 CPU, 양수 byte |

Kill round는 `3*cycles`회다. 전체 timeout은 `180s + warmup + steady + rounds*(markerAckTimeout + killTimeout + down + readyTimeout + resyncTimeout + interRoundSettle) + settle`이다.

기본 workload는 15 client다. MQTT 5 publisher는 QoS 0/1/2, MQTT 3.1.1 publisher는 QoS 1을 쓴다. 대응 subscriber와 persistent idle client가 있다. `bp06/stream/<stream>` topic과 payload는 deterministic이다. Schedule/inflight 규칙은 §3.5를 따른다. Persistent reconnect는 `CleanStart=false`, `SessionPresent=true`와 QoS2 state resume를 요구한다. Backoff는 100ms부터 2초까지 exponential full-jitter이며 seed는 `(seed, clientIndex)`다. Reference가 있는 server DISCONNECT는 protocol error다. EOF/reset과 reference 없는 DISCONNECT는 같은 dialer에서 reason을 유지하며 재연결한다.

#### Phase·RTO·validity

Runner phase는 readiness, setup, warmup, failure, settle, final_check, cleanup이다. Readiness에서 NaruMQ `session-metadata`와 `proposal-recovery` feature를 모두 활성화하고 확인한다. Controller는 atomic control marker를 쓰고 runner는 첫 관찰을 ACK하며 round 순서를 되돌리지 않는다. Round마다 marker ACK, Docker SIGKILL, N-1 down, 같은 container 재시작, ready, resync, settle을 수행한다. Failure artifact에는 단계별 상태와 미완료 round를 남긴다.

Round의 `injectionAt`은 귀속 가능한 가장 이른 disconnect다. 없으면 최초 `downObservedAt`을 쓰고, 둘 다 없으면 실패다. RTO는 영향받은 모든 client의 재연결과 각 stream의 장애 주입 뒤 첫 성공 QoS 완료 중 늦은 시점까지다(QoS1 `ackAt`, QoS2 `completeAt`, QoS0 첫 receipt). 성공 관찰 하나라도 없으면 `Recovered=false`; `rtoMillis <= recoveryTimeoutMillis`여야 한다. `rejoinReadyMillis`·`resyncMillis`는 재시작 시작부터 잰다. NaruMQ resync는 live leader를 선택해 `lastAppliedIndex` watermark를 한 번 캡처한 뒤 target voter match index가 watermark 이상이고 `replicationHealth=HEALTHY`인지 확인한다. Leader 변경은 watermark를 초기화하지 않는다. Paged node listing, leader 부재, non-200, incomplete state는 deadline까지 재시도한다. EMQX resync는 pinned cluster readiness와 5,000ms durable-session heartbeat를 확인한다.

모든 phase/controller 성공, 계획 밖 disconnect·protocol error·ACK timeout·overload·capacity 초과 0, 기한 내 전체 reconnect, persistent `SessionPresent`, 예상 밖 QoS1/2 ACKed gap·QoS2 duplicate 0, 모든 round RTO 내 회복, final check 전체 연결 유지가 workload validity 조건이다. QoS1 duplicate와 QoS0 lost/reorder는 기록만 한다. Resource steady window는 `failure.jsonl` steady 구간이며 round window는 kill부터 settle까지다. 실행 중 broker만 수집한다. Steady는 node마다 최소 2 sample, round는 survivor 최소 2 node와 재시작한 target sample 최소 1개가 필요하다. CPU median, memory maximum, reset-aware block write와 owner network delta를 기록한다. 필요한 window/sample이 없으면 resource invalid다. Network delta가 0이면 unavailable로 기록하며 workload 자체는 invalid가 아니다.

#### EMQX crash-window 예외

예외는 EMQX에만 적용하며 NaruMQ에는 예외가 없다.

- ACKed gap은 `killingObservedAt <= completeAt <= injectionAt+2000ms`이고 QoS1/2 ACK 완료, 미수신, interrupted/aborted 아님을 모두 만족할 때만 면제한다. 면제 gap은 settle wait에서 제외한다.
- QoS2 duplicate는 첫 receipt가 subscriber 계획 disconnect 5,000ms 이내 앞에 있고 마지막 duplicate가 disconnect 뒤인 경우만 면제한다. Subscriber ID는 `bp06-sub-<stream>`과 일치해야 한다.
- Round 순서대로 살펴보고 각 record는 처음으로 조건에 맞는 round에 귀속한다. 그 밖의 모든 조건은 invalid다.

경계는 pinned EMQX 5.8.8 동작에 따른다. `emqx_persistent_message:store_message/1`은 `emqx_ds:store_batch`를 `sync => false`로 부르고 durable session state는 5,000ms heartbeat에 저장한다. 이후 실패를 이유로 예외를 넓히지 않는다.

#### Artifact·수용 테스트

Target은 phase/health/connection/message/workload/failure/resource record, round control marker, log, inspect, EMQX durability snapshot을 보존한다. `workload-summary.json` schemaVersion 2는 `exemptGaps`, `exemptQoS2Duplicates`, `comparatorDurabilityExceptions`를 포함하며 `messages.jsonl`은 `lastReceivedAt`을 담는다. Suite metric은 round RTO/rejoin-ready/resync/reconnect-max, CPU·memory peak, aggregate reconnect 수, QoS1 duplicate, QoS0 lost다. Child 하나가 invalid면 parent metric을 비운다.

Metric key는 round별 `roundNN_rto_millis`, `roundNN_rejoin_ready_millis`, `roundNN_resync_millis`, `roundNN_reconnect_latency_max_millis`, `roundNN_recovery_cpu_peak_percent`, `roundNN_recovery_memory_peak_bytes`; aggregate는 `reconnects_total`, `qos1_duplicates_total`, `qos0_lost_total`이다.

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#36-bp-06-failure-recovery-기록)에 있다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-027 | - | 실행 중인 broker container를 kill하고 inspect한 뒤 다시 start한다. | 같은 container와 volume을 유지한 채 재시작한다. 소유권이 바뀌지 않는다. |
| PRF-SCN-028 | - | Smoke profile에 알 수 없는 field, 범위 밖 값과 timeout·expiry·capacity 경계 위반을 입력한다. | 모든 경우를 거절한다. 경계값은 허용한다. |
| PRF-SCN-029 | - | BP-06 fixture의 인증 정보, ACL과 client 허용 목록을 만든다. | 인증 정보와 ACL이 규칙대로 구성되고 허용 목록 밖의 client는 거절한다. |
| PRF-SCN-030 | - | Control marker, 재연결과 연결 끊김을 round에 귀속한다. | 영향받은 연결의 끊김과 재연결을 올바른 round에 귀속한다. 계획 밖 disconnect는 invalid다. |
| PRF-SCN-031 | - | RTO, 복구와 resync watermark·deadline을 판정한다. | RTO와 resync 기한 안에 회복한 round만 통과한다. 기한 초과와 watermark 미달은 invalid다. |
| PRF-SCN-032 | - | EMQX target에 `killingObservedAt <= completeAt <= injectionAt+2000ms`인 ACKed gap과 5,000ms 안에 첫 receipt가 있는 QoS 2 duplicate, 그 밖의 gap·duplicate를 입력한다. NaruMQ target에도 같은 입력을 넣는다. | EMQX의 지정 crash window 안의 gap과 checkpoint 재전달만 면제한다. 그 밖의 gap·duplicate는 EMQX를 invalid로 만든다. NaruMQ에는 면제가 없다. |
| PRF-SCN-033 | - | Steady·round 리소스 구간, counter 재설정과 표본 수 임계치를 입력한다. | 필요한 window와 sample이 없으면 resource invalid다. Counter 재설정을 반영한 값을 기록한다. |
| PRF-SCN-034 | - | Suite 지표와 단위, 하위 실행 실패를 입력한다. 반복 값의 중앙값과 MAD를 계산한다. | 지표와 단위를 정확히 만든다. 하위 실행이 하나라도 invalid이면 상위 지표를 비운다. 중앙값과 MAD를 계산한다. |

```text
go run ./tests/benchmark/bp06 compare --profile tests/benchmark/bp06/testdata/smoke.json
go run ./tests/benchmark/bp06 suite --profile tests/benchmark/bp06/testdata/smoke.json
```

### 3.7 BP-04 Subscription Stress smoke 관찰 경계

BP-04은 QoS 0의 고정 workload로 wildcard, overlap, shared subscription과 높은 fan-out을 관찰한다. 이는 하네스와 local 실행 경로의 증거이며 production 부하·release threshold·PERF 전체 수용을 결정하지 않는다. Artifact는 모두 `releaseGating=false`다.

#### Topology와 profile

두 target 모두 3-node다. NaruMQ cluster ID는 `narumq-bp04`; EMQX는 pinned OSS 5.8.8이다. Broker limit은 node마다 1 CPU/1 GiB, generator는 2 CPU/512 MiB다. MQTT 5 plaintext bridge와 RS256 JWKS JWT를 쓰며 workload 연결은 QoS 0, clean session, expiry 0, keepalive 30초다. NaruMQ node 간에는 peer mTLS를 쓴다.

`tests/benchmark/bp04/testdata/smoke.json`은 strict decode로 unknown field를 거절한다.

| 필드 | smoke 값 | 허용 범위 |
| --- | --- | --- |
| `profileId` / `purpose` / `seed` | `bp04-smoke-v1` / `smoke` / 4 | 공백 불가 / `smoke` 전용 / `uint64` |
| `payloadBytes` / `keepAliveSeconds` | 128 / 30 | 16–65,536 bytes / 10–300초 |
| fanout/wildcard/overlap/shared 구독자 수 | 48 / 24 / 12 / 12 | 1–256 / 6–96(6의 배수) / 1–64 / 2–64 |
| `ratesPerStreamPerSecond` | `[5,20]` | 1–4개 오름차순, 각각 1–200 |
| warmup/measure/drain 구간 | 2,000 / 10,000 / 3,000 ms | 1,000–60,000 / 3,000–120,000 / 1,000–30,000 ms |
| 기본 유휴 / 구독 유휴 구간 | 3,000 / 3,000 ms | 각각 3,000–60,000 ms |
| route 프로브 수 / 간격 / 타임아웃 | 12 / 5 / 5,000 | 1–64 / 1–100 ms / 1,000–30,000 ms |
| 준비 / 설정 타임아웃 | 120,000 / 60,000 ms | 10,000–600,000 / 5,000–600,000 ms |
| `maxMessages` | 4,096 | 1–1,000,000, 계산된 워크로드 이상 |
| 브로커 / 제너레이터 리소스 제한 | 1 CPU / 1 GiB; 2 CPU / 512 MiB | 유한한 양수 CPU, 양수 바이트 |

`maxMessages >= ceil(Σ_rate 4*rate*(warmup+measure)/1000)`다. Smoke workload는 구독자 96명이다. Global ordinal로 client를 세 node에 순환 배치한다.

#### Subscription과 topic oracle

Class 순서는 `fanout`, `wildcard`, `overlap`, `shared`이며 ID는 `bp04-sub-<class>-%03d`다. Fan-out 구독자 48명은 모두 `bp04/fanout/all`을 구독한다. Wildcard 구독자 24명은 index `i`마다 `g=(i/6)%4`를 계산하고 index modulo 6에 따라 다음 filter를 사용한다: `bp04/wild/r{g}/+`, `bp04/wild/+/d{g}`, `bp04/wild/#`, `bp04/wild/r{g}/#`, `bp04/+/r{g}/d{g}`, `bp04/wild/r{g}/d{g}`. Publisher ordinal `n`의 topic은 `bp04/wild/r{n%4}/d{(n/4)%4}`다.

Overlap 구독자는 각자 한 SUBSCRIBE에 `bp04/ovl/a/b`, `bp04/ovl/+/b`, `bp04/ovl/#`를 넣는다. NaruMQ는 구독자별 최대 세 matching copy를 허용하며 `extraCopies`로 기록한다. 네 번째 copy부터 duplicate다. Shared 구독자는 `$share/bp04g/bp04/shared/#`를 쓰며 group 전체 수신은 message당 정확히 하나다.

Stream index 0–3은 위 class 순서다. Publisher ID는 `bp04-pub-<class>`, node 순서는 0,1,2,0이다. Fanout·overlap topic은 각각 `bp04/fanout/all`, `bp04/ovl/a/b`; shared topic은 `bp04/shared/j%d`이며 index는 `ordinal%8`이다. Payload는 seed·stream index·sequence에 의해 결정된다. 독립 시험 oracle은 `+`를 한 topic level, 마지막 `#`을 나머지 level로 매칭하고 `$share/<group>/`을 제거한 뒤 subscriber 집합으로 기대 수신자를 계산한다.

Readiness는 여섯 cross-node QoS 1 pair를 시험한다. Route probe는 workload 밖의 publisher/subscriber로 `bp04/route/%03d`를 쓴다. Subscriber node는 `j%3`다.

#### Phase·route propagation·validity

Phase 순서는 readiness, idle-baseline, setup, subscribed-idle, 각 rate의 warmup/measure/drain, route-propagation, final_check, cleanup이다. Readiness는 health·cluster ID와 여섯 pair probe를 확인한다. Setup에서는 route publisher, workload publisher/subscriber, route subscriber를 연결하며 SUBACK 성공 뒤 reader를 시작한다.

Publisher schedule은 절대 시각 `start+(ordinal-1)*interval+streamIndex*interval/4`다. `schedule lag >= interval`이면 overload다. Drain은 기대 delivery 완료 또는 `drainMillis`까지다. Route probe QoS 0 publish는 설정 interval을 따른다. `T0`는 SUBSCRIBE 직전, `T1`은 SUBACK, `T2`는 첫 유효 delivery다. `subscribeAckNanoseconds=T1-T0`, `routeVisibleNanoseconds=T2-T0`이며 `T2<T1`도 유효하다. `T0+routeTimeoutMillis`까지 delivery가 없으면 실패한다. 첫 delivery가 visibility를 정하고 이후 delivery는 기록만 한다.

Target은 measure의 모든 missing/duplicate/unexpected가 0이고 overload·protocol error·transport error가 없으며 setup 수가 계획과 같고 모든 route probe 및 final_check가 성공해야 valid다. Shared group은 정확히 한 delivery를 요구한다. Latency percentile은 nearest-rank p50/p95/p99/p99.9/max다.

Resource는 idle-baseline, subscribed-idle, 각 measure window에서 Docker live stats로 수집한다. Node/window마다 최소 2 sample이 필요하다. CPU median, memory median, block read/write, network-owner Rx/Tx를 기록한다. Network delta가 모두 0이면 unavailable이지 workload 실패가 아니다. 짝수 sample median은 `lower+(upper-lower)/2`; cluster memory는 node median 합이며 signed memory delta를 보존한다. Top-level setup client 수는 115다.

Suite metric은 step별 delivered/s·cluster CPU, class별 delivery p99/p999, network Tx bytes/delivery(가능할 때), route visibility p50/p99/max, setup SUBACK p99, setup client당 memory다. `Equivalent`는 profile SHA, limits, runner image ID, 3-node 일치가 필요하다. `nonEquivalent`: `overlapDelivery`는 target별 matching-copy 처리 차이, `sharedSelection`은 member 선택 순서가 implementation-specific, `authentication`은 EMQX가 tenant/client-ID binding과 peer mTLS를 재현하지 않음, `subscriptionReplication`은 Raft projection과 EMQX cluster database 차이이며 client-observed visibility만 비교한다.

#### BP-04 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#37-bp-04-subscription-stress-기록)에 있다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-035 | - | Smoke profile을 읽는다. 알 수 없는 field와 범위 밖 경계값을 입력하고 subscriber ID와 topic oracle을 만든다. | 원문 해시를 보존한다. 알 수 없는 field와 범위 밖 값은 거절한다. Subscriber ID와 topic oracle이 class 순서와 이 절의 규칙을 따른다. |
| PRF-SCN-036 | - | Shared와 overlap 구독자의 수신을 집계한다. | Shared group과 overlap 구독의 수신을 이 절의 oracle대로 집계한다. |
| PRF-SCN-037 | - | BP-04 fixture의 JWT, ACL과 인증서를 만들고 정리한다. | JWT, ACL과 인증서가 규칙대로 구성되고 정리된다. |
| PRF-SCN-038 | - | 리소스 표본의 중앙값, 네트워크 증가량과 표본 한도를 입력한다. | 중앙값과 네트워크 값을 계산하고 표본 한도를 넘거나 부족하면 거절한다. |
| PRF-SCN-039 | - | Compare에 증거와 지표를 입력하고 실패한 suite를 실행한다. | 동등성 증거가 맞을 때만 비교한다. 지표를 선택하고 suite가 실패하면 상위 지표를 비워 둔다. |

```text
go run ./tests/benchmark/bp04 compare --profile tests/benchmark/bp04/testdata/smoke.json
go run ./tests/benchmark/bp04 suite --profile tests/benchmark/bp04/testdata/smoke.json
```

### 3.8 BP-02 Production Mix smoke 관찰 경계

BP-02는 QoS 0/1/2, payload size cycle, 여러 subscription 종류, retained state, persistent session, tenant, TLS를 섞은 고정 workload다. 하네스와 local 실행만 증명한다. 고정 tenant·stream·TLS 비율은 production 분포가 아니다. `B_*`, 실제 N/N-1 SLO, profile별 `SPEC-TBD-08` cutoff, production storage와 release acceptance는 미정이다. EMQX Enterprise 6.3.1 comparator는 선택했지만 95%/105% gate 제안은 미채택이며 artifact는 모두 `releaseGating=false`다.

현재 local BP-02 smoke artifact는 계속 `releaseGating=false`다. 선택한 ARM64 qualification profile을 사용한다는 사실만으로 production performance gate를 통과하지 않는다. Candidate run도 승인된 production workload와 full EKS profile에서 공통 계약을 충족해야 gate 증거로 인정한다.

#### Topology와 비교 경계

두 target은 3-node다. NaruMQ는 `narumq-bp02` dynamic cluster, EMQX는 공통 `EMQXImageReference`의 pinned OSS 5.8.8을 쓴다. Broker limit은 node마다 1 CPU/1 GiB, generator는 2 CPU/512 MiB다. MQTT 5와 RS256 JWKS JWT를 쓰며 TCP listener는 1883, server-auth TLS listener는 1.2+의 8883이다. NaruMQ node 간에는 peer mTLS를 쓴다. Tenant `bp02-a`, `bp02-b`는 각각 `bp02/a/#`, `bp02/b/#` ACL을 쓴다. NaruMQ는 `(iss,sub)`를 tenant와 allowed client ID에 연결한다. EMQX는 issuer/audience/subject를 username과 확인하고 username 기준 file ACL을 쓴다. Persistent session은 첫 연결 `CleanStart=true`, 재연결 `CleanStart=false`, expiry 300초다. 나머지는 clean session, expiry 0이다.

`Equivalent`에는 profile SHA, CPU/memory limit, runner image ID와 3개 node가 일치해야 한다. 이는 구현 동등성을 뜻하지 않는다.

| `nonEquivalent` dimension | 비교 차이 |
| --- | --- |
| `sessionDurability` | NaruMQ는 persistent session·subscription·QoS1/2 inflight·offline message를 Raft quorum에 commit한다. EMQX OSS 5.8.8은 memory session을 쓴다. Offline 결과는 durability 비교가 아니다. |
| `tenantIsolation` | NaruMQ의 JWT tenant/client-ID binding과 EMQX username ACL은 다르며, 동일 raw topic을 tenant 사이에서 시험하지 않는다. |
| `authentication` | 양쪽 모두 같은 JWKS의 RS256을 검증하지만 EMQX는 client-ID binding과 peer mTLS를 재현하지 않는다. |
| `tlsListener` | NaruMQ fixture는 peer mTLS와 분리한 node별 MQTT server certificate/key를 `mqtt.tls`에 지정하고 TLS 1.2 이상, client-auth `none`을 설정한다. EMQX는 3-SAN server certificate를 공유한다. Cipher와 resumption은 구현별이다. |
| `sharedSelection` | Shared group member 선택 순서는 구현별이며 `memberDeliveries`는 기록만 하고 비교하지 않는다. |
| `retainedReplication` | NaruMQ Raft와 EMQX retainer storage가 다르며 client-observed replay만 비교한다. |
#### 고정 smoke profile

`tests/benchmark/bp02/testdata/smoke.json`은 unknown/missing field를 거절하고 원문 SHA-256을 보존한다. Bounds는 smoke 안전 범위이며 production capacity/SLO가 아니다.

| JSON field | Smoke 값 | 허용 범위 |
| --- | --- | --- |
| `profileId` / `purpose` / `seed` | `bp02-smoke-v1` / `smoke` / 2 | 공백 불가 / `smoke`만 / `uint64` |
| `payloadSizeCycle` | `[128,128,128,128,128,128,512,512,2048,16384]` | 1–32개, 각각 32–65,536 byte |
| `keepAliveSeconds` / `sessionExpirySeconds` | 30 / 300 | 10–300 / 60–3,600초 |
| `tlsEveryNthClient` | 2 | 1–16 |
| `idleClientsTenantA` / `idleClientsTenantB` | 12 / 6 | 각각 0–256 |
| `ratesPerStreamPerSecond` | `[2,5]` | 1–4개 엄격 오름차순, 각각 1–50 |
| `maxInflightPerPublisher` / `ackTimeoutMillis` | 16 / 10,000 | 1–64 / 1,000–60,000 ms |
| `warmupMillis` / `measureMillis` / `drainMillis` | 2,000 / 10,000 / 5,000 | 1,000–60,000 / 3,000–120,000 / 1,000–30,000 ms |
| `idleBaselineMillis` / `subscribedIdleMillis` | 3,000 / 3,000 | 각각 3,000–60,000 ms |
| `offlineSettleMillis` | 1,000 | 100–10,000 ms |
| `offlineMessagesPerStream` / `offlinePublishRatePerSecond` | 16 / 10 | 1–64 / 1–50 |
| `reconnectTimeoutMillis` | 20,000 | 1,000–120,000 ms |
| `retainedTopicsTenantA` / `retainedTopicsTenantB` | 8 / 4 | 각각 1–64 |
| `retainedReplayTimeoutMillis` | 10,000 | 1,000–60,000 ms |
| `readyTimeoutMillis` / `setupTimeoutMillis` | 120,000 / 60,000 | 10,000–600,000 / 5,000–600,000 ms |
| `maxMessages` | 2,048 | 1–1,000,000, 계산된 하한 이상 |
| `brokerCPUs` / `brokerMemoryBytes` | 1 / 1,073,741,824 | 유한한 양수 / 양수 byte |
| `generatorCPUs` / `generatorMemoryBytes` | 2 / 536,870,912 | 유한한 양수 / 양수 byte |

`maxMessages` 하한은 `Σ_rate ceil(6*rate*(warmupMillis+measureMillis)/1000)+3*offlineMessagesPerStream+2*(retainedTopicsTenantA+retainedTopicsTenantB)`다. Smoke 계산값은 576이다. `offlineMessagesPerStream` 최대 64는 durable per-session message 한도와 같으며 alert stream의 subscriber 하나가 모든 offline message를 받을 수 있다.

#### Workload·oracle·phase

| Index/stream | Tenant/QoS | Topic·구독자·filter |
| --- | --- | --- |
| 0 `a_telemetry` | A/QoS0 | `bp02/a/telemetry/dev%02d`, `(s-1)%8`; 구독자 6명, `bp02/a/telemetry/+`, QoS0 clean |
| 1 `a_alert` | A/QoS1 | `bp02/a/alert/zone%d`, `(s-1)%4`; 구독자 4명, `bp02/a/alert/#`, QoS1 persistent |
| 2 `a_command` | A/QoS2 | `bp02/a/command/dev%02d`, `(s-1)%4`; 각 구독자가 exact topic, QoS2 persistent |
| 3 `a_job` | A/QoS1 | `bp02/a/job/%d`, `(s-1)%4`; shared 구독자 4명 `$share/bp02a/bp02/a/job/#`, clean |
| 4 `b_telemetry` | B/QoS0 | `bp02/b/telemetry/dev%02d`, `(s-1)%8`; 구독자 3명, `bp02/b/telemetry/+`, clean |
| 5 `b_alert` | B/QoS1 | `bp02/b/alert/zone%d`, `(s-1)%4`; 구독자 2명, `bp02/b/alert/#`, persistent |
| 6 `a_state` | A/QoS1 retained | `bp02/a/state/dev%02d`, index `0..retainedTopicsTenantA-1`; 구독자 2명, `bp02/a/state/#`, clean |
| 7 `b_state` | B/QoS1 retained | `bp02/b/state/dev%02d`, index `0..retainedTopicsTenantB-1`; 구독자 2명, `bp02/b/state/#`, clean |

Publisher ID는 `bp02-<tenant>-pub-<stream>`, subscriber ID는 `bp02-<tenant>-sub-<stream>-%02d`다. 계획 순서는 stream 0–5 publisher, state publisher 2개, stream 0–5 구독자(stream/index 순), tenant A·B idle, state 구독자(A 다음 B)다. Idle ID는 `bp02-a-idle-%03d`, `bp02-b-idle-%03d`다. `nodeIndex=ordinal%3`, TLS는 `ordinal%tlsEveryNthClient==0`이다. Retained replay 구독자 4명을 제외한 setup client는 49개(TLS 25, TCP 24)다.

Readiness probe는 workload 순서 밖이며 tenant A credential을 쓴다. TCP pair 6개는 `bp02-probe-pub-%d-%d`, `bp02-probe-sub-%d-%d`, topic `bp02/a/probe/%d-%d`를 쓴다. Node마다 TLS CONNECT/DISCONNECT probe `bp02-probe-tls-%d`가 있다. Tenant allowlist는 계획 client와 probe ID를 중복 없이 포함한다.

`topicMatches`는 독립 oracle이다. `$share/<group>/`을 제거하고 MQTT `+`와 마지막 `#`을 매칭하며 같은 tenant의 수신자만 계산한다. 일반 subscriber는 message당 한 copy, shared group은 전체 한 copy를 기대한다. Payload는 seed·stream index·sequence로 결정하며 크기는 `payloadSizeCycle[(sequence-1)%len]`이다. Header의 sequence/index, 크기와 deterministic byte를 검사한다. Retained publisher sequence는 단조 증가하며 round `r=1,2`와 topic index `i`는 `(r-1)*topicCount+i+1`을 쓴다.

Warmup과 measure 각각 실제 publish 수는 `floor(rate*millis/1000)`이다. Offline sequence는 모든 rate·phase의 floor 합 뒤에서 시작한다. Duration을 합산한 뒤 한 번만 반올림하지 않는다. 실제 publish 수가 topic별 offline 기대 수에 사용되며 `maxMessages` 상한 공식과는 별개다.

Phase 순서는 readiness(health/leader, TCP pair 6개, node별 TLS probe), idle-baseline, setup, subscribed-idle, rate별 warmup/measure/drain, offline-disconnect, offline-publish, offline-reconnect, retained-publish, retained-replay, final_check, cleanup이다. Setup은 49 client를 연결한다. SUBACK reason은 요청 QoS와 같아야 reader를 시작한다. Keepalive는 setup 뒤 시작한다. Stream 0–5의 절대 schedule은 `start+(ordinal-1)*interval+streamIndex*interval/6`이다. Inflight credit 대기는 lag에 포함되며 `lag >= interval`은 overload다. Drain은 기대 delivery와 ACK 완료 또는 `drainMillis`까지다.

Persistent 구독자 10명은 DISCONNECT reason 0을 쓰고 socket을 닫는다. Settle 뒤 stream 1,2,5가 `offlineMessagesPerStream`개를 설정 rate로 발행한다. 모든 publish ACK와 다음 settle을 기다린다. 같은 transport로 `(nodeIndex+1)%3`에 `CleanStart=false`로 재연결한다. SUBSCRIBE는 다시 보내지 않으며 `SessionPresent=true`여야 한다. 모든 offline 기대 delivery는 phase reconnect timeout 안에 와야 한다. 구독자별 drain duration은 CONNACK부터 마지막 기대 delivery까지다.

State publisher는 QoS1 retained message를 두 round 발행하고 모든 PUBACK을 기다린다. Replay 구독자 4명은 clean 연결 후 QoS1 구독한다. Tenant별 topic마다 round-2 payload, `Retain=true`, QoS1인 message가 정확히 하나 와야 한다. Replay 시간은 SUBSCRIBE 직전부터 마지막 기대 delivery까지다. 기대 수신 뒤 500ms(`bp02RetainedQuietPeriod`) 더 읽으며 추가 PUBLISH는 실패다. Replay timeout 내 missing은 실패, round-1 payload는 stale다.

Publisher packet ID는 1–65,535 중 미사용 값을 순환 배정하며 inflight는 profile 한도를 넘지 않는다. QoS1 완료는 PUBACK, QoS2는 PUBREC→PUBREL→PUBCOMP다. Reason ≥`0x80`은 protocol error, ACK deadline 초과는 `ackTimeouts`다. ACK latency는 PUBLISH 직전부터 PUBACK/PUBCOMP까지다. Subscriber는 QoS1 수신을 기록한 뒤 PUBACK한다. QoS2는 첫 PUBLISH를 기록하고 PUBREC한다. 같은 packet ID 재전송은 delivery record를 늘리지 않고 PUBREC만 다시 보낸다. PUBREL을 받으면 PUBCOMP를 보내고 pending을 지운다. Topic/filter, payload, stream QoS가 맞아야 한다. 일반 delivery는 `Retain=false`, QoS0는 `DUP=false`여야 한다. QoS1/2 `DUP` flag는 validity 판정에 쓰지 않고 copy 수로 중복을 판정한다. PINGRESP는 무시한다. Runner가 직접 유발한 offline disconnect를 제외한 예상 밖 packet, EOF, disconnect는 오류다.

Valid 조건: measure stream별 missing/duplicate/unexpected/QoS mismatch/ACK timeout이 모두 0이고 overload·protocol·transport error가 없어야 한다. Setup 수는 계획과 같아야 한다. Offline은 모든 `SessionPresent=true`이고 missing/duplicate/unexpected가 0이어야 한다. Retained는 missing/duplicate/unexpected/stale/wrongFlags가 모두 0이어야 한다. `final_check`도 성공해야 한다. Warmup missing/duplicate는 별도 기록한다. 첫 copy만 delivery latency에 반영한다. 전체 timeout은 readiness + idle 2회 + setup + 모든 rate의 warmup/measure/drain + `2*offlineSettleMillis+ceil(offlineMessagesPerStream*1000/offlinePublishRatePerSecond)+reconnectTimeoutMillis` + `2*ackTimeoutMillis+retainedReplayTimeoutMillis+500ms` + 3분이다.

#### Artifact·metric·수용 테스트

Target artifact는 `workload-summary.json`, `resource-summary.json`, `phases.jsonl`, `messages.jsonl`, `retained.jsonl`, `connections.jsonl`이다. Workload summary는 step/stream별 sent, expected/delivered, missing, duplicates, unexpected, QoS mismatch, ACK timeout, delivery/ACK latency, warmup counter를 담는다. Shared는 `memberDeliveries`, offline·retained는 각자 별도 summary를 가진다. Connection record에는 tenant, `tls|tcp`, setup/offline-reconnect/retained-replay phase, connect latency, `SessionPresent`가 있다. Resource는 idle-baseline, subscribed-idle, measure window의 Docker live stats를 쓰며 signed memory delta와 setup client 49개를 기록한다. Suite는 5회 반복한다. Child 하나라도 invalid면 parent metric 전체를 비운다. Network stats가 없으면 network metric만 생략한다.

Suite metric은 step별 delivered/s·cluster median CPU, stream별 delivery p99/p999, QoS1/2 publish ACK p99, 선택적 network Tx bytes/delivery, 존재하는 transport별 setup TLS/TCP connect p99, offline reconnect CONNACK p99, 최대 offline drain·retained replay, setup client당 memory다.

Key는 `stepNN_delivered_per_second`, `stepNN_cluster_median_cpu_percent`, stream별 `stepNN_<stream>_delivery_latency_p99_ns`·`p999_ns`다. QoS1/2 stream은 `stepNN_<stream>_publish_ack_latency_p99_ns`도 포함한다. Network가 있으면 `stepNN_network_tx_bytes_per_delivery`를 포함한다. Transport가 있을 때만 setup TLS/TCP connect p99를 만든다. 공통 key는 `offline_reconnect_connack_latency_p99_ns`, `offline_drain_duration_max_ns`, `retained_replay_duration_max_ns`, `memory_bytes_per_setup_client`다. Latency 단위는 ns, throughput은 deliveries/s, CPU는 percent, network·memory는 bytes다.

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#38-bp-02-production-mix-smoke-기록)에 있다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-040 | - | Smoke profile JSON을 읽는다. 알 수 없는 field, 누락 field와 범위 밖 경계값을 입력한다. | 원문 SHA-256을 보존한다. 알 수 없는 field, 누락 field와 범위 밖 값은 거절한다. |
| PRF-SCN-041 | - | Client 계획, 기대 수신자 oracle과 payload size cycle을 만든다. | Client마다 tenant, node와 TLS·TCP 전송이 규칙대로 배정된다. 기대 수신자는 같은 tenant와 shared group 규칙을 따른다. Payload size cycle은 profile 값과 같고 검증 규칙을 지킨다. |
| PRF-SCN-042 | - | Server certificate를 검증하는 TLS client로 broker에 연결한다. | 서버 인증서를 검증한다. |
| PRF-SCN-043 | - | BP02 fixture를 만든다. | Tenant A·B가 분리되고 node별 TLS listener가 구성되며 fixture가 정리된다. |
| PRF-SCN-044 | - | Shared group의 두 번째 member 중복 전달, QoS 불일치, offline session 없음, 오래된 retained round와 retain flag 누락을 입력한다. | 중복 전달은 중복으로 센다. QoS 불일치는 step을 invalid로 만든다. Offline session이 없으면 target이 invalid다. 오래된 round와 retain flag 누락은 거절한다. |
| PRF-SCN-045 | - | Phase별 정수 발행 수, 정확한 topic, 늦은 measure 전달, 중단된 step과 profile의 retained topic 수를 입력한다. | Oracle이 phase별 publish 수를 쓴다. Offline command 기대는 정확한 topic을 따른다. Drain 시점의 missing snapshot을 보존한다. 중단된 step은 관찰한 미수신을 보고한다. Retained oracle은 profile의 topic 수를 따른다. |
| PRF-SCN-046 | - | Subscriber가 PUBREL을 받기 전에 같은 QoS 2 PUBLISH 재전송을 받는다. | 한 번만 센다. |
| PRF-SCN-047 | - | 리소스 표본의 메모리 중앙값, 0 byte 네트워크 증가량과 부족한 표본을 입력한다. | 메모리 중앙값은 가운데 두 값의 하위 값에 차이의 절반을 더해 구한다. 0 byte 네트워크 증가량을 받아들인다. 표본이 부족하면 거절한다. |
| PRF-SCN-048 | - | Compare에 증거가 맞지 않는 target, stream·phase별 metric 입력과 실패한 suite를 넣는다. | 증거가 일치해야 비교한다. Metric은 stream과 phase에 맞게 선택한다. Suite가 실패하면 상위 metric을 비워 둔다. |

```text
go run ./tests/benchmark/bp02 compare --profile tests/benchmark/bp02/testdata/smoke.json
go run ./tests/benchmark/bp02 suite --profile tests/benchmark/bp02/testdata/smoke.json
```

### 3.9 BP-02 EKS 용량 측정 계약

이 절은 [PRD §18 질문 1~5](../requirements/PRD.md#18-미결정-사항)의 채택에 따라 선택한 `EKS_EC2_HA` profile의 수용 용량을 재고 `B_*`로 기록하는 방법을 정한다. 2026-10-15 사용자는 다음을 선택했다.

| 항목 | 결정 |
| --- | --- |
| Workload 분포 | §3.8 BP-02 production mix의 비율을 그대로 두고 규모만 키운다. |
| 용량 판정 | N-1 유지 기준. Broker 하나가 빠진 동안과 복구 과정에서도 아래 판정 조건을 지키는 가장 높은 부하 단계를 용량으로 본다. |
| Latency 한도 | Publish-to-delivery p99 100ms 이하 |
| 부하 생성 위치 | 같은 EKS cluster의 전용 node group에서 Kubernetes Job으로 실행하고 내부 NLB로 접속한다. |

#### Workload 단위(cell)

Cell 하나는 §3.8의 stream 8개, 구독자 23명, retained 구독자 4명과 idle client 18명(A 12, B 6)을 그대로 복제한 단위다. Cell마다 publisher 8명을 포함해 client 53명이 있다. Tenant도 cell마다 두 개(`bp02-a-c%03d`, `bp02-b-c%03d`)를 둔다. 그래서 tenant와 client의 비율이 smoke와 같고, tenant별 quota 상한이 cell 수와 함께 커지는 부하에 걸리지 않는다. Cell `c`의 topic은 tenant 경로 다음에 `c%03d` 단계를 둔다. 예를 들어 `bp02/a/c007/telemetry/dev03`이고 ACL은 `bp02/a/c007/#`이다. Filter도 같은 단계를 포함한다. Shared group 이름은 `bp02a-c%03d`다. Client ID는 tenant 다음에 `c%03d`를 붙인다(`bp02-a-c007-pub-telemetry`). 각 tenant의 JWT identity는 그 cell의 client ID를 정확히 나열한다. QoS·persistent·retained 구성, `payloadSizeCycle`, keepalive 30초, Session Expiry 300초, `tlsEveryNthClient=2`는 §3.8 smoke와 같다. 따라서 `B_msg_size`는 p50 128 byte, p95·p99·최대 16,384 byte(nearest-rank)다.

부하 단계는 `(cells, ratePerStreamPerSecond)` 쌍이다. 단계 `i`의 offered publish 속도는 `8*cells*rate` msg/s이고 연결 수는 `53*cells`다. Profile은 offered 부하가 엄격히 증가하는 단계 목록을 가진다. 첫 실패 단계가 나오면 마지막 통과 단계와 첫 실패 단계 사이의 cell 수를 이분 탐색으로 두 번 좁힌다. Rate는 §3.8 smoke 상한과 같은 `ratePerStreamPerSecond` 1–50 범위를 쓴다.

#### 환경

- Broker는 [EKS 배포 구조](../architectures/eks-ec2-deployment.md#production-qualification-profile-selected)의 선택 profile을 쓴다(`m7g.large` 3대, broker container마다 1 CPU·4 GiB, 20 GiB `gp3`, 내부 NLB IP target). Broker image는 ARM64 immutable digest로 고정한다. 용량 overlay는 broker container 자원을 그대로 두고 Pod에 JWKS sidecar와 init container를 더한다(2026-10-15 사용자 결정). 그래서 Pod 합계는 선택 profile보다 크다. 결과에는 broker container와 sidecar·init container의 requests·limits를 따로 기록하고, 이 값이 시험 overlay 조건임을 밝힌다.
- Broker cluster는 준비 단계에서 `session-metadata`, `proposal-recovery`, `shared-subscription-reroute` feature를 활성화하고 확인한다. `proposal-recovery`는 leader가 바뀔 때 전송 중이던 commit을 새 leader에 다시 제안해, 생존 node의 연결이 commit timeout으로 닫히지 않게 한다(2026-10-15 사용자 결정). `shared-subscription-reroute`는 member session이 끝날 때 shared group message를 다른 member로 옮긴다([ADR-0060](../decisions/ADR-0060-shared-subscription-durable-owner-loss-reroute.md)). 활성 feature 목록을 artifact에 기록한다.
- MQTT TLS는 broker listener가 종료한다. 용량 시험 overlay는 broker에 TLS listener 8883과 시험용 server certificate를 설정하고 NLB에 TCP 8883 listener를 추가한다. 기본 `eks-ec2-ha` manifest는 바꾸지 않는다.
- 인증은 §3.8과 같은 RS256 JWT다. Broker는 loopback이 아닌 `http` JWKS 주소를 거절한다. 그래서 각 broker Pod에 JWKS를 `127.0.0.1`로 제공하는 시험용 sidecar를 둔다. Tenant quota는 설정하지 않아 기본값(상한)을 쓰고, 적용된 값을 artifact에 기록한다. Tenant 한도에 걸린 거절이 나오면 해당 단계는 invalid다. 거절 분류는 [Quota 계열 거절 판정](#quota-계열-거절-판정)을 따른다.
- Generator Job은 broker worker와 다른 전용 node group에서 실행한다. Generator Pod의 CPU 사용률이 80%를 넘거나 publish schedule lag가 publish 간격 이상이면 generator 포화다. 포화한 단계는 실패가 아니라 invalid이며, generator 자원을 늘려 다시 잰다.

#### 단계 절차

각 단계는 새 client 집합으로 다음 순서를 따른다. 단계 사이에는 세 broker가 ready이고 resync를 마친 상태여야 한다.

1. `setup`: 모든 client를 NLB 주소로 연결하고 구독한다. 연결 속도는 profile의 `connectRatePerSecond`로 제한한다.
2. `warmup`: 정해진 시간 동안 부하를 건다. 판정하지 않는다.
3. `measureN`: 세 broker에서 `measureMillis` 동안 판정 구간을 잰다.
4. `failure`: 그 시점의 Raft leader Pod를 강제 삭제(`gracePeriodSeconds=0`)한다. 같은 시각에 그 worker를 cordon해 대체 Pod가 다시 schedule되지 않게 한다. Broker EBS는 AZ에 묶여 있어 다른 worker로 옮겨 가지 않는다. 부하는 계속 건다. 이 장애는 Pod 하나의 손실이다. PRD `EKS_EC2_HA`의 EC2 worker 또는 AZ 손실은 이 측정이 검증하지 않으며 별도 장애 시험이 다룬다.
5. `recovery`: 영향받은 client가 NLB로 재연결하고 stream마다 장애 뒤 첫 성공 완료를 관찰할 때까지다. RTO 정의는 §3.6과 같다.
6. `measureN1`: RTO 뒤 남은 두 broker에서 `measureMillis` 동안 판정 구간을 잰다.
7. `restore`: Worker를 uncordon하고 대체 Pod의 ready와 resync를 기다린다. 판정하지 않으며 `readyTimeoutMillis`를 넘기면 단계는 invalid다.
8. `cleanup`: 모든 client를 정리한다. Client ID는 단계마다 같으므로 persistent client ID마다 `CleanStart=true`, Session Expiry 0으로 접속했다가 끊어 이전 session을 지운다.

#### 판정

한 단계는 다음 조건을 모두 지킬 때 통과다.

- `measureN`과 `measureN1`에서 QoS별 publish-to-delivery p99가 100ms 이하다. Latency는 publish 직전부터 첫 copy 수신까지이며 generator 한 process의 단일 clock으로 잰다.
- 단계 전체에서 broker가 ACK한 QoS 1/2 message의 미수신(gap)이 0이고 QoS 2 중복이 0이다. QoS 1은 PUBACK, QoS 2는 PUBREC(reason < `0x80`)을 받으면 broker가 ACK한 것으로 본다. ACK를 받지 못한 publish는 `unacked`로 기록하고 gap에 넣지 않는다. Clean session publisher가 PUBREC을 받은 뒤 PUBCOMP 전에 연결을 잃은 QoS 2 message는 전달 보장 대상이 아니다([PRD §8](../requirements/PRD.md#8-메시지-보장-모델) QoS 2 행). 이 미수신은 gap이 아니라 `cleanPublisherInterrupted`로 수를 기록한다. QoS 1 중복과 QoS 0 손실은 기록만 한다. Gap 판정 규칙은 아래와 같다.
  - Message의 보장 수신자는 publish를 보낸 시점에 구독이 유효하고, 다음 중 하나를 만족하는 subscriber다. Persistent session subscriber이거나, clean session subscriber로서 그 연결이 drain 끝까지 유지된 경우다. 구독은 현재 연결에서 SUBACK을 받았거나 persistent session이 있으면 유효하다.
  - 보장 수신자의 미수신은 모두 gap이다. 시간 기준의 예외는 두지 않는다.
  - Clean session subscriber의 연결이 publish 뒤 수신 전에 끊기면 그 message는 보장 대상이 아니다. MQTT clean session은 끊긴 연결의 미전달 message를 보존하지 않기 때문이다. 이 미수신은 gap이 아니라 `cleanSessionLoss`로 QoS별 수와 publish부터 끊김까지의 최대 간격을 기록한다. 끊긴 뒤 다시 SUBACK을 받기 전에 보낸 message도 기대하지 않는다.
  - 단, publish부터 끊김까지가 p99 한도(EKS 100ms)를 넘은 `cleanSessionLoss`는 실패다. 장애 전부터 전달이 멈춘 message를 끊김이 가리지 않게 하기 위해서다(2026-10-15 사용자 결정). 한도와 같은 간격은 제외 대상이다.
- `measureN`과 `measureN1`에서 ACK timeout, protocol error와 계획 밖 disconnect가 0이다. Publisher가 inflight credit을 기다려 생긴 schedule lag가 publish 간격 이상이면 broker ACK가 부하를 따라가지 못한 것이므로 실패다. Credit 대기 없이 생긴 lag는 generator 포화이므로 invalid다.
- RTO가 30초 이하다([PRD §4.2](../requirements/PRD.md#42-초기-성공-지표)). Persistent session은 재연결에서 `SessionPresent=true`여야 한다.

통과한 단계 중 offered 부하가 가장 큰 단계를 후보로 둔다. 후보 단계를 독립적으로 5번 더 실행해 모두 통과해야 그 단계의 값을 `B_*`로 기록한다. 하나라도 실패하면 바로 아래 통과 단계로 같은 확인을 반복한다.

| 기호 | 기록 값 |
| --- | --- |
| `B_conn` | 채택 단계의 연결 수 `53*cells`와 TLS·TCP 비율 |
| `B_pub` | 채택 단계의 offered publish 속도와 QoS별 비율 |
| `B_sub` | 채택 단계 `measureN1`의 delivery 속도와 QoS별 비율 |
| `B_msg_size` | p50 128 byte, p95·p99·최대 16,384 byte |
| `B_topic` | 채택 단계의 filter 수와 exact·wildcard·shared 구성 |

이 결과는 선택 profile의 용량이다. EMQX 대비 상대 판정과 release gate는 별도이며 artifact는 `releaseGating=false`로 둔다. 실제 운영 traffic이 `B_*`를 넘으면 `B_*`를 다시 정한다.

<a id="quota-계열-거절-판정"></a>
#### Quota 계열 거절 판정

Quota 계열 거절은 runner가 MQTT reason code `0x97` 또는 `0x9F`를 받은 경우다. `0x97`은 tenant 한도와 서버 backpressure에 함께 쓰인다. 그래서 reason code만으로 원인을 정하지 않는다. Broker는 PUBACK·PUBREC `0x97`에 거절 원인을 Reason String으로 알린다([MQTT §7.2](mqtt-protocol-and-session.md#72-quota-계열-거절의-원인-표시)). Runner는 거절 하나마다 아래 분류를 정하고 기록한다.

| 분류 | 조건 | 판정 |
| --- | --- | --- |
| `tenant_limit` | PUBACK·PUBREC Reason String의 class·cause가 MQTT §7.2 표의 tenant 한도다. 또는 CONNACK `0x97`이다(cause `connections_or_sessions`). | 단계는 invalid다. |
| `server_backpressure` | PUBACK·PUBREC Reason String의 class·cause가 MQTT §7.2 표의 서버 backpressure다. | 단계는 fail이다. |
| `unclassified` | Reason String이 없거나 형식·class·cause를 알 수 없다. 또는 `0x9F`이거나 SUBACK `0x97`이다. | 단계는 fail이다. |

- 판정은 거절이 일어난 구간(`setup`, `warmup`, `measureN`, `failure`, `measureN1`, `restore`, `cleanup`)과 관계없이 단계 전체에 적용한다. `warmup`을 판정하지 않는다는 규칙은 이 거절에 적용하지 않는다. 거절은 broker가 publish를 받지 않았다는 뜻이다. 어느 구간이든 용량 판정을 통과시키면 안 된다. 서버 backpressure와 분류 불가를 구간별로 면제하지 않는다.
- `server_backpressure`는 broker가 부하를 감당하지 못한 증거이므로 fail이다. generator나 설정 문제를 뜻하는 invalid가 아니다. 분류 불가는 원인을 모르므로 통과시키지 않고 fail로 둔다. 원인을 추측해서 `tenant_limit`나 `server_backpressure`로 바꾸지 않는다.
- Invalid가 fail보다 우선한다는 규칙은 그대로다. 한 단계에 `tenant_limit`과 `server_backpressure`가 함께 있으면 단계는 invalid이고, 다른 분류의 건수는 `failConditions`에 증거로 남는다.
- 거절된 publish는 ACK를 받지 못한 publish다. gap에 넣지 않는다.
- Broker metric의 합계나 시각이 비슷하다는 이유로 거절의 분류를 바꾸지 않는다. Reason String을 보내지 않는 binary가 보낸 `0x97`은 `unclassified`다.
- `steps/<NN>-r<R>/quota-rejections.jsonl`에 거절마다 한 줄을 쓴다. 필드는 `at`, `phase`, `clientId`, `tenant`, `packet`(`connack`, `suback`, `puback`, `pubrec`), `reasonCode`, `reasonString`(있을 때), `classification`, `cause`(있을 때)다. `step-summary.json`의 `quotaRejections`는 분류별 건수와 구간별 건수를 담는다.

#### Runner 구성

- Generator는 target에 무관한 runner로 만든다. 입력은 MQTT endpoint, management endpoint, credential과 profile이다. 단계 경계에서 control marker(§3.6과 같은 방식)를 써서 외부 controller가 장애 주입과 복구를 수행한다.
- MQTT endpoint 주소 대체는 CONNECT를 보내기 전의 TCP dial·TLS handshake 실패에만 허용한다. 공통 MQTT client는 이 실패를 typed dial error로 구분한다. CONNECT 쓰기 오류(일부 byte만 보낸 경우 포함), CONNACK 대기 timeout·거절·protocol 오류는 다른 주소로 대체하지 않고 그대로 보고한다. 첫 서버가 ownership 처리를 계속하는 동안 같은 client의 두 번째 CONNECT를 보내지 않기 위해서다. Session reconnect loop와 단계의 timeout·재시도 횟수는 바꾸지 않는다.
- Local controller는 Docker 3-node cluster에서 같은 runner를 실행하고 container `SIGKILL`과 재시작으로 장애를 만든다. Runner와 판정 코드의 local 검증에 쓰며 용량 값의 증거가 아니다.
- EKS controller는 저장소 root의 workstation에서 `kubectl`로 Job 실행, marker 교환, Pod 강제 삭제, cordon·uncordon과 artifact 수집을 수행한다.
- 2026-10-16 사용자는 실제 worker 정지 대신 Pod 장애 시험 보완을 선택했다. 시험 overlay만 `broker-process.sh`를 PID 1(container 안의 첫 process)로 실행하고 broker를 자식 process로 둔다. 같은 PID namespace에서 PID 1에 보낸 `SIGSTOP`은 성공을 반환해도 실행을 멈추지 않을 수 있으므로 PID 1을 직접 멈추는 방식은 쓰지 않는다. 평소 preStop은 기본 배포와 같고 wrapper는 TERM·INT를 broker에 전달하고 종료 상태를 기다린다. Wrapper의 메모리·CPU도 broker container의 1 CPU·4 GiB 예산에 포함된다.
- Wrapper는 실행마다 UUID를 `/capacity-runtime`의 작은 memory emptyDir에 기록한다. Controller는 준비 때 Pod UID·container ID·broker PID·이 UUID를 보관한다. Runner ACK 뒤에는 UID·worker·container ID를 다시 대조한다. Container 안의 helper도 UUID와 실행 파일을 대조하므로 `kubectl exec`가 교체된 process에 도착하면 신호를 보내지 않는다. Broker 자식에게 `SIGSTOP`을 보내고 모든 thread가 실제 stopped 상태인지 확인한 뒤 Pod를 강제 삭제한다. 이 순서로 preStop이 broker에 정상 drain을 시작할 기회를 차단한다. 멈춘 시각·PID·thread 수·대상 identity와 삭제 시각을 기록한다. Worker·OS·EBS는 계속 실행되므로 전원 손실 증거는 아니다.
- `kubectl delete --wait=false`의 성공은 삭제 요청 수락이며 기존 Pod의 삭제 완료가 아니다. 요청 수락 뒤에도 정지 대상의 추적을 유지한다. `restore`에서 다른 UID인 Ready Pod를 확인한 뒤 추적을 마친다. 삭제 실패, controller 예외, Ctrl+C 또는 대기 시간 초과로 Python의 `finally`가 실행되면 원래 UID·container ID·실행 UUID가 확인되는 대상에만 `SIGCONT`를 보내 재개를 시도한다. 교체된 container에는 신호를 보내지 않는다. 주입·정지 확인·복구 실패는 invalid이며 기준을 낮춰 통과시키지 않는다. 정상 preStop과 기본 `eks-ec2-ha` manifest는 바꾸지 않는다.
- 2026-10-16 사용자는 정리 가능한 중단만 지원하기로 결정했다. `SIGKILL`, workstation 장애, 강제 process 종료처럼 `finally`가 실행되지 않는 중단에는 자동 재개를 보장하지 않는다. Lease나 watchdog은 두지 않는다. 이 경우 또는 Kubernetes API·exec 실패로 재개할 수 없는 경우에는 격리 fixture를 수동 복구하거나 삭제해야 한다. 중단된 측정을 용량 채택의 증거로 쓰지 않는다.
- Artifact는 §5를 따른다. 단계마다 `step-summary.json`, phase·message·connection 기록, broker와 generator의 resource 표본, Raft leader와 삭제한 Pod·worker, cordon·uncordon 시각을 남긴다.
- EKS controller는 `controller/runtime-health.jsonl`에 준비 뒤·각 단계의 ready와 complete·실행 종료 및 `--sample-seconds` 간격(기본 10초)의 표본을 남긴다. 경계 관측은 비동기로 요청해 장애 주입과 marker ACK를 지연시키지 않는다. 요청 시각(`requestedAt`)과 실제 수집 시각(`at`)을 구분한다. 수집 시간은 주기 간격에 더해진다. Broker container의 cgroup `cpu.stat`에서 `nr_periods`, `nr_throttled`, `throttled_usec` 누계와 같은 container의 직전 표본 대비 증가량을 기록한다. Cgroup v1의 `throttled_time`은 ns에서 µs로 바꾼다. 첫 표본·Pod UID 또는 container ID 변경·누계 감소 때 증가량은 알 수 없음(`null`)으로 기록한다.
- 같은 표본에는 broker와 generator Pod의 UID·worker, regular·init container별 restart count, 현재 상태와 직전 종료 상태(reason, exit code, signal, 시작·종료 시각)를 기록한다. `OOMKilled`는 Kubernetes가 보고한 reason 그대로 보존한다. 읽기 실패·권한 부족·교체 중 표본은 알 수 없음으로 남기며 0이나 정상으로 바꾸지 않는다. CPU 표본 수집 전후의 Pod·container identity가 다르면 그 CPU 표본을 버린다.
- 이 기록은 CPU limit에 걸린 지연과 OOM·재시작 원인을 분석하기 위한 관측 자료다. 판정 기준을 바꾸거나 장애 중 Pod를 복구하는 추가 동작은 하지 않는다. 주기 사이에 삭제된 Pod나 여러 번 재시작한 container의 모든 OOM 이력을 보장하지 않는다. 실제 EKS 수집은 별도 실행으로 확인해야 한다.
- `prepare`는 fixture ConfigMap과 Secret을 server-side apply로 갱신한다. 512 cell의 credential 파일은 약 586 KB이므로 client-side apply의 `last-applied-configuration` annotation(최대 262,144 bytes)에 복제할 수 없다. Secret 본문은 그대로 유지한다. 기존 client-side apply manager가 소유한 값과 충돌하면 강제로 덮어쓰지 않는다.
- Kill marker의 `target.nodeId`는 runner의 `killTarget.NodeID`에 맞는 JSON 문자열이다. Python 내부의 숫자 node ID는 marker를 쓸 때 문자열로 바꾼다. 숫자로 쓰면 Go가 marker를 읽지 못하므로 runner는 ACK를 쓰지 않고 해당 단계는 invalid가 된다.
- Local smoke profile(`tests/benchmark/capacity/testdata/local-smoke.json`)은 Docker에서 단계 진행·확인·정리 경로를 실행하려고 p99 한도를 1,000ms로 둔다. 이 값은 판정 기준이 아니다. EKS profile(`eks-capacity-v1.json`)은 100ms를 쓴다.

#### 용량 측정 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#39-bp-02-eks-용량-측정-기록)에 있다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-049 | NFR-01, NFR-03 | 용량 profile을 읽고 cell별 client를 계획한다. 범위 밖 profile 값을 입력한다. | Profile 범위를 검증한다. Cell당 client는 53명이고 tenant별 client ID를 정확히 나열한다. Filter는 cell 밖으로 나가지 않는다. |
| PRF-SCN-050 | NFR-02 | 보장 수신자, persistent session, clean session 연결 끊김, PUBCOMP 전에 끊긴 clean publisher, p99 한도와 같은 간격의 `cleanSessionLoss`를 입력해 gap을 판정한다. | 보장 수신자의 미수신은 persistent session에서 예외 없이 gap이다. PUBCOMP 뒤 미수신도 gap이다. Clean publisher의 PUBCOMP 전 끊김은 `cleanPublisherInterrupted`로 기록한다. 끊긴 동안의 publish는 기대에서 제외한다. p99 한도를 넘는 `cleanSessionLoss`는 실패다. |
| PRF-SCN-051 | NFR-02 | 단계 판정에 p99 경계값, invalid 사유, RTO와 `SessionPresent` 입력을 넣고 부하 단계를 탐색한다. | p99 100ms 경계를 지킨다. invalid가 실패보다 우선한다. RTO 30초 이하와 `SessionPresent=true`를 요구한다. 첫 실패 단계 앞뒤를 이분 탐색하고 후보 단계를 5회 확인한다. |
| PRF-SCN-052 | - | 준비 단계에서 `session-metadata`, `proposal-recovery`, `shared-subscription-reroute` feature를 활성화하고 확인한다. | 세 feature를 활성화한다. 하나라도 비활성이면 오류다. |
| PRF-SCN-053 | - | Runner와 controller가 control marker를 교환한다. Python controller가 쓴 상태 JSON 5개를 Go runner가 읽는다. Broker·sidecar·init container 자원을 기록한다. | Marker 순서와 timeout을 지킨다. 숫자 node ID가 아니라 JSON 문자열 node ID를 쓰므로 runner가 5개 상태를 모두 읽는다. Broker, sidecar와 init container의 requests·limits를 분리해 기록한다. |
| PRF-SCN-054 | - | 기본 `eks-ec2-ha` manifest와 용량 overlay `deploy/kubernetes/eks-ec2-ha-capacity/`를 렌더링해 비교한다. | MQTT TLS·security 경로만 기본 설정과 다르다. Sidecar, Job 자원과 routing 경계가 overlay 규칙을 따른다. |
| PRF-SCN-055 | - | 장애 신호 요청에 다른 Pod UID·worker·container, 종료·삭제·상태 미확인 대상과 sidecar를 지정한다. | 원래 live broker identity가 확인되지 않는 대상에는 신호를 보내지 않는다. |
| PRF-SCN-056 | - | Native amd64 Linux Docker의 실제 standalone broker에서 broker thread를 정지한다. 이어서 `SIGCONT`, 잘못된 실행 UUID, 삭제 오류 주입, 정상 `TERM` 전달을 확인한다. | Broker thread가 모두 정지하고 HTTP가 응답하지 않는다. `SIGCONT` 뒤 Ready가 된다. 잘못된 실행 UUID는 거절한다. 삭제 오류를 주입한 controller가 실제 process를 다시 실행시킨다. 정상 `TERM` 전달 뒤 exit 0이다. |
| PRF-SCN-057 | - | 삭제 요청은 성공했지만 원래 UID인 Pod가 남은 상태에서 Ctrl+C, controller 예외, restore timeout을 각각 주입한다. | 세 경우 모두 원래 identity의 process를 재개하고 HTTP Ready와 worker uncordon을 확인한다. |
| PRF-SCN-058 | - | cgroup v1·v2 CPU 값, 누계 증가, 재시작, Pod 교체, 읽기 실패와 `OOMKilled` 상태를 runtime 진단에 입력한다. | 단위를 변환하고 같은 container의 증가량을 계산한다. 재시작·Pod 교체·읽기 실패 때 baseline을 무효화한다. 현재·직전 OOM 상태를 보존한다. |
| PRF-SCN-059 | - | CPU·메모리를 제한한 Docker container에서 실제 `cpu.stat`를 읽고 `ResourceSampler`와 `RuntimeDiagnostics`가 JSONL을 기록한다. | Throttling 누계와 증가량을 기록한다. 재시작 뒤 증가량은 `null`이다. 실제 `OOMKilled` exit 137을 기록한다. |
| PRF-SCN-060 | - | 실제 EKS broker Pod에서 `kubectl exec`와 Pod status 조회로 cgroup v2 누계와 증가량을 수집한다. | Broker 3개 모두 읽기에 성공하고 누계와 증가량을 기록한다. |
| PRF-SCN-061 | - | 512 cell의 credential 파일(약 586 KB)을 EKS fixture Secret으로 준비한다. | Credential을 `last-applied-configuration` annotation에 넣지 않고 server-side apply로 Secret에 저장한다. Secret 값은 출력과 공개 artifact에서 제외한다. |
| PRF-SCN-062 | NFR-02 | `go run ./tests/benchmark/capacity local --profile tests/benchmark/capacity/testdata/local-smoke.json`을 실행한다. | 장애 주입, 복구, 판정과 정리가 끝까지 실행되고 모든 단계가 통과한다. |
| PRF-SCN-063 | NFR-01, NFR-02 | `tests/benchmark/capacity/eks/controller.py`로 `EKS_EC2_HA` profile에서 용량을 측정한다. | 모든 단계 판정을 통과하고 후보 단계를 5회 확인해 `B_*`를 기록한다. invalid 단계가 하나라도 있으면 `adopted`와 `bounds`는 `null`이다. |
| PRF-SCN-064 | NFR-02 | EKS에서 원래 Pod UID·container·실행 UUID를 확인하고 broker thread를 정지한 뒤 Pod를 강제 삭제한다. | `killing`·`down`·`restoring`·`restored`를 runner가 모두 관찰한다. 새 Pod가 Ready가 되고 resync를 마치며 active drain에 묶이지 않는다. |
| PRF-SCN-117 | NFR-02 | PUBACK·PUBREC `0x97`을 MQTT §7.2의 알려진 class·cause, 형식이 틀린 값, 모르는 class·cause, 빈 값으로 받는다. CONNACK `0x97`, `0x9F`와 SUBACK `0x97`도 받는다. | 알려진 class·cause는 그대로 분류·기록한다. CONNACK `0x97`은 `tenant_limit`, cause는 `connections_or_sessions`다. 형식이 틀린 값, 모르는 class·cause, 빈 값, `0x9F`와 SUBACK `0x97`은 `unclassified`다. 원인을 추측하지 않는다. |
| PRF-SCN-118 | NFR-02 | 단계 판정에 `tenant_limit`, `server_backpressure`, `unclassified` 거절을 따로 또는 함께 입력한다. | `tenant_limit`은 invalid다. `server_backpressure`와 `unclassified`는 fail이다. 함께 있으면 invalid가 우선하고 나머지 건수는 `failConditions`에 남는다. 거절이 없으면 판정을 바꾸지 않는다. |
| PRF-SCN-119 | NFR-02 | Runner가 구간(`setup`, `warmup`, `measureN`, `failure`, `measureN1`, `restore`, `cleanup`)마다 서로 다른 client·tenant로 quota 계열 거절을 관찰한다. Artifact 쓰기 오류도 입력한다. | `quota-rejections.jsonl`에 거절마다 시각, 구간, client, tenant, packet, reason code, Reason String, 분류와 cause가 한 줄씩 남는다. `step-summary.json`의 `quotaRejections`가 분류별·구간별 건수를 담는다. 어떤 구간의 거절도 판정에서 빠지지 않는다. 기록 실패는 실행 오류로 보존한다. |
| PRF-SCN-120 | NFR-02 | 외부 DNS 없이 실제 scripted TCP listener로 dial 실패, CONNECT를 받은 뒤 CONNACK timeout·거절, CONNECT 일부 byte를 받은 뒤 연결 reset을 만든다. | CONNECT 전 dial 실패만 다음 주소로 연결한다. 나머지는 오류를 보고하고 두 번째 CONNECT를 보내지 않는다. 공통 client의 typed dial error는 원래 오류의 `errors.Is`·`errors.As` 동작을 보존한다. |

## 4. 필수 benchmark profile

| ID | 고정 workload | 확인할 결과 |
| --- | --- | --- |
| BP-01 Protocol Baseline | MQTT 5.0 QoS 0, 비영속 세션, 고정 크기 payload | 프로토콜 처리량, latency, CPU 사용률 |
| BP-02 Production Mix | 승인된 `B_*` 부하, 실제 운영 환경의 TLS·인증·tenant 분포 | 운영 대체 가능성 검증 및 N/N-1 SLO |
| BP-03 Durable Delivery | QoS 1/2, 영속·offline 세션, 동등 수준 복제 구성 | ACK latency, 메시지 유실(gap), 디스크·네트워크 비용 |
| BP-04 Subscription Stress | wildcard, overlap, shared 구독, 높은 fan-out | 경로 전파 지연, 전달 처리량, tail latency |
| BP-05 Connection Scale | 점진적 연결 증가(ramp), 유휴 대기, keepalive, 일부 재연결 | 연결 성공률, 연결당 메모리 사용량, 재연결 속도 |
| BP-06 Failure Recovery | 노드 강제 종료(kill), 재참여, 상태 재동기화 | RTO, 유실·중복 여부, 복구 중 리소스 급증 관찰 |
| BP-07 Rolling Drain | 연결 유지 상태에서 순차적 노드 교체 | 드레인 완료 시간, 재연결 분포, Will 메시지 결과 |

BP-03은 durable 구간이, BP-07은 drain 구간이 정상 완료되어야 해당 프로파일이 통과된다. 일부 구간만 실행한 결과는 프로파일 전체 통과로 보지 않는다.

## 5. Artifact manifest

각 run ID는 Git 커밋, NaruMQ 및 Go 버전, EMQX 버전·digest·에디션·라이선스·설정, 배포 프로파일·지원 상태·토폴로지·스토리지를 연결한다. 해당할 경우 AWS/EKS/EC2/EBS CSI/NLB의 해석된 설정도 기록한다. 제너레이터·클라이언트 SDK·seed·명령어, 확정 프로파일, 단계 경계·반복·타임스탬프, 원본 표본·로그·지표 및 오류·유실·중복 결과를 보존한다. 보안 비밀 값은 저장하지 않고 식별 참조만 남긴다. 모든 실행 이미지에는 역할, immutable digest와 OS·architecture를 기록한다. BP-02 EKS run에서는 NaruMQ·EMQX·generator와 실제 실행한 rollout controller·필수 add-on image를 각각 기록한다. Container가 아닌 generator도 실행파일의 OS·architecture를 남긴다.

- **BP-01**: 프로파일 및 SHA, 확정 설정, `samples.jsonl`, NaruMQ runtime 지표, workload summary, 브로커 통계·로그, 불변 이미지 메타데이터를 보존한다.
- **BP-02**: `workload-summary.json`, `resource-summary.json`, `phases.jsonl`, `messages.jsonl`, `retained.jsonl`, `connections.jsonl`, 브로커 통계·로그와 각 image의 역할·immutable digest·OS·architecture를 보존한다. EKS run은 실제 실행한 rollout controller와 필수 add-on image도 포함한다.
- **BP-03**: target별 workload·resource·WAL summary, `phases.jsonl`, `messages.jsonl`, `diagnostics.jsonl`, 브로커 로그를 보존한다.
- **BP-04**: workload·resource summary, `phases.jsonl`, `messages.jsonl`, `route.jsonl`, 브로커 로그를 보존한다.
- **BP-05**: `connections.jsonl`, `phases.jsonl`, workload·resource summary를 보존한다.
- **BP-06**: target별 failure·workload·resource summary, `phases.jsonl`, `messages.jsonl`, `connections.jsonl`, 브로커 로그를 보존한다.
- **BP-07**: target별 rolling·workload·resource summary, `phases.jsonl`, `messages.jsonl`, `connections.jsonl`, 브로커 로그를 보존한다.

기본 실행 결과 경로는 `.artifacts/benchmark/<bp>/<run-id>/`다. 반복 suite는 해당 프로파일 루트 아래 suite ID 디렉터리와 `runs/001/`부터의 하위 실행 결과를 보존한다. 로컬 smoke 결과는 모두 `releaseGating=false`다. BP-03의 로컬 Docker 볼륨 검증은 운영 파일시스템이나 물리 전원 손실 검증을 대신하지 않는다.

Suite 루트에는 `suite-manifest.json`, `suite-summary.json`, 하위 실행 디렉터리가 있다. Manifest는 요청·완료 반복 횟수, 교차 실행 순서, 호스트·Docker 메타데이터, 유효성 판정을 기록한다. Summary는 모든 하위 실행과 지표가 유효할 때만 target별 지표와 중앙값·MAD·범위를 기록한다. 하위 실행이 하나라도 실패하면 suite 전체를 무효로 처리하고 지표를 비운다. EMQX OSS 5.8.8은 2026-02-28 EOL된 역사적 비교 대상이며 현재 지원 기준선이 아니다.

NaruMQ는 누적 `runtime/metrics` 표본을 `runtime-metrics.jsonl`에 기록한다. 측정 대상 구간에는 시작과 종료가 정확히 하나씩 있어야 하며, 구간 안에 서로 다른 표본 시각이 최소 2개 있어야 한다.

```text
elapsedSeconds = lastCollectedAt - firstCollectedAt
allocationBytesPerSecond = delta(heapAllocBytesTotal) / elapsedSeconds
allocationsPerSecond = delta(heapAllocObjectsTotal) / elapsedSeconds
gcCPUFraction = delta(gcCPUSecondsTotal) / delta(runtimeCPUSecondsTotal)
```

GC pause 백분위수는 누적 히스토그램 표본 차이에 nearest-rank를 적용하고 선택된 버킷의 상한(초)을 기록한다. Pause 표본이 없으면 0으로 기록한다. 무한대 버킷 선택, 카운터 감소, 버킷 경계 변경이 발생하면 요약을 무효로 처리한다. GC 주기가 실행되지 않았으면 추정하지 않고 `cpuMetricsAvailable=false`와 사유를 기록한다. Runtime CPU 차이가 0인데 GC CPU 차이가 있으면 무효다.

이 값은 NaruMQ Go 프로세스 진단 전용이다. GC CPU는 Docker 컨테이너 CPU가 아니며 EMQX BEAM 지표로 환산하지 않는다. EMQX는 0 대신 사유와 함께 `not_applicable`로 보고한다. 이 결과만으로 Gate 7·8이나 운영 SLO를 닫지 않는다.

## 6. Regression gate

모든 릴리스는 이전 안정 NaruMQ와의 회귀를 대조한다. 이전 안정 NaruMQ 대비 처리량이 5% 이상 낮아지거나 p99 latency가 5% 이상 나빠지면 릴리스를 차단한다. 측정 변동성이 5%를 초과하면 합격·불합격을 판정하지 않고 원인을 조사한 뒤 반복 횟수를 늘린다.

2026-10-16 사용자 결정에 따라 상용 EMQX Enterprise 라이선스를 요구하는 95%/105% 상대 비율 조건은 필수 릴리스 게이트에서 제외하고 선택적 외부 벤치마크로 운영한다. 필수 제품 출시 게이트는 5% self-regression 규칙과 `EKS_EC2_HA` qualification profile에서 측정한 N-1 자립형 수용 용량(`B_*`)으로 판정한다.

선택적 EMQX 비교를 실행할 때 적용하는 참고 기준은 다음과 같다.

- 같은 `B_*`와 동등 profile에서 NaruMQ 처리량은 EMQX의 95% 이상이어야 한다.
- Publish-to-delivery p99/p99.9 latency, 성공 전달당 CPU와 연결당 memory는 EMQX 값의 105% 이하여야 한다.
- Network/disk 비용도 측정한다. 기능·durability가 `nonEquivalent`인 지표는 직접 우열 합격 판정에서 제외한다.
- 각 target은 최소 5회 독립 실행하고 중앙값을 비교한다.

필수 성능 릴리스 게이트는 N-1 수용 용량 `B_*` 산출 및 자체 회귀 5% 미만 충족 시 통과로 판정한다.

## 7. Verification 계층

| 계층 | 범위 | 필요 환경 |
| --- | --- | --- |
| unit | 정상·오류·경계 상태 머신, ACL, 할당량(quota) | 없음 (결정적 테스트 더블 사용) |
| property/fuzz | 파서, 인코더, 토픽·필터 매칭, 세션 불변식 | 없음 |
| mutation | 프로토콜, 전달, 소유권, ACL, 드레인 상태 머신 | 결정적 하네스 |
| integration | 리스너, 스토리지, 합의 전송 계층, 인증 어댑터 | 격리된 의존성 |
| conformance | MQTT 3.1.1 및 5.0 표준 규격 문장과 와이어 동작 | MQTT 테스트 클라이언트 |
| end-to-end | Kubernetes, 관리 API, 클라이언트 호환성, 롤링 업데이트 | 운영 유사 클러스터 |
| chaos | 프로세스·호스트 손실, 네트워크 장애, 디스크·스토리지 오류 | 운영 유사 클러스터 |
| benchmark | 성능, 수용량 한계, 성능 회귀, EMQX 비교 | 독립된 격리 환경 |

### 7.1 MQTT 클라이언트 호환성 검증

SDK 상호운용성은 Docker/E2E release evidence gate이며 일반 unit/local 검증과 별도다. 저장소 root에서 Go 1.26.5와 Linux/amd64 Docker가 필요하다.

```text
go run ./tests/compatibility/mqtt-clients-v1 \
  --run-id phase1-paho-go-python \
  --output .artifacts/compatibility/mqtt-clients-v1/phase1-paho-go-python
```

Checked-in strict profile은 amd64 image에서 Paho Go MQTT 3.1.1 v1.5.1, Go MQTT5 v0.23.0, Python `paho-mqtt` 2.1.0을 build한다. Docker가 없거나 amd64가 아니면 case를 시작하지 않고 환경 실패 evidence를 쓴다. Manifest, summary, case, build log, image inspect, case별 broker/client 출력을 보존한다. Payload, credential, raw certificate, 외부 client ID, secret은 artifact에 넣지 않는다. Phase 1은 Go 10, Python 10, SDK 간 QoS1 4, MQTT 3.1.0 reject 2개 등 26 case다. `MQTT5_DRAIN_SAFE`와 malformed server DISCONNECT는 여기서 다루지 않는다. Adapter 계약은 [Drain §10](drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)을 따른다.

필수 drain suite는 별도 run ID와 output 경로를 쓴다. 기존 artifact 경로를 재사용하지 않는다.

```text
go run ./tests/compatibility/mqtt-clients-v1 --suite drain \
  --run-id drain-required-storage-fixed-20261016 \
  --output .artifacts/compatibility/mqtt-clients-v1/drain-required-storage-fixed-20261016
```

2026-10-16 사용자 결정에 따라 `--suite drain`은 SDK cluster drain 2 case만 필수로 실행한다. 새 3-node broker, JWT/JWKS, test readiness LB에서 redirect·session resume·후속 전달·Will 억제와 `SAFE_TO_STOP`을 검증한다. Fake server/trap listener를 쓰는 malformed DISCONNECT 8 case는 `--suite sdk-diagnostics`로 분리한다. SDK 진단은 선택 사항이며 제품 release gate와 정상 drain 판정에 합치지 않는다. 실행한 진단의 실패·누락은 별도 summary와 nonzero exit로 보존한다. 전체 case 목록과 선택 기준은 [Client contract §6.2·§6.3](../contracts/mqtt-client-compatibility-v1.md#62-drain-compatibility-suite)을 따른다.

Timeout, Will delay, reconnect, LB settle, rate는 test 전용이다. Secret/private key는 임시 경로에만 둔다. Local verification은 이 Docker 실행들을 제외하며 nested SDK module을 다운로드하거나 Docker를 시작하지 않는다.

Suite 분리와 fixture 수정 뒤의 실행 결과는 [Client contract §6.4](../contracts/mqtt-client-compatibility-v1.md#64-검증-범위-분리-실행-기록-2026-10-16)에 둔다. 필수 정상 drain의 최초 fixture 실패와 최종 통과, 별도 SDK 진단 실패를 구분해 보존한다.

### 7.2 Fuzz 게이트와 수동 mutation 검증

표준 실행 명령:

```text
go run ./tools/verify local
go run ./tools/verify ci --base <merge-base>
go run ./tools/verify acceptance [--run <regexp>]
go run ./tools/verify mutation
```

`local`과 `ci`는 gofmt 사전 점검 뒤 `.artifacts/verification/branch/module`의 계측 사본에서 atomic coverage를 포함한 전체 `go test ./...`를 한 번 실행한다.

Coverage의 전체 package 실행에는 `-timeout=3h`를 적용한다. 이는 opt-in 환경 변수를 켜고 gate를 실행할 때 100회 rolling process 시험이 쓰는 기존 fixture 수명과 같은 실행 예산이다. Broker 요청·장애 복구·시나리오별 제한 시간은 바꾸지 않는다. 테스트 doc comment에 `Acceptance: <환경 변수>`를 표시한 opt-in 수용 시험은 그 환경 변수가 없으면 건너뛰고 `미실행 (게이트 밖)`으로 보고한다. 이 표시가 없는 연결 시험을 건너뛰면 뒤의 실행 검사가 실패한다.

시나리오 실행 검사는 Go 도구가 현재 빌드에서 제외한 파일의 테스트를 결과가 없다는 이유로 실패시키지 않는다. 파일명의 GOOS·GOARCH 제약과 `//go:build`를 그대로 따른다. 소스 연결 검사에서는 제외된 파일도 유지한다. 다른 플랫폼에서 명시적으로 받은 통과·실패 결과는 버리지 않는다. 활성 파일의 일반 테스트가 건너뛰거나 결과가 없으면 여전히 실패다. 외부 실행 결과의 빌드 환경 재현 범위는 [테스트 컨벤션 §5](../conventions/test-conventions.md#5-검증-명령과-통과-기준)를 따른다.
같은 package·테스트 이름을 서로 배타적인 플랫폼 파일에 정의하고 같은 시나리오에 연결하면 하나의 결과 키로 합친다. 현재 빌드에 포함되는 선언이 하나라도 있으면 그 키는 실행 범위 안에 남는다.

Local의 `-parallel`은 8이다. CI는 Go의 유효 `GOMAXPROCS` 값을 `-parallel`에 넘긴다. 이 값은 package 안에서 `t.Parallel()`로 실행하는 test의 상한이다. Preflight, coverage, race, fuzz는 한 병렬 단계에서 실행한다. `local`과 `ci`는 mutation을 실행하지 않는다.

병렬 명령 하나가 실패하면 나머지 명령을 취소한다. 하위 process를 종료한 OS가 종료 코드만 반환하더라도 runner는 context의 취소 원인을 보존해야 한다. 이 취소를 별도의 시험 실패로 보고하지 않는다. 취소 없이 끝난 명령의 실제 non-zero 종료 코드는 그대로 실패로 보고한다.

Process integration test(`tests/integration`의 `clusterMembershipProcessAddresses`)는 child broker의 `127.0.0.1` port를 OS 동적 포트 범위 아래에서 무작위로 고른다. 범위는 `[10000, 상한)`이다. 상한은 32768이고, Linux에서 `/proc/sys/net/ipv4/ip_local_port_range`의 시작 port가 더 낮으면 그 값을 쓴다. OS는 `bind(0)`과 `connect()`에 동적 범위의 port만 준다. 예약한 port를 닫은 뒤 broker가 bind하기 전에 다른 process가 같은 port를 받으면 broker는 `bind: address already in use`로 시작에 실패한다. 병렬 단계의 race test와 다른 package test가 `:0` listener를 자주 열기 때문에 이 경합이 실제로 일어난다([검증 기록 §14](../verification/performance-and-verification.md#14-process-test-port-경합-수정-2026-10-18)). 동적 범위가 10000부터 1024개 미만의 여유만 남기면 OS가 port를 고르며 같은 경합이 남는다. Windows·macOS는 동적 범위를 읽지 않고 기본 시작 port(49152) 아래만 보장한다. 동적 범위를 넓힌 host에서는 같은 경합이 남는다.

`local`과 `ci`의 preflight는 `go vet`, `go run ./tools/verify traceability`, `go run ./tools/verify scenarios`, `go run ./tools/verify architecture` 순서로 실행한다. `scenarios`는 이 문서의 `시나리오 ID` 표와 테스트 소스의 `Scenarios:` 메타 정보를 대조하는 연결 검사다. 결과 파일 없이 실행하므로 `오류`, `미연결`, `실패`가 하나라도 있으면 gate가 실패하고 `미실행`은 실패가 아니다. 계측 사본의 coverage `go test ./...`는 `-json`으로 실행해 stdout을 `.artifacts/verification/go-test.json`에 쓰며 실패하면 이 파일에서 실패한 시험 목록을 출력한다. 병렬 검사가 끝나면 gate는 `go run ./tools/verify scenarios --go-test-json .artifacts/verification/go-test.json --require-executed`로 실행 검사를 한다. 실행 검사는 `오류`·`미연결`·`실패`에 더해 실행 범위(결과 JSON에 나온 Go package의 `Test*`·`Fuzz*`, `--results` 파일에 결과가 있는 시험)에서 통과 결과가 없는 연결 시험(`미실행 (건너뜀)`, `미실행 (결과 없음)`)도 실패로 처리한다. 실행 범위 밖의 연결 시험(결과 파일이 없는 Python·EKS 함수, 결과 JSON에 없는 Go package, 결과 기록이 없는 `Test*`가 아닌 Go 함수, 통과·실패 결과가 없는 `Acceptance:` 표시 시험)은 `미실행 (게이트 밖)`으로 보고하며 실패로 처리하지 않는다. 그런 시험의 통과·실패 증거는 [검증 기록](../verification/performance-and-verification.md)이나 아래 `acceptance` 결과에 둔다.

Python 시나리오 함수는 `tests/scenario_results.py`로 `.artifacts/verification/scenarios/python-results.jsonl`에 결과(`<file>#<def>` 키, 상태, commit, 환경, 명령, 종료 시각)를 JSON Lines로 덧붙일 수 있다. 이 기록은 gate에 포함하지 않는다. 시나리오별 보고서는 `.artifacts/verification/scenarios/report.md`와 `report.json`에 쓰고 CI는 두 파일을 artifact로 올린다. 같은 보고서를 수동으로 만들려면 `go test -json ./... > .artifacts/verification/go-test.json` 뒤 `go run ./tools/verify scenarios --go-test-json .artifacts/verification/go-test.json`을 실행한다. 메타 정보 문법과 상태 정의는 [테스트 컨벤션](../conventions/test-conventions.md)을 따른다.

Opt-in 수용 시험은 `go run ./tools/verify acceptance [--run <regexp>]`로 따로 실행한다. 이 시험들은 100회 rolling, Kind, Docker 같은 별도 환경과 긴 실행 시간이 필요하다. 2026-10-17 측정에서 이 시험 11개는 integration package 시간의 약 78%(약 2420초)였다. 기본 gate가 매번 이 시험을 요구하면 `local`이 약 52분 걸리고, opt-in 환경이 없는 CI는 통과할 수 없다. 그래서 기본 gate는 일반 시험의 실행을 증명하고, opt-in 수용은 이 명령의 결과로 증명한다. 두 결과를 함께 보고하려면 `scenarios`에 두 Go JSON 파일을 모두 넘긴다.

기본 gate는 논리 시험만 실행한다([ADR-0064](../decisions/ADR-0064-ci-gate-logic-only.md)). 합격 여부가 측정한 지연·처리량·percentile·지연 비율 한도나 부하 강도에 달린 부하·품질 시험은 `Acceptance: NARUMQ_LOAD_QUALITY`를 표시해 gate 밖에 둔다. 기본 gate는 이 시험을 건너뛰고 해당 시나리오를 `미실행 (게이트 밖)`으로 보고한다. 이 시험은 `NARUMQ_LOAD_QUALITY=1 go run ./tools/verify acceptance --run <regexp>`로 실행하며, 통과 증거는 이 명령의 결과와 검증 기록이다. 현재 대상은 네 개다. TEN-09(`SEC-SCN-030`), CPU 경쟁 아래 persistent subscriber 시험 `TestN1PersistentClose`(`CLU-SCN-241`), in-process BP-01·BP-05 runner 부하 실행(`PRF-SCN-122`·`PRF-SCN-123`)이 해당한다. 멈춤을 잡는 제한 시간, 설정한 대기의 최소 시간, fake clock으로 계산하는 rate 한도처럼 논리 결과를 판정하는 시험의 시간 값은 gate에 남긴다.

`acceptance`는 다음 순서로 동작한다.

1. 연결 검사와 같은 scanner로 `Acceptance:` 표시가 있는 Go `Test*` 함수를 찾는다. `--run`을 주면 시험 이름이 이 Go 정규식과 맞는 것만 고른다. 고른 시험이 없으면 exit 1이다.
2. 고른 시험마다 표시한 환경 변수가 `1`인지 확인한다. 하나라도 아니면 시험을 실행하지 않고 빠진 환경 변수 목록과 함께 exit 1이다. Kind registry 주소 같은 추가 입력은 각 시험이 직접 확인한다.
3. 원본 root에서 `go test -json -count=1 -timeout=3h -run '^(<시험 이름>)$' <package>`를 실행한다. Stdout은 `.artifacts/verification/acceptance-go-test.json`에 쓴다. 이 명령은 coverage 계측 사본을 쓰지 않고 coverage를 판정하지 않는다.
4. 고른 시험이 모두 통과해야 `acceptance: PASS`와 exit 0이다. 건너뜀·실패·결과 없음이 하나라도 있으면 해당 시험 이름과 함께 `acceptance: FAIL`과 exit 1이다. 이 명령은 Kind cluster, registry, Docker image를 만들거나 지우지 않는다.

```text
go run ./tools/verify acceptance --run 'ReleaseCompatibility'
go run ./tools/verify scenarios --go-test-json .artifacts/verification/go-test.json --go-test-json .artifacts/verification/acceptance-go-test.json
```

CI workflow는 Go gate 외에 S3 helper와 EKS worker observer의 Python 회귀를 별도 단계로 실행한다. Observer만 로컬에서 확인하려면 `python -B tests/test_eks_worker_recovery.py -v`를 실행한다. 이 시험은 AWS·Docker 없이 관찰 독립성·오류 보존·종료·Node identity 경쟁 경계를 확인한다. 실제 broker·Kubernetes API smoke와 남은 EKS gate는 [Drain spec §10](drain-and-rolling-update.md#eks-worker-observer-보완-기준)을 따른다. 이 로컬 통과는 production 장애 수용을 대신하지 않는다.

개발자가 mutation을 실행하려면 `go run ./tools/verify mutation`을 사용한다. 이 명령은 `delivery`, `mqtt`, `nodedrain`, `security`, `session` 다섯 package를 대상으로 한다. Gremlins worker와 test CPU는 각각 1이다. Runner는 한 번에 `max(1, min(5, floor(GOMAXPROCS/2)))`개 package를 실행하고 나머지는 다음 batch로 넘긴다. 실패하면 같은 batch의 남은 명령을 취소한다.

각 결과는 `.artifacts/verification/mutation-<package>.json`에 저장한다. 기본 Gremlins 설정은 `tests/verification/gremlins.yaml`에 있다. 기본 mutator는 `ARITHMETIC_BASE`, `CONDITIONALS_NEGATION`, `CONDITIONALS_BOUNDARY`, `INCREMENT_DECREMENT`, `INVERT_NEGATIVES`의 5종이다. Source, file, mutant exclusion은 없다. CI의 `--base <merge-base>`는 coverage diff 계산에만 쓴다.
`go run ./tools/verify mutation`은 package별 efficacy와 mutant coverage를 보고한다. 각 점수는 80% 이상이어야 한다. `TIMED OUT` mutant는 완료한 `KILLED`나 `LIVED`로 세지 않는다.

```text
go run ./tools/verify mutation
```

`efficacy = KILLED/(KILLED+LIVED)*100`; `mutant coverage=(KILLED+LIVED)/(KILLED+LIVED+NOT_COVERED)*100`이다. 두 값 모두 package마다 ≥80%여야 한다. `LIVED`는 test assertion이 mutant를 잡지 못했다는 뜻이므로 정상·오류·경계 전이 시험을 보강한다. `TIMED OUT`은 kill로 계산하지 않는다.

Fuzz target 5개는 별도 process로 병렬 실행하며 각각 `-parallel=1 -fuzztime=10000x`를 쓴다.

```text
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzDecoder$ -fuzztime=10000x -parallel=1
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzCodecRoundTrip$ -fuzztime=10000x -parallel=1
go test ./internal/domain/routing -run=^$ -fuzz=^FuzzTopicMatching$ -fuzztime=10000x -parallel=1
go test ./internal/domain/session -run=^$ -fuzz=^FuzzLifecycleTransitions$ -fuzztime=10000x -parallel=1
go test ./internal/adapters/tcp -run=^$ -fuzz=^FuzzConnectionInput$ -fuzztime=10000x -parallel=1
```

Discovery 순서는 비결정적이다. 실패 corpus는 `testdata/fuzz/<Target>/`에 보존해 일반 시험과 다음 campaign에서 재현한다. 수정한 feature는 재현 시험을 포함해야 한다. Flaky isolation은 임시 조치이며 owner와 해결 기한을 정한다.

MQTT normative statement 연결표는 `tests/conformance/mqtt-compliance-v1.json`이다. `go run ./tools/verify traceability`는 모든 `cmd`, `internal`, `tests` 아래 Go test·fuzz doc comment의 `Traceability:` marker를 읽는다. MQTT statement와 compliance matrix를 양방향 비교하고, marker의 모든 요구사항 ID가 PRD에 있는지 확인한다. PRD의 모든 P0 요구사항은 하나 이상의 `Test*`·`Fuzz*` 함수에 연결하거나 `tests/conformance/p0-requirement-exceptions-v1.json`에 정렬된 예외로 등록해야 한다. 시험 연결과 예외가 함께 있거나 둘 다 없으면 gate가 실패한다. `-prd`와 `-p0-exceptions`는 입력 경로를 바꿀 때 사용한다.

#### 검증 gate 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#72-fuzz-게이트와-수동-mutation-기록)에 있다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-065 | TEST-10 | Traceability 검증에 P0 연결, 미연결 P0, stale 예외, PRD에 없는 ID, 정렬되지 않은 예외와 tracking 경계의 8개 사례를 입력한다. | 연결됐거나 올바른 예외로 등록된 P0만 통과한다. 미연결 P0, stale 예외, PRD에 없는 ID, 정렬 위반과 tracking 경계 위반은 거절한다. 시험 연결과 예외가 함께 있거나 둘 다 없으면 실패한다. |
| PRF-SCN-066 | TEST-10 | P0 예외 JSON에 schema 오류, 중복, 빈 값과 경로 경계 위반 9개 사례를 넣어 strict decode한다. | 각 사례를 거절한다. |
| PRF-SCN-067 | TEST-10 | PRD의 요구사항 ID prefix와 MQTT statement ID를 traceability가 파싱한다. | PRD prefix를 받아들이고 MQTT statement ID 규칙은 그대로 유지한다. |
| PRF-SCN-068 | TEST-10 | 임시 예외 파일에서 `UI-01` 등록을 제거한 경우와 `API-01` stale 예외를 등록한 경우에 `go run ./tools/verify traceability`를 실행한다. | 두 경우 모두 exit 1이며 지정한 오류를 출력한다. |
| PRF-SCN-069 | TEST-10 | 저장소 root에서 `go run ./tools/verify traceability`를 실행한다. | exit 0이다. 모든 P0 요구사항이 `Test*`·`Fuzz*` 연결이나 정렬된 예외 중 하나에만 속한다. |
| PRF-SCN-070 | TEST-06, TEST-07, TEST-08, TEST-13, TEST-14 | 저장소 root에서 `go run ./tools/verify local`을 실행한다. | `local: PASS`다. Branch instrumentation, traceability, scenarios, fuzz 5개 target, architecture, 전체 package·integration 시험, production race, vet과 coverage 판정이 모두 통과한다. Mutation은 실행하지 않는다. |
| PRF-SCN-071 | TEST-06, TEST-07, TEST-08, TEST-13, TEST-14 | 저장소 root에서 `go run ./tools/verify ci --base <merge-base>`를 실행한다. | `ci: PASS`다. Coverage는 유효 `GOMAXPROCS`를 `-parallel`로 쓴다. `local`과 같은 단계를 실행하며 mutation은 실행하지 않는다. `--base`는 coverage diff 계산에만 쓴다. |
| PRF-SCN-072 | - | 스펙 시나리오 표에 있으나 테스트 메타 정보가 없는 시나리오가 있는 상태에서 `go run ./tools/verify scenarios`를 실행한다. | 해당 시나리오를 `미연결`로 보고하고 exit 1이다. |
| PRF-SCN-073 | - | `Scenarios:` 메타 정보가 없는 스펙 파일이나 없는 시나리오 ID를 가리키거나, 형식이 틀리거나, 테스트 함수 바로 위가 아닌 곳에 있다. | 해당 메타 정보를 `오류`로 보고하고 exit 1이다. |
| PRF-SCN-074 | - | 한 시나리오에 연결된 테스트의 결과가 `통과`, `실패`, 결과 없음 또는 건너뜀으로 섞여 있다. `--go-test-json`이나 `--results`로 결과를 읽는다. | 시나리오 상태는 `실패`, `미실행`, `통과` 순으로 우선한다. 연결된 테스트가 하나라도 실패하면 `실패`이고 exit 1이다. 결과가 없거나 건너뛴 테스트가 있으면 `미실행`이다. |
| PRF-SCN-075 | - | `go run ./tools/verify local` 또는 `go run ./tools/verify ci --base <merge-base>`를 실행하고 시나리오 연결에 `오류`나 `미연결`이 있다. | `scenarios`가 결과 파일 없이 preflight 단계로 실행되고 gate가 실패한다. 순서는 `go vet`, `traceability`, `scenarios`, `architecture`다. |
| PRF-SCN-076 | - | 같은 스펙 파일 안에 같은 시나리오 ID가 두 번 이상 있다. | 중복 ID를 `오류`로 보고하고 exit 1이다. |
| PRF-SCN-110 | - | `go run ./tools/verify scenarios --go-test-json <파일> --require-executed`를 실행한다. 실행 범위의 연결 시험이 건너뛰어졌거나 결과 이벤트가 없다. | `오류`·`미연결`·`실패`와 함께 통과 결과가 없는 연결 시험(`미실행 (건너뜀)`, `미실행 (결과 없음)`)도 실패로 처리하고 exit 1이다. |
| PRF-SCN-111 | - | 연결된 시험이 실행 범위 밖에 있다. 결과 파일이 없는 Python·EKS 함수, 결과 JSON에 없는 Go package, 결과 기록이 없는 `Test*`가 아닌 Go 함수를 연결한다. | 해당 시나리오를 `미실행 (게이트 밖)`으로 보고하고 실행 검사를 실패시키지 않는다. |
| PRF-SCN-112 | - | 결과 파일 없이 `go run ./tools/verify scenarios`를 실행한다. 연결된 시험이 실행되지 않았다. | `오류`·`미연결`·`실패`만 실패로 처리한다. `미실행`은 실패가 아니다. |
| PRF-SCN-113 | - | `go run ./tools/verify local` 또는 `ci`를 실행한다. | 계측 사본의 coverage `go test ./...`가 `-json`으로 실행되어 stdout이 `.artifacts/verification/go-test.json`에 쓰인다. 실패하면 이 파일에서 실패한 시험 목록을 출력한다. 병렬 검사가 끝난 뒤 같은 파일로 실행 검사를 한다. 검증 명령은 모두 14개다. |
| PRF-SCN-114 | - | Python 시나리오 함수가 `tests/scenario_results.py`로 결과를 기록한다. CI의 Python unittest는 `tests/run_scenario_unittest.py`로 실행한다. | 결과 파일에 `<file>#<def>` 키, 상태, commit, 환경, 명령과 종료 시각을 JSON Lines로 덧붙인다. 보고서에 commit과 환경이 나온다. CI unittest 결과는 `python-ci-results.jsonl`로 Go 결과와 함께 실행 검사에 입력한다. 라이브 EKS 하네스의 `python-results.jsonl`은 별도 수동 실행 결과이며 CI에서 실행한 것으로 세지 않는다. |
| PRF-SCN-115 | - | 기본 gate의 실행 검사에서 `Acceptance: <환경 변수>`를 표시한 연결 시험이 건너뛰어졌거나 결과 이벤트가 없다. 별도로 표시가 없는 연결 시험을 건너뛴다. `Acceptance:`를 `_test.go`의 `Test*` 함수가 아닌 곳에 쓰거나 환경 변수 이름 없이 쓴다. | 표시한 시험의 시나리오는 `미실행 (게이트 밖)`이며 실행 검사를 실패시키지 않는다. 표시한 시험이 실제로 통과·실패하면 그 결과를 그대로 센다. 표시가 없는 시험의 건너뜀은 `미실행 (건너뜀)`으로 실패한다. 잘못 쓴 `Acceptance:`는 `오류`다. |
| PRF-SCN-116 | - | `go run ./tools/verify acceptance [--run <regexp>]`를 실행한다. 고른 시험의 환경 변수가 빠졌거나, 고른 시험이 건너뛰어지거나 실패하거나 결과가 없다. | 환경 변수가 빠지면 시험을 실행하지 않고 빠진 이름을 출력하며 exit 1이다. 고른 시험이 모두 통과하면 `acceptance: PASS`와 exit 0이다. 하나라도 통과하지 않으면 해당 시험 이름과 `acceptance: FAIL`을 출력하며 exit 1이다. 결과는 `.artifacts/verification/acceptance-go-test.json`에 남는다. |
| PRF-SCN-121 | - | Windows·Linux 파일명, GOARCH 파일명과 `//go:build` 제약이 있는 테스트를 연결하고 현재 Go 환경으로 실행 검사한다. 활성 파일의 결과 누락·건너뜀과 다른 플랫폼의 명시적 통과·실패 결과도 입력한다. | 제외된 파일도 연결되지만 결과가 없으면 게이트 밖이다. GOFLAGS의 tag를 포함한 Go 빌드 제약을 따른다. 활성 파일의 결과 누락·건너뜀은 실패한다. 명시적 통과·실패 결과는 현재 플랫폼과 무관하게 센다. |

### 7.3 Cluster failpoint 빌드

Membership crash-boundary와 durable-ACK acceptance 시험은 `narumq_failpoint` build tag를 사용한다. 이 build만 `NARUMQ_FAILPOINT`, `NARUMQ_FAILPOINT_CONTROL`을 읽는다. 기본 build는 두 값을 무시하며 이 설정으로 process를 종료하거나 peer를 격리하지 않는다. 일치하는 crash point는 `narumq failpoint reached: <point>`를 출력하고 exit 86으로 종료한다.

허용 crash point: `add-learner-before-persist`, `add-learner-after-persist`, `add-learner-after-apply`, `promote-voter-before-persist`, `promote-voter-after-persist`, `promote-voter-after-apply`, `remove-member-before-persist`, `remove-member-after-persist`, `remove-member-after-apply`, `snapshot-install-before-persist`, `snapshot-install-after-persist`, `begin-remove-after-apply`, `route-projection-rebuild-error`, `ownership-acquire-after-commit`, `offline-detach-before-commit`, `offline-detach-after-commit`, `delivery-wal-before-write`, `delivery-wal-before-sync`, `delivery-wal-after-sync`, `delivery-commit-before-apply`, `delivery-commit-after-apply`.

Tagged runtime은 JSON control file을 동기식으로 읽는다. 예: `{"crash":"delivery-wal-before-sync","isolatePeers":[2],"holdCommands":["subscription"]}`. `isolatePeers`는 inbound/outbound traffic 모두에 적용한다. 파일을 비우거나 제거하면 해제한다. 잘못된 JSON/value는 log를 남기고 해당 read를 비활성화한다. Hold 종류는 `ownership`, `subscription`, `delivery`다. 일치한 command는 read barrier/profile 확인 뒤, encoding/proposal 전에 멈춘다. 해제될 때까지 poll하고 60초 뒤 경고 후 진행한다. Proposal은 `WithoutCancel`에서 만든 10초 context로 실행한다. 결과는 applied/conflict/error다. Test 전용 기능이다.

`dropProposalPeers`는 해당 peer로 가는 forwarded proposal(`MsgProp`) frame만 Raft가 보낸 것으로 처리하고 버린다. Read proof와 다른 Raft message는 막지 않는다. 이미 전송한 proposal이 리더 장애와 함께 사라진 상태를 process 시험에서 결정적으로 만들기 위한 test 전용 기능이다. 파일을 비우거나 제거하면 해제한다.

Raft adapter는 일반 non-empty WAL entry에서 `PersistenceBoundaryHook`과 별도 `WALBoundaryHook`을 호출한다. Callback은 node lock을 보유하며 빌린 byte는 callback 동안만 유효하다. Composition root가 entry를 decode하고 crash 대상은 durable `DeliveryActionPublish`/`DeliveryActionReceive`로 제한한다. `ownership-acquire-after-commit`은 quorum commit 뒤 CONNECT 활성화 전에 실행된다. Offline detach hook은 proposal 직전과 `Applied` 뒤에 있다.

### 7.4 Cluster storage chaos 검증

FUSE chaos는 별도 gate다. Go 1.26.5, Linux/amd64 Docker, `/dev/fuse`, `CAP_SYS_ADMIN`, module network가 필요하다. Unsynced journal loss를 모사하며 물리 power loss나 EBS 증거는 아니다.

```text
go run ./tests/chaos/cluster-storage-v1 \
  --run-id storage-faults-local \
  --output .artifacts/chaos/cluster-storage-v1/storage-faults-local
```

Runner는 새 network와 세 broker process를 만들고 아래 일곱 scenario를 차례로 실행한다: `follower-disk-full`, `quorum-disk-full`, `follower-slow-io`, `follower-sync-error`, `follower-torn-power-loss`, `cluster-power-loss`, `follower-disk-pressure`. Persistent QoS1/2 subscriber와 sentinel로 ACK·복구를 확인한다. Healthy quorum은 follower fault 중에도 진행하고 quorum loss 중에는 ACK하지 않아야 한다. QoS2는 같은 packet으로 재개하고 QoS1은 at-most-once를 유지한다. Sync error는 fail closed여야 한다. Torn WAL rollback과 전체 cluster restart 뒤 READY/index/session/delivery가 수렴해야 한다. Manifest·summary·image metadata·scenario events·logs를 보존하며 secret은 저장하지 않는다. 이 gate는 `go test ./...` 및 `verify local`과 별개다.

### 7.5 Local ext4 파일시스템 게이트

Local ext4 profile `local-ext4-v1`은 Linux/amd64 Docker, privileged container, kernel ext4/loop 지원과 module network가 필요하다. Broker마다 격리된 256 MiB ext4 image를 쓰고, 별도 1 GiB recovery fixture를 사용한다. Host block device나 기존 volume은 넘기지 않는다.

```text
go run ./tests/chaos/cluster-storage-v1 \
  --profile local-ext4-v1 \
  --run-id ext4-local \
  --output .artifacts/chaos/cluster-storage-v1/ext4-local
```

네 scenario는 follower/quorum ENOSPC, 전체 broker SIGKILL/restart, filesystem recovery boundary를 다룬다. 마지막 scenario는 `TMPDIR`을 ext4에 두고 WAL/snapshot/backup/voter-volume acceptance를 실행한다. Nested skip/failure와 cleanup failure는 전체 실패다. Manifest schema 2에는 profile, evidence boundary, kernel/mount/loop/image/broker hash와 raw event가 있다. 실행은 disposable environment로 제한한다. 증거 범위는 local host/kernel의 process crash다. 자연 torn write, 물리 power loss, 독립 host/AZ/EBS, DR RPO/RTO와 production 지원을 입증하지 않는다. `verify local`과 별도로 실행한다.

## 8. Coverage와 test quality gate

- 전체 production code 목표: line ≥85%, function ≥90%, branch 정책 ≥80%.
- 핵심 domain 9개(`mqtt`, `session`, `routing`, `delivery`, `tenancy`, `security`, `nodedrain`, `release`, `connrebalance`)는 line/function ≥95%, branch ≥90%다. `application/membership`은 핵심 domain이 아니며 전체 기준을 따른다. 정책은 `tests/verification/coverage-policy.json`이 source of truth다.
- 개발자가 실행하는 `go run ./tools/verify mutation`은 `delivery`, `mqtt`, `nodedrain`, `security`, `session`의 mutation efficacy와 mutant coverage를 각각 ≥80%로 확인한다. 이 명령은 `local`·`ci`에 포함되지 않는다. 동시성 코드는 race detector 또는 동등한 검증을 CI에서 실행하고 해당 기준을 통과해야 한다.
- Domain 규칙/state machine은 정상·오류·경계·전이 assertion coverage 100%를 기본으로 한다. Consensus·drain·infrastructure 검증은 integration/E2E/chaos에 연결한다.
- 기본 gate(`local`·`ci`)는 논리 시험만 실행한다. 지연·처리량·percentile·지연 비율 한도나 부하 강도로 판정하는 부하·품질 시험은 `Acceptance: NARUMQ_LOAD_QUALITY` opt-in 수용 시험으로 gate 밖에서 실행하고 결과를 검증 기록에 남긴다([ADR-0064](../decisions/ADR-0064-ci-gate-logic-only.md), [§7.2](#72-fuzz-게이트와-수동-mutation-검증)).

`verify local`과 `verify ci`는 gofmt, vet, traceability, scenarios, architecture, package/integration coverage, production race, fuzz 5종과 coverage policy를 실행한다. 두 명령은 mutation을 실행하지 않는다. Local coverage는 `-parallel=8`, CI coverage는 Go의 유효 `GOMAXPROCS`만큼 `-parallel`을 허용한다. `-parallel`은 package 안에서 `t.Parallel()`로 실행하는 test의 상한이다. 다섯 package mutation은 개발자가 `go run ./tools/verify mutation`으로 별도 실행한다. CI의 `--base <merge-base>`는 coverage diff 계산에만 쓴다. Atomic profile은 `.artifacts/verification/coverage.out`, mutation 결과는 `.artifacts/verification/mutation-<package>.json`이다. Integration은 build tag별 일반·failpoint broker를 임시 경로에 한 번씩 build하고 child process 정리 뒤 제거한다. Race 범위는 `./cmd/... ./internal/...`이며 별도 build되는 integration child broker는 race-instrumented가 아니다. `scenarios`는 시나리오 표와 테스트 메타 정보의 `오류`·`미연결`·`실패`를 gate 실패로 처리한다([§7.2](#72-fuzz-게이트와-수동-mutation-검증)).

Coverage는 Go atomic coverprofile의 executable block을 집계한다. [ADR-0040](../decisions/ADR-0040-branch-coverage-arm-injection.md)에 따라 branch는 `if`의 then·else와 clause가 있는 switch·type switch·select의 각 case·default arm을 센다. Loop 조건, 단락 평가와 `func _` body는 제외한다. 빠진 else·switch default만 줄바꿈 없이 사본에 주입한다. Select default는 주입하지 않는다. 사본에서 전체 시험을 실행하고 manifest 좌표와 원본 SHA-256을 대조한다. Line coverage로 branch를 대체하지 않는다. 실행 가능한 변경 line이나 변경 결정점이 없으면 해당 diff coverage는 100%다.

현재 gate는 local coverage의 package별 `-parallel=8`, CI는 유효 `GOMAXPROCS`를 `-parallel` 상한으로 쓴다. Mutation은 자동 gate가 아니며 별도 수동 command로 다섯 package를 검사한다. 점수 threshold는 package마다 그대로 적용한다. 위 9개 core domain을 쓴다. Final ratchet은 overall production과 core별 threshold를 `coverage-policy.json`에 둔다. Branch는 두 번 측정한 값 중 작은 값을 소수점 둘째 자리에서 내림한 baseline ratchet을 사용한다. 전체·core ratchet은 측정값이 ratchet에서 0.20%p를 뺀 값보다 낮을 때 실패하며, 비교에는 0.005%p 반올림 오차를 추가로 허용한다. 허용폭은 branch ratchet에만 적용하며 line·function gate, diff 하한과 PRD shortfall에는 적용하지 않는다. 허용폭은 [ADR-0041](../decisions/ADR-0041-branch-ratchet-noise-tolerance.md), 현재 diff 하한은 [ADR-0063](../decisions/ADR-0063-diff-coverage-floor-80.md)을 따른다. PRD 전체 80%·core 90% 미달은 report의 `branchCoverage.requirementShortfalls`로 보고한다. 이를 ratchet gate 실패 사유로 쓰지 않으며, 미달이 남는 동안 TEST-06·TEST-07은 미검증으로 유지한다. Ratchet 변경에는 반복 안정성 증거와 정책/요구사항 갱신이 필요하다. Coverage 숫자만으로는 충분하지 않으며 모든 P0 requirement와 지원 MQTT normative statement에 assertion이 연결되어야 한다.

### 8.1 Verification 실행 정책

Repository root에서 전체 local·CI gate와 수동 mutation 검증을 실행한다.

```text
go run ./tools/verify local
go run ./tools/verify ci --base <merge-base>
go run ./tools/verify acceptance [--run <regexp>]
go run ./tools/verify mutation
```

`local`·`ci`는 저장소 검증 순서를 소유한다. `acceptance`는 `Acceptance:` 표시가 있는 opt-in 수용 시험만 실행하는 별도 명령이다([§7.2](#72-fuzz-게이트와-수동-mutation-검증)). `mutation`은 별도 실행 명령이며 coverage/race/fuzz gate를 다시 실행하지 않는다. 단일 package 분석에는 Gremlins 직접 명령을 사용할 수 있다.

반복 branch baseline은 `go run ./tools/verify baseline [--results FILE]`로 측정할 수 있다. 이 명령은 §8.2의 세 번 실행과 baseline 계산을 수행하며 policy를 자동 변경하지 않는다. 검증 도구 패키지만 실제 실행하려면 `go run ./tools/verify package-tests [--results FILE]`를 사용한다. 두 명령의 수동 결과 파일은 [테스트 컨벤션 §4](../conventions/test-conventions.md#4-예외와-자동-생성-산출물)를 따른다.

### 8.2 Branch 측정 gate 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#82-branch-측정-gate-수용-증거)에 있다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-077 | TEST-06, TEST-07, TEST-11 | 실제 Go coverage block이 있는 fixture를 branch 계측하고 coverage를 실행한다. | Branch outcome 17개 중 6개가 실행되고 의도한 미실행 11개와 정확히 일치한다. 원본과 사본의 line·function total이 같다. |
| PRF-SCN-078 | TEST-11 | 빠진 else·switch default만 주입하는 계측을 실행한다. Manifest에 잘못된 schema와 JSON을 넣는다. | 줄 수가 보존된다. 이미 있는 arm은 중복되지 않고 select 의미가 보존되며 제외 결정점은 계측되지 않는다. 잘못된 schema와 JSON manifest는 거절한다. |
| PRF-SCN-079 | TEST-11 | 계측 출력 directory로 repository root, 상위 directory, source, 검증 증거 상위 directory, root를 가리키는 symlink·Windows junction, 저장소 내부에서 외부 directory로 향하는 link를 지정한다. 기본 branch directory와 외부 directory도 지정한다. | 위험한 출력은 파일을 삭제하기 전에 거절한다. 기본 branch directory와 외부 출력은 허용한다. `branch-instrument --output .`와 `--output .artifacts/verification`은 exit 1이다. |
| PRF-SCN-080 | TEST-06, TEST-07 | SHA가 다른 source, 누락·초과 파일, 대응 block이 없는 manifest로 branch coverage를 평가한다. | 불일치를 모두 거절한다. |
| PRF-SCN-081 | TEST-08, TEST-09 | Ratchet 하락, diff branch 하한 미달, PRD shortfall만 있는 입력을 각각 평가한다. | Ratchet 하락과 diff branch 하한 미달은 실패한다. PRD shortfall만 있으면 baseline gate는 통과하고 `requirementShortfalls`로 보고한다. |
| PRF-SCN-082 | TEST-06, TEST-07, TEST-09 | 저장소 root에서 `go run ./tools/verify local`을 세 번 실행해 branch baseline을 정한다. | 세 실행 모두 `local: PASS`다. Summary는 schema 2이고 branch가 `measured`이며 전체 outcome과 9개 core metric을 보고한다. Baseline은 두 측정값 중 작은 값을 소수점 둘째 자리에서 내림한 값이다. |
| PRF-SCN-083 | TEST-11 | 계측 전 원본의 coverage summary와 계측 사본의 coverage summary를 비교한다. | Line total과 function total이 정확히 일치한다. |
| PRF-SCN-084 | TEST-09 | 저장소 밖 임시 policy에서 routing branch ratchet만 측정값보다 높게 올려 `coverage`를 실행한다. | exit 1이며 `narumq/internal/domain/routing branch ratchet <ratchet>% is above measured <측정값>%`를 출력한다. |
| PRF-SCN-085 | TEST-08 | 하한에 미달하는 과거 변경 범위를 `--base`로 지정해 `go run ./tools/verify coverage --profile .artifacts/verification/coverage.out --report <path> --base <base>`를 실행한다. | exit 1이며 diff branch와 line의 하한 미달을 각각 보고한다. |
| PRF-SCN-086 | TEST-14 | `Node.Close`에서 보관한 append와 선행 storage 오류 경로(`consumed-prior-error`)를 실패 flag가 hold 중인 sync 뒤에서 주입한다. 선행 오류를 소비한 직후 보관한 append와 미처리 queue가 모두 남았는지 확인한다. | `Close`가 pending append 오류 분기를 거쳐 `ErrStorage`와 `os.ErrPermission`을 반환한다. race detector 아래 CPU 설정 1·2·4로 반복해도 모두 통과한다. |

### 8.3 Branch ratchet 노이즈 허용폭 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#83-branch-ratchet-노이즈-허용폭-수용-증거)에 있다.

허용폭 규칙은 [§8 본문](#8-coverage와-test-quality-gate)을 따른다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-087 | TEST-09 | Overall branch ratchet이 측정값보다 높은 policy 사본으로 `coverage`를 실행한다. 차이는 +0.21%p와 +0.19%p다. 허용폭 경계 값으로 `tools/verify` 단위 시험도 실행한다. | +0.21%p는 exit 1이며 `overall branch ratchet <ratchet>% (tolerance 0.20%p) is above measured <측정값>%`를 출력한다. +0.19%p는 exit 0과 `coverage: PASS`다. 허용폭은 branch ratchet에만 적용하고 line·function gate, diff 하한과 PRD shortfall에는 적용하지 않는다. |
| PRF-SCN-088 | TEST-06, TEST-07, TEST-14 | Cluster, Raft, TCP와 delivery queue의 branch outcome(`if`의 then·else, switch case)을 clock·sleep·스케줄 순서에 의존하지 않는 결정적 시험으로 실행한다. 해당 시험을 race detector 아래 반복한다. | 대상 outcome이 모두 covered이며 `uncoveredOutcomes`에 남지 않는다. 반복한 race 시험이 모두 통과한다. |

### 8.4 전체 branch floor 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#84-전체-branch-floor-수용-증거)에 있다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-089 | TEST-06 | 전체 production branch 기준 80%를 반영한 `tests/verification/coverage-policy.json`으로 `go run ./tools/verify local`을 실행한다. | `coverage: PASS`, `local: PASS`이며 `branchCoverage.requirementShortfalls`가 비어 있다. Core 9개 branch 기준과 ratchet, line·function 기준은 바뀌지 않는다. |

### 8.5 변경 코드 diff floor 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#85-변경-코드-diff-floor-수용-증거)에 있다.

변경 코드의 line·function·branch diff coverage 하한은 각각 80%다([ADR-0063](../decisions/ADR-0063-diff-coverage-floor-80.md)). 하한과 같은 값은 통과하고 미달은 거절한다. Ratchet 허용폭은 diff 하한에 적용하지 않는다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-090 | TEST-08 | 최신 commit의 atomic profile과 branch manifest를 `tests/verification/coverage-policy.json`의 `diffFloor` 80%로 평가한다. `go run ./tools/verify coverage --base <부모 commit>`을 실행한다. | 세 diff 지표가 모두 하한을 넘어 `coverage: PASS`, exit 0이다. Report의 `passed=true`, `branchCoverage.status=measured`이고 `requirementShortfalls`는 비어 있다. |
| PRF-SCN-091 | TEST-08 | Diff branch 측정값과 diff 하한이 같다. | 통과한다. |
| PRF-SCN-092 | TEST-08 | Diff line과 function 측정값을 각각 79.99%, 80%, 80.01%로 바꿔 평가한다. 한 지표만 79.99%이고 다른 지표는 80%다. | 80%와 80.01%는 통과한다. 해당 지표가 79.99%이면 다른 지표가 80%여도 독립적으로 거절한다. |
| PRF-SCN-093 | TEST-08, TEST-09 | Diff branch 하한이 측정값보다 0.01%p 높다. Ratchet 허용폭은 0.20%p다. | 허용폭을 적용하지 않고 diff 하한 미달로 거절한다. |
| PRF-SCN-094 | TEST-08 | 하한에 미달하는 과거 변경 범위를 `--base`로 지정해 같은 coverage CLI를 실행한다. | exit 1이며 `diff branch coverage <측정값>% is below 80.00%`를 출력한다. |
| PRF-SCN-095 | TEST-08, TEST-09, TEST-11 | 저장소 루트에서 `go test ./tools/verify -count=1`을 실행한다. | `tools/verify` package의 coverage·branch 시험이 모두 통과한다. |
| PRF-SCN-096 | TEST-10 | 저장소 루트에서 `go run ./tools/verify traceability`를 실행한다. | exit 0으로 통과한다. P0 연결 수와 예외 수가 허용된 예외 목록과 일치한다. |

### 8.6 간헐 실패 원인 수정 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#86-간헐-실패-원인-수정-수용-증거)에 있다.

시험을 제외하거나 대기 시간을 늘리지 않고 대기 경로를 고친다. 아래 세 시나리오는 그 수정을 확인한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-097 | TEST-14, TEST-15 | 두 Raft peer driver를 먼저 시작하고 node 1의 bootstrap commit이 적용된 뒤 `Campaign()`을 호출한다. Peer 3은 응답하지 않는다. | 기존 5초 선출 제한 안에 node 1 또는 node 2가 리더로 선출된다. Campaign이 bootstrap 적용 전에 무시되어 election timeout에 의존하지 않는다. |
| PRF-SCN-098 | TEST-14, TEST-15 | Release profile controller가 local `MemberProfileChange`를 제안하고, driver는 proposal을 인수한 뒤 결과를 보내지 않는다. | `Commit`에 전달한 context에 controller context보다 이른 5초 상한이 있다. 상한이 지나면 기존 오류 분기로 반환하고 gate는 닫힌 채 유지하며 incompatibility를 latch하지 않는다. Controller는 다음 reconcile 주기에 다시 확인한다. |
| PRF-SCN-099 | TEST-14, TEST-15 | 실제 process cluster에서 joiner node 4를 추가하는 동안 node 2가 `promote-voter-before-persist` failpoint로 종료된다. | Join이 `SUCCEEDED`로 끝나고 node 4를 포함한 READY 검사를 통과한다. Node 4의 readiness에 `release_profile_unsynced`가 남지 않는다. |

### 8.7 Mutation gate 범위 확장 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#87-mutation-gate-범위-확장-수용-증거)에 있다.

Mutation 대상 package와 점수 기준은 [§7.2](#72-fuzz-게이트와-수동-mutation-검증)를 따른다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-100 | TEST-12 | Variable byte integer 폭과 전체 packet 크기의 경계값 8개를 `variableByteIntegerSize`에 입력한다. 같은 시험을 경계 판정을 `case value <= 128:`로 바꾼 mutant 사본에서도 실행한다. | 원본에서는 통과한다. `<= 128` mutant 사본에서는 `variableByteIntegerSize(128) = 1, want 2`로 실패한다. |
| PRF-SCN-101 | TEST-12 | `go run ./tools/verify mutation`이 `delivery`, `mqtt`, `nodedrain`, `security`, `session` 각 package의 Gremlins 결과를 판정한다. | package마다 efficacy와 mutant coverage가 각각 80% 이상이어야 통과한다. `TIMED OUT` mutant는 `KILLED`로 세지 않는다. |

### 8.8 수동 mutation 검증 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#88-수동-mutation-검증-수용-증거)에 있다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-102 | TEST-12 | 저장소 루트에서 `go run ./tools/verify mutation`을 실행한다. | `delivery`, `mqtt`, `nodedrain`, `security`, `session` 다섯 package를 선택하고 각각 Gremlins `workers=1`, `test-cpu=1`로 검사한다. 모든 package가 기준을 넘으면 exit 0과 `mutation: PASS`를 반환한다. |
| PRF-SCN-103 | TEST-12 | `go run ./tools/verify local` 또는 `go run ./tools/verify ci --base <merge-base>`를 실행한다. | 두 명령은 mutation 단계를 실행하지 않는다. `ci`는 `--base`가 없으면 거절하고 base를 coverage diff 계산에만 전달한다. Mutation output은 없다. |

### 8.9 GitHub Actions 원격 CI와 required check 수용 테스트

시나리오 결과와 테스트 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거)에 있다.

`.github/workflows/verify.yml`은 pull request, `main` push와 `workflow_dispatch`에서 `go run ./tools/verify ci --base <base>`를 실행한다. PR은 `origin/$BASE_REF`를, main push는 push 이전 commit을 diff base로 쓴다. Coverage profile, summary와 branch manifest는 30일 보관한다. Mutation은 자동 gate에서 제외하고 `go run ./tools/verify mutation`으로 별도 실행한다. Workflow는 `ubuntu-24.04`와 `runs.using: node24`인 `actions/*@v7`을 쓴다.

보류된 DEP P0 예외가 `tests/conformance/p0-requirement-exceptions-v1.json`에 남아 있는 동안 TEST-10은 [ADR-0042](../decisions/ADR-0042-p0-requirement-traceability-gate.md) §1.6에 따라 미검증으로 관리한다. CI의 traceability 통과는 허용된 예외 목록과 test 연결이 일치한다는 뜻이며, 모든 P0가 test에 연결됐다는 뜻은 아니다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| PRF-SCN-104 | TEST-06, TEST-07, TEST-08, TEST-09, TEST-13, TEST-14, TEST-15 | pull request, `main` push 또는 `workflow_dispatch`로 `.github/workflows/verify.yml`을 실행한다. Runner는 `ubuntu-24.04`다. | `go run ./tools/verify ci --base <base>`가 `ci: PASS`로 끝나고 필수 `verify` check가 `success`다. Fuzz와 race 단계를 통과한다. Coverage profile, summary와 branch manifest artifact를 30일 보관한다. |
| PRF-SCN-105 | TEST-10 | `main` 보호 설정을 GitHub Branch Protection API로 조회한다. | 필수 status check는 `verify`다. Strict status check는 `true`이며 최신 `main` 반영이 필요하다. Required review는 없다. Admin enforcement는 `true`이며 관리자에게도 동일하게 보호가 적용된다([ADR-0056](../decisions/ADR-0056-branch-protection-for-admins.md)). Force push와 branch deletion은 둘 다 금지한다. Push restriction은 없다. |
| PRF-SCN-106 | TEST-10 | 별도 probe PR에 `tools/verify/branch_protection_probe_test.go`로 의도적 실패 test를 주입한다. | CI의 `verify` check가 `--- FAIL: TestBranchProtectionProbe`와 `TEST-10 probe: intentional test failure`를 출력하며 실패한다. PR `mergeable_state`는 `blocked`이고 관리자 계정의 merge API 호출은 HTTP 405(`Required status check "verify" is failing.`)로 거절된다. |
| PRF-SCN-107 | TEST-10 | 별도 probe PR에 `internal/domain/mqtt/branch_protection_probe.go`로 test 없는 함수를 주입한다. | CI의 `verify` check가 `coverage gate failed:`와 함께 function ratchet 미달, diff line coverage 0.00% 미달, diff function coverage 0.00% 미달을 출력하며 실패한다. PR `mergeable_state`는 `blocked`이고 관리자 계정의 merge API 호출은 HTTP 405로 거절된다. |
| PRF-SCN-108 | TEST-10 | 별도 probe PR에서 `docs/requirements/PRD.md`에 test도 예외도 없는 `API-99` P0 행을 추가한다. | CI의 `verify` check가 `traceability: P0 requirement API-99 has no Traceability test and no exception in tests/conformance/p0-requirement-exceptions-v1.json`을 출력하며 실패한다. PR `mergeable_state`는 `blocked`이고 관리자 계정의 merge API 호출은 HTTP 405로 거절된다. |
| PRF-SCN-109 | TEST-10 | 별도 probe PR에서 `tests/conformance/mqtt-compliance-v1.json`에 test 배열이 빈 `MQTT-4.9.0-1` statement를 추가한다. | CI의 `verify` check가 `traceability: invalid compliance matrix tests/conformance/mqtt-compliance-v1.json: statement[61].tests must not be empty`를 출력하며 실패한다. PR `mergeable_state`는 `blocked`이고 관리자 계정의 merge API 호출은 HTTP 405로 거절된다. |

## 9. Architecture 검증

CI 아키텍처 검증은 ADR-0001의 클린 아키텍처 경계를 강제한다:

- Domain은 스토리지, Kubernetes, HTTP, UI, 메트릭, application, adapter 패키지를 일절 import하지 않는다.
- Application은 domain과 자신이 소유한 port 인터페이스에만 의존한다.
- Adapter의 프레임워크·RPC·스토리지 외부 타입은 application DTO나 domain 경계를 넘지 않는다.
- 구체 어댑터 조립은 Composition Root(`cmd/narumq`)에서만 수행한다.
- 세션, 라우팅, 전달, 멀티테넌시, 보안, 클러스터 생명주기 간 금지된 의존성은 명시적으로 차단한다.
- N-1과 N 버전 간 와이어 프로토콜, 영속화 상태, 스냅샷 호환성은 릴리스 매트릭스로 관리한다.

`go run ./tools/verify architecture` 명령은 `go list -json ./cmd/... ./internal/...`을 통해 프로덕션 패키지의 import 그래프를 검사한다. 프로덕션 코드가 테스트·도구 패키지를 참조하거나, 어댑터 간 직접 의존 또는 Composition Root 외부에서 구체 어댑터를 직접 조립하면 검증이 실패한다.

## 10. 릴리스 증거와 P0 게이트

다음 모든 항목의 검증 증거가 확보되어야 P0 운영 트래픽 진입을 승인한다:

1. 각 P0 요구사항이 단위 테스트와 자동화된 인수 시험에 연결되어야 한다.
2. 지원하는 모든 MQTT 표준 규격 문장이 적합성 매트릭스에 완전히 매핑되어야 한다.
3. 커버리지·Race 검사, 5개 Fuzz 캠페인, delivery·mqtt·nodedrain·security·session 5개 패키지의 mutation 검사(각 지표 ≥80%)를 모두 통과해야 한다.
4. 기능이 완료된 단계마다 BP-01~BP-07 벤치마크를 실행하고 원본 artifact를 보존해야 한다.
5. 승인된 운영 profile 부하에서 3노드 정상 상태 및 N-1 상태의 SLO를 충족해야 한다.
6. 단일 노드 강제 종료(kill) 시 durable ACK 완료 메시지의 유실(gap)이 0건이어야 한다.
7. `EKS_EC2_HA` 검증으로 EC2·AZ·EBS가 분리된 3개 Pod의 단일 AZ 장애 및 NLB 등록 해제를 검증해야 한다.
8. 100회 연속 롤링 업데이트에서 영속 세션 소실 0건, 지원 클라이언트의 비정상 Will 발행 0건이어야 한다.
9. 테넌트 간 격리와 관리 API의 RBAC 침투 테스트를 통과해야 한다.
10. 운영 유사 환경에서 백업·복구, 롤백, 메트릭·경보·대시보드 동작을 확인해야 한다.

릴리스 필수 소스 artifact는 `.artifacts/verification/coverage.out`, `coverage-summary.json`, mutation 결과 JSON 5개, 보존된 fuzz 결함 코퍼스다. 누락된 artifact나 미검증 게이트가 있으면 완료로 간주하지 않는다. Smoke 결과의 `releaseGating=false`와 미채택 `SPEC-TBD-08` 기준·미계산 profile cutoff는 계속 열어 둔다.
