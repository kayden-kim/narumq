# Performance와 verification 스펙

이 문서는 NaruMQ의 성능·신뢰성 요구, benchmark 판정 경계와 검증 증거를 정의한다. 수치가 확정되지 않은 release gate는 닫지 않는다.

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

Profile SHA-256은 저장소에 커밋된 LF bytes로 계산한다. `.gitattributes`의 `tests/benchmark/*/testdata/*.json text eol=lf` 규칙으로 `core.autocrlf=true`인 Windows checkout에서도 같은 bytes를 쓴다. 2026-10-08 이전 BP-06 실행 artifact의 `profileSha256` `e8e4aa2e5df1a3e55ad901acff5a4505cdb1acf4699ff061f1a2bfc51754054d`는 같은 내용을 CRLF로 checkout한 bytes의 digest다. 같은 profile의 LF digest는 `ca5e28b04623e708d9521ab34ce5ed3b9dd617d4ac57c4571243bdd6061f6a15`다. 과거 artifact의 값은 실행 당시 사실이므로 고치지 않는다.

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

시험은 `internal/adapters/metrics/registry_test.go::TestRaftStageHistogramsKeepStagesIsolatedAndBoundOverflow`, `internal/adapters/raft/node_dynamic_test.go::TestDynamicFollowerForwardsApplicationProposal`, `cmd/narumq/cluster_runtime_test.go::TestClusterCommitReadProofPreservesSubmissionOrderAndCancellation`, `cmd/narumq/cluster_read_admission_test.go::TestClusterFollowerReadBurstCompletesAfterEgressDrain`, `TestClusterLeaderReadBurstPreservesHealthyQuorumWithBlockedMinority`, `TestClusterUnissuedReadReturnsCancellationAndDriverFailure`, `internal/adapters/tcp.TestDynamicOutboundCapacityRemainsChargedUntilWireCompletion`, `tests/integration.TestDynamicClusterConcurrentFreshReadProcessAcceptance`에 연결한다.

#### Readiness 진단 계약

Readiness는 storage 검사, fresh quorum proof, local snapshot 판정을 기존 순서로 수행하며 기존 fail-closed 반환 오류를 유지한다. 진단은 결과를 기다리거나 재판정하지 않는다.

- `purpose`: `mqtt`, `health`, `summary`, `backup`, `drain`만 사용한다. Management summary의 quorum 조회는 MQTT admission으로 분류하지 않는다.
- `readiness_stage`: `readiness_storage`, `readiness_fresh_read`, `readiness_local_state`만 사용한다. 단계별로 실제로 평가한 값만 기록하고 미실행 판정값을 채우지 않는다.
- Fresh proof 거절은 오류 분류를 기록한다. Local-state 거절은 첫 불충족 조건, 모든 판정 flag, proof/applied/projection index를 기록한다. 동시 실패 조건은 따로 유추하지 않는다.
- Local snapshot 앞뒤 `ready_sequence_before`·`ready_sequence_after`를 기록한다. 홀수는 Ready WAL/apply/projection 구간이며, 값이 달라지면 snapshot 수집 중 해당 구간이 변했다는 뜻이다. 이 사실만으로 기다리거나 재판정하지 않는다.
- Storage 값은 원래 검사 시점의 값이다. 이후 값으로 다시 sample하지 않는다. 동일 purpose·stage·거절 분류는 최초 건과 1초 간격 표본만 출력하고 건수를 누적한다. 표본 log는 개별 CONNECT 기록이 아니다.
- Client/tenant ID, payload, topic, token을 기록하지 않는다. Retry, checkpoint 대기, proof 재사용, ACK 경계 변경을 하지 않는다.

Readiness 판정·diagnostic field는 `cmd/narumq`의 readiness 거절 시험에 연결한다. 수집 경계와 log rate limit은 이 절의 계약이며 stage histogram 시험과 구분한다.

Target은 `phases.jsonl`, `messages.jsonl`, `workload-summary.json`, `resource-summary.json`, `wal-summary.json`, runner/broker log, redacted inspect와 broker/network-owner stats를 보존한다. NaruMQ의 raw metric은 `metrics/` 아래에 둔다. `diagnostics.jsonl`은 node·요청 시각·raw 경로 또는 scrape 오류를 잇는다. Failure에도 최종 scrape를 시도한다. Resource window에서 source/node 표본이 2개 미만이면 invalid다. Block write가 0인 표본은 유효하다. Network owner delta가 모두 0이면 `networkStatsAvailable=false`로 기록하고 workload를 invalid로 하지 않으며, suite에서 해당 network metric만 생략한다. Stats는 host clock, phase는 runner clock이므로 `clockSource`에 둘 다 기록한다.

Suite metric key는 `stepNN_qQ_ack_latency_p99_ns`, `stepNN_qQ_ack_latency_p999_ns`, `stepNN_qQ_delivery_latency_p99_ns`, `stepNN_qQ_ack_rate_per_second`와 `stepNN_block_write_bytes_per_acked_message`다. Network 표본이 있으면 `stepNN_network_tx_bytes_per_acked_message`를 추가한다. NaruMQ 전용 key는 `stepNN_wal_syncs_per_second`, `stepNN_wal_mean_entries_per_sync`, `stepNN_wal_bytes_per_acked_message`다. `offline_reconnect_drain_millis_max`는 양 target metric이다. EMQX의 NaruMQ WAL key는 `not_applicable`이며 metric으로 0을 만들지 않는다.

| 검증 계약 | 영구 시험 |
| --- | --- |
| 프로파일 strict decode·범위·소스 해시 | `profile_test.go::TestDecodeProfileStrictAndComputesMessageBound`, `TestDecodeProfileRejectsMissingUnknownTrailingAndOverflow`, `TestProfileRejectsEveryOutOfRangeValue`, `TestLoadProfileReturnsExactBytesAndSHA256` |
| 스트림 노드 배치·발행자 reader·ACK 타임아웃 | `runner_test.go::TestMakeStreamsAssignsCrossNodeRoles`, `TestWaitSlotReturnsReaderFailure`, `TestPublisherACKTimeoutQuarantinesPacketIdentifier` |
| 세션의 QoS 2 PUBREL 재개 | `subscriber_resume_test.go::TestSubscriberReaderResumesQoS2Release` |
| Gap, 중복, 스케줄 지연, 드레인 스냅샷, QoS 2 PUBCOMP·오프라인 경계 | `aggregate_test.go::TestAckedUnreceivedMessageIsGapAndInvalid`, `TestQoS2DuplicateInvalid`, `TestQoS1DuplicateReportedButAllowed`, `TestScheduleLagEqualToIntervalIsOverload`, `TestMessageSnapshotDoesNotObserveLaterAcknowledgements`, `TestQoS2PUBCOMPTimeoutCountsIncompleteExchange`, `TestOfflineCompletionWaitsForQoS2PUBCOMP`, `TestOfflineCompletionRequiresEveryExpectedReceipt`, `TestOfflineCompletionFailsWhenReceiptMissesDeadline` |
| WAL 히스토그램 델타, 비정상 시리즈, EMQX 적용성 | `metrics_test.go::TestWriteWALSummaryUsesBucketDeltasAndOverflow`, `TestWriteWALSummaryMissingFamilyPersistsInvalidSummary`, `TestWriteWALSummaryEMQXNotApplicable` |
| 리소스 표본, 0 바이트 블록 쓰기, 네트워크 적용성 | `resource_test.go::TestBuildResourceSummaryInsufficientSamplesInvalidates`, `TestBuildResourceSummaryZeroBlockWritesValid`, `TestBuildResourceSummaryNetworkUnavailableDoesNotInvalidate` |
| 비교 검증, 지표 생략 및 실패 suite 부모 무효화 | `compare_test.go::TestBuildBP03ComparisonEquivalenceAndNonEquivalentDimensions`, `TestSummarizeBP03RunDoesNotRequireEMQXWALApplicability`, `suite_test.go::TestBP03MetricsEmitRequiredKeysAndUnits`, `TestBuildBP03SuiteSummaryOmitsNetworkMetricsWhenAnyRunLacksNetworkStats`, `TestBuildBP03SuiteSummaryInvalidatesAllMetricsOnOneFailedRun` |

저장소 root에서 실행한다.

```text
go test ./tests/benchmark/bp03 -count=1
go run ./tests/benchmark/bp03 compare --profile tests/benchmark/bp03/testdata/smoke.json
go run ./tests/benchmark/bp03 suite --profile tests/benchmark/bp03/testdata/smoke.json
```

#### 최종 실행 증거와 열린 조건

최종 async WAL code에서 compare `bp03-20261002T193345.142241000Z-38956`와 suite `bp03-suite-20261002T193711.351279300Z-4364`는 valid이며 suite는 5/5다. 5회 모두 QoS별 Step 3 offered/sent/ACK/delivery 600, offline QoS별 64, SessionPresent 4/4, gap·duplicate 0이었다. Max schedule lag는 1.177–1.641 ms, overload는 없었다. Step 3 QoS 1 ACK p99 median/MAD 54.895/1.657 ms (range 53.238–57.203 ms), QoS 2 56.611/3.149 ms (53.387–62.605 ms), mean entries/sync 2.053607/0.003409 (2.036770–2.071090)였다. Suite는 고정 local smoke의 반복 증거이며 production SLO, direct target superiority, ADR-0003 전체 또는 production acceptance가 아니다. `releaseGating=false`, `ackLatencyDirectlyComparable=false`를 유지한다.


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

최종 compare `bp07-20261005T031404.679399800Z-37844`에서 양쪽 workload·rolling·resource가 valid였고 QoS1/2 gap·duplicate, 계획 밖 종료, protocol error, overload는 0이었다. Suite `bp07-suite-20261005T032136.047989300Z-30100`는 교차 순서 5/5 valid였다. NaruMQ QoS1 duplicate는 첫 4회 각 1, 마지막 회 0이며 QoS0 loss 1–2건은 기록 전용이었다. EMQX QoS0 loss·QoS1 duplicate는 0이었다. NaruMQ v5-safe Will은 0, MQTT 3.1.1 server-close와 고유 Will은 매 회 1이었다. 12개 metric key 모두 5 sample을 가졌다. Round metric은 `roundNN_drain_millis`, `roundNN_restart_ready_millis`, `roundNN_reconnect_latency_max_millis`; aggregate는 `reconnects_total`, `qos1_duplicates_total`, `qos0_lost_total`이다. Drain contract가 달라 target 우열로 해석하지 않는다.

`SPEC-TBD-08`의 comparator 수치 제안은 미채택이다. Profile별 수치 기준, Linux/EBS power loss, 실제 NLB·rollout controller와 최대 traffic 연속 100회 rolling은 미완료다. Manifest의 baseline과 `dirty=true`는 실행 당시 상태이며 확인 commit이나 CI rerun을 뜻하지 않는다.

#### 명령과 시험

```text
go run ./tests/benchmark/bp07 compare --profile tests/benchmark/bp07/testdata/smoke.json
go run ./tests/benchmark/bp07 suite --profile tests/benchmark/bp07/testdata/smoke.json
```

Profile·dialer·session·QoS·marker round attribution·Will validity·drain state·EMQX CLI·resource·suite 검증은 실제 파일 `tests/benchmark/bp07/{profile,dialer,session,aggregate,rolling,resource,suite}_test.go`에 있다. MQTT packet 경계는 `tests/benchmark/internal/mqttclient/client_test.go`, published owner port·restart·stats는 `tests/benchmark/internal/environment/cluster_test.go`에서 확인한다. Broker-side proof·projection/FIFO/cancellation acceptance는 `cmd/narumq`, `internal/adapters/{raft,tcp}`, `tests/integration`의 시험에 연결된다. Local gate PASS는 invalid workload나 미완료 production gate를 대체하지 않는다.

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

경계는 pinned EMQX 5.8.8 동작에 따른다. `emqx_persistent_message:store_message/1`은 `emqx_ds:store_batch`를 `sync => false`로 부르고 durable session state는 5,000ms heartbeat에 저장한다. 관찰된 ACKed gap은 주입 후 270ms 안에 있었다. 이후 실패를 이유로 예외를 넓히지 않는다.

#### Artifact·시험·최종 증거

Target은 phase/health/connection/message/workload/failure/resource record, round control marker, log, inspect, EMQX durability snapshot을 보존한다. `workload-summary.json` schemaVersion 2는 `exemptGaps`, `exemptQoS2Duplicates`, `comparatorDurabilityExceptions`를 포함하며 `messages.jsonl`은 `lastReceivedAt`을 담는다. Suite metric은 round RTO/rejoin-ready/resync/reconnect-max, CPU·memory peak, aggregate reconnect 수, QoS1 duplicate, QoS0 lost다. Child 하나가 invalid면 parent metric을 비운다.

Metric key는 round별 `roundNN_rto_millis`, `roundNN_rejoin_ready_millis`, `roundNN_resync_millis`, `roundNN_reconnect_latency_max_millis`, `roundNN_recovery_cpu_peak_percent`, `roundNN_recovery_memory_peak_bytes`; aggregate는 `reconnects_total`, `qos1_duplicates_total`, `qos0_lost_total`이다.

| 검증 계약 | 영구 시험 |
| Kill/inspect/start와 컨테이너 소유권 유지 | `tests/benchmark/internal/environment/cluster_test.go::TestKillAndStartClusterBrokerKeepContainer` |
| 엄격 프로파일·경계치·타임아웃/만료/용량 | `tests/benchmark/bp06/profile_test.go` |
| 픽스처 인증정보·ACL·클라이언트 허용 목록 | `tests/benchmark/bp06/fixture_test.go` |
| 마커·재연결·연결 끊김 귀속 | `tests/benchmark/bp06/session_test.go`, `aggregate_test.go` |
| RTO·복구·재동기화 워터마크/기한 | `tests/benchmark/bp06/aggregate_test.go`, `failure_test.go` |
| EMQX crash-window 경계·대상 범위 | `aggregate_test.go::TestCollectorExemptsOnlyEMQXCrashWindowGaps`, `TestCollectorExemptsOnlyEMQXCheckpointRedelivery` |
| 리소스 구간·재설정·표본 임계치 | `tests/benchmark/bp06/resource_test.go` |
| 지표/단위·하위 실패 시 빈 지표·중앙값/MAD | `tests/benchmark/bp06/suite_test.go` |

```text
go run ./tests/benchmark/bp06 compare --profile tests/benchmark/bp06/testdata/smoke.json
go run ./tests/benchmark/bp06 suite --profile tests/benchmark/bp06/testdata/smoke.json
```

최종 suite `bp06-suite-20261006T163812.777212000Z-92296`는 5/5 valid다. NaruMQ는 매 반복 gap, QoS2 duplicate, 계획 밖 disconnect, protocol error, overload가 0이었다. QoS1 duplicate는 `0,0,0,1,0`이며 기록 전용이다. EMQX crashDurability의 gap/exempt는 `1/1, 5/5, 5/5, 5/5, 5/5`(총 21), QoS2 duplicate/exempt는 `40/40, 50/50, 60/60, 41/41, 60/60`(총 251)이었다. 이는 EMQX crash durability 동등성이나 guaranteed delivery를 뜻하지 않는다. 지정 window 밖 gap/duplicate는 모두 EMQX를 invalid로 만든다. `releaseGating=false`다. `SPEC-TBD-08`, production storage/release matrix와 CI-only check는 미완료다. `dirty=true`와 baseline은 실행 당시의 역사적 metadata이며 post-commit/CI 재실행 증거가 아니다.

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

| 영구 시험 범위 | 실제 시험 파일 |
| --- | --- |
| 프로파일 해시·엄격 경계·ID·토픽 오라클 | `tests/benchmark/bp04/profile_plan_test.go` |
| Shared·overlap 수신 집계 | `tests/benchmark/bp04/collector_test.go` |
| JWT·ACL·인증서 정리 | `tests/benchmark/bp04/fixture_test.go` |
| 중앙값·네트워크·표본 한도 | `tests/benchmark/bp04/resource_test.go` |
| 동등성·지표 선택·실패 suite 빈 지표 처리 | `tests/benchmark/bp04/compare_suite_test.go` |

```text
go run ./tests/benchmark/bp04 compare --profile tests/benchmark/bp04/testdata/smoke.json
go run ./tests/benchmark/bp04 suite --profile tests/benchmark/bp04/testdata/smoke.json
```

최종 compare `bp04-20261007T110131.450776700Z-190180`에서 두 target 모두 valid, setup 115, probe 12/12 성공, protocol/transport/unexpected delivery 0, resource/network valid, `equivalent=true`였다. Suite `bp04-suite-20261007T110511.057084500Z-196860`는 5/5 valid이며 모든 metric에 5 sample이 있었다. Production capacity나 release 합격 증거는 아니다.


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
| `tlsListener` | NaruMQ fixture는 peer mTLS와 분리한 node별 MQTT server certificate/key를 `mqtt.tls`에 지정하고 TLS 1.2 이상, client-auth `none`을 설정한다. EMQX는 3-SAN server certificate를 공유한다. Cipher와 resumption은 구현별이다. BP02 fixture 시험은 통과했지만 compare와 suite는 인증서 분리 변경 뒤 다시 실행하지 않았다. |
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

#### Artifact·metric·시험·최종 증거

Target artifact는 `workload-summary.json`, `resource-summary.json`, `phases.jsonl`, `messages.jsonl`, `retained.jsonl`, `connections.jsonl`이다. Workload summary는 step/stream별 sent, expected/delivered, missing, duplicates, unexpected, QoS mismatch, ACK timeout, delivery/ACK latency, warmup counter를 담는다. Shared는 `memberDeliveries`, offline·retained는 각자 별도 summary를 가진다. Connection record에는 tenant, `tls|tcp`, setup/offline-reconnect/retained-replay phase, connect latency, `SessionPresent`가 있다. Resource는 idle-baseline, subscribed-idle, measure window의 Docker live stats를 쓰며 signed memory delta와 setup client 49개를 기록한다. Suite는 5회 반복한다. Child 하나라도 invalid면 parent metric 전체를 비운다. Network stats가 없으면 network metric만 생략한다.

Suite metric은 step별 delivered/s·cluster median CPU, stream별 delivery p99/p999, QoS1/2 publish ACK p99, 선택적 network Tx bytes/delivery, 존재하는 transport별 setup TLS/TCP connect p99, offline reconnect CONNACK p99, 최대 offline drain·retained replay, setup client당 memory다.

Key는 `stepNN_delivered_per_second`, `stepNN_cluster_median_cpu_percent`, stream별 `stepNN_<stream>_delivery_latency_p99_ns`·`p999_ns`다. QoS1/2 stream은 `stepNN_<stream>_publish_ack_latency_p99_ns`도 포함한다. Network가 있으면 `stepNN_network_tx_bytes_per_delivery`를 포함한다. Transport가 있을 때만 setup TLS/TCP connect p99를 만든다. 공통 key는 `offline_reconnect_connack_latency_p99_ns`, `offline_drain_duration_max_ns`, `retained_replay_duration_max_ns`, `memory_bytes_per_setup_client`다. Latency 단위는 ns, throughput은 deliveries/s, CPU는 percent, network·memory는 bytes다.

| 검증 계약 | 영구 시험 |
| --- | --- |
| 프로파일 소스 해시·엄격 디코딩·경계치 | `profile_test.go::TestLoadProfilePreservesSourceHash`, `TestDecodeProfileRejectsUnknownFieldAndInvalidBounds` |
| 테넌트/노드/전송 계획·오라클·페이로드 사이클 | `profile_plan_test.go::TestClientPlanAssignsTenantNodeAndTransport`, `TestExpectedRecipientsRespectTenantAndShareGroup`, `TestPayloadSizeCycleAndValidation` |
| TLS 서버 인증서 검증 | `tests/benchmark/internal/mqttclient/client_test.go::TestConnectOverTLSVerifiesServerCertificate` |
| 테넌트/TLS 픽스처와 정리 | `fixture_test.go::TestBP02FixtureSeparatesTenantsAndConfiguresTLS` |
| Shared 중복·QoS·오프라인·retained 전달 | `collector_test.go::TestSharedGroupSecondMemberDeliveryCountsDuplicate`, `TestQoSMismatchInvalidatesStep`, `TestOfflineSessionNotPresentInvalidatesTarget`, `TestRetainedReplayRejectsStaleRoundAndMissingRetainFlag` |
| 단계별 정수 발행·정확 토픽/오프라인·지연/중단·retained 수 | `collector_test.go::TestOfflineCommandOracleUsesPerPhasePublishCounts`, `TestOfflineCommandExpectationsFollowExactTopics`, `TestLateMeasureDeliveryPreservesDrainMissingSnapshot`, `TestInterruptedStepReportsObservedMissingDeliveries`, `TestRetainedOracleSupportsProfileTopicCounts` |
| PUBREL 수신 전 QoS 2 재전송 처리 | `client_test.go::TestSubscriberQoS2RetransmitBeforePubRelCountsOnce` |
| 리소스 중앙값·0 바이트 네트워크·표본 부족 처리 | `resource_test.go::TestMedianMemoryUsesLowerPlusHalfDifference`, `TestBuildResourceSummaryAcceptsZeroNetworkDeltas`, `TestBuildResourceSummaryRejectsInsufficientSamples` |
| 동등성 판정·지표 선택·실패 suite 빈 지표 처리 | `compare_suite_test.go::TestBuildBP02ComparisonRequiresMatchingEvidence`, `TestBP02MetricsSelectStreamsAndPhases`, `TestBP02SuiteFailureLeavesParentMetricsEmpty` |

```text
go run ./tests/benchmark/bp02 compare --profile tests/benchmark/bp02/testdata/smoke.json
go run ./tests/benchmark/bp02 suite --profile tests/benchmark/bp02/testdata/smoke.json
```

최종 valid fixed compare는 `bp02-20261007T133602.315971400Z-174004`, valid 5/5 suite는 `bp02-suite-20261007T133920.246951900Z-122940`다. 두 target 모두 setup 49/49(TLS 25/TCP 24), persistent offline session 10/10, retained replay 구독자 4명, validity error 0, `finalCheck=true`였다. Suite는 target당 metric 44개, metric별 sample 5개였고 모든 반복에 network stats가 있었다. Fractional boundary compare `bp02-20261007T140157.761617200Z-105696`도 양쪽 valid이며 phase별 FLOOR publish count를 확인했다. Fixed smoke 입력은 변하지 않았다. 최종 local gate는 130.42초 PASS; focused mqttclient/environment/BP02 tests, benchmark vet, traceability도 통과했다. 이는 local smoke/코드 검증이지 production gate가 아니다.

Manifest의 baseline `6c200085368d701f34dedfb31e1a968a84c09028`, `dirty=true`, profile SHA `b929efabd5f99e68758bec86a2034b0b80d153e9ce0291e64a8236b3e4afd7d4`는 실행 당시 사실이다. Profile 원문은 artifact에 연결된다. 이 smoke 실행 당시 `B_*`, production workload/storage, N/N-1 SLO, `SPEC-TBD-08`의 수치 제안 채택과 profile cutoff, release regression과 release acceptance는 미정이었다. 그 뒤 `B_*` 측정 방법과 N-1 기준은 §3.9에서 정했다. EMQX JWT placeholder와 SSL environment fallback은 사용하지 않았다.

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
- 인증은 §3.8과 같은 RS256 JWT다. Broker는 loopback이 아닌 `http` JWKS 주소를 거절한다. 그래서 각 broker Pod에 JWKS를 `127.0.0.1`로 제공하는 시험용 sidecar를 둔다. Tenant quota는 설정하지 않아 기본값(상한)을 쓰고, 적용된 값을 artifact에 기록한다. Tenant 한도에 걸린 거절이 나오면 해당 단계는 invalid다.
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

#### Runner 구성

- Generator는 target에 무관한 runner로 만든다. 입력은 MQTT endpoint, management endpoint, credential과 profile이다. 단계 경계에서 control marker(§3.6과 같은 방식)를 써서 외부 controller가 장애 주입과 복구를 수행한다.
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
- `prepare`는 fixture ConfigMap과 Secret을 server-side apply로 갱신한다. 512 cell의 credential 파일은 약 586 KB이므로 client-side apply의 `last-applied-configuration` annotation(최대 262,144 bytes)에 복제할 수 없다. Secret 본문은 그대로 유지한다. 기존 client-side apply manager가 소유한 값과 충돌하면 강제로 덮어쓰지 않는다. 이번 새 fixture에서 실패한 준비 작업이 만든 Secret 두 개만 삭제하고 다시 만들었다.
- Kill marker의 `target.nodeId`는 runner의 `killTarget.NodeID`에 맞는 JSON 문자열이다. Python 내부의 숫자 node ID는 marker를 쓸 때 문자열로 바꾼다. 숫자로 쓰면 Go가 marker를 읽지 못하므로 runner는 ACK를 쓰지 않고 해당 단계는 invalid가 된다.
- Local smoke profile(`tests/benchmark/capacity/testdata/local-smoke.json`)은 Docker에서 단계 진행·확인·정리 경로를 실행하려고 p99 한도를 1,000ms로 둔다. 이 값은 판정 기준이 아니다. EKS profile(`eks-capacity-v1.json`)은 100ms를 쓴다.

#### 시험 연결과 실행 기록

| 대상 | 시험 위치·명령 | 확인하는 규칙 | 결과 |
| --- | --- | --- | --- |
| Profile·cell·client ID | `tests/benchmark/capacity/{profile,plan}_test.go` | Profile 범위 검증, cell당 client 53명, tenant별 정확한 client ID, cell 밖으로 나가지 않는 filter | PASS |
| Gap·`cleanSessionLoss` 판정 | `tests/benchmark/capacity/collector_test.go` | 보장 수신자 규칙, persistent 무예외, PUBCOMP 뒤 미수신의 gap, clean publisher의 PUBCOMP 전 끊김(`cleanPublisherInterrupted`), 끊긴 동안의 publish 제외, p99 한도 경계의 `cleanSessionLoss` | PASS |
| 단계 판정·탐색 | `tests/benchmark/capacity/{verdict,ladder}_test.go` | p99 경계, invalid 우선, RTO·SessionPresent, 이분 탐색과 5회 확인 | PASS |
| Feature 활성화 | `tests/benchmark/capacity/management_test.go` `TestEnsureFeaturesActivatesAndVerifiesEveryRequiredFeature` | 세 feature를 활성화하고 하나라도 비활성이면 오류 | PASS |
| Marker 교환·Pod 자원 기록 | `tests/benchmark/capacity/control_test.go`; `tests/benchmark/capacity/eks_control_test.go` `TestEKSKillMarkersDecodeInRunner`; `tests/test_eks_capacity_controller.py` | Runner와 controller의 marker 순서와 timeout, 실제 Python producer → Go consumer의 5개 상태 JSON 형식, broker·sidecar·init container 자원의 분리 기록 | PASS. EKS에서 발견한 숫자 node ID 회귀는 수정 전 5개 상태 모두 FAIL → 문자열로 수정 뒤 모두 PASS. `GOMAXPROCS=2 go test ./tests/benchmark/capacity -count=1` 및 Python 33개 PASS, 2026-10-16 |
| EKS overlay 경계 | `GOMAXPROCS=2 go test -p=2 ./tests/integration -run '^TestEKSCapacityOverlay$' -count=1` | 기본 node 설정에서 MQTT TLS·security 경로만 다름, sidecar·Job 자원·routing 경계 | PASS, 2026-10-16 |
| Pod 장애 대상 identity | `tests/test_eks_capacity_controller.py` `FaultIdentityTests.test_signals_require_the_original_live_broker_identity` | 다른 Pod UID·worker·container, 종료·삭제·상태 미확인 대상과 sidecar에 신호를 보내지 않음 | PASS. Controller 시험 총 34개 |
| 실제 broker의 process 정지·복구 | [실행 결과](../../.artifacts/verification/capacity-pod-fault-20261016T033428Z/result.json), [controller rollback](../../.artifacts/verification/capacity-pod-fault-20261016T033428Z/controller-rollback-events.jsonl) | Native amd64 Linux Docker에서 실제 standalone broker PID 8의 thread 7개 정지 → HTTP 불통 → SIGCONT 뒤 Ready. 잘못된 실행 UUID 거절. 삭제 오류를 주입한 controller가 실제 process를 다시 실행시킴. 정상 TERM 전달 뒤 exit 0 | PASS. Docker stop 0.408초. 삭제 오류는 adapter에서 주입했으며 Kubernetes·EKS 수용 증거는 아님 |
| 삭제 요청 수락 뒤 종료 정리 | [실행 결과](../../.artifacts/verification/capacity-pod-cleanup-20261016T043338Z/result.json), [수정 전](../../.artifacts/verification/capacity-pod-cleanup-20261016T043338Z/before.json), [수정 뒤](../../.artifacts/verification/capacity-pod-cleanup-20261016T043338Z/after.json), [복구 timeout](../../.artifacts/verification/capacity-pod-cleanup-20261016T043338Z/restore-timeout.json) | 삭제 요청은 성공했지만 원래 UID인 Pod가 남은 상태에서 controller가 끝나도 같은 process의 재개를 시도함 | 수정 전에는 Ctrl+C 뒤 실제 broker가 계속 stopped였다. 수정 뒤 Ctrl+C·controller 예외·restore timeout 모두 HTTP Ready와 worker uncordon 확인. Kubernetes 삭제 지연은 adapter에서 주입했으며 이 정리 수정의 EKS 실행은 하지 않음 |
| CPU throttling·OOM·재시작 관측 | `tests/test_eks_capacity_controller.py` `RuntimeDiagnosticsTests`의 4개 시험; `python -B -W error::ResourceWarning tests/test_eks_capacity_controller.py` | cgroup v1/v2 단위 변환, 누계 증가량, 재시작·Pod 교체·읽기 실패 때 baseline 무효화, 현재·직전 OOM 상태 보존 | PASS, 2026-10-16 controller 시험 총 33개 |
| 관측 수집 경로 smoke | [실행 결과](../../.artifacts/verification/capacity-runtime-health-20261016T014702Z/result.json), [수집 자료](../../.artifacts/verification/capacity-runtime-health-20261016T014702Z/runtime-health.jsonl) | CPU·메모리 제한 Docker container의 실제 `cpu.stat` 읽기 → `ResourceSampler`·`RuntimeDiagnostics` → JSONL 기록. Docker inspect의 실제 상태를 Pod DTO로 변환한 adapter 사용 | PASS, 0.25 CPU·32 MiB에서 약 3초 부하: 30개 period 중 30개 throttled, 증가 시간 2,405,808µs. 재시작 뒤 delta `null`, 실제 OOMKilled exit 137 기록. Container 삭제 완료. Kubernetes exec/status 전송은 미검증 |
| 실제 EKS 관측 사전 확인 | [runtime-health](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/preflight/runtime-health.jsonl) | 실제 `kubectl exec`·Pod status 조회, cgroup v2 누계와 증가량 수집 | PASS, broker 3개 모두 읽기 성공. 유휴 상태 throttling 0. Kubernetes 1.37·AL2023 ARM64. 부하 중 여유나 모든 OOM 이력의 증거는 아님 |
| EKS의 큰 credential Secret 준비 | [실패](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/fixture-prepare-failure.json), [수정 뒤 성공](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/fixture-prepare-success.json) | Credential 파일 585,729 bytes를 annotation에 넣지 않고 Secret으로 저장 | Client-side apply FAIL → server-side apply PASS. Secret 값은 출력·공개 artifact에서 제외. Controller 회귀 시험 33개 PASS |
| Local 단계 실행 | `go run ./tests/benchmark/capacity local --profile tests/benchmark/capacity/testdata/local-smoke.json` | 장애 주입·복구·판정·정리가 끝까지 실행됨 | 수정 전 FAIL([기록](../../.artifacts/verification/capacity-local-20261015T231636Z/capacity-summary.json)), 수정 뒤 PASS 3/3([기록](../../.artifacts/verification/capacity-local-20261016T011708Z/capacity-summary.json)) |
| EKS 측정 | `tests/benchmark/capacity/eks/controller.py`, 아래 2026-10-16 실행 기록과 Pod 장애 보완 재실행 | `B_*` 기록 | 초기 3회와 주입 보완 뒤 1회 모두 invalid. 보완 뒤에는 삭제·N-1 측정·Ready·resync 복구가 끝났지만 quota 계열 거절 3건으로 중단했다. `B_*`와 bounds는 채택하지 않음 |
| 보완한 Pod 장애의 EKS 실행 | [Controller events](../../.artifacts/verification/eks-podfix-20261016T033737Z/capacity/controller/events.jsonl), [최종 node 상태](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/management-nodes.json) | 원래 Pod UID·container·실행 UUID 확인 → broker thread 정지 → 강제 삭제 → N-1 → 새 Pod Ready·resync | 해당 경로 1회 확인. 전체 용량 판정은 invalid이며 5회 확인과 상위 단계는 실행하지 않음 |

2026-10-15 수정 전 local 실행(Docker 3 node, 1–2 cell)은 단계 3개 중 1개가 통과하고 2개가 실패했다. 아래 원인 분석에서 실패가 broker 동작에서 나왔음을 재현으로 확인했다.

- Leader kill 뒤 생존 node가 PUBACK한 shared group `a_job`(clean session member) QoS 1 message를 살아 있는 member가 받지 못했다(gap 1개, 3개).
- 생존 node에 연결된 persistent `alert` subscriber 3개가 `measureN1` 중 broker 쪽에서 끊겼다. 약 0.4초 뒤 `SessionPresent=true`로 다시 연결했다.
- 1 cell 확인 실행의 `measureN1` QoS 1 p99는 2.59초였다.

원인 분석은 [N-1 delivery 분석](../../.artifacts/analysis/n1-delivery/analysis.md)에 있다. 2026-10-15 사용자 결정에 따라 다음과 같이 처리했다.

- Shared group gap: member 선택이 owner node의 도달 여부를 보지 않았고, durable 대상은 primary만 썼다. 죽은 node의 clean member queue에 들어간 message는 그 member가 `CleanStart=true`로 돌아오면 사라졌다. [ADR-0060](../decisions/ADR-0060-shared-subscription-durable-owner-loss-reroute.md)으로 ADR-0015 §8을 고쳤다. Ingress node는 도달할 수 없는 owner의 member를 고르지 않는다. Member session이 reset·폐기·만료되면 남은 shared message를 같은 group의 다른 member로 옮긴다. 수용 기록은 [Cluster §11.35](cluster-session-and-delivery.md#1135-공유-구독-durable-전달의-owner-손실-수용-매핑)다.
- 생존 node의 persistent 연결 끊김: 죽은 leader에 전달된 PUBACK COMPLETE commit이 5초 commit timeout에 걸려 broker가 연결을 닫았다. `proposal-recovery`를 켜면 재현 8회 중 0회로 사라졌다(끄면 8회 중 4회). Broker는 바꾸지 않았고 위 환경 조건에서 이 feature를 켠다.
- QoS 2: PUBREC 뒤 PUBREL commit 전에 clean session publisher의 연결이 끊기면 message가 전달되지 않는다. PRD §8에서 이 경우를 보장 대상에서 뺐고, 판정은 `cleanPublisherInterrupted`로 따로 기록한다.

수정 뒤 같은 명령을 다시 실행했다([기록](../../.artifacts/verification/capacity-local-20261016T011708Z/capacity-summary.json)). 세 feature가 활성 상태였고 단계 3개가 모두 통과했다.

| 단계 실행 | Cell | RTO | p99 `measureN` QoS 0/1/2 | p99 `measureN1` QoS 0/1/2 | Gap | `cleanSessionLoss` | `measureN1` 계획 밖 disconnect |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 01-r1 | 1 | 1.85초 | 2.2 / 53.7 / 57.0ms | 1.4 / 54.5 / 39.4ms | 0 | 0 | 0 |
| 02-r1 | 2 | 3.55초 | 25.2 / 96.9 / 160.7ms | 3.3 / 115.3 / 88.3ms | 0 | 0 | 0 |
| 02-r2(확인) | 2 | 4.12초 | 81.1 / 606.2 / 652.7ms | 1.3 / 81.7 / 98.5ms | 0 | QoS 1 2개, 최대 36.7ms | 0 |

이 실행은 Docker 한 host의 local smoke이며 p99 한도는 1,000ms다. 판정 경로가 동작한다는 증거일 뿐 `B_*`나 EKS 100ms 기준의 증거가 아니다. 확인 실행의 `measureN` p99가 600ms를 넘은 것은 장애 전 구간이다. 원인은 확인하지 않았다 [INFERENCE: 같은 host의 Docker CPU 경합].

#### EKS 용량 실행 기록 (2026-10-16)

사용자가 승인한 격리 fixture `eks-capacity-20261016T015110Z-db4c38`에서 실행했다. Broker는 3-AZ의 `m7g.large` 3대, generator는 taint로 분리한 `c7g.xlarge` 1대다. Broker container는 각각 1 CPU·4 GiB이고 sidecar 예산은 별도다. [Image 기록](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/image-records.json)에 ARM64 digest·OCI label을 고정했다. Peer 인증서·bootstrap pin을 새로 만들었고 합성 데이터만 썼다. `proposal-recovery`·`session-metadata`·`shared-subscription-reroute`를 모두 활성화했다.

| 실행 | 결과와 근거 |
| --- | --- |
| 기본 profile, 첫 8 cell·424 연결·320 publish/s | Setup 중 `CONNACK 0x88`로 invalid. Measure·장애 주입 전에 종료. 원인은 미확정이며 CPU 포화로 해석하지 않음. [Summary](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity/generator-output/capacity-summary.json) |
| 작은 부하 탐색, 첫 1 cell·53 연결·40 publish/s | 기본 단계 앞에 1·2·4 cell을 추가한 [profile](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/eks-capacity-low-range-v1.json). Rate·p99 100ms·RTO 30초·5회 확인·손실 기준은 그대로다. Python marker가 숫자 node ID를 써서 Go runner가 ACK하지 못함. Pod 삭제 없이 invalid. [Summary](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity-low/generator-output/capacity-summary.json) |
| Marker 수정 뒤 같은 작은 부하 | Leader node 2 Pod 삭제, N-1 60초 측정, uncordon까지 실행. 대체 Pod는 running이지만 10분 안에 Ready가 되지 않아 invalid. [Summary](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity-fixed/generator-output/capacity-summary.json), [controller events](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity-fixed/controller/events.jsonl) |

세 번째 실행의 마지막 직접 조회에서 node 2는 replication `HEALTHY`·`SAFE_TO_STOP`·`ready=false`였다([관리 API](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/management-nodes.json)). `/health/ready`는 503과 `node drain record is active`를 반환했다([응답](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/restored-pod-health.json)). 삭제 때 `FailedPreStopHook` event가 있었고, 대체 broker는 같은 drain operation을 재개해 `COMPLETED`를 commit했다([event](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/events.json), [log](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/narumq-1.log)). 현재 `force delete`가 preStop을 실행하지 않는다는 가정은 성립하지 않았다. 정상 drain을 포함한 이 실행을 갑작스러운 broker 손실의 수용 증거로 쓰지 않는다. 장애 전용 lifecycle 분리 또는 정상 drain 시험으로의 구분은 다음 실행 전에 결정해야 한다. 기준을 낮추거나 active drain을 임의로 지우지 않았다.

복구 실패와 별도로 runner는 QoS 1·2 ACKed gap을 각각 1개 기록했다. QoS 1은 `a_job` sequence 517, QoS 2는 `a_command` sequence 717이다([message 표본](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity-fixed/generator-output/steps/01-r1/messages-sample.jsonl)). 원인은 아직 확인하지 않았다. 다음은 이 invalid 실행의 관찰값이며 통과 판정이 아니다([step summary](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity-fixed/generator-output/steps/01-r1/step-summary.json)).

| QoS | `measureN` p99 | `measureN1` p99 |
| --- | --- | --- |
| 0 | 4.629ms | 4.937ms |
| 1 | 63.083ms | 66.681ms |
| 2 | 64.700ms | 65.851ms |

Runner가 관찰한 영향 client 13개의 reconnect RTO는 7.597초, SessionPresent failure는 0이었다. 이는 대체 broker의 Ready 복구 시간이 아니다. Generator의 측정 구간 CPU는 3 CPU 한도 기준 약 0.294%였다. [Pod resource 표본의 최댓값](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/resource-peaks.json)은 broker Pod 263m CPU·36 MiB, generator Pod 16m CPU·19 MiB다. Pod 값은 sidecar를 포함하며 연속 관측의 절대 최댓값이 아니다.

[Runtime 관측 분석](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/telemetry-analysis.json)에는 세 실행의 원본 JSONL과 같은 container에서 얻은 증가량만 합산했다. 세 번째 실행의 broker별 관측 period 합은 7,225·6,162·7,408이고 `nr_throttled`·`throttled_usec` 증가량은 모두 0이었다. 삭제·교체 중 node 2에서 CPU 값을 얻지 못한 표본 5개는 정상값으로 대체하지 않았다. `run-start`·`step-ready`·`step-complete`·`run-end` 경계와 periodic 표본이 실제 EKS에서 수집됐다. 관측된 현재·직전 종료 상태에 `OOMKilled`는 없었다. 삭제 사이의 모든 종료나 고부하에서의 안전성을 보장하지 않는다.

모든 실행의 `adopted`·`bounds`는 `null`이다. 5회 확인과 상위 단계 탐색은 완료하지 못했다. `B_*`, production 지원과 release gate는 미검증으로 유지한다.

시험 뒤 Namespace·NLB·target group·시험 PVC/EBS를 먼저 정리했다. 별도 state에서 만든 Terraform 자원은 계획한 42개만 생성하고 42개 모두 삭제했다. Fixture CloudWatch log group과 이번에 만든 ECR image tag 두 개도 삭제했다. AWS API로 cluster·VPC·활성 instance·시험 EBS·EIP·NLB·target group·IAM role·log group이 없고 NAT gateway가 deleted임을 확인했다([정리 계획](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/destroy-plan-review.json), [적용 기록](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/destroy-apply.txt), [최종 확인](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/cleanup-verification.json)). 보호 snapshot 3개와 `observability-20261014` image digest는 유지했다.

현재 Secret·TLS key·kubeconfig·Terraform state와 계획·비공개 log 등 58개 파일은 DPAPI CurrentUser로 암호화했다. 복호화한 SHA-256이 원본과 같은지 확인한 뒤 평문·임시 script·binary·provider cache를 지웠다([보관 결과](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/private-archive.json)). [집중 검증 기록](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/focused-verification.json)은 capacity package·Python 33개·vet·traceability PASS를 연결한다. 앞선 전체 local gate의 coverage 실패는 해결하거나 재실행하지 않았다.

#### Pod 장애 주입 보완의 EKS 재실행 (2026-10-16)

사용자가 Pod 장애 보완을 선택한 뒤 `eks-podfix-20261016T033737Z` fixture에서 다시 실행했다. 앞선 fixture는 이미 정리했으므로 별도 state로 같은 규모의 새 환경을 만들었다. Broker·runner ARM64 image digest와 작은 부하 탐색 profile은 앞선 실행과 같다([image](../../.artifacts/verification/eks-podfix-20261016T033737Z/image-records.json), [profile](../../.artifacts/verification/eks-podfix-20261016T033737Z/eks-capacity-low-range-v1.json)). Broker binary나 운영용 preStop은 바꾸지 않았다.

준비 때 broker 3개가 서로 다른 worker·AZ에 있고 generator는 전용 node에 있는지 확인했다([배치](../../.artifacts/verification/eks-podfix-20261016T033737Z/preflight/placement.json)). 모든 broker의 process helper가 실제 Pod UID를 반환했다. Kustomize가 만든 process ConfigMap은 namespace를 명시해 StatefulSet volume과 같은 이름으로 연결했다([실제 render](../../.artifacts/verification/eks-podfix-20261016T033737Z/rendered-broker.json)). MQTT 두 target group의 대상 3개가 모두 healthy인 상태에서 시작했다([NLB](../../.artifacts/verification/eks-podfix-20261016T033737Z/nlb-preflight.json)).

AWS Load Balancer Controller는 Helm chart 1.17.1·image v2.17.1을 썼다. 실제 container status의 digest `sha256:3f4a2222c128a7e2baccae3f1d2cc566c70aa7a6cddda6caa22488fd3aa0337c`는 teardown 중 남아 있던 EKS audit log에서 확보했다. 새로 pending인 Pod의 빈 status를 runtime 증거로 쓰지 않았다([도구 정보](../../.artifacts/verification/eks-podfix-20261016T033737Z/tooling.json), [선별한 audit 증거](../../.artifacts/verification/eks-podfix-20261016T033737Z/controller-image-audit-evidence.json)). Metrics server managed addon은 `v0.9.0-eksbuild.13`이다.


첫 1 cell(53 연결, 40 publish/s)에서 leader node 3을 골랐다. 원래 UID·container ID·실행 UUID를 확인하고 PID 8의 thread 9개를 멈췄다. 이어서 worker를 cordon한 상태로 Pod를 강제 삭제했다. `killing`·`down`·`restoring`·`restored`를 runner가 모두 관찰했다. Restore 처리는 uncordon과 새 Pod Ready·resync 확인을 포함해 28.93초였다([controller events](../../.artifacts/verification/eks-podfix-20261016T033737Z/capacity/controller/events.jsonl), [step summary](../../.artifacts/verification/eks-podfix-20261016T033737Z/capacity/generator-output/steps/01-r1/step-summary.json)). 삭제 때 preStop 실패 event는 있었지만 대체 broker는 active drain에 묶이지 않았다. 마지막 직접 조회에서 세 broker 모두 `READY`·`ready=true`였고 `/health/ready`도 모두 200이었다([node 상태](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/management-nodes.json)).

| QoS | `measureN` p99 | `measureN1` p99 |
| --- | --- | --- |
| 0 | 6.042ms | 3.273ms |
| 1 | 56.665ms | 61.815ms |
| 2 | 73.060ms | 68.873ms |

두 측정 구간의 ACK timeout·protocol error·계획 밖 disconnect는 모두 0이었다. 영향 client 13개가 복구됐고 SessionPresent 실패는 0이었다. Runner가 첫 장애 연결 끊김을 기준으로 잰 RTO는 2.227초였다. 주입 전 `killing` 관찰(ACK 직전)부터 회복까지를 같은 generator clock으로 넓게 잡으면 상한은 11.534초다. 이 상한에는 장애 전 controller 처리도 포함된다. 서로 다른 host 시각을 빼서 정확한 process 정지 RTO로 보고하지 않는다([clock 계산](../../.artifacts/verification/eks-podfix-20261016T033737Z/recovery-clock-analysis.json)).

이 한 번의 실행에서 ACKed gap·QoS 2 중복·`cleanSessionLoss`는 모두 0이었다. QoS 1 unacked 48개와 QoS 0 미수신 132개는 별도 기록이다. 모든 offered message가 전달됐다는 뜻은 아니다. 앞선 drain 혼합 실행에서 나온 ACKed gap의 원인과 재현 조건을 닫는 증거로도 쓰지 않는다.

전체 판정은 **invalid**다([summary](../../.artifacts/verification/eks-podfix-20261016T033737Z/capacity/generator-output/capacity-summary.json)). Runner는 `tenant limit rejection observed 3 times`를 기록했다. 그러나 이 counter는 MQTT `0x97`·`0x9F`를 모두 tenant 한도로 분류한다(`tests/benchmark/capacity/verdict.go` `isLimitRejection`, `client.go` `finishAck`). Broker는 서버 commit lag·delivery capacity 거절에도 `0x97`을 쓴다(`internal/adapters/tcp/server_submission.go`). Node 2의 실제 metric은 `narumq_publish_backpressure_rejections_total{reason="commit_lag"} 3`, `reason="tenant_quota"`는 0이었다([metric](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/narumq-1-metrics.txt), [log](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/narumq-1.log)). 따라서 runner 문구만으로 실제 tenant 한도 초과라고 단정하지 않는다.

Metric 합계는 서버 backpressure 설명을 뒷받침하지만 패킷별 원인이나 발생 구간과 직접 연결한 증거는 아니다. 재생성한 node의 metric에는 죽은 container의 이력이 없다. Quota 계열 거절의 원인·구간 기록과 tenant/server 분류 보완은 남았다. 이번 3건을 임의로 failure 구간의 허용 거절로 바꾸거나 제외하지 않았다([분석·한계](../../.artifacts/verification/eks-podfix-20261016T033737Z/rejection-analysis.json)). `adopted`·`bounds`는 null이고 5회 확인·상위 단계·`B_*` 채택은 미완료다.

[Resource 표본](../../.artifacts/verification/eks-podfix-20261016T033737Z/resource-peaks.json)의 broker Pod 최댓값은 193m CPU·35 MiB, generator Pod는 15m CPU·18 MiB였다. [Runtime 관측](../../.artifacts/verification/eks-podfix-20261016T033737Z/telemetry-analysis.json) 14개에서 읽을 수 있었던 같은 container의 throttling 증가량은 모두 0이었다. 삭제·교체 중 node 3의 CPU를 읽지 못한 표본 5개는 제외했다. 수집한 현재·직전 종료 상태에 OOMKilled는 없었다. 이 낮은 부하의 표본은 CPU 100% 포화나 더 높은 부하의 안전성을 검증하지 않는다.

시험 뒤 Namespace·NLB·target group·시험 PVC/EBS를 정리하고 Terraform 자원 42개를 모두 삭제했다. AWS API로 fixture cluster·VPC·활성 instance·시험 EBS·EIP·NLB·target group·IAM role·log group이 없고 NAT gateway가 deleted임을 확인했다. 이번에 다시 올린 image tag 두 개도 삭제했다. 보호 snapshot 3개와 `observability-20261014` digest는 유지했다([정리 계획](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/destroy-plan-review.json), [적용 기록](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/destroy-apply.txt), [최종 확인](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/cleanup-verification.json)).

TLS key·JWT credential·현재 Secret·kubeconfig·state·계획·비공개 log·임시 Docker 인증 등 62개 파일은 DPAPI CurrentUser로 암호화했다. 복호화한 SHA-256을 원본과 대조한 뒤 평문·임시 script·binary·provider cache를 지웠다([보관 결과](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/private-archive.json)). Capacity package·Python 34개·EKS overlay 집중 시험·vet는 PASS다([실행 기록](../../.artifacts/verification/eks-podfix-20261016T033737Z/focused-verification.json)). `GOMAXPROCS=2 go run ./tools/verify traceability`도 PASS(`P0 mapped 71/81, exceptions 10`)다. 전체 local gate는 재실행하지 않았으며 기존 coverage 미달은 남아 있다.

#### 삭제 지연 시 종료 정리 보완 (2026-10-16)

EKS 재실행과 자원 정리 뒤 `handle_kill`의 종료 정리 결함을 확인했다. 삭제 요청이 수락되자 `frozen` 추적을 끝내고 있어, 실제 삭제가 지연된 상태에서 controller가 중단되면 `finally`가 broker를 다시 실행시키지 않았다. 추적 종료를 `handle_restore`에서 다른 UID인 Ready Pod를 확인한 뒤로 옮겼다. 기존 대상이 남아 있으면 원래 identity에 대한 재개 경로를 유지한다.

Native amd64 Linux의 실제 standalone broker로 수정 전·후를 확인했다. 수정 전에는 삭제 수락 뒤 Ctrl+C를 주입하면 HTTP가 계속 응답하지 않았다. 수정 뒤에는 Ctrl+C·controller 예외·restore timeout 모두 `RunController.run`의 `finally`가 broker를 재개했고 HTTP Ready가 됐다. Worker uncordon도 확인했다. 시험 container는 0.5 CPU·256 MiB로 제한했다. Python controller 시험 34개도 PASS다([결과와 범위](../../.artifacts/verification/capacity-pod-cleanup-20261016T043338Z/result.json)).

실제 broker와 HTTP는 실행했지만 Kubernetes의 삭제 지연과 worker 상태는 adapter에서 주입했다. 관측 sampler는 이 정리 시험에서 제외했다. 이 수정으로 EKS를 다시 만들지 않았고, 앞선 EKS invalid 판정이나 `B_*` 미채택 상태도 바꾸지 않았다. Container는 정상 종료 코드 0을 확인한 뒤 지웠고 임시 binary도 지웠다.

사용자 결정에 따라 정리 가능한 중단만 지원한다. Controller 강제 종료나 workstation 장애를 복구하는 lease·watchdog은 추가하지 않았다. 이 경우에는 fixture의 수동 복구 또는 삭제가 필요하다. 따라서 앞선 “controller 중단 시 복구” 설명은 모든 process 종료를 보장하는 뜻이 아니다.





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

Drain suite는 별도 run ID와 output 경로를 쓴다.

```text
go run ./tests/compatibility/mqtt-clients-v1 --suite drain \
  --run-id drain-paho-go-python-will-acl-final \
  --output .artifacts/compatibility/mqtt-clients-v1/drain-paho-go-python-will-acl-final
```

Suite는 SDK cluster drain 2 case와 malformed DISCONNECT 8 case를 고정한다. Cluster case는 새 3-node broker, JWT/JWKS, test readiness LB를 쓴다. Malformed case는 fake server/trap listener를 쓴다. Timeout, Will delay, reconnect, LB settle, rate는 test 전용이다. Secret/private key는 임시 경로에만 둔다. 최신 기록은 9/10이며 Go의 중복 Server Reference trap case가 실패했다. 이는 전체 SDK 승인 결과가 아니다. Phase 1 adapter suite는 26/26 통과했다. Local verification은 Docker suite 두 개를 제외하며 nested SDK module을 다운로드하거나 Docker를 시작하지 않는다.

### 7.2 Fuzz 게이트와 수동 mutation 검증

표준 실행 명령:

```text
go run ./tools/verify local
go run ./tools/verify ci --base <merge-base>
go run ./tools/verify mutation
```

`local`과 `ci`는 gofmt 사전 점검 뒤 `.artifacts/verification/branch/module`의 계측 사본에서 atomic coverage를 포함한 전체 `go test ./...`를 한 번 실행한다.

Local의 `-parallel`은 8이다. CI는 Go의 유효 `GOMAXPROCS` 값을 `-parallel`에 넘긴다. 이 값은 package 안에서 `t.Parallel()`로 실행하는 test의 상한이다. Preflight, coverage, race, fuzz는 한 병렬 단계에서 실행한다. `local`과 `ci`는 mutation을 실행하지 않는다.

CI workflow는 Go gate 외에 S3 helper와 EKS worker observer의 Python 회귀를 별도 단계로 실행한다. Observer만 로컬에서 확인하려면 `python -B tests/test_eks_worker_recovery.py -v`를 실행한다. 이 시험은 AWS·Docker 없이 관찰 독립성·오류 보존·종료·Node identity 경쟁 경계를 확인한다. 실제 broker·Kubernetes API smoke와 남은 EKS gate는 [Drain spec §10](drain-and-rolling-update.md#eks-worker-observer-보완-기준)을 따른다. 이 로컬 통과는 production 장애 수용을 대신하지 않는다.

개발자가 mutation을 실행하려면 `go run ./tools/verify mutation`을 사용한다. 이 명령은 `delivery`, `mqtt`, `nodedrain`, `security`, `session` 다섯 package를 대상으로 한다. Gremlins worker와 test CPU는 각각 1이다. Runner는 한 번에 `max(1, min(5, floor(GOMAXPROCS/2)))`개 package를 실행하고 나머지는 다음 batch로 넘긴다. 실패하면 같은 batch의 남은 명령을 취소한다.

각 결과는 `.artifacts/verification/mutation-<package>.json`에 저장한다. 기본 Gremlins 설정은 `tests/verification/gremlins.yaml`에 있다. Source, file, mutant exclusion은 없다. CI의 `--base <merge-base>`는 coverage diff 계산에만 쓴다.
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

| 검증 계약 | 영구 시험 또는 실행 결과 |
| --- | --- |
| P0 연결·미연결·stale 예외·PRD ID·정렬·tracking 경계 | `tools/verify/requirements_test.go::TestVerifyP0RequirementMapping` — 8개 사례 PASS |
| 예외 JSON strict decode·schema·중복·빈 값·경로 경계 | `TestLoadP0ExceptionsStrictlyValidatesJSON` — 9개 사례 PASS |
| PRD prefix 파싱과 MQTT statement ID 규칙 | `TestParseTraceabilityAcceptsPRDPrefixesAndKeepsStatementRule` PASS |
| 미연결·stale P0의 CLI 거절 | 임시 예외 파일에서 `UI-01` 제거와 `API-01` stale 등록을 실행; 두 명령 모두 exit 1 및 지정 오류 출력 |
| 저장소 traceability gate (2026-10-10) | `go run ./tools/verify traceability` PASS. P0 요구사항 81개 중 68개가 시험에 연결됐고 예외 13개가 남았다. SEC-10은 16개 시험에 연결됐으며 예외에 없다. |
| 전체 local gate (2026-10-10) | `go run ./tools/verify local` PASS, 205.10초. Branch instrumentation, traceability, 5개 fuzz target, architecture, 전체 package·integration(125.359초), race, vet과 coverage가 통과했다. |
| CI 병렬도 실험 (2026-10-10, mutation 수동 분리 전) | `go run ./tools/verify ci --base 23005e1a0d819a357507ce173b8d0f89cf40cf19` PASS, 1,388.95초. Coverage는 `-parallel=16`, integration은 156.691초였다. Mutation 단계는 18분 29초였다. 다섯 package 결과는 662 killed, 0 lived, 6 not covered였다. MQTT는 efficacy 100%, mutant coverage 98.80%였지만 console에 22 timed out도 기록됐다. 이 기록은 mutation을 CI에서 제외하기 전 실행 결과다. |

2026-10-11 현재 작업 트리의 P0 예외는 10개다. `go run ./tools/verify traceability`는 PASS하며 `P0 mapped 71/81, exceptions 10`을 출력했다. 원격 main `verify` gate 결과, 네 조건의 merge 거절 probe와 `enforce_admins=true` 적용 증거는 §8.9에 기록했다. 보류된 DEP P0 예외 10개가 남아 있어 TEST-10은 미검증으로 유지한다.


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

`verify local`과 `verify ci`는 gofmt, vet, traceability, architecture, package/integration coverage, production race, fuzz 5종과 coverage policy를 실행한다. 두 명령은 mutation을 실행하지 않는다. Local coverage는 `-parallel=8`, CI coverage는 Go의 유효 `GOMAXPROCS`만큼 `-parallel`을 허용한다. `-parallel`은 package 안에서 `t.Parallel()`로 실행하는 test의 상한이다. 다섯 package mutation은 개발자가 `go run ./tools/verify mutation`으로 별도 실행한다. CI의 `--base <merge-base>`는 coverage diff 계산에만 쓴다. Atomic profile은 `.artifacts/verification/coverage.out`, mutation 결과는 `.artifacts/verification/mutation-<package>.json`이다. Integration은 build tag별 일반·failpoint broker를 임시 경로에 한 번씩 build하고 child process 정리 뒤 제거한다. Race 범위는 `./cmd/... ./internal/...`이며 별도 build되는 integration child broker는 race-instrumented가 아니다.

Coverage는 Go atomic coverprofile의 executable block을 집계한다. [ADR-0040](../decisions/ADR-0040-branch-coverage-arm-injection.md)에 따라 branch는 `if`의 then·else와 clause가 있는 switch·type switch·select의 각 case·default arm을 센다. Loop 조건, 단락 평가와 `func _` body는 제외한다. 빠진 else·switch default만 줄바꿈 없이 사본에 주입한다. Select default는 주입하지 않는다. 사본에서 전체 시험을 실행하고 manifest 좌표와 원본 SHA-256을 대조한다. Line coverage로 branch를 대체하지 않는다. 실행 가능한 변경 line이나 변경 결정점이 없으면 해당 diff coverage는 100%다.

현재 gate는 local coverage의 package별 `-parallel=8`, CI는 유효 `GOMAXPROCS`를 `-parallel` 상한으로 쓴다. Mutation은 자동 gate가 아니며 별도 수동 command로 다섯 package를 검사한다. 점수 threshold는 package마다 그대로 적용한다. 위 9개 core domain을 쓴다. Final ratchet은 overall production과 core별 threshold를 `coverage-policy.json`에 둔다. Branch는 두 번 측정한 값 중 작은 값을 소수점 둘째 자리에서 내림한 baseline ratchet을 사용한다. 전체·core ratchet은 측정값이 ratchet에서 0.20%p를 뺀 값보다 낮을 때 실패하며, 비교에는 0.005%p 반올림 오차를 추가로 허용한다. 허용폭은 branch ratchet에만 적용하며 line·function gate, diff 90%와 PRD shortfall에는 적용하지 않는다. 허용폭은 [ADR-0041](../decisions/ADR-0041-branch-ratchet-noise-tolerance.md), 현재 diff 하한은 [ADR-0044](../decisions/ADR-0044-diff-coverage-floor-90.md)를 따른다. PRD 전체 80%·core 90% 미달은 report의 `branchCoverage.requirementShortfalls`로 보고한다. 이를 ratchet gate 실패 사유로 쓰지 않으며, 미달이 남는 동안 TEST-06·TEST-07은 미검증으로 유지한다. Ratchet 변경에는 반복 안정성 증거와 정책/요구사항 갱신이 필요하다. Coverage 숫자만으로는 충분하지 않으며 모든 P0 requirement와 지원 MQTT normative statement에 assertion이 연결되어야 한다.

### 8.1 Verification 실행 정책

Repository root에서 전체 local·CI gate와 수동 mutation 검증을 실행한다.

```text
go run ./tools/verify local
go run ./tools/verify ci --base <merge-base>
go run ./tools/verify mutation
```

`local`·`ci`는 저장소 검증 순서를 소유한다. `mutation`은 별도 실행 명령이며 coverage/race/fuzz gate를 다시 실행하지 않는다. 단일 package 분석에는 Gremlins 직접 명령을 사용할 수 있다.

### 8.2 Branch 측정 gate 수용 증거

2026-10-08에 Windows/amd64, Go 1.26.5와 Git으로 확인했다. 변경 전 revision은 `3b1541f2d4fd916db314f56cd3e091132592a6f7`이다. Production source는 변경하지 않았다. 아래 수치는 이번 작업 트리에서 실행한 결과다. CI 플랫폼의 필수 status나 release acceptance 증거는 아니다.

| 시나리오 | 요구사항 | 시험 위치·실행 | 관찰 결과 |
| --- | --- | --- | --- |
| 실제 Go coverage block과 arm 대응 | TEST-06·TEST-07·TEST-11 | `tools/verify/branch_test.go::TestBranchCoverageMatchesGoCoverBlocks`; `go test ./tools/verify -count=1` | PASS. 17개 outcome 중 6개 실행, 의도한 미실행 11개 정확히 일치. 원본·사본 line/function total 일치 |
| 계측 경계와 strict manifest | TEST-11 | `TestInstrumentBranchesPreservesLinesAndAddsOnlyMissingArms`, `TestLoadBranchManifestRejectsInvalidSchemaAndJSON` | PASS. 줄 수 보존, 이미 있는 arm 중복 없음, select 의미 보존, 제외 결정점 미계측 |
| 출력 교체의 삭제 경계 | TEST-11 | `TestBranchInstrumentRejectsUnsafeOutputBeforeDeletingFiles`, `TestBranchOutputRejectsSymlinkToRepositoryRoot`, `TestBranchOutputRejectsRepositoryLinkToExternalDirectory`; `go test ./tools/verify -count=1 -v` | PASS. Root·상위·source·검증 증거 상위 directory 삭제 거절. 기본 branch directory와 외부 출력 허용. Windows junction의 root alias와 저장소 내부→외부 출력 alias도 거절 |
| Source·manifest·profile 불일치 거절 | TEST-06·TEST-07 | `TestEvaluateBranchCoverageRejectsStaleOrMismatchedManifest` | PASS. SHA 불일치, 파일 누락·초과와 대응 block 없음 거절 |
| Ratchet·diff·shortfall 분리 | TEST-08·TEST-09 | `TestBranchRatchetDiffFloorAndRequirementShortfall` | PASS. Ratchet 하락과 diff branch 95% 미달 실패. PRD shortfall만 있으면 baseline gate 통과 |
| 실제 사본 실행과 baseline 확정 | TEST-06·TEST-07·TEST-09 | Repository root에서 `go run ./tools/verify local` Run A·B·C | 세 실행 모두 `local: PASS`. Schema 2, branch `measured`, 전체 outcome 13,073개와 9개 core metric 확인 |
| 변경 전 분모 보존 | TEST-11 | 같은 명령의 Run 0과 Run A summary 비교 | Line total 33,701, function total 1,673으로 정확히 일치 |
| 실제 ratchet 거절 | TEST-09 | 저장소 밖 임시 policy의 routing branch ratchet만 100으로 올려 `coverage` 실행 | Exit 1. `narumq/internal/domain/routing branch ratchet 100.00% is above measured 93.15%` |
| 실제 historical diff | TEST-08 | `go run ./tools/verify coverage --profile .artifacts/verification/coverage.out --report <tmp>/diff-summary.json --base 02ef51d` | Exit 1. Diff branch 280/405(69.14%), line 697/823(84.69%), function 62/63(98.41%). Branch·line 95% 미달을 각각 보고 |

Run 0은 원본에서 coverage를 실행했다. Run A·B·C는 계측 사본에서 coverage를 실행하고 나머지 검사는 원본에서 실행했다.

| 실행 | Line covered/total (%) | Function covered/total (%) | Branch covered/total (%) |
| --- | --- | --- | --- |
| Run 0 | 28,716/33,701 (85.21) | 1,631/1,673 (97.49) | 미측정 |
| Run A | 28,710/33,701 (85.19) | 1,631/1,673 (97.49) | 10,434/13,073 (79.81) |
| Run B | 28,729/33,701 (85.25) | 1,632/1,673 (97.55) | 10,441/13,073 (79.87) |
| Run C | 28,714/33,701 (85.20) | 1,631/1,673 (97.49) | 10,439/13,073 (79.85) |

Baseline은 `floor(min(A,B)*100)/100`으로 정했다. Line·function threshold는 변경하지 않았다.

| 범위 | Run A branch (%) | Run B branch (%) | 확정 branch ratchet (%) | Run C branch (%) |
| --- | --- | --- | --- | --- |
| Overall | 79.81 | 79.87 | 79.81 | 79.85 |
| `domain/connrebalance` | 96.15 | 96.15 | 96.15 | 96.15 |
| `domain/delivery` | 100.00 | 100.00 | 100.00 | 100.00 |
| `domain/mqtt` | 99.27 | 99.27 | 99.27 | 99.27 |
| `domain/nodedrain` | 97.73 | 97.73 | 97.73 | 97.73 |
| `domain/release` | 100.00 | 100.00 | 100.00 | 100.00 |
| `domain/routing` | 93.15 | 93.15 | 93.15 | 93.15 |
| `domain/security` | 90.91 | 90.91 | 90.91 | 90.91 |
| `domain/session` | 98.73 | 98.73 | 98.73 | 98.73 |
| `domain/tenancy` | 99.33 | 99.33 | 99.33 | 99.33 |

원래 실패 시험 계획은 delivery ratchet 100%를 사용했다. 실제 delivery branch는 118/118(100%)이므로 그 명령은 exit 0과 `coverage: PASS`였다. 이를 실패로 기록하지 않았다. 같은 실패 경로를 routing ratchet 100%로 확인했다.

성공한 Run A 이전의 첫 시도는 원본 race 단계의 `TestNodeCloseFlushesHeldAppendOrReportsFailure/consumed-prior-error`에서 실패했다. 수정 전 `node_shutdown_storage_test.go:80`은 `ErrStorage`와 `os.ErrPermission`을 기대했지만 `close WAL: permission denied`만 받았다. 취소 뒤 integration process가 남아 있었으며, 바로 이어진 시도는 Windows 파일 잠금으로 계측 directory 삭제에 실패했다. 해당 process가 끝난 뒤 Run A·B·C가 연속 통과했다. 두 실패 시도는 baseline에 포함하지 않았다. Branch baseline을 확정할 때는 Raft 코드와 해당 test를 수정하거나 제외하지 않았다.

2026-10-08 후속 조사에서 `go test -race ./internal/adapters/raft -run "^TestNodeCloseFlushesHeldAppendOrReportsFailure$/^consumed-prior-error$" -count=300 -cpu=1,2,4 -failfast`가 같은 `close WAL: permission denied`로 FAIL했다. 원인은 시험 준비 코드(fixture)의 실패 주입 순서였다. Fixture는 실패 flag를 hold 중인 sync에 들어가기 전에 검사했으므로, release 뒤 index 4 sync는 성공할 수 있었다. `ProcessReady`가 성공 완료를 소비하면 보관한 append가 queue로 옮겨지고, worker의 다음 실패 batch에 함께 들어갈 수 있었다. 이때 미처리 queue와 보관한 append가 모두 없어져 `Close`는 pending append 오류 분기를 거치지 않았다.

실패 flag 검사를 held sync 뒤로 옮겼다. 이제 `consumed-prior-error`는 hold 중인 index 4 sync부터 실패한다. 선행 오류를 소비한 직후 `heldAppend`와 미처리 queue가 모두 남았는지도 assertion으로 확인한다. Production `Node.Close`는 변경하지 않았다. 기존 실패 경로는 스펙의 “미처리 queue가 남는 경우”가 아니며, 저장 실패는 이미 `ProcessReady`가 `ErrStorage`로 보고했다. Production 호출자는 해당 Close 오류 분류에 의존하지 않는다. `cmd/narumq/cluster_runtime.go`는 `defer node.Close()`를 사용하고, `cmd/narumq/raft_peer_config.go`는 driver 오류를 우선 반환한다.

수정 뒤 Windows/amd64, Go 1.26.5, `CGO_ENABLED=1`, `CC=gcc`에서 아래 명령을 저장소 루트에서 실행했다. Wall time은 명령 시작부터 종료까지 걸린 시간이다. CPU 설정별 반복 횟수는 각각 적용된다.

| 실행 명령 | 실행 범위 | 결과 | Wall time |
| --- | --- | --- | --- |
| `go test -race ./internal/adapters/raft -run "^TestNodeCloseFlushesHeldAppendOrReportsFailure$/^consumed-prior-error$" -count=1000 -cpu=1,2,4 -failfast` | CPU 설정 1·2·4마다 1,000회, 총 3,000회 | PASS, exit 0 | 105.07s |
| `go test -race ./internal/adapters/raft -run "^TestNodeCloseFlushesHeldAppendOrReportsFailure$" -count=300 -cpu=1,2,4 -failfast` | 세 subtest 각각 CPU 설정마다 300회, subtest당 총 900회 | PASS, exit 0 | 168.77s |
| `go test -race ./internal/adapters/raft -count=1` | Raft package 전체 1회 | PASS, exit 0 | 10.74s |

전체 local gate는 환경 변수 `GOMAXPROCS=4`로 `go run ./tools/verify local`을 실행했다. 각 시도 사이에 코드, 시험 선택과 coverage 합격선을 변경하지 않았다. 아래 실패를 제외하거나 PASS로 계산하지 않았다. 최종 PASS는 전체 gate가 매번 통과한다는 증거가 아니다. Branch coverage 변동과 다른 시험의 실패 원인은 이번 fixture 수정 범위에서 해결하지 않았다.

| Local gate 시도 | 결과 | 관찰 결과 | Wall time |
| --- | --- | --- | --- |
| 1 | FAIL, exit 1 | 시험·race·fuzz 통과 뒤 branch 10,430/13,073(79.78%)로 ratchet 79.81% 미달 | 138.14s |
| 2 | FAIL, exit 1 | `TestRaftPeerDriverTicksQueuesAndStops`의 5초 리더 선출 검사 실패 | 72.06s |
| 3 | FAIL, exit 1 | 취소 뒤 남은 `integration.test.exe`의 Windows 파일 잠금으로 계측 directory 삭제 실패 | 2.15s |
| 4 | FAIL, exit 1 | 파일 잠금 해소 뒤 `TestDynamicClusterMembershipTransitionCrashRecovery`에서 node 4 READY 대기 실패; `release_profile_unsynced` 관찰 | 164.32s |
| 5 | FAIL, exit 1 | 시험·race·fuzz 통과 뒤 branch 10,430/13,073(79.78%)로 ratchet 미달 | 130.76s |
| 6 | FAIL, exit 1 | 시험·race·fuzz 통과 뒤 branch 79.80%로 ratchet 미달 | 136.21s |
| 7 | PASS, exit 0 | `coverage: PASS`, `local: PASS`; branch 10,441/13,073(79.87%) | 129.72s |

각 gate 로그는 `.artifacts/verification/raft-close-fixture-local-{first,second,third,fourth,fifth,sixth,seventh}.log`에 보존한다. Coverage 판정까지 진행한 시도의 summary는 같은 directory의 `coverage-summary-raft-close-fixture-{first,fifth,sixth,seventh}.json`에 보존한다. 기존 branch baseline과 CI·release 미검증 상태는 유지한다.

이 섹션의 2026-10-08 Run C는 당시 PRD overall branch 85%보다 낮아 `requirementShortfalls`를 기록했다. 그 시점에는 TEST-06의 PRD floor와 TEST-06·TEST-07 CI 증거가 미검증이었다. 2026-10-09에 overall branch 기준을 80%로 낮춘 뒤의 Run C와 현재 상태는 §8.4에 있다.

출력 경로 안전성 보완 뒤 실제 `branch-instrument --output .`와 `--output .artifacts/verification`은 각각 exit 1로 삭제 전에 거절했다. 저장소 branch subtree에 외부 임시 directory로 향하는 Windows junction을 만들고 그 하위를 출력으로 지정한 CLI도 exit 1로 거절했다. 외부 sentinel 파일은 그대로 남았고 임시 junction은 제거했다. 기본 `branch-instrument`는 PASS였다. 기존 Run C profile을 새 manifest로 다시 평가한 `coverage`도 PASS였으며 측정값은 같았다. 안전성 회귀 시험은 skip 없이 통과했다. 이 추가 증거는 `.artifacts/verification/branch-output-safety-evidence.json`과 `coverage-summary-after-safety-review.json`에 보존한다. 전체 local gate의 Run A·B·C는 이 보완 전 실행이며, 보완 뒤에는 도구 package 전체 시험과 위 CLI 경로를 검증했다.

### 8.3 Branch ratchet 노이즈 허용폭 수용 증거

2026-10-08에 Windows/amd64, Go 1.26.5에서 확인했다. Production source는 변경하지 않았다. Run A·B·C는 `branchTolerance=0.20`을 사용했고 모두 `coverage: PASS`, `local: PASS`로 끝났다.

| 실행 | Branch covered/total (%) | 결과 |
| --- | --- | --- |
| Run A | 10,469/13,073 (80.08) | PASS |
| Run B | 10,462/13,073 (80.03) | PASS |
| Run C | 10,469/13,073 (80.08) | PASS |

세 실행의 overall spread는 0.05%p다. 허용폭 0.20%p보다 작고, 기존 9회 측정 spread 0.09%p보다도 작다. 계획한 35개 branch outcome ID는 `branch/manifest.json`에 모두 있으며, A·B·C의 `uncoveredOutcomes`에는 하나도 없다.

Run A·B의 작은 branch 값을 소수점 둘째 자리에서 내림해 overall ratchet을 80.03%로 바꿨다. Core ratchet은 A·B 값이 같아 기존 값을 유지했다. Line·function 기준은 변경하지 않았다.

| 범위 | 확정 branch ratchet (%) | Run C (%) |
| --- | ---: | ---: |
| Overall | 80.03 | 80.08 |
| `domain/connrebalance` | 96.15 | 96.15 |
| `domain/delivery` | 100.00 | 100.00 |
| `domain/mqtt` | 99.27 | 99.27 |
| `domain/nodedrain` | 97.73 | 97.73 |
| `domain/release` | 100.00 | 100.00 |
| `domain/routing` | 93.15 | 93.15 |
| `domain/security` | 90.91 | 90.91 |
| `domain/session` | 98.73 | 98.73 |
| `domain/tenancy` | 99.33 | 99.33 |

#### 결정적 시험과 branch outcome

아래 outcome ID는 각 Run A·B·C에서 모두 covered였다. `if`의 `then`과 `else`는 서로 다른 outcome ID다.

| 시험 | 확인한 branch outcome ID |
| --- | --- |
| `TestClusterMembershipRemovalOfLocalLeaderTransfersLeadership` | `cmd/narumq/cluster_drain.go:617:3 if then`, `cmd/narumq/cluster_drain.go:617:3 if else`, `cmd/narumq/cluster_drain.go:620:3 if then`, `cmd/narumq/cluster_drain.go:620:3 if else`, `cmd/narumq/cluster_drain.go:625:2 if then`, `cmd/narumq/cluster_drain.go:625:2 if else`, `cmd/narumq/cluster_drain.go:628:2 if else`, `cmd/narumq/cluster_runtime.go:1896:4 if then`, `cmd/narumq/cluster_runtime.go:2144:3 switch case@2147:3` |
| `TestReclaimUnsentProposalsStopsWhenDeferredRoomIsZero` | `cmd/narumq/cluster_proposals.go:69:3 if then`, `internal/adapters/raft/node_deferred_proposals.go:22:2 if then` |
| `TestClusterReadinessGate` | `cmd/narumq/cluster_readiness_diagnostics.go:65:2 switch case@82:2` |
| `TestClusterProposeMembershipChangeIgnoresPendingChangeAndFollower` | `cmd/narumq/cluster_runtime.go:1979:2 if then`, `internal/adapters/raft/node.go:608:2 if then` |
| `TestReconcileCommittedPeersSkipsRetiredLocalIdentityOutsideRaft` | `cmd/narumq/cluster_runtime.go:2151:9 if else`, `cmd/narumq/cluster_runtime.go:2157:9 if else`, `cmd/narumq/cluster_runtime.go:2160:2 if then` |
| `TestClusterAdmitPeerRejectsWhenNotLeader` | `cmd/narumq/cluster_runtime.go:2177:2 if then` |
| `TestClusterGetOperationReportsReadBarrierFailure` | `cmd/narumq/cluster_runtime.go:2871:2 if then` |
| `TestClusterStateProviderRemoteSummaryUsesLeaderMatchIndex` | `cmd/narumq/cluster_runtime.go:3619:3 if then`, `cmd/narumq/cluster_runtime.go:3619:3 if else`, `internal/adapters/raft/node.go:895:2 if then` |
| `TestNodeCloseFlushesHeldAppendOrReportsFailure` | `internal/adapters/raft/node.go:924:4 if then` |
| `TestQueueBatchOperationsRejectMissingSessionAndShortQueue` | `internal/adapters/memory/deliveryqueue/queue.go:180:2 if then`, `internal/adapters/memory/deliveryqueue/queue.go:260:3 if then` |
| `TestCSVWriterLoopQueuedControlCloseAndStoppedFailure` | `internal/adapters/tcp/server.go:1708:4 if then`, `internal/adapters/tcp/server.go:1702:5 if else` |
| `TestCSVWriteDeliveriesQueuePeekOutcomes` | `internal/adapters/tcp/server.go:1809:4 if then` |
| `TestServeBootstrapProbeDropsFailedHandshakeAndBadMagic` | `internal/adapters/tcp/peer_bootstrap_probe.go:153:4 if then`, `internal/adapters/tcp/peer_bootstrap_probe.go:157:4 if then` |
| `TestProbeBootstrapPeerReportsResponseReadFailure` | `internal/adapters/tcp/peer_bootstrap_probe.go:209:2 if then` |
| `TestDynamicDialDropsConnectionWhenIdentityChangesDuringHandshake` | `internal/adapters/tcp/peer_transport.go:2053:3 if then`, `internal/adapters/tcp/peer_transport.go:2058:4 if else` |
| `TestDynamicDialSuppressesErrorWhenIdentityChangesBeforeHelloResponse` | `internal/adapters/tcp/peer_transport.go:2104:4 if else`, `internal/adapters/tcp/peer_transport.go:2228:2 if else` |

`TestDynamicMembershipChangesApplyAndReplayAfterRestart`는 peer match index와 pending membership 변경 거절을 확인한다. `TestCSVSubscribeAckWriteFailureKeepsDeliveryBlocked`는 SUBACK write 실패 뒤 delivery 차단과 writer 종료를 확인한다. `TestRunClusterModeInProcess`는 joiner seed를 follower부터 시도하고, 제거 요청은 항상 follower를 대상으로 보낸다.

반복 race 시험은 모두 PASS했다.

```text
go test -race ./cmd/narumq -run '^(TestClusterMembershipRemovalOfLocalLeaderTransfersLeadership|TestReclaimUnsentProposalsStopsWhenDeferredRoomIsZero|TestClusterReadinessGate|TestClusterProposeMembershipChangeIgnoresPendingChangeAndFollower|TestReconcileCommittedPeersSkipsRetiredLocalIdentityOutsideRaft|TestClusterAdmitPeerRejectsWhenNotLeader|TestClusterGetOperationReportsReadBarrierFailure|TestClusterStateProviderRemoteSummaryUsesLeaderMatchIndex)$' -count=20
go test -race ./cmd/narumq -run '^TestRunClusterModeInProcess$' -count=3
go test -race ./internal/adapters/raft -run '^(TestDynamicMembershipChangesApplyAndReplayAfterRestart|TestNodeCloseFlushesHeldAppendOrReportsFailure)$' -count=50
go test -race ./internal/adapters/memory/deliveryqueue -run '^TestQueueBatchOperationsRejectMissingSessionAndShortQueue$' -count=20
go test -race ./internal/adapters/tcp -run '^(TestCSVSubscribeAckWriteFailureKeepsDeliveryBlocked|TestCSVWriterLoopQueuedControlCloseAndStoppedFailure|TestCSVWriteDeliveriesQueuePeekOutcomes|TestServeBootstrapProbeDropsFailedHandshakeAndBadMagic|TestProbeBootstrapPeerReportsResponseReadFailure|TestDynamicDialDropsConnectionWhenIdentityChangesDuringHandshake|TestDynamicDialSuppressesErrorWhenIdentityChangesBeforeHelloResponse)$' -count=20
```

#### 재기준과 허용폭 경계

재기준 뒤 Run C profile을 최종 policy로 평가한 결과는 `coverage: PASS`였다. `GOMAXPROCS=4 go run ./tools/verify local`도 한 번 더 실행해 `coverage: PASS`, `local: PASS`를 확인했다. 추가 Run의 overall branch는 10,470/13,073(80.09%)였다.

저장소 밖 policy 사본과 `.artifacts/verification/coverage-tolerance-c.out`으로 CLI 경계를 확인했다.

| Overall branch ratchet | Run C 대비 | 결과 |
| ---: | ---: | --- |
| 80.29% | +0.21%p | Exit 1. `overall branch ratchet 80.29% (tolerance 0.20%p) is above measured 80.08%` |
| 80.27% | +0.19%p | Exit 0. `coverage: PASS` |

초기 focused 시험 실행은 test import/API type 불일치로 compile에 실패했다. 첫 TCP race 실행에서는 TLS server name 불일치와 `writerDone` 종료를 기다리지 않는 assertion이 드러났다. 두 시험 문제를 수정한 뒤 위 반복 횟수로 모두 통과했다. Run A·B·C 및 재기준 뒤 local gate에는 실패가 없었다.

이 섹션의 2026-10-08 Run C와 추가 실행은 당시 PRD overall branch 85%보다 낮아 `requirementShortfalls`를 기록했다. Core 9개는 모두 local branch 90% 이상이었다. 이 실행에는 CI 필수 status, TEST-10 P0 mapping, TEST-07 diff CI 증거, `TestRaftPeerDriverTicksQueuesAndStops`와 `TestDynamicClusterMembershipTransitionCrashRecovery`가 포함되지 않았다. 2026-10-09에 overall branch 기준을 80%로 낮춘 뒤의 local coverage 결과는 §8.4에 있다.

### 8.4 전체 branch floor 수용 증거

2026-10-09에 Windows/amd64, Go 1.26.5에서 확인했다. Production source는 변경하지 않았다. 기준선은 10,470/13,073(80.09%)였다. Run A·B는 당시 overall branch 85% 기준에서 각각 82.82%·82.89%로 미달했다. 이후 사용자가 전체 branch 기준을 80%로 낮춰 PRD와 policy를 갱신했고, Run C를 새 기준으로 실행했다.

`cmd/`와 `internal/`의 production source 변경 여부를 확인했다. `git diff --name-only -- cmd internal` 결과는 추적 중인 파일 46개였고, `git ls-files --others --exclude-standard -- cmd internal` 결과는 새 파일 2개였다. 48개 경로는 모두 `_test.go`로 끝났다. `git diff --cached --name-only -- cmd internal` 결과는 비어 있었다.

| 실행 | Branch covered/total (%) | Coverage gate | 전체 local gate |
| --- | --- | --- | --- |
| Run A | 10,827/13,073 (82.82) | PASS | PASS |
| Run B | 10,836/13,073 (82.89) | PASS | PASS |
| Run C (80% 기준) | 10,830/13,073 (82.84) | PASS | PASS |

Run A·B는 기존 85% 기준 policy로 실행해 `branchCoverage.requirementShortfalls`에 각각 82.82%·82.89%의 shortfall을 기록했다. 새 80% 기준으로 실행한 Run C에서는 `requirementShortfalls`가 비었다. Run C line은 87.59%, function은 98.68%였다.

기준선보다 Run A에서 357개, Run B에서 366개의 branch outcome이 추가로 covered됐다. 아래 수치는 Run A와 기준선의 미실행 outcome 수 차이다.

| 범위 | 추가 covered outcome | 대표 시험 |
| --- | ---: | --- |
| `cmd/narumq` | 170 | `TestClusterDrainBarrierBlocksWhenOtherVotersAreDisconnected`, `TestRecoverProposalsFlushesCountAndByteBoundedBatches`, `TestRecoverProposalsReturnsClosedNodeErrors` |
| `internal/adapters/auditlog` | 18 | `TestVerifyDirectoryRejectsCorruptSequenceAndHashChain` |
| `internal/adapters/http/management` | 6 | `TestClusterBackupHTTPReportsUnavailableAndInternalErrors` |
| `internal/adapters/memory/deliveryqueue` | 5 | `TestQueueBatchEnqueueRejectionsPreserveQueuedState` |
| `internal/adapters/raft` | 11 | `TestSnapshotPersistenceFailuresRecoverFromDurableState`, `TestCompleteRestoreWALPersistenceFailuresCanBeCleaned` |
| `internal/adapters/raft/records` | 26 | `TestSnapshotRejectsStateIndexesBeyondLastIncluded`, `TestBackupArchiveRejectsInconsistentMembershipAndEntries` |
| `internal/adapters/tcp` | 33 | `TestNewServerRejectsIncompleteClusterBoundary`, `TestDynamicSendRejectsStagedForwardingAndStoppedPeers` |
| `internal/application/audit` | 28 | `TestValidateEventRequiredFields`, `TestValidateEventDetailsJSONStringTraversal` |
| `internal/application/connect` | 26 | `TestConnectRejectsDuplicateAndRepositoryErrors`, `TestRecoverOwnedSessionRejectsOutOfRangeMetadataWithoutInstallingState` |
| `internal/application/membership` | 29 | `TestProposalReceiptCapacityLimits`, `TestCMAApplyRejectsMalformedCommandsWithoutAdvancing` |
| `internal/application/publish` | 5 | `TestPublishRemoteForwardErrorDependsOnHandling` |

Core 모듈 9개의 coverage와 ratchet은 바뀌지 않았다. 기존 값은 §8.3에 있다.

#### 반복 race 검증

새 cluster, TCP, auditlog, delivery queue 시험을 race detector 아래 20회 반복했다. 네 명령 모두 PASS했다.

```text
go test -race ./cmd/narumq -run '^(TestClusterReleaseProfileReconcileMapsProposalConflictsToReadiness|TestClusterStateProviderValidatesNodeQueriesAndPaginatesFilteredResults|TestClusterDrainBarrierBlocksWhenOtherVotersAreDisconnected|TestRefreshReadWaitersKeepsRequestWhenPeerCapacityIsExhausted|TestIssueReadProofReturnsClosedRaftNodeError|TestRecoverProposalsFlushesCountAndByteBoundedBatches|TestRecoverProposalsDropsStaleProofWhenCapacityIsExhausted|TestRecoverProposalsReturnsClosedNodeErrors)$' -count=20
go test -race ./internal/adapters/tcp -run '^(TestPacketIDAllocatorSkipsZeroAfterIdentifierWrap|TestBeginOutboundAckRejectsZeroAndIgnoresUnknownPacketID|TestCommittedOwnershipFenceDoesNotRegressToStaleEpoch|TestStoppedConnectionCannotBindSession|TestCloseSessionForDrainDoesNotClaimUnknownSession|TestNewServerRejectsIncompleteClusterBoundary|TestDynamicSendRejectsStagedForwardingAndStoppedPeers)$' -count=20
go test -race ./internal/adapters/auditlog -run '^(TestVerifyDirectoryRejectsCorruptSequenceAndHashChain|TestFileAuditPublisherRejectsUnencodableEventAndBecomesUnavailable|TestFileAuditPublisherClosedStateRejectsPublishAndReadiness|TestVerifyDirectoryEmptyAndEmptyActiveLog|TestScanAuditDataRejectsMalformedRecords|TestOpenFileAuditPublisherRejectsNonRegularActivePath)$' -count=20
go test -race ./internal/adapters/memory/deliveryqueue -run '^(TestFinishDetachCommittedRemovesAndWakesWaiters|TestRemoveWakesDetachWaiters|TestQueueBatchEnqueueRejectionsPreserveQueuedState)$' -count=20
```

#### 미실행 outcome과 미달 원인

Run A에는 2,246개의 미실행 outcome이 남았다. 아래 네 종류는 해당 ID를 직접 확인해 개수를 셌다.

| 제외 사유 | Run A 미실행 outcome |
| --- | ---: |
| `sort.Slice` comparator 안의 nil guard | 22 |
| 생성 protobuf marshal 오류 처리 | 15 |
| 1 GiB archive 크기 제한 | 2 |
| Windows stable-store sync 분기 | 5 |
| 직접 확인한 제외 outcome 합계 | 44 |

이 44개는 전체 미실행 수가 아니다. 남은 목록에는 실제 clock·sleep·스케줄 순서, 주입할 수 없는 OS 오류, 넓은 cluster network harness에 속한 분기도 있다. 같은 파일과 함수에 일반 동작 분기도 섞여 있어 이 세 사유는 파일 전체를 제외 수로 계산하지 않았다. 나머지 2,202개도 모두 제외 대상이거나 모두 테스트 가능한 outcome이라고 단정하지 않는다.

Run A에서 미실행 outcome이 가장 많은 파일은 `cluster_runtime.go` 434개, `cluster_drain.go` 156개, `stable_store.go` 131개, `records/codec.go` 85개, `tcp/server.go` 75개였다. 이 수에는 계획에서 명시적으로 제외하지 않은 경로도 포함된다.

Run A·B는 당시 85% 기준에 미달했지만, 사용자의 요청으로 PRD TEST-06과 policy의 overall branch 기준을 80%로 낮췄다. Core branch 90%, diff branch 95%, line·function 기준은 바꾸지 않았다. Run C는 82.84%로 새 전체 branch floor를 충족했고 `requirementShortfalls`는 비었다. Overall ratchet 80.03%와 core ratchet은 유지했다. 로컬 coverage 기준은 충족했지만 CI 필수 status, TEST-10 P0 mapping, TEST-07 diff CI 증거가 없어 TEST-06~TEST-10 항목은 미검증으로 남는다.

Run B profile에서 scratch policy로 허용폭 경계를 확인했다. 83.10%(측정값보다 +0.21%p)는 tolerance를 적용해도 측정값보다 높아 exit 1을 반환했다. 83.08%(+0.19%p)는 `coverage: PASS`를 반환했다. 결과는 `.artifacts/verification/coverage-boundary-plus-021.json`과 `.artifacts/verification/coverage-boundary-plus-019.json`에 있다.

결과 파일은 `.artifacts/verification/coverage-summary-floor-{a,b,c}.json`, `.artifacts/verification/floor-local-{a,b,c}.log`, `.artifacts/verification/coverage-boundary-plus-{021,019}.json`, `.artifacts/verification/traceability-branch-floor80.log`에 있다. CI 필수 status, TEST-10 P0 mapping, TEST-07 diff CI 증거는 이 작업에서 확인하지 않았다.

### 8.5 변경 코드 diff floor 90% 수용 증거

2026-10-09에 사용자가 변경 코드의 line·function·branch 기준을 모두 90%로 정했다. [ADR-0044](../decisions/ADR-0044-diff-coverage-floor-90.md)에 따라 PRD TEST-08과 `tests/verification/coverage-policy.json`의 `diffFloor`를 함께 갱신했다. 전체·core 기준, ratchet과 측정 방식은 변경하지 않았다. §8.2~§8.4의 95% diff 기준과 실행 결과는 당시 기록이다.

최신 커밋은 `de19302d9887bfe2650dd6dcc5ad975281d4a192`이며 부모 커밋은 `74c3cdbb4abc9b01e07e8fa7b936653e25c446f7`이다. 앞선 전체 local gate에서 만든 atomic profile과 branch manifest를 재사용했다. Coverage CLI가 원본 source와 manifest를 대조한 뒤 새 policy로 평가했다. 이번 작업에서 production source는 변경하지 않았다.

저장소 루트에서 다음 명령을 실행해 `coverage: PASS`, exit 0을 확인했다.

```text
go run ./tools/verify coverage --profile .artifacts/verification/coverage.out --policy tests/verification/coverage-policy.json --branch-manifest .artifacts/verification/branch/manifest.json --base 74c3cdbb4abc9b01e07e8fa7b936653e25c446f7 --report .artifacts/verification/coverage-summary-diff-floor90.json
```

| 최신 커밋 diff 지표 | Covered / total | 측정값 | 하한 | 결과 |
| --- | --- | --- | --- | --- |
| Line | 82 / 84 | 97.62% | 90% | PASS |
| Function | 16 / 16 | 100% | 90% | PASS |
| Branch | 32 / 34 | 94.12% | 90% | PASS |

| 수용 시나리오 | 요구사항 | 실행·시험 위치 | 확인 결과 |
| --- | --- | --- | --- |
| 새 policy로 최신 커밋 평가 | TEST-08 | 위 coverage CLI | 세 diff 지표 통과. Report의 `passed=true`, `branchCoverage.status=measured`, `requirementShortfalls` 비어 있음 |
| Diff 하한과 같은 branch 값 허용 | TEST-08 | `tools/verify/branch_test.go::TestBranchRatchetDiffFloorAndRequirementShortfall` | 측정값 90%, 하한 90%에서 통과 |
| Line·function diff 하한 경계 | TEST-08 | `tools/verify/coverage_test.go::TestDiffCoverageLineAndFunctionFloorBoundaries` | 각 지표 90%·90.01%에서 통과. 다른 지표가 90%여도 해당 지표 89.99%는 독립적으로 거절 |
| Diff에는 ratchet 허용폭 미적용 | TEST-08·TEST-09 | `tools/verify/branch_test.go::TestBranchRatchetDiffFloorAndRequirementShortfall` | 측정값 90%, 하한 90.01%, ratchet 허용폭 0.20%p에서도 diff 거절 |
| 실제 하한 미달 거절 | TEST-08 | 위 CLI의 `--base`를 `02ef51d`, `--report`를 `.artifacts/verification/coverage-summary-diff-floor90-historical.json`으로 바꿔 실행 | Branch 366/445(82.25%)로 exit 1. `diff branch coverage 82.25% is below 90.00%` |
| 도구 package 회귀 검증 | TEST-08·TEST-09·TEST-11 | `go test ./tools/verify -count=1` | Line·function 경계 시험 보강 뒤 PASS, package 6.191초 |
| 요구사항 추적성 유지 | TEST-10 | `go run ./tools/verify traceability` | PASS, P0 60/81 연결, 예외 21개 |

이번 결과는 기존 profile을 새 기준으로 평가한 로컬 증거다. 전체 local·CI gate를 새로 실행한 결과가 아니며, 실제 필수 CI status와 merge 차단도 확인하지 않았다. P0 예외 21개와 CI 증거가 남아 TEST-10은 미검증으로 유지한다.

### 8.6 간헐 실패 원인 수정 수용 증거

대상은 §8.2의 2026-10-08 local gate 시도 2·4다. 시험을 제외하거나 대기 시간을 늘리지 않고 두 대기 경로를 고쳤다.

**원인 A — bootstrap 적용 전 campaign:** 시도 2의 `.artifacts/verification/raft-close-fixture-local-second.log:82-83`에서 `TestRaftPeerDriverTicksQueuesAndStops`가 5.04초 뒤 실패했다. 두 node 모두 `Role:1`, `Term:3`, `LeaderID:0`이었다. 시험은 driver 시작 전에 `node1.Campaign()`을 호출했다. 이때 bootstrap ConfChange entry 3개가 아직 적용되지 않았다.

승인된 계획의 선행 조사에서는 임시 사본에 etcd raft debug log를 켜 다음 원문을 확인했다: `1 cannot campaign at term 1 since there are still pending configuration changes to apply`. Campaign이 무시되면 두 node는 100ms tick의 randomized election timeout(10~19 tick)에 의존한다. Peer 3은 응답하지 않는다. 두 node가 같은 tick에 선출을 시작하면 각각 자신에게 투표해 split vote가 생길 수 있다. 이때 재선출마다 1~2초가 더 걸린다.

선행 조사에서 수정 전 무부하 race 시험 180회는 모두 통과했지만 시험 시간이 1.2~4.9초였다. 별도 계측 40회의 “선출+peer 3 거절” 구간은 1.01~4.41초였고 8회가 2초를 넘었다. 4.41초 사례는 term 1→2→3의 split vote 뒤 term 4에서 선출됐다. 병렬 부하로 tick 처리가 늦어지면 기존 5초 제한을 넘을 수 있다. 이는 production 결함이 아니라 시험 준비 순서의 오류다.

수정한 `cmd/narumq/raft_peer_driver_test.go`는 두 driver를 먼저 시작한다. Node 1의 bootstrap commit이 적용된 뒤 campaign한다. 기존 5초 선출 제한과 node 1·2 중 어느 리더든 허용하는 검사는 유지한다. Production `runRaftPeerDriver`와 Raft adapter는 변경하지 않았다. 수정 뒤 CPU 설정 1·2·4의 총 300회 race 시험은 모두 통과했다. `-v`에 표시된 시험 전체 시간은 0.42~0.54초였다. 이 값은 선출 구간만의 시간이 아니다.

**원인 B — profile proposal의 무기한 대기:** 시도 4의 `.artifacts/verification/raft-close-fixture-local-fourth.log:130-283`에서 node 4의 READY 대기가 실패했다. Join은 index 17에서 `SUCCEEDED`였다. 그 직전 node 2가 `promote-voter-before-persist` failpoint로 종료됐다. Node 4의 readiness에는 `release_profile_unsynced`가 남았다.

Stage 요청은 `releaseVersion`만 보내지만 joiner build는 feature 3개를 광고한다. 따라서 controller는 local `MemberProfileChange`를 제안한다. `clusterReleaseProfileController.run`은 `reconcile`의 반환을 기다린다. 기존 propose 분기는 process 수명의 context를 `Commit`에 전달했다. `beginCommit`의 2초 상한은 read barrier에만 적용됐고 `waitProposal`은 원래 context로 결과를 기다렸다. 결과가 오지 않으면 controller가 ticker와 wake를 처리하지 못하고 gate는 닫힌 채 남았다.

[추정] 기존 실패 로그는 proposal 유실 자체를 직접 보여 주지 않는다. Leader 손실 뒤 proposal 결과가 오지 않았다는 설명은 로그와 일치하는 무기한 대기 경로에 근거한다. 새 회귀 시험은 실제 leader 손실을 재현하는 대신 driver가 proposal을 인수한 뒤 결과를 보내지 않는 조건을 만들어 이 대기를 검증한다. 실제 process 회귀는 별도로 아래 3회 실행했다.

수정한 `cmd/narumq/cluster_release_profile.go`는 다른 cluster commit 경로와 같은 5초 상한을 `Commit`에 적용한다. 상한이 지나면 기존 오류 분기로 반환한다. Gate는 닫힌 채 유지하고 incompatibility는 latch하지 않는다. Controller는 다음 reconcile 주기에 다시 확인한다. Ticker 간격은 500ms지만 대기 중 쌓인 tick이 있으면 반환 직후 다시 확인할 수 있다. 늦게 적용된 profile은 기존 적용 알림으로 gate를 무효화하고 controller를 깨운다. Commit의 read barrier도 이 5초 context를 사용한다. Drain·rebalance·tenant quota의 Commit 경로는 변경하지 않았다.

2026-10-09에 Windows/amd64, Go 1.26.5, `CGO_ENABLED=1`, `CC=gcc`에서 아래 명령을 저장소 루트에서 실행했다. Wall time은 명령 시작부터 종료까지의 시간이다.

| 실행 명령 | 결과와 관찰 | Wall time |
| --- | --- | --- |
| `go test ./cmd/narumq -run '^TestClusterReleaseProfileReconcileBoundsUnansweredProposal$' -count=1` (수정 전) | FAIL, exit 1. `profile proposal deadline = (..., true), want earlier than controller context ...`; 두 deadline이 같았다. | 4.38s |
| 같은 명령 (수정 뒤) | PASS, exit 0; package 6.021s | 9.27s |
| 같은 명령에 `-v` 추가 (수정 뒤) | PASS, exit 0; 회귀 시험 5.00s | 9.23s |
| `go test -race ./cmd/narumq -run '^TestClusterReleaseProfile' -count=1` | PASS, exit 0; controller suite | 16.27s |
| `go test -race ./cmd/narumq -run '^TestClusterReleaseProfile' -count=3` | PASS, exit 0; controller suite 3회 | 23.11s |
| `go test -race ./cmd/narumq -run '^TestRaftPeerDriverTicksQueuesAndStops$' -count=100 -cpu=1,2,4 -v` | PASS, exit 0; 총 300회, 시험 전체 시간 최대 0.54s | 142.24s |
| `go test -race ./cmd/narumq -count=1` | PASS, exit 0; package 전체 | 28.22s |
| `go test ./tests/integration -run '^TestDynamicClusterMembershipTransitionCrashRecovery$' -count=3 -v -timeout 30m` | PASS, exit 0; 실제 process 3회(26.50s·22.60s·23.06s), node 4를 포함한 READY 검사 통과 | 74.91s |
| `GOMAXPROCS=4 go run ./tools/verify local` | 첫 시도 PASS, exit 0; branch-instrument·traceability·architecture, fuzz 5개 각각 10,000회, package·integration 시험, production package race와 coverage 통과; `local: PASS` | 150.11s |
| `go run ./tools/verify traceability` | PASS, exit 0; P0 66/81 연결, 예외 15개 유지 | 1.02s |

새 실행의 원본 출력은 `.artifacts/verification/flaky-cluster-{profile-before,profile-after,profile-after-verbose,profile-race-once,profile-race,driver-race,package-race,membership-process,local-first,traceability}.log`에 보존한다. Exit code와 wall time은 같은 directory의 `flaky-cluster-results.json`에 보존한다. Local gate의 합격선과 시험 선택은 변경하지 않았다. Process 시험의 성공 출력은 개별 node 로그를 표시하지 않으므로 일시적인 `release_profile_unsynced`의 지속 시간은 측정하지 않았다.

2026-10-09 실행 시점에는 필수 CI 증거가 없어 TEST-12~TEST-15를 `미검증`으로 기록했다. TEST-12의 현재 manual-only policy와 결과는 §8.8에 기록한다. 격리한 flaky test는 없어 owner·해소 기한 기록 대상은 없었다. 당시 파일 배치와 호출 경로는 바꾸지 않았다.

### 8.7 Mutation gate 범위 확장 수용 증거

이 절은 기존 CI mutation 정책의 역사적 증거를 보존한다. 2026-10-09, Windows/amd64, Go 1.26.5에서 저장소 루트의 명령을 실행했다. 변경 전 revision은 `471bf60`이다. 당시 사용자는 CI mutation 범위에 `mqtt`·`security`를 추가하고, `variableByteIntegerSize`의 경계 assertion을 보강하며, Gremlins 기본 mutator 5종을 유지하기로 정했다. Production `framing.go`와 `tests/verification/gremlins.yaml`은 변경하지 않았다. 현재 manual-only policy의 실행 결과는 §8.8을 따른다.

다섯 mutation 명령을 §7.2의 순서대로 하나씩 실행했다. 아래 시간은 명령 시작부터 종료까지의 wall time이다. 모든 명령은 exit 0을 반환했다.

| Package | Killed | Lived | Not covered | Efficacy | Mutant coverage | 시간 |
| --- | --- | --- | --- | --- | --- | --- |
| `internal/domain/delivery` | 27 | 0 | 0 | 100% | 100% | 209.599s |
| `internal/domain/mqtt` | 516 | 0 | 6 | 100% | 98.85% | 958.839s |
| `internal/domain/nodedrain` | 64 | 0 | 0 | 100% | 100% | 132.628s |
| `internal/domain/security` | 34 | 0 | 0 | 100% | 100% | 91.381s |
| `internal/domain/session` | 56 | 0 | 0 | 100% | 100% | 136.568s |

Timed out·Not viable·Skipped는 각 package에서 모두 0개였다. Timed out은 KILLED에 합산하지 않는다. Gremlins 공식대로 efficacy 계산에서 제외한다. 기본 mutator는 `ARITHMETIC_BASE`, `CONDITIONALS_NEGATION`, `CONDITIONALS_BOUNDARY`, `INCREMENT_DECREMENT`, `INVERT_NEGATIVES`의 5종이다.

MQTT의 Not covered 6개는 `framing.go:43:13`, `45:13`, `47:13`의 `CONDITIONALS_BOUNDARY`·`CONDITIONALS_NEGATION`이다. 계획 시점과 위치·수치가 같다. [추정] Go coverage block이 expression `switch`의 `case` 식을 별도 실행 구간으로 잡지 않아 Gremlins가 해당 mutant를 시험 대상에서 뺀다. 경계 assertion을 보강했지만 Gremlins의 이 수치는 바뀌지 않았다. 아래 수동 mutant는 실제 검출력의 차이를 확인한다.

수동 mutant는 `git archive --format=tar -o "$TEMP/narumq-vbi.tar" HEAD`로 만든 임시 사본에서 확인했다. 작업 트리의 수정된 `packet_test.go`를 복사한 뒤 사본의 `framing.go`만 `case value <= 128:`로 바꿨다. `go test ./internal/domain/mqtt -run '^TestRemainingLengthFramingHelperEdges' -count=1`은 exit 1로 실패했다. 오류는 `variableByteIntegerSize(128) = 1, want 2`였다. 같은 mutant에서 `git show HEAD:internal/domain/mqtt/packet_test.go`의 원본 시험을 복원하면 같은 명령이 exit 0으로 통과했다. 기존 assertion은 이 경계 오류를 놓쳤고, 보강한 assertion은 잡았다. 원본 출력은 `.artifacts/verification/mutation-scope-manual-{strengthened,original}.log`에 있다.

임시 사본과 tar 파일은 비교 결과를 기록한 뒤 삭제했다. 작업 트리의 production source는 수동 mutant 확인 중에도 변경하지 않았다.

| 수용 시나리오 | 요구사항 | 실행·시험 위치 | 확인 결과 |
| --- | --- | --- | --- |
| Variable byte integer 폭과 전체 packet 크기의 8개 경계값 | TEST-12 | `internal/domain/mqtt/packet_test.go::TestRemainingLengthFramingHelperEdges_MQTT153_1_2_MQTT154_1_2` | 원본에서 PASS. 사본의 `<= 128` mutant는 보강 시험에서 FAIL, 기존 시험에서 PASS |
| CI의 5개 mutation command와 Local의 mutation 제외 | TEST-12 | `tools/verify/runner_test.go::TestRunCIRequiresBaseAndPropagatesBaseToCoverage`, `TestRunLocalOmitsMutationChecksAndCoverageWithoutBase` | 두 시험 PASS. 실제 CI runner도 5개 package를 병렬 검사하고 PASS |
| Package별 efficacy·mutant coverage 80% 하한 | TEST-12 | §7.2의 Gremlins 명령과 `tools/verify/runner.go::runVerification` | 순차 측정과 CI runner의 재측정 모두 각 package 통과. LIVED·timeout 0개 |
| 기존 traceability 유지 | TEST-10 | `go run ./tools/verify traceability` | PASS, P0 66/81 연결, 예외 15개 |

모든 명령은 저장소 루트에서 실행했다. 수동 mutant 시험 두 개만 임시 사본에서 실행했다. CI runner는 로컬 Windows 환경에서 첫 시도에 통과했다. 시험을 건너뛰거나 timeout을 늘리지 않았다. 원격 CI 실행 또는 필수 status 확인 결과는 아니다.

| 실행 명령 | 결과와 관찰 | Wall time |
| --- | --- | --- |
| `go build ./internal/domain/mqtt` | PASS, exit 0 | 0.447s |
| `go test ./internal/domain/mqtt -run '^TestRemainingLengthFramingHelperEdges' -count=1` | PASS, exit 0 | 1.698s |
| `go test ./internal/domain/mqtt -count=1` | PASS, exit 0 | 1.244s |
| `go build ./tools/verify` | PASS, exit 0 | 2.466s |
| `go test ./tools/verify -run '^TestRunCIRequiresBaseAndPropagatesBaseToCoverage$\|^TestRunLocalOmitsMutationChecksAndCoverageWithoutBase$' -count=1` | PASS, exit 0 | 1.735s |
| `go test ./tools/verify -count=1` | PASS, exit 0 | 7.620s |
| `go build ./internal/domain/mqtt ./tools/verify` | PASS, exit 0 | 2.065s |
| `go test ./internal/domain/mqtt ./tools/verify -count=1` | PASS, exit 0 | 7.456s |
| 위 경계 시험 명령, 임시 사본의 `<= 128` mutant와 보강 assertion | 기대한 FAIL, exit 1. `variableByteIntegerSize(128) = 1, want 2` | 1.956s |
| 같은 mutant와 기존 assertion | PASS, exit 0. 기존 시험의 검출 부족 확인 | 1.765s |
| §7.2 `delivery` mutation 명령 | PASS, exit 0 | 209.599s |
| §7.2 `mqtt` mutation 명령 | PASS, exit 0 | 958.839s |
| §7.2 `nodedrain` mutation 명령 | PASS, exit 0 | 132.628s |
| §7.2 `security` mutation 명령 | PASS, exit 0 | 91.381s |
| §7.2 `session` mutation 명령 | PASS, exit 0 | 136.568s |
| `go run ./tools/verify ci --base HEAD` | PASS, exit 0. Branch instrumentation·vet·traceability·architecture·전체 package/integration 시험·production race·fuzz 5개 각각 10,000회·mutation 5개·coverage 통과. 최종 출력 `ci: PASS` | 1450.060s |
| `go run ./tools/verify traceability` | PASS, exit 0. `P0 mapped 66/81, exceptions 15` | 1.017s |

과거 순차 mutation JSON은 `.artifacts/verification/mutation-scope-{delivery,mqtt,nodedrain,security,session}.json`에 있다. 과거 CI runner도 `.artifacts/verification/mutation-{delivery,mqtt,nodedrain,security,session}.json`에 결과를 기록했지만, 현재 수동 command가 같은 경로를 재사용한다. 따라서 해당 경로의 JSON은 latest manual execution을 나타내며 과거 CI JSON의 보존본이 아니다. 당시 원본 출력은 `.artifacts/verification/mutation-scope-<name>.log`, 순차 command의 exit code와 wall time은 `.artifacts/verification/mutation-scope-results.json`에 기록했다. `name`은 `boundary-test`, `runner-tests`, `manual-strengthened`, `manual-original`, `mutation-<package>`, `ci`, `traceability`와 단계별 `stepN-build`·`stepN-package`·`step3-packages`, `manual-archive`다. `.artifacts/`는 Git 추적 대상이 아니다.

2026-10-09 실행 당시 JSON의 `elapsed_time`은 전체 command wall time과 달랐다. 당시 MQTT 순차 결과는 948.8832532초, CI 결과는 1414.9420929초였다. 전체 wall time은 각각 958.839초와 1450.060초다. 현재 manual command는 CI가 사용한 `mutation-<package>.json` 경로를 재사용하므로 해당 파일은 최신 수동 실행 결과를 담는다. 이 파일을 과거 CI JSON의 보존본으로 취급하지 않는다.

현재 HTML 시각 자료는 Chromium desktop·mobile 화면에서 확인했다. 수동 command, 다섯 대상 package와 흐름이 보였고 browser error는 없었다. 이번 변경은 verification runner entry flow만 분리했으며 domain production code는 바꾸지 않았다. 과거 실행 수치와 결과는 해당 시점의 증거로 유지한다.

당시 정책에서는 원격 필수 CI 실행 증거가 없어 TEST-12를 `미검증`으로 기록했다. 이 기록은 기존 CI mutation 정책의 이력이다. 현재 manual-only 정책과 실행 결과는 §8.8에 기록한다. 이번 변경은 `runVerification`에서 mutation을 제거하고 별도 `runMutation` 진입점을 추가했으며 domain production code는 수정하지 않았다.

### 8.8 수동 mutation 검증 수용 증거

2026-10-11, Windows/amd64, Go 1.26.5에서 저장소 루트에서 `go run ./tools/verify mutation`을 실행했다. 명령은 exit 0과 `mutation: PASS`를 반환했다. Wall time은 1464.90초(24분 24.90초)였다. 수동 runner는 다섯 package를 각각 Gremlins `workers=1`, `test-cpu=1`로 검사했다.

| Package | Killed | Lived | Not covered | Timed out | Efficacy | Mutant coverage |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `internal/domain/delivery` | 26 | 0 | 0 | 1 | 100% | 100% |
| `internal/domain/mqtt` | 515 | 0 | 6 | 1 | 100% | 98.85% |
| `internal/domain/nodedrain` | 63 | 0 | 0 | 1 | 100% | 100% |
| `internal/domain/security` | 90 | 0 | 0 | 2 | 100% | 100% |
| `internal/domain/session` | 56 | 0 | 0 | 1 | 100% | 100% |

다섯 package 모두 80% 기준을 넘었다. 전체 결과는 750 Killed, 0 Lived, 6 Not covered, 6 Timed out이다. Timed out은 Killed로 세지 않는다. MQTT의 6 Not covered는 `framing.go`의 경계 mutant다. `Not viable`과 `Skipped`는 모두 0개였다. 각 package JSON은 `.artifacts/verification/mutation-<package>.json`에 기록했다. 이 경로는 다음 수동 실행 때 덮어쓴다.

| 수용 시나리오 | 요구사항 | 확인 방법 | 결과 |
| --- | --- | --- | --- |
| 수동 command가 다섯 package를 worker 1개씩 선택 | TEST-12 | `tools/verify/runner_test.go::TestRunMutationExecutesAllPackagesWithOneGremlinsWorker`; `go run ./tools/verify mutation` | test PASS; 실제 command PASS |
| local·CI 자동 gate에서 mutation 제외 | TEST-12 | `tools/verify/runner_test.go::TestRunLocalOmitsMutationChecksAndCoverageWithoutBase`, `TestRunCIRequiresBaseAndPropagatesBaseToCoverage`; `go run ./tools/verify ci --base HEAD` | 두 test PASS; 자동 CI runner PASS, mutation 단계 없음 |

2026-10-11에 `go run ./tools/verify ci --base HEAD`를 로컬에서 실행했다. `git merge-base HEAD main`은 `HEAD`를 반환했다. Runner는 `coverage: PASS`, `ci: PASS`를 출력했고 wall time은 175.09초(2분 55.09초)였다. 이 명령은 로컬 CI runner 검증이며 원격 필수 status 실행은 아니다. Mutation output은 없었다.

먼저 과거 실행에서 사용한 `--base 23005e1a0d819a357507ce173b8d0f89cf40cf19`로 실행했을 때는 diff line 88.74%, branch 82.41%로 90% 기준에 미달해 exit 1이었다. 이 base는 현재 `main`의 merge base가 아니며, 비교 범위에 과거 Go 변경 43개가 들어 있었다. 실제 merge base인 `HEAD`로 다시 실행한 결과가 위 PASS다.

기존 mutation 포함 CI 실행은 §7.2 표에서 1388.95초(23분 08.95초)였다. 이번 local CI runner 실행은 175.09초(2분 55.09초)다. 관측 시간 차이는 1213.86초(약 87%)다. 두 실행의 `--base`가 달라 정식 동일 조건 성능 비교가 아니라 참고 수치다. 세부 mutation 점수와 timeout은 위 표에 있다.

자동 실행의 예상 시간을 늘리는 mutation 단계는 `local`·`ci`에서 제외했다. 검출력 검토가 필요한 변경에서는 저장소 루트에서 `go run ./tools/verify mutation`을 수동 실행한다. 이전 CI 정책의 시간 비교는 §7.2, 기존 범위 확장 기록은 §8.7에 있다.

HTML 시각 자료는 Chromium desktop·mobile 화면에서 확인했다. Console error는 없었다. 이번 변경은 verification runner와 문서만 수정했고 domain production code는 바꾸지 않았다.

### 8.9 GitHub Actions 원격 CI와 required check 증거

2026-10-11에 `.github/workflows/verify.yml`을 `main`에 push하고 원격 실행을 확인했다. Workflow는 pull request, `main` push와 `workflow_dispatch`에서 `go run ./tools/verify ci --base <base>`를 실행한다. PR은 `origin/$BASE_REF`를, main push는 push 이전 commit을 diff base로 쓴다. Coverage profile, summary와 branch manifest는 30일 보관한다. Mutation은 자동 gate에서 제외하고 `go run ./tools/verify mutation`으로 별도 실행한다.

| 실행 | Commit·환경 | 확인 결과 |
| --- | --- | --- |
| Remote main | `03c84f40c345195f0a323c5af2e623e9ebbbc998`; GitHub Actions | [Run 38112386284](https://github.com/kayden-kim/narumq/actions/runs/38112386284)의 `verify` check가 5분 49초 뒤 `success`였다. `verification-38112386284-1` artifact는 9,979,845 bytes이며 2026-11-10까지 보관한다. |
| Full Linux CI 사전 실행 | `03c84f40c345195f0a323c5af2e623e9ebbbc998`; Go 1.26.5 Docker container; `--base f847a491e7b4a68d7fc48a868ad801895894d32a` | `go run ./tools/verify ci --base f847a491e7b4a68d7fc48a868ad801895894d32a`가 `coverage: PASS`, `ci: PASS`로 종료했다. Wall time은 696.35초다. Diff coverage는 line 24/25(96%), function 5/5(100%), branch 11/12(91.67%)였다. 로그는 `.artifacts/verification/ci-linux-one-shot-admission-close.log`에 있다. |

성공한 main run은 기존 `actions/checkout@v4`, `actions/setup-go@v5`, `actions/upload-artifact@v4`와 `ubuntu-latest`를 사용했다. GitHub는 Node 20 deprecation과 `ubuntu-latest`의 Ubuntu 26 전환 예정 경고를 표시했다. 현재 workflow는 `ubuntu-24.04`와 public action manifest에서 `runs.using: node24`로 확인한 `actions/*@v7`을 사용한다. 이 변경은 문서 PR [#2](https://github.com/kayden-kim/narumq/pull/2)의 [verify run 38113714789](https://github.com/kayden-kim/narumq/actions/runs/38113714789)에서 4분 59초 뒤 PASS했다. 해당 run에 Node 20·Ubuntu 26 경고는 없었다. Run artifact `verification-38113714789-1`도 30일 보관한다.

초기 원격 실행에서 서로 다른 세 회귀가 발견됐다. 테스트를 끄거나 대기를 늘리지 않고 연결 종료와 inbound slot 반환 경계를 수정했다.

| Run | 실패 관찰 | 수정 |
| --- | --- | --- |
| [38107031157](https://github.com/kayden-kim/narumq/actions/runs/38107031157) | BP-05 reconnect 시험에서 두 시도 중 하나만 성공했다. | `f847a49`에서 client가 peer close를 확인한 뒤 reconnect하도록 했다. `go test ./tests/benchmark/bp05 -count=25`가 통과했다. |
| [38108358932](https://github.com/kayden-kim/narumq/actions/runs/38108358932) | Bootstrap probe의 후속 TLS handshake가 `EOF`로 실패했다. [추론] 직전 응답 뒤 client가 바로 반환하면서 server의 inbound slot 반환과 다음 연결이 경합했다. | `f7b24da`에서 client가 peer close를 기다리고, server는 slot을 반환한 뒤 connection을 닫도록 했다. 관련 시험을 race detector 아래 100회 반복해 통과했다. |
| [38111263856](https://github.com/kayden-kim/narumq/actions/runs/38111263856) | `TestDynamicSeedAdmissionRequiresTLSBoundCommittedStage`의 순차 seed admission 요청이 연결되지 못했다. | `03c84f4`에서 `ProbeBootstrapPeer`와 `RequestSeedAdmission`이 공통 close helper로 peer 종료를 기다리게 했다. Server는 close 전에 inbound slot을 반환한다. 해당 시험은 Linux race detector 아래 50회, Windows 아래 10회 통과했다. |

### Main branch protection

`main` 보호 설정은 GitHub Branch Protection API로 적용한 뒤 다시 조회했다.

| 설정 | 조회 결과 |
| --- | --- |
| 필수 status check | `verify` |
| Strict status check | `true`; 최신 `main` 반영 필요 |
| Required review | 없음 |
| Admin enforcement | `true` (2026-10-11 [ADR-0056](../decisions/ADR-0056-branch-protection-for-admins.md) 적용); 관리자에게도 동일하게 보호 적용 |
| Force push·branch deletion | 둘 다 금지 |
| Push restriction | 없음 |

### 실패 PR probe (초기 관찰)

초기 임시 PR [#1](https://github.com/kayden-kim/narumq/pull/1)은 `tools/verify/branch_protection_probe_test.go::TestBranchProtectionProbe`에서 의도적으로 실패했다. [Run 38112802413](https://github.com/kayden-kim/narumq/actions/runs/38112802413)에서 필수 `verify` check가 `failure`였고 PR API는 `mergeable_state=blocked`를 반환했다. 이 조회 뒤 PR을 닫고 probe branch를 삭제했다. 시험 파일은 `main`에 남지 않았다. 초기 실행 시점에는 `enforce_admins=false`였고 실제 merge API 호출은 검증하지 않았다.

### TEST-10 merge 거절 probe

2026-10-11에 [ADR-0056](../decisions/ADR-0056-branch-protection-for-admins.md)에 따라 `enforce_admins=true`를 영구 설정하고, PRD TEST-10의 네 조건(test 실패, coverage 저하, 미매핑 P0, 미매핑 normative statement)마다 별도 probe PR을 열어 CI 실패와 관리자 merge API 거절(HTTP 405)을 검증했다.

| Probe | 주입 내용 | PR | Run ID | CI 실패 문자열 | `mergeable_state` | Merge API 결과 |
| --- | --- | --- | --- | --- | --- | --- |
| `test-failure` | `tools/verify/branch_protection_probe_test.go`에 의도적 실패 테스트 주입 | [#3](https://github.com/kayden-kim/narumq/pull/3) | [38125505236](https://github.com/kayden-kim/narumq/actions/runs/38125505236) | `--- FAIL: TestBranchProtectionProbe`, `TEST-10 probe: intentional test failure` | `blocked` | HTTP 405 (`Required status check "verify" is failing.`) |
| `coverage-drop` | `internal/domain/mqtt/branch_protection_probe.go`에 테스트 없는 함수 주입 | [#4](https://github.com/kayden-kim/narumq/pull/4) | [38125509535](https://github.com/kayden-kim/narumq/actions/runs/38125509535) | `coverage gate failed:`, `narumq/internal/domain/mqtt function ratchet 100.00% is above measured 98.91%`, `diff line coverage 0.00% is below 90.00%`, `diff function coverage 0.00% is below 90.00%` | `blocked` | HTTP 405 (`Required status check "verify" is failing.`) |
| `unmapped-p0` | `docs/requirements/PRD.md`에 테스트/예외 없는 `API-99` P0 행 추가 | [#5](https://github.com/kayden-kim/narumq/pull/5) | [38125513065](https://github.com/kayden-kim/narumq/actions/runs/38125513065) | `traceability: P0 requirement API-99 has no Traceability test and no exception in tests/conformance/p0-requirement-exceptions-v1.json` | `blocked` | HTTP 405 (`Required status check "verify" is failing.`) |
| `unmapped-mqtt-statement` | `tests/conformance/mqtt-compliance-v1.json`에 test 빈 배열인 `MQTT-4.9.0-1` 추가 | [#6](https://github.com/kayden-kim/narumq/pull/6) | [38125516594](https://github.com/kayden-kim/narumq/actions/runs/38125516594) | `traceability: invalid compliance matrix tests/conformance/mqtt-compliance-v1.json: statement[61].tests must not be empty` | `blocked` | HTTP 405 (`Required status check "verify" is failing.`) |

증거 수집 후 네 PR을 모두 닫고 remote branch를 삭제했다. Probe 변경은 `main`에 남지 않았다. 관리자 계정으로 호출한 merge API 거절(HTTP 405)과 호출 후 `merged=false`, `origin/main` SHA 불변(`4c1ab4cc143426411cb5ea9ad1c0d2b501b2b5d4`)을 확인했다.

네 가지 조건의 merge 차단은 모두 증명했다. 그러나 보류된 인프라 범위의 DEP P0 예외 10개가 `tests/conformance/p0-requirement-exceptions-v1.json`에 남아 있으므로, [ADR-0042](../decisions/ADR-0042-p0-requirement-traceability-gate.md) §1.6에 따라 TEST-10은 계속 **미검증**으로 관리한다.

Traceability gate는 `P0 mapped 71/81, exceptions 10`을 보고했다. 보류된 DEP 예외 10개가 남아 있다. CI의 traceability PASS는 허용된 예외 목록과 test 연결이 일치한다는 뜻이며, 81개 P0 모두가 test에 연결됐다는 뜻은 아니다. Full CI는 fuzz와 race 단계를 통과했고 위의 알려진 회귀 경로를 반복 검증했다. 이 결과는 TEST-06~TEST-09, TEST-13~TEST-15의 원격 CI 실행 증거다. Production workload와 release gate는 별도다.

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
