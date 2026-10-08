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

Phase 0은 production workload를 다음 값과 단위로 확정해야 한다. 평균만으로 profile을 만들지 않는다.

| 기호 | 의미 | 필수 분해 요소 |
| --- | --- | --- |
| `B_conn` | 최대 동시 연결 수 | 프로토콜 버전, TLS 여부, 테넌트, keepalive |
| `B_pub` | 발행 속도(초당) | QoS, payload 크기, retained 여부, 영속화 수준 |
| `B_sub` | 수신 속도(초당) | QoS, fan-out 배수, shared 구독 여부 |
| `B_msg_size` | payload 크기 분포 | p50, p95, p99, 최대값 |
| `B_topic` | 토픽·필터 규모 | exact 매칭, wildcard, overlap, fan-out |

또한 persistent session 비율, offline 비율·시간, Session Expiry, reconnect ramp, 인증 backend latency, tenant 부하 편차와 AZ network 특성을 기록한다. 이 값과 SLO를 정하는 `SPEC-TBD-08`은 미확정이다.

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

고정 EMQX version·edition과 NaruMQ 대비 수치 합격선인 `SPEC-TBD-08`은 미정이다. BP-01/BP-04/BP-05/BP-02는 pinned OSS 5.8.8을 사용한다. BP-03/BP-06/BP-07은 pinned Enterprise 5.8.8을 사용한다. EMQX 5.8.8 OSS는 EOL historical comparator이며 현재 지원 release 기준선이나 승인된 commercial 운영 사용을 뜻하지 않는다.

Profile SHA-256은 저장소에 커밋된 LF bytes로 계산한다. `.gitattributes`의 `tests/benchmark/*/testdata/*.json text eol=lf` 규칙으로 `core.autocrlf=true`인 Windows checkout에서도 같은 bytes를 쓴다. 2026-10-08 이전 BP-06 실행 artifact의 `profileSha256` `e8e4aa2e5df1a3e55ad901acff5a4505cdb1acf4699ff061f1a2bfc51754054d`는 같은 내용을 CRLF로 checkout한 bytes의 digest다. 같은 profile의 LF digest는 `ca5e28b04623e708d9521ab34ce5ed3b9dd617d4ac57c4571243bdd6061f6a15`다. 과거 artifact의 값은 실행 당시 사실이므로 고치지 않는다.

| BP | 프로파일 | 스펙 위치 |
| --- | --- | --- |
| BP-01 | Protocol Baseline | §3.1 |
| BP-02 | Production Mix | §3.8 |
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

BP-07은 같은 연결 수를 유지한 3-node cluster에서 node 하나씩 drain하고 같은 volume으로 재시작한다. Retained workload와 최대 traffic에서 연속 100회 rolling은 별도 범위다. 모든 artifact `releaseGating=false`; `SPEC-TBD-08` 미정이다.

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

`SPEC-TBD-08`, Linux/EBS power loss, 실제 NLB·rollout controller와 최대 traffic 연속 100회 rolling은 미완료다. Manifest의 baseline과 `dirty=true`는 실행 당시 상태이며 확인 commit이나 CI rerun을 뜻하지 않는다.

#### 명령과 시험

```text
go run ./tests/benchmark/bp07 compare --profile tests/benchmark/bp07/testdata/smoke.json
go run ./tests/benchmark/bp07 suite --profile tests/benchmark/bp07/testdata/smoke.json
```

Profile·dialer·session·QoS·marker round attribution·Will validity·drain state·EMQX CLI·resource·suite 검증은 실제 파일 `tests/benchmark/bp07/{profile,dialer,session,aggregate,rolling,resource,suite}_test.go`에 있다. MQTT packet 경계는 `tests/benchmark/internal/mqttclient/client_test.go`, published owner port·restart·stats는 `tests/benchmark/internal/environment/cluster_test.go`에서 확인한다. Broker-side proof·projection/FIFO/cancellation acceptance는 `cmd/narumq`, `internal/adapters/{raft,tcp}`, `tests/integration`의 시험에 연결된다. Local gate PASS는 invalid workload나 미완료 production gate를 대체하지 않는다.

### 3.6 BP-06 Failure Recovery smoke 관찰 경계

BP-06은 3-node cluster에서 broker 하나를 drain 없이 `SIGKILL`해 N-1을 유지한 뒤 같은 container·volume으로 재시작한다. Artifact는 모두 `releaseGating=false`; `SPEC-TBD-08`은 미정이다.

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

BP-02는 QoS 0/1/2, payload size cycle, 여러 subscription 종류, retained state, persistent session, tenant, TLS를 섞은 고정 workload다. 하네스와 local 실행만 증명한다. 고정 tenant·stream·TLS 비율은 production 분포가 아니다. `B_*`, `SPEC-TBD-08`, production workload, N/N-1 SLO, release threshold와 release acceptance는 미정이다. Artifact는 모두 `releaseGating=false`다.

#### Topology와 비교 경계

두 target은 3-node다. NaruMQ는 `narumq-bp02` dynamic cluster, EMQX는 공통 `EMQXImageReference`의 pinned OSS 5.8.8을 쓴다. Broker limit은 node마다 1 CPU/1 GiB, generator는 2 CPU/512 MiB다. MQTT 5와 RS256 JWKS JWT를 쓰며 TCP listener는 1883, server-auth TLS listener는 1.2+의 8883이다. NaruMQ node 간에는 peer mTLS를 쓴다. Tenant `bp02-a`, `bp02-b`는 각각 `bp02/a/#`, `bp02/b/#` ACL을 쓴다. NaruMQ는 `(iss,sub)`를 tenant와 allowed client ID에 연결한다. EMQX는 issuer/audience/subject를 username과 확인하고 username 기준 file ACL을 쓴다. Persistent session은 첫 연결 `CleanStart=true`, 재연결 `CleanStart=false`, expiry 300초다. 나머지는 clean session, expiry 0이다.

`Equivalent`에는 profile SHA, CPU/memory limit, runner image ID와 3개 node가 일치해야 한다. 이는 구현 동등성을 뜻하지 않는다.

| `nonEquivalent` dimension | 비교 차이 |
| --- | --- |
| `sessionDurability` | NaruMQ는 persistent session·subscription·QoS1/2 inflight·offline message를 Raft quorum에 commit한다. EMQX OSS 5.8.8은 memory session을 쓴다. Offline 결과는 durability 비교가 아니다. |
| `tenantIsolation` | NaruMQ의 JWT tenant/client-ID binding과 EMQX username ACL은 다르며, 동일 raw topic을 tenant 사이에서 시험하지 않는다. |
| `authentication` | 양쪽 모두 같은 JWKS의 RS256을 검증하지만 EMQX는 client-ID binding과 peer mTLS를 재현하지 않는다. |
| `tlsListener` | 같은 CA를 쓰지만 NaruMQ는 node별 peer certificate, EMQX는 3-SAN server certificate를 공유한다. Cipher와 resumption은 구현별이다. |
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

Manifest의 baseline `6c200085368d701f34dedfb31e1a968a84c09028`, `dirty=true`, profile SHA `b929efabd5f99e68758bec86a2034b0b80d153e9ce0291e64a8236b3e4afd7d4`는 실행 당시 사실이다. Profile 원문은 artifact에 연결된다. `B_*`, production workload/storage, N/N-1 SLO, `SPEC-TBD-08`, release regression과 release acceptance는 미정이다. EMQX JWT placeholder와 SSL environment fallback은 사용하지 않았다.

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

각 run ID는 Git 커밋, NaruMQ 및 Go 버전, EMQX 버전·digest·에디션·라이선스·설정, 배포 프로파일·지원 상태·토폴로지·스토리지를 연결한다. 해당할 경우 AWS/EKS/EC2/EBS CSI/NLB의 해석된 설정도 기록한다. 제너레이터·클라이언트 SDK·seed·명령어, 확정 프로파일, 단계 경계·반복·타임스탬프, 원본 표본·로그·지표 및 오류·유실·중복 결과를 보존한다. 보안 비밀 값은 저장하지 않고 식별 참조만 남긴다.

- **BP-01**: 프로파일 및 SHA, 확정 설정, `samples.jsonl`, NaruMQ runtime 지표, workload summary, 브로커 통계·로그, 불변 이미지 메타데이터를 보존한다.
- **BP-02**: `workload-summary.json`, `resource-summary.json`, `phases.jsonl`, `messages.jsonl`, `retained.jsonl`, `connections.jsonl`, 브로커 통계·로그·이미지 정보를 보존한다.
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

모든 릴리스는 이전 안정 버전 NaruMQ 및 승인된 고정 EMQX 기준선과 대조해야 한다. 처리량이나 p99 latency가 5% 이상 악화하면 릴리스를 차단한다. 측정 변동성이 5%를 초과하면 합격·불합격을 성급히 판정하지 않고 원인을 조사하며 반복 횟수를 늘린다.

Phase 0은 프로파일별 최소 처리량, 최대 p99/p99.9 latency, 연결당 메모리, N-1 상태 SLO, 복구 RTO 수치를 확정해야 한다. 수치가 누락된 항목이 있으면 성능 게이트를 열린 상태로 둔다.

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

### 7.2 Phase 0 mutation·fuzz 게이트

표준 실행 명령:

```text
go run ./tools/verify local
go run ./tools/verify ci --base <merge-base>
```

두 명령은 gofmt 사전 점검 뒤 `.artifacts/verification/branch/module`의 계측한 사본에서 atomic coverage를 포함한 전체 `go test ./...`를 한 번 실행한다. Local parallel limit은 8, CI는 2다. 독립 명령은 병렬 실행하며 하나가 실패하면 나머지를 취소한다. CI는 `internal/domain/delivery`, `internal/domain/nodedrain`, `internal/domain/session`에 각각 mutation을 추가한다. 정책은 `workers=1`, `test-cpu=1`, efficacy 80%, mutant coverage 80%이며 source/file/mutant exclusion은 없다. 한 package 결과로 다른 package 실패를 상쇄하지 않는다.

```text
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-delivery.json internal/domain/delivery
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-nodedrain.json internal/domain/nodedrain
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-session.json internal/domain/session
```

`efficacy = KILLED/(KILLED+LIVED)*100`; `mutant coverage=(KILLED+LIVED)/(KILLED+LIVED+NOT_COVERED)*100`이다. 두 값 모두 package마다 ≥80%여야 한다. 관찰 가능한 `LIVED`는 assertion 부족을 뜻하므로 정상·오류·경계 전이 시험을 보강한다.

Fuzz target 5개는 별도 process로 병렬 실행하며 각각 `-parallel=1 -fuzztime=10000x`를 쓴다.

```text
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzDecoder$ -fuzztime=10000x -parallel=1
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzCodecRoundTrip$ -fuzztime=10000x -parallel=1
go test ./internal/domain/routing -run=^$ -fuzz=^FuzzTopicMatching$ -fuzztime=10000x -parallel=1
go test ./internal/domain/session -run=^$ -fuzz=^FuzzLifecycleTransitions$ -fuzztime=10000x -parallel=1
go test ./internal/adapters/tcp -run=^$ -fuzz=^FuzzConnectionInput$ -fuzztime=10000x -parallel=1
```

Discovery 순서는 비결정적이다. 실패 corpus는 `testdata/fuzz/<Target>/`에 보존해 일반 시험과 다음 campaign에서 재현한다. 수정한 feature는 재현 시험을 포함해야 한다. Flaky isolation은 임시 조치이며 owner와 해결 기한을 정한다.

MQTT normative statement 연결표는 `tests/conformance/mqtt-compliance-v1.json`이다. `go run ./tools/verify traceability`는 source comment와 matrix를 양방향 비교한다. 지원 동작 link 누락이나 requirement 집합 불일치는 실패다.

### 7.3 Cluster failpoint 빌드

Membership crash-boundary와 durable-ACK acceptance 시험은 `narumq_failpoint` build tag를 사용한다. 이 build만 `NARUMQ_FAILPOINT`, `NARUMQ_FAILPOINT_CONTROL`을 읽는다. 기본 build는 두 값을 무시하며 이 설정으로 process를 종료하거나 peer를 격리하지 않는다. 일치하는 crash point는 `narumq failpoint reached: <point>`를 출력하고 exit 86으로 종료한다.

허용 crash point: `add-learner-before-persist`, `add-learner-after-persist`, `add-learner-after-apply`, `promote-voter-before-persist`, `promote-voter-after-persist`, `promote-voter-after-apply`, `remove-member-before-persist`, `remove-member-after-persist`, `remove-member-after-apply`, `snapshot-install-before-persist`, `snapshot-install-after-persist`, `begin-remove-after-apply`, `route-projection-rebuild-error`, `ownership-acquire-after-commit`, `offline-detach-before-commit`, `offline-detach-after-commit`, `delivery-wal-before-write`, `delivery-wal-before-sync`, `delivery-wal-after-sync`, `delivery-commit-before-apply`, `delivery-commit-after-apply`.

Tagged runtime은 JSON control file을 동기식으로 읽는다. 예: `{"crash":"delivery-wal-before-sync","isolatePeers":[2],"holdCommands":["subscription"]}`. `isolatePeers`는 inbound/outbound traffic 모두에 적용한다. 파일을 비우거나 제거하면 해제한다. 잘못된 JSON/value는 log를 남기고 해당 read를 비활성화한다. Hold 종류는 `ownership`, `subscription`, `delivery`다. 일치한 command는 read barrier/profile 확인 뒤, encoding/proposal 전에 멈춘다. 해제될 때까지 poll하고 60초 뒤 경고 후 진행한다. Proposal은 `WithoutCancel`에서 만든 10초 context로 실행한다. 결과는 applied/conflict/error다. Test 전용 기능이다.

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

- 전체 production code 목표: line ≥85%, function ≥90%, branch 정책 ≥85%.
- 핵심 domain 9개(`mqtt`, `session`, `routing`, `delivery`, `tenancy`, `security`, `nodedrain`, `release`, `connrebalance`)는 line/function ≥95%, branch ≥90%다. `application/membership`은 핵심 domain이 아니며 전체 기준을 따른다. 정책은 `tests/verification/coverage-policy.json`이 source of truth다.
- CI의 변경 production code는 line/function/branch diff coverage ≥95%를 충족해야 한다. Generated exclusion은 `cmd/`·`internal/` 아래의 `.pb.go`와 표준 generated marker에만 적용한다. 알 수 없는 coverage file은 실패다.
- `delivery`, `session`, `nodedrain`은 mutation efficacy와 mutant coverage가 각각 ≥80%여야 한다. 동시성 코드는 race detector 또는 동등한 검증을 통과한다.
- Domain 규칙/state machine은 정상·오류·경계·전이 assertion coverage 100%를 기본으로 한다. Consensus·drain·infrastructure 검증은 integration/E2E/chaos에 연결한다.

`verify local`과 `verify ci`는 gofmt, vet, traceability, architecture, package/integration coverage, production race, fuzz 5종과 coverage policy를 실행한다. Local parallelism은 8, CI는 2이며 CI는 여기에 mutation gate 3개를 더한다. CI의 `--base <merge-base>`는 coverage diff 계산에만 쓴다. Atomic profile은 `.artifacts/verification/coverage.out`, mutation 결과는 `.artifacts/verification/mutation-<package>.json`이다. Integration은 build tag별 일반·failpoint broker를 임시 경로에 한 번씩 build하고 child process 정리 뒤 제거한다. Race 범위는 `./cmd/... ./internal/...`이며 별도 build되는 integration child broker는 race-instrumented가 아니다.

Coverage는 Go atomic coverprofile의 executable block을 집계한다. [ADR-0040](../decisions/ADR-0040-branch-coverage-arm-injection.md)에 따라 branch는 `if`의 then·else와 clause가 있는 switch·type switch·select의 각 case·default arm을 센다. Loop 조건, 단락 평가와 `func _` body는 제외한다. 빠진 else·switch default만 줄바꿈 없이 사본에 주입한다. Select default는 주입하지 않는다. 사본에서 전체 시험을 실행하고 manifest 좌표와 원본 SHA-256을 대조한다. Line coverage로 branch를 대체하지 않는다. 실행 가능한 변경 line이나 변경 결정점이 없으면 해당 diff coverage는 100%다.

현재 gate는 local integration 8-way, CI 2-way와 위 9개 core domain을 쓴다. Final ratchet은 overall production과 core별 threshold를 `coverage-policy.json`에 둔다. Branch는 두 번 측정한 값 중 작은 값을 소수점 둘째 자리에서 내림한 baseline ratchet과 diff 95%를 강제한다. PRD 전체 85%·core 90% 미달은 report의 `branchCoverage.requirementShortfalls`로 보고한다. 이를 ratchet gate 실패 사유로 쓰지 않으며, 미달이 남는 동안 TEST-06·TEST-07은 미검증으로 유지한다. Ratchet 변경에는 반복 안정성 증거와 정책/요구사항 갱신이 필요하다. Coverage 숫자만으로는 충분하지 않으며 모든 P0 requirement와 지원 MQTT normative statement에 assertion이 연결되어야 한다.

### 8.1 Verification 실행 정책

Repository root에서 전체 local·CI gate는 다음 명령으로 실행한다.

```text
go run ./tools/verify local
go run ./tools/verify ci --base <merge-base>
```

검증 순서, cancellation, artifact 경로는 runner가 소유한다. 부분적인 과거 명령 목록으로 전체 gate를 대체하지 않는다.

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

증거는 `.artifacts/verification/coverage-summary-run-{0,a,b,c}.json`, `branch/manifest.json`, `coverage.out`, `coverage-summary-ratchet-rejection.json`, `coverage-summary-delivery-100.json`, `coverage-summary-diff-02ef51d.json`, `branch-gate-execution-evidence.json`에 보존한다. Run C의 `requirementShortfalls`는 전체 branch 79.85%가 PRD 85%보다 낮다는 한 항목이다. Core 9개는 모두 local branch 90% 이상이다. TEST-06의 PRD floor 충족과 TEST-06·TEST-07의 필수 CI 증거는 미검증으로 남는다. CI mutation gate와 CI 플랫폼 status·TEST-10 P0 매핑은 이번 작업에서 실행하거나 완료하지 않았다.

출력 경로 안전성 보완 뒤 실제 `branch-instrument --output .`와 `--output .artifacts/verification`은 각각 exit 1로 삭제 전에 거절했다. 저장소 branch subtree에 외부 임시 directory로 향하는 Windows junction을 만들고 그 하위를 출력으로 지정한 CLI도 exit 1로 거절했다. 외부 sentinel 파일은 그대로 남았고 임시 junction은 제거했다. 기본 `branch-instrument`는 PASS였다. 기존 Run C profile을 새 manifest로 다시 평가한 `coverage`도 PASS였으며 측정값은 같았다. 안전성 회귀 시험은 skip 없이 통과했다. 이 추가 증거는 `.artifacts/verification/branch-output-safety-evidence.json`과 `coverage-summary-after-safety-review.json`에 보존한다. 전체 local gate의 Run A·B·C는 이 보완 전 실행이며, 보완 뒤에는 도구 package 전체 시험과 위 CLI 경로를 검증했다.

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
3. 커버리지·Race 검사, 5개 Fuzz 캠페인, delivery·nodedrain·session 3개 패키지의 mutation 검사(각 지표 ≥80%)를 모두 통과해야 한다.
4. 기능이 완료된 단계마다 BP-01~BP-07 벤치마크를 실행하고 원본 artifact를 보존해야 한다.
5. 승인된 운영 profile 부하에서 3노드 정상 상태 및 N-1 상태의 SLO를 충족해야 한다.
6. 단일 노드 강제 종료(kill) 시 durable ACK 완료 메시지의 유실(gap)이 0건이어야 한다.
7. `EKS_EC2_HA` 검증으로 EC2·AZ·EBS가 분리된 3개 Pod의 단일 AZ 장애 및 NLB 등록 해제를 검증해야 한다.
8. 100회 연속 롤링 업데이트에서 영속 세션 소실 0건, 지원 클라이언트의 비정상 Will 발행 0건이어야 한다.
9. 테넌트 간 격리와 관리 API의 RBAC 침투 테스트를 통과해야 한다.
10. 운영 유사 환경에서 백업·복구, 롤백, 메트릭·경보·대시보드 동작을 확인해야 한다.
11. MQTT 3.1.0 프로토콜 식별 목록과 마이그레이션·격리 증거가 존재해야 한다.

릴리스 필수 소스 artifact는 `.artifacts/verification/coverage.out`, `coverage-summary.json`, mutation 결과 JSON 3개, 보존된 fuzz 결함 코퍼스다. 누락된 artifact나 미검증 게이트가 있으면 완료로 간주하지 않는다. Smoke 결과의 `releaseGating=false`와 미확정된 `SPEC-TBD-08` 운영 기준치는 계속 열어 둔다.
