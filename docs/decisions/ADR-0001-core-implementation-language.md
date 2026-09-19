# ADR-0001: NaruMQ broker core를 Go로 구현한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-08-16 |
| 결정자 | NaruMQ 프로젝트 팀 |
| 관련 문서 | [NaruMQ 제품 요구사항 정의서](../requirements/PRD.md), [시스템 스펙](../specs/README.md), [ADR catalog](README.md), [Contract catalog](../contracts/README.md) |
| 관련 Architecture | [Broker core](../architectures/broker-core.md), [Project structure](../architectures/project-structure.md), [System overview](../architectures/system-overview.md) |
| 관련 Convention | [Go coding conventions](../conventions/go-coding-conventions.md) |
| 관련 요구사항 | NFR-11~NFR-15, TEST-01~TEST-17, PERF-01~PERF-12 |
| 관련 해설 | [Go 동시성과 backpressure](../explainers/go-concurrency-and-backpressure.md), [Broker core의 계층 경계](../explainers/broker-core-boundaries.md) |

## 1. 결정

NaruMQ의 MQTT 데이터 경로와 클러스터 core는 Go로 구현한다. 프로젝트 시작 버전은 Go 1.26 계열의 최신 보안 패치 버전으로 고정한다.

Go 구현 범위에는 다음 구성요소가 포함된다.

- MQTT 3.1.1/5.0 parser와 encoder
- QoS 0/1/2, session, retained message와 Will 상태 머신
- topic routing과 shared subscription
- session ownership, fencing과 replicated delivery
- 노드 간 통신과 consensus 연동
- drain, takeover와 rolling update lifecycle
- 관리 API, Metric과 Kubernetes 연동

Dashboard는 broker core와 분리하며 TypeScript를 사용할 수 있다. Dashboard의 언어와 웹 프레임워크는 이 ADR의 결정 범위가 아니다.

저장소, consensus 라이브러리, 노드 간 RPC, 관리 API 프레임워크와 Dashboard 프레임워크는 별도 ADR에서 결정한다. 특정 제품의 자료형이나 API가 MQTT domain으로 전파되어서는 안 된다.

## 2. 배경

NaruMQ는 Kubernetes에서 동작하는 고가용성 MQTT 클러스터다. 일반적인 요청·응답 서비스와 달리 다음 특성이 언어 선택을 좌우한다.

- 많은 장기 TCP/TLS 연결
- 연결마다 독립적인 MQTT와 QoS 상태
- persistent session과 미전달 메시지 복제
- quorum, fencing, partition과 node failure 처리
- rolling update 중 제한된 속도의 재접속과 session takeover
- parser fuzzing, race detection과 높은 branch coverage
- 실제 EMQX와 동일 조건의 성능 비교

EMQX가 Erlang/OTP를 사용하는 것은 중요한 참고 사례다. Erlang의 경량 process와 supervision은 이 문제에 잘 맞는다. 그러나 EMQX의 고가용성과 확장성은 Erlang만으로 제공되지 않는다. EMQX도 Mnesia full-mesh 구조의 한계를 해결하기 위해 Mria, RLOG, Core/Replicant, Ekka와 별도의 routing·replication 구조를 만들었다.

따라서 NaruMQ의 가용성은 언어보다 session ownership, durable ACK, replicated state, fencing과 drain 설계에 의해 결정된다.

## 3. 결정 요인

우선순위가 높은 결정 요인은 다음과 같다.

1. 대규모 network concurrency를 안정적으로 처리할 수 있어야 한다.
2. MQTT와 분산 상태 머신을 결정적인 unit test로 검증할 수 있어야 한다.
3. fuzzing, race detection, coverage와 profiling을 CI와 운영 환경에서 사용할 수 있어야 한다.
4. Kubernetes, Prometheus와 RPC 생태계가 성숙해야 한다.
5. 단일 팀이 broker를 장기간 유지보수할 수 있어야 한다.
6. 단일 바이너리와 작은 container image로 배포할 수 있어야 한다.
7. storage와 consensus 구현을 domain에서 분리할 수 있어야 한다.

## 4. 선택 근거

### 4.1 Network concurrency와 구현 복잡도의 균형

Go의 goroutine과 network runtime은 많은 동시 연결을 비교적 단순한 코드로 표현할 수 있다. Rust보다 개발과 review 부담이 낮고, Erlang/OTP보다 일반적인 Kubernetes 플랫폼 팀이 운영하기 쉽다.

다만 goroutine 수만 늘리는 것을 확장성으로 간주하지 않는다. connection reader/writer, session owner, routing shard, storage writer와 consensus apply loop는 다음 규칙을 따라야 한다.

- queue와 channel은 크기를 제한한다.
- timeout, cancellation과 종료 순서를 명시한다.
- payload 복사와 heap allocation을 측정한다.
- connection 또는 session별 메모리 비용을 benchmark한다.
- backpressure를 drop보다 먼저 적용한다.

Goroutine lifecycle, bounded queue, cancellation과 GC 비용의 기술 배경은 [Go 동시성과 backpressure](../explainers/go-concurrency-and-backpressure.md)를 참고한다.

### 4.2 테스트와 진단 도구

Go는 표준 도구에서 unit test, benchmark, coverage, fuzzing과 data race detector를 제공한다. `pprof`와 runtime trace로 CPU, heap, goroutine, mutex와 blocking 원인을 분석할 수 있다.

이는 PRD의 parser fuzzing, race detector, coverage ratchet, property test와 실제 성능 비교 요구에 직접 대응한다.

### 4.3 분산 시스템과 Kubernetes 생태계

Go에는 Kubernetes `client-go`, Prometheus `client_golang`, gRPC-Go와 검증된 Raft 구현이 있다. 특히 `etcd/raft`는 deterministic replicated state machine으로 설계되어 있지만 network와 disk I/O는 제공하지 않는다.

NaruMQ는 consensus 알고리즘을 직접 구현하지 않는다. 동시에 consensus 라이브러리 선택만으로 클러스터가 완성되었다고 간주하지 않는다. WAL, snapshot, transport, membership, committed entry 적용, backpressure와 복구 절차는 NaruMQ가 검증해야 한다.

### 4.4 배포와 유지보수

broker core는 기본적으로 CGO 없이 단일 Go 바이너리로 빌드한다. CGO가 필요한 의존성을 도입하려면 배포 이식성, 보안 업데이트와 성능 영향을 별도 ADR에서 승인받아야 한다.

CI와 release image는 정확한 Go patch version을 고정한다. 지원 중인 최신 patch로 올릴 때 unit, race, fuzz, compatibility와 benchmark gate를 다시 실행한다.

## 5. Architecture 제약

Go를 선택해도 외부 라이브러리가 제품 구조를 결정하게 해서는 안 된다. source dependency는 바깥 adapter에서 안쪽 정책으로만 향한다.

```text
cmd/narumq
    composition root

internal/domain
    mqtt
    session
    routing
    delivery
    tenancy
    clusterlifecycle

internal/application
    connect
    publish
    subscribe
    takeover
    drain
    ports
        consensus
        logstore
        sessionstore
        transport
        authentication
        clock
    dto

internal/adapters
    tcp
    tls
    raft
    storage
    kubernetes
    prometheus
    managementapi
```

다음 의존성 규칙을 CI architecture test로 검증한다.

- `domain`은 `application`, application port, `adapters`, Kubernetes, HTTP, Metric, storage와 consensus package를 import하지 않는다.
- `application`은 `domain`과 자신이 소유한 port에만 의존한다.
- port interface는 이를 사용하는 use case 또는 `internal/application/ports`가 소유한다. 구현 adapter 옆에 port를 선언하지 않는다.
- adapter는 외부 자료형을 application DTO 또는 domain value로 변환한다. framework, RPC와 저장소 자료형을 안쪽 경계로 넘기지 않는다.
- `cmd/narumq`만 concrete adapter를 조립한다.
- 실제 clock, network, random, storage와 consensus는 domain unit test에 나타나지 않는다.

계층별 책임, port ownership과 경계 자료형은 [Broker core의 계층 경계](../explainers/broker-core-boundaries.md)를 참고한다.

## 6. 예상 결과

### 긍정적 결과

- Kubernetes와 관측성 도구를 같은 언어와 생태계에서 구현할 수 있다.
- MQTT와 cluster 상태 머신을 빠르고 결정적인 unit test로 검증할 수 있다.
- race detector, fuzzing, profiling을 개발 초기부터 CI에 포함할 수 있다.
- 단일 바이너리 배포와 container 운영이 단순해진다.
- Rust와 Erlang/OTP보다 일반적인 backend 팀의 진입 장벽이 낮다.
- storage와 consensus 제품을 adapter로 교체할 수 있다.

### 부정적 결과와 대응

| 결과 | 대응 |
| --- | --- |
| Erlang/OTP의 supervisor와 process isolation을 자동으로 얻지 못한다. | connection, session, routing, storage와 consensus loop의 lifecycle, failure boundary와 health 상태를 명시한다. node failure는 replicated state와 Kubernetes 재시작으로 복구한다. |
| heap allocation과 GC가 p99.9 latency와 메모리에 영향을 줄 수 있다. | allocation profile, connection당 heap, GC CPU, pause와 tail latency를 필수 benchmark 항목으로 둔다. bounded buffer와 복사 최소화는 측정 결과에 따라 적용한다. |
| goroutine과 channel 오용이 leak, race와 무제한 queue를 만들 수 있다. | `go test -race`, goroutine profile, bounded queue, cancellation test와 leak test를 CI에 포함한다. |
| Rust보다 동일 자원에서 낮은 성능이 나올 수 있다. | 실제 EMQX와 동일 조건으로 비교한다. 합의한 SLO를 충족하지 못하면 최적화 후 언어 결정을 재검토한다. |
| consensus와 durable storage를 직접 연결해야 한다. | library와 storage를 port 뒤에 두고 fault injection, crash recovery와 snapshot test를 P0로 수행한다. |

## 7. 검증 게이트

Phase 0에서 다음 spike를 완료해야 본격적인 기능 구현으로 진행한다. 이 gate는 아직 존재하지 않는 durable cluster와 rolling drain 기능을 요구하지 않는다.

1. MQTT 3.1.1/5.0 parser와 encoder를 구현하고 compliance seed test, fuzz test와 malformed packet test를 통과한다.
2. QoS 1/2와 session expiry 상태 머신을 실제 network와 storage 없이 unit test한다.
3. 여러 연결에서 publish, subscribe, wildcard routing과 backpressure를 시험한다.
4. 3노드 consensus skeleton에서 leader failure, quorum loss, WAL replay와 snapshot 복구 가능성을 시험한다. 이는 BP-03 또는 BP-06 통과를 뜻하지 않는다.
   - Gate 4의 실제 증거는 [consensus adapter unit tests](../../internal/adapters/raft/node_test.go), [experimental storage tests](../../internal/adapters/raft/stable_store_test.go), [deterministic integration spike](../../tests/integration/raft_consensus_test.go)다. 이 증거는 etcd/raft skeleton과 local WAL·snapshot recovery 가능성만 다루며 BP-03/BP-06 완료를 의미하지 않는다.
5. BP-01 Protocol Baseline의 최소 기능 집합을 [공통 benchmark harness](../../tests/benchmark/bp01)로 실제 EMQX와 비교한다. 대상은 MQTT 5, QoS 0, 비영속 workload이며, broker CPU·memory·network·TLS와 고정 workload 조건을 동일하게 맞춘다.
   - 현재 BP-01 smoke evidence는 plaintext loopback, MQTT 5 QoS 0 조건만 관찰한다. TLS·인증 workload와 수치형 release 기준은 별도 profile 및 `SPEC-TBD-08` 확정 뒤 gate 판정에 포함한다.
   - smoke 실행 결과는 `.artifacts/benchmark/bp01/<run-id>/manifest.json`과 target별 `summary.json`, `samples.jsonl`, `phases.jsonl`, NaruMQ `runtime-metrics.jsonl`/`runtime-summary.json`, EMQX `runtime-summary.json`, `broker-stats.jsonl`에 보존한다. EMQX 런타임 요약은 `not_applicable`로 기록하며 Go runtime 수치를 억지로 비교하지 않는다.
   - 현재 harness의 EMQX는 `5.8.8` 역사적 OSS comparator이며 `releaseGating=false`다. 이 증거만으로 gate 5 또는 production 대체 가능성을 완료로 표시하지 않는다.
6. BP-05 Connection Scale에서 connection ramp, idle connection당 memory와 reconnect subset을 우선 측정한다. 전체 BP-05 통과 판정은 Phase 1에서 수행한다.
   - 제한된 smoke 구현은 [`tests/benchmark/bp05`](../../tests/benchmark/bp05)에서 MQTT 5 연결 ramp, idle PINGREQ/PINGRESP, 동일 Client ID reconnect와 broker memory 표본을 관찰한다.
   - 실행 결과는 `.artifacts/benchmark/bp05/<run-id>/manifest.json`, target별 `connections.jsonl`, `phases.jsonl`, `workload-summary.json`, `resource-summary.json`, NaruMQ `runtime-metrics.jsonl`/`runtime-summary.json`, EMQX `runtime-summary.json`, 합성 `summary.json`과 `comparison.json`에 보존한다.
   - 이 smoke는 64 연결·1 CPU/512 MiB broker·1 CPU/256 MiB generator라는 하네스 값만 증명하며 `B_conn`, production SLO 또는 release 합격 비율을 정하지 않는다. `releaseGating=false`이고 gate 6·7·8 완료를 의미하지 않는다.
7. throughput, p99.9 latency, connection당 memory, allocation rate와 GC CPU를 기록한다.
   - Gate 7 구현은 `runtime/metrics`의 누적 표본을 NaruMQ 프로세스에서 수집하고, BP-01의 `measure`와 BP-05의 `idle` phase 경계 안에서 allocation rate·GC CPU·GC pause percentile·GOMAXPROCS를 요약한다. 측정 구간에 GC cycle이 한 번도 실행되지 않으면 현재 Go runtime에서 `/cpu/classes` 누적값이 증가하지 않을 수 있으므로 `cpuMetricsAvailable=false`와 이유를 기록하고 CPU 값을 추정하지 않는다. 이 값은 Go 내부 진단값이므로 Docker CPU 사용량이나 EMQX BEAM 런타임 수치와 같은 의미로 비교하지 않는다. raw/summary artifact의 구조와 applicability는 검증하지만, 수치형 기준이 확정되지 않았고 `releaseGating=false`이므로 이 구현만으로 Gate 7 또는 Gate 8을 닫지 않는다.
8. PRD Phase 0에서 확정한 수치형 성능·메모리 기준을 만족한다. 기준이 확정되지 않으면 이 gate를 닫을 수 없다.
9. 모든 결과와 raw artifact를 version control 또는 artifact storage에 보관한다.

BP-03 Durable Delivery는 Phase 2, BP-07 Rolling Drain은 Phase 3에서 해당 기능의 release gate로 실행한다. 전체 BP-01~BP-07 비교는 PRD의 P0 출시 gate에서 완료한다.

Go 구현이 PRD Phase 0에서 수치로 확정한 성능·메모리·복구 SLO를 충족하지 못하면 다음 순서로 대응한다.

1. profile로 병목을 확인하고 allocation, locking, batching과 I/O 구조를 개선한다.
2. 같은 조건으로 benchmark를 반복한다.
3. 여전히 SLO를 충족하지 못하면 Erlang/OTP 또는 Rust prototype과 비교한다.
4. 언어를 변경할 경우 이 ADR을 새 ADR로 대체한다.

## 8. 검토한 대안

### Erlang/OTP

경량 process, message passing, supervision과 fault isolation이 MQTT에 가장 자연스럽게 대응한다. 팀에 production Erlang/OTP 전문가가 충분하고 BEAM 운영을 장기 지원할 수 있다면 유효한 대안이다.

선택하지 않은 이유는 인력 확보와 학습 비용, Kubernetes 생태계 통합 비용, 그리고 Erlang을 선택해도 NaruMQ의 quorum·fencing·durable session·drain을 별도로 설계해야 하기 때문이다.

### Rust

GC가 없고 memory safety와 낮은 runtime overhead를 제공한다. Tokio와 OpenRaft 같은 생태계도 사용할 수 있다.

선택하지 않은 이유는 async ownership, lifetime과 concurrent state 관리가 개발·review 비용을 높이고, 초기 제품의 기능 완성과 광범위한 테스트 구축 속도를 늦출 가능성이 크기 때문이다. Go가 측정된 SLO를 충족하지 못할 때 우선 재검토한다.

### Java와 Netty

Netty는 검증된 비동기 network framework이고 JVM의 테스트·profiling 생태계도 성숙하다. 조직이 이미 Netty와 JVM 운영 역량을 보유했다면 적합한 선택이다.

선택하지 않은 이유는 현재 NaruMQ에 JVM 생태계가 필수라는 근거가 없고, container runtime과 GC 운영 구성이 Go보다 복잡하기 때문이다.

### C 또는 C++

낮은 수준의 memory와 I/O 제어가 가능하다.

선택하지 않은 이유는 memory safety, concurrency 결함, fuzzing 결과 대응과 장기 유지보수 비용이 NaruMQ의 목표에 비해 크기 때문이다.

### 여러 언어로 broker core 분할

network data path, session과 cluster를 서로 다른 언어의 service로 분리하는 방안이다.

선택하지 않은 이유는 serialization, network hop, failure mode와 배포 복잡도가 늘고 QoS 상태를 원자적으로 관리하기 어려워지기 때문이다. Dashboard와 외부 adapter는 별도 process로 둘 수 있지만 broker data path는 Go로 유지한다.

## 9. 재검토 조건

다음 중 하나가 발생하면 이 결정을 재검토한다.

- Phase 0 최적화 후에도 PRD Phase 0에서 수치로 확정한 p99.9 latency 또는 connection당 memory 목표를 충족하지 못한다.
- race, goroutine leak 또는 GC 비용 때문에 PRD의 안정성 기준을 반복해서 위반한다.
- 목표 connection 규모가 현재 benchmark 범위를 크게 넘어선다.
- 팀 구성과 운영 역량이 Erlang/OTP 또는 Rust 중심으로 바뀐다.
- 선택한 핵심 Go library의 유지보수 또는 보안 상태가 허용할 수 없는 수준으로 악화된다.

재검토는 기존 ADR을 수정해 결론을 뒤집는 방식이 아니라 새 ADR로 이 결정을 supersede하는 방식으로 기록한다.

## 10. 참고 자료

- [NaruMQ 제품 요구사항 정의서](../requirements/PRD.md)
- [NaruMQ 시스템 스펙](../specs/README.md)
- [NaruMQ ADR catalog](README.md)
- [NaruMQ Contract catalog](../contracts/README.md)
- [Performance와 verification 스펙](../specs/performance-and-verification.md)
- [NaruMQ 기술 해설](../explainers/README.md)
- [Go Release History and Support Policy](https://go.dev/doc/devel/release)
- [Go Fuzzing](https://go.dev/doc/security/fuzz/)
- [Go Data Race Detector](https://go.dev/doc/articles/race_detector)
- [Go Diagnostics and pprof](https://go.dev/doc/diagnostics)
- [A Guide to the Go Garbage Collector](https://go.dev/doc/gc-guide)
- [etcd/raft](https://github.com/etcd-io/raft)
- [Kubernetes client-go](https://github.com/kubernetes/client-go)
- [Prometheus client_golang](https://github.com/prometheus/client_golang)
- [gRPC-Go](https://github.com/grpc/grpc-go)
- [Erlang/OTP Design Principles](https://www.erlang.org/docs/27/system/design_principles.html)
- [EMQX Clustering](https://docs.emqx.com/en/emqx/latest/deploy/cluster/introduction.html)
- [Tokio](https://tokio.rs/tokio/tutorial)
- [OpenRaft](https://github.com/databendlabs/openraft)
- [Netty](https://netty.io/)
