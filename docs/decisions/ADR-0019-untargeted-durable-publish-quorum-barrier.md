# ADR-0019: Durable target이 없는 QoS 1/2 publish는 delivery command 없이 quorum barrier 뒤 완료한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (zero-durable-target delivery command 생략 scope only) |
| 결정일 | 2026-10-02 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD §8](../requirements/PRD.md#8-메시지-보장-모델), CLU-07, CLU-09 |
| 관련 스펙 | [Cluster §4.2](../specs/cluster-session-and-delivery.md#42-qos-12-durable-delivery), [Cluster §11.11](../specs/cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑), [MQTT §3.1](../specs/mqtt-protocol-and-session.md#31-수신-qos-상태) |
| 관련 아키텍처 | [Broker core architecture](../architectures/broker-core.md) |
| 대체 범위 | [ADR-0015](ADR-0015-cluster-durable-qos-delivery.md)의 §1–8에서 “Durable target이 0개여도 PUBLISH command를 commit한다”는 문장만 대체한다. [ADR-0003](ADR-0003-durable-ack-barrier.md) §1의 적용 범위 문장을 함께 개정한다. |
| 대체한 새 결정 | 없음 |

## 1. 결정

- Durable target 후보는 PUBLISH 준비 시점의 ingress node route projection에 있는 primary 후보 가운데 effective QoS가 1 이상인 subscription이다. Session의 persistent 여부는 후보 포함 여부를 바꾸지 않는다. Apply 단계의 ownership·subscription 재검증은 기존 규칙을 그대로 따른다.
- QoS 1 client PUBLISH와 QoS 1/2 Will에 후보가 하나도 없으면 `DeliveryChange PUBLISH` command를 제안하지 않는다. 대신 새 quorum ReadIndex barrier를 시작하고, barrier가 확인한 index까지 local apply와 route projection이 끝난 뒤에만 QoS 0 lane을 처리한다. QoS 1 client PUBLISH는 그 뒤 PUBACK을 반환하며 Will에는 publisher ACK가 없다. Barrier timeout은 기존 durable commit timeout인 5초다.
- Barrier가 실패하거나 timeout되면 성공 ACK를 반환하지 않고 QoS 0 lane도 실행하지 않는다. 연결 종료는 기존 durable commit 실패와 같다.
- Durable target 후보가 하나 이상이면 ADR-0015대로 delivery command를 commit한다. Apply 단계에서 모든 후보가 `Skipped`가 되더라도 command를 생략하지 않는다.
- Retained mutation은 기존 순서대로 commit한다. QoS 2 `RECEIVE`는 target 수와 무관하게 기존대로 commit하고 PUBREC 전에 저장한다. QoS 2 `RELEASE`도 target이 0개여도 commit한 뒤 PUBCOMP를 반환한다.
- Target 판정 기준은 기존과 같이 prepare-time projection이다. Ingress node에서 아직 적용되지 않은 SUBSCRIBE는 publish보다 뒤에 정렬되며, 이 기준점은 target이 있는 publish에도 동일하게 적용된다.
- Replicated schema, wire format과 apply 동작은 바꾸지 않는다. 이 변경은 별도 feature activation이나 target 없는 publish에 대한 release profile compatibility gate를 추가하지 않는다. 혼합 release cluster의 구버전 node는 같은 결과를 내는 빈 PUBLISH command를 여전히 commit할 수 있다.

## 2. 배경

현재 Apply는 PUBLISH action에서 durable target이 없으면 delivery record, sequence 증가와 enqueue를 만들지 않는다. 다만 빈 PUBLISH command도 proposal, WAL sync와 replication 비용을 소비한다. 만료된 record 정리는 delivery action apply 전에 수행되므로, target 없는 publish가 command를 생략하면 만료 정리도 그 publish 때문에 실행되지 않고 다음 delivery action apply 시점으로 미뤄진다.

EMQX 5.8.8은 매칭 durable-session route가 있을 때 메시지 저장 경로를 사용한다. 이 선택만으로 NaruMQ의 ACK 정책을 정하지 않는다. NaruMQ는 저장할 durable delivery가 없어도 성공 PUBACK 전에 quorum 확인을 유지하며, EMQX의 asynchronous ACK 동작을 채택하지 않는다. 자세한 비교는 [EMQX와 NaruMQ 기능 대조표](../explainers/emqx-and-narumq-feature-comparison.md)를 따른다.

## 3. 결정 기준

1. 모든 성공 QoS 1/2 ACK에서 quorum 확인을 유지한다.
2. 저장할 durable delivery state가 없을 때만 불필요한 replicated write를 생략한다.
3. 기존 QoS 1/2 target 판정, apply 재검증, QoS 2 handshake와 retained ordering을 바꾸지 않는다.
4. Target 없는 publish가 quorum 없는 minority node에서 성공 ACK되는 것을 막는다.
5. Schema, wire format과 release feature activation 변경을 요구하지 않는다.

## 4. 결정 이유

빈 PUBLISH command는 durable delivery record를 남기지 않지만 write quorum을 기다리고 WAL을 sync한다. Fresh quorum ReadIndex barrier는 같은 ordering 기준에서 leader/quorum의 유효성을 확인하고 local projection이 해당 index까지 따라온 뒤 publish 처리를 허용한다. 따라서 ACK의 quorum 확인을 subscriber 유무와 분리하면서 저장 대상만 target 유무에 따라 구분한다. Target 있는 publish의 기존 commit 경계와 같은 prepare-time projection 기준은 그대로 보존한다.

## 5. 고려한 대안

| 대안 | 장점 | 기각 이유 |
| --- | --- | --- |
| Target이 없어도 기존 PUBLISH command를 commit | 현재 동작을 유지한다. | 저장 효과가 없는 command가 WAL sync와 replication 비용을 계속 쓴다. |
| Target이 없으면 quorum 확인도 생략 | 가장 적은 작업으로 응답한다. | quorum 없는 minority node가 QoS 1 PUBLISH에 성공 ACK할 수 있어 ADR-0003의 ACK 경계를 깨뜨린다. |
| Barrier 뒤 target을 다시 계산 | barrier 시점의 projection을 사용한다. | 모든 publish의 target 판정 시점을 바꾸고 shared subscription 선택 및 route race 의미를 변경한다. |
| Persistent session만 durable target으로 인정 | 영속 session만 저장할 수 있다. | 현재 effective QoS 1/2 matching subscription 경로의 범위를 바꾸며 별도 설계 결정이 필요하다. |
| EMQX처럼 asynchronous ACK 적용 | 저장 지연을 ACK와 분리할 수 있다. | 성공 ACK의 quorum 경계를 약화해 ADR-0003 계약과 맞지 않는다. |

## 6. 결과와 한계

### 긍정적 결과

- Target이 없는 QoS 1 client PUBLISH와 QoS 1/2 Will은 delivery PUBLISH entry와 그에 따른 WAL write를 만들지 않는다. QoS 2 client PUBLISH의 inbound `RECEIVE` state는 target 유무와 무관하게 commit한다.
- QoS 1 성공 PUBACK과 QoS 1/2 Will의 QoS 0 fan-out 전에 fresh quorum 확인은 계속 요구된다.
- Target이 있는 publish와 QoS 2 RECEIVE/RELEASE의 저장·ACK 순서는 바뀌지 않는다.

### 부정적 결과와 경계

- Target 없는 QoS 1 client PUBLISH와 QoS 1/2 Will은 delivery payload를 영속 저장하지 않는다. QoS 2 client PUBLISH의 inbound `RECEIVE`와 retained 값은 별도 상태로 계속 저장한다. 이 경로의 성공 PUBACK은 quorum barrier 통과를 뜻하며 미래 subscriber 전달이나 장애 뒤 전달을 보장하지 않는다.
- target 없는 publish는 만료 purge를 수행하지 않으므로 그 정리가 다음 delivery action apply까지 지연될 수 있다.
- 이 경로에는 release profile compatibility gate가 없다. 혼합 release에서 구버전 node가 빈 PUBLISH command를 commit할 수 있다.
- 기존 BP-03 workload는 매칭 target을 사용하므로 이 최적화로 그 workload의 비용이나 미달성 결과가 바뀌지 않는다.

## 7. 검증과 실행 증거

| 시나리오 | 시험 위치 | 확인 결과 | 실행 증거 |
| --- | --- | --- | --- |
| Target 없는 publish와 QoS 2 경계 | `internal/application/publish/durable_delivery_test.go::TestPublishDurableWithoutTargetsUsesQuorumBarrier` | sync/begin 두 경로에서 barrier 시작·대기 실패가 QoS 0 전달을 막는다. BeginPublish 제출만으로는 전달하지 않으며 retained·RELEASE 저장은 유지한다. | 회귀 보강 뒤 focused publish package PASS (0.367s); 명령과 인접 package 결과는 [Cluster §11.11](../specs/cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑)의 Acceptance commands 행을 따른다. |
| TCP QoS 1 ACK 경계 | `internal/adapters/tcp/server_durable_test.go::TestTCPDurableQoS1WithoutTargetAcknowledgesAfterQuorumBarrier`, `TestTCPDurablePhasesWithoutTargetWaitForQuorumBarrier` | Channel로 barrier 대기를 보류한 phase worker에서 완료 전 ACK 부재, 성공 후 PUBACK, quorum 오류 후 ACK 없는 연결 종료를 확인한다. | 보강 뒤 focused TCP package PASS (10.080s); 명령은 Cluster §11.11. |
| Cluster barrier는 write를 만들지 않음 | `cmd/narumq/cluster_durable_test.go` | ReadIndex를 사용하며 proposal 없이 성공하고, not-leader/driver 종료 오류를 반환한다. | `TestClusterQuorumBarrierUsesReadIndexWithoutProposal` — `go test ./cmd/narumq -run 'TestClusterQuorumBarrierUsesReadIndexWithoutProposal|TestDriverRequestsRejectAfterExit|TestClusterDurable' -count=1 -v` PASS |
| Focused durable package suite | Membership, records, delivery queue, publish, connect, TCP and runtime packages | 기존 저장 경계와 보강한 zero-target 분기가 일곱 package에서 통과한다. | `GOMAXPROCS=4 go test ./internal/application/membership ./internal/adapters/raft/records ./internal/adapters/memory/deliveryqueue ./internal/application/publish ./internal/application/connect ./internal/adapters/tcp ./cmd/narumq -count=1` — PASS; 14.91s wall. |
| 실제 MQTT 3-process acceptance | `tests/integration/cluster_durable_delivery_process_test.go` | Quorum이 있을 때 target 없는 QoS 1 PUBACK, quorum 상실 시 성공 응답 부재와 기존 takeover·backup·QoS 2 Will을 확인한다. | 네 process acceptance 재검증 PASS (43.353s); 정확한 명령·개별 시간은 Cluster §11.11의 Acceptance commands 행. |
| Matched-target durable ACK fault regression | `tests/integration/cluster_durable_ack_fault_process_test.go` | ACK 직후 replica kill, WAL/commit crash 및 leader/follower partition에서도 기존 matched-target 저장 경계를 보존한다. | `GOMAXPROCS=4 NARUMQ_DURABLE_ACK_FAULTS=1 go test ./tests/integration -run '^TestDynamicClusterDurableAck(ReplicaKill|CrashPoint|Partition)ProcessAcceptance$' -count=1 -v -timeout 30m` — 보강 후 재검증 PASS (156.803s). 앞선 시험의 leader/follower 선택 불확실성은 두 역할을 명시적으로 먼저 선택하는 fixture로 수정되어 있다. |
| Full local repository gate | `tools/verify local` | 보강한 회귀를 포함한 package·integration·race, 네 fuzz target, architecture·coverage·traceability가 통과했다. 앞선 동시 CONNECT 실패는 ownership bind fence 수정으로 대응했고, retained readiness timeout 원인은 여전히 미확정이다. | `GOMAXPROCS=4 go run ./tools/verify local` — 최신 PASS (286.65s wall; integration 257.138s). 앞선 실패·수정·후속 PASS 이력은 [Cluster §11.15](../specs/cluster-session-and-delivery.md#1115-cross-node-delivery-matrix동시-connectstale-epoch-수용-매핑)에 보존한다. |

이 실행은 production storage/EBS power-loss, BP-03 workload, N/N-1 compatibility 또는 전체 cluster acceptance의 완료를 뜻하지 않는다.

## 8. 재검토 조건

다음 조건 중 하나가 발생하면 이 결정을 재검토한다.

- PRD가 매칭 durable target이 없는 QoS 1/2 payload도 미래 전달 또는 장애 뒤 전달을 위해 저장하도록 요구한다.
- 제품 요구가 target을 prepare-time projection이 아니라 quorum proof 이후 fresh projection에서 판정하도록 바뀐다.
- 혼합 release에서 빈 PUBLISH command를 쓰는 구버전 node를 지원하지 않는 명시적 compatibility 정책이 채택된다.
