# ADR-0002: Cluster consensus core로 etcd/raft를 사용한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Proposed |
| 제안일 | 2026-08-16 |
| 결정일 | 미정 |
| 결정자 | 미정 |
| 관련 요구사항 | CLU-01~CLU-02, CLU-05, CLU-09~CLU-10, CLU-14, NFR-07~NFR-10 |
| 관련 스펙 | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md) |
| 관련 Architecture | [Cluster architecture](../architectures/cluster-architecture.md) |
| 관련 Contract | [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md) |
| 관련 해설 | [Raft quorum과 durable ACK](../explainers/raft-quorum-and-durable-ack.md), [WAL, snapshot과 crash recovery](../explainers/wal-snapshot-and-crash-recovery.md) |

## 1. 결정

NaruMQ cluster의 consensus algorithm core로 Go module `go.etcd.io/raft/v3`를 사용한다.

이 결정은 Raft algorithm 구현만 선택한다. NaruMQ가 다음 책임을 직접 구현하고 검증한다.

- WAL과 `HardState`, log entry, snapshot의 영속화
- Node 간 transport, authentication과 message framing
- Membership discovery와 join·leave workflow
- Committed entry apply loop와 application state machine
- Snapshot 생성·설치, compaction와 crash recovery
- Backpressure, admission control과 health signal
- Release 간 wire·persisted-state compatibility

Production version은 Phase 0 spike가 통과한 정확한 module version과 checksum으로 고정한다. 이 ADR은 아직 그 version을 확정하지 않는다.

### 1.1 Phase 0 gate 4 spike profile

Phase 0 gate 4의 검증용 구현은 `go.etcd.io/raft/v3 v3.7.0`을 사용한다. 이 버전은 Go 1.26을 선언하는 현재 stable module이며, 이번 기록은 production version 승인이나 persisted-state·wire contract 승인이 아니다. ADR 상태는 `Proposed`로 유지한다.

Spike는 `raft.RawNode`의 synchronous `Ready`/`Advance` 경계와 `AsyncStorageWrites=false`를 사용한다. `PreVote=true`, `CheckQuorum=true`, `ElectionTick=10`, `HeartbeatTick=1`, `MaxSizePerMsg=64 KiB`, `MaxCommittedSizePerReady=1 MiB`, `MaxUncommittedEntriesSize=1 MiB`, `MaxInflightMsgs=16`, `MaxInflightBytes=1 MiB`를 고정하고, logical tick·in-process FIFO message delivery로 실행을 결정적으로 재생한다. Voter는 고정된 `{1,2,3}`이며 membership change, real peer RPC, authentication과 MQTT command encoding은 이 profile의 범위가 아니다.

Failover 검증은 follower 전용 `RawNode.ForgetLeader` seam을 `Campaign` 앞에 사용한다. 이 호출은 membership를 변경하지 않는다. Leader 장애 직후 `CheckQuorum`과 `PreVote`가 campaign을 무시할 수 있는 비결정적 lease window를 피하기 위한 결정이다.

`internal/adapters/raft`는 etcd/raft 자료형을 adapter 안에 캡슐화하고, experimental WAL·snapshot과 deterministic node boundary를 소유한다. 검증 결과는 [adapter unit tests](../../internal/adapters/raft/node_test.go), [stable-store tests](../../internal/adapters/raft/stable_store_test.go), [integration spike](../../tests/integration/raft_consensus_test.go)에 기록한다. 이 evidence는 algorithm과 local recovery 가능성을 증명할 뿐 BP-03/BP-06, durable MQTT ACK, production transport 또는 production durability 완료를 뜻하지 않는다.

## 2. 배경

NaruMQ는 단일 node 장애에서 durable ACK message RPO 0, minority의 새 ownership 금지와 자동 복구를 요구한다. Consensus algorithm을 직접 구현하면 제품 고유의 MQTT·session 문제보다 algorithm 안전성 검증에 더 큰 비용이 든다.

`etcd/raft`는 deterministic replicated state machine 형태로 Raft core를 제공한다. 반면 disk와 network I/O는 제공하지 않는다. 따라서 library 선택만으로 WAL, snapshot, membership과 durable ACK 요구가 충족되지는 않는다.

## 3. 결정 요인

1. 3-node quorum과 membership change를 지원해야 한다.
2. Partition과 leader failure 동작을 결정적으로 시험할 수 있어야 한다.
3. Storage와 transport를 application-owned port 뒤에 둘 수 있어야 한다.
4. WAL ordering, snapshot과 backpressure를 NaruMQ 요구에 맞게 통제해야 한다.
5. Go core와 같은 process에서 운영할 수 있어야 한다.

## 4. 선택 근거

`etcd/raft`는 algorithm, storage와 transport 책임을 분리한다. 이 경계는 [ADR-0001](ADR-0001-core-implementation-language.md)의 port·adapter 구조와 맞는다.

Library의 `Ready` 처리 규칙은 entry·`HardState`·snapshot을 안정 저장하고 필요한 ordering을 지킨 뒤 message를 보내도록 application에 책임을 둔다. NaruMQ는 이 책임을 명시적으로 소유해야 한다. 이 제약은 durable ACK barrier를 별도 [ADR-0003](ADR-0003-durable-ack-barrier.md)으로 정의하는 근거다.

## 5. 대안

| 대안 | 장점 | 제외 이유 |
| --- | --- | --- |
| Raft 직접 구현 | 제품 요구에 완전히 맞춘 API | Consensus 안전성·membership·snapshot 검증 범위가 지나치게 커진다. |
| HashiCorp Raft | Storage·transport 통합 선택지가 많다. | NaruMQ가 요구하는 deterministic apply·I/O ordering 경계를 더 직접 통제하기 어렵다. Phase 0 비교에서 반증할 수 있다. |
| 외부 etcd를 system of record로 사용 | Consensus와 운영 도구를 외부 제품이 제공한다. | Message·inflight data path에 외부 hop과 별도 용량·장애 경계를 추가한다. Control metadata 저장소 후보로는 별도 검토할 수 있다. |
| 단일 writer와 비동기 replica | 구현이 단순하고 지연이 낮다. | Leader 장애에서 ACK된 state RPO 0과 minority fencing을 충족하지 못한다. |

## 6. 결과

### 긍정적 결과

- 검증된 consensus core를 사용하고 NaruMQ state machine에 집중할 수 있다.
- Deterministic simulation과 fault injection을 구축하기 쉽다.
- Storage와 transport를 adapter로 교체할 수 있다.

### 부정적 결과와 후속 작업

- WAL, snapshot, transport와 membership은 여전히 NaruMQ 책임이다.
- `Ready` ordering 위반은 data loss를 만들 수 있으므로 model test와 crash test가 필요하다.
- Module upgrade가 persisted state와 wire compatibility에 영향을 줄 수 있다.
- 실제 storage engine과 RPC encoding은 별도 ADR로 결정해야 한다.

## 7. 검증

Phase 0 spike는 다음을 통과해야 한다.

1. 3-node에서 leader kill 뒤 committed state가 유지된다.
2. Minority partition에서 새 write와 ownership이 commit되지 않는다.
3. WAL append, `HardState` 저장과 outbound message ordering을 fault injection으로 검증한다.
4. Snapshot 중 crash 뒤 마지막 committed index로 복구한다.
5. Disk full과 느린 follower가 무제한 queue를 만들지 않는다.
6. Membership change 중 crash와 재시작에서 member set이 수렴한다.

## 8. 재검토 조건

- Phase 0에서 required durability나 p99.9 목표를 충족하지 못한다.
- 유지보수·보안 상태가 release 기준을 충족하지 못한다.
- Storage·transport 책임 때문에 대안 library보다 결함과 구현 비용이 커진다.
- N-1↔N compatibility를 안전하게 유지할 수 없다.

## 9. 참고 자료

- [etcd/raft](https://github.com/etcd-io/raft)
- [Raft thesis](https://github.com/ongardie/dissertation/blob/master/stanford.pdf)
- [ADR-0001](ADR-0001-core-implementation-language.md)
