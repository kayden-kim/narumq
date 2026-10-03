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

Production target version은 `go.etcd.io/raft/v3 v3.7.0`으로 고정하며 module zip과 manifest checksum은 scoped [ADR-0009](ADR-0009-raft-production-state-profile.md)에 기록한다. 이는 production 검증이나 persisted-state·wire 호환성 증거가 아니며 이 ADR 자체를 승인하지 않는다. 나머지 §7 기준을 충족하기 전까지 ADR-0002는 `Proposed`로 유지한다.

### 1.1 Phase 0 gate 4 spike profile

Phase 0 gate 4의 검증용 구현은 `go.etcd.io/raft/v3 v3.7.0`을 사용한다. 이 spike는 버전 pin과 production validation을 같은 의미로 취급하지 않는다. ADR-0009가 이 버전을 scoped design target으로 선택했지만, 그 선택은 production 작동이나 persisted-state·wire contract 승인을 뜻하지 않는다. ADR 상태는 `Proposed`로 유지한다.

Spike는 `raft.RawNode`의 synchronous `Ready`/`Advance` 경계와 `AsyncStorageWrites=false`를 사용한다. `PreVote=true`, `CheckQuorum=true`, `ElectionTick=10`, `HeartbeatTick=1`, `MaxSizePerMsg=64 KiB`, `MaxCommittedSizePerReady=1 MiB`, `MaxUncommittedEntriesSize=1 MiB`, `MaxInflightMsgs=16`, `MaxInflightBytes=1 MiB`를 고정하고, logical tick·in-process FIFO message delivery로 실행을 결정적으로 재생한다. Voter는 고정된 `{1,2,3}`이며 membership change, real peer RPC, authentication과 MQTT command encoding은 이 profile의 범위가 아니다.

Failover 검증은 follower 전용 `RawNode.ForgetLeader` seam을 `Campaign` 앞에 사용한다. 이 호출은 membership를 변경하지 않는다. Leader 장애 직후 `CheckQuorum`과 `PreVote`가 campaign을 무시할 수 있는 비결정적 lease window를 피하기 위한 결정이다.

`internal/adapters/raft`는 etcd/raft 자료형을 adapter 안에 캡슐화하고, opaque `[][]byte` command boundary, NMQSTATE1 opaque command snapshot/WAL과 deterministic node boundary를 소유한다. 이 spike adapter는 command payload를 해석하거나 application sequence를 만들지 않고 bytes를 순서대로 보존한다. ownership codec과 application committed-command sequence 해석은 `internal/application/takeover`가 소유한다. 검증 결과는 [adapter unit tests](../../internal/adapters/raft/node_test.go), [stable-store tests](../../internal/adapters/raft/stable_store_test.go), [integration spike](../../tests/integration/raft_consensus_test.go)에 기록한다. 이 evidence는 algorithm과 local recovery 가능성을 증명할 뿐 BP-03/BP-06, durable MQTT ACK, production transport 또는 production durability 완료를 뜻하지 않는다.

### 1.2 Dynamic driver의 제한된 발행 제어 채택

2026-10-02 사용자 지시 `개선 진행해`는 dynamic driver의 peer 용량 기반 fresh ReadIndex 발행과 공정한 Ready 진행을 채택한다. 64개 request를 먼저 발행한 뒤 32개 egress에서 거절하는 방식 대신, 현재 count·byte 여유와 heartbeat·proof response 비용에 맞춰 미발행 FIFO head를 보존한다. 실제 follower burst 시험은 수정 전 64개 중 32개만 완료했고, 첫 local admission 구현의 process 시험도 `MsgReadIndexResp` 포화에서 네 summary의 quorum proof에 실패했다. 따라서 요청 방향만 계산하는 대안은 채택하지 않고 응답 비용과 transport writer의 실행 기회를 함께 반영한다.

후속 고정 BP-03에서는 여러 origin이 같은 leader queue를 쓰며 `MsgReadIndexResp`가 거절됐다. 따라서 local origin당 15개 예산을 독립적으로 사용하는 대신 voter·learner 수로 비용을 나눈다(빈 3-voter cluster의 node별 5개). 분모는 1–15 범위이며 최소 한 요청의 진행을 허용한다. 이 보수적인 분할은 단일 origin의 burst 여유를 줄이는 대신 동시 fan-out 압력을 줄인다. 전역 credit 예약·incoming frame 무손실·BP-03 수용 보장은 아니며 실제 실행으로 판정한다.

이는 RawNode의 synchronous Ready·WAL sync·Advance와 command별 fresh proof를 유지하는 내부 scheduling 선택이다. Queue 확대, timeout 연장, proof 공유·cache, outbound 자동 retry 또는 readiness 완화는 포함하지 않는다. `AsyncStorageWrites`는 storage 메시지와 영속화 후 응답 경로가 구현되지 않았으므로 별도 adapter 재설계·crash-ordering 검증 없이 켜지 않는다. 실제 시험과 BP-03의 유효 여부는 [성능 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)를 기준으로 하며 이 scoped 채택은 ADR-0002 전체를 Accepted로 바꾸지 않는다.

### 1.3 비동기 저장 경로 채택

사용자 지시 `제안한 방식대로 개선해보자`로 [ADR-0020](ADR-0020-asynchronous-raft-wal-group-commit.md)의 공식 `AsyncStorageWrites` adapter와 bounded WAL group commit을 채택한다. §1.1·§1.2의 synchronous scheduling 설명은 이전 구현의 기록이며 현재 실행 경계는 새 ADR을 따른다. Version pin·fresh proof·quorum 영속화 뒤 ACK는 유지한다. ADR-0002 전체와 BP-03 수용 상태는 이 설계 채택만으로 바꾸지 않는다.


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
Scoped file-backed WAL/snapshot target과 replicated application-state encoding family는 [ADR-0009](ADR-0009-raft-production-state-profile.md)에 기록한다. Production storage lifecycle과 exact persisted-state schema/compatibility는 별도 구현·contract·검증으로 남는다.

## 7. 검증

Phase 0 spike 및 Phase 2 session ownership core는 다음 검증 기준을 갖는다.

1. 3-node에서 leader kill 뒤 committed state가 유지된다(`TestRaftConsensusSpike/leader_failure_preserves_committed_state`).
2. Minority partition에서 새 write와 ownership이 commit되지 않는다(`TestRaftConsensusSpike/quorum_loss_cannot_commit`, `TestReplicatedSessionOwnership/minority_cannot_acquire_or_advance_epoch`).
3. WAL append, `HardState` 저장과 outbound message ordering을 fault injection으로 검증한다(`internal/adapters/raft` unit test).
4. Snapshot 중 crash 뒤 마지막 committed index로 복구한다(`TestRaftConsensusSpike/snapshot_plus_WAL_tail_restores_state`, `TestReplicatedSessionOwnership/restart_recovers_ownership_epoch_and_command_sequence`). Ownership assertion의 commit identifier는 Raft index가 아니라 replay한 ownership command bytes의 application sequence position이다.
5. WAL write 오류와 느린 follower flow-control에는 in-process partial evidence가 있다. `wal.bin` write에 `ENOSPC`를 주입하면 adapter가 outbound batch 없이 `ErrFailed`를 반환하고 terminal failed state에 머문다. Deterministic 3-node harness는 64개 command를 적용하는 동안 node 1→3의 미응답 entry-bearing `MsgApp` 고유 범위가 `MaxInflightMsgs=16`을 넘지 않고 연결 복구 후 세 node가 수렴함을 보인다(`TestNodeWALWriteFailureStopsOutbound`, `TestRaftConsensusSpike/slow_follower_keeps_outbound_append_window_bounded`). 빈 `MsgApp` commit update는 inflight entry window에 포함되지 않는다. 별도의 실제 tmpfs ENOSPC·FUSE slow-I/O Docker run은 readiness, MQTT admission, applied-index 진행/정지와 bounded peer queue를 검증했다([cluster spec §11.6](../specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping), run `storage-fault-20260925d`). 이 제한된 runtime evidence는 durable MQTT ACK, file/directory sync crash, torn-tail 복구 또는 backup/restore를 검증하지 않는다. Production fixed-voter transport/queue evidence는 [ADR-0008 §7](ADR-0008-raft-peer-transport.md#7-검증)에 별도로 기록한다.
6. Membership change 중 crash/restart에서 member set, operation phase와 readiness가 수렴한다(`TestDynamicClusterMembershipProcessAcceptance`, `TestDynamicClusterMembershipTransitionCrashRecovery`, `TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum`).
7. Snapshot finalize의 rename 전·후 process crash 뒤 restart가 복구하고, v1 backup restore 뒤 crash/restart 및 joiner resync가 수렴한다(`TestSnapshotFinalizationCrashRecoveryAcrossProcessRestart`, `TestClusterBackupRestoreProcessCrashAndResync`; [Cluster §11.7](../specs/cluster-session-and-delivery.md#117-cluster-backuprestore와-snapshot-finalize-수용-매핑)).

항목 1~4는 in-process deterministic harness와 adapter unit test에서 검증했다. 항목 5는 in-process failure/flow-control와 [실제 storage fault chaos run](../specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)의 제한된 evidence를 포함한다. 항목 6~7은 실제 `narumq` process acceptance로 local membership crash/restart, snapshot finalize, 3-voter backup/restore와 resync를 검증한다. 이 시험은 durable MQTT ACK, `offline`/`retained`, production EBS/power-loss, WAL torn-tail 보존 또는 N/N-1 호환성을 검증하지 않는다. Fixed-voter transport/queue는 별도 [ADR-0008](ADR-0008-raft-peer-transport.md)의 범위에서 구현·검증했지만 이 ADR의 전체 수용 기준은 미완료다. 따라서 이 결정은 `Proposed` 상태를 유지한다.

## 8. 재검토 조건

- Phase 0에서 required durability나 p99.9 목표를 충족하지 못한다.
- 유지보수·보안 상태가 release 기준을 충족하지 못한다.
- Storage·transport 책임 때문에 대안 library보다 결함과 구현 비용이 커진다.
- N-1↔N compatibility를 안전하게 유지할 수 없다.

## 9. 참고 자료

- [etcd/raft](https://github.com/etcd-io/raft)
- [Raft thesis](https://github.com/ongardie/dissertation/blob/master/stanford.pdf)
- [ADR-0001](ADR-0001-core-implementation-language.md)
