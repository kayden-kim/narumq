# ADR-0009: Raft production target의 상태 저장·인코딩·그룹 profile

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (design target only) |
| 결정일 | 2026-09-24 |
| 결정자 | NaruMQ 프로젝트 |
| 관련 요구사항 | [PRD](../requirements/PRD.md#92-클러스터와-세션) CLU-01, CLU-04~CLU-11 |
| 관련 스펙·데이터 계약 | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md), Draft [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md) |
| 관련 ADR | [ADR-0002](ADR-0002-cluster-consensus-library.md), [ADR-0003](ADR-0003-durable-ack-barrier.md), [ADR-0004](ADR-0004-session-ownership-and-fencing.md), [ADR-0008](ADR-0008-raft-peer-transport.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

이 ADR은 아래 네 가지 설계 선택만 채택한다. 현재 runtime의 production readiness, production crash recovery·내구성, durable MQTT ACK 또는 release N/N-1 호환성을 승인하지 않는다. 상위 [ADR-0002](ADR-0002-cluster-consensus-library.md), [ADR-0003](ADR-0003-durable-ack-barrier.md), [ADR-0004](ADR-0004-session-ownership-and-fencing.md)는 계속 `Proposed`이며, cluster architecture와 [release compatibility contract](../contracts/cluster-release-compatibility-v1.md)는 각각 `Target / Draft`, `Draft`로 남는다.

## 1. 결정

### 1.1 Consensus module 버전

Production target은 Go module `go.etcd.io/raft/v3 v3.7.0`으로 고정한다. 현재 [`go.mod`](../../go.mod)와 [`go.sum`](../../go.sum)에 기록된 checksum은 다음과 같다.

| 대상 | Checksum |
| --- | --- |
| Module zip (`go.sum`) | `h1:BGzlwx07bLv8PW6OU5HObuz1y4hlPZUXA07pM1mPUh4=` |
| Module manifest (`go.mod` checksum in `go.sum`) | `h1:6gX6T2X907DjnjsFLODnTxba77stjs84W9gTTI0GUNA=` |

이 pin은 선택한 production target version이지 production validation 증거가 아니다. 버전 변경은 supply-chain review와 persisted-state 및 wire의 N/N-1 호환성 시험을 거친 별도 결정이 있어야 한다. 이번 ADR은 dependency를 변경하지 않는다.

### 1.2 WAL·snapshot storage와 동기화

`internal/adapters/raft`가 소유하는 append-only file WAL과 immutable snapshot file을 선택한다. `etcd/raft` `RawNode`와 in-memory index는 process 안의 계산·조회 cache이며 system of record가 아니다. 기존의 synchronous Ready 처리 경계를 유지한다.

영속할 `HardState`(term·vote·commit), entries 또는 수신 snapshot이 하나라도 있는 Ready는 이를 하나의 checksummed WAL frame으로 기록하고 WAL 파일에 `os.File.Sync`를 호출한다.
셋 다 없는 Ready(outbound message, ReadState, 이미 기록된 committed entry의 apply만 있는 Ready)는 복구할 상태가 없으므로 frame을 쓰지 않고 sync도 호출하지 않는다. 그 message가 의존하는 상태는 이전 Ready에서 이미 sync되었다.
Dynamic cluster driver는 호출을 유발한 request 또는 inbound frame을 이미 처리한 뒤, 대기 중인 request와 inbound Raft frame을 최대 64개 추가로 Raft node에 적용하고 Ready를 처리한다. Inbound frame이 대기 중이면 한 frame을 먼저 처리한 뒤 나머지 공용 예산으로 request와 inbound frame을 staging해 request backlog가 Raft progress를 굶기지 않게 한다. 그래서 여러 proposal이 한 frame과 한 번의 sync로 영속된다(group commit, ADR-0003 §8). 이 호출이 성공하기 전에는 관련 outbound message를 내보내거나 `Advance`를 호출하거나 해당 영속화를 stable하다고 취급해서는 안 된다. Snapshot은 같은 filesystem directory 안의 임시 파일에 기록한 뒤 파일 `Sync`, close, atomic rename, 포함 directory `Sync` 순서로 확정한다. Snapshot을 durable하다고 선언하거나 WAL을 compact·폐기하기 전에 이 순서를 완료한다. WAL이나 metadata 파일을 새로 만들거나 rename할 때도 directory를 `Sync`한다.

Write, file sync, rename 또는 directory sync가 실패하면 해당 node는 fail-closed 상태가 되어야 한다. 실패한 Ready와 연결된 outbound batch나 ACK를 내보내서는 안 된다. 복구는 완전하고 checksum을 확인한 WAL frame과 finalized snapshot만 replay한다. Commit된 기록 또는 WAL 중간 기록의 손상, 지원하지 않는 format은 fail closed다. Torn tail은 미commit 상태만 안전하게 버릴 수 있고, 마지막 durable commit을 잃을 가능성이 있으면 자동 절단하지 말고 복구를 실패시켜야 한다. WAL truncation·retention은 복구 가능한 snapshot과 그 뒤의 committed tail을 보존해야 한다.

`File.Sync`와 directory sync는 지원 대상인 Linux local filesystem/EBS에서 요구하는 primitive다. Snapshot finalization과 dynamic cluster metadata는 file/directory sync를 수행한다. 신규 WAL 생성은 header file sync 뒤 parent directory sync를 수행하며, 매번 WAL을 open할 때 file과 parent directory를 다시 sync한다. Focused unit test는 신규 생성, 기존 WAL 재-open 및 directory sync 실패 뒤 같은 directory에서 재시도하는 경계를 확인한다. 이는 구현 순서의 검증이지 production volume의 power-loss 보장이 아니다. v1 backup/restore는 ADR-0011 범위에서 구현·수용 시험했으나, scheduled backup이나 전체 disaster-recovery 보장을 뜻하지 않는다.

기존 file-backed spike 패턴을 재사용하며 embedded database를 추가하지 않는다. `NMQWAL01`은 checksummed Ready frame을 WAL 파일에 sync하고, `NMQSNAP1`은 file `Sync`, atomic rename, 포함 directory `Sync`로 확정한다. 신규 WAL file의 directory sync는 구현·unit test 완료이며, 재-open도 file과 directory를 sync해 실패 뒤 retry에서 이름을 신뢰하지 않는다. Production WAL lifecycle·retention은 미완료다. `NMQSTATE1`은 fixed-voter 경로의 legacy opaque command-history snapshot이며, dynamic cluster는 Draft [`replicated-state-v1.proto`](../contracts/replicated-state-v1.proto)를 `records` codec으로 사용한다. 이 구현을 production format으로 승인하지 않으며 production volume/power-loss 검증도 남아 있다.

### 1.3 Replicated application command와 snapshot-state encoding

Replicated application command와 snapshot state에는 versioned `proto3` protobuf, generated Go bindings, 안정된 persisted bytes와 checksum을 위한 deterministic serialization을 선택한다. 이 선택은 [Active Raft peer transport v1](../contracts/raft-peer-transport-v1.md)의 Raft protobuf message와 별도 경계다. 또한 [`takeover`의 experimental JSON acquire codec](../../internal/application/takeover/codec.go)의 `schemaVersion=1` 및 `NMQSTATE1`의 opaque byte history를 production encoding으로 승격하지 않는다.

Draft [`replicated-state-v1.proto`](../contracts/replicated-state-v1.proto)와 `records` codec은 command schema major/minor·typed payload, snapshot schema·포함 index·object count·canonical state SHA-256을 이미 정의·검증하며, epoch 같은 `uint64` 값은 손실 없이 표현한다. Codec은 unknown field, 미지원 schema, 잘못된 count/digest를 거절한다. 이는 현재 Draft schema/codec 동작이지 검토된 cross-release compatibility 계약은 아니다. Application acceptance에서는 의미상 거절된 committed command도 sequence position을 소비해야 하며, optional addition은 검증된 N/N-1 지원 뒤에만 feature-gate를 해제한다.

이 기술 선택만으로 persisted-state의 cross-release compatibility는 승인되지 않는다. Draft schema는 [`replicated-state-v1.proto`](../contracts/replicated-state-v1.proto)에 있고 generated codec도 구현되어 있지만, 별도 contract의 size limits, migration 규칙과 release compatibility vectors는 미완료다. Schema와 local producer/consumer 검증만으로 persisted-state compatibility를 주장하지 않는다.

### 1.4 Consensus group partition profile

고정 voter `{1,2,3}`의 consensus group 하나가 모든 authoritative replicated cluster·session·message state를 소유한다. 동일 `(tenantId, clientId)`의 ownership, subscription, inflight aggregate 변경은 그 하나의 log에서 순서화하고 원자적으로 적용한다. Route index는 committed subscription으로부터 재생성하는 node-local projection이며 routing readiness 전에 복구한다. Membership 또는 configuration 변경 기능이 추가되면 그 변경도 같은 group에서 quorum commit한다.

이 profile은 key hash/shard routing과 cross-group transaction을 도입하지 않는다. 추가 group은 측정된 처리량·상태 크기 압력이 확인된 뒤 별도로 승인된 state migration·compatibility 설계가 있어야 검토한다. 이는 현재 spike의 단일 group보다 앞서 cross-group atomicity protocol을 발명하지 않기 위한 선택이다.

## 2. 배경

현재 제한된 Phase 0/2 evidence에는 `go.etcd.io/raft/v3 v3.7.0`, file-backed WAL/snapshot, opaque command bytes, 실험용 takeover JSON codec, 고정 3-voter peer transport가 있다. 그러나 이 사실만으로 네 production target 사이의 계약이나 실제 production readiness가 정해지지는 않는다. 저장소 선택은 복구 경계, command bytes는 release 호환성, group 경계는 session aggregate의 원자성에 직접 영향을 준다.

이 ADR은 그 네 target 선택을 기록하되, production 구현 완료와 기존 Proposed 결정의 전체 승인 사이에 경계를 둔다. 동작 요구와 시험의 기준은 [cluster spec](../specs/cluster-session-and-delivery.md), mixed-release 계약의 기준은 Draft [cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md)다.

## 3. 결정 요인

1. 한 node의 process 재시작과 disk 오류에서 마지막 durable commit을 보존할 복구 경계가 명확해야 한다.
2. Ready 처리 중 안정 저장 이전의 외부 전송·ACK를 막을 수 있어야 한다.
3. Persisted command와 snapshot bytes를 버전·형식·checksum 기준으로 검증할 수 있어야 한다.
4. 한 session aggregate의 fencing과 route-affecting 변경을 하나의 원자적 ordering으로 적용해야 한다.
5. 현재 spike evidence의 범위를 넘는 runtime, power-loss 또는 release compatibility 완료를 주장하지 않아야 한다.

## 4. 선택 근거

Pinned module은 현재 `go.mod`/`go.sum` 및 deterministic Raft spike가 사용하는 버전과 일치한다. 정확한 pin은 build 입력을 재현 가능하게 하지만, checksum 대조와 module 검증은 supply-chain/runtime 적합성이나 N/N-1 동작 시험을 대신하지 않는다.

File WAL은 Ready의 durable ordering을 NaruMQ adapter에서 통제하고 snapshot과 committed tail의 수명을 함께 설계하게 한다. 별도 embedded database보다 현재 file-backed spike와 가까워 추가 engine의 transaction 및 recovery semantics를 도입하지 않는다. 반면 directory metadata sync, crash point, tail 복구와 retention은 production 전 별도 구현·시험이 필요하다.

Generated protobuf와 deterministic serialization은 타입을 가진 application payload와 persisted bytes의 재현 가능한 표현을 목표로 한다. 현재 JSON acquire bytes는 실험용이며 opaque state history는 snapshot에서 application schema를 보장하지 않는다. 그러므로 protobuf 채택은 format의 존재 자체가 아니라 별도 contract와 migration을 요구한다.

단일 group은 session aggregate의 command ordering과 원자성을 하나의 log에서 유지한다. 여러 group은 처리량·격리 확장 여지가 있지만 동일 aggregate에 대한 cross-group transaction과 migration 문제를 지금 해결해야 하므로, 측정 근거 없이 선택하지 않는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Embedded database를 WAL/snapshot 엔진으로 사용 | Transaction, index와 일부 recovery 도구를 제공할 수 있다. | 별도 engine의 durability·compaction·backup 정책 및 failure mode를 추가로 검증해야 한다. | 현재 file-backed spike에서 필요한 append/Sync 경계를 직접 통제할 수 있으며, 측정된 필요 없이 engine을 추가하지 않는다. |
| Experimental JSON acquire bytes를 production command encoding으로 승격 | 기존 acquire codec을 재사용할 수 있다. | JSON codec은 acquire 한 종류에 한정되고 snapshot의 전체 typed state schema·migration·compatibility를 정의하지 않는다. | Typed replicated state를 위해 versioned protobuf를 선택하되 exact schema는 별도 Draft contract에서 결정한다. |
| 처음부터 여러 Raft group으로 key hash/shard 분산 | 여러 group에 부하를 나눌 여지가 있다. | 한 session aggregate를 원자 적용할 cross-group transaction, 재분배와 state migration을 추가해야 한다. | 현재 evidence에는 단일 group만 있으며 throughput/size pressure 측정 전에는 단일 group을 유지한다. |

## 6. 결과

### 긍정적 결과

- Raft dependency version, file-backed persistence target, application serialization family와 group count가 독립적으로 명시된다.
- Ready 저장, snapshot finalization, recovery와 fail-closed 동작의 기준을 production implementation 전에 비교할 수 있다.
- Session aggregate 변경이 하나의 consensus log를 공유하므로 cross-group transaction을 이 profile이 요구하지 않는다.

### 부정적 결과와 비용

- Local snapshot finalization과 process crash/recovery는 시험했지만 production WAL lifecycle·retention 및 지원 filesystem/EBS의 power-loss 검증은 남는다.
- Deterministic protobuf serialization만으로 N/N-1 호환성이 생기지 않으며 별도 schema contract, migration과 compatibility 시험이 필요하다.
- 단일 group은 처리량 확장 경계를 둔다. 확장 전 실측과 별도 migration 결정이 필요하다.
- Version pin과 storage/encoding profile만으로 production runtime, MQTT readiness, durable ACK, full disaster-recovery 또는 full CLU-14 완료를 뜻하지 않는다.

### 후속 작업

- Draft replicated-state contract의 migration limits, release compatibility vectors와 N/N-1 rules를 확정한다.
- Local directory sync/snapshot-finalize crash 및 3-voter backup restore acceptance는 [Cluster §11.7](../specs/cluster-session-and-delivery.md#117-cluster-backuprestore와-snapshot-finalize-수용-매핑)에서 통과했다. WAL torn-tail, retention 및 production EBS/power-loss 검증은 미완료다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거·상태 |
| --- | --- | --- | --- |
| Module pin이 저장소의 선택과 일치한다. | `go.mod`, `go.sum` 대조 및 `go mod verify`, `go mod download -json go.etcd.io/raft/v3@v3.7.0` 실행 | 버전과 양 checksum이 정확히 일치한다. | 2026-09-24: `go mod verify`가 `all modules verified`를 출력했고 module `Sum`/`GoModSum`이 `go.sum`과 일치했다([cluster spec §11.4](../specs/cluster-session-and-delivery.md#114-adr-0009-design-target-module-checksum-증거)). Module integrity 확인이며 production validation은 아니다. |
| WAL Ready ordering과 snapshot file 순서를 spike와 대조한다. | `stable_store.go`의 `appendReady`, `persistSnapshotFile`, WAL open/retry 흐름과 fault-injection unit test를 확인한다. | WAL frame file Sync가 outbound/Advance보다 앞서고 snapshot은 temp write·Sync·close·atomic rename·directory Sync를 수행한다. 실패한 WAL directory sync 뒤 retry도 file/parent directory를 다시 sync한다. | PASS: implementation/unit tests와 [§11.7 process acceptance](../specs/cluster-session-and-delivery.md#117-cluster-backuprestore와-snapshot-finalize-수용-매핑); `TestOpenStableStoreSyncsDirectoryWhenOpeningWAL`가 initial create, reopen, failed directory-sync retry를 검증한다. Production power-loss 보장은 아니다. |
| Production storage 장애·복구가 durable commit을 보존한다. | 실제 filesystem에서 WAL/snapshot crash point, torn tail, disk full, file/directory sync 오류 주입 및 reopen 시험 | 마지막 durable commit 보존, 손상·호환 불가 state fail-closed, 오류 Ready의 outbound/ACK 없음 | Local evidence PASS: mounted FUSE acceptance와 Docker run `storage-power-loss-20260930g`의 여섯 scenario. Sync-error reopen fail-closed(exit 1), held fsync torn cut(`node/wal.bin`, 16 unsynced bytes 중 8 보존), delivery 복구와 simultaneous three-node cut 뒤 `SessionPresent=true`, index 39→42 수렴을 확인했다. PUBLISH write 뒤 `awaitAck` 전 보류한 `qos1-05`의 PUBACK 결과만 unknown으로 기록하고 known timeout/closed outcomes는 구분했다([Cluster §11.6·§11.13](../specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)). 이 evidence는 production Linux filesystem/EBS 및 physical power-loss를 시험하지 않으므로 production criterion은 미통과다. |
| Local snapshot crash와 v1 backup/restore가 commit boundary를 유지한다. | Snapshot-finalize failpoint process restart와 online archive→오프라인 3-voter restore→node restart/resync 시험 | committed snapshot/tail 및 state summary가 복구되고 readiness가 quorum 수렴 뒤에만 성공한다. | PASS: [Cluster §11.7](../specs/cluster-session-and-delivery.md#117-cluster-backuprestore와-snapshot-finalize-수용-매핑)의 두 process test. 범위는 동일 cluster의 정확한 3 bootstrap voter이며 learner archive는 거절한다. |
| Production power-loss와 전체 CLU-14 state가 안전하다. | Production-like Linux filesystem/EBS power-loss, `retained`/`offline` count/checksum·restore, volume-loss 및 restore retention 시험 | 마지막 durable commit 및 모든 required state 복구, checksum/object count 일치, 잘못된 node의 readiness 차단 | 미실행. Process crash와 mounted Docker/FUSE power-cut simulation은 실제 전원/volume 손실 evidence가 아니다. 전체 CLU-14, production EBS, retention/RPO/RTO는 미완료다. |
| 3/2 quorum에서 commit 이전 성공 ACK가 없다. | Cluster spec §11.13의 replica kill, WAL/commit process crash, leader/follower partition과 storage-fault/power-loss chaos acceptance | 기존 ADR-0003 기준을 충족하고 committed ACK state의 message gap이 없다. | PASS: process acceptance에서 세 replica의 QoS 1/2 ACK 직후 kill은 message count=1, 20 crash points와 두 partition cases도 통과했다. Docker run `storage-power-loss-20260930g`의 여섯 scenario도 disk-full/quorum-loss/slow-I/O/sync-error ACK barrier, torn cut과 simultaneous cluster power-cut 뒤 ACKed delivery를 통과했다. In-flight `qos1-05` PUBLISH write는 성공했지만 PUBACK 관찰이 끊겨 unknown으로 기록했으며 commit 상태를 부정하지 않았다. Known timeout/closed outcomes는 unknown evidence에 포함하지 않는다([Cluster §11.6·§11.13](../specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)). BP-03 및 Linux/EBS·physical power-loss는 별도 미실행이며 ADR-0003은 `Proposed` 상태다. |
| State codec와 release N/N-1이 호환된다. | Draft machine-readable state contract 및 양방향 producer/consumer·snapshot compatibility 시험 | Unknown required record와 지원하지 않는 major는 readiness를 차단하고 N/N-1 행을 검증한다. | 미실행. Release matrix는 Draft이며 미검증 상태를 유지한다. |
| 단일 group이 session aggregate ordering을 보존한다. | 동일 session key의 ownership/subscription/inflight command를 한 log로 적용하고 committed projection을 rebuild하는 통합 시험 | aggregate 변경의 동일 ordering·원자성 및 route readiness 전 rebuild | PASS: `TestClusterSnapshotAndWALTailReplayRestoresSessionAggregateAndRouteProjection`은 production protobuf command와 typed snapshot/tail을 복원하고, `TestDynamicClusterPersistentSubscriptionReplayProcessAcceptance`는 전체 process crash/restart, first-ready joiner projection과 joiner ingress routing을 확인했다([Cluster §11.14](../specs/cluster-session-and-delivery.md#1114-persistent-subscriptioninflight-replay와-route-projection-rebuild-수용-매핑)). 이는 Linux/EBS power-loss, N/N-1 compatibility 또는 drain 검증이 아니다. |
BP-03 구현 단위 검증: `go test ./internal/adapters/raft -count=1`과 `go test ./cmd/narumq -count=1` PASS. `TestProcessReadySkipsWALFrameWithoutPersistentState`는 message/ReadState-only Ready에서 WAL frame·sync가 생기지 않음을, `TestStageQueuedDriverRequestsStopsAtLimit`는 staged request cap과 후속 FIFO 처리를 확인한다. `TestClusterDurableQueueDefersPromotionWhenQuorumUnavailable`와 TCP delivery writer retry 회귀 시험도 해당 package suite에서 PASS했다. Storage chaos run `storage-power-20260930-walskip3`의 여섯 scenario는 PASS했다([Cluster §11.6](../specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)). 반면 BP-03 compare `bp03-20260930T123200.653059500Z-32492`의 NaruMQ workload는 invalid였으며 suite를 실행하지 않았으므로 group-commit 성능 acceptance는 미완료다([성능 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)).

## 8. 재검토 조건

- Supply-chain review 또는 persisted-state·wire N/N-1 시험에서 module pin의 유지가 부적절하다고 확인된다.
- Supported filesystem/volume에서 요구한 `Sync` 순서로 마지막 durable commit을 보존할 수 없다는 crash evidence가 나온다.
- Typed state의 별도 contract가 기존 experimental data의 migration 불가능성 또는 다른 encoding 필요성을 입증한다.
- 측정된 throughput·state size가 단일 group 한계를 지속해서 넘고, aggregate consistency를 유지하는 승인된 migration 설계가 준비된다.
