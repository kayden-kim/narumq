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

각 Ready에서 `HardState`, entries, 수신 snapshot을 하나의 checksummed WAL frame으로 기록하고 WAL 파일에 `os.File.Sync`를 호출한다. 이 호출이 성공하기 전에는 관련 outbound message를 내보내거나 `Advance`를 호출하거나 해당 영속화를 stable하다고 취급해서는 안 된다. Snapshot은 같은 filesystem directory 안의 임시 파일에 기록한 뒤 파일 `Sync`, close, atomic rename, 포함 directory `Sync` 순서로 확정한다. Snapshot을 durable하다고 선언하거나 WAL을 compact·폐기하기 전에 이 순서를 완료한다. WAL이나 metadata 파일을 새로 만들거나 rename할 때도 directory를 `Sync`한다.

Write, file sync, rename 또는 directory sync가 실패하면 해당 node는 fail-closed 상태가 되어야 한다. 실패한 Ready와 연결된 outbound batch나 ACK를 내보내서는 안 된다. 복구는 완전하고 checksum을 확인한 WAL frame과 finalized snapshot만 replay한다. Commit된 기록 또는 WAL 중간 기록의 손상, 지원하지 않는 format은 fail closed다. Torn tail은 미commit 상태만 안전하게 버릴 수 있고, 마지막 durable commit을 잃을 가능성이 있으면 자동 절단하지 말고 복구를 실패시켜야 한다. WAL truncation·retention은 복구 가능한 snapshot과 그 뒤의 committed tail을 보존해야 한다.

`File.Sync`와 directory sync는 지원 대상인 Linux local filesystem/EBS에서 요구하는 primitive다. 구체 filesystem·volume의 power-loss 보장은 production과 유사한 환경에서 별도 검증해야 한다. 주기적 compaction이나 backup이 구현·시험되었다고 주장하지 않는다.

기존 file-backed spike 패턴을 재사용하며 embedded database를 추가하지 않는다. 현재 `NMQWAL01`, `NMQSNAP1`, `NMQSTATE1` 형식은 directory sync, production WAL lifecycle과 typed application state가 빠져 있으므로 production format으로 부르지 않는다. 참고 구현은 [`stable_store.go`](../../internal/adapters/raft/stable_store.go)이며, 이 ADR은 그 구현의 production 적합성을 승인하지 않는다.

### 1.3 Replicated application command와 snapshot-state encoding

Replicated application command와 snapshot state에는 versioned `proto3` protobuf, generated Go bindings, 안정된 persisted bytes와 checksum을 위한 deterministic serialization을 선택한다. 이 선택은 [Active Raft peer transport v1](../contracts/raft-peer-transport-v1.md)의 Raft protobuf message와 별도 경계다. 또한 [`takeover`의 experimental JSON acquire codec](../../internal/application/takeover/codec.go)의 `schemaVersion=1` 및 `NMQSTATE1`의 opaque byte history를 production encoding으로 승격하지 않는다.

향후 각 command에는 schema major/minor, command type과 typed payload가 명시되어야 한다. `uint64` epoch와 session identity는 값 손실 없이 표현한다. 문법적으로 유효하지만 의미상 거절되는 committed command도 application sequence position을 소비한다. Snapshot에는 schema version, 포함된 committed index, object count와 canonical state representation의 SHA-256 digest가 있어야 한다. 알 수 없는 required command 또는 schema major, 잘못된 object count나 digest는 recovery/readiness를 실패시켜야 한다. Optional addition은 N/N-1 지원 조건을 확인하고 feature-gate 뒤에만 활성화한다.

이 기술 선택만으로 exact protobuf message, field number, size limit, experimental file migration이나 byte vector는 승인되지 않는다. Production codec 구현 전에 [Contract 작성 규칙](../contracts/README.md)에 따라 Draft machine-readable replicated-state contract를 정의해야 한다. 해당 contract와 producer/consumer 검증 전에는 persisted-state compatibility를 주장하지 않는다.

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

- Directory sync 및 crash recovery를 포함한 production storage 구현과 power-loss 검증이 남는다.
- Deterministic protobuf serialization만으로 N/N-1 호환성이 생기지 않으며 별도 schema contract, migration과 compatibility 시험이 필요하다.
- 단일 group은 처리량 확장 경계를 둔다. 확장 전 실측과 별도 migration 결정이 필요하다.
- 세 선택 모두 production runtime, MQTT readiness, durable ACK, backup/DR 또는 membership workflow 완료를 뜻하지 않는다.

### 후속 작업

- Draft replicated-state contract에서 exact protobuf fields, limits, vectors, migration과 N/N-1 rules를 정의한다.
- Storage 구현에서 directory sync, crash/torn-tail, WAL retention과 snapshot restore를 검증한다.
- Backup/DR, 실제 disk-full·slow-I/O, quorum ACK와 release N/N-1 acceptance는 연결된 스펙의 미완료 수용 시험으로 유지한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거·상태 |
| --- | --- | --- | --- |
| Module pin이 저장소의 선택과 일치한다. | `go.mod`, `go.sum` 대조 및 `go mod verify`, `go mod download -json go.etcd.io/raft/v3@v3.7.0` 실행 | 버전과 양 checksum이 정확히 일치한다. | 2026-09-24: `go mod verify`가 `all modules verified`를 출력했고 module `Sum`/`GoModSum`이 `go.sum`과 일치했다([cluster spec §11.4](../specs/cluster-session-and-delivery.md#114-adr-0009-design-target-module-checksum-증거)). Module integrity 확인이며 production validation은 아니다. |
| WAL Ready ordering과 snapshot file 순서를 기존 spike와 대조한다. | `stable_store.go`의 `appendReady` 및 `persistSnapshotFile` 흐름 검토 | WAL frame file Sync가 outbound/Advance보다 앞서고 snapshot은 temp write·Sync·close·rename을 사용한다. | 코드 경계 대조는 설계 근거일 뿐 directory sync와 production lifecycle의 증거가 아니다. |
| Production storage 장애·복구가 durable commit을 보존한다. | 실제 filesystem에서 WAL/snapshot crash point, torn tail, disk full, file/directory sync 오류 주입 및 reopen 시험 | 마지막 durable commit 보존, 손상·호환 불가 state fail-closed, 오류 Ready의 outbound/ACK 없음 | 제한된 chaos evidence: run `storage-fault-20260925d`는 tmpfs 실제 ENOSPC 및 FUSE slow-I/O에서 `FAILED`/`DEGRADED`, readiness·MQTT admission, peer queue와 restart/recovery를 검증했다([cluster spec §11.6](../specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)). Durable commit 손실 부재, torn tail, file/directory sync 오류 및 ACK barrier는 미실행이므로 이 기준은 미통과 상태다. |
| Snapshot, backup/restore와 power-loss 경계가 안전하다. | Production-like Linux local filesystem/EBS에서 snapshot crash, backup/restore와 power-loss 시험 | durable snapshot과 committed tail을 복구하고 checksum·object count 검증, readiness 차단 조건 충족 | 미실행. Volume power-loss 보장은 별도 증거가 필요하다. |
| 3/2 quorum에서 commit 이전 성공 ACK가 없다. | Cluster spec과 ADR-0003 수용 시험에서 node별 kill, quorum loss와 storage failure 검증 | 기존 ADR-0003 기준을 충족하고 committed ACK state의 message gap이 없다. | 미실행. ADR-0003은 `Proposed`, MQTT 경로도 미연결이다. |
| State codec와 release N/N-1이 호환된다. | Draft machine-readable state contract 및 양방향 producer/consumer·snapshot compatibility 시험 | Unknown required record와 지원하지 않는 major는 readiness를 차단하고 N/N-1 행을 검증한다. | 미실행. Release matrix는 Draft이며 미검증 상태를 유지한다. |
| 단일 group이 session aggregate ordering을 보존한다. | 동일 session key의 ownership/subscription/inflight command를 한 log로 적용하고 committed projection을 rebuild하는 통합 시험 | aggregate 변경의 동일 ordering·원자성 및 route readiness 전 rebuild | 설계 선택만 기록. Production application wiring과 시험은 미완료다. |

## 8. 재검토 조건

- Supply-chain review 또는 persisted-state·wire N/N-1 시험에서 module pin의 유지가 부적절하다고 확인된다.
- Supported filesystem/volume에서 요구한 `Sync` 순서로 마지막 durable commit을 보존할 수 없다는 crash evidence가 나온다.
- Typed state의 별도 contract가 기존 experimental data의 migration 불가능성 또는 다른 encoding 필요성을 입증한다.
- 측정된 throughput·state size가 단일 group 한계를 지속해서 넘고, aggregate consistency를 유지하는 승인된 migration 설계가 준비된다.
