# ADR-0011: Cluster backup v1은 commit 경계 archive와 오프라인 voter restore를 사용한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Superseded (archive format/scope만; v1의 boundary capture와 offline restore 범위는 historical record) |
| 결정일 | 2026-09-26 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD CLU-14](../requirements/PRD.md#92-클러스터와-세션) |
| 관련 스펙·데이터 계약 | [Cluster §10·§11.7](../specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore), Superseded [Cluster backup v1](../contracts/cluster-backup-v1.md), Superseded [Cluster backup v2](../contracts/cluster-backup-v2.md), Draft [Cluster backup v3](../contracts/cluster-backup-v3.md) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md) |
| 상위 결정 | [ADR-0002](ADR-0002-cluster-consensus-library.md) (`Proposed` 유지), [ADR-0003](ADR-0003-durable-ack-barrier.md) (`Proposed`), [ADR-0009](ADR-0009-raft-production-state-profile.md) (design target only) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | [ADR-0012](ADR-0012-retained-replication-and-cluster-backup-v2.md) (archive format과 retained scope만) |

## 1. 결정

- 실행 중인 cluster의 online backup은 ClusterAdmin 전용 `POST /api/v1/cluster/backups`에서 fresh quorum/read barrier를 확인한 뒤 하나의 commit 경계에 맞춘 typed snapshot과 committed tail을 반환한다.
- Archive는 versioned deterministic protobuf frame, per-frame CRC32C, trailer SHA-256과 commit boundary state summary를 사용한다. v1의 byte 형식은 [historical contract](../contracts/cluster-backup-v1.md), 현재 형식은 [Cluster backup v3](../contracts/cluster-backup-v3.md)다.
- Restore는 offline CLI `narumq -cluster-config <file> -cluster-restore <archive>`로 수행한다. 현재 bootstrap 구성과 archive는 같은 cluster ID, release/protocol/schema 및 정확히 동일한 3 voter ID와 member identity를 가져야 한다. Archive에 learner ID가 있으면 거절한다.
- Restore는 각 voter의 별도 빈 data directory를 seed한 뒤 종료한다. `restore-incomplete` marker가 있는 node는 일반 startup에서 READY가 되지 않는다.
- v1 archive scope는 members, tenant-derived identifiers, session ownership, subscriptions, QoS inflight/checkpoints 및 membership history다. `offline`/`retained`는 제외하며 전체 CLU-14는 미완료로 유지한다.

## 2. 배경

CLU-14는 snapshot·backup·restore와 checksum/object count 검증을 요구한다. 기존 local Raft snapshot은 application state와 backup file contract, online consistent boundary 또는 restore 운영 명령을 제공하지 않았다. 단순 파일 복사는 commit 경계, archive identity, state 검증 또는 interrupted restore의 readiness를 규정하지 못한다.

현재 cluster state 경로는 모든 MQTT 상태를 복제하지 않는다. 따라서 v1은 구현된 typed replicated kinds만 기록하고 지원하지 않는 `offline`/`retained` 종류를 manifest에 명시한다. 이 구분으로 archive가 전체 broker state를 보존한다고 오해하지 않도록 한다.

## 3. 결정 요인

1. Archive는 snapshot과 이후 committed entry를 동일한 Raft commit 경계로 복구해야 한다.
2. 손상, schema mismatch, cluster identity mismatch는 target data를 변경하기 전에 검출해야 한다.
3. voter identity와 certificate binding을 바꾸지 않으며 learner 복원을 지원하지 않아야 한다.
4. interrupted durable restore 뒤 일반 startup이 READY가 되면 안 된다.
5. 구현하지 않은 state kind와 power-loss/RPO 보장을 완료로 과장하지 않아야 한다.

## 4. 선택 근거

Fresh quorum/read barrier 이후 capture한 snapshot과 committed command/configuration tail은 online write가 계속되는 동안 archive의 boundary를 한 점으로 고정한다. `boundary_state`의 전체/kind별 count와 SHA-256은 restore 뒤 replay 결과를 검사한다. CRC32C는 개별 frame 손상을, trailer SHA-256은 archive 전체 framing 손상을 검출한다.

Offline restore는 실행 중인 node와 복구 대상 directory의 경쟁을 없앤다. Restore code는 archive와 target identity를 먼저 검증하고 durable marker를 설치한 뒤 snapshot/WAL/meta를 publish한다. Marker는 cluster metadata보다 먼저 startup에서 확인되므로 interrupted restore는 fail-closed된다.

Exactly-three voter 제한은 현재 bootstrap configuration과 함께 복구할 수 있는 범위를 보수적으로 고정한다. Archive에 learner가 있는 경우 partial membership transition을 재개하거나 추측하지 않고 거절한다. 다른 voter 수, membership migration과 different-cluster restore는 이 결정에 포함하지 않는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Raft snapshot/WAL 파일을 복사 | 구현이 간단해 보인다. | Commit boundary, checksum, cluster identity, version과 restore atomicity를 외부에서 검증하지 못한다. | Copy 자체를 backup 계약으로 취급하지 않는다. |
| 실행 중인 각 node에서 독립적으로 파일을 archive | Cluster를 중지하지 않아도 된다. | 서로 다른 node/index의 snapshot과 WAL이 섞여 하나의 committed boundary를 보장하지 못한다. | Fresh quorum boundary에서 한 archive를 생성한다. |
| Online restore로 live voter를 교체 | Operator 단계가 줄어든다. | Membership, disk ownership, data seed와 peer reconnect를 한 API transaction으로 만들며 부분 실패 경계가 불분명하다. | Offline restore 후 명시적으로 startup한다. |
| Learner 또는 임의 voter 수 복원 지원 | 복원 대상 유연성이 높다. | Dynamic membership 중 ConfState와 bootstrap identity의 안전성을 추가로 증명해야 한다. | v1은 현재 bootstrap 3-voter 집합으로 제한하고 learner archive를 거절한다. |

## 6. 결과

### 긍정적 결과

- Online backup과 offline restore가 동일한 versioned archive contract를 공유한다.
- Restore는 snapshot만이 아니라 committed tail을 재생하고 boundary summary를 비교한다.
- Process restart 후 committed stage operation이 유지되고, restored voters가 quorum READY에 도달한 뒤 joiner가 catch up할 수 있음을 acceptance 시험으로 확인한다.
- Snapshot crash point는 rename 전과 rename/directory sync 이후를 구분해 process restart acceptance로 검증한다.

### 부정적 결과와 비용

- v1 archive는 `offline`과 `retained` message를 제외했다. [ADR-0012](ADR-0012-retained-replication-and-cluster-backup-v2.md)가 retained replication과 backup v2를 별도로 채택했으며, offline queue는 계속 미포함이다.
- Restore는 정확히 세 current bootstrap voter만 지원한다. Learner가 포함된 archive, voter ID 변경, cross-cluster migration은 거절된다.
- Local process crash 시험은 Linux EBS/실제 전원 손실을 대체하지 않는다. Directory `Sync` 순서의 production power-loss 보장은 미검증이다.
- 보존 기간 및 disaster-recovery RPO/RTO는 `SPEC-TBD-09`로 남는다. ADR-0002/0003도 각 범위의 미완료 기준 때문에 `Proposed` 상태를 유지한다.

### 후속 작업

- Offline queue replication/archive와 전체 CLU-14 object count/checksum 기준을 구현·시험한다.
- Production-like Linux filesystem/EBS에서 power-loss, volume loss와 backup retention을 검증한다.
- Release 간 archive compatibility matrix와 migration/rollback 정책을 정한다.

## 7. 검증
아래 검증 결과는 v1 구현 당시 기록한 historical evidence다. Clean cutover 뒤 current records/process tests는 v2를 검증하며 v1 archive를 다시 실행한 결과가 아니다.


| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Archive framing, canonical protobuf, CRC/SHA-256과 commit ranges가 검증된다. | `go test ./internal/adapters/raft/records -count=1` | 유효 archive round-trip, checksum·unknown field·noncanonical·잘못된 range 거절 | PASS: `internal/adapters/raft/records.TestBackupArchiveRoundTripAndChecksums`, `TestBackupArchiveRejectsCorruptAndNoncanonicalFrames`, `TestBackupArchiveRejectsInvalidManifestAndEntryRanges` |
| Restore target를 건드리기 전에 identity와 boundary가 검증되고 crash marker가 startup을 막는다. | `go test ./cmd/narumq/... ./internal/adapters/raft/... -count=1` | 잘못된 archive/identity 거절, foreign file 보존, partial restore 뒤 open 실패, 유효 seed open | PASS: cmd/narumq 및 raft adapter package test 전체 |
| Online archive가 staged command tail을 포함하고 restore/restart/resync 후 동작한다. | `go test ./tests/integration -run '^TestClusterBackupRestoreProcessCrashAndResync$' -count=1` | archive commit index가 staged operation 이상이며 세 voter restore, crash/restart, joiner resync 후 READY/convergence | PASS: 전체 membership/process 회귀 명령에서 확인; 상세는 [Cluster §11.7](../specs/cluster-session-and-delivery.md#117-cluster-backuprestore와-snapshot-finalize-수용-매핑) |
| Rename 전·후 snapshot crash에서 restart가 수렴한다. | `go test ./tests/integration -run '^TestSnapshotFinalizationCrashRecoveryAcrossProcessRestart$' -count=1` | 두 failpoint exit 뒤 snapshot 재생성/재생 및 cluster READY | PASS: failpoint process acceptance; 상세는 [Cluster §11.7](../specs/cluster-session-and-delivery.md#117-cluster-backuprestore와-snapshot-finalize-수용-매핑) |
| Production storage power-loss, offline queue와 전체 CLU-14 object set이 지원되는가. | Production-like EBS, offline restore 및 full state acceptance | crash/power-loss 뒤 모든 요구 state와 boundary checksum/object count 일치 | 미실행; 이 ADR의 Accepted 범위 밖. Retained replication/v2 backup의 제한된 process evidence는 [ADR-0012](ADR-0012-retained-replication-and-cluster-backup-v2.md) §7에 기록 |
