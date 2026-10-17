# Cluster session과 durable delivery 검증 기록

이 문서는 [Cluster session과 durable delivery 스펙](../specs/cluster-session-and-delivery.md) §11의 **과거 실행 증거**다. 아래 통과는 당시 소스·환경의 결과이며 현재 코드의 통과를 뜻하지 않는다.

- 수용 기준·기대 결과는 스펙이 기준이다. 시나리오별 테스트 연결은 소스의 `Scenarios:` 메타 정보에서 관리한다.
- 현재 연결·실행 결과는 [테스트 컨벤션](../conventions/test-conventions.md)에 따라 생성하는 [시나리오 보고서](../../.artifacts/verification/scenarios/report.md)를 따른다. 결과 입력 없는 연결 검사는 테스트 통과가 아니다.
- 전체 local gate는 미통과다. `CLU-SCN-241`의 과거 Windows WAL 교체 실패와 `CLU-SCN-017`의 결합 실행 timeout은 보존한다. Windows 파일 공유 잠금 수정 뒤 집중 검증은 [후속 검증](#2026-10-17-windows-파일-공유-잠금-후속-검증)에 구분한다.
- EKS P1·T1/T2·R1의 채택 근거는 §11.20에 있다. 이 결과로 이전 `FAILED/UNRESOLVED`를 덮어쓰지 않는다. 백업의 지속 RPO·실제 30일 경과 보존·production 데이터 크기와 업로드 실패 matrix는 [TODO](../TODO.md)의 `SPEC-TBD-09`가 남은 범위다.

절 번호와 시나리오 ID 범위는 스펙과 같다. 같은 기능의 중간 재실행·과거 전체 local PASS·작업 중 수치 대신 의미 있는 결과, 실패 경계, 명령과 증거 위치를 남겼다. 명령은 별도 표시가 없으면 저장소 root 기준이다. `GOMAXPROCS=...` 등은 실행 환경값이며 Windows에서는 child process 환경으로 전달했다.

## 11.1 Phase 0 gate 4 deterministic spike mapping

스펙: [11.1](../specs/cluster-session-and-delivery.md#111-phase-0-gate-4-deterministic-spike-mapping). 대상: `CLU-SCN-001`~`CLU-SCN-006`.

`TestRaftConsensusSpike`와 `TestNodeWALWriteFailureStopsOutbound`의 in-process spike 기록이다. CLU-01/02/06/07/09/10, ADR-0001 gate 4와 ADR-0002 §7의 skeleton·local recovery 범위다.

주입한 `ENOSPC`는 `filePersistenceOps.write` 오류이며 실제 파일시스템 용량 고갈이 아니다. 느린 follower는 in-process route 차단이다. 실제 peer transport·느린 I/O·production queue 전체·readiness·MQTT durable ACK 또는 cluster 전체 수용의 증거가 아니다. 실제 storage chaos는 §11.6, EKS는 §11.20과 구분한다.

## 11.2 Phase 2 replicated session ownership core mapping

스펙: [11.2](../specs/cluster-session-and-delivery.md#112-phase-2-replicated-session-ownership-core-mapping). 대상: `CLU-SCN-007`~`CLU-SCN-011`.

`TestReplicatedSessionOwnership`의 in-process ownership/fencing core 기록이다(CLU-04/05/10, ADR-0004 §8 항목 1~5). Production transport·durable ACK는 범위 밖이다. 실제 MQTT CONNECT와 다중 process socket 종료는 §11.12, persistent replay·projection은 §11.14의 별도 증거다.

## 11.3 Phase 0 gate 4 production peer transport mapping

스펙: [11.3](../specs/cluster-session-and-delivery.md#113-phase-0-gate-4-production-peer-transport-mapping). 대상: `CLU-SCN-012`~`CLU-SCN-016`.

Fixed-voter transport v1의 wire/framing, mTLS 거절, bounded queue, 3-node TLS replication/reconnect와 peer-only lifecycle의 기록이다. 실행 명령:

```text
go test ./internal/adapters/tcp -run '^TestPeerTransport' -count=1
go test ./tests/integration -run '^TestRaftPeerTransport' -count=1
go test ./cmd/narumq -run '^(TestProcessRaftMessagesContinuesOtherPeersAndSkipsRejectedFIFO|TestRaftPeerDriverTicksQueuesAndStops)$' -count=1
go test ./cmd/narumq -run '^(TestRaftPeerOnlyRejectsOtherFlags|TestRaftPeerOnlyStartsThreePeersAndStops)$' -count=1
```

이 범위는 dynamic membership, release N/N-1, production snapshot transfer·storage, MQTT replicated state·durable ACK·readiness의 수용을 대신하지 않는다. Peer-only executable을 전체 production broker cluster로 해석하지 않는다.

## 11.4 ADR-0009 design target과 module checksum 증거

스펙: [11.4](../specs/cluster-session-and-delivery.md#114-adr-0009-design-target과-module-checksum-증거). 대상: `CLU-SCN-017`.

2026-09-24 `go mod verify`는 `all modules verified`를 출력했다. `go mod download -json go.etcd.io/raft/v3@v3.7.0` 결과:

| 항목 | 값 |
| --- | --- |
| Version | `v3.7.0` |
| `Sum` | `h1:BGzlwx07bLv8PW6OU5HObuz1y4hlPZUXA07pM1mPUh4=` |
| `GoModSum` | `h1:6gX6T2X907DjnjsFLODnTxba77stjs84W9gTTI0GUNA=` |

이는 `go.sum`과 [ADR-0009](../decisions/ADR-0009-raft-production-state-profile.md)의 module 선택 근거다. Power-loss, snapshot recovery, backup/restore, quorum ACK나 release 호환성 증거가 아니다. 2026-10-17 재실행의 timeout·단독 통과는 마지막 절을 따른다.

## 11.5 Dynamic membership·restart convergence·MQTT readiness mapping

스펙: [11.5](../specs/cluster-session-and-delivery.md#115-dynamic-membershiprestart-convergencemqtt-readiness-mapping). 대상: `CLU-SCN-018`~`CLU-SCN-025`.

세 process acceptance가 통과했다. Voter 교체·idempotency·identity/compatibility 거절, transition 경계 crash/restart와 readiness/projection 복구, quorum 복귀 후 planned-leave handoff를 확인했다.

```text
go test ./tests/integration -run '^(TestDynamicClusterMembershipProcessAcceptance|TestDynamicClusterMembershipTransitionCrashRecovery|TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum)$' -count=1
go test -race ./tests/integration -run '^TestDynamicClusterMembershipTransitionCrashRecovery$' -count=2
```

근거는 [ADR-0010 §7](../decisions/ADR-0010-dynamic-cluster-membership.md#7-검증)과 해당 실행 기록이다. 빈 Raft entry 뒤 typed state index를 전진시켜 snapshot 복구를 유지하는 회귀도 포함했다. Storage·backup·durable ACK·release 호환성은 이 실행의 범위가 아니다. 같은 주소의 인증 순서 결함(`CLU-SCN-021`)은 §11.20의 ADR-0061 결과로 구분한다.

## 11.6 Storage fault chaos mapping

스펙: [11.6](../specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping). 대상: `CLU-SCN-026`~`CLU-SCN-031`.

Docker/FUSE `local-fuse-v1`의 disk-full, quorum disk-full, slow I/O, sync-error, follower torn-cut, 동시 three-node cut 기록이다. `storage-power-loss-20260930g`에서 여섯 scenario가 통과했으며 ACK, in-flight `unknown`, cut·recovery 결과는 `.artifacts/chaos/cluster-storage-v1/storage-power-loss-20260930g/summary.json`과 scenario별 `events.jsonl`에 남아 있다.

보존할 실패 경계:

| 실행 | 결과·제약 |
| --- | --- |
| `storage-power-20260930-walskip3` 이전 slow-I/O | Subscriber EOF는 durable SEND quorum 실패가 writer의 연결 종료로 이어진 문제였다. Local queue head와 TCP 연결을 보존한 뒤 통과했다. |
| `storage-power-20260930-walskip4` | Quorum 복구 뒤 persistent client의 CONNACK `0003`으로 실패, 나머지 5개 통과. 다음 `walskip5`는 6/6 통과했지만 앞선 거절의 원인은 미확정이다. |
| `storage-flow-20261002` | 5/6 통과. Slow-I/O의 HEALTHY·READY와 index 373 수렴 뒤 leader의 sentinel CONNECT가 `0003`으로 거절됐다. Delivery 판정 전 중단이므로 빈 missing/duplicate 배열은 성공 근거가 아니다. 원인은 미확정이다. |

이 실패들은 각 run의 `summary.json`, scenario event와 node log에 남아 있다. 후속 통과를 원인 규명이나 readiness 정책 완화의 증거로 쓰지 않는다.

Immutable image ID 보완 뒤 2026-10-03 회귀는 exit 0·6/6 통과였다:

```text
go run ./tests/chaos/cluster-storage-v1 --run-id fuse-ext4-final-20261003 --output .artifacts/chaos/cluster-storage-v1/fuse-ext4-final-20261003
```

증거는 해당 directory의 `summary.json`, manifest, `events.jsonl`·node log다. 이 실행은 FUSE 오류·held fsync·torn rollback·process cut의 simulation이며 실제 OS cache/EBS 전원 손실은 아니다. ACK를 관측하지 못한 in-flight 결과는 `unknown`으로 남긴다. Ext4는 §11.23, disk-pressure를 추가한 7개 scenario 회귀는 §11.28, 실제 EKS P1/T1/T2는 §11.20을 따른다.

## 11.7 Cluster backup/restore와 snapshot finalize 수용 매핑

스펙: [11.7](../specs/cluster-session-and-delivery.md#117-cluster-backuprestore와-snapshot-finalize-수용-매핑). 대상: `CLU-SCN-032`~`CLU-SCN-036`.

Archive 검증, offline restore·crash/restart·resync와 snapshot finalize failpoint를 확인했다. 아래 다섯 process 시험의 `-parallel=2 -count=3` 실행은 모두 통과했다(package 190.857s).

```text
go test ./tests/integration -parallel=2 -run '^(TestClusterBackupRestoreProcessCrashAndResync|TestDynamicClusterMembershipProcessAcceptance|TestDynamicClusterMembershipTransitionCrashRecovery|TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum|TestSnapshotFinalizationCrashRecoveryAcrossProcessRestart)$' -count=3 -v
```

이 실행의 archive format은 v2였고 `NMQBKUP1`은 unsupported-format으로 거절했다. Production volume 경계(`CLU-SCN-036`)는 이 process 시험에서 미실행이다. 이후 EKS 연결·실행은 §11.20과 마지막 절에 구분한다.

## 11.8 Retained replication과 backup v2 수용 매핑

스펙: [11.8](../specs/cluster-session-and-delivery.md#118-retained-replication과-backup-v2-수용-매핑). 대상: `CLU-SCN-037`~`CLU-SCN-040`.

Membership·records·retained repository·publish/subscribe/management·runtime·contract focused suite와 §11.7의 다섯 process 시험이 통과했다(process package 90.062s). Retained cross-node replay와 v2 restore·restart·catch-up 범위다.

```text
go test ./internal/application/membership/... ./internal/adapters/raft/records/... ./internal/adapters/memory/retained/... ./internal/application/publish/... ./internal/application/subscribe/... ./internal/application/management/... ./internal/adapters/http/management/... ./cmd/narumq/... ./tests/contracts/... -count=1
```

Offline queue·durable QoS ACK·release 호환성·보존 정책·production power loss는 이 retained 실행으로 판정하지 않는다.

## 11.9 Offline queue replication과 backup v3 수용 매핑

스펙: [11.9](../specs/cluster-session-and-delivery.md#119-offline-queue-replication과-backup-v3-수용-매핑). 대상: `CLU-SCN-041`~`CLU-SCN-051`.

QoS 0 offline FIFO·fencing/capacity, connect/publish 결합, backup v3와 옛 v1/v2 거절, owner crash·restore·restart·joiner 수렴의 focused·process 시험이 통과했다. Process 경로는 `TestClusterBackupRestoreProcessCrashAndResync`이며 durable QoS 1/2 backup은 §11.11의 별도 증거다.

### EKS 백업·보존·S3 복원 결과

| 대상·실행 | 결과·남은 범위 | 증거 |
| --- | --- | --- |
| `CLU-SCN-046`, 2026-10-14 live EKS | 부분 검증. NMQBKUP3 archive SHA-256, format 3.0/protocol 2.0/schema 1.0, commit index 100과 3-voter offline seed를 확인했다. Seed 합계 0.070s는 기동·readiness·quorum·서비스 복구를 제외하므로 전체 RTO가 아니다. | `.artifacts/verification/dr-backup-restore-rto-report.json` |
| `CLU-SCN-047`, 같은 날 retention probe | 수동 Job이 synthetic-age probe 하나만 삭제하고 기존 archive 두 개를 보존·새 archive 하나를 생성했다. 실제 scheduled cadence나 30일 경과를 관찰한 것이 아니다. | [보존 보고서](../../.artifacts/verification/backup-retention-verification-report.json) |
| `CLU-SCN-050`, 2026-10-15 격리 EKS·실제 S3/IRSA | **합성 fixture 범위 통과**. Commit index 52 archive 업로드·checksum, writer/restore 권한 거절 경계를 확인했다. 새 `restore-data-narumq-*` PVC 3개만 seed했다. 전체 RTO **124.79초**, Ready 3·quorum, 추가 발행 없이 `SessionPresent=true`·retained SHA-256 일치. | [전체 복원](../../.artifacts/verification/eks-resume-20261015T000337Z-21df3b/s3-full-restore-result.json), 같은 directory의 `s3-restore-seed.json`, `s3-live-configuration.json`, `writer-irsa-permissions.json`, `restore-irsa-permissions.json` |

10월 14일에는 격리 runtime이 없어 full restore를 시작하지 않았다([사전 점검](../../.artifacts/verification/dr-isolation-preflight-report.json)). 다음 날 합성 복원이 통과한 사실과 당시의 seed-only 범위를 구분한다. **지속적인 매일 백업의 RPO ≤ 24시간, 실제 30일 경과 보존, production 데이터 크기와 업로드 실패 matrix는 미검증**이며 `SPEC-TBD-09`를 닫지 않는다.

### Local S3 게시·maintenance 결과

- `CLU-SCN-048`: Python helper·별도 Kustomize render·Terraform fmt/init/validate·Docker smoke와 `go test ./tests/integration -run '^(TestEKSDeployment|TestEKSDeploymentS3OptInBoundary)$' -count=1`이 통과했다. Go manifest parser의 통과는 실제 S3 호출이나 Terraform apply 증거가 아니다.
- `CLU-SCN-049`: shell 강제 종료 때 `archive became visible before its request`를 재현했다. Directory 단위 게시 수정 후 opt-in `python -B tests/test_backup_publication.py`가 통과했다. 환경은 `NARUMQ_BACKUP_PUBLICATION_SMOKE=1`, image `narumq-compat-python-drain-paho-go-python-will-acl-final:phase1`, Docker `--network none`·AWS CLI 대역이다. 실제 S3 호출은 없다. 수정 전후 증거는 `.artifacts/verification/eks-final-20261014T221024Z/backup-publication-before.json`, `backup-publication-after-complete.json`이다.
- `CLU-SCN-051`: `python -B tests/test_backup_s3.py -v`는 Windows 23개 통과·symlink 1개 skip, Linux Docker 24개 모두 통과였다. Offline staging 복구·명시적 discard·기존 publication/legacy/symlink 보존과 opt-in publication smoke도 통과했다. 기준은 `ef6e561` 위 maintenance 작업 트리다. [Smoke 명령·출력](../../.artifacts/verification/todo-refresh-20261015T073649Z/backup-stage-smoke.json), [검증 기록](../../.artifacts/verification/todo-refresh-20261015T073649Z/final-summary.json)을 따른다. 전체 Go local gate·원격 CI·실제 AWS 호출은 이 작업에서 미실행이다.

## 11.10 Live cross-node QoS 0 forwarding 수용 매핑

스펙: [11.10](../specs/cluster-session-and-delivery.md#1110-live-cross-node-qos-0-forwarding-수용-매핑). 대상: `CLU-SCN-052`~`CLU-SCN-058`.

TCP·publish·runtime focused suite와 `TestDynamicClusterLiveQoS0CrossNodeForwardingProcessAcceptance`가 통과했다. 실제 subscriber 전달·순서·owner 이전을 확인했다. Local projection admission 회귀는 초기화·실패·index gap을 확인하며 PUBLISH의 quorum ReadIndex 호출을 요구하지 않는다.

```text
go test ./internal/adapters/tcp/... ./internal/application/publish/... ./cmd/narumq/... ./tests/contracts/... -count=1
go test ./tests/integration -run '^TestDynamicClusterLiveQoS0CrossNodeForwardingProcessAcceptance$' -count=1
```

Queue admission만으로 QoS 1/2 durability를 주장하지 않는다. Durable ACK는 §§11.11·11.13, cross-node matrix·동시 CONNECT·stale epoch는 §11.15의 별도 증거다.

## 11.11 QoS 1/2 durable delivery 수용 매핑

스펙: [11.11](../specs/cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑). 대상: `CLU-SCN-059`~`CLU-SCN-077`.

Focused 7개 package와 takeover·ACK-then-kill·backup/restore·QoS 2 Will process 시험 4개가 통과했다(process package 43.353s).

```text
GOMAXPROCS=4 go test ./internal/application/membership ./internal/adapters/raft/records ./internal/adapters/memory/deliveryqueue ./internal/application/publish ./internal/application/connect ./internal/adapters/tcp ./cmd/narumq -count=1
GOMAXPROCS=4 go test ./tests/integration -run '^(TestDynamicClusterDurableQoSTakeoverProcessAcceptance|TestDynamicClusterDurableQoSAckThenKillProcessAcceptance|TestDynamicClusterDurableQoSBackupRestoreProcessAcceptance|TestDynamicClusterDurableQoS2WillProcessAcceptance)$' -count=1 -v
```

이 실행은 detached persistent queue·takeover inflight 재개·backup v3/tombstone·성공 ACK 뒤 process kill·quorum loss 중 no-ACK·Will reconnect 범위다. Production storage·BP-03·release 호환성·tenant quota·drain 수용을 대신하지 않는다.

`CLU-SCN-073`의 late commit 시험은 실제 dynamic Raft/WAL과 `BeginDelivery`·`Wait`를 쓰지만 ingress에서 `stageProposalResult`·`resolveAppliedProposals`를 직접 연결한다. 각 node의 `Batch.Commands`에 원 command가 한 번 나오는지, 원 Wait의 `ErrQuorumUnavailable`·늦은 결과 buffer를 확인했다. `runtime.applyBatch`나 delivery state machine 변경 전체를 실행한 시험은 아니다. TCP 오류 시험이 성공 ACK 부재를 별도로 확인하며 MQTT process 수준 late-commit 경합은 재현하지 않았다.

## 11.12 MQTT CONNECT ownership acquisition과 takeover 수용 매핑

스펙: [11.12](../specs/cluster-session-and-delivery.md#1112-mqtt-connect-ownership-acquisition과-takeover-수용-매핑). 대상: `CLU-SCN-078`~`CLU-SCN-080`.

Ownership takeover process 시험이 통과했다(8.15s). Commit 이전에 연결을 끊는 negative control에서는 기존 owner socket이 열린 채 30초 close assertion이 실패했다. Post-commit failpoint로 돌린 실행은 통과하여 commit-before-CONNACK 경계를 구분했다.

```text
go build ./...
go build -tags narumq_failpoint ./...
go test ./cmd/narumq -run '^TestClusterFailpoint' -count=1
go test ./tests/integration -run '^TestDynamicClusterMQTTConnectOwnershipTakeoverProcessAcceptance$' -count=1 -v
```

## 11.13 ADR-0003 durable ACK fault 수용 매핑

스펙: [11.13](../specs/cluster-session-and-delivery.md#1113-adr-0003-durable-ack-fault-수용-매핑). 대상: `CLU-SCN-081`~`CLU-SCN-084`.

Fresh ReadIndex 발행 제어 뒤 matched-target opt-in suite는 통과했다(package 158.343s). Replica 3개 × QoS 1/2 ACK 뒤 kill, WAL/commit crash 20개 조합과 follower/leader partition을 확인했다.

```text
GOMAXPROCS=4 NARUMQ_DURABLE_ACK_FAULTS=1 go test ./tests/integration -run '^TestDynamicClusterDurableAck(ReplicaKill|CrashPoint|Partition)ProcessAcceptance$' -count=1 -v -timeout 30m
go test ./tests/chaos/cluster-storage-v1 -count=1
```

고정 ID 순서의 kill이 leadership 변경 뒤 role coverage를 놓치던 fixture는 현재 미검증 leader·남은 follower를 고르도록 고쳤다. 이 suite는 matched-target ACK/WAL 불변식이며 targetless barrier·BP-03 성능·실제 EBS power-loss 증거가 아니다. Docker/FUSE 결과는 §11.6, EKS 실패 모델은 §11.20을 따른다.

## 11.14 Persistent subscription·inflight replay와 route projection rebuild 수용 매핑

스펙: [11.14](../specs/cluster-session-and-delivery.md#1114-persistent-subscriptioninflight-replay와-route-projection-rebuild-수용-매핑). 대상: `CLU-SCN-085`~`CLU-SCN-089`.

Snapshot+WAL tail unit와 persistent subscription replay process 시험 3회가 통과했다. MQTT 3.1.1에서 전체 crash·joiner 첫 READY projection·inflight tail·unsubscribe/stale filter 부재를 확인했다.

```text
go test ./cmd/narumq -run '^TestClusterSnapshotAndWALTailReplayRestoresSessionAggregateAndRouteProjection$' -count=1 -v
go test ./tests/integration -run '^(TestClusterSubscriptionReplaySnapshotIndexRejectsNonDecimalDigits|TestDynamicClusterPersistentSubscriptionReplayProcessAcceptance)$' -count=3 -v -timeout 20m
```

앞선 reduced-resource 실행에는 JWKS request-count 불일치와 ThreadSanitizer 할당 실패가 있었다. 옛 fixture의 QoS 2 packet 42 EOF는 subscriber drain 전에 즉시 발행하는 sequence로 보강한 뒤 통과했다. Packet 11·21 EOF에는 readiness/index 근거가 없어 원인은 미확정이다. 이 통과로 앞선 오류의 원인을 모두 규명했다고 주장하지 않는다. MQTT 5 옵션·동시 CONNECT·stale epoch의 별도 process 결과는 §11.15에 있다.

## 11.15 Cross-node delivery matrix·동시 CONNECT·stale-epoch 수용 매핑

스펙: [11.15](../specs/cluster-session-and-delivery.md#1115-cross-node-delivery-matrix동시-connectstale-epoch-수용-매핑). 대상: `CLU-SCN-090`~`CLU-SCN-097`.

부분 CONNACK EOF 보강 뒤 combined matrix·concurrent CONNECT·stale-epoch process 시험이 통과했다(package 20.545s). Concurrent CONNECT 20회·자연 경합 200 round도 통과했지만 `closed before CONNACK after takeover` 분기는 관찰되지 않았다. 해당 허용·거절 경계는 결정적 socket 시험으로 확인했다.

```text
GOMAXPROCS=4 go test ./tests/integration -run '^(TestDynamicClusterCrossNodeDeliveryMatrixProcessAcceptance|TestDynamicClusterConcurrentConnectProcessAcceptance|TestDynamicClusterStaleEpochFencingProcessAcceptance)$' -count=1 -v -timeout 20m
go test ./tests/integration -run '^TestDynamicClusterConcurrentConnectProcessAcceptance$' -count=20 -v -timeout 30m
```

남긴 실패·해석 경계:

- 2026-10-02 자연 경합의 두 live connection은 apply-before-bind race였다. Epoch 2가 먼저 적용된 뒤 epoch 1 bind를 거절하는 결정적 회귀를 추가했다. 같은 시기 retained RH1의 Server Unavailable/read-barrier timeout 원인은 미확정이다.
- 2026-10-07 round 6의 CONNECT EOF는 반복 실행에서 재현하지 못했다. 실패 log가 없어 bind 전 fence와 CONNACK 전 takeover close 중 어느 경로인지는 확인하지 못했다 [INFERENCE]. `quota_profile_missing`은 startup log이며 원인으로 단정하지 않는다.
- Raw reader가 부분 CONNACK 뒤 EOF를 응답 전 종료와 혼동한 결함을 실제 socket으로 재현·수정했다. 첫 byte 전 종료만 `io.EOF`, remaining-length/body 중 EOF는 `io.ErrUnexpectedEOF`다. 부분 CONNACK은 다른 live owner가 있어도 실패다.
- TCP cleanup 시험은 `deliveryMu`로 주입한 lifecycle 전이의 epoch fence 시험이지 자연 production race 재현이 아니다. Deadline 설정 실패 회귀는 PING read/write를 하지 않고 즉시 실패하는 경계를 확인했다.

## 11.16 Tenant storage quota 수용 매핑

스펙: [11.16](../specs/cluster-session-and-delivery.md#1116-tenant-storage-quota-수용-매핑). 대상: `CLU-SCN-098`~`CLU-SCN-102`.

Profile/accounting·snapshot/codec·config/readiness·bounded metric focused suite와 실제 3-node MQTT quota·rolling profile process 시험이 통과했다(11.10s).

```text
go test ./internal/application/membership -count=1
go test ./internal/adapters/raft/records ./cmd/narumq -run 'TenantQuota|Records' -count=1
go test ./cmd/narumq ./internal/adapters/metrics -count=1
go test ./tests/integration -run '^TestDynamicClusterTenantStorageQuotaProcessAcceptance$' -count=1 -v -timeout 20m
```

이 결과는 TEN-08 quota 범위이며 backup retention·EBS power-loss와 별도다.

## 11.17 N-1↔N rolling upgrade·rollback 수용 매핑

스펙: [11.17](../specs/cluster-session-and-delivery.md#1117-n-1n-rolling-upgraderollback-수용-매핑). 대상: `CLU-SCN-103`~`CLU-SCN-113`.

Same-source synthetic release process 시험이 통과했다(105.55s). N/N-1 양방향 installed snapshot과 서로 다른 N node 두 개의 snapshot transfer, rollback·재upgrade·N-2 거절·feature activation 경계를 확인했다.

```text
NARUMQ_RELEASE_COMPAT=1 GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterReleaseCompatibilityProcessAcceptance$' -count=1 -v -timeout 20m
go test ./cmd/narumq -run '^TestClusterReleaseProfile' -count=1 -v
```

Snapshot wake의 periodic reconcile 대기와 응답 없는 profile proposal deadline은 각각 수정 전 실패·수정 후 controller race 통과였다. 명령·범위는 [성능 §8.6](performance-and-verification.md#86-간헐-실패-원인-수정-수용-증거)에 있다. 이 synthetic 결과를 실제 게시 release 전체 호환성으로 확장하지 않는다. 지정 historical source는 §11.21의 별도 증거다.

## 11.18 비동기 WAL·application 완료 경계 수용 매핑

스펙: [11.18](../specs/cluster-session-and-delivery.md#1118-비동기-walapplication-완료-경계-수용-매핑). 대상: `CLU-SCN-114`~`CLU-SCN-118`.

Raft package·race와 실제 broker process 회귀, opt-in ACK fault suite가 통과했다. Docker/FUSE `storage-releasewake-20261003`은 6/6 통과였으며 증거는 `.artifacts/chaos/cluster-storage-v1/storage-releasewake-20261003/`에 있다.

중요한 수정 전 실패와 검증 범위:

- Held WAL fsync가 tick을 막던 실패를 재현했다. 초기 async 구현은 leader before-write QoS 1 조기 ACK가 실패했고, 저장 의존 `MsgApp` 보류 뒤 isolated `acked=false,count=0`과 crash 20개 조합이 통과했다.
- 첫 `storage-asyncwal-20261003`은 5/6 통과였다. Quorum-disk-full은 장애 주입 전 `applied=21,projection=20`으로 CONNECT를 거절했다. Driver의 application 완료 게시 경계와 read 결과 반환을 맞춘 뒤 6/6 통과했다. Fresh proof·ACK·fsync 정책은 완화하지 않았다.
- `TestNodeCloseFlushesHeldAppendOrReportsFailure`는 held append의 FIFO flush, 마지막 sync 실패, consumed/unconsumed prior error와 미처리 queue를 구분한다. `ErrStorage`·`os.ErrPermission` 종료 보고와 held sync 중 heartbeat/entry-bearing `MsgApp` 경계도 확인했다. 세부 명령은 [성능 §8.2](performance-and-verification.md#82-branch-측정-gate-수용-증거)·[§8.3](performance-and-verification.md#83-branch-ratchet-노이즈-허용폭-수용-증거)에 있다.

Synthetic release process도 통과했다. BP-03 반복 수용의 기준 기록은 [성능 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)이며 이 WAL 회귀가 성능 판정을 대신하지 않는다. FUSE는 실제 EBS 전원 손실이 아니다.

## 11.19 Backup archive N/N-1 restore 수용 매핑

스펙: [11.19](../specs/cluster-session-and-delivery.md#1119-backup-archive-nn-1-restore-수용-매핑). 대상: `CLU-SCN-119`~`CLU-SCN-124`.

2026-10-03 Windows same-source synthetic process는 N-1→N·N→N-1 restore, unsupported feature·N-2 거절과 대상 directory 미생성을 확인했다(20.71s). Focused suite는 adjacent release와 exact protocol/schema 경계를 통과했다.

```text
GOMAXPROCS=4 go test ./cmd/narumq -run '^(TestClusterRestoreSeed|TestClusterRestoreMemberReleaseWindow)' -count=1 -v
NARUMQ_RELEASE_COMPAT=1 GOMAXPROCS=4 go test ./tests/integration -run '^TestClusterBackupRestoreReleaseWindowProcessAcceptance$' -count=1 -v -timeout 20m
```

지정 historical baseline은 §11.21과 구분한다. Skip은 수용 증거가 아니며 실제 게시 release별 matrix와 `SPEC-TBD-09` 보존·RPO/RTO는 이 결과로 닫지 않는다.

## 11.20 Voter volume 손실 보호와 재시드 수용 매핑

스펙: [11.20](../specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑). 대상: `CLU-SCN-125`~`CLU-SCN-145`.

### Local 보호·재시드 결과

| 대상 | 의미 있는 결과 |
| --- | --- |
| `CLU-SCN-125`~`CLU-SCN-130` | Bootstrap probe TLS/listener·all-peer barrier·취소와 late co-bootstrap, 손실 voter 거절·새 ID 복구가 통과했다. 수정 전 lost-voter 시험은 30초 내 거절하지 못해 실패했다. |
| `CLU-SCN-131`~`CLU-SCN-134` | 수정 전 fresh VOTER seed 승인이 4-voter heartbeat panic(`tocommit(28) ... lastIndex(0)`)으로 이어졌다. 수정 후 raw seed 거절·startup/readiness 503·MQTT 거절·snapshot 부재와 새 ID의 retained/offline·restart·retired ID 거절을 확인했다. |
| `CLU-SCN-138`·`CLU-SCN-021`, ADR-0061 | 같은 주소 재시드 수정 전 process 64회 중 4회 실패. 이전 ID dialer가 새 ID의 인증서를 hello 뒤에 확인해 정상 peer 연결을 교체·종료했다. TLS 직후 hello 전에 identity를 확인한 뒤 전송 회귀 50회와 process 40회가 통과했다. 단순 반복 통과 외에 hello 유출·정상 연결 교체의 수정 전 실패를 직접 재현했다. |

2026-10-03 Windows 실행과 ADR-0061 후속 명령:

```text
GOMAXPROCS=4 go test ./internal/adapters/tcp -run 'BootstrapProbe' -count=1 -v
GOMAXPROCS=4 go test ./cmd/narumq -run '^(TestClusterDirectoryEmpty|TestClusterBootstrapProbe)' -count=1 -v
GOMAXPROCS=4 go test ./tests/integration -run '^(TestDynamicClusterVoterVolumeLossProcessAcceptance|TestDynamicClusterBootstrapProbeAllowsLateCoBootstrapPeer)$' -count=1 -v -timeout 20m
go test ./internal/adapters/tcp -run '^(TestDynamicSeedAdmissionRejectsFreshVoterIdentity|TestDynamicSeedAdmissionRequiresTLSBoundCommittedStage)$' -count=1 -v
go test ./tests/integration -run '^TestDynamicClusterJoinerVoterVolumeLossProcessAcceptance$' -count=1 -v -timeout 20m
GOMAXPROCS=2 go test -p=2 ./tests/integration -run '^TestDynamicClusterSameAddressReseedProcessAcceptance$' -count=40 -v -timeout 55m
```

ADR-0024 before/after 원시 출력은 `.artifacts/planning/joiner-voter-admission-before.txt`, `joiner-volume-loss-process-before.txt`, `joiner-tls-after.txt`, `joiner-process-after.txt`에 있다. 이 local process 삭제는 실제 EBS 손실이 아니다.

`CLU-SCN-135`의 2026-10-14 preflight는 안전한 fixture가 없어 destructive 시험을 시작하지 않았다([증거](../../.artifacts/verification/ebs-loss-isolation-preflight-report.json)). `CLU-SCN-136`의 EKS `/empty-data`는 disposable `emptyDir`였다. PROGRESSED 응답·exit 1·파일 미생성·기존 3/3 Ready만 확인했다([관찰](../../.artifacts/verification/ebs-volume-loss-guard-report.json)). 이후 실제 EBS 결과와 혼동하지 않는다.

### EKS 실패 모델과 실행 도구

P1은 OS 종료를 생략한 전체 voter EC2 동시 중단 모델이다. 자연 torn-write 관찰과 T1/T2의 주입 손상 모델은 다르다. R1은 실제 EBS 삭제 후 새 ID·큰 snapshot stream 재시드다. 기준은 [EKS 전체 voter 전원 손실·torn-tail·큰 snapshot 재시드 기준](../specs/cluster-session-and-delivery.md#eks-전체-voter-전원-손실torn-tail큰-snapshot-재시드-기준-2026-10-16)에 있다.

| 실행 경로 | 대상·증거 |
| --- | --- |
| `tests/chaos/eks-worker-recovery/main.py power-loss` | P1, `power-loss-preflight.json`, `power-loss-stop-request.json`, `power-loss-recovery.json`, `power-loss-durable.json`, `power-loss-oracle.json`, `power-loss-nodes.json` |
| `tests/chaos/eks-wal-torn-tail/main.py run` | T1/T2, `torn-<case>.json`, `summary.json`, private WAL 백업·`state.json` 복원 journal |
| `tests/chaos/eks-volume-reseed/main.py run` | R1, plan의 `largeSnapshot`, `reseed-feature.json`, `reseed-large-load.json`, `reseed-verify.json`, `summary.json` |

실행은 `--execute --plan`을 요구한다. 도구는 disposable fixture·synthetic data·run 소유 tag, VPC·ASG·instance 소유권을 검사한다. T1/T2 벡터는 실제 Go WAL reader와 대조한다. Private WAL 복원 전 cordon을 풀지 않으며 P1 복구 실패 때는 ASG process를 멈춘 채 실패를 기록한다. 이 통제의 Python fake 시험 통과를 EKS 실행으로 계산하지 않는다.

#### 2026-10-16 CLU-14 완결 matrix 실행 결과

대상: `CLU-SCN-140`~`CLU-SCN-145`.

Run `eks-clu14-20261016T103521Z-a1925a`, cluster `narumq-clu14-20261016-a1925a`, VPC `vpc-0f10394cdf57d734a`. EKS 1.37, `m7g.large` 3대, 암호화 gp3 20 GiB EBS, 전용 snapshot 3개 `completed`를 확인했다. 소스는 `28df719` 위 ADR-0062 포함 작업 트리, ARM64 image index는 `sha256:177617627a4784fb0833141059f6e420df5d65f1ffe1484a51173522391c4f64`다.

| 시험 | 결과·범위 | 증거 |
| --- | --- | --- |
| P1 | **1회 통과**. 11:16:56 UTC 한 요청으로 voter EC2 3개를 OS 종료 없이 중단하고 모두 멈춘 뒤 120초 유지했다. 시작 요청 뒤 70.842초·중단 요청 뒤 212.341초에 Ready 3·quorum 회복. 발행 45개 중 ACK 9개 모두 복구, 누락·중복 0. 결과 미상 36개는 전달되지 않았다. `SessionPresent=true`, offline·retained SHA 일치. Container 각 1회 재시작, startup 우회 readiness 호출 없음. ASG 해제 뒤 180초 교체 없음. 시작 로그 3개에 **자연 torn-tail 절단은 없었다**. | [판정](../../.artifacts/verification/eks-clu14-20261016T103521Z-a1925a/p1/eks-clu14-20261016T103521Z-a1925a/summary.json), [Durable](../../.artifacts/verification/eks-clu14-20261016T103521Z-a1925a/p1/eks-clu14-20261016T103521Z-a1925a/power-loss-durable.json), [Tail 관찰](../../.artifacts/verification/eks-clu14-20261016T103521Z-a1925a/p1/eks-clu14-20261016T103521Z-a1925a/power-loss-nodes.json) |
| T1/T2 | **4개 주입 case 전체 1회 통과**. 실제 EBS ext4·`data-narumq-0`의 clean WAL을 백업하고 주입했다. T1 `short-header`·`zero-tail`은 정상 절단 후 Ready·quorum·marker 복구. T2 `final-crc-no-zero-evidence`·`middle-crc`는 시작 거절·NotReady와 거절 중 WAL 불변을 확인했다. Private 백업 복원 후 정상 복구. | [판정](../../.artifacts/verification/eks-clu14-20261016T103521Z-a1925a/torn/eks-clu14-20261016T103521Z-a1925a/summary.json), [파일시스템](../../.artifacts/verification/eks-clu14-20261016T103521Z-a1925a/torn/eks-clu14-20261016T103521Z-a1925a/torn-filesystem.json) |
| R1 | **1회 통과**. 장애 전 `snapshot-stream` 활성화 후 8 topic × 256 KiB와 padding 1,100개 발행. Slot 2/member 3의 worker 종료·EBS 삭제 뒤 빈 PVC bootstrap은 exit 1. 새 ID 4 stage 뒤 2,105,083 byte·5 chunk stream 송수신 완료. Ready 3, voters `{1,2,4}`, 이전 3 `RETIRED`·stage 409. Durable 51/51 ACKed, 대형 retained 8개 SHA 일치. | [판정](../../.artifacts/verification/eks-clu14-20261016T103521Z-a1925a/r1/eks-clu14-20261016T103521Z-a1925a/summary.json), [상세](../../.artifacts/verification/eks-clu14-20261016T103521Z-a1925a/r1/eks-clu14-20261016T103521Z-a1925a/reseed-verify.json) |

이 matrix는 위 실패 모델의 수용 근거다. 2026-10-15의 다른 fixture 실패, 자연 torn-write 미관측과 보존/RPO/RTO 제약을 지우지 않는다. 이 run의 cluster·VPC·worker·EBS·snapshot 3개·log group·ECR tag는 제거했고 private 평문은 삭제·DPAPI 암호화했다([정리](../../.artifacts/verification/eks-clu14-20261016T103521Z-a1925a/cleanup.json)). 다른 run에서 의도적으로 보존한 snapshot·공유 image와 구분한다.

#### 2026-10-16 EKS slot 재시드 실행 결과

대상: `CLU-SCN-139`.

Run `eks-matrix-20261016T065350Z-24adc2`, fixture `narumq-ha-acceptance-20261015`(m7g·암호화 gp3), slot 0/member 1 follower의 실제 worker 종료·EBS 삭제 후 재시드다. **수정·시험 통제·재개를 거친 제한 범위 통과**이며 중단 없는 단일 실행이나 RTO 증거가 아니다.

- 빈 PVC Pod의 `cluster bootstrap refused`·exit 1과 생존 Ready 2·quorum을 확인했다. 손실 직전 applied index 최댓값은 2598이었다.
- Member 4 stage 뒤 1,055,832-byte snapshot이 한도를 넘었다. Leader들이 `outbound message exceeds 1048576 bytes`로 실패하여 quorum을 잃었다. Image 교체만으로 oversized snapshot을 경고·거절하도록 고친 뒤 생존 voter가 복구됐다. 이는 큰 snapshot의 전송 성공이 아니다.
- 시험 통제로 retained 8 topic을 지우고 entry 1,100개를 더 써 snapshot을 갱신했다. 새 크기는 측정하지 않았다. 한도 아래로 줄었다는 해석은 catch-up·경고 부재에 근거한 추정이다 [INFERENCE]. 이 실행은 1 MiB 초과 상태 재시드의 증거가 아니며 그 수용은 위 R1이다.
- Stage는 한 번만 보냈다. Port-forward 사전 확인과 MQTT 의존성 확인 때문에 재개했으며 최종 데이터 판정은 `resume-4`에서만 완료했다. Member 4 index 3780 ≥ 2598, stage `SUCCEEDED`, 이전 1 `RETIRED`·409, Ready 3·quorum, 48/48 PUBACK·누락/중복/거절 전달 0·`SessionPresent=1`·retained SHA 일치를 확인했다.

증거: [원래 실행](../../.artifacts/verification/eks-matrix-20261016T065350Z-24adc2/reseed/eks-matrix-20261016T065350Z-24adc2/summary.json), [재시드 완료](../../.artifacts/verification/eks-matrix-20261016T065350Z-24adc2/reseed/eks-matrix-20261016T065350Z-24adc2/resume-3/summary.json), [최종 데이터 판정](../../.artifacts/verification/eks-matrix-20261016T065350Z-24adc2/reseed/eks-matrix-20261016T065350Z-24adc2/resume-4/summary.json), [복구 상태](../../.artifacts/verification/eks-matrix-20261016T065350Z-24adc2/reseed/recovery/recovered-state.json), [시험 부하 정리](../../.artifacts/verification/eks-matrix-20261016T065350Z-24adc2/reseed/recovery/shrink-state.json).

#### 2026-10-15 격리 EKS 전원 손실 후속 관찰 — FAILED/UNRESOLVED

대상: `CLU-SCN-137`.

장애 전 QoS 1 retained marker PUBACK과 snapshot 3개 `completed`를 확인했다. `--skip-os-shutdown` stop/start 뒤 원 instance `i-01ee5c360cbebe3bc`는 종료되고 같은 EBS `vol-0ffad539c1775d890`이 ASG 대체 instance에 붙었다. 300초 관찰에서 member 3은 index 62→64로 전진했지만 `VOTER/FOLLOWER DRAINING/NotReady`, quorum 2/3이었다. Marker SHA는 voter 1·2에서만 읽었다. **당시 복구 판정은 FAILED/UNRESOLVED**다.

2026-10-16 분석에서 startup probe가 `JOINING`의 readiness를 평가하지 않아 kubelet이 readiness probe를 시작하지 못하는 결함을 확인했다. Startup timeout→preStop drain 완료→재시작·CLEAR→startup 실패의 순환으로 DRAINING/JOINING을 설명한다. 첫 기동 진단의 `/health/ready` 호출은 이를 가렸지만 장애 뒤 대체 Pod에는 그 호출이 없었다. 대체 image digest와 첫 container/preStop 출력은 보존하지 않아 해당 부분의 동일성·순서는 추정을 포함한다 [INFERENCE].

수정 전 `9ddd24d`의 실제 3-broker startup-only 재현은 실패했고 `8096f30`의 `CheckStartup` 보완 뒤 같은 시험은 통과했다(`TestDynamicClusterRestartStartupOnlyAfterProbeKillAndCrashProcessAcceptance`). [Drain §8.1](../specs/drain-and-rolling-update.md#81-startup-probe-종료와-drain-기록-재시작-순환), [원인 분석](../../.artifacts/analysis/clu14-powerloss-0210-rca.md)을 따른다. 이후 matrix의 통과를 이 과거 실행의 PASS로 소급하지 않는다. 이 실행은 전체 voter P1·torn-tail·실제 volume 삭제와도 다르다.

증거: [안전 사전 점검](../../.artifacts/verification/isolated-fixture-safety-preflight-storage-dr-reseed-20261015-0057-kst.json), [stop/start](../../.artifacts/verification/clu14-powerloss-member3-storage-dr-reseed-20261015-0057-kst.json), [복구](../../.artifacts/verification/clu14-powerloss-recovery-storage-dr-reseed-20261015-0057-kst.json), [300초 후속 관찰](../../.artifacts/verification/clu14-powerloss-readonly-followup-20261015-0235-kst.json), [장애 전 ACK](../../.artifacts/verification/clu14-durable-qos1-marker-storage-dr-reseed-20261015-0057-kst.json), [marker 조회](../../.artifacts/verification/clu14-qos1-retained-readonly-followup-20261015-0235-kst.json).

별도 재개 fixture에서는 취소한 FAILED drain의 failure fields가 남아 `snapshot node drain failure fields require FAILED phase`로 시작을 거절했다. 이 원인을 위 fixture에 소급 적용하지 않는다. `NodeDrainActionCancel` 보완 후 같은 Pod UID·EBS·instance에서 WAL 변경 없이 복구했고, 후속 same-instance/same-volume 비정상 stop/start는 235.05초에 Ready 3·quorum·`SessionPresent=true`·retained SHA 일치를 확인했다. 증거는 `.artifacts/verification/eks-resume-20261015T000337Z-21df3b/`의 `powerloss-recovery-failed.json`, `powerloss-failure-analysis.json`, `drainfix-same-volume-recovery.json`, `powerloss-afterfix-result.json`이다.

옛 fixture는 폐기했고 기준 snapshot 3개·공유 image는 의도적으로 보존했다([정리](../../.artifacts/verification/isolated-fixture-cleanup-serial138-20261015.json)). 원본 cluster의 옛 port-forward/context는 확인하지 못해 그 영향은 판단하지 않는다([감사 제한](../../.artifacts/verification/source-audit-follow-up-20261015.json)).

## 11.21 Historical-source N-1↔N 호환성 수용 매핑

스펙: [11.21](../specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑). 대상: `CLU-SCN-146`~`CLU-SCN-149`.

2026-10-03 N source `fbe5edebfe944921bdd95e56b6d74e7dc4d1a3ae`와 historical N-1 `58a95f801e1467379db2eb6f3454ae44e954418a`, 당시 harness 작업 트리로 두 시험이 통과했다(package 108.681s). Historical rolling·snapshot·rollback·N-2 거절과 N-1 archive→HEAD 복원을 확인했다.

```text
NARUMQ_RELEASE_COMPAT=1 GOMAXPROCS=4 go test ./tests/integration -run '^(TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance|TestClusterBackupRestoreHistoricalReleaseProcessAcceptance)$' -count=1 -v -timeout 30m
```

Full git history와 PATH의 git이 필요하며 revision 부재는 실패다. Pre-ADR-0021 historical consumer는 exact producer release 정책이므로 HEAD archive의 N→historical N-1 복원은 target 생성 전 거절이 수용 결과다. 이를 downgrade 복원 성공으로 쓰지 않는다. 실제 게시 release마다 별도 matrix가 필요하다.

## 11.22 WAL compaction·snapshot retention·torn-tail 수용 매핑

스펙: [11.22](../specs/cluster-session-and-delivery.md#1122-wal-compactionsnapshot-retentiontorn-tail-수용-매핑). 대상: `CLU-SCN-150`~`CLU-SCN-159`.

2026-10-03 Windows에서 compaction·frame split·crash state·오류 복구·torn-tail/corruption unit의 수정 전 실패·수정 후 통과를 확인했다. 실제 3-node process는 snapshot index 2003, node별 WAL 1745 bytes와 handle 교체·MQTT 복구를 확인했다. Race, synthetic/historical release, ACK fault와 Docker/FUSE 회귀도 통과했다.

```text
go test ./internal/adapters/raft -run '^(TestStableStoreSnapshotCompactsWALToSnapshotTail|TestStableStoreCompactionSplitsLargeTailIntoFrames|TestStableStoreRecoversEveryCompactionCrashState|TestStableStoreCompactionFailureKeepsRecoverableState|TestWALReaderHandlesTornAndCorruptFrames)$' -count=1 -v
GOMAXPROCS=4 go test -race ./internal/adapters/raft -count=1
NARUMQ_WAL_LIFECYCLE=1 GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterWALCompactionProcessAcceptance$' -count=1 -v -timeout 20m
GOMAXPROCS=2 go run ./tests/chaos/cluster-storage-v1 --run-id storage-walcompaction-20261003 --output .artifacts/chaos/cluster-storage-v1/storage-walcompaction-20261003
```

FUSE 6/6 결과는 해당 `summary.json`에 있다. Windows identity assertion은 `File.Stat`으로 교체 전 handle identity를 확보해 같은 `os.SameFile` 기준을 적용했다. 이 simulation·process restart는 실제 EBS 내구성이나 backup 보존 정책의 증거가 아니다. `CLU-SCN-159`의 retention/RPO/RTO는 미완료다. `CLU-SCN-156`의 후속 worker/snapshot 결합 회귀는 마지막 절에 있다.

## 11.23 Local ext4 filesystem 수용 매핑

스펙: [11.23](../specs/cluster-session-and-delivery.md#1123-local-ext4-filesystem-수용-매핑). 대상: `CLU-SCN-160`~`CLU-SCN-163`.

2026-10-03 최종 `ext4-20261003c`는 4/4 통과였다. Kernel `6.6.87.2-microsoft-standard-WSL2`, ext4 `rw,relatime,data=ordered`, broker SHA-256 `d7c2be5e51271a130ce31250d7cc805c0afce65ae0cea5ef7786aaa0167e4170`이다.

```text
go run ./tests/chaos/cluster-storage-v1 --profile local-ext4-v1 --run-id ext4-20261003c --output .artifacts/chaos/cluster-storage-v1/ext4-20261003c
```

Follower/quorum ENOSPC, 전체 process crash와 recovery boundaries를 확인했다. ACKed payload missing/duplicate 0·persistent 재개를 확인했고 recovery suite에 nested skip/fail은 없었다. 증거는 해당 directory의 manifest/summary·event/node log·`filesystem-recovery-boundaries/recovery.jsonl`이다.

첫 실행은 shell의 SIGUSR1/2 전달 누락으로 restart/crash event가 없어 실패했다. 신호 전달과 shell wait를 보완한 뒤 통과했다. 최종 실행은 검사한 immutable image ID로 container를 시작했다. 취소 시 ext4 unmount/detach·자원 제거 smoke도 별도로 통과했지만 image ID 보완 전 실행이다(`.artifacts/chaos/cluster-storage-v1/ext4-cancel-20261003/`). 공유 host의 process crash·입력 복구 결과이며 OS cache 손실·EBS 전원 손실을 입증하지 않는다.

## 11.24 Session metadata 복제와 owner 손실 복구 수용 매핑

스펙: [11.24](../specs/cluster-session-and-delivery.md#1124-session-metadata-복제와-owner-손실-복구-수용-매핑). 대상: `CLU-SCN-164`~`CLU-SCN-173`.

Windows/amd64·Go 1.26.5에서 focused package, owner crash/restart·expiry/capacity process와 inactive-feature 회귀가 통과했다. ADR-0028 영향 회귀는 process 26.79s였다. Delayed Will은 TCP close 직후 T0와 READY 뒤 양수 무수신 관찰, 첫 Will 뒤 중복 부재를 확인했다.

```text
go test ./internal/domain/session ./internal/application/membership ./internal/adapters/raft/records ./internal/application/connect ./internal/application/management ./cmd/narumq -count=1
GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterSessionMetadataProcessAcceptance$' -count=1 -v -timeout 20m
```

Feature 비활성 wire와 synthetic/historical release도 별도 opt-in에서 통과했다. QoS 0 local queue는 `BeginDetach`와 DETACH commit 사이 process 손실 시 metadata만으로 복원하지 못한다. 이 경계는 [ADR-0013 §6](../decisions/ADR-0013-offline-queue-replication-and-cluster-backup-v3.md#6-결과)을 따르며 local publish race·owner crash는 §§11.25·11.26과 구분한다. 자동 failure detection·production storage와 미래 게시 release 전체는 이 local 결과의 범위 밖이다.

## 11.25 Detach 전환 local QoS 0 보존 수용 매핑

스펙: [11.25](../specs/cluster-session-and-delivery.md#1125-detach-전환-local-qos-0-보존-수용-매핑). 대상: `CLU-SCN-174`~`CLU-SCN-182`.

2026-10-04 Windows/amd64·Go 1.26.5. 실제 queue의 concurrent local publish 회귀는 수정 전 실패·수정 후 통과했다. Expiry 보강 후 focused 6개 package, 아래 race 3회와 backup/persistent replay/metadata process 3개가 통과했다.

```text
go test ./cmd/narumq -run '^TestClusterDetachCapturesConcurrentLocalQoS0Publish$' -count=1 -v
go test ./internal/adapters/memory/deliveryqueue ./internal/application/connect ./internal/application/publish ./internal/application/subscribe ./internal/adapters/tcp ./cmd/narumq -count=1
go test -race ./internal/adapters/memory/deliveryqueue ./internal/application/publish ./cmd/narumq -run 'Detach|LocalDetach|AcceptForwarded' -count=3
GOMAXPROCS=4 go test ./tests/integration -run '^(TestClusterBackupRestoreProcessCrashAndResync|TestDynamicClusterPersistentSubscriptionReplayProcessAcceptance|TestDynamicClusterSessionMetadataProcessAcceptance)$' -parallel=2 -count=1 -v -timeout 20m
```

대기 전 시각 재사용은 deadline 도달/경과 message를 Delivered로 보고하여 실패했다. 새 현재 시각으로 absolute deadline을 판정한 뒤 실제 queue smoke는 999.999999ms에서 1개, 1s·2s에서 0개였다. Queue 조회도 최초 시각을 써서 expiry filter가 잘못된 enqueue를 숨기지 않게 했다. 임시 smoke는 제거했다. 보호 범위는 owner-node local publish이며 forwarded delivery와 연결 중 owner crash는 다음 절의 별도 경계다.

## 11.26 Detach 전환 forward·연결 중 owner crash 유실 경계 수용 매핑

스펙: [11.26](../specs/cluster-session-and-delivery.md#1126-detach-전환-forward연결-중-owner-crash-유실-경계-수용-매핑). 대상: `CLU-SCN-183`~`CLU-SCN-186`.

2026-10-04 Windows/amd64·Go 1.26.5에서 forward 비대기·유실 경계와 DETACH commit 전/후 owner crash 두 process가 통과했다(package 28.651s).

```text
go test ./cmd/narumq -run '^(TestClusterForwardDuringDetachIsDroppedWithoutBlockingLane|TestClusterDetachCapturesConcurrentLocalQoS0Publish)$' -count=1 -v
go test -race ./internal/adapters/memory/deliveryqueue ./internal/application/publish ./cmd/narumq -run 'Detach|LocalDetach|AcceptForwarded|ForwardDuringDetach' -count=3
GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterDetachOwnerCrashProcessAcceptance$' -count=1 -v -timeout 20m
```

Exit 86과 before/after-commit marker, owner 정지 중 offline count·metadata를 대조했다. 재접속은 `SessionPresent=1`, 이미 받은 live 메시지 재전달 없음이었다. Commit 뒤에는 outage→after-recovery를 받았고 commit 전에는 base=0이었다. 지연 peer frame에 따른 base=1도 허용한다. Crash 전 local queue 잔여의 유실 자체는 assert하지 않는다(socket write가 queue를 비울 수 있음). 연결 중 QoS 0 무손실·production 보존의 증거가 아니다.

## 11.27 Replication factor·write quorum profile 수용 매핑

스펙: [11.27](../specs/cluster-session-and-delivery.md#1127-replication-factorwrite-quorum-profile-수용-매핑). 대상: `CLU-SCN-187`~`CLU-SCN-194`.

2026-10-04 Windows/amd64·Go 1.26.5에서 focused·runtime·process·partition 시험이 통과했다. 수정 전 runtime은 profile 0/0·3-voter removal 202로 실패했고, 수정 뒤 profile 3/2·`409 replication_factor_unsatisfied`, voter 추가 후 같은 key의 removal 성공을 확인했다.

```text
go test ./internal/application/membership/... ./internal/application/management/... ./internal/adapters/http/management/... ./tests/contracts/... -count=1
go test ./cmd/narumq -run '^(TestClusterMembershipRemovalWaitsForReplicationFactor|TestRunClusterModeInProcess)$' -count=1 -v
GOMAXPROCS=4 NARUMQ_DURABLE_ACK_FAULTS=1 go test ./tests/integration -run '^TestDynamicClusterDurableAckPartitionProcessAcceptance$' -count=1 -v -timeout 20m
```

Leader 재검사 회귀는 service를 우회해 이미 commit된 BeginRemove를 모형화한다. Session 유무 두 경우 모두 transfer·ConfChange 미commit과 voters `{1,2,3}`·ownership/epoch·REMOVING 유지를 확인했다. 실제 partition에서는 minority no-ACK·majority handshake·복구 후 QoS 2 재개가 통과했다. 실제 production storage 수용은 범위 밖이다.

## 11.28 Publish backpressure 수용 매핑

스펙: [11.28](../specs/cluster-session-and-delivery.md#1128-publish-backpressure-수용-매핑). 대상: `CLU-SCN-195`~`CLU-SCN-205`.

2026-10-04 Windows local focused와 실제 3-broker acceptance는 commit lag·disk pressure 거절/회복, 허용 topic 각각 1회·거절 topic 미전달을 확인했다. 기존 quota/matrix/fencing와 ACK fault도 통과했다.

```text
go test ./internal/application/membership ./internal/application/publish ./internal/adapters/metrics ./internal/adapters/diskspace ./cmd/narumq ./internal/adapters/tcp ./tests/chaos/cluster-storage-v1 -count=1
go test ./tests/integration -run '^TestDynamicClusterPublishBackpressureProcessAcceptance$' -count=1 -v -timeout 20m
go run ./tests/chaos/cluster-storage-v1 --run-id backpressure-20261004 --output .artifacts/chaos/cluster-storage-v1/backpressure-20261004
go run ./tests/chaos/cluster-storage-v1 --profile local-ext4-v1 --run-id ext4-backpressure-20261004 --output .artifacts/chaos/cluster-storage-v1/ext4-backpressure-20261004
```

FUSE 7/7 통과. 새 follower-disk-pressure는 4,194,304 bytes를 남겨 1,003ms에 감지했고 readiness 200·HEALTHY를 유지하면서 target PUBLISH를 거절했다. Healthy quorum은 ACK하고 ballast 제거 뒤 target도 회복했다. ACKed missing/duplicate와 ACK outcome unknown은 없었다. 증거는 해당 `summary.json`과 scenario 기록이다.

Local ext4 4/4도 통과했다(256 MiB, WSL2 kernel `6.6.87.2-microsoft-standard-WSL2`, `data=ordered`). 이는 EBS·OS cache 손실이 아니다. 단일 BP-03 smoke `bp03-20261004T072411.781053400Z-42588`은 양 target valid였지만 `releaseGating=false`이며 production 성능·계약 등가·우위를 판정하지 않는다. 성능 기준 기록은 [성능 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)다.

#### 2026-10-06 failover-aware backlog와 미전송 proposal 회수 검증

대상: `CLU-SCN-199`~`CLU-SCN-202`.

확인 source `b175fc4563cdc00838f16829effbf9ccb4fb8246`의 runtime·TCP·Raft focused, 세 package race와 3-process backpressure acceptance가 통과했다. 미전송 frame만 회수·재생하며 write failure/phase refresh와 caller timeout·late replay의 기존 결과를 유지했다.

```text
go test ./internal/adapters/tcp -run '^(TestDynamicPeerReclaimUnsentReturnsOnlyUnwrittenProposals|TestCptPeerConnectionWriteFailureRetainsInFlightFrame|TestDynamicPeerPhaseRefreshPreservesInProgressOutboundFrame)$' -count=1 -v
go test ./internal/adapters/raft -run '^(TestDynamicDeferForwardedProposalReplaysUnsentFrameOnce|TestDynamicDeferredProposalsRespectDownstreamCredits|TestDynamicStepAllowsOnlyAuthenticatedForwardedRequests)$' -count=1 -v
go test -race ./cmd/narumq ./internal/adapters/tcp ./internal/adapters/raft -count=1
```

BP-06 failover·suite 판정은 [성능 §3.6](../specs/performance-and-verification.md#36-bp-06-failure-recovery-smoke-관찰-경계)에 있다. 이 회귀로 당시 invalid suite를 통과 처리하지 않는다.

#### 2026-10-06 driver 소유 backlog의 최종 검증

대상: `CLU-SCN-199`~`CLU-SCN-202`.

확인 source는 `90aa3b9515f9c22670b0c4a82160b2d3a80593e4`다. Wait의 무조건 제거를 없애고 accepted entry를 caller timeout 뒤에도 driver가 소유하는 최종 코드로 다음 명령이 통과했다.

```text
go build ./...
go vet ./...
go test ./cmd/narumq -run '^(TestClusterAppliedDeliveryStopsBackpressureBeforeCallerWait|TestClusterDurableCommitCallerTimeoutLateReplayAppliesOnceWithoutSuccess|TestClusterCommitBacklogOldestAge|TestClusterPublishPressureCommitLag)$' -count=1 -v
go test -race ./cmd/narumq -count=1
cmd /d /c "set GOMAXPROCS=4&& go test ./tests/integration -run ^TestDynamicClusterPublishBackpressureProcessAcceptance$ -count=1 -v -timeout 20m"
```

완료 entry는 Wait 전에 제거되고 미완료 accepted entry는 유지됐다. Late apply 뒤 제거·늦은 결과 buffer, 실제 process의 lag/disk 거절·회복을 확인했다. 이 회귀는 in-process Raft/WAL·state machine apply를 쓰지만 TCP worker나 `runtime.applyBatch` 전체를 직접 실행하지 않는다. 당시 최종 BP-06 suite는 invalid였으며 이후 proposal-recovery 결과와 구분한다.

## 11.29 Replicated-state v1 계약 수용 매핑

스펙: [11.29](../specs/cluster-session-and-delivery.md#1129-replicated-state-v1-계약-수용-매핑). 대상: `CLU-SCN-206`~`CLU-SCN-209`.

2026-10-04 Windows local vector·baseline·inactive-feature guard와 synthetic/historical release·archive 4개 process가 통과했다. Guard에 ownership field 15(`78 01`)를 주입하면 unknown-field 경로를 출력하며 실패하고 제거 뒤 통과했다. 벡터 hex와 baseline은 testdata가 기준이다.

```text
go test ./internal/adapters/raft/records ./cmd/narumq -count=1
NARUMQ_RELEASE_COMPAT=1 GOMAXPROCS=4 go test ./tests/integration -run '^(TestDynamicClusterReleaseCompatibilityProcessAcceptance|TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance|TestClusterBackupRestoreReleaseWindowProcessAcceptance|TestClusterBackupRestoreHistoricalReleaseProcessAcceptance)$' -count=1 -v -timeout 30m
```

증거는 `.artifacts/verification/replicated-state-v1-release.log`다. 지정 baseline `58a95f801e1467379db2eb6f3454ae44e954418a`의 거절 정책을 유지했으며 생산 storage·미래 게시 release 호환성의 증거가 아니다.

## 11.30 Feature activation 운영 승인 수용 매핑

스펙: [11.30](../specs/cluster-session-and-delivery.md#1130-feature-activation-운영-승인-수용-매핑). 대상: `CLU-SCN-210`~`CLU-SCN-222`.

2026-10-04 ADR·contract·guide의 입력/행동 표 대조는 **문서 정책 검토 통과**다. Runtime이 외부 승인 이력이나 operator 판단을 강제한다는 증거가 아니다.

별도 local shell smoke는 Windows/amd64, PowerShell 5.1.26100.9444·curl.exe 8.21.0·jq 1.8.2·Go 1.26.5·GOMAXPROCS=4, 임시 mTLS 3-broker synthetic release 1.2.0에서 통과했다. HTTP 22개와 JSON filter 13개가 성공했고 body·approval header·Idempotency-Key 없는 `compatibility-probe` activation, replay·GET의 동일 token과 readiness를 확인했다. 운영 `session-metadata`는 활성화하지 않았다.

```text
go test ./internal/application/membership ./internal/adapters/http/management ./tests/contracts/management-api-v1 -count=1
go test ./tests/integration -run '^TestFeatureActivationApprovalRunbookSmoke$' -count=1 -v -timeout 10m
NARUMQ_RELEASE_COMPAT=1 GOMAXPROCS=4 go test ./tests/integration -run '^(TestDynamicClusterReleaseCompatibilityProcessAcceptance|TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance|TestClusterBackupRestoreReleaseWindowProcessAcceptance|TestClusterBackupRestoreHistoricalReleaseProcessAcceptance)$' -count=1 -v -timeout 30m
```

일회용 `TestFeatureActivationApprovalRunbookSmoke`는 제거되어 이 명령으로 재실행할 수 없다. 비밀 없는 요청·출력·도구 버전·focused/smoke와 release 로그는 `.artifacts/verification/feature-activation-approval/`에 있다. 임시 directory의 사용자 전용 ACL도 확인했지만 secret manager·production ACL 시험은 아니다. 외부 ticket 승인·membership operation GET·불명 요청 취소·production activation은 미실행이다. Source 조합별 release 4개 통과를 미래 게시 release matrix로 쓰지 않는다. 후속 영구 하네스의 미실행 상태는 마지막 절에 있다.

## 11.31 연결 rebalance 수용 매핑

스펙: [11.31](../specs/cluster-session-and-delivery.md#1131-연결-rebalance-수용-매핑). 대상: `CLU-SCN-223`~`CLU-SCN-226`.

2026-10-04 Windows 작업 트리에서 focused 7개 package와 실제 3-node rebalance·기존 drain process가 통과했다. Rebalance 최종 분포 7/7/5, fairShare 7·ceiling 8, node 1 session 5개 이전·node 2 이전 0개였다.

```text
go test ./internal/domain/connrebalance ./internal/application/membership ./internal/adapters/raft/records ./internal/adapters/http/management ./internal/adapters/tcp ./cmd/narumq ./tests/contracts/management-api-v1 -count=1
go test ./tests/integration -run '^TestDynamicClusterConnectionRebalance' -count=1 -v -timeout 20m
```

초기 local shed가 tick마다 재선언되어 12/7/0에서 멈추던 결함은 대입으로 고친 뒤 통과했다. 취소·idempotency·drain 상호 배제·Server Reference·새 epoch ATTACH도 확인했다. Production 부하·실제 LB 배치·게시 release 사이 활성 feature 호환성은 미실행이다.

## 11.32 QoS 1/2 retained 수용 매핑

스펙: [11.32](../specs/cluster-session-and-delivery.md#1132-qos-12-retained-수용-매핑). 대상: `CLU-SCN-227`~`CLU-SCN-232`.

2026-10-05 Windows 작업 트리, 실제 broker 3개·MQTT socket·retained API로 unit와 process 1회+3회가 통과했다. Commit/ACK·replay·replace/delete·expiry·node 손실과 quorum no-ACK·QoS 2 재개를 확인했다. 네 실행 모두 `liveOpen=true`, `lateFailCommit=false`여서 reconnect·late fail commit 분기는 관찰하지 않았다.

```text
go test ./internal/application/publish -run '^TestPublishDurableRetainedCommitFailureStopsDeliveryCommit$' -count=1 -v
GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterRetainedQoS12ProcessAcceptance$' -count=3 -v -timeout 20m
```

첫 fixture는 연속 PUBLISH를 PUBREL로 읽어 실패했다. Live 발행은 한 건씩 확인하고 replay는 Packet ID별 PUBREL/PUBCOMP를 추적하도록 고쳤다. 미완료 QoS 2 timeout을 정상 종료로 처리하지 않는다. 제품 코드는 바꾸지 않았다. Retained Will·CI mutation·production storage는 이 실행에서 미실행이다.

## 11.33 안전한 proposal 복구 수용 매핑

스펙: [11.33](../specs/cluster-session-and-delivery.md#1133-안전한-proposal-복구-수용-매핑). 대상: `CLU-SCN-233`~`CLU-SCN-242`.

Windows/amd64·Go 1.26.5, 확인 source `4145b757e2d847a4c30549827e6f5b4b6f88785a`에서 receipt·digest·capacity·snapshot/baseline과 실제 Raft/WAL leader-loss 회귀가 통과했다.

```text
go test ./internal/application/membership ./internal/adapters/raft/records ./cmd/narumq ./tests/benchmark/bp06 -count=1
go test ./cmd/narumq -run TestClusterProposalRecoveryRequiresNewQuorumProofAndAppliesOnce -count=1 -v
go test ./cmd/narumq -run TestClusterProposalRecoveryNoQuorumNoSuccessAndNoLateAck -count=1 -v
go test ./tests/integration -run '^TestDynamicClusterProposalRecoveryLeaderKillInFlightCommit$' -count=3 -v
```

No-quorum 단위 시험은 취소를 쓰며 실제 5초 deadline을 기다리지 않는다. Process의 PUBACK COMPLETE·PUBREL RELEASE 활성/비활성 비교는 3/3 통과였다. 활성은 연결 유지·PUBCOMP를 관찰했고 비활성은 kill 뒤 약 4.5초에 종료했다.

옛 CPU 경쟁 진단은 feature 비활성 8회 중 4회 종료, 활성 8회 종료 0이었다. 활성 QoS 2 kill 20회 중 1회는 Windows WAL `Access is denied`로 시나리오 전에 시작 실패했고 나머지 19회는 PUBCOMP를 받았다. 이는 진단 기록이며 후속 영구 `CLU-SCN-241`의 실제 실패는 마지막 절에 보존한다.

BP-06 최종 suite의 두 target 5/5 valid와 NaruMQ의 gap·QoS 2 duplicate·계획 밖 disconnect·protocol error·overload 0은 [성능 §3.6](../specs/performance-and-verification.md#36-bp-06-failure-recovery-smoke-관찰-경계)의 run·immutable image 증거를 따른다. EMQX 예외·비등가 계약을 유지하며 production storage·게시 release의 feature mixed rollout은 이 결과로 승인하지 않는다.

## 11.34 Dynamic peer TCP 생존 확인 수용 매핑

스펙: [11.34](../specs/cluster-session-and-delivery.md#1134-dynamic-peer-tcp-생존-확인-수용-매핑). 대상: `CLU-SCN-243`~`CLU-SCN-244`.

Windows/amd64·Go 1.26.5·Docker Engine 29.4.0(linux/amd64). 기준 `2b23116` image `sha256:b7b092a6aa5d…`는 이전 IP blackhole 후 대체 node가 28.74초 정체하여 실패했다. 작업 트리 image `sha256:a1ccfbb4ff04…`는 첫 probe에서 통과했다. 0.016초는 process 시작이 아닌 `docker run` 반환 뒤 probe 시간이다.

```text
python -B tests/chaos/peer-blackhole-reconnect/run.py --image <2b23116 image> --output .artifacts/verification/peer-blackhole-before-20261015T1840Z
python -B tests/chaos/peer-blackhole-reconnect/run.py --output .artifacts/verification/peer-blackhole-after-20261015T1840Z
```

[수정 전](../../.artifacts/verification/peer-blackhole-before-20261015T1840Z/result.json)·[수정 후](../../.artifacts/verification/peer-blackhole-after-20261015T1840Z/result.json) 결과다. TCP package는 통과했지만 전체 local gate는 coverage에서 실패했다(line 83.85% < 85%, branch 79.16% < ratchet 80.03%). 기준 `2b23116`도 같은 coverage 단계에서 미달이었다. 기준 첫 시도의 `TestWALSyncObserverReportsSuccessfulReadyFsyncs` 실패도 있었으며, coverage 뒤 단계는 실행되지 않았다. 현재 전체 local 실패는 [TODO](../TODO.md)에서 추적한다.

#### EKS 재측정 관찰 (2026-10-16)

격리 `eks-capacity-20261016T015110Z-db4c38`, `2b23116` 작업 트리 ARM64 `sha256:edd5ef4a502e6b07c73bbf3464a92bc16f386e4fb9a9c906b5eddeda3ae211c1`에서 follower node 3을 종료했다. Ready 3 관찰은 workstation clock 기준 413.094초였다. 대체 node의 이전 약 130초 정체는 없었지만 preStop/drain이 실행되어 무응답 전원 손실 시험은 아니었다. 기존 Node가 이미 없어 `node_absent`·`skipped_not_fenced`였으며 detach 단축 재현도 아니다.

Host 시계 차로 음수였던 초기 health 차이는 `null`로 남겼다. Payload oracle·정확한 RTO·TCP_USER_TIMEOUT 효과 분리는 입증하지 않았다. [결과](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/peer-retest/result.json)의 부분 관찰을 아래 무응답 종료와 구분한다.

#### EKS 무응답 종료 재측정 (2026-10-16)

두 실행 모두 통과했다. 기준 증거는 [결과](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/result.json)·[분석](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/analysis.json)이다.

- 격리 synthetic fixture `eks-adr59-dns-20261016T053249Z-f3b329`, leader 2·대상 3·관찰 Pod 1. `2b23116` 작업 트리 ARM64 digest `sha256:3db1943f010bb8e6128e637c5a6713dce96bdbf4b7256c36564fbe5ad9a7f9a0`.
- `tests/chaos/eks-worker-recovery/main.py fault --execute`의 `terminate-instances skipOsShutdown=true`. 생존·대체 log와 표본에 preStop/drain·DRAINING·SAFE_TO_STOP이 없었다.
- Leader Pod `/proc/net/tcp`를 0.2초마다 읽어 tx_queue·재전송·상태를 기록했다. 두 실행은 CoreDNS 배치만 다르며 [Drain DNS 재현](drain-and-rolling-update.md#coredns-배치와-healthy-node-unknown의-eks-재현-2026-10-16)을 따른다.

| 관찰(초, fault 요청 기준) | CoreDNS 동시 손실 재현 | 배치 전제 적용 |
| --- | --- | --- |
| 이전 IP 송신 대기 첫 표본 | 6.01 | 5.51 |
| 이전 IP 연결 부재 첫 표본 | 16.32 | 15.83 |
| Fencing·기존 Pod 해제 | 39.09·43.85 | 21.85·25.79 |
| 대체 Pod 생성·container 시작 | 55.2·60.2 | 36.4·43.4 |
| 대체 release 동기화 | 64.56 | 46.86 |
| Leader 새 IP 연결 첫 표본 | 64.57 | 46.89 |
| Broker 3 Ready 관찰 | 79.11 | 55.98 |
| 관리 API·MQTT 성공 표본 | 27/27·46/46 | 21/21·33/33 |
| 데이터 oracle | 통과 | 통과 |

송신 대기 첫 표본 뒤 연결 종료는 10.31·10.32초였다. FailedAttachVolume은 모두 0건이고 이전 약 130초 정체는 없었다. DNS가 옛 IP를 반환하는 동안 재시도는 약 5초 뒤 닫혔고 새 Pod IP 게시 뒤 연결됐다. 실행마다 한 표본이며 host 간 시계 오차는 측정하지 않았다. 정확한 RTO·표본 사이 연속 가용성·keepalive와 TCP_USER_TIMEOUT의 개별 기여는 입증하지 않는다.

## 11.35 공유 구독 durable 전달의 owner 손실 수용 매핑

스펙: [11.35](../specs/cluster-session-and-delivery.md#1135-공유-구독-durable-전달의-owner-손실-수용-매핑). 대상: `CLU-SCN-245`~`CLU-SCN-256`.

Windows/amd64·Go 1.26.5에서 focused와 owner-loss process 5회가 통과했다. 수정 전 clean/persistent 각각 ACKed 40개 중 20개가 live member에게 가지 않았고, 수정 뒤 같은 재현의 미전달은 0이었다. 증거는 `.artifacts/analysis/n1-delivery/before-fix-shared.txt`, `after-fix-shared.txt`다.

```text
go test ./internal/domain/routing ./internal/application/membership ./internal/application/publish ./internal/adapters/raft/records -count=1
go test ./tests/integration -run TestDynamicClusterSharedSubscriptionOwnerLossProcessAcceptance -count=1 -v
```

초기 persistent 실행은 선출 중 PUBACK commit quorum 오류로 생존 연결이 닫혔고 test가 재접속하지 않아 queue가 남아 실패했다. Session 이어받기로 재접속하도록 고친 뒤 통과했다. 감지 전 죽은 node의 clean queue에 들어간 message는 그 member의 reconnect/reset까지 남고 persistent queue는 돌아올 때 받는다. 한 host process 증거이며 EKS·cross-AZ 감지 시간은 측정하지 않았다.

## 11.36 1 MiB 초과 snapshot 송신 수용 매핑

스펙: [11.36](../specs/cluster-session-and-delivery.md#1136-1-mib-초과-snapshot-송신-수용-매핑). 대상: `CLU-SCN-257`~`CLU-SCN-258`.

`TestDynamicOversizedSnapshotIsNotSentAndNodeStaysHealthy`는 수정 전 EKS와 같은 `raft node has failed: outbound message exceeds 1048576 bytes`로 실패했다. 수정 뒤 oversized snapshot 거절 9회·WARN 1회와 node health, `TestPeerWarningLimiterAdmitsOncePerPeerInterval`이 통과했다. 이는 거절 시 생존 보장이지 큰 snapshot 전송 성공이 아니다.

## 11.37 Raft snapshot stream 수용 매핑

스펙: [11.37](../specs/cluster-session-and-delivery.md#1137-raft-snapshot-stream-수용-매핑). 대상: `CLU-SCN-259`~`CLU-SCN-264`.

2026-10-16 Windows local focused·Raft/TCP/runtime/membership package와 Raft/TCP race가 통과했다. 실제 process 큰 snapshot catch-up은 10.71초에 송신 시작/완료·수신 log·retained digest를 확인했다. Synthetic release 66.47초, 지정 historical release 248.01초도 통과했다. Release 원시 출력은 `.artifacts/analysis/clu14-release-after.txt`, `clu14-historical-after.txt`다.

```text
GOMAXPROCS=2 go test -p=2 ./tests/integration -run '^TestDynamicClusterSnapshotStreamProcessAcceptance$' -count=1 -v -timeout 300s
GOMAXPROCS=2 go test -p=2 ./internal/adapters/raft ./internal/adapters/tcp ./cmd/narumq ./internal/application/membership -count=1
GOMAXPROCS=2 go test -race -p=2 ./internal/adapters/raft ./internal/adapters/tcp -count=1
NARUMQ_RELEASE_COMPAT=1 GOMAXPROCS=2 go test -p=2 ./tests/integration -run '^TestDynamicClusterReleaseCompatibilityProcessAcceptance$' -count=1 -v -timeout 400s
NARUMQ_RELEASE_COMPAT=1 GOMAXPROCS=2 go test -p=2 ./tests/integration -run '^TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance$' -count=1 -v -timeout 15m
```

별도 개발 시험에서 비활성 feature로 stage한 learner는 큰 snapshot을 받지 못했고 activation은 `409 feature_not_supported_by_all_members`였다. **그 learner 제거 후 복구도 timeout으로 입증하지 못했다.** 따라서 장애·큰 snapshot join 전에 feature를 활성화해야 한다. Local 시험을 실제 EBS power-loss·게시 release 전체 수용으로 확장하지 않는다. 실제 EKS R1은 §11.20에 있다.

첫 synthetic release 실패는 crash 직후 live `/api/v1/nodes`의 null version을 읽은 fixture 문제였다. Commit된 `/api/v1/cluster/features` member profile을 조회한 뒤 통과했다. Feature 미지원 거절·stream 실패로 분류하지 않으며 assertion·production 코드는 바꾸지 않았다.

## 11.38 Fresh read proof 응답 유실 복구 수용 매핑

스펙: [11.38](../specs/cluster-session-and-delivery.md#1138-fresh-read-proof-응답-유실-복구-수용-매핑). 대상: `CLU-SCN-265`. 성능 spec의 burst 증거는 [성능 검증 기록 §3.4](performance-and-verification.md#34-bp-03-durable-delivery-기록)에 있다.

대상 변경은 HEAD `c608aa7`에 더한 미커밋 변경이다. 환경은 Windows 10.0.26200 amd64, Go 1.26.5다. Race 실행은 `CGO_ENABLED=1`과 gcc를 썼다. 2026-10-18 local 실행이며 EKS와 실제 process 수준 시험은 미실행이다.

수정 전에는 `TestClusterConcurrentOriginsReadBurstCompletesAfterEgressDrain`이 `-count=20`에서 일반 10회·`GOMAXPROCS=2` 4회·`-race` 1회 실패했다. 실패 문구는 `fresh proofs completed=189/192 after egress drain`에서 `191/192`까지였고, `GOMAXPROCS=2`에는 `128/192`도 있었다. 원인은 임시 계측으로 확인했다. 늦게 연결한 node 3으로 가는 leader FIFO(`peerQueueMessages=32`)가 오래된 heartbeat와 `MsgApp`으로 가득 차 `MsgReadIndexResp`가 `ErrPeerBackpressure`로 거절됐다. `sendMessages`는 거절한 frame을 다시 보내지 않았고, `refreshReadWaiters`는 leader가 바뀔 때만 proof를 다시 발행했다. 그 read는 2초 deadline까지 남아 node 3의 발행 예산 5개 중 하나를 계속 차지했다. 실패 실행의 leader는 `MsgReadIndex` 64개를 받고 응답 61–63개만 보냈다.

Egress가 비기를 기다리는 시험 변경은 `-race` 19/20, `GOMAXPROCS=2` 3/20 통과에 그쳐 채택하지 않았다. Production을 고쳐 ReadState가 `clusterReadProofRetryInterval`(500ms) 안에 오지 않은 proof를 새 nonce로 다시 발행한다. Burst 시험의 192개 proof 완료와 queue 상한 assertion, 2초 deadline은 바꾸지 않았다.

```text
go test ./cmd/narumq -run '^TestClusterConcurrentOriginsReadBurstCompletesAfterEgressDrain$' -count=50 -v
CGO_ENABLED=1 go test -race ./cmd/narumq -run '^TestClusterConcurrentOriginsReadBurstCompletesAfterEgressDrain$' -count=20 -v
GOMAXPROCS=2 go test ./cmd/narumq -run '^TestClusterConcurrentOriginsReadBurstCompletesAfterEgressDrain$' -count=20 -v
```

수정 뒤 결과는 일반 50/50, `-race` 20/20, `GOMAXPROCS=2` 20/20 통과다. 재발행이 필요한 실행은 약 0.6초, 나머지는 약 0.15초 걸렸다. 새 시험 세 개(`TestClusterLostProofResponseIsReissuedAfterRetryInterval` 외 둘)와 같은 파일의 다른 burst 시험 두 개는 `-count=30`에서 통과했고 새 시험은 `-race -count=5`도 통과했다. 재발행 판정을 항상 거짓으로 바꾼 임시 복사본에서는 새 시험 두 개와 burst 시험이 실패했다. 따라서 새 시험은 이 결함을 잡는다.

`go vet ./cmd/narumq`, `go test ./cmd/narumq -count=1`(30.8초), `go run ./tools/verify scenarios`(635개, 미연결 0·오류 0), `go run ./tools/verify markdown-links`가 통과했다. 이 결과는 재발행이 유실한 응답을 복구한다는 근거다. Incoming frame의 무손실이나 BP-03 수용 보장은 아니다.

## 2026-10-17 시나리오 연결 보완

이번 작업은 소스 메타 정보와 영구 회귀를 보완했으며 과거 EKS를 재실행하지 않았다. 시나리오별 연결의 현재 기준은 소스 `Scenarios:`와 생성 보고서다. 전체 623개 중 연결 586개·미연결 37개·오류 0개였으며 결과 입력 없는 검사는 미실행을 통과로 바꾸지 않는다.

- `CLU-SCN-017` module pin·go.sum·ADR와 실제 module verify/download 검사, `CLU-SCN-156`의 worker append+snapshot handle 교체 회귀, `CLU-SCN-241`의 CPU 경쟁 persistent subscriber 진단을 영구 시험으로 보완했다.
- 기존 P1/T1/T2/R1·release activation/replay·BP-06 실행 함수에는 메타 정보를 연결했을 뿐 EKS·Docker benchmark를 재실행하지 않았다.
- 삭제된 activation shell smoke 대신 `tests/chaos/feature-activation-runbook/main.py`를 추가했지만 실제 smoke는 미실행이다. 문법 검사만 통과했다.

미연결 범위를 대체 시험으로 덮지 않았다:

| 시나리오 | 남은 이유 |
| --- | --- |
| `CLU-SCN-046`, `CLU-SCN-047`, `CLU-SCN-050` | EKS backup·retention probe·S3 full restore 실측은 있으나 전체 실행 재현 하네스를 찾지 못했다. 배포 script·fake S3 시험은 대체가 아니다. |
| `CLU-SCN-136` | EmptyDir Job 생성부터 빈 directory·기존 3/3 Ready까지 판정하는 하네스를 찾지 못했다. Local survivor 2개 조건과 다르다. |
| `CLU-SCN-137` | 기준 snapshot 3개 completed부터 동일 voter EC2/EBS stop/start·marker 복구까지의 하네스를 찾지 못했다. 전체 voter P1은 이 행 전체를 대체하지 않는다. |
| `CLU-SCN-159` | `SPEC-TBD-09` retention/RPO/RTO가 열려 있다. Power-loss/reseed로 보존 정책을 승인하지 않는다. |
| `CLU-SCN-210`~`CLU-SCN-215`, `CLU-SCN-217` | 외부 승인·재승인·결과 불명 POST·전진 복구/별도 DR 승인은 operator 판단이다. API token/readiness 시험은 해당 절차 전체를 자동화하지 않는다. |

### 로컬 실행 결과

2026-10-17 Windows/amd64 작업 트리, 생산 코드 변경 없이 실행했다.

| 명령 | 실제 결과 |
| --- | --- |
| `GOMAXPROCS=2 go test -p=2 -count=1 ./internal/adapters/raft -run TestWALWorkerAndSnapshotHandleReplacementKeepRecoverableState -v` | 통과. 서로 다른 snapshot index 20개·수락 proposal apply·재시작 복구를 확인했다. |
| `GOMAXPROCS=2 go test -race -p=2 -count=3 ./internal/adapters/raft -run TestWALWorkerAndSnapshotHandleReplacementKeepRecoverableState` | 통과, race 오류 없음. |
| `GOMAXPROCS=2 go test -p=2 -count=1 ./tests/integration -run '^(TestRaftModuleMatchesApprovedVersionAndChecksums\|TestN1PersistentClose)$' -v` | **실패**. `TestN1PersistentClose` 첫 회 subscriber 종료 4개·PUBACK quorum 오류 4개, 나머지 2회는 각각 0개. Module 검사는 `go mod verify`의 120초 context timeout으로 실패. Package 180.415s. |
| `python -m py_compile tests/chaos/feature-activation-runbook/main.py tests/chaos/eks-worker-recovery/power_loss.py tests/chaos/eks-wal-torn-tail/main.py tests/chaos/eks-volume-reseed/main.py` | 문법 검사 통과. 실제 외부 실행은 아님. |
| `go run ./tools/verify scenarios` | **실패**. 미연결 37개·오류 0개·연결 테스트 미실행 586개. 결과 파일을 입력하지 않았다. |

`CLU-SCN-241` 실패 회차는 `proposal-recovery` 활성 상태, publisher 기록 76개였다. Node 2에는 `persist typed membership snapshot: raft node has failed: raft storage operation failed: compact WAL: replace WAL: Access is denied.`·`storage_io_error`, node 1·2에는 `dispatch MQTT PUBACK: quorum unavailable`이 남았다. 시험 부하·판정은 완화하지 않았고 이 storage 실패를 재실행하거나 수정하지 않았다.

`CLU-SCN-017`은 분리 실행에서 후속 통과했다. `GOMAXPROCS=2 go mod verify`는 47.18초에 `all modules verified`, `GOMAXPROCS=2 go mod download -json go.etcd.io/raft/v3@v3.7.0`은 지정 version·Sum·GoModSum을 반환했다. 이어 `GOMAXPROCS=2 go test -p=2 -count=1 ./tests/integration -run '^TestRaftModuleMatchesApprovedVersionAndChecksums$' -v`는 통과했다(test 5.75s·package 8.016s). 120초 기한·판정은 바꾸지 않았다. **결합 실행 timeout과 단독 실행 통과는 서로 다른 결과이며 `CLU-SCN-241`의 실패는 남아 있다.**

## 2026-10-17 Windows 파일 공유 잠금 후속 검증

대상은 `b13d8254754bdb5333ae28d47e25c9d51cd85590` 기반 작업 트리다. 이번 변경은 Raft adapter의 Windows atomic rename, 실제 파일 공유 잠금 회귀, `CLU-SCN-153` 기준과 관련 문서다. 환경은 Windows/amd64, Go 1.26.5, `GOMAXPROCS=2`다. Application proposal·MQTT deadline·CPU 부하·process 시험의 판정은 바꾸지 않았다.

`os.Open`으로 기존 WAL을 읽는 handle을 유지하면 store가 자기 handle을 닫아도 교체가 거절됐다. 수정 전 `TestStableStoreCompactionWindowsSharingLock`의 transient case는 `compact WAL: replace WAL: Access is denied.`로 실패했다. 교체 시도까지 9.3559ms였고 reader는 100ms 뒤에 닫혔다. Persistent case는 오류 반환 뒤 기존 기록 복구를 확인했다. 이는 실제 Windows 파일 공유 잠금 재현이다. **과거 `CLU-SCN-241` 실패 당시 잠금을 잡은 프로세스는 확인하지 못했다.**

수정은 [ADR-0023](../decisions/ADR-0023-raft-wal-compaction-and-snapshot-retention.md)의 제한된 atomic rename 재시도를 따른다. Linux 경로와 저장 형식은 그대로다. 실제 reader 잠금이 풀리면 교체 후 entry 11 append와 commands 1–11 복구가 성공했다. 잠금을 유지하면 약 258ms에 오류를 반환했고 commands 1–10이 복구됐다. 원본 temp의 reader가 막는 교체도 별도로 통과했다.

명령은 저장소 root에서 실행했다. 각 명령의 실행 시간·exit code·원문 log는 [증거 디렉터리](../../.artifacts/verification/wal-sharing-20261017/)에 있다.

| 명령 | 실제 결과 |
| --- | --- |
| `go test -p=2 ./internal/adapters/raft -count=1 -timeout=180s` | 통과. Package 7.888s. |
| `go test -p=2 ./tests/integration -run '^TestN1PersistentClose$' -count=1 -v -timeout=240s` | 통과. Package 61.423s. 기본 CPU 경쟁 아래 3회 모두 subscriber 종료 0·PUBACK quorum 오류 0. Publisher 기록은 차례로 268·274·274개. |
| `go test -race -p=2 ./internal/adapters/raft -run '^(TestStableStoreCompactionWindowsSharingLock\|TestRenameFileWindowsSourceSharingLock\|TestWALWorkerAndSnapshotHandleReplacementKeepRecoverableState\|TestStableStoreCompactionFailureKeepsRecoverableState)$' -count=3 -json -timeout=180s` | 통과. Package 5.311s. 실제 source·destination 공유 잠금, 지속 잠금의 복구, 기존 오류 주입과 worker/handle 교체를 확인했다. Race 오류 없음. |
| `go run ./tools/verify local` | **실패**. Branch 계측 통과와 연결 검사의 미연결 37개·오류 0개를 확인했다. 같은 log에는 vet·traceability·architecture·coverage·race·fuzz 명령도 exit 1로 기록됐다. 당시 runner가 취소 원인을 보존하지 않아 이 log만으로 각 명령의 자체 실패와 취소를 구분할 수 없다. 전체 package·coverage·production race·fuzz 통과를 주장하지 않는다. |
| `go run ./tools/verify scenarios --results .artifacts/verification/wal-sharing-20261017/scenario-results.jsonl --output .artifacts/verification/wal-sharing-20261017/scenarios` | **실패**. 이번 실제 결과만 입력한 [보고서](../../.artifacts/verification/wal-sharing-20261017/scenarios/report.md)에서 `CLU-SCN-153`·`CLU-SCN-156`·`CLU-SCN-241`은 통과했다. 전체는 통과 3·미실행 583·미연결 37·실패 0·오류 0이다. |
| `go run ./tools/verify markdown-links` | 최종 문서 변경 뒤 통과. 파일 190개·로컬 링크 3151개. |
| `go vet ./internal/adapters/raft` | 통과. 전체 local의 vet과 별도 실행이다. |

시나리오 결과 입력은 race JSON의 최상위 `pass` event와 실제 process 통과 log에서 만들었다. 과거 실패는 앞 절에 보존하고 이번 보고서에는 이번 실행 결과만 넣었다. 기본 생성 보고서는 local preflight의 연결 검사이며 이번 실행 결과 보고서는 위 전용 경로에 보존했다. 기존 전체 coverage 미달, 미연결 37개, 원격 CI와 EKS 수용은 이 집중 검증으로 해제하지 않는다.

## 2026-10-17 Gate 연결의 실행 경로 보완

앞 절의 미연결 사유에 맞춰 실행 경로를 추가했다. 과거 EKS 실행 결과를 새 하네스의 통과 결과로 재사용하지 않았다.

| 범위 | 새 실행 경로와 판정 경계 |
| --- | --- |
| `CLU-SCN-046`·`047` | `tests/chaos/eks-backup-dr/main.py backup-retention`. 실제 CronJob·backup PVC·NMQBKUP3 integrity·commit index 100·3-voter offline seed와 오래된 단일 probe의 삭제를 판정한다. 전체 cluster RTO는 주장하지 않는다. |
| `CLU-SCN-050`·`159` | 같은 도구의 `s3-full-restore`·`production-storage-retention`. 새 PVC 세 개의 실제 복원과 MQTT 복구를 판정한다. Production 조합은 실제 30일 백업 이력, power-loss·EBS 재시드와 1시간 RTO를 모두 요구한다. 새 fixture만으로는 30일 조건을 채울 수 없다. |
| `CLU-SCN-136` | `tests/chaos/eks-emptydir-guard/main.py`. Member 3이 member 1의 term 6·index 100을 보고 bootstrap을 거절하는 고정 입력을 요구한다. 빈 directory 무변경과 기존 broker 3/3 Ready도 판정한다. 다른 term·index로 바꾼 시험을 이 행의 통과로 세지 않는다. |
| `CLU-SCN-137` | `tests/chaos/eks-worker-recovery/main.py single-voter-stop-start`. Marker PUBACK·snapshot 세 개 완료 뒤 단일 voter의 EC2 stop/start와 같은 EBS·WAL prefix·나머지 파일·marker 복구를 판정한다. `createSnapshots: true`가 만든 snapshot은 자동 삭제하지 않는다. |
| `CLU-SCN-210`~`215`·`217` | `tests/chaos/feature-activation-runbook/decision.py`. Runbook 형식의 관찰·외부 승인 기록을 읽고 보류·재승인·결과 불명·rollback window 폐쇄·전진 복구를 판정한다. 이는 운영 정책 모델의 검증이다. 사람의 절차 준수나 broker의 승인 기록 강제를 입증하지 않는다. |

EKS 도구는 `--execute`, 전용 private 입력과 같은 run의 kubeconfig·AWS 자원 신원을 요구한다. `--help`에 입력·변경 범위가 있다. 실제 EKS 수용은 이번 작업에서 미실행이다. 로컬의 fake fixture 단위 시험을 해당 EKS 시나리오에 연결하지 않았다.

실제 Alpine container에서는 `single_voter_stop_start.py`의 inventory·prefix hash shell을 실행했다. 전체 파일과 앞 5 byte의 SHA-256이 독립 계산과 같았고 read-only로 mount한 원본은 그대로였다. [실행 증거](../../.artifacts/verification/gate-completion-20261017/wal-inventory-proof.json)는 shell 경로만 입증하며 EC2·EBS 복구를 대신하지 않는다.
