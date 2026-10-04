# Cluster 잔여 작업

이 문서는 cluster [수용 시험](specs/cluster-session-and-delivery.md#11-수용-시험)과 관련 결정 기록(ADR)에서 아직 닫히지 않은 작업을 상태별로 모은 목록이다. 프로젝트 전체 백로그는 아니다. 동작과 완료 판정의 기준은 각 항목에 링크한 스펙·ADR이다. 시험 명령, 실행 시간, run ID 같은 실행 증거는 링크한 수용 매핑에만 기록하고 이 목록에는 반복하지 않는다.

| 표기 | 뜻 |
| --- | --- |
| `[ ]` | 열린 작업이다. 착수 전 링크한 기준 문서에서 범위를 확인한다. |
| `[ ]` **보류** | 외부 결정이나 실행 환경이 없어 착수하지 않는 작업이다. 보류 사유와 해제 조건을 함께 적는다. |
| `[x]` | 적힌 범위의 수용 시험을 통과했다. 범위 밖 gate는 §1의 열린 작업으로 따로 추적한다. |

열린 항목의 **미구현**은 필요한 기능이 없다는 뜻이고, **부분 구현**은 현재 기능이 요구 범위의 일부만 충족한다는 뜻이다. **결정 필요**는 구현·시험 전에 정책이나 계약을 확정해야 하며, **미검증**은 구현 증거가 있어도 해당 수용 시험은 남아 있다는 뜻이다. 후속 ADR과 최신 수용 매핑에서 해결된 과거 항목은 다시 열지 않는다.

## 1. 열린 작업

### 1.1 Production storage와 disaster recovery

2026-10-03 사용자 결정으로 실제 인프라 시험을 보류한다. 아래 두 항목이 닫히기 전에는 CLU-14, [ADR-0009 §7](decisions/ADR-0009-raft-production-state-profile.md#7-검증)의 production 행과 [ADR-0003 §9](decisions/ADR-0003-durable-ack-barrier.md#9-검증) 전체를 완료로 표시하지 않는다.

- [ ] **보류** — Linux/EBS 전원·volume 손실 수용: 지원 Linux filesystem/EBS에서 전원 손실 뒤 마지막 durable commit 보존, production WAL torn-tail 판정, volume loss/reseed를 시험한다.
  - 보류 사유: 인프라 시험을 보류했다. 시험용 AWS 계정이 없고 [PRD §18](requirements/PRD.md#18-미결정-사항) 질문 16(EKS minor version, EC2 instance family, EBS volume type·IOPS, NLB target mode)이 정해지지 않았다.
  - 해제 조건: 인프라 시험 재개 결정, 시험 계정·region 확보와 질문 16의 storage profile 확정.
  - 이 gate를 대신하지 않는 local 증거: Docker/FUSE 장애·power-cut simulation [§11.6](specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping), WAL lifecycle·torn-tail [§11.22](specs/cluster-session-and-delivery.md#1122-wal-compactionsnapshot-retentiontorn-tail-수용-매핑), voter volume 손실 [§11.20](specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑), local ext4 [§11.23](specs/cluster-session-and-delivery.md#1123-local-ext4-filesystem-수용-매핑).
- [ ] **보류** — Backup 보존 기간과 disaster-recovery RPO/RTO(`SPEC-TBD-09`): [Cluster §10](specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore) 7항의 보존 기간과 RPO/RTO를 정하고 restore 시간·보존을 시험한다. 현재 backup은 수동 online API와 offline restore만 제공하며 정기 backup과 외부 저장소는 없다.
  - 보류 사유: [PRD §18](requirements/PRD.md#18-미결정-사항) 질문 12의 정책 값이 정해지지 않았다.
  - 해제 조건: 질문 12에 대한 답변. 시험은 위 Linux/EBS 항목과 같은 실행 환경을 사용한다.

### 1.2 Release와 rollout gate

- [ ] **미검증** — 게시 release별 historical-source N-1↔N 호환성: 실제 release를 게시할 때마다 직전 게시 release와의 rolling upgrade·rollback·archive restore를 [§11.21](specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑) 방식으로 추가한다. 지정 baseline `58a95f8`만 통과했으며 현재 게시 release tag는 없다.
- [ ] **보류 · 미구현** — External rollout controller: [ADR-0006](decisions/ADR-0006-external-rollout-controller.md)(Proposed)의 `SAFE_TO_STOP` 기반 Pod 삭제 승인을 구현·시험한다. Custom resource와 reconciliation 계약, version preflight, 실패·timeout·unknown barrier에서 현재와 다음 node의 교체 중단을 포함한다([Release contract §8](contracts/cluster-release-compatibility-v1.md#8-blocker와-제외-범위)). Broker 쪽 drain은 완료했다([Drain spec §10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)).
  - 보류 사유: Kubernetes 실행 환경이 필요한 인프라 시험이다.
  - 해제 조건: 인프라 시험 재개 결정.
- [ ] **미검증** — BP-07 rolling drain 비교: PRD Phase 3 출시 gate다. 착수 전 측정 환경과 node 교체 경로를 정한다. Local BP-03 smoke 통과를 대신 쓰지 않으며, release의 수치형 합격 기준은 [성능 §3](specs/performance-and-verification.md#3-benchmark-공통-계약)의 `SPEC-TBD-08`을 따른다.
- [ ] **미검증** — 최대 traffic 연속 100회 rolling: [Drain spec §9](specs/drain-and-rolling-update.md#9-수용-시험)의 MQTT 3.1.1/5·QoS 0/1/2·retained·Will·persistent session 혼합, gap·duplicate·reorder와 controller 중단을 검증한다. ADR-0028의 3-round broker process 시험으로 이 gate를 대신하지 않는다.
- [ ] **미검증** — 지원 SDK의 Server redirect·Will 호환성: [Client contract §6.2](contracts/mqtt-client-compatibility-v1.md#62-drain-compatibility-suite)의 `drain-paho-go-python-will-acl-final`은 9/10이다. Paho Go MQTT 5 `v0.23.0`의 `go-mqtt5-malformed-duplicate-server-reference`가 `serverReferenceAccepted=true`, `trap_accept=1`로 실패했다. 두 SDK의 callback adapter redirect·session resume·후속 전달·Will 억제와 Python malformed 4종은 통과했다([Drain spec §10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)). Artifact는 `.artifacts/compatibility/mqtt-clients-v1/drain-paho-go-python-will-acl-final/`이다. 범위는 test-only readiness LB와 시간 값이며 `SPEC-TBD-04` 운영 수치와 rollout timing은 제외한다. SDK 결함은 우회하지 않았다. Will 사유별 broker 계측은 §2.4에서 완료 범위를 추적한다.
  - 원인: Paho `Properties.Unpack`이 중복 Server Reference를 검사하지 않고 마지막 값으로 덮어쓴다. Callback에는 중복 정보가 남지 않는다([Client contract §6.2](contracts/mqtt-client-compatibility-v1.md#62-drain-compatibility-suite)).
  - 결정: 공식 SDK만 유지한다. 직접 패치한 SDK와 raw socket 검사는 도입하지 않는다.
  - 해제 조건: 이 결함을 고친 공식 SDK release의 version 변경을 검토한 뒤 drain 10개와 Phase 1 26개 case를 모두 통과한다. 조사 시점에는 수정된 공식 release가 없어 SDK 수정과 새 version 회귀는 실행하지 않았다.
- [ ] **결정 필요** — Replicated-state 계약과 schema migration: [ADR-0009 §1.3](decisions/ADR-0009-raft-production-state-profile.md#13-replicated-application-command와-snapshot-state-encoding)의 공식 크기 제한, migration 규칙과 release compatibility vectors를 확정한다. 현재 [protobuf schema](contracts/replicated-state-v1.proto)와 codec은 구현됐지만 Draft이며 unknown field를 거절한다. [Release contract §2·§5](contracts/cluster-release-compatibility-v1.md#2-compatibility-축)의 optional field 처리·persisted record·양방향 snapshot 규칙과의 관계를 정리하고 producer/consumer 시험을 연결한다. Synthetic·지정 baseline 통과만으로 미래 schema 변경의 호환성을 완료로 표시하지 않는다.
- [ ] **결정 필요** — Rollback window 종료와 irreversible feature activation 승인: [Release contract §8](contracts/cluster-release-compatibility-v1.md#8-blocker와-제외-범위)에 남은 운영자 판정 정책과 승인 절차를 정한다. 현재 feature 광고·activation·미지원 build readiness 차단은 구현됐지만, rollback 가능 기간을 끝내는 운영 정책은 별도다. 확정한 절차와 [Cluster §8](specs/cluster-session-and-delivery.md#8-mixed-version-compatibility)의 irreversible migration 금지를 연결해 검증한다.

### 1.3 Cluster 기능과 알려진 구현 한계

- [ ] **부분 구현 · 결정 필요** — Replication factor와 write quorum 설정(CLU-09): [PRD §9.2](requirements/PRD.md#92-클러스터와-세션)와 [Cluster §4](specs/cluster-session-and-delivery.md#4-replicated-state)는 설정 가능한 replica·write quorum을 요구하지만, [ADR-0009 §1.4](decisions/ADR-0009-raft-production-state-profile.md#14-consensus-group-partition-profile)는 고정 voter `{1,2,3}`의 단일 group profile을 채택했다. 현재 dynamic membership은 voter 수를 바꿀 수 있으나 commit은 Raft 과반수를 따르며 `cmd/narumq/cluster_config.go`에는 별도 replication factor/write quorum 설정이 없다. 이 요구와 채택된 profile의 설계 차이를 먼저 결정 기록으로 해소한다. Voter 수 변경만으로 별도 설정 요구를 충족했다고 표시하거나 Raft 과반수보다 약한 ACK를 도입하지 않는다. 확정한 지원 설정·거절 조건에 따라 replica 조건이 부족할 때 성공 durable ACK가 없는지 시험한다.
- [ ] **부분 구현 · 결정 필요** — Publish backpressure와 포화 target 처리(CLU-12): storage health와 bounded peer queue 제어는 있지만, [ADR-0015 §5·§6](decisions/ADR-0015-cluster-durable-qos-delivery.md#5-대안)은 포화 target을 `Saturated`로 건너뛰고 publisher 성공을 허용하는 차이를 명시한다. [Cluster §4](specs/cluster-session-and-delivery.md#4-replicated-state)의 replication lag·disk pressure 기반 admission과 PRD의 drop보다 publish 제한 우선 기준에 대해 남은 범위를 정한다. 현재 승인된 포화 처리 의미를 조용히 바꾸지 않고, 기준·계측·admission 동작을 결정한 뒤 포화와 회복 시험을 연결한다.
- [ ] **미구현 · P1** — 연결·session rebalance(CLU-13): [Cluster §9](specs/cluster-session-and-delivery.md#9-rebalance)와 [PRD §9.2](requirements/PRD.md#92-클러스터와-세션)를 따른다. 현재 planned removal/drain의 ownership 인계는 있지만 증설 뒤 부하 편차를 줄이는 rebalance는 없다. 편차 기준과 이동 속도를 정하고, 증설 뒤 연결 수 수렴·새 epoch fencing·committed delivery state의 ATTACH 재개를 검증한다. P0 수용 범위와 분리한다.

### 1.4 Drain 정책·관측과 production 배포

- [ ] **결정 필요** — Drain·reconnect 운영값(`SPEC-TBD-04`): [ADR-0016 §6](decisions/ADR-0016-node-drain-and-safe-to-stop.md#6-결과)과 [MQTT client contract §4](contracts/mqtt-client-compatibility-v1.md#4-session과-expiry)의 connection rate, LB 전파 대기, timeout, reconnect/backoff·Will safety margin과 최악 termination grace를 확정한다. 현재 drain API는 rate/delay/timeout을 호출자가 지정하며 제품 기본값이 없다. Phase 1 test-only 값을 production 기본값으로 쓰지 않는다. 확정한 값은 SDK 시험과 배포 admission의 기준으로 연결한다.
- [ ] **보류 · 미구현** — Kubernetes 종료 연동과 `EKS_EC2_HA` 배포 artifact(DEP-13~DEP-23): [Drain spec §7·§7.1](specs/drain-and-rolling-update.md#7-external-rollout-gate와-kuberneteseks), [ADR-0007 §7](decisions/ADR-0007-aws-production-deployment-platform.md#7-검증)과 [EKS architecture](architectures/eks-ec2-deployment.md)의 목표 범위다. `preStop`의 idempotent drain 호출·결과 기록, grace period 검증, PDB·3-AZ placement·교체 동시성, stable node identity와 EBS 결합, NLB target lifecycle 및 profile별 지원 상태를 배포 artifact에 연결한다. Controller 구현은 §1.2, storage 전원·volume 시험은 §1.1에서 추적한다.
  - 보류 사유: 인프라 시험 보류 중이며 ADR-0006/0007은 Proposed, EKS architecture는 Target/Draft이고 구현 증거가 없다.
  - 해제 조건: 인프라 작업 재개와 배포 결정 채택, [PRD §18](requirements/PRD.md#18-미결정-사항) 질문 16의 profile 확정 및 실행 환경 확보.
  - 완료 확인: hook 중복·실패·grace 부족, NLB 전파 중 신규 연결, Pod·EC2·단일 AZ 손실, EBS 재연결과 연속 rolling upgrade·rollback을 실제 profile에서 검증한다. Broker process의 `SAFE_TO_STOP` 통과로 대신하지 않는다.

### 1.5 설계 미결정과 비지원 범위

[Cluster architecture의 미결정 사항](architectures/cluster-architecture.md#미결정-사항)은 과거 target과 현재 구현을 함께 언급한다. 아래 경계는 새 기능의 구현 승인이 아니며, 현재 지원 범위를 넓힐 때 먼저 결정해야 할 사항이다.

- Route-index 물리적 배치와 rebuild 방식: 현재 node-local projection rebuild는 §2.4의 수용 범위에서 완료했다. Architecture의 물리적 배치 질문을 기존 rebuild 미구현으로 다시 열지 않는다. 별도 분산 배치나 rebuild 방식 변경이 필요하면 [ADR-0009 §1.4](decisions/ADR-0009-raft-production-state-profile.md#14-consensus-group-partition-profile)의 단일 group·node-local projection과의 관계, ordering과 routing readiness를 먼저 확정한다.
- Active peer transport 밖 RPC encoding: 현재 [Raft membership v2](contracts/raft-membership-v2.md)와 live QoS 0 forwarding은 구현·수용 범위가 있다. 그 밖의 RPC를 도입할 때 producer/consumer, message 형식·한도·인증·version 호환 계약을 먼저 정한다. Architecture의 일반적인 encoding 질문만으로 현재 경로 전체를 미구현으로 표시하지 않는다. Persisted schema의 미결정 계약은 §1.2에서 추적한다.
- Cross-cluster migration과 3-bootstrap-voter identity 제한 밖 restore: [Cluster §10](specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore)의 현재 비지원 범위다. V1/v2 archive, learner를 포함한 archive와 voter-set 변경도 현재 restore로 지원하지 않는다. 별도 migration·identity 계약의 채택 없이 자동으로 구현 백로그에 넣거나 CLU-14의 현 수용 범위로 포함하지 않는다. Retained·offline·durable delivery backup 자체는 §2.3·§2.4에서 완료 범위를 확인한다.

## 2. 완료한 범위

### 2.1 합의·transport·membership

- [x] 고정 voter `{1,2,3}` production peer transport(mTLS 인증, framing, bounded queue, Raft 재생성): [ADR-0002 §7](decisions/ADR-0002-cluster-consensus-library.md#7-검증) 항목 5, [§11.3](specs/cluster-session-and-delivery.md#113-phase-0-gate-4-production-peer-transport-mapping). [ADR-0008](decisions/ADR-0008-raft-peer-transport.md) 범위에 한정한다.
- [x] Raft production profile 설계 결정과 module checksum 확인: [ADR-0009](decisions/ADR-0009-raft-production-state-profile.md), [§11.4](specs/cluster-session-and-delivery.md#114-adr-0009-design-target과-module-checksum-증거). 설계 목표만 채택했으며 runtime 수용은 아니다.
- [x] Dynamic membership join·leave, 변경 중 crash/restart 수렴, quorum 확인 전 MQTT readiness 거절: [§11.5](specs/cluster-session-and-delivery.md#115-dynamic-membershiprestart-convergencemqtt-readiness-mapping).

### 2.2 Storage 장애와 local 복구

- [x] Docker/FUSE storage 장애·power-cut simulation(disk full, quorum loss, slow I/O, fsync 오류, follower torn WAL, 세 node 동시 cut): [Failure recovery §7](specs/cluster-session-and-delivery.md#7-failure-recovery), [§11.6](specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping).
- [x] Local WAL compaction·snapshot retention·torn-tail 판정: [ADR-0023](decisions/ADR-0023-raft-wal-compaction-and-snapshot-retention.md), [§11.22](specs/cluster-session-and-delivery.md#1122-wal-compactionsnapshot-retentiontorn-tail-수용-매핑).
- [x] Bootstrap·joiner voter의 빈 volume 보호와 새 ID 재시드: [ADR-0022](decisions/ADR-0022-bootstrap-volume-loss-guard.md), [ADR-0024](decisions/ADR-0024-joiner-voter-volume-loss-admission.md), [§11.20](specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑).
- [x] Local ext4 ENOSPC, quorum loss 중 ACK 거절, 전체 broker SIGKILL과 기존 recovery 시험: [§11.23](specs/cluster-session-and-delivery.md#1123-local-ext4-filesystem-수용-매핑). OS cache가 유지되는 process crash 증거다.

### 2.3 Replicated state와 backup

- [x] Backup v1 commit 경계·offline restore·snapshot finalize crash: [ADR-0011](decisions/ADR-0011-cluster-backup-restore-v1.md), [§11.7](specs/cluster-session-and-delivery.md#117-cluster-backuprestore와-snapshot-finalize-수용-매핑). v1 archive는 v2 전환 뒤 읽지 않는다.
- [x] Tenant-scoped retained 복제와 backup v2: [ADR-0012](decisions/ADR-0012-retained-replication-and-cluster-backup-v2.md), [§11.8](specs/cluster-session-and-delivery.md#118-retained-replication과-backup-v2-수용-매핑).
- [x] Detached persistent session의 QoS 0 offline 복제와 backup v3: [ADR-0013](decisions/ADR-0013-offline-queue-replication-and-cluster-backup-v3.md), [§11.9](specs/cluster-session-and-delivery.md#119-offline-queue-replication과-backup-v3-수용-매핑).
- [x] Archive N/N-1 restore(같은 source의 synthetic build): [ADR-0021](decisions/ADR-0021-backup-archive-release-window.md), [§11.19](specs/cluster-session-and-delivery.md#1119-backup-archive-nn-1-restore-수용-매핑).
- [x] Tenant storage quota(PRD TEN-08 범위): [ADR-0017](decisions/ADR-0017-tenant-storage-quota.md), [§11.16](specs/cluster-session-and-delivery.md#1116-tenant-storage-quota-수용-매핑).

### 2.4 MQTT session과 delivery

- [x] MQTT CONNECT ownership 획득과 takeover: [ADR-0004 §8](decisions/ADR-0004-session-ownership-and-fencing.md#8-검증), [§11.12](specs/cluster-session-and-delivery.md#1112-mqtt-connect-ownership-acquisition과-takeover-수용-매핑).
- [x] Persistent subscription·inflight replay와 route projection rebuild: [§11.14](specs/cluster-session-and-delivery.md#1114-persistent-subscriptioninflight-replay와-route-projection-rebuild-수용-매핑).
- [x] Cross-node delivery matrix, 동시 CONNECT, stale-epoch fencing: [§11.15](specs/cluster-session-and-delivery.md#1115-cross-node-delivery-matrix동시-connectstale-epoch-수용-매핑).
- [x] Durable QoS 1/2 delivery, 서버 생성 QoS 2 Will, target 없는 durable PUBLISH의 quorum barrier: [ADR-0015](decisions/ADR-0015-cluster-durable-qos-delivery.md), [ADR-0019](decisions/ADR-0019-untargeted-durable-publish-quorum-barrier.md), [§11.11](specs/cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑).
- [x] Session metadata 전체 복제(CLU-08): [ADR-0025](decisions/ADR-0025-replicated-session-metadata-and-owner-loss-recovery.md), [§4.3](specs/cluster-session-and-delivery.md#43-session-metadata-복제와-owner-손실-복구), [§11.24](specs/cluster-session-and-delivery.md#1124-session-metadata-복제와-owner-손실-복구-수용-매핑). 수동 feature 활성화 뒤 protocol·retention·expiry·Will·principal과 ownership epoch를 복제하며 committed cluster 조회와 owner 재시작·handoff 복구를 제공한다. 영구 owner host 손실의 자동 회수는 지원하지 않는다.
- [x] Owner crash 뒤 DETACH 없는 session의 expiry·capacity 회수: [ADR-0025](decisions/ADR-0025-replicated-session-metadata-and-owner-loss-recovery.md), [§11.24](specs/cluster-session-and-delivery.md#1124-session-metadata-복제와-owner-손실-복구-수용-매핑). Owner가 다시 READY가 되면 finite expiry를 시작하고 Will claim·만료 회수와 durable delivery를 보존한다. Planned handoff는 [ADR-0028](decisions/ADR-0028-planned-handoff-will-and-reason-metric.md)의 표준 Will 규칙을 적용한다. Quorum이 없으면 Will을 발행하지 않는다.
- [x] Detach 전환 중 owner-node local QoS 0 publish 보존: [ADR-0026](decisions/ADR-0026-detach-transition-local-qos0-gate.md), [§11.25](specs/cluster-session-and-delivery.md#1125-detach-전환-local-qos-0-보존-수용-매핑).
- [x] Broker 쪽 drain과 `SAFE_TO_STOP`: [ADR-0016](decisions/ADR-0016-node-drain-and-safe-to-stop.md), [Drain spec §10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑). External rollout은 §1.2에서 추적한다.
- [x] 계획 drain·planned removal의 표준 Will 처리와 사유별 metric(DEP-07·DEP-12): [ADR-0028](decisions/ADR-0028-planned-handoff-will-and-reason-metric.md), [Drain §5·§10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑). `session-metadata` 활성 상태에서 후계 recovery와 recovery 전 재접속에 표준 Will 규칙을 적용한다. 고정 6개 reason을 집계하며 3-node 연속 rolling에서 `unexpected_supported_client=0`, 재접속 억제 합계 3을 확인했다. 비활성 cluster·N-1 후계의 기존 억제와 복원 timer의 `unplanned_failure` 분류는 결정한 한계다. External rollout·연속 100회 rolling·SDK gate는 §1.2에 남긴다.
- [x] Detach 전환 중 forward QoS 0과 DETACH commit 전 owner crash의 유실 경계: [ADR-0027](decisions/ADR-0027-detach-forward-and-owner-crash-qos0-boundary.md), [§11.26](specs/cluster-session-and-delivery.md#1126-detach-전환-forward연결-중-owner-crash-유실-경계-수용-매핑). 보호는 DETACH commit부터이며 그 전의 local queue와 전환 중 forward는 at-most-once로 유실될 수 있다.

### 2.5 Durable ACK와 성능 smoke

- [x] ADR-0003의 local·process 안전성 범위: ACK 직후 replica kill·WAL/commit crash·partition([§11.13](specs/cluster-session-and-delivery.md#1113-adr-0003-durable-ack-fault-수용-매핑)), storage 장애 중 ACK([§11.6](specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)), 비동기 WAL group commit([ADR-0020](decisions/ADR-0020-asynchronous-raft-wal-group-commit.md), [§11.18](specs/cluster-session-and-delivery.md#1118-비동기-walapplication-완료-경계-수용-매핑)). ADR-0003 전체의 승인이나 production power-loss 수용은 아니며, production 경계는 §1.1에 남는다.
- [x] 고정 BP-03 local smoke compare와 독립 5회 suite: [성능 §3.4](specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)의 release-profile wake 보완 뒤 유효한 compare와 parent valid·5/5 suite 범위다. 이전 invalid 진단 run을 통과로 재해석하지 않는다. `releaseGating=false`이며 production 성능·storage/security 계약 등가·모든 향후 EOF 해소를 보증하지 않는다. Release 수치형 기준과 BP-07은 §1.2에서 추적한다.

### 2.6 Release 호환성

- [x] N-1↔N rolling upgrade·rollback(같은 source의 synthetic build): [ADR-0018](decisions/ADR-0018-release-compatibility-window.md), [Cluster §8](specs/cluster-session-and-delivery.md#8-mixed-version-compatibility), [§11.17](specs/cluster-session-and-delivery.md#1117-n-1n-rolling-upgraderollback-수용-매핑).
- [x] 지정 historical baseline `58a95f8`의 N-1↔N rolling·archive: [§11.21](specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑). N archive를 historical N-1에서 restore하면 pre-ADR-0021 제약 때문에 fail-closed로 거절한다.

항목을 마치면 해당 [수용 시험 매핑](specs/cluster-session-and-delivery.md#11-수용-시험)에 시험 위치와 실행 결과를 연결하고, 영향받은 결정·구조 문서의 범위와 상태를 갱신한 뒤 이 목록의 상태를 바꾼다. 실행하지 않은 시험이나 미승인 `Proposed` 결정을 완료로 표시하지 않는다.
