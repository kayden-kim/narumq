# Cluster release compatibility v1 Contract

이 문서는 release N과 직전 지원 release N-1 node가 rolling upgrade·rollback 중 지켜야 할 cluster 호환성 범위를 정의한다. 승인된 [ADR-0018](../decisions/ADR-0018-release-compatibility-window.md)에 따라 release는 같은 major의 인접 minor끼리만 혼합 운영한다. Same-source synthetic 시험과 구분해 `58a95f8` historical baseline으로 [Cluster §11.21](../specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑)에서 별도 검증한다.
이 Draft contract는 Cluster RPC, replicated state와 snapshot의 release 간 동작을 다루며 acceptance matrix가 통과되기 전까지 검증된 호환성으로 표시하지 않는다. 이는 fixed-voter wire만 정의하는 Active [Raft peer transport v1](raft-peer-transport-v1.md)과 별도 경계다.

| 항목 | 값 |
| --- | --- |
| 이름과 version | `narumq-cluster-release-compatibility` 1.0.0 |
| 상태 | Draft |
| Producer | Release N·N-1 broker node와 rollout controller |
| Consumer | Release N·N-1 broker node와 rollout controller |
| 방향 | Cluster RPC, replicated log, snapshot, management·rollout status |
| 기준 artifact | 이 문서의 compatibility rule과 release matrix |
| 기준 spec | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md), [Drain과 rolling update](../specs/drain-and-rolling-update.md) |
| 관련 Architecture | [Cluster architecture](../architectures/cluster-architecture.md), [EKS on EC2 deployment](../architectures/eks-ec2-deployment.md) |
| 관련 requirement | CLU-11, NFR-15, DEP-18 |
| 관련 결정 | [ADR-0002](../decisions/ADR-0002-cluster-consensus-library.md), [ADR-0006](../decisions/ADR-0006-external-rollout-controller.md), [ADR-0009](../decisions/ADR-0009-raft-production-state-profile.md), [ADR-0018](../decisions/ADR-0018-release-compatibility-window.md), [ADR-0031](../decisions/ADR-0031-replicated-state-schema-strict-evolution.md), [ADR-0032](../decisions/ADR-0032-feature-activation-approval-policy.md) |
| 검증 | `tests/integration/cluster_release_compat_process_test.go`; [Cluster spec §11.17](../specs/cluster-session-and-delivery.md#1117-n-1n-rolling-upgraderollback-수용-매핑) process acceptance PASS (105.55s; package 105.695s); release-profile controller transitions PASS, including compatible snapshot invalidation and unsupported restored-feature latching; repository-wide instrumented `go test ./...` PASS (40 packages, 6 without tests; 288.23s); `GOMAXPROCS=4 go run ./tools/verify local` PASS (292.39s). |

## 1. 지원 범위

Release N은 N-1과 다음 조합을 지원해야 한다.

- N-1 node가 leader이고 N node가 follower인 mixed cluster
- N node가 leader이고 N-1 node가 follower인 mixed cluster
- N-1에서 N으로 한 node씩 upgrade
- N에서 N-1로 한 node씩 rollback
- Mixed cluster 중 leader failure와 재선출
- Mixed cluster 중 drain, session takeover와 snapshot 전송

N-2 이하와의 mixed cluster는 지원하지 않는다. Rollout preflight는 지원 범위를 벗어난 version 조합을 Pod 교체 전에 거절해야 한다.

## 2. Compatibility 축

| 축 | 같은 major에서 지켜야 할 규칙 |
| --- | --- |
| Cluster RPC | N과 N-1이 서로 필수 message를 decode하고 모르는 optional field를 무시한다. |
| Replicated command | N은 N-1이 이해하지 못하는 command·field를 feature activation 전에 propose하지 않는다. |
| Persisted record | Activation 전 N은 baseline schema에 없는 field·record를 쓰지 않는다. Activation 뒤 해당 feature의 새 기록은 N-1이 읽지 못하며 그 feature의 rollback window가 끝난다. |
| Snapshot | Sender·receiver 양방향으로 schema와 checksum을 검증한다. 모르는 field·collection은 거절한다. |
| Backup archive | Producer·voter release가 restore build와 window 안이고 restore build가 모든 active feature를 지원할 때만 restore한다. Protocol/schema exact·strict codec 검증은 유지한다 ([ADR-0021](../decisions/ADR-0021-backup-archive-release-window.md)). |
| Membership | Node ID와 member generation의 의미를 바꾸지 않는다. |
| Management API | Rollout controller가 사용하는 v1 field와 error code를 유지한다. |
| Metric·health | Readiness와 drain state 의미를 release 사이에서 바꾸지 않는다. |

## 3. Version handshake

Authenticated peer hello와 seed admission은 canonical `releaseVersion`을 교환한다. Feature set은 hello field가 아니며 committed member profile에서 전파된다.

| Field | Type | 의미 |
| --- | --- | --- |
| `releaseVersion` | `MAJOR.MINOR.PATCH` string | Build release identity. 각 구성 요소는 선행 0 없는 `uint32` decimal이며 `v` 접두어, pre-release·build suffix는 허용하지 않는다. |
| `clusterProtocolMajor` | unsigned integer | Breaking wire compatibility generation |
| `clusterProtocolMinor` | unsigned integer | Backward-compatible wire capability level |
| `stateSchemaMajor` | unsigned integer | Breaking persisted-state generation |
| `stateSchemaMinor` | unsigned integer | Backward-compatible state capability level |

두 release는 major가 같고 minor 차이가 0 또는 1이면 호환된다. Patch 차이는 무시하며 규칙은 대칭이다. Major가 다르거나 minor 차이가 2 이상이면 peer admission과 dynamic peer reconciliation을 거절한다. `minimumPeerRelease`를 wire 또는 config에 추가하지 않는다. Cluster protocol은 정확히 `2.0`, state schema는 정확히 `1.0`으로 비교한다.

Release version과 feature set은 mutable committed member profile이다. Restart한 node는 build identity를 `MemberProfileChange`로 commit한 뒤 READY가 된다. Feature는 정렬된 opaque token set으로 기록되고, active feature를 지원하지 않는 node는 READY가 되지 않는다.

## 4. Feature activation

- 새 feature는 mixed cluster에서 기본 비활성화한다.
- `FeatureActivation`은 모든 non-retired member(Staged 포함)가 feature를 광고할 때만 persistent feature state에 commit한다. Active feature 재요청은 성공 no-op이며, 조건을 충족하지 못하면 non-retryable conflict `feature_not_supported_by_all_members`를 반환한다.
- Activation은 quorum에 commit된 cluster setting으로 기록한다.
- Feature activation이 그 feature의 rollback window를 끝낸다.
- Rollback window 안에는 irreversible migration을 실행해서는 안 된다.

### 4.1 운영 승인과 rollback window 종료

이 절은 [ADR-0032](../decisions/ADR-0032-feature-activation-approval-policy.md)에서 채택한 운영 정책의 단일 기준이다. 기존 API·권한·replicated schema와 commit/readiness 규칙은 유지한다. 승인 기록은 외부 ticket 또는 배포 기록이며 runtime이 검사하지 않는다. 실행 절차는 [Feature activation 가이드](../how-to/cluster-feature-activation.md)를 따른다.

1. 승인 책임자는 해당 배포의 release 담당자다. 실행자는 기존 `ClusterAdmin` 권한을 가진 운영자다. 같은 사람이 두 책임을 맡을 수 있다. 새 이중 승인 제도를 요구하지 않는다.
2. 승인 대상은 cluster ID와 feature token 한 개다. 여러 feature를 묶거나 새 feature를 자동 활성화하지 않는다. Token은 `^[a-z][a-z0-9-]{0,62}$` 형식이다. `compatibility-probe`는 시험용이며 운영 기능이 아니다.
3. 예정한 binary 교체·membership 변경·drain을 끝낸 뒤 승인해야 한다. 알려진 membership operation의 완료를 배포 기록과 기존 operation GET으로 확인해야 한다. Cluster summary는 `quorumAvailable=true`, `underReplicated=0`이어야 한다. 운용 member 각각의 실제 관리 endpoint에서 readiness 200을 확인해야 한다. 같은 endpoint의 local node 조회에서 `storagePressure=false`와 제공되는 local `replicationHealth=HEALTHY`를 확인해야 한다. 필요한 값이 missing·unknown이거나 조회가 실패하면 승인하지 않는다. 원격 node 행의 `HEALTHY`는 원격 disk health 증거가 아니다.
4. `GET /api/v1/cluster/features`가 반환한 모든 non-retired member ID·`releaseVersion`·`features`를 승인 배포 목록과 대조해야 한다. Staged member도 대상이다. 누락·예상 밖 member·미지원 feature·승인하지 않은 build가 있으면 중단한다. Release 집합은 기존 same-major 인접 minor window 안이어야 한다. 모든 member의 release나 patch가 반드시 같아야 한다는 조건은 요구하지 않는다.
5. Target/rollback build 조합의 release matrix와 해당 feature 기능 시험 증거를 확인해야 한다. 게시 release에는 해당 release의 matrix가 있어야 한다. Synthetic·지정 historical baseline 증거를 미래 게시 release의 증거로 쓰지 않는다. 고정 24/48시간 안정화 기간이나 미합의 SLO 수치를 만들지 않는다. 진행 중인 배포 결함이 있으면 승인하지 않는다.
6. 승인 전에 유효한 commit-boundary backup, 이를 읽는 지원 build, 동일 identity restore 구성과 복구 시험 증거를 확보해야 한다. 허용 범위는 [Backup v3 §2](cluster-backup-v3.md#2-producer와-consumer-동작)·[§4](cluster-backup-v3.md#4-호환성-규칙)다. Learner 포함이나 세 bootstrap voter identity 조건 위반 등으로 지원 복구 경로가 없으면 승인을 보류한다. Production storage·retention/RPO/RTO 보류를 이 체크리스트로 해제하지 않는다.
7. 승인 책임자는 “이 feature를 지원하지 않는 이전 build로의 무손실 in-place rollback을 포기한다”는 결정을 기록해야 한다. 승인 자체는 window를 닫지 않는다. 해당 `FeatureActivation`의 **commit 순간**이 종료 기준이다. 배포 완료·clock 경과·승인 시각으로 종료를 판정하지 않는다.
8. 승인은 확인한 member/release/activeFeatures 집합과 feature token에만 유효하다. POST 직전에 재조회해야 한다. 집합이나 배포 상태가 바뀌면 재확인·재승인해야 한다. 조회와 commit 사이의 경쟁은 기존 apply의 전체 member 광고 검증으로 처리한다. 사전 확인과 commit은 원자적이지 않다. Feature 조회도 Members와 ActiveFeatures의 두 조회이며 atomic snapshot이 아니다.
9. HTTP 200과 응답 `data.activeFeatures`의 해당 token, 또는 성공한 GET의 해당 token으로 commit을 확인한다. Feature API는 phase·Raft index·정확한 commit 시각을 반환하지 않는다. 승인 시각·요청 시각·최초 commit 관찰 시각을 UTC로 각각 기록해야 한다. 관찰 시각을 정확한 commit 시각으로 쓰지 않는다.
10. POST timeout·연결 종료·500·503·검증할 수 없는 성공 응답은 **commit 결과 불명**이다. `ActivateFeature`는 commit 뒤 상태를 다시 조회하므로 오류 응답도 미commit 증명이 아니다. GET에서 token이 없다는 한 번의 관찰도 pending proposal 부재를 보증하지 않는다. 불명 상태에서는 rollback하지 않는다. Quorum 복구 뒤 기존 승인이 여전히 유효한지 확인하고 같은 token의 idempotent POST를 재실행한다. 승인 대상이 바뀌었다면 재확인·재승인하되 불명 요청을 실패로 처리하지 않는다. 불명 상태에서 새 target/feature는 승인하지 않는다. 200+token 또는 성공 GET+token이면 종료를 확인한다. 확인할 수 없으면 담당자에게 이관한다.
11. 처음 요청의 `409 feature_not_supported_by_all_members`는 이번 요청의 미활성화다. 배포를 수정한 뒤 새 승인을 받아야 한다. 이전 결과 불명 요청이 있으면 현재 409만으로 이전 요청까지 실패했다고 판단하지 않는다. 400·401·403이면 입력·권한을 수정한다. `not_leader`이면 현재 leader endpoint를 확인한다.
12. Activation 뒤에는 feature를 지원하는 build로 전진 수정하거나 같은 volume에 지원 build를 다시 배포한다. Active feature·snapshot field·WAL·metadata를 지워 downgrade하지 않는다. Activation 전 backup 복원은 backup 이후 commit 손실을 수반할 수 있는 disaster recovery다. 무손실 rollback이 아니며 별도 장애 복구 승인이 필요하다.

## 5. Readiness와 persisted state

- 모든 node는 running build의 release/features와 committed member profile이 일치하기 전 READY가 되어서는 안 된다.
- 모든 non-retired member의 release는 동일 major이며 minor 범위 폭이 1 이하여야 한다. Retired member는 현재 release window 계산에서 제외한다.
- Feature token은 `^[a-z][a-z0-9-]{0,62}$` 형식이며 build당 최대 16개다. Committed feature set과 active feature set은 정렬되고 중복이 없어야 한다.
- 새 feature는 mixed cluster에서 inactive로 시작한다. `FeatureActivation`은 모든 non-retired member(Staged 포함)가 feature를 광고할 때만 commit할 수 있다. 그렇지 않으면 HTTP `409 feature_not_supported_by_all_members`를 반환한다.
- 활성화된 feature를 지원하지 않는 build는 `incompatible_state_schema: ...` readiness reason으로 READY를 거절한다. 해당 node가 제안하는 profile update도 `active_feature_missing` conflict로 거절한다.
- 모든 record는 schema major/minor와 record type을 식별할 수 있어야 한다. Consumer는 같은 major라도 모르는 field와 record type을 거절한다([Replicated-state v1 §3](replicated-state-v1.md#3-호환성-규칙)).
- 모르는 field·record type·schema를 만나면 apply·복구를 중단하고 오류에 `incompatible_state_schema`를 남기며 READY가 되지 않는다.
- Snapshot manifest는 producer release, schema version, last included index, object count와 checksum을 포함해야 한다.
- Snapshot restore 뒤 object count와 checksum이 맞지 않으면 apply하지 않는다.
- Snapshot restore와 committed local profile 변경은 release-profile gate를 즉시 닫는다. 이때 controller에 알림을 보내 다음 500ms 주기를 기다리지 않고 기존 `Members`·`ActiveFeatures` fresh read barrier로 재확인을 시작한다. 알림은 대기 중 최대 한 개로 합치며, 적용 전 revision의 조회 결과는 gate를 다시 열 수 없다. 재확인 오류·profile 불일치·지원하지 않는 active feature에서는 계속 닫힌다. PUBLISH를 별도로 재시도하거나 admission 대기를 늘리지 않는다.

## 6. Release matrix

아래 synthetic matrix는 동일 source를 서로 다른 `-ldflags` build identity로 build한다. 이는 gate·activation·rollback mechanics의 증거이며 실행 결과는 Cluster spec §11.17에 기록한다. `58a95f8` historical baseline으로 §11.21에서 별도 검증한다.

| Current | Peer | Upgrade | Rollback | Leader 양방향 | Snapshot 양방향 | Test | 실행 증거 | 상태 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| N | N | 필수 | 해당 없음 | 필수 | 필수 | `TestDynamicClusterReleaseCompatibilityProcessAcceptance`, scenario 6 | [Cluster spec §11.17](../specs/cluster-session-and-delivery.md#1117-n-1n-rolling-upgraderollback-수용-매핑), two strict same-release snapshot transfers (105.695s) | PASS (synthetic builds) |
| N | N-1 | 필수 | 필수 | 필수 | 필수 | `TestDynamicClusterReleaseCompatibilityProcessAcceptance`, scenarios 2–5 | [Cluster spec §11.17](../specs/cluster-session-and-delivery.md#1117-n-1n-rolling-upgraderollback-수용-매핑), strict installed snapshots in both release directions (105.695s) | PASS (synthetic builds) |
| N | N-2 | 비지원 | 비지원 | 비지원 | 비지원 | 같은 process acceptance의 N-2 rejection scenario | [Cluster spec §11.17](../specs/cluster-session-and-delivery.md#1117-n-1n-rolling-upgraderollback-수용-매핑), same run (105.695s) | PASS (명시적 거절) |

모든 synthetic matrix 행이 PASS했다. 서로 다른 source의 결과는 아래 historical matrix와 [Cluster §11.21](../specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑)에서 구분한다. 지정 baseline은 게시된 release가 아니며 이후 게시 release의 호환성을 대신 보증하지 않는다.

| Current·Peer | Source | 검증 범위 | 시험 위치 | 실행 증거·상태 |
| --- | --- | --- | --- | --- |
| N·N-1 | N=`HEAD`, N-1=`58a95f8`@`1.1.0` | Rolling upgrade·rollback, leader·snapshot 양방향, drain, feature activation 차단과 미지원 rollback 거절 | `TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance` | [Cluster §11.21](../specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑): PASS (91.38s; historical 두 시험 wall 110.81s) |
| N·N-2 | N=`HEAD`, N-2=`58a95f8`@`1.0.0` | 비지원 release 명시적 거절 | 같은 historical process acceptance | 같은 §11.21 실행: PASS (10회 readiness 503, committed N profile 유지) |

## 7. Breaking change

다음 변경은 cluster protocol 또는 state schema major 변경 후보이다.

- 기존 required field 삭제·type·의미 변경
- 기존 node가 해석할 수 없는 required command 추가
- Snapshot checksum 또는 framing의 비호환 변경
- Epoch, commit index, node ID와 membership 의미 변경
- N-1 rollback 경로를 제거하는 persisted-state migration

Major 변경은 in-place mixed upgrade로 처리하지 않는다. 별도 migration plan, backup·restore 검증과 downtime 또는 bridge release가 필요하다.

## 8. Blocker와 제외 범위

- 해결: 운영 체크리스트·외부 승인 기록과 commit 기준의 rollback window 종료 정책을 [§4.1](#41-운영-승인과-rollback-window-종료)에서 확정했다. API가 승인 evidence를 강제하지 않는다. 수용 범위는 [Cluster §11.30](../specs/cluster-session-and-delivery.md#1130-feature-activation-운영-승인-수용-매핑)에서 관리한다.
- 남음: [ADR-0006](../decisions/ADR-0006-external-rollout-controller.md)의 rollout controller preflight. 이 ADR-0018 범위는 Pod 교체 전 preflight 구현을 포함하지 않는다.
- 해결 범위: [ADR-0021](../decisions/ADR-0021-backup-archive-release-window.md)의 backup archive N/N-1 restore 정책. Synthetic 증거는 [Cluster §11.19](../specs/cluster-session-and-delivery.md#1119-backup-archive-nn-1-restore-수용-매핑), historical baseline의 upgrade restore와 pre-ADR-0021 consumer 거절 제약은 [§11.21](../specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑)에서 관리한다.
- `58a95f8` baseline은 §11.21에서 검증했다. 이후 실제 게시 release 사이의 wire/state matrix는 release마다 추가한다.
- 해결: same-source synthetic 및 지정 historical baseline의 rolling·양방향 installed snapshot·N-2 거절 수용이 PASS했다. 실제 게시 release별 검증은 별도다.
