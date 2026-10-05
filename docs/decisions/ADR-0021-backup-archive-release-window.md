# ADR-0021: Backup archive v3의 N/N-1 restore release window

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (backup archive v3 restore release window only; historical-source wire·production storage 제외) |
| 결정일 | 2026-10-03 |
| 결정자 | NaruMQ 프로젝트 — 사용자 선택 “양방향 허용, active feature 있으면 거절” |
| 관련 요구사항 | [PRD CLU-11·CLU-14](../requirements/PRD.md#92-클러스터와-세션) |
| 관련 스펙·데이터 계약 | [Cluster §10·§11.19](../specs/cluster-session-and-delivery.md#1119-backup-archive-nn-1-restore-수용-매핑), [Cluster backup v3](../contracts/cluster-backup-v3.md), [Release compatibility v1](../contracts/cluster-release-compatibility-v1.md) |
| 관련 아키텍처 | [Project structure](../architectures/project-structure.md), [Cluster architecture](../architectures/cluster-architecture.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | [ADR-0013](ADR-0013-offline-queue-replication-and-cluster-backup-v3.md)의 v3 restore exact-release 규칙과 [ADR-0018](ADR-0018-release-compatibility-window.md)의 archive cross-release restore 제외만 대체한다. 나머지 결정은 유지한다. |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

Archive의 producer와 모든 voter의 release를 restore build와 같은 major·minor 차이 1 이하인 호환 창(release window)으로 검사한다. 기존 `release.CompatibleStrings` 정책을 재사용하며 양방향 N↔N-1과 patch 차이를 허용한다. Archive format `3.0`, cluster protocol `2.0`, state schema `1.0`은 exact 비교를 유지한다.

Commit 경계까지 복구한 state의 active feature(이미 활성화된 기능 표시)를 restore build가 모두 지원해야 한다. 하나라도 지원하지 않으면 target storage를 만들기 전에 거절한다. Archive의 committed member profile은 다시 쓰지 않는다. 정상 startup 뒤 기존 release-profile controller가 build profile을 `MemberProfileChange`로 commit하고 fresh read로 확인한 다음에만 READY가 된다.

## 2. 배경

기존 restore는 producer·voter release의 정확한 일치를 요구해 인접 release로 복원할 수 없었다. ADR-0018의 mixed-release window는 archive restore를 제외했다. 사용자 선택은 인접 release로 양방향 복원하되 미지원 active feature는 거절하는 것이다.

Release 번호와 schema 번호만으로 과거 binary의 decode 능력을 증명할 수 없다. 이 결정은 기존 엄격한 codec 경계를 넓히지 않는다. Manifest·entry의 unknown field와 비정규 encoding은 `records.DecodeBackupArchive`가 거절하고, typed snapshot은 `decodeMembershipSnapshot`, committed tail은 `replayClusterApplied`, commit-boundary summary는 `records.EqualBackupStateSummary`가 검증한다. 이해하지 못하는 state·command·summary는 storage 변경 전 오류로 종료하며 field를 무시하거나 변환하지 않는다. 모든 기능이 inactive여도 decode 실패를 허용으로 바꾸지 않는다.

## 3. 결정 요인

1. 인접 release로 rollback·upgrade restore를 허용한다.
2. 복원 후 미지원 feature 때문에 영구적으로 READY가 되지 못하는 상태를 미리 거절한다.
3. 기존 codec·identity·summary와 storage 경계를 유지하고 실제 검증 범위를 명시한다.

## 4. 선택 근거

기존 release 정책과 feature set 조회를 재사용하면 mixed operation과 restore의 release 판정이 일치한다. State를 boundary까지 replay한 뒤 active feature를 검사하므로 snapshot 이후 활성화도 판정에 포함된다. `BeginRestore`보다 앞에서 검증하므로 거절 시 새 target directory도 만들지 않는다.

Same-source synthetic build는 restore 방향, release profile commit, retained·offline 데이터와 target 불변을 확인할 수 있다. 서로 다른 historical source의 wire/state 호환성을 증명하지 않으므로 이를 별도 gate로 남긴다.

## 5. 대안

| 대안 | Rollback 가능성 | READY 불가 위험 | 검증 범위·판단 |
| --- | --- | --- | --- |
| 양방향 window + active feature 거절 (선택) | 인접 release 양방향 허용 | 미지원 feature를 storage 변경 전에 거절 | 기존 strict codec 유지; synthetic mechanics만 검증 |
| 업그레이드 방향만 허용 | 이전 release 복원 불가 | 미지원 feature 검사도 필요 | 승인된 양방향 요구를 충족하지 못함 |
| 양방향 무조건 허용 | 인접 release 양방향 허용 | 미지원 feature가 복원 후 READY를 막음 | 안전하지 않아 제외 |
| Exact release 유지 | 동일 release로만 복원 | 기존 feature 거절 공백이 남음 | CLU-14 인접 release 요구를 충족하지 못함 |

## 6. 결과

### 긍정적 결과

- Archive restore와 runtime이 같은 release window를 따른다.
- 미지원 active feature를 target 변경 전에 거절한다.
- Format·schema·identity·summary 검증과 committed profile을 보존한다.

### 부정적 결과와 비용

- 복원 뒤 build profile commit 전에는 READY가 지연된다.
- 지원 release window 안이어도 알 수 없는 typed data는 거절한다.
- Same-source build는 실제 과거 release artifact의 호환성 증거가 아니다.

### 후속 작업

- ADR-0013의 N-1 archive 정책 후속은 이 결정으로 대체한다. 실행 증거와 완료 범위는 [Cluster §11.19](../specs/cluster-session-and-delivery.md#1119-backup-archive-nn-1-restore-수용-매핑)와 [PRD 구현 잔여 작업](../TODO.md)에 기록한다.
- Historical-source compatibility, production Linux/EBS, `SPEC-TBD-09` retention 및 RPO/RTO는 별도로 검증한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Producer window와 profile 보존 | `TestClusterRestoreSeedAppliesAdjacentReleaseWindow` | 양방향·patch 허용, N-2·major 거절, 원래 profile 보존 | [Cluster §11.19](../specs/cluster-session-and-delivery.md#1119-backup-archive-nn-1-restore-수용-매핑) |
| Active feature와 voter window | `TestClusterRestoreSeedRejectsUnsupportedActiveFeature`, `TestClusterRestoreMemberReleaseWindow` | 미지원 feature와 window 밖 voter 거절 | 같은 수용 매핑 |
| 실제 binary restore와 target 불변 | `TestClusterBackupRestoreReleaseWindowProcessAcceptance` | upgrade·downgrade 후 retained·offline 복구, feature·N-2 거절 시 target 미생성 | 같은 수용 매핑; same-source synthetic builds |

## 8. 재검토 조건

- Protocol/schema 또는 typed codec가 바뀌어 인접 release의 decode/apply 의미가 달라질 때.
- Historical release artifact가 확보되어 실제 source 간 matrix를 실행할 수 있을 때.
- Feature activation의 rollback 정책이나 지원 release window가 달라질 때.
