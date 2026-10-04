# ADR-0032: Feature activation의 운영 승인과 rollback window 종료

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-04 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD CLU-11·CLU-14](../requirements/PRD.md#92-클러스터와-세션) |
| 관련 스펙·데이터 계약 | [Cluster §8·§11.30](../specs/cluster-session-and-delivery.md#8-mixed-version-compatibility), [Release compatibility v1 §4.1](../contracts/cluster-release-compatibility-v1.md#41-운영-승인과-rollback-window-종료), [Cluster backup v3](../contracts/cluster-backup-v3.md) |
| 관련 아키텍처 | [Project structure](../architectures/project-structure.md), [Cluster architecture](../architectures/cluster-architecture.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음. [ADR-0018](ADR-0018-release-compatibility-window.md)과 [ADR-0031](ADR-0031-replicated-state-schema-strict-evolution.md)의 기술 경계를 유지하고 운영 승인 책임을 구체화한다. |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

해당 배포의 release 담당자가 운영 체크리스트와 외부 승인 기록으로 feature token 한 개의 activation을 승인한다. 기존 `ClusterAdmin` 운영자가 body 없는 feature POST를 실행한다. 같은 사람이 승인과 실행을 맡을 수 있다. 새 이중 승인 제도를 요구하지 않는다.

정책의 단일 기준은 [Release contract §4.1](../contracts/cluster-release-compatibility-v1.md#41-운영-승인과-rollback-window-종료)이다. 실행 방법은 [Feature activation 절차](../how-to/cluster-feature-activation.md)를 따른다. 승인 자체가 아니라 해당 `FeatureActivation`의 commit이 그 feature의 rollback window를 끝낸다. 결과 불명 요청 뒤에는 미지원 build로 rollback하지 않는다.

API·권한·replicated schema와 Go 구현은 바꾸지 않는다. Runtime은 사람의 승인 기록을 검사하지 않는다. 별도 승인 token·승인 API·feature deactivation을 추가하지 않는다.

## 2. 배경

모든 non-retired member의 feature 광고와 미지원 build의 readiness 차단은 구현되어 있다. 그러나 운영자가 언제 무손실 in-place rollback을 포기하는지는 열려 있었다. Commit 전 승인 책임과 복구 경로를 기록해야 한다. Commit 뒤 상태 조회가 실패할 수 있으므로 POST 오류만으로 미활성화를 판단해서는 안 된다.

## 3. 결정 요인

- 기존 commit·readiness와 strict decode 경계를 유지한다.
- 승인 배포와 실제 member/build 목록을 대조한다.
- 응답 유실을 미commit으로 오인한 downgrade를 막는다.
- 유효한 backup과 지원되는 동일 identity restore 경로를 먼저 확인한다.
- Production storage·보존 기간·RPO/RTO와 게시 release별 검증을 local 증거로 대신하지 않는다.

## 4. 선택 근거

체크리스트는 기존 조회 API와 배포 기록을 함께 사용한다. 별도 runtime state 없이 승인자와 실행자의 책임을 기록할 수 있다. POST 직전 재조회가 승인 대상의 변화를 잡지만 조회와 commit을 원자적으로 만들지는 않는다. Apply의 전체 non-retired member 광고 검증은 그대로 유지한다.

고정 24/48시간 안정화 기간이나 미합의 SLO를 새로 정하지 않는다. 배포별 시험 증거와 진행 중인 결함으로 판단한다. Synthetic·지정 historical baseline은 미래 게시 release의 matrix를 대신하지 않는다.

## 5. 대안

| 대안 | 제외 이유 |
| --- | --- |
| 별도 승인 token·API와 replicated 승인 state | 현재 요청보다 넓은 schema·권한·운영 체계를 요구한다. |
| 전체 member 지원 시 자동 activation | 운영자의 무손실 rollback 포기 판단과 복구 증거 확인을 생략한다. |
| 고정 안정화 시간 뒤 승인 | 시간이 경과했다는 사실만으로 배포 결함이나 복구 경로를 확인할 수 없다. |
| Feature deactivation으로 downgrade | 이미 기록된 새 형식을 되돌리지 못하며 기존 strict decode 경계를 깨뜨린다. |

## 6. 결과

- 승인 대상 cluster와 feature, 확인한 배포 집합, backup·시험 증거와 무손실 rollback 포기를 외부 기록에 남긴다.
- Runtime은 절차 준수를 자동으로 보장하지 않는다. 체크리스트 누락은 기존 `ClusterAdmin` 권한만으로 막을 수 없다.
- Activation 뒤에는 지원 build로 전진 수정하거나 같은 volume에 지원 build를 다시 배포한다.
- Activation 전 backup restore는 이후 commit 손실이 가능한 disaster recovery다. 별도 장애 복구 승인이 필요하다.
- Rollout controller, 실제 게시 release별 matrix와 production storage·retention/RPO/RTO gate는 열린 상태로 유지한다.

## 7. 검증

수동 정책 판정, 기존 application·HTTP·OpenAPI 시험, Windows local 실제 cluster 명령 smoke와 synthetic·historical release/backup 시험을 [Cluster §11.30](../specs/cluster-session-and-delivery.md#1130-feature-activation-운영-승인-수용-매핑)에 구분해 기록한다. 외부 승인 기록을 runtime이 강제하는 시험은 만들지 않는다.

## 8. 재검토 조건

- 승인 증거를 runtime이 검증하거나 이중 승인을 강제해야 한다.
- 지원 복구 identity나 major migration 경로를 넓힌다.
- 운영 storage·retention/RPO/RTO 또는 게시 release의 검증 기준이 확정된다.
- Commit 상태 확인용 API field를 추가해야 한다.
