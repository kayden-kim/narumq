# ADR-0031: Replicated-state schema의 strict decode와 feature gate

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-04 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD CLU-11·CLU-14](../requirements/PRD.md) |
| 관련 스펙·데이터 계약 | [Cluster §8·§11.29](../specs/cluster-session-and-delivery.md#8-mixed-version-compatibility), [Replicated-state v1](../contracts/replicated-state-v1.md), [Release compatibility v1](../contracts/cluster-release-compatibility-v1.md) |
| 관련 아키텍처 | [Project structure](../architectures/project-structure.md), [Cluster architecture](../architectures/cluster-architecture.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | [ADR-0009 §1.3](ADR-0009-raft-production-state-profile.md#13-replicated-application-command와-snapshot-state-encoding)의 미완료 계약 항목을 확정한다. |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

1. Command·snapshot·backup archive는 strict decode를 따른다. 이는 모르는 형식을 해석하지 않고 거절하는 규칙이다. Root와 하위 message의 unknown field, 모르는 command payload·snapshot collection, `1.0`이 아닌 schema version과 정의되지 않은 enum 값을 거절한다. Unknown field를 보존하거나 무시하지 않는다.
2. Schema 거절 오류는 `incompatible_state_schema`로 분류한다. 기존 fail-closed 경로는 바꾸지 않는다. Committed apply에서는 Raft driver가 중단되고 process가 끝난다. Startup recovery에서는 시작이 실패한다. 값 범위·정렬·count·digest 등 손상과 비정규 bytes 오류는 이 분류에 넣지 않는다. Enum validator의 기존 오류도 유지한다.
3. Schema 번호는 `1.0`으로 고정한다. 추가 변경마다 minor를 올리지 않는다. Major 변경은 [Release contract §7](../contracts/cluster-release-compatibility-v1.md#7-breaking-change)의 breaking change다.
4. 새 field 번호는 해당 message의 기존 최대 번호보다 커야 한다. 번호·enum 값·reserved 이름은 재사용하지 않는다. 기존 field의 번호·type·cardinality(단일 값인지 목록인지)·의미를 바꾸지 않는다. Proto3 기본값은 기존 동작을 뜻해야 한다. 새 field·payload·collection·enum 값은 해당 feature token의 `FeatureActivation`이 commit된 뒤에만 propose하거나 저장한다. Activation 전 bytes는 baseline schema로 unknown 없이 decode되어야 한다.
5. Feature activation이 그 feature의 rollback window를 끝낸다. Activation 뒤 해당 feature를 지원하지 않는 N-1은 READY가 되지 않는다. 새 field를 만나면 fail-closed다. 운영자의 window 종료 판정과 activation 승인 정책은 [tasks §1.2](../tasks.md#12-release와-rollout-gate)에 남긴다.
6. 기존 membership contract의 크기 제한을 [Replicated-state v1 §1](../contracts/replicated-state-v1.md#1-형식과-의미)로 옮겨 공식 한도로 채택한다. `CommandEnvelope` 1 MiB는 Raft의 `maxDynamicCommandSize`와 같다. 한도 변경에는 contract 변경과 vector 추가가 필요하다.
7. 기준 bytes와 baseline descriptor는 `internal/adapters/raft/records/testdata/replicated-state-v1/`에 둔다. 이번 baseline은 현재 schema descriptor다. 실제 release를 게시할 때만 해당 release의 descriptor로 교체한다. 게시 release별 process matrix는 tasks §1.2의 별도 작업이다.

## 2. 배경

Release contract는 같은 major의 모르는 optional field를 보존하거나 무시하도록 요구했다. 현재 codec은 모든 unknown field를 재귀적으로 거절한다. 두 기준을 strict decode와 feature gate로 통일한다. [ADR-0012 §1](ADR-0012-retained-replication-and-cluster-backup-v2.md#1-결정) 5항의 strict 규칙과 같은 방향이다.

## 3. 결정 요인

- 조용한 데이터 유실과 replica별 해석 차이를 막는다.
- 기존 apply·recovery 중단 경로와 exact schema admission을 유지한다.
- Activation 전 rolling upgrade와 rollback을 지원한다.
- Gate 없이 새 형식을 쓰는 실수를 release 전에 잡는다.

## 4. 선택 근거

새 build는 feature activation 전에는 이전 build가 모르는 bytes를 쓰지 않는다. 모든 non-retired member가 feature를 광고할 때만 activation을 commit한다. 이미 검증한 [§11.17](../specs/cluster-session-and-delivery.md#1117-n-1n-rolling-upgraderollback-수용-매핑)·[§11.21](../specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑)의 경계를 유지한다.

## 5. 대안

| 대안 | 제외 이유 |
| --- | --- |
| Snapshot record의 unknown 보존 | Command가 strict여서 이득이 작다. 삭제 뒤 같은 값으로 다시 만든 record에서 오래된 bytes가 되살아날 위험이 있다. |
| Unknown 무시 | 조용한 데이터 유실과 replica 불일치가 생긴다. |
| Apply 정지와 voter 유지 | Raft driver와 leader 처리를 새로 설계해야 한다. |
| 추가마다 minor 증가 | Peer admission의 exact `1.0` 비교를 바꿔야 한다. |

## 6. 결과

- Rolling upgrade와 activation 전 rollback이 가능하다.
- 해당 feature를 지원하지 않는 N-1으로 activation 뒤 rollback은 불가능하다.
- Baseline guard는 시험 입력에서 gate 없이 쓰인 새 field를 잡는다. 입력으로 다루지 않은 경로까지 보증하지 않는다. 이를 놓치면 N-1이 fail-closed로 종료한다.
- 기존 field의 의미 보존과 기본값의 의미는 descriptor 비교만으로 확인할 수 없다. 변경 시 계약과 동작 시험을 함께 검토해야 한다.

## 7. 검증

Vector producer/consumer, 거절 분류, baseline 보존, inactive-feature producer guard와 release 회귀의 시험 위치·실행 증거는 [Cluster §11.29](../specs/cluster-session-and-delivery.md#1129-replicated-state-v1-계약-수용-매핑)에 기록한다.

## 8. 재검토 조건

- Major migration이나 feature 활성화 뒤 이전 build로의 복구 지원이 필요해진다.
- 공식 한도 변경이나 새 형식의 feature gate를 검토한다.
- 실제 게시 release의 matrix가 baseline guard와 다른 호환성 결과를 보인다.
