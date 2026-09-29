# ADR-0018: Release compatibility window and committed build profiles

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (N/N-1 mixed-release window scope; rollout preflight·archive cross-release restore·historical-source wire 제외) |
| 결정일 | 2026-09-28 |
| 결정자 | NaruMQ project |
| 관련 요구사항 | [CLU-11, NFR-15](../requirements/PRD.md) |
| 관련 스펙·데이터 계약 | [Cluster session과 durable delivery §8·§11](../specs/cluster-session-and-delivery.md), [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md), [Raft membership v2](../contracts/raft-membership-v2.md), [Management API v1](../contracts/management-api-v1.md) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md), [Project structure](../architectures/project-structure.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

Dynamic cluster는 같은 major의 인접 minor release만 혼합 운영한다. 각 node의 release와 feature set은 build identity에서 얻고, 재시작한 node는 이를 replicated member profile로 commit한 뒤 READY가 된다. Opaque feature는 모든 non-retired member가 광고할 때만 cluster state에 활성화할 수 있다. 활성 feature를 지원하지 않는 build와 호환 창 밖 release는 READY 또는 peer admission을 허용하지 않는다.

이 결정은 N-1↔N의 upgrade·rollback 및 wire·persisted-state·snapshot 동작을 다룬다. Rollout controller preflight, backup archive의 cross-release restore 및 실제 과거 source revision 사이의 wire 차이는 범위 밖이다.

## 2. 배경

현재 membership과 peer 검증은 release version을 immutable identity와 함께 exact 비교한다. Release N과 N-1이 안전하게 같은 cluster에서 동작할 수 있는 규칙, replicated profile update와 feature activation 경계가 필요하다. 실제 historical release source가 없으므로 acceptance는 동일 source를 서로 다른 release identity로 세 번 build해 upgrade·gate·rollback mechanics를 관찰한다.

## 3. 결정 요인

1. Mixed cluster에서 기존 protocol/schema와 member identity 경계를 보존한다.
2. Rollback node가 이미 활성화한, 자신이 모르는 persisted capability를 조용히 읽거나 쓰지 않게 한다.
3. Release identity의 입력을 운영 config에서 분리하고 build artifact에 고정한다.
4. Acceptance가 증명하는 범위와 synthetic evidence의 한계를 명확히 한다.

## 4. 선택 근거

- Release는 canonical `MAJOR.MINOR.PATCH`이며 각 부분은 선행 0 없는 `uint32` decimal이다. `v` 접두어, pre-release 및 build suffix는 허용하지 않는다. Major가 같고 minor 차이가 0 또는 1이면 호환한다. Patch는 gate에 영향을 주지 않는다. 호환성은 대칭이다.
- `releaseVersion`과 최대 16개 정렬된 feature token은 build identity다. Cluster config JSON의 `releaseVersion`은 제거하며, 남아 있는 key는 strict decode에서 거절한다. Cluster protocol `2.0`과 state schema `1.0`은 exact하게 유지한다.
- Release와 features는 mutable member profile이다. `MemberProfileChange`가 profile을 replicated state에 기록하고, local READY는 build profile이 committed profile과 같아진 뒤에만 허용한다.
- `FeatureActivation`은 모든 non-retired member(Staged 포함)가 feature를 광고할 때만 commit된다. Active feature를 모르는 build는 READY가 되지 않고, profile update도 거절된다.
- Build feature는 opaque token이며 현 production behavior를 추가하지 않는다. `compatibility-probe`는 test acceptance에서 activation 및 rollback gate를 관찰하기 위한 synthetic feature다.
- Same-source build는 rolling replacement, version gate, committed-profile sync, snapshot transfer, durable session takeover 및 feature rollback block을 증명한다. 서로 다른 historical source revision 사이의 wire compatibility나 실제 release artifact 호환성을 증명하지 않는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 모든 release bytewise exact match | 단순한 gate | 인접 release rolling upgrade 불가 | CLU-11 N-1 목표를 충족하지 못한다. |
| 최소 peer version 필드 | 오래된 peer 기준을 명시할 수 있음 | 중복된 정책 입력과 비대칭 설정 위험 | 같은 major·minor window 규칙으로 충분하며 wire field 추가를 피한다. |
| Feature를 peer hello에서 협상 | 초기 연결에서 capability를 확인 | cluster 전체 committed state와 불일치할 수 있음 | Feature advertisement는 committed member profile로 전파하고 activation은 replicated command로 처리한다. |
| 실제 historical release source로 시험 | Source 간 차이를 직접 증명 | 필요한 release source가 현재 없다 | 사용자가 승인한 same-source synthetic build를 사용하되 증거 한계를 남긴다. |

## 6. 결과

### 긍정적 결과

- N-1↔N 인접 minor mixed operation과 양방향 profile update에 결정적 gate가 생긴다.
- Active feature는 cluster 전체가 지원하는 경우에만 committed state가 되며 rollback build는 READY로 오인되지 않는다.
- Protocol/schema compatibility와 release compatibility가 서로 다른 축으로 유지된다.

### 부정적 결과와 비용

- Release profile sync가 끝나기 전 node readiness와 MQTT admission이 지연된다.
- Active feature가 있는 cluster에서 이를 지원하지 않는 node는 수동으로 호환 build를 배포해야 한다.
- Synthetic build acceptance는 historical-source wire 차이를 검증하지 않는다.
- Rollout preflight와 cross-release archive restore는 별도 작업으로 남는다.

### 후속 작업

- [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md)과 [Raft membership v2](../contracts/raft-membership-v2.md)의 교환·호환 규칙을 갱신한다.
- [Cluster spec §11.17](../specs/cluster-session-and-delivery.md#1117-n-1n-rolling-upgraderollback-수용-매핑)에 process acceptance를 연결한다.
- Pass한 acceptance 뒤에만 [docs/tasks.md](../tasks.md)의 N-1↔N 항목을 닫는다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| 인접 minor 허용, N-2·major mismatch 거절 | Release domain 및 peer transport tests | `1.1.0↔1.2.0` 수락, `1.0.0↔1.2.0`과 다른 major 거절 | `internal/domain/release`, `internal/adapters/tcp` |
| Committed profile sync와 양방향 snapshot | Mixed-binary process acceptance | N-1 leader→N follower 및 N leader→N-1 follower snapshot 뒤 READY와 동일 retained state | `tests/integration.TestDynamicClusterReleaseCompatibilityProcessAcceptance` |
| Feature activation과 rollback block | Mixed-binary process acceptance | 미지원 member 존재 시 409, 전체 광고 후 activation 200, N-1 rollback node 503 | 같은 process acceptance |
| Synthetic evidence 범위 제한 | Test binary identity 구성 및 결과 문서 | 세 binary가 같은 source이며 limitation이 문서에 명시됨 | [Cluster spec §11.17](../specs/cluster-session-and-delivery.md#1117-n-1n-rolling-upgraderollback-수용-매핑) |

## 8. 재검토 조건

- 지원 release 체계가 SemVer의 major/minor 인접 window와 달라질 때.
- `clusterProtocol` 또는 `stateSchema`의 변경이 인접 release의 decode/apply 의미를 깨뜨릴 때.
- Historical release artifact가 확보되어 실제 source 간 acceptance를 추가할 수 있을 때.
- Rollout controller preflight 또는 cross-release archive restore의 승인된 범위가 정해질 때.
