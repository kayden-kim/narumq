# ADR-0053: GitHub Actions `verify`를 `main`의 필수 CI gate로 사용한다

| 항목 | 값 |
| --- | --- |
| 상태 | `Accepted` |
| 결정일 | 2026-10-11 |
| 결정자 | NaruMQ 프로젝트 사용자 |
| 관련 요구사항 | [PRD](../requirements/PRD.md)의 TEST-06~TEST-10, TEST-13~TEST-15 |
| 관련 스펙·계약 | [Performance와 verification](../specs/performance-and-verification.md), [개발 안내](../how-to/development-guide.md) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md) |
| 관련 ADR | [ADR-0042](ADR-0042-p0-requirement-traceability-gate.md), [ADR-0044](ADR-0044-diff-coverage-floor-90.md), [ADR-0063](ADR-0063-diff-coverage-floor-80.md), [ADR-0064](ADR-0064-ci-gate-logic-only.md)(필수 check는 논리 시험만 실행) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | [ADR-0056](ADR-0056-branch-protection-for-admins.md)가 §1의 관리자 보호 적용 행만 대체한다. 나머지 결정은 유지한다. |

## 1. 결정

GitHub Actions의 `.github/workflows/verify.yml`이 pull request와 `main` push에서 `go run ./tools/verify ci --base <base>`를 실행한다. `workflow_dispatch`로 수동 실행할 수도 있다.

`main` branch protection은 다음 상태를 요구한다.

| 설정 | 값 |
| --- | --- |
| 필수 status check | `verify` |
| 최신 base 반영 | 필수 (`strict=true`) |
| 필수 review | 없음 |
| 관리자에 대한 보호 적용 | 적용하지 않음 (`enforce_admins=false`) |
| Force push·branch 삭제 | 금지 |
| Push 제한 | 없음 |

Workflow는 `ubuntu-24.04` runner와 Node 24를 사용하는 `actions/checkout@v7`, `actions/setup-go@v7`, `actions/upload-artifact@v7`을 사용한다. Coverage profile과 summary, branch manifest를 30일 보관한다.

## 2. 배경

`go run ./tools/verify ci --base <merge-base>`는 로컬에서 CI gate를 실행하지만 GitHub pull request의 merge를 막지는 않는다. 변경 코드 coverage는 base commit이 필요하다. 따라서 PR base와 main push 이전 commit을 workflow가 명시적으로 고른다.

Mutation 검증은 CI에서 실행하지 않는다. 2026-10-11 수동 실행은 다섯 package를 검사하는 데 1464.90초가 걸렸다. 개발자는 필요할 때 `go run ./tools/verify mutation`으로 별도 실행한다.

## 3. 결정 요인

1. TEST-06~TEST-09의 coverage와 TEST-13~TEST-15의 fuzz·race·flaky 기준을 원격 PR gate에서 실행해야 한다.
2. Diff coverage는 실제 PR base와 비교해야 한다.
3. 성공한 verification 결과와 coverage 자료를 다시 확인할 수 있어야 한다.
4. Mutation 단계가 PR 검증 시간을 불필요하게 늘리지 않아야 한다.
5. 관리자 우회와 PR review 요구 여부를 명시해야 한다.

## 4. 선택 근거

2026-10-11 main commit `03c84f40c345195f0a323c5af2e623e9ebbbc998`의 GitHub Actions run [38112386284](https://github.com/kayden-kim/narumq/actions/runs/38112386284)은 `verify` check와 `ci` gate를 통과했다. Job은 5분 49초 뒤 완료됐고 `verification-38112386284-1` artifact를 생성했다. Artifact는 2026-11-10까지 보관한다.

GitHub API 조회에서 main protection은 필수 `verify`, `strict=true`, review 없음, 제한 없음, force push 금지, branch 삭제 금지, `enforce_admins=false`로 확인했다. 실패 test를 넣은 probe PR은 `verify`가 실패한 뒤 `mergeable_state=blocked`였다. 이 증거와 TEST-10의 남은 한계는 [§8.9](../verification/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거)에 기록한다.

`P0 mapped 70/81, exceptions 11`이며 traceability gate는 PASS했다. 이 결과는 등록된 예외가 허용된다는 뜻이며, 모든 P0 요구사항에 test가 있다는 뜻은 아니다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 로컬 `verify ci`만 사용 | 원격 설정이 필요 없다. | GitHub PR merge를 막지 못한다. | TEST-10을 보장하지 않는다. |
| Mutation을 모든 CI run에 포함 | 매 PR마다 mutation score를 확인한다. | 수동 run만 24분 24.90초였고, 일반 CI 시간이 크게 늘어난다. | Mutation은 별도 수동 gate로 둔다. |
| PR review와 관리자 보호를 모두 강제 | 승인 절차와 관리자 제한을 강화한다. | 현재 승인 흐름과 관리 정책을 추가로 바꾼다. | 사용자 결정에 따라 review는 필수로 하지 않고 관리자 보호도 적용하지 않는다. |

## 6. 결과

### 긍정적 결과

- 일반 PR은 최신 `main`을 반영하고 `verify`가 성공해야 merge할 수 있다.
- PR과 main push에서 같은 CI entrypoint를 사용한다. PR은 target branch를, push는 이전 commit을 diff base로 쓴다.
- Coverage 증거를 30일 동안 확인할 수 있다.
- Runner와 action runtime을 고정해 `ubuntu-latest` 전환과 Node 20 deprecation 경고를 피한다.

### 부정적 결과와 비용

- Full CI는 main run에서 5분 49초가 걸렸다. CI는 local보다 오래 실행된다.
- Mutation 결과는 매 PR에서 확인하지 않는다.
- `enforce_admins=false`이므로 관리자는 보호를 우회할 수 있다. Probe PR은 test 실패만 확인했다. Coverage 저하나 미매핑 P0 실패를 따로 주입하지 않았고 관리자 인증으로 실제 merge 거절도 시험하지 않았다. 따라서 TEST-10은 미검증으로 유지한다.
- TEST-06~TEST-09, TEST-13~TEST-15의 gate 통과는 production workload, release acceptance 또는 전체 P0 mapping 완료를 뜻하지 않는다.

### 후속 작업

- Verification run과 branch protection 증거를 [Performance와 verification §8.9](../verification/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거)에 유지한다.
- Action major와 `ubuntu-24.04` runner를 지원 상태에 맞춰 정기 검토한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| 원격 main CI가 전체 gate를 실행한다. | GitHub Actions run의 `verify` check 확인 | `success` | [Run 38112386284](https://github.com/kayden-kim/narumq/actions/runs/38112386284) |
| Updated runner·action runtime | 문서 PR #2의 `verify` check | `ubuntu-24.04`, Node 24 action v7에서 `success` | [Run 38113714789](https://github.com/kayden-kim/narumq/actions/runs/38113714789) |
| main protection이 `verify`를 요구한다. | Branch protection API 조회 | `strict=true`, check `verify`, force push·삭제 금지 | [§8.9](../verification/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거) |
| 실패한 PR은 merge 가능 상태가 아니다. | Intentional failing test가 있는 probe PR의 check와 merge state 조회 | `verify=failure`, `mergeable_state=blocked` | [Probe PR #1](https://github.com/kayden-kim/narumq/pull/1), [run 38112802413](https://github.com/kayden-kim/narumq/actions/runs/38112802413). Coverage·traceability 거절과 실제 merge API는 별도로 시험하지 않았다. |
| 로컬 Linux CI가 같은 commit에서 통과한다. | Docker의 Go 1.26.5 Linux container에서 `go run ./tools/verify ci --base f847a491e7b4a68d7fc48a868ad801895894d32a` 실행 | `coverage: PASS`, `ci: PASS` | `.artifacts/verification/ci-linux-one-shot-admission-close.log`; 세부 내용은 [§8.9](../verification/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거) |

## 8. 재검토 조건

- TEST-10의 관리자 우회를 막아야 하거나 merge 거절을 별도 권한의 사용자로 검증할 수 있게 되면 branch protection과 acceptance 상태를 다시 검토한다.
- `ubuntu-24.04` 또는 Action v7이 지원 종료·호환성 문제를 일으키면 새 runner·major로 전환하고 PR CI에서 검증한다.
- CI 전체 gate가 반복해서 15분을 넘으면 병렬도와 수동 mutation 분리 정책을 측정 근거로 다시 검토한다.
