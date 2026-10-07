# ADR-0040: 빈 arm 주입으로 branch coverage 측정

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-08 |
| 결정자 | 사용자 |
| 관련 요구사항 | [TEST-06, TEST-07, TEST-08, TEST-09, TEST-11](../requirements/PRD.md#115-테스트와-coverage) |
| 관련 스펙 | [Performance와 verification §8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate) |
| 관련 아키텍처 | [Project structure](../architectures/project-structure.md#verification-and-integration-test-entrypoints) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

1. Branch coverage는 결정점의 결과인 arm을 실행한 비율이다. `if`의 `then`·`else`, `switch`·type switch의 각 case·default, `select`의 각 comm clause를 센다. `for`·`range`, `&&`·`||` 단락 평가, clause가 없는 switch·select와 `func _` body는 세지 않는다. Package 수준 func literal은 포함한다.
2. Line coverage와 같은 production source 집합을 사용한다. 현재 GOOS·기본 build tag에서 활성인 `cmd/`·`internal/` 파일을 읽으며 generated `.pb.go`는 기존 제외 정책을 따른다.
3. 빠진 `else`에는 ` else {}`를 넣는다. Clause가 있는 switch·type switch에 default가 없으면 `default:`를 넣는다. 줄바꿈과 `;`을 추가하지 않는다. `select`에는 default를 넣지 않는다. Default를 넣으면 blocking 동작이 바뀐다.
4. Git의 tracked·unignored untracked 파일을 `.artifacts/verification/branch/module`에 복사한다. Production source만 계측한다. 이 사본에서 atomic coverage를 포함한 전체 `go test ./...`를 실행한다. Race·vet·traceability·architecture·fuzz·mutation은 원본에서 실행한다. 출력 directory를 교체하기 전에 실제 경로를 확인한다. 저장소 root·상위 directory는 삭제하지 않는다. 저장소 안의 출력은 `.artifacts/verification/branch` 또는 그 하위로 제한해 다른 검증 증거를 보존한다. 저장소 밖의 별도 출력 directory는 사용할 수 있다. 저장소 안의 symlink·junction을 거치는 출력은 실제 경로를 해석하기 전에 거절한다. 실제 경로를 확인한 뒤에도 남은 symlink·junction 경로는 거절한다.
5. Manifest schema 1은 원본 SHA-256과 계측한 좌표를 기록한다. `if then`은 body span, `else`는 then body 끝부터 else 끝까지, clause는 clause 시작부터 끝까지를 사용한다. Span 양 끝을 포함한다. Span 안에서 시작 위치가 가장 이른 coverage block 중 하나라도 count가 양수면 해당 arm을 실행한 것으로 판정한다. Source 집합·hash·block이 맞지 않으면 평가를 거절한다.
6. Coverage policy와 report는 schema 2를 사용한다. Report에 전체·core·diff branch metric, manifest 경로, 미실행 outcome과 `requirementShortfalls`를 기록한다. Diff는 변경 줄에 결정점이 있는 outcome만 센다. 변경 결정점이 없으면 diff branch는 100%다.
7. 전체와 core branch는 두 번 실행한 값 중 작은 값을 소수점 둘째 자리에서 내림한 baseline ratchet만 강제한다. Diff branch는 95%를 바로 강제한다. PRD 전체 85%·core 90% 미달은 `requirementShortfalls`로 보고하되 gate 실패 사유로 쓰지 않는다. 미달이 남으면 TEST-06·TEST-07은 미검증으로 유지한다. Line·function gate는 그대로 유지한다.

## 2. 배경

기존 gate는 branch를 `not_evaluated`로 기록했다. Line coverage는 실행한 결과 arm을 모두 구분하지 못한다. Go coverage의 빈 block counter를 이용하면 원본 line 번호를 유지하면서 빠진 결과 경로도 측정할 수 있다.

## 3. 결정 요인

- Build constraint와 현재 GOOS를 기존 production source 로더와 동일하게 적용한다.
- 원본 production code와 line·function metric의 분모를 바꾸지 않는다.
- Toolchain의 실제 coverage block 위치와 대조해 판정한다.
- 측정 gate 도입과 PRD floor 충족을 위한 test 보강을 구분한다.

## 4. 선택 근거

Go coverage는 빈 body와 clause에도 0-statement block을 만든다. 기존 line 집계는 이 block을 제외한다. 원본과 계측 module을 각각 실행해 line·function total이 같은지 확인한다. Manifest의 원본 hash는 이전 계측 결과를 새 source에 잘못 적용하지 못하게 한다. 사본은 `.artifacts`에 두어 원본의 `./...` 검사에 섞이지 않게 한다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| gobco v1.3.4 | 기존 branch 도구를 사용한다. | Build constraint를 무시한 ParseDir로 OS별 파일 쌍의 type check가 panic한다. Package 단위 측정이다. | 현재 production source 집합에서 동작하지 않는다. |
| `go test -overlay` | 원본 파일을 바꾸지 않는다. | Go 1.26.5의 cover 실행은 원본 경로를 읽는다. | Coverage 계측 대상 source를 대체하지 못한다. |
| `-toolexec` | Build tool 호출을 가로챌 수 있다. | Build cache key에 계측 결과가 반영되지 않는다. | 계측 source와 profile의 일치를 보장하기 어렵다. |

## 6. 결과와 후속 작업

사본 생성과 추가 AST parse 비용이 생긴다. 원본 production source는 바뀌지 않는다. Branch 정의는 언어의 모든 조건 경로가 아니라 §1의 arm 집합으로 제한한다.

- PRD floor를 충족하도록 미실행 arm의 test를 보강한다.
- CI 플랫폼의 필수 status와 merge 차단 증거를 확보한다.
- TEST-10 P0 요구사항 매핑은 별도 작업으로 유지한다.

## 7. 검증

| 결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Toolchain block 대응과 기존 분모 보존 | `tools/verify/branch_test.go`의 실제 Go cover 시험 | 의도한 미실행 outcome 정확히 일치, 원본·사본 line/function total 일치 | [스펙 §8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate) |
| 계측·manifest 경계 | Instrumenter 경계와 stale/mismatch 시험 | 줄 수 보존, 의미를 바꾸는 select default 미주입, 불일치 거절 | 같은 스펙 절 |
| Enforcement | Ratchet·diff·shortfall 시험과 실제 coverage CLI | Ratchet 하락·diff 95% 미달 실패, PRD shortfall은 보고 | 같은 스펙 절 |
| Baseline 안정성 | `verify local` Run A·B·C | A·B로 ratchet 확정, C PASS | 같은 스펙 절 |

## 8. 재검토 조건

- Go toolchain이 빈 arm의 coverage block 위치나 계측 동작을 바꾸는 경우
- Branch 정의에 loop·단락 평가를 포함하도록 요구사항을 바꾸는 경우
- 전체·core PRD branch floor를 즉시 강제할 수 있게 test 보강을 완료한 경우
