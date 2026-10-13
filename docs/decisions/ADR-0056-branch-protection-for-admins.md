# ADR-0056: main branch protection을 관리자에게도 적용한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-11 |
| 결정자 | NaruMQ 프로젝트 사용자 |
| 관련 요구사항 | [PRD](../requirements/PRD.md)의 TEST-10 |
| 관련 스펙·계약 | [Performance와 verification](../specs/performance-and-verification.md), [개발 안내](../how-to/development-guide.md) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md) |
| 이 ADR이 대체하는 이전 ADR | [ADR-0053](ADR-0053-github-actions-required-ci-gate.md) §1 표의 "관리자에 대한 보호 적용" 행만 대체한다. 나머지 결정은 유지한다. |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

1. `main` branch protection의 `enforce_admins`를 영구히 `true`로 설정한다.
2. 저장소 관리자도 실패한 필수 check(`verify`)가 있거나 최신 `main`을 반영하지 않은 PR은 merge할 수 없다.
3. [ADR-0053](ADR-0053-github-actions-required-ci-gate.md)의 다른 branch protection 규칙(필수 status check `verify`, `strict=true`, force push 및 branch 삭제 금지)은 그대로 유지한다.

## 2. 배경

[ADR-0053](ADR-0053-github-actions-required-ci-gate.md)은 `verify`를 필수 CI status check로 지정했다. 그러나 관리자 보호(`enforce_admins`)는 `false`로 설정했다. 관리자 권한을 가진 계정은 필수 check가 실패하거나 최신 base를 반영하지 않은 상태에서도 GitHub API나 웹 UI로 merge할 수 있었다.

PRD TEST-10은 단위/통합 테스트 실패, 커버리지 하락, 미매핑 P0 요구사항, 미매핑 normative statement가 발생했을 때 merge가 차단되는지 증거를 요구한다. 관리자 보호가 꺼져 있으면 관리자 자격으로 실행한 merge 시도가 실패 check를 우회할 수 있어 merge 차단 효력을 확정할 수 없었다.

## 3. 결정 요인

1. **merge 차단 강제력**: 관리자를 포함한 모든 기여자가 실패한 check를 우회해 merge하지 못해야 한다.
2. **증거 신뢰성**: GitHub merge API(`PUT /repos/{owner}/{repo}/pulls/{n}/merge`) 호출 시 실제로 HTTP 405 거절이 발생해야 한다.
3. **단순성**: 추가 봇이나 외부 승인 도구 없이 GitHub 네이티브 branch protection 설정을 사용한다.

## 4. 선택 근거

`enforce_admins=true`를 활성화하면 관리자 토큰으로 merge API를 호출해도 HTTP 405 `Method Not Allowed`와 함께 `Required status check "verify" is failing.` 오류를 반환한다. 네 가지 실패 조건(test 실패, coverage 저하, 미매핑 P0, 미매핑 normative statement) 각각에서 관리자 계정의 merge API 호출이 거절됨을 확인했다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| `enforce_admins=false` 유지 | 긴급 상황 시 관리자가 즉시 merge 가능 | 관리자가 실수로 실패한 check를 우회할 위험이 있고, TEST-10 merge 차단을 증명할 수 없음 | TEST-10 검증 요구와 안전성을 만족하지 못함 |
| 임시로 `true`로 켜서 시험 후 다시 `false`로 복원 | 시험 증거는 확보하면서 평소 관리자 재량 유지 | 평상시 merge 차단 강제력이 사라지고 설정이 비일관적임 | 사용자가 영구히 `true`로 적용하기로 결정함 |
| `enforce_admins=true` 영구 적용 (선택) | 관리자를 포함해 어떤 계정도 실패한 PR을 merge할 수 없음 | 긴급 우회 시 관리자가 branch protection 설정을 먼저 변경해야 함 | merge 차단 강제력을 확실히 보장함 |

## 6. 결과

### 긍정적 결과

- 관리자 계정이라도 필수 check `verify`가 실패하거나 최신 `main`이 아니면 merge할 수 없다.
- 네 가지 결함 조건(test 실패, coverage 저하, 미매핑 P0, 미매핑 normative statement)에서 merge API 거절(HTTP 405)을 확인했다.

### 부정적 결과와 비용

- 긴급 hotfix 배포 등 보호를 우회해야 하는 예외 상황이 발생하면, 관리자가 저장소 설정에서 `enforce_admins`를 일시 해제하거나 protection을 변경해야 한다.
- 로컬 `main`의 미푸시 커밋이나 단독 수정 사항도 반드시 PR과 `verify` 성공을 거쳐야만 `main`에 반영할 수 있다.

### 후속 작업

- 성능 스펙 §8.9에 네 probe의 PR 번호, CI run ID, HTTP 405 응답을 기록한다.
- DEP P0 예외 10개가 남아 있으므로 TEST-10은 계속 `미검증`으로 관리한다.

## 7. 검증

2026-10-11에 `enforce_admins=true`를 적용하고 네 probe PR을 열어 merge API 거절을 검증했다.

| Probe | PR | Head SHA | CI Run ID | CI 결론 | Merge API 결과 |
| --- | --- | --- | --- | --- | --- |
| `test-failure` | [#3](https://github.com/kayden-kim/narumq/pull/3) | `8a57c37` | [38125505236](https://github.com/kayden-kim/narumq/actions/runs/38125505236) | `failure` | HTTP 405 (`Required status check "verify" is failing.`) |
| `coverage-drop` | [#4](https://github.com/kayden-kim/narumq/pull/4) | `c0eb473` | [38125509535](https://github.com/kayden-kim/narumq/actions/runs/38125509535) | `failure` | HTTP 405 (`Required status check "verify" is failing.`) |
| `unmapped-p0` | [#5](https://github.com/kayden-kim/narumq/pull/5) | `a60dd8e` | [38125513065](https://github.com/kayden-kim/narumq/actions/runs/38125513065) | `failure` | HTTP 405 (`Required status check "verify" is failing.`) |
| `unmapped-mqtt-statement` | [#6](https://github.com/kayden-kim/narumq/pull/6) | `2f96cf8` | [38125516594](https://github.com/kayden-kim/narumq/actions/runs/38125516594) | `failure` | HTTP 405 (`Required status check "verify" is failing.`) |

모든 probe에서 merge API 호출 후 `merged=false`와 `origin/main` SHA 불변(`4c1ab4cc143426411cb5ea9ad1c0d2b501b2b5d4`)을 확인했다. 확인 후 모든 PR을 닫고 remote branch를 삭제했다.

## 8. 재검토 조건

- 긴급 장애 대응 등에서 branch protection을 임시 우회하는 자동화 절차가 별도로 필요해진 경우.
- GitHub Actions 외의 다른 CI 게이트 시스템으로 이전하는 경우.
