# ADR-0041: Branch ratchet 노이즈 허용폭

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-08 |
| 결정자 | 사용자 |
| 관련 요구사항 | [TEST-06, TEST-07, TEST-09](../requirements/PRD.md#115-테스트와-coverage) |
| 관련 스펙 | [Performance와 verification §8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate) |
| 관련 아키텍처 | [Project structure](../architectures/project-structure.md#verification-and-integration-test-entrypoints) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음. [ADR-0040 §1](ADR-0040-branch-coverage-arm-injection.md#1-결정)을 보완하며 대체하지 않는다. |
| 이 결정을 대체한 새 결정 기록(ADR) | [ADR-0044](ADR-0044-diff-coverage-floor-90.md)가 §1.4의 diff 95% 수치만 대체한다. 허용폭의 적용 범위와 나머지 결정은 유지한다. |

## 1. 결정

1. 전체 branch ratchet과 core branch ratchet은 `측정값 < ratchet − branchTolerance`일 때 실패한다. 비교에는 반올림 오차 0.005%p를 추가로 허용한다. 구현 비교식은 `actual.Percent+0.005 < ratchet-branchTolerance`다.
2. `branchTolerance`는 `tests/verification/coverage-policy.json`의 `ratchet.branchTolerance`에 기록한다. 단위는 퍼센트포인트(%p)이고 허용 범위는 0–1이다. 필드가 없으면 값은 0이며, 기존처럼 엄격하게 비교한다.
3. 현재 `branchTolerance`는 0.20%p다. 같은 HEAD에서 9회 측정한 covered 수의 폭은 12/13,073, 약 0.09%p였다. 이 폭을 0.10%p로 올림하고 2배를 허용폭으로 정했다. 측정 결과에 따라 자동으로 바꾸지 않는다. 변경하려면 새 반복 측정 증거와 결정 기록이 필요하다.
4. 허용폭은 전체·core branch ratchet에만 적용한다. Line·function gate, diff 95% floor와 `requirementShortfalls`에는 적용하지 않는다.
5. Ratchet 기준값은 [ADR-0040 §1.7](ADR-0040-branch-coverage-arm-injection.md#1-결정)의 식을 유지한다. 두 실행 중 작은 값을 소수점 둘째 자리에서 내림한다.

## 2. 배경

`verify local`은 시험, race, fuzz를 통과해도 전체 branch 측정값의 작은 변동으로 ratchet 아래에 떨어질 수 있다. 같은 HEAD의 9회 측정에서는 covered block 수가 10,429–10,441/13,073으로 달랐다. 고정된 baseline과 실제 시험 동작의 비결정성이 이 흔들림에 함께 영향을 준다.

## 3. 결정 요인

- 측정 잡음은 baseline의 의미를 바꾸지 않으면서 gate에서 허용해야 한다.
- 허용폭은 정책 파일에서 명시하고 범위를 검증해야 한다.
- 허용폭을 line·function·diff gate나 PRD 요구사항 충족 판단으로 넓히지 않는다.
- 실제 미실행 경로는 결정적 시험으로 보강한다.

## 4. 선택 근거

0.20%p는 관측한 최대 spread 약 0.09%p를 넘지만, 시험 경로가 바뀌는 문제를 덮을 만큼 넓게 설정하지 않았다. 허용폭을 코드 상수 대신 coverage policy에 두면 평가 환경에서 값을 확인할 수 있고, 명시하지 않은 기존 policy는 0으로 동작한다. 반복 결과에 따라 값을 자동으로 움직이지 않으므로 ratchet은 재현 가능한 기준으로 남는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 허용폭 없이 엄격 비교 | 기준과 결과가 직접 일치한다. | 같은 HEAD의 계측 변동으로 gate가 간헐 실패한다. | 관측된 0.09%p의 측정 변동을 반영하지 못한다. |
| 실행마다 ratchet 자동 조정 | 매 실행 결과에 쉽게 맞출 수 있다. | 기준이 측정 결과를 따라가므로 회귀를 감출 수 있다. | Ratchet의 고정된 비교 기준을 잃는다. |
| 모든 coverage floor에 허용폭 적용 | 비교 정책이 단순해진다. | PRD floor, diff floor와 line/function gate의 의미가 달라진다. | Branch ratchet의 반복 측정 잡음만 다루는 범위를 넘는다. |

## 6. 결과와 후속 작업

Branch ratchet은 작은 측정 변동을 허용하지만 실제 branch 미실행을 자동으로 보정하지 않는다. 정책의 `branchTolerance`를 0.20으로 설정하고, in-process 경로를 고정하며 직접 검증이 없던 동작에 결정적 시험을 추가한다.

- 반복 실행으로 시험 경로가 고정되고 branch spread가 허용폭 아래인지 확인한다.
- 반복 측정 A·B로 전체·core ratchet을 다시 정하고 Run C와 실제 CLI 경계로 검증한다.
- 측정 결과와 uncovered outcome은 [Performance와 verification §8.3](../verification/performance-and-verification.md#83-branch-ratchet-노이즈-허용폭-수용-증거)에 기록한다.

## 7. 검증

| 결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| 허용폭 비교와 범위 | `tools/verify` 단위 시험 및 coverage CLI 경계 실행 | 79.61% 통과, 79.60% 거절; -0.01과 1.01 거절; CLI 경계에서 0.21%p 초과 거절, 0.19%p 이내 통과 | [성능 검증 기록 §8.3](../verification/performance-and-verification.md#83-branch-ratchet-노이즈-허용폭-수용-증거) |
| 시험 경로 고정 | 계획된 반복 `-race` 시험 및 Run A·B·C outcome 확인 | 지정 outcome이 각 실행에서 covered이고 spread가 0.20%p 미만 | 같은 스펙 절 |
| Ratchet 재기준 | Run A·B로 전체·core ratchet 계산, Run C와 추가 local gate 실행 | Run C 및 local gate가 PASS | 같은 스펙 절 |

## 8. 재검토 조건

- 동일 HEAD 반복 측정 spread가 0.20%p 이상인 경우
- 더 좁은 반복 측정 spread와 결정적 outcome 증거로 허용폭 축소를 제안하는 경우
- Coverage metric 또는 rounding 동작이 바뀌는 경우
