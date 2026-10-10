# ADR-0044: 변경 코드 coverage 기준 90%

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-09 |
| 결정자 | 사용자 |
| 관련 요구사항 | [TEST-08, TEST-09, TEST-10](../requirements/PRD.md#115-테스트와-coverage) |
| 관련 스펙·계약 | [Performance와 verification §8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md#repository-verification-모듈-흐름) |
| 이 ADR이 대체하는 이전 ADR | [ADR-0040 §1.7](ADR-0040-branch-coverage-arm-injection.md#1-결정)의 diff branch 95% 기준과 [ADR-0041 §1.4](ADR-0041-branch-ratchet-noise-tolerance.md#1-결정)의 diff 95% 수치만 대체한다. 나머지 결정은 유지한다. |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

1. 변경 production code의 line·function·branch diff coverage 하한을 각각 90%로 정한다. 정확한 요구사항은 PRD TEST-08, 실행 정책은 `tests/verification/coverage-policy.json`의 `diffFloor`를 따른다.
2. 특정 커밋에만 예외를 두지 않는다. `--base`로 변경 범위를 평가하는 모든 coverage 실행에 같은 기준을 적용한다.
3. 전체 코드와 핵심 모듈의 coverage 기준, baseline ratchet, branch 측정 방식과 생성 코드 제외 규칙은 바꾸지 않는다.
4. Diff 기준에는 overall·core branch ratchet의 노이즈 허용폭을 적용하지 않는다. 기준 미달은 계속 exit 1로 거절한다.
5. P0 예외와 필수 CI merge 차단 증거가 남아 있으므로 이번 기준 변경만으로 TEST-10을 검증 상태로 바꾸지 않는다.

## 2. 배경

사용자는 최신 커밋의 coverage 기준을 검토한 뒤 변경 코드의 line·function·branch 기준을 모두 낮추기로 결정했다. 최종 승인 값은 90%다. 최신 커밋 `de19302d9887bfe2650dd6dcc5ad975281d4a192`의 변경 범위는 부모 커밋 `74c3cdbb4abc9b01e07e8fa7b936653e25c446f7`과 비교한다. 기존 profile을 사용한 평가는 line 97.62%, function 100%, branch 94.12%였으며 당시 branch 하한 95%에 미달했다.

## 3. 결정 요인

1. 사용자가 승인한 변경 코드 기준을 PRD와 실행 정책에 동일하게 적용한다.
2. 특정 커밋만 통과시키는 예외 없이 이후 변경에도 같은 기준을 적용한다.
3. 기준 미달을 거절하는 동작과 전체·핵심 모듈의 기존 보호를 유지한다.

## 4. 선택 근거

90%는 사용자가 선택한 정책 값이다. 측정 결과만으로 자동 조정한 값이 아니며, 품질 영향이 없다고 입증한 값도 아니다. 기존 evaluator는 policy에서 세 하한을 읽으므로 측정 알고리즘을 바꾸지 않고 적용할 수 있다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 95% 유지 | 기존의 더 높은 변경 코드 하한을 유지한다. | 최종 사용자 결정과 다르다. | 사용자가 90%를 채택했다. |
| 세 하한을 85%로 변경 | 더 낮은 coverage로도 변경을 허용한다. | 사용자가 최종 선택한 90%보다 완화된다. | 최종 결정은 90%다. |
| Branch만 90%로 변경 | Line·function 하한 95%를 유지한다. | 사용자가 선택한 세 지표의 동일 기준과 다르다. | Line·function·branch 모두 변경한다. |
| 최신 커밋에만 예외 적용 | 이후 변경에는 95%를 유지한다. | 커밋별 특례로 정책이 나뉜다. | 공통 diff 하한을 적용한다. |

## 6. 결과

### 긍정적 결과

- 세 diff 지표에 같은 하한을 적용한다.
- 측정 방식과 전체·핵심 모듈 ratchet을 유지한다.

### 부정적 결과와 비용

- 변경 코드에서 허용하는 미실행 비율이 기존보다 커진다.
- Coverage 통과는 P0 요구사항의 구현이나 실제 CI merge 차단을 증명하지 않는다.

### 후속 작업

- 실제 CI의 필수 status와 merge 차단 증거는 TEST-10에서 계속 추적한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| 세 diff 하한과 기존 보호를 적용한다. | Coverage evaluator 시험과 실제 coverage CLI | 각 지표가 90% 이상이면 통과하고 미달하면 거절한다. | 실행 결과는 [성능 스펙 §8.5](../specs/performance-and-verification.md#85-변경-코드-diff-floor-90-수용-증거)에 기록한다. |
| 최신 커밋에 새 정책을 적용한다. | 기존 atomic profile과 branch manifest로 부모 커밋 대비 평가 | Source·manifest 검증 뒤 diff 하한을 충족한다. | 같은 스펙 절에 기준 커밋, 명령과 결과를 기록한다. |

## 8. 재검토 조건

- 변경 코드의 미실행 경로에서 결함이 반복되는 경우
- 사용자가 변경 코드의 품질 하한을 다시 정하는 경우
