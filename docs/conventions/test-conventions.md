# 테스트 컨벤션

이 컨벤션은 스펙의 시나리오와 테스트를 잇는 방법을 정한다. 연결은 테스트 소스 코드의 메타 정보로 둔다. 시나리오별 연결과 결과는 보고 명령이 생성한다. 사람이 스펙에 테스트 위치나 결과를 적지 않는다.

| 항목 | 값 |
| --- | --- |
| 상태 | Active |
| 적용 대상 | 스펙 시나리오를 검증하는 Go 테스트·시나리오 함수와 `tests/` 아래 Python 테스트·시나리오 함수 |
| 기준 버전 | Go 1.26 계열(`go.mod`), CI의 Python 3.9 |
| 관련 아키텍처 문서 | [Project structure의 코드 지도](../architectures/project-structure.md) |
| 관련 ADR | 없음 |
| 적용 시작일 | 2026-10-17 |
| 규칙 준수 확인 방법 | `go run ./tools/verify scenarios`(`local`·`ci` 게이트에 포함), 코드 리뷰 |

## 1. 규칙 우선순위

[컨벤션 작성 규칙](AGENTS.md#규칙-우선순위)을 따른다.

## 2. 규칙

| 대상 | 해야 할 것 | 피해야 할 것 | 판정 방법 |
| --- | --- | --- | --- |
| 스펙 시나리오 표 | 헤더가 `시나리오 ID`로 시작하는 표에 시나리오 ID와 기대 결과만 정의한다 | 표에 테스트 파일 위치, 실행 결과, 실행 기록을 적는다 | `scenarios`의 `오류`·리뷰 |
| 테스트 연결 | 시나리오를 검증하는 최상위 테스트 함수의 메타 정보에 `<스펙 파일>:<시나리오 ID>`를 적는다 | 서브테스트마다 따로 적거나 ID를 스펙 밖에서 만든다 | `scenarios`의 `미연결`·`오류` |
| 연결 대상 | 테스트가 시나리오의 조건과 입력을 실행하고 기대 결과를 확인할 때만 연결한다 | 관련 없는 테스트를 연결하거나 연결하려고 시나리오와 맞지 않는 테스트를 만든다 | 코드 리뷰 |
| 통과 표시 | 보고서나 [검증 기록](../verification/)에 남은 실제 실행 결과로 표시한다 | 커밋이나 테스트 파일 링크만으로 통과를 표시한다 | 코드 리뷰 |
| 시나리오 ID | 스펙 파일 안에서 유일한 ID를 문서 순서대로 쓰고, 한번 쓴 ID는 다시 쓰지 않는다 | 기존 ID의 번호를 바꾼다 | `scenarios`의 중복 ID `오류`·리뷰 |

## 3. 예제

```go
// Traceability: PRD DEP-11; docs/specs/drain-and-rolling-update.md §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-004, drain-and-rolling-update.md:DRN-SCN-005
func TestRolloutRetryReapprovesSafeToStop(t *testing.T) {
```

```python
# Scenarios: drain-and-rolling-update.md:DRN-SCN-007
def scenario_safe_retry(client):
```

```text
좋음: Scenarios 줄이 함수 바로 위 주석에 있고, 가리키는 ID가 스펙 표에 있다.
나쁨: Scenarios 줄과 함수 사이에 빈 줄이 있다. 스펙 파일 이름에 철자 오류가 있다.
이유: 도구는 줄 바로 아래의 함수에 연결한다. 없는 파일이나 ID를 가리키면 `오류`가 된다.
```

## 4. 예외와 자동 생성 산출물

- 보고서는 `.artifacts/`에 생성되는 산출물이다. 저장소에 커밋하지 않는다.
- Go는 `Test`·`Benchmark`·`Fuzz`·`Example`이 아닌 함수와 메서드에도 `Scenarios:`를 쓸 수 있다. Docker SDK 호환성 suite 같은 실행 진입점이 해당한다. 이 함수의 결과는 `go test -json`에 없으므로 `--results` 파일로 넘긴다.
- 별도 환경과 긴 실행 시간이 필요한 Go opt-in 수용 시험은 같은 doc comment에 `Acceptance: <환경 변수>` 줄을 쓴다. `_test.go`의 `Test*` 함수에만 쓸 수 있으며, 환경 변수는 그 시험이 `1`인지 확인하는 이름이다. 같은 doc comment에 `Scenarios:` 줄이 있어야 한다. `acceptance` 명령은 시나리오 연결을 통해 시험을 찾기 때문이다. 다른 곳에 쓰거나, 이름을 빠뜨리거나, 한 함수에 두 번 쓰거나, `Scenarios:` 줄 없이 쓰면 연결 검사의 `오류`다. 이 시험은 기본 게이트에서 건너뛰어도 실패하지 않고 `go run ./tools/verify acceptance`로 실행한다. Docker 없음처럼 환경 변수와 무관한 이유로 건너뛰는 일반 시험에는 쓰지 않는다.

  ```go
  // Acceptance: NARUMQ_ROLLING_100
  // Scenarios: drain-and-rolling-update.md:DRN-SCN-018
  func TestDynamicClusterContinuousRollingDrainProcessAcceptance(t *testing.T) {
  ```

- Python은 `def`와 `class` 바로 위 주석에 쓴다. 데코레이터와 다른 주석 줄이 사이에 있어도 된다. 결과는 [`tests/scenario_results.py`](../../tests/scenario_results.py)로 `--results` 파일에 남길 수 있다.
- EKS·kind 실측, 수동 운영 검증과 실패 기록은 보고서가 대신하지 못한다. 게이트 밖 시나리오는 [검증 기록](../verification/)에 실행 기록을 남긴다.

수동 수용 시험은 기본 게이트에서 외부 자원을 만들지 않는다. `tests/acceptance_docker_capacity.py`, `tests/acceptance_eks_capacity.py`, `tests/acceptance_github_ci.py`는 각 파일의 명시적 opt-in 환경 변수가 있을 때만 실행한다. `Acceptance:`를 표시한 Go 시험은 `go run ./tools/verify acceptance [--run <regexp>]`로 실행한다. 이 명령은 고른 시험의 환경 변수가 모두 `1`이어야 시작하고, 고른 시험이 모두 통과해야 성공한다. 건너뜀과 결과 없음은 실패다. 결과는 `.artifacts/verification/acceptance-go-test.json`에 남으며 `scenarios --go-test-json`으로 보고서에 합칠 수 있다. EKS chaos 도구는 별도 합성 fixture의 신원을 확인한 뒤 실행한다. 연결 검사의 통과를 이 시험의 실행 통과로 표시하지 않는다.

`go run ./tools/verify package-tests [--results FILE]`는 검증 도구 패키지를 실제 실행한다. `go run ./tools/verify baseline [--results FILE]`는 전체 `local` 게이트를 세 번 실행한다. Baseline은 첫 두 측정값 중 작은 branch 비율을 소수 둘째 자리까지 내림해 계산한다. 세 번째 실행도 통과해야 한다. 이 명령은 coverage policy를 바꾸지 않는다. 두 명령은 기존 수동 결과 형식으로 `.artifacts/verification/scenarios/go-acceptance-results.jsonl`에 결과를 추가한다. 새 실행을 따로 보고하려면 새 `--results` 경로를 지정한다.

```sh
go run ./tools/verify package-tests --results .artifacts/verification/scenarios/package-tests-results.jsonl
go run ./tools/verify scenarios --results .artifacts/verification/scenarios/package-tests-results.jsonl --require-executed
```

## 5. 검증 명령과 통과 기준

- 실행 환경: Windows·Linux, Go 1.26 계열. `go test`가 만드는 JSON은 UTF-8이어야 한다. Windows PowerShell 5의 `>`는 UTF-16으로 저장하므로 쓰지 않는다. bash, PowerShell 7 또는 `cmd /c "... > 파일"`을 쓴다.
- 실행 위치: 저장소 루트.
- 성공 조건: 종료 코드 0.

검사는 둘이며 같은 명령군이다.

| 검사 | 명령 | 종료 코드가 0이 아닌 경우 | 실행 시점 |
| --- | --- | --- | --- |
| 연결 검사 | `go run ./tools/verify scenarios` | `오류`, `미연결`. 결과 파일을 함께 주면 `실패`도 센다 | `local`·`ci` 게이트의 preflight |
| 실행 검사 | `go run ./tools/verify scenarios --go-test-json FILE... [--results FILE...] --require-executed` | 연결 검사의 모든 경우, 그리고 실행 범위 안의 `미실행` | 게이트가 병렬 검사를 마친 뒤 |

연결 검사는 스펙 표와 테스트 메타 정보만 대조한다. `미실행`은 실패로 세지 않는다. 실행 검사는 결과 입력이 하나 이상 있어야 한다.

실행 범위는 다음과 같다.

- `--go-test-json` 파일에 나온 Go 패키지의 `Test`·`Fuzz` 함수. 게이트가 실행한 패키지다.
- `--results` 파일에 키가 있는 테스트. Python 함수, `Benchmark`·`Example`, `Test`가 아닌 Go 함수와 메서드가 해당한다.

실행 범위 안의 연결 테스트에 통과 결과가 없으면 실행 검사가 실패한다. 건너뛴 테스트는 `미실행 (건너뜀)`, 이벤트가 없는 테스트는 `미실행 (결과 없음)`이다. 범위 밖의 연결 테스트는 `미실행 (게이트 밖)`으로 보고서에 남고 검사를 실패시키지 않는다. `Acceptance:`를 표시한 테스트는 통과나 실패 결과가 있을 때만 실행 범위에 든다. 건너뛰었거나 이벤트가 없으면 `게이트 밖`이다.

`local`·`ci` 게이트는 전체 package test를 `go test -json`으로 실행한다. 이 명령의 stdout은 콘솔이 아니라 `.artifacts/verification/go-test.json`에 저장된다. stderr는 콘솔에 그대로 나온다. 병렬 검사가 실패하면 게이트가 이 파일에서 실패한 테스트와 출력을 골라 콘솔에 보여 준다. 병렬 검사가 모두 성공하면 게이트가 이 파일로 실행 검사를 실행한다.

CI는 Go 게이트와 별도로 `python tests/run_scenario_unittest.py <시험 파일>...`로 Python unittest를 실행한다. Runner는 시험 파일을 import하기 전에 `.artifacts/verification/scenarios/python-ci-results.jsonl`을 새 실행용 빈 파일로 초기화한다. 이전 실행 결과를 재사용하지 않는다. Import·class setup이 실패해 method가 실행되지 않아도 빈 파일은 남고 Python 명령은 실패한다.

Runner는 실제 method별 통과·실패·건너뜀을 기록한다. Subtest 하나가 실패하면 뒤의 subtest가 통과해도 해당 method는 실패다. Expected failure는 통과가 아니라 건너뜀으로 기록한다. 파일별 결과 키는 `<파일 경로>#<method 이름>`이다. Class·module fixture 오류를 method의 통과 결과로 바꾸지 않는다.

`.github/workflows/verify.yml`의 마지막 실행 검사는 Go JSON과 Python JSONL 중 생성된 파일만 입력한다. Go preflight가 실패해 JSON이 없어도 Python JSONL로 보고서를 갱신한다. 어느 입력도 없으면 보고 단계를 실패시킨다. 빈 JSONL은 실행한 시험이 없다는 뜻이며, Python 단계의 실패를 취소하지 않는다. 생성된 입력 파일과 보고서는 CI artifact에 보관한다. 이 절차는 라이브 EKS 하네스를 실행하지 않는다.

```sh
# 연결 검사
go run ./tools/verify scenarios

# 게이트와 같은 실행 검사. go-test.json은 게이트가 만든다. 직접 만들려면 아래처럼 한다.
go test -json -count=1 ./... > .artifacts/verification/go-test.json
go run ./tools/verify scenarios --go-test-json .artifacts/verification/go-test.json --require-executed

# Python 시나리오 결과를 더하려면 --results를 붙인다.
go run ./tools/verify scenarios --go-test-json .artifacts/verification/go-test.json --results .artifacts/verification/scenarios/python-results.jsonl --require-executed
```

실패한 테스트가 있으면 `go test`가 0이 아닌 코드로 끝나지만 JSON 파일은 만들어진다. 그 파일로 보고서를 만들어 실패한 시나리오를 확인한다.

## 6. 기존 코드 마이그레이션

기존 `// Traceability:` 줄은 그대로 둔다. 이 줄은 PRD 요구사항 ID와 MQTT statement를 테스트에 잇는다. `go run ./tools/verify traceability`가 검사하며, 시나리오 연결과는 별개다. 한 테스트에 두 줄을 함께 둘 수 있다. `Traceability:` 줄을 먼저 쓰고 `Scenarios:` 줄을 그 아래에 쓴다. `traceability` 검사는 `Scenarios:` 줄을 읽지 않으므로 시나리오 ID가 요구사항 ID처럼 보여도 충돌하지 않는다.

| 줄 | 잇는 대상 | 검사 명령 |
| --- | --- | --- |
| `// Traceability:` | PRD 요구사항 ID, MQTT statement ↔ 테스트 | `go run ./tools/verify traceability` |
| `// Scenarios:` | 스펙 시나리오 ID ↔ 테스트 | `go run ./tools/verify scenarios` |

시나리오 연결을 추가하려고 기존 테스트의 이름·동작·`Traceability:` 줄을 바꾸지 않는다.

## 7. 테스트와 스펙 시나리오 연결

규칙은 [검증 기록](../AGENTS.md#검증-기록)을 따른다. 스펙의 시나리오 표는 [스펙 목록](../specs/README.md)의 각 스펙에 있다.

- 메타 정보 방식: 테스트 함수 바로 위의 주석 한 줄이다. Go는 함수 doc comment에 `Scenarios:`로 시작하는 줄을 쓴다. Python은 `def`·`class` 바로 위에 `# Scenarios:` 줄을 쓴다. 참조 여러 개는 `, `로 구분한다. `Scenarios:` 줄을 여러 줄로 나눠도 된다.
- 형식 예: `// Scenarios: drain-and-rolling-update.md:DRN-SCN-004` (참조는 `<docs/specs/ 기준 파일 이름>:<시나리오 ID>`다)
- 커버리지·결과 보고 명령: `go run ./tools/verify scenarios [--go-test-json FILE]... [--results FILE]... [--require-executed] [--output DIR]`
- 보고서·CI 결과 위치: `.artifacts/verification/scenarios/report.md`와 `report.json`이다. `--output`으로 바꿀 수 있다. 게이트는 연결 검사에 이어 실행 검사를 같은 위치에 쓰므로 게이트 뒤의 보고서는 실행 검사 결과다. CI는 `verify` 워크플로의 두 보고서 파일과 `go-test.json`을 `verification-<run id>-<시도 번호>` 아티팩트에 올린다.
- 통과 조건: 게이트 범위의 시나리오는 연결 검사와 실행 검사가 모두 깨끗해야 한다. 미연결 시나리오가 없다. 오류가 없다. 실패가 없다. 실행 범위 안의 연결 테스트가 모두 통과했다. 게이트 밖 시나리오는 이 검사만으로 통과를 주장하지 않는다. 실행 기록을 [검증 기록](../verification/)에 남기거나 `--results` 파일로 보고서에 포함한다.

### 7.1 보고서가 읽는 것

| 입력 | 읽는 방법 |
| --- | --- |
| 스펙 | `docs/specs/*.md`(`README.md`, `AGENTS.md` 제외)에서 첫 헤더 칸이 `시나리오 ID`인 표의 첫 칸을 ID로 읽는다. 코드 펜스 안의 표와 다른 표는 읽지 않는다. |
| Go 소스 | 모든 `.go` 파일의 함수·메서드 doc comment. 패키지 import path는 가장 가까운 `go.mod`의 모듈 경로로 정한다. |
| Python 소스 | `tests/` 아래 `.py` 파일의 `def`·`class` 바로 위 주석. |
| 건너뛰는 폴더 | `.git`, `.artifacts`, `node_modules`, `vendor` |
| `--go-test-json` | `go test -json`의 이벤트 파일. 최상위 테스트의 `pass`·`fail`·`skip`을 읽는다. 하위 테스트가 실패하면 최상위 테스트도 실패다. 파일에 나온 Go 패키지는 실행 범위에 든다. 여러 번 지정할 수 있다. |
| `--results` | 아래 JSON Lines 파일. 파일에 키가 있는 테스트는 실행 범위에 든다. 여러 번 지정할 수 있다. |
| `--require-executed` | 실행 검사를 켠다. 실행 범위 안의 `미실행`을 실패로 센다. |

### 7.2 결과 파일 형식

한 줄에 JSON 객체 하나를 쓴다. 알 수 없는 필드와 빈 `test`는 오류다.

```json
{"test":"tests/chaos/eks-rollout-faults/main.py#scenario_safe_retry","status":"pass","commit":"1a2b3c4d","environment":"python 3.9 Linux/x86_64","command":"python run.py","finishedAt":"2026-10-17T01:02:03Z"}
{"test":"narumq/tests/compatibility/mqtt-clients-v1.compatibilityRun.runDrainClusterCase","status":"fail"}
```

| 필드 | 값 |
| --- | --- |
| `test` | 필수. 테스트 키. Python은 `<저장소 기준 파일 경로>#<def 또는 class 이름>`이다. Go는 `<package import path>.<함수 이름>`이고, 메서드는 `<package import path>.<타입 이름>.<메서드 이름>`이다. |
| `status` | 필수. `pass`, `fail`, `skip` 중 하나 |
| `commit` | 선택. 실행한 `git HEAD`. 작업 트리에 변경이 있으면 `+dirty`가 붙는다 |
| `environment` | 선택. 실행 환경 설명 |
| `command` | 선택. 실행 명령 |
| `finishedAt` | 선택. 종료 시각(UTC) |

선택 필드는 보고서의 연결 테스트 칸에 그대로 나온다. 같은 키가 여러 번 나오면 `fail`, `pass`, `skip` 순으로 센 쪽을 쓰고, 세기가 같으면 나중 기록을 쓴다. 같은 파일에서 같은 이름의 Python 함수를 둘 이상 쓰면 한 키로 합쳐지므로 피한다.

`tests/scenario_results.py`가 Python 쪽 결과를 남긴다. `@scenario_result` 데코레이터가 시나리오 함수가 끝날 때 `pass`·`fail`을 기록한다. 반환값과 예외는 바꾸지 않는다. 반환값이 `{"result": "FAIL"}`이면 `fail`, 예외가 나면 `fail`이다. 기본 파일은 `.artifacts/verification/scenarios/python-results.jsonl`이다. 환경 변수 `NARUMQ_SCENARIO_RESULTS`로 경로를, `NARUMQ_SCENARIO_ENVIRONMENT`로 환경 설명을 바꾼다. 현재 `tests/chaos/eks-rollout-faults/main.py`의 시나리오 함수에 적용했다. 다른 chaos 도구는 아직 기록하지 않으므로 그 시나리오는 게이트 밖이며 실행 기록을 [검증 기록](../verification/)에 남긴다.

### 7.3 시나리오 상태

| 상태 | 뜻 | 검사 실패 |
| --- | --- | --- |
| `미연결` | 이 시나리오를 가리키는 테스트 메타 정보가 없다 | 연결·실행 검사 |
| `오류` | 메타 정보나 시나리오 표에 문제가 있다. 없는 스펙 파일, 없는 ID, 형식 오류, 중복 ID, 함수 바로 위가 아닌 `Scenarios:` 줄이 해당한다. 보고서에 `파일:줄`로 나온다 | 연결·실행 검사 |
| `실패` | 연결된 테스트 중 하나라도 실패했다 | 연결·실행 검사 |
| `미실행 (결과 없음)` | 실행 범위 안의 연결 테스트에 이벤트가 없다 | 실행 검사 |
| `미실행 (건너뜀)` | 실행 범위 안의 연결 테스트가 건너뛰어졌다 | 실행 검사 |
| `미실행 (게이트 밖)` | 연결된 테스트가 실행 범위 밖이다. 결과 입력이 없는 경우, `Acceptance:` 테스트를 건너뛰었거나 이벤트가 없는 경우도 같다 | 없음 |
| `통과` | 연결된 테스트가 모두 통과했다 | 없음 |

우선순위는 `실패`, `미실행`, `통과` 순이다. 사유가 여러 개면 `결과 없음`, `건너뜀`, `게이트 밖` 순으로 보인다. 보고서의 연결 테스트 칸에는 테스트마다 `통과`, `실패`, `건너뜀`, `결과 없음`, `게이트 밖` 중 하나가 나온다. 게이트 밖 `미실행`은 이유와 영향을 [검증 기록](../verification/)에 적는다.
