# ADR-0064: CI gate는 논리 검증만 실행한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-18 |
| 결정자 | 사용자 |
| 관련 요구사항 | [TEN-09, TEST-10, TEST-15](../requirements/PRD.md) |
| 관련 스펙·계약 | [Performance와 verification §7.2·§8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate), [Tenant와 security §5.3](../specs/tenancy-and-security.md#53-noisy-neighbor-완화ten-09), [테스트 컨벤션](../conventions/test-conventions.md) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md#repository-verification-모듈-흐름) |
| 관련 ADR | [ADR-0053](ADR-0053-github-actions-required-ci-gate.md), [ADR-0054](ADR-0054-tenant-noisy-neighbor-latency-acceptance.md), [ADR-0063](ADR-0063-diff-coverage-floor-80.md) |
| 이 ADR이 대체하는 이전 ADR | 없음. ADR-0053의 필수 check 구성과 ADR-0054의 합격 기준은 바꾸지 않는다. |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

1. 기본 gate(`go run ./tools/verify local`·`ci`)와 GitHub Actions 필수 check `verify`는 논리 시험만 실행한다. 논리 시험은 합격 여부가 기능의 결과(상태, 순서, 개수, 오류, 데이터 손실 없음)로 정해지는 시험이다.
2. 부하·품질 시험은 기본 gate에서 제외한다. 부하·품질 시험은 합격 여부가 측정한 지연, 처리량, percentile, 지연 비율 한도나 부하 강도에 달린 시험이다.
3. 제외는 이미 있는 `Acceptance: <환경 변수>` opt-in 수용 시험 방식으로 한다. 시험 doc comment에 `Acceptance: NARUMQ_LOAD_QUALITY`와 `Scenarios:`를 쓰고, 시험은 그 변수가 `1`이 아니면 건너뛴다. 기본 gate는 이 시나리오를 `미실행 (게이트 밖)`으로 보고하고 실패로 처리하지 않는다. 새 build tag, 새 옵션과 새 하위 명령은 만들지 않는다.
4. 부하·품질 시험은 `NARUMQ_LOAD_QUALITY=1 go run ./tools/verify acceptance [--run <regexp>]`로 실행한다. 합격 증거는 이 명령의 `acceptance: PASS`와 `.artifacts/verification/acceptance-go-test.json`이다. 실행 결과를 [검증 기록](../verification/performance-and-verification.md#15-ci-gate-논리-검증-한정-2026-10-18)이나 해당 영역 기록에 남긴다. 실행 기록이 없는 부하·품질 시나리오는 검증되지 않은 것이다.
5. 지금 gate에서 빠지는 시험은 하나다.

   | 시험 | 시나리오 | 판정 기준 | 게이트 밖에서 실행하는 명령 |
   | --- | --- | --- | --- |
   | `tests/integration`의 `TestDynamicClusterTenantNoisyNeighborProcessAcceptance` | `tenancy-and-security.md:SEC-SCN-030`(TEN-09) | noisy 구간 victim p99가 baseline p99의 1.5배 이하 | `NARUMQ_LOAD_QUALITY=1 go run ./tools/verify acceptance --run TenantNoisyNeighbor` |

6. TEN-09 합격 기준은 바꾸지 않는다. p99, 1.5배(`2 × noisyP99 ≤ 3 × baselineP99`), 5 round, 구간당 500개 표본, round별 noisy 수신 수 범위는 [ADR-0054](ADR-0054-tenant-noisy-neighbor-latency-acceptance.md)와 [스펙 §5.3](../specs/tenancy-and-security.md#53-noisy-neighbor-완화ten-09)을 그대로 따른다.
7. 논리 시험 안의 시간 값은 gate에 남긴다. 멈춤을 잡는 제한 시간, 설정한 대기보다 짧게 끝나지 않는지 보는 최소 시간, fake clock으로 계산하는 rate 한도가 해당한다.
8. 이후 추가하는 부하·품질 시험도 같은 방식으로 표시하고 같은 환경 변수를 쓴다.

## 2. 배경

PR [#9](https://github.com/kayden-kim/narumq/pull/9)의 원격 CI [run 39077367789](https://github.com/kayden-kim/narumq/actions/runs/39077367789)에서 `TestDynamicClusterTenantNoisyNeighborProcessAcceptance`가 `TEN-09 victim p99 under noisy load = 16.932534ms, baseline p99 = 6.244284ms, ratio 2.712 > 1.5`로 실패했다. 같은 코드는 앞선 실행에서 통과했다. 같은 시험이 2026-10-15 로컬 gate에서도 1.517로 실패했다.

조사 결과는 다음과 같다. 원본은 `.artifacts/verification/ten09-flake-20261018/exp/`(`analysis.txt`, 표본 CSV, 시험 로그, 실험 소스)와 `.artifacts/verification/remote-ci-20261018-39077367789/`에 있다.

| 사실 | 근거 |
| --- | --- |
| 격리는 실패한 실행에서도 동작했다. | Round별 noisy 수신 수 범위와 noisy 연결 오류 검사는 p99 판정보다 먼저 실행되고 5 round 모두 통과했다. CI 실행의 victim 중앙값은 두 구간이 같다(run 39047813458: 4.84ms와 4.90ms). |
| 실패한 실행은 다른 시험과 겹치지 않았다. | race·fuzz 명령은 23:02:54 이전에 끝났고 이 시험은 23:04:27~23:05:03에 단독 직렬로 실행됐다. |
| CI 비율이 한도 근처에서 움직인다. | Linux runner에서 run 39034250585 0.999, 39036288101 1.062, 39045919473 0.912, 39047179768 0.903, 39047813458 1.485, 39077367789 2.712(실패). Baseline p50은 4~5ms, p99는 5.2~9.2ms다. 1.5배의 절대 여유는 3~4ms다. |
| 판정 통계량이 불안정하다. | n=500의 p99는 6번째로 큰 표본이다. 1~2초 이어지는 지연 에피소드(8~14개 표본)가 한쪽 구간에만 있으면 비율이 1.5를 넘는다. |
| noisy 부하가 원인이 아니다. | WSL2 Linux(커널 6.6, `taskset -c 0-3`)에서 변형당 10회 실행했다. Noisy tenant가 아무것도 보내지 않는 변형도 비율이 최대 1.455였다. Rate limit을 풀어 noisy를 제한 없이 보내도 victim 중앙값은 3~12%만 늘었다. 부하 중 실행은 0.892~1.671이었다. |

이 시험은 broker 결함이 아니라 실행 환경의 지연 변동으로 실패한다. 사용자는 2026-10-18에 CI gate가 논리만 검증하고 부하·품질 시험은 gate에서 빼도록 결정했다. TEN-09 합격 기준은 그대로 둔다.

## 3. 결정 요인

1. gate는 코드의 논리 결함이 있을 때만 실패해야 한다. Runner의 지연 변동이 merge를 막으면 TEST-15의 flaky 금지에 어긋난다.
2. 이미 있는 opt-in 수용 방식을 다시 쓴다. 같은 일을 하는 두 번째 방식은 만들지 않는다.
3. 시험과 합격 기준을 남기고 실행 증거를 계속 요구한다. 게이트 밖이 곧 미실행이 되면 안 된다.

## 4. 선택 근거

`Acceptance:` 방식은 이미 gate 안에서 세 가지를 보장한다. 표시한 시험은 환경 변수가 없으면 건너뛰고, 시나리오 연결은 유지되며, 보고서가 `미실행 (게이트 밖)`으로 드러낸다. `acceptance` 명령은 고른 시험이 모두 통과해야 성공하므로 건너뜀이 증거로 세지지 않는다([성능 검증 스펙 §7.2](../specs/performance-and-verification.md#72-fuzz-게이트와-수동-mutation-검증)). 부하·품질 시험에 같은 방식을 적용하면 새 도구가 필요 없다.

분류 기준은 §1의 정의다. 저장소의 모든 Go·Python 시험(`go test ./...`와 CI의 Python unittest 대상)을 이 기준으로 점검했다. gate에 있던 부하·품질 시험은 TEN-09 하나다.

경계 사례는 아래와 같다. 모두 논리 결과로 판정하므로 gate에 남긴다. 판정은 사용자가 바꿀 수 있다.

| 시험 | 시간·부하 요소 | 남기는 이유 |
| --- | --- | --- |
| `TestN1PersistentClose`(CLU-SCN-241) | CPU를 쓰는 goroutine 2개를 돌리며 leader를 kill한다. | 합격은 subscriber 종료 0과 `quorum unavailable` 로그 0이라는 논리 결과다. 시나리오는 진단 시험이라고 밝힌다. 부하 강도에 따라 결함이 드러나는지가 달라지므로 사용자 확인이 필요하다. |
| `TestMQTTResourceLimitsReleaseConnectionsGoroutinesAndMemory`(MQTT-SCN-040) | 연결·goroutine 반환 확인과 함께 heap 증가 `< 16 MiB`를 본다. | 자원 해제 한도다. 시간과 무관하고 부하 크기는 고정이다. Heap 한도만 따로 분리할 수도 있다. |
| `TestDynamicClusterNodeDrainProcessAcceptance`, `TestMQTTTenantLimits`, `TestTCPTenantPublishRateWaitThenReject`, drain worker 3개, partial packet timeout 시험 | 설정한 대기보다 일찍 끝나지 않는지 최소 시간을 본다. | 느린 host에서 실패하지 않는 하한이다. |
| `TestNodeHeldWALSyncDoesNotBlockTick`, `TestTCPTenantPublishWaitCancelledByConnectionStop`, WebSocket 종료 시험, `TestClusterProjectionReadyRequiresInitializedCurrentProjection` | 100ms~2s 안에 끝나는지 상한을 본다. | 멈춤을 잡는 상한이다. 즉시 반환하는 구현은 한참 아래다. |
| `TestDynamicClusterRetainedQoS12ProcessAcceptance`, `TestDynamicClusterSessionMetadataProcessAcceptance`, `TestDynamicClusterClientBlocklistProcessAcceptance` | setup이나 전파가 5~7초 안에 끝나는지 본다. | 만료·전파 시험 자체의 유효 조건이다. 부하 한도가 아니다. |
| `tests/benchmark/bp01`, `bp05`의 in-process 시험 | `Valid`가 송신 schedule lag이 interval 안인지 함께 본다. | 하네스의 단순 smoke다. Lag 판정이 느린 runner에서 실패하면 같은 기준으로 다시 분류한다. |

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| TEN-09 round를 5개에서 20개로 늘린다. | 모의 결과 거짓 실패가 디스크 0.12%, tmpfs·부하 0.00%이고 1.6배 위반 검출은 65~93%로 현재(62~79%) 이상이다. | 시험이 약 36초에서 약 2분으로 늘고 거짓 실패가 0이 아니다. 통계 시험이 gate에 남는다. | 사용자가 gate를 논리 검증으로 한정하기로 결정했다. |
| 비율 한도를 2.0배로 올린다. | 거짓 실패가 거의 없다. | 1.6배 위반의 검출이 5~14%로 떨어진다. | 합격선을 낮추는 변경이다. |
| p90·p95로 바꾼다. | 통계량이 안정적이다. | 한도 기준이 달라진다. | PRD TEN-09는 p99를 요구한다. |
| 실패하면 다시 실행한다. | 구현이 단순하다. | 간헐 실패를 가린다. | TEST-15가 허용하지 않는다. |
| 시험을 삭제하거나 영구 `t.Skip`을 둔다. | gate가 가볍다. | 실행 증거와 시나리오 연결이 사라진다. | 증거를 계속 요구한다. |
| 별도 build tag나 `verify` 하위 명령을 만든다. | 분류가 눈에 띈다. | 같은 일을 하는 방식이 둘이 된다. | 기존 `Acceptance:` 방식을 재사용한다. |
| 이 시험만 직렬 단계로 실행한다. | 다른 시험의 부하가 없다. | 실패한 실행은 이미 단독이었다. gate에 직렬 단계가 없다. | 원인에 해당하지 않는다. |

## 6. 결과

### 긍정적 결과

- gate 결과가 runner의 지연 변동에 좌우되지 않는다.
- 부하·품질 시험은 기존 수용 시험과 같은 방식으로 표시하고 실행하고 기록한다.
- gate 시간이 TEN-09 한 개 분량(약 36초) 줄어든다.

### 부정적 결과와 비용

- PR CI는 tenant rate limit 경로의 지연 회귀를 잡지 못한다. TEN-09 evidence는 acceptance 실행 기록에 의존한다.
- acceptance 실행은 사람이 시작한다. 자동 주기는 없다.
- 부하·품질 시험인지 판단하는 일이 시험을 추가할 때마다 필요하다. 경계 사례는 §4를 따른다.
- 이 이동은 flaky 격리가 아니다. 시험은 결함이 있어서가 아니라 판정 종류가 논리가 아니어서 gate 밖에 있다. 임시 조치가 아니므로 owner와 기한을 두지 않는다. 대신 §1.4의 실행 기록을 요구한다.

### 후속 작업

- `internal/application/tenantlimit`, `internal/adapters/tcp/server_tenant_limits.go`, tenant rate 설정을 바꾼 변경과 release 전에 TEN-09 acceptance를 실행하고 [Tenant와 security 검증 기록 §9](../verification/tenancy-and-security.md#9-ten-09-noisy-neighbor-실행-기록)에 결과를 남긴다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| 기본 gate가 TEN-09를 실행하지 않고 보고한다. | `go run ./tools/verify ci --base origin/main`과 시나리오 보고서 | `ci: PASS`. 시험은 `SKIP`이고 `SEC-SCN-030`은 `미실행 (게이트 밖)`이며 `미연결`·`오류`는 0이다. | [성능 검증 기록 §15](../verification/performance-and-verification.md#15-ci-gate-논리-검증-한정-2026-10-18) |
| acceptance 단계가 TEN-09를 실행한다. | `NARUMQ_LOAD_QUALITY=1 go run ./tools/verify acceptance --run TenantNoisyNeighbor` | `acceptance: PASS` | 같은 절 |
| 환경 변수가 없으면 acceptance가 실행하지 않는다. | 변수 없이 같은 명령 실행 | 시험을 시작하지 않고 `NARUMQ_LOAD_QUALITY=1`을 요구하며 exit 1 | 같은 절 |
| diff coverage 하한을 유지한다. | `ci` 안의 coverage 판정 | 세 diff 지표 80% 이상 | 같은 절 |

## 8. 재검토 조건

- 부하·품질 시험을 주기적으로 자동 실행해야 하는 경우
- §4의 경계 사례에서 시간·부하 요소 때문에 실패가 기록되는 경우
- 사용자가 TEN-09 합격 기준이나 gate 범위를 다시 정하는 경우
