# ADR-0055: Drain과 reconnect 운영 기본값

| 항목 | 내용 |
| --- | --- |
| 상태 | `Accepted` |
| 결정일 | 2026-10-11 |
| 결정자 | 사용자 |
| 관련 요구사항 | [PRD DEP-02·DEP-04·DEP-05·DEP-14, CLIENT-01~CLIENT-09](../requirements/PRD.md) |
| 관련 스펙·계약 | [Drain과 rolling update §2.1·§7](../specs/drain-and-rolling-update.md), [MQTT client compatibility v1 §1·§4](../contracts/mqtt-client-compatibility-v1.md), [Management API v1](../contracts/management-api-v1.md) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md), [EKS on EC2 deployment](../architectures/eks-ec2-deployment.md) |
| 관련 ADR | [ADR-0006](ADR-0006-external-rollout-controller.md), [ADR-0016](ADR-0016-node-drain-and-safe-to-stop.md), [ADR-0033](ADR-0033-connection-rebalance-operation.md), [ADR-0051](ADR-0051-cluster-update-strategy.md) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

Drain과 reconnect의 운영 기본값은 다음 표를 따른다. NLB 전파와 process 종료 시간은 실측하지 않았으므로 이 값들은 운영 예산이며, 실제 profile 검증을 대신하지 않는다.

| 항목 | 채택값 | 근거·적용 |
| --- | --- | --- |
| `connectionRatePerSecond` 기본값 | `100` | `timeoutMillis=600000`과 LB 대기 `30000`에서 `100 × (600 − 30) = 57000` 연결/node까지 예산을 둔다. 이를 넘는 node는 호출자가 rate 또는 timeout을 늘린다. |
| `loadBalancerPropagationDelayMillis` 기본값 | `30000` | Readiness 실패 뒤 NLB target 제외 전파를 위한 여유다. 실측 전 값이다. |
| `timeoutMillis` 기본값 | `600000` | Deadline은 `startedAt + timeoutMillis`이며 LB 대기를 포함한다([Drain 구현](../../internal/application/membership/drain.go)). |
| `terminationGracePeriodSeconds` 하한 | `ceil(timeoutMillis / 1000) + 30 + 30`; 기본값 `660` | Drain 최악 시간, `preStop` 예산 30초, SIGTERM 뒤 정상 종료 예산 30초를 합한다. 두 30초 값은 측정값이 아니다. |
| Reconnect full jitter `base` / `cap` / 최대 attempt | `1000ms` / `30000ms` / `10` | Client contract §1의 `random(0, min(cap, base × 2^attempt))`를 적용한다. |
| `maximumReconnectDuration` / `willSafetyMargin` | `60s` / `30s` | `MQTT5_DRAIN_SAFE`의 Will Delay는 `90s`보다 커야 한다. 60초 안에 재접속하지 못한 client는 Will 억제 보장 대상이 아니다. |
| `maximumRolloutDuration` / `reconnectSafetyMargin` | `3600s` / `600s` | 3-voter rolling에서 node당 grace 660초와 재시작·READY 예산 540초를 합해 1200초를 둔다. MQTT 5 Session Expiry는 `4200s`보다 커야 한다. |

Drain POST에서 `connectionRatePerSecond`, `loadBalancerPropagationDelayMillis`, `timeoutMillis`를 생략하면 위 기본값으로 채운다. 기본값을 채운 뒤 기존 범위와 `timeoutMillis > loadBalancerPropagationDelayMillis` 검증을 그대로 적용한다. 예를 들어 `loadBalancerPropagationDelayMillis=600000`만 보내면 `400 invalid_request`다.

Idempotency 비교는 기본값을 채운 뒤의 값으로 한다. 따라서 field를 생략한 요청과 같은 값을 명시한 요청은 같은 key에서 replay된다. `POST /api/v1/cluster/rebalance`의 네 field는 [ADR-0033](ADR-0033-connection-rebalance-operation.md) §1 2항에 따라 계속 필수이며 기본값을 추가하지 않는다.

Grace 공식과 client profile 값은 배포·client 문서의 기준으로 사용한다. Kubernetes admission 구현은 [TODO §1.4](../TODO.md#14-drain-정책관측과-production-배포)의 보류 항목으로 남긴다.

## 2. 배경

`SPEC-TBD-04`는 drain rate, LB 전파 대기, timeout, grace period와 reconnect 운영값을 정하지 않은 채 남아 있었다. Drain API에는 제품 기본값이 없어 호출자가 세 값을 모두 지정해야 했다. Node별 연결 수 `B_conn`과 NLB 전파 시간의 실측값도 없다.

## 3. 결정 요인

1. Drain 기본 호출이 가능해야 하며 기존 범위·deadline·idempotency 규칙을 유지해야 한다.
2. 기본값으로 감당할 수 있는 연결 수와 기본값 초과 시 조정 방법을 계산할 수 있어야 한다.
3. Client reconnect, Will Delay, Session Expiry와 node rollout 예산이 서로 맞아야 한다.
4. 측정하지 않은 NLB·종료 시간을 실측 결과처럼 표현하지 않아야 한다.
5. Rebalance 요청 계약은 별도 결정 없이 바꾸지 않아야 한다.

## 4. 선택 근거

Drain timeout은 LB 전파 대기를 포함한다. 기본 timeout 600초에서 30초를 제외하면 연결 인계에 570초를 쓸 수 있다. 초당 100개를 기준으로 하면 node당 57000개가 기본 예산이다. 더 많은 연결을 가진 node는 호출자가 rate 또는 timeout을 늘려야 한다.

Grace 하한 660초는 600초 drain 예산과 `preStop`, SIGTERM 뒤 종료에 각각 30초씩을 더한 값이다. 3-voter rollout은 node마다 grace 660초와 재시작·READY 예산 540초를 두어 1200초로 계산한다. 세 node의 총 예산은 3600초다. Reconnect 허용 60초와 안전 여유 30초, rollout 3600초와 재접속 여유 600초를 각각 MQTT Will Delay와 Session Expiry 기준으로 연결한다.

이 계산은 승인된 예산값을 일관되게 연결한다. NLB target 제외 시간, `preStop` overhead, SIGTERM 종료 시간과 실제 연결 부하를 측정한 결과는 아니다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 빠른 교체 세트: `200/s`, `15s`, `300s` | 기본 drain과 grace 시간이 짧다. | 더 높은 인계 rate를 요구하고 NLB 15초 전파와 300초 timeout을 뒷받침하는 실측이 없다. | 30초 LB 예산과 100/s 기준을 채택한다. 빠른 profile은 실측 없이 운영 기본값으로 쓰지 않는다. |
| 보수적 세트: `50/s`, `60s`, `1200s` | 인계 rate가 낮고 긴 deadline을 둔다. | 같은 node당 57000 연결 예산에 더 긴 LB·timeout·grace가 든다. | 필요한 실제 부하·전파 측정 없이 모든 배포에 긴 대기와 종료 예산을 강제하지 않는다. |
| 문서만 확정하고 API 기본값은 두지 않는다. | 기존 API 입력과 runtime 동작은 바뀌지 않는다. | 기본 drain 요청을 단순하게 만들지 못하고 API 호출자가 같은 운영값을 반복해서 지정해야 한다. | drain API에도 기본값을 적용한다. |

## 6. 결과

### 긍정적 결과

- Drain POST에서 세 운영값을 생략할 수 있다.
- 기본 연결 예산과 더 큰 node에서 rate 또는 timeout을 조정할 기준이 생긴다.
- Termination grace, reconnect backoff, Will Delay와 Session Expiry의 배포 기준이 서로 계산 가능하다.

### 부정적 결과와 비용

- 기본 timeout은 LB 대기 뒤 최대 57000 연결/node의 인계를 예산으로 둔다. 더 많은 연결은 호출자가 rate 또는 timeout을 늘려야 한다.
- NLB 전파와 종료 여유는 실측 전 예산이다. 인프라 시험 뒤 값이 바뀔 수 있다.
- API 기본값은 호출자의 명시적 입력 없이 선택되므로 OpenAPI 기본값과 idempotency 비교 의미를 함께 유지해야 한다.

### 후속 작업

- NLB 전파, SIGTERM 종료와 Kubernetes admission은 [TODO §1.1·§1.4](../TODO.md) 및 Drain spec §7의 보류 항목에서 추적한다.
- `B_conn`이 정해지거나 실측이 예산을 넘으면 값을 재검토한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| 기본 domain 값이 기존 검증을 통과한다. | `TestDefaultParametersAreValid` | `ValidateParameters`가 nil을 반환한다. | 2026-10-11 PASS: focused domain·HTTP·membership·contract 시험; `TestDefaultParametersAreValid` 전용 실행 PASS. [Drain spec §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) |
| 생략한 HTTP field에 기본값을 적용하고 명시값을 보존한다. | `TestNodeDrainHTTPAppliesProductDefaultsToOmittedFields` | 빈 body·일부 생략·명시적 0·기존 명시값에서 기대한 값으로 202를 반환한다. | 2026-10-11 PASS: `all_omitted`, `timeout_only`, `explicit_zero`, `all_explicit` 모두 PASS. [Drain spec §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) |
| OpenAPI drain 기본값과 domain 상수가 일치하고 Rebalance는 필수다. | `TestNodeDrainRequestDefaultsMatchDomain` | 세 기본값과 선택 field가 일치하고 Rebalance 네 field가 필수다. | 2026-10-11 PASS: `TestNodeDrainRequestDefaultsMatchDomain`; OpenAPI contract suite PASS. [Drain spec §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) |
| 실제 broker process가 생략 요청에 기본 timeout을 적용한다. | 임시 process test `TestZZDrainDefaultsSmoke` | POST가 202, deadline 차이가 600초, LB 대기 중 DELETE가 200이다. | 2026-10-11 PASS: `go test ./tests/integration -run '^TestZZDrainDefaultsSmoke$' -count=1 -v -timeout 10m`; test 7.72s. 시험 파일은 확인 뒤 제거했다. [Drain spec §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) |
| 명시값 drain 요청과 전체 저장소 기준을 유지한다. | Drain process 회귀와 `go run ./tools/verify local` | 지정된 process 시험과 local gate가 통과한다. | 2026-10-11 PASS: drain·cancel/timeout/resume process 회귀 13.61s·18.91s; Bash `go run ./tools/verify local` PASS(`local: PASS`, 263.74s; traceability P0 71/81, exceptions 10); 계획에 따른 PowerShell `pwsh -NoProfile -Command '$env:GOMAXPROCS = "4"; Get-Date -Format yyyy-MM-dd; go run ./tools/verify local'` PASS(`local: PASS`, 270.95s; traceability P0 71/81, exceptions 10). 첫 Bash 실행은 Windows WAL replace `Access is denied`로 실패했다. Rebalance process test 단독 재실행 PASS(16.24s). [Drain spec §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) |

## 8. 재검토 조건

- PRD §18 질문 1의 `B_conn` 값이 정해지면 기본 연결 예산과 timeout을 다시 계산한다.
- TODO §1.1·§1.4의 인프라 시험을 재개해 NLB 전파와 SIGTERM 종료 시간을 측정하면 grace와 LB 대기를 재검토한다.
- 측정값이 이 ADR의 시간 예산을 넘으면 drain, grace, reconnect와 rollout 값을 함께 재검토한다.
