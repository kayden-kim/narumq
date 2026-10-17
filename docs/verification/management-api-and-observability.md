# Management API와 observability 검증 기록

이 문서는 [Management API와 observability 스펙](../specs/management-api-and-observability.md)의 과거 실행 증거다. 아래 통과는 당시 작업 트리의 결과이며 현재 코드의 통과를 뜻하지 않는다. 현재 연결은 테스트 소스의 `Scenarios:` 메타 정보, 현재 결과는 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서를 따른다. 원기록에 대상 버전·환경·명령이 없으면 아래에 그 한계를 적었다. EKS 시험환경은 폐기했으므로 live 관찰은 현재 접속 상태가 아니다.

## 1. API-05 rate limit 집중 시험 결과(2026-10-09)

대상 시나리오: MGT-SCN-005, MGT-SCN-006, MGT-SCN-007, MGT-SCN-008, MGT-SCN-009, MGT-SCN-010, MGT-SCN-011

결과: 통과

대상 커밋·변경 범위와 환경은 원문에 기재되지 않았다. 명령은 test 이름으로만 적혀 있다.

- API-05 집중 시험 결과(2026-10-09): adapter rate-limit 시험 7개, management OpenAPI contract suite, cluster·standalone 설정 시험이 모두 통과했다. 실행 파일 smoke 시험은 인증된 요청에서 `200, 200, 429`, 잘못된 자격 증명에서 `401, 429`를 반환했다. Liveness probe 5회는 모두 `200`이었다. 두 `429` 응답은 `Retry-After: 1`을 포함했다. 범위를 벗어난 standalone rate는 exit 1과 `-http-request-rate and -http-auth-failure-rate must be between 1 and 1000000` 오류를 반환했다.

## 2. API-05 전체 검증 결과(2026-10-09)

대상 시나리오: MGT-SCN-005, MGT-SCN-006, MGT-SCN-007, MGT-SCN-008, MGT-SCN-009, MGT-SCN-010, MGT-SCN-011

결과: 통과

`go run ./tools/verify traceability`와 `go run ./tools/verify local`이 통과했다. Cluster process fixture는 다른 process 시험을 막지 않도록 두 rate를 최댓값으로 설정한다. 64개 동시 fresh-read 요청 시험도 통과했다.

## 3. SEC-11·API-08·NFR-17 수용 시험 기록(2026-10-09)

대상 시나리오: MGT-SCN-012, MGT-SCN-013, MGT-SCN-014, MGT-SCN-015

결과: 통과. 원기록의 대상 버전·환경과 명령은 없다. `TestDynamicClusterManagementAuditProcessAcceptance` 3-node 실행은 당시 활성 management mutation 8개와 401·403 거절을 확인했다. 감사 package·contract 시험은 fail-closed admission, hash-chain 손상, schema, directory 경계와 Windows rotation 충돌을 확인했다. 현재 mutation 범위는 스펙 §1.4가 기준이며 이 기록으로 확대하지 않는다.

## 4. Cluster startup probe 실행 기록(2026-10-15)

대상 시나리오: MGT-SCN-021

결과: 통과. 대상은 2026-10-15 작업 트리다. 대상 커밋과 환경은 원문에 기재되지 않았다. 명령은 test 이름으로만 적혀 있다.

- Cluster startup probe는 typed Raft recovery가 끝나기 전 `STARTING`을 반환한다. Recovery 뒤 `JOINING`이면 readiness 조건을 직접 확인한다. `cmd/narumq/cluster_storage_health_test.go::TestClusterStartupWaitsForTypedRecovery`가 typed recovery 경계를, `tests/integration/cluster_membership_process_test.go::TestDynamicClusterStartupWithoutReadinessProbeProcessAcceptance`가 readiness 요청 없는 3-node startup을 확인한다. 2026-10-15 작업 트리 실행은 각각 포함한 health 시험 PASS(2.196초)와 process 시험 PASS(19.516초)다. EKS의 수정 전 startup `503`·readiness `200` 불일치는 `.artifacts/verification/eks-resume-20261015T000337Z-21df3b/initial-startup-diagnostic.json`에 연결한다.

## 5. Dashboard 수용 시험 결과(2026-10-09)

대상 시나리오: MGT-SCN-040, MGT-SCN-041, MGT-SCN-042

결과: 통과

- Dashboard 수용 시험 결과(2026-10-09): `TestDashboardProvisioningMatchesContracts`와 opt-in Docker E2E `TestDashboardStackProcessAcceptance`가 통과했다. E2E 실행은 `NARUMQ_DASHBOARD_E2E=1`, `NARUMQ_DASHBOARD_E2E_HOLD=90s` 조건에서 test 229.14초, package 230.254초였다. 세 process의 `/health/ready` `200`, Prometheus target 3개, datasource health 2개, ReadOnly cluster GET `200`, backup·drain POST `403`, cluster traffic·protocol Metric과 완료된 drain 결과를 확인했다. Grafana `/api/ds/query`가 panel 2, 4, 5, 6, 15의 frame을 반환했다. Replica 부족은 `0`, Member·Quorum은 `3/2/3/2`, node 상태 수는 `Ready=1`·`Joining=2`, node topology는 local `READY`와 원격 node의 `JOINING`·정확한 readiness reason, drain 상세는 `COMPLETED`·`SAFE_TO_STOP`·`safeToStop=true`·remaining `0`·takeover `1`을 표시했다. Node별 query에서는 node 1 connection·received PUBLISH rate와 MQTT 3.1.1 성공, node 2 sent PUBLISH rate와 MQTT 5.0 성공·같은 series의 `not_authorized` 거절을 확인했다. 성공 protocol label은 선택한 node 밖에서 나오지 않았다.

## 6. Dashboard named API key regression(2026-10-10)

대상 시나리오: MGT-SCN-041, MGT-SCN-042

결과: 통과

- Dashboard named API key regression (2026-10-10): opt-in Docker E2E `TestDashboardStackProcessAcceptance`가 새 `management.apiKeys` 설정에서 통과했다. `NARUMQ_DASHBOARD_E2E=1`, `NARUMQ_DASHBOARD_E2E_HOLD=90s` 실행에서 test는 228.46초, package는 229.503초였다. ReadOnly credential로 cluster GET을 통과하고 backup·drain POST를 거절하는 acceptance가 통과했다.

## 7. EKS dashboard acceptance 결과(2026-10-13)

대상 시나리오: MGT-SCN-043

결과: 실패. 당시 EKS `narumq-eks`의 3 broker pod·3 AZ에서 Management API datasource와 panel 1/2는 정상(`quorumAvailable=true`, `memberCount=3`, `underReplicated=0`)이었다. 그러나 Prometheus target은 `up=0`, `net/http: invalid header field value for "X-Api-Key"`였고 panel 7은 empty frame이었다. 후속 수정 결과는 다음 절에 둔다.

## 8. EKS 제한 수정 실행 기록(2026-10-14)

대상 시나리오: MGT-SCN-043

결과: 통과. 문자열 API key와 관측 RPC를 포함한 ARM64 image로 재배포한 뒤 node 3개 `OK/READY`, replica 부족 `0`, Prometheus target 3개 `UP`, Grafana datasource 2개 `OK`, topology와 panel 7 시계열 frame을 확인했다. 환경·image digest·확인 방법·종료 hook·보안 처리와 범위는 [EKS 재배포 실행 기록](../how-to/operations-dashboard.md#eks-재배포-실행-기록-2026-10-14)에 둔다. Production 최대 부하·보안 운영 승인은 포함하지 않는다.

## 9. Node observability 실행 기록(2026-10-14)

대상 시나리오: MGT-SCN-022, MGT-SCN-023, MGT-SCN-024

결과: 통과. 대상 커밋과 환경은 원문에 기재되지 않았다.

- Node observability 실행 기록(2026-10-14): `GOMAXPROCS=2 GOFLAGS=-p=1 go test ./internal/application/observability ./internal/adapters/http/management ./tests/contracts/management-api-v1 -count=1` 통과. `GOMAXPROCS=2 GOFLAGS=-p=1 go test ./tests/integration -run '^TestDynamicClusterObservabilityProcessAcceptance$' -parallel=1 -count=1 -v`도 통과했다(시험 9.41초). 정상 3-node 조회, 강제 종료한 node의 `UNREACHABLE`·null measurement, `unknown=1`, partial unaggregated JSON, aggregated JSON·cluster text의 `503 metrics_collection_incomplete`를 확인했다.

## 10. Dashboard topology mapping 실행 기록(2026-10-14)

대상 시나리오: MGT-SCN-040, MGT-SCN-041

결과: 수정 뒤 provisioning 시험과 opt-in Docker E2E 통과.

- Dashboard topology mapping 실행 기록(2026-10-14): `GOMAXPROCS=4 GOFLAGS=-p=1 go test ./tests/integration -run '^TestDashboardProvisioningMatchesContracts$' -parallel=1 -count=1 -v` 통과했다. 첫 opt-in E2E 시도는 `/api/ds/query`의 raw boolean `nodeQueryComplete`를 숫자로 읽는 test assertion 때문에 실패했다. Assertion을 boolean으로 수정했다. Browser에서 정상 조회(`Ready=3`, `조회 완료=완료`), node 3의 not-ready 상태(`SAFE_TO_STOP`, `NOT READY`, `조회 성공`), 조회 불가 상태(`미확인`, `조회 불가`, `node_unreachable`, `unknown=1`, 일부 미확인)를 확인했다. 오른쪽 열에서 null `Replication`이 회색 `미확인`으로 표시되는 것도 확인했다. 수정 후 opt-in Docker E2E `GOMAXPROCS=4 GOFLAGS=-p=1 NARUMQ_DASHBOARD_E2E=1 NARUMQ_DASHBOARD_E2E_HOLD=15s go test ./tests/integration -run '^TestDashboardStackProcessAcceptance$' -parallel=1 -count=1 -v` 통과했다(시험 205.86초, package 207.727초).

## 11. Dashboard visual 결과(2026-10-09)

대상 시나리오: MGT-SCN-041

결과: 통과. 원문에 따라 CI 실행 링크와 production deployment 검증은 없다.

- Dashboard visual 결과(2026-10-09): browser에서 Quorum `정상`, replica 부족 `0`, Leader `1`, Members `3`, quorum size `2`, replication factor `3`, write quorum `2`를 확인했다. Node topology는 node 1 `READY`, node 2 `JOINING`과 `remote readiness is not available from this node`, node 3 `SAFE_TO_STOP`을 표시했다. `Node=2` 선택 화면에서 Connections, Sent PUBLISH, MQTT 5.0 CONNECT 성공과 CONNECT 거절 panel을 확인했다. Node load에는 세 node가 보였다. Drain panel에는 node 3 `connections_initial=1`, `connections_remaining=0`, `sessions_taken_over=1`, 상세 단계 `COMPLETED`, 상태 `SAFE_TO_STOP`, `safeToStop=true`가 표시됐다. Browser error와 console error는 없었다. CI 실행 링크와 production deployment 검증은 없다.

## 12. API-03 process 실행 증거(2026-10-09)

대상 시나리오: MGT-SCN-028

결과: 수정 전 실패, 수정 후 통과. 수정 전 실패 기록을 보존한다.

- API-03 process 실행 증거(2026-10-09): 수정 전 `go test ./tests/integration -run '^TestDynamicClusterSubscriptionGaugeProcessAcceptance$' -count=1 -v`는 96.22초 뒤 실패했다. 두 SUBSCRIBE를 commit한 뒤 node 1에서 gauge 2를 확인하지 못했다. 수정 후 `GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterSubscriptionGaugeProcessAcceptance$' -count=1 -v -timeout 20m`는 PASS했다(시험 7.80초, package 8.799초). 모든 node의 gauge `0→2→1→0`과 MQTT 5 `UNSUBACK` body `00 03 00 00`을 확인했다.

## 13. Traceability와 local gate 기록

이 절의 기록은 저장소 전체 gate다. 개별 시나리오의 통과를 대신하지 않는다. 대상 시나리오는 각 기록에 적었다.

### 13.1 전체 local gate 실행 기록(2026-10-14)

대상 시나리오: 저장소 전체

결과: 첫 실행 실패(중단), 같은 test 단독 통과 뒤 전체 gate 재실행 통과. 첫 실패 기록을 보존한다.

`GOMAXPROCS=4 GOFLAGS=-p=1 go run ./tools/verify local`의 첫 실행은 `TestDynamicClusterDurableQoSBackupRestoreProcessAcceptance`에서 Windows `compact WAL: replace WAL: Access is denied`로 실패했다. 같은 test의 `-parallel=1` 실행과 전체 gate 재실행은 통과했다. 이 과거 재실행 통과는 현재 전체 local 실패를 해소한 결과가 아니다.

### 13.2 P0 traceability gate(2026-10-09)

대상 시나리오: MGT-SCN-040, MGT-SCN-041, MGT-SCN-042와 UI-01~UI-05·UI-10 요구사항

결과: 통과

`go run ./tools/verify traceability` PASS. UI-01~UI-05·UI-10의 연결을 확인했다.

### 13.3 SEC-10 traceability gate(2026-10-10)

대상 시나리오: MGT-SCN-016, MGT-SCN-017, MGT-SCN-018, MGT-SCN-019

결과: 통과

`go run ./tools/verify traceability` PASS. SEC-10의 연결을 확인했다.

### 13.4 SEC-10 full local gate(2026-10-10)

대상 시나리오: 저장소 전체. SEC-10 변경(MGT-SCN-016~MGT-SCN-019)을 포함한다.

결과: 통과

실행 증거는 [SEC-10 검증 기록](tenancy-and-security.md#6-sec-10-실행-기록)에 둔다.

### 13.5 Repository local gate(2026-10-09)

대상 시나리오: 저장소 전체

결과: 통과

`go run ./tools/verify local` PASS. Branch instrumentation·fuzz·architecture·package·coverage·traceability를 포함한 당시 실행이다.

## 14. 미실행 기록

### 14.1 MGT-SCN-044 snapshot 한도와 collection 기한

2026-10-17 테스트 연결 전에는 snapshot 한도·worker 수·collection 기한을 직접 확인할 시험이 없어 미실행이었다. 추가한 회귀 시험과 결과는 [§15](#15-snapshot-한도와-readiness-로그-회귀-시험-2026-10-17)에 둔다.

### 14.2 MGT-SCN-045 readiness 거절 로그 필드

2026-10-17 테스트 연결 전에는 readiness 거절 로그 필드를 직접 확인할 시험이 없어 미실행이었다. 추가한 회귀 시험과 결과는 [§15](#15-snapshot-한도와-readiness-로그-회귀-시험-2026-10-17)에 둔다.

## 15. Snapshot 한도와 readiness 로그 회귀 시험 (2026-10-17)

대상 시나리오: MGT-SCN-044, MGT-SCN-045

결과: 선택한 테스트 모두 통과. Collection 기한 시험은 기본 5초와 더 짧은 caller 기한 0.1초 뒤 종료했다.

대상 범위: 시나리오 메타 정보와 회귀 테스트를 추가한 현재 작업 트리다. 제품 코드는 바꾸지 않았다. 환경은 Windows/amd64다.

확인 방법: `GOMAXPROCS=2`에서 `go test -p=2 -count=1`의 `-run`으로 아래 테스트를 선택했다.

- `internal/application/observability`: `TestMetricFamiliesFromSnapshotRejectsOverLimitInsteadOfTruncating`, `TestCollectMetricsQueriesWithAtMostEightWorkersUnderFiveSecondDeadline`, `TestCollectMetricsCancellationStopsEveryWorker`, `TestCollectMetricsTimesOutAndHonorsShorterCallerDeadline`.
- `internal/adapters/http/management`: `TestMetricsResponseOverSixtyFourMiBReturnsErrorWithoutTruncating`.
- `internal/adapters/tcp`: 기존 `TestPeerObservabilitySanitizesInvalidAndOversizedSnapshots`.
- `cmd/narumq`: `TestReadinessRejectionLogRecordsFreshProofOnlyAfterFreshReadBarrier`.

MGT-SCN-044는 schema 한도, 원격 snapshot·JSON/text 응답 크기, worker 수, collection 기한과 취소 뒤 worker 종료를 확인한다. MGT-SCN-045는 실제 admission·readiness 경로의 JSON logger를 캡처해 fresh read 성공·실패와 snapshot 판정 전 필드 누락을 확인한다.

## 16. EKS Dashboard harness 확인 (2026-10-17)

대상 시나리오: MGT-SCN-043

결과: 미실행

이유: [EKS Dashboard 실행 절차](../how-to/operations-dashboard.md#eks-재배포-실행-기록-2026-10-14)는 `kubectl`, Prometheus, Grafana API와 browser의 수동 확인을 기록한다. `.artifacts/`의 Python·PowerShell·JavaScript·TypeScript·shell 파일과 dashboard·grafana 이름을 검색했지만, 승격할 EKS probe 소스는 찾지 못했다. 남아 있는 `dashboard_stack_test.go` 사본은 Docker suite이며 이 EKS 시나리오를 실행하지 않는다.

영향: 기존 EKS 통과·실패 이력은 유지한다. 현재 시나리오 연결은 `미연결`이다. EKS 환경과 해당 환경을 확인하는 harness가 필요하다.
