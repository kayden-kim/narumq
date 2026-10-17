# 실행 옵션과 설정 검증 기록

이 문서는 [실행 옵션과 설정 스펙](../specs/configuration-and-runtime-options.md)의 과거 실행 증거다. 아래 통과는 당시 작업 트리의 결과이며 현재 코드의 통과를 뜻하지 않는다. 현재 연결은 테스트 소스의 `Scenarios:` 메타 정보, 현재 결과는 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서를 따른다. 대상 커밋·날짜가 없는 실행은 당시 기록에 그 정보가 없었다.

## 1. 시나리오별 기록 색인

시나리오 범위는 각 실행 기록에 적었다. 시험 위치와 결과를 나열한 옛 색인은 테스트 소스의 연결 정보와 중복되어 생략한다.

## 2. 실행 기록

<a id="configuration-test-execution"></a>
### 설정 시험 실행 기록

대상 시나리오: CFG-SCN-01~CFG-SCN-19

실행 환경은 Windows/amd64, Go 1.26.5다. 다음 명령은 `cmd/narumq`의 설정·시작 경계 시험을 실행했다.

```sh
go test ./cmd/narumq -count=1 -run '^(TestHashPasswordFlagIsExclusive|TestClusterConfigFlagIsExclusive|TestRaftPeerOnlyRejectsOtherFlags|TestAuditVerifyFlagRequiresExclusiveDirectory|TestMainNoListenerPath|TestRun_Defaults|TestRunWithContextMQTTSecurityMode|TestRunRejectsRuntimeMetricsFlagCombinations|TestRunWithContextRejectsConstructionBounds|TestParseClusterConfig.*|TestValidateClusterConfig.*|TestClusterManagementAccessConfiguration|TestRunWithContextClusterConfigLoading|TestParseSecurityConfig.*|TestParseRaftPeerConfig.*|TestRejectDuplicateJSONKeys|TestConfiguredTenantQuotaProfileUsesBuiltinOrStrictConfig|TestClusterTenantQuotaControllerLogsCommittedMismatchOnce|TestResolveBuildIdentity|TestClusterMQTTListenersBindConfiguredEndpointsAndClosePartialOpen|TestRunRaftPeerOnlyStartupFailuresReleaseResources|TestRunRejectsManagementRateFlagsOutOfRange|TestValidateTLSAuthenticationBindings.*|TestRunWithContextManagementAuthTokenFile|TestRunWithContextReportsManagementListenFailure)$' -v
```

결과는 `PASS`다. 설정·시작 경계 시험과 G9 token-file 시험이 통과했다.

CLI 기본 경로도 smoke 실행했다. `go run ./cmd/narumq`는 `MQTT TCP and management HTTP listeners disabled`를 기록하고 정상 종료했다.

중복 key 시작 거절 smoke도 실행했다. Security: `go run ./cmd/narumq -tcp-listen 127.0.0.1:0 -security-config .artifacts/verification/g4-duplicate-key/dup-security.json`은 종료 코드 1과 `load security config: check security config JSON keys: duplicate JSON key`를 반환했다. Peer-only: `go run ./cmd/narumq -raft-peer-config .artifacts/verification/g4-duplicate-key/dup-peer.json`은 종료 코드 1과 `load raft peer config: check raft peer config JSON keys: duplicate JSON key`를 반환했다. 코드에서 security 설정 적재는 `openMQTTListener`보다 먼저, peer config 적재는 `net.Listen`보다 먼저 실행됨을 확인했다.

G9 CLI smoke에서 `go run ./cmd/narumq -http-listen 127.0.0.1:18080 -http-auth-token x`는 종료 코드 1과 `flag provided but not defined: -http-auth-token`을 반환했다. `.artifacts/verification/g9-token-file/management.token`으로 시작한 server는 인증 없는 `/api/v1/cluster` 요청에 `401`, 파일 token의 `Authorization: Bearer` 요청에 `200`을 반환했다. 종료한 process의 log에 token 값은 없었다.

<a id="cluster-mqtt-tls-g2-g10-execution"></a>
### Cluster MQTT TLS(G2·G10) 실행 기록

대상 시나리오: CFG-SCN-20~CFG-SCN-22

실행 환경은 Windows/amd64, Go 1.26.5다. Parser·runtime 시험과 build는 다음 명령으로 확인했다.

```sh
go test ./cmd/narumq -count=1 -run '^(TestParseClusterConfig.*|TestValidateClusterConfig.*|TestClusterMQTTListeners.*|TestClusterMQTTSecurityValidatesTLSClientAuthBindings|TestValidateTLSAuthenticationBindings.*|TestRunWithContextMQTTSecurityMode)$' && go build ./...
```

결과는 `PASS`다. BP-02 fixture와 전체 build도 확인했다.

```sh
go test ./tests/benchmark/bp02 -count=1 -run '^TestBP02Fixture' && go build ./...
```

결과는 `PASS`다. BP02 fixture 시험은 node별 MQTT server certificate/key가 peer mTLS 인증서와 다른지, serverAuth 용도와 CA·DNS 검증이 맞는지 확인했다. `go build ./...`도 통과했다. Compare와 suite는 인증서 분리 변경 뒤 다시 실행하지 않았다.

3-node process acceptance는 다음 명령으로 실행했다.

```sh
go test ./tests/integration -count=1 -timeout 15m -v -run '^TestDynamicClusterMQTTTLSClientCertificateProcessAcceptance$'
```

결과는 `PASS`다. 세 node 모두 MQTT 3.1.1과 MQTT 5에서 TLS 1.3 mTLS 연결을 받아 `CONNACK=0x00`을 반환했다. MQTT server certificate는 peer certificate와 달랐다. Client certificate가 없거나 TLS 1.2를 사용하면 handshake가 실패했다. 미등록 certificate와 허용되지 않은 Client ID는 MQTT 3.1.1에서 각각 `CONNACK=0x05`, MQTT 5에서 각각 `CONNACK=0x87`을 반환했다.

CLI startup rejection smoke도 실행했다.

```sh
go run ./cmd/narumq -cluster-config .artifacts/verification/g2-g10-mqtt-tls/missing-tls.json
go run ./cmd/narumq -cluster-config .artifacts/verification/g2-g10-mqtt-tls/invalid-client-auth.json
```

두 명령 모두 종료 코드 `1`을 반환했다. 첫 번째는 `load cluster config: mqtt tls is required when tlsListenAddress is set`, 두 번째는 `load cluster config: mqtt tls clientAuth must be none, request, or require`를 기록했다.

저장소 gate는 `GOMAXPROCS=4 go run ./tools/verify local`에서 `local: PASS`, `coverage: PASS`였다.

<a id="cluster-runtime-metrics-g6-execution"></a>
### Cluster runtime metrics(G6) 실행 기록

대상 시나리오: CFG-SCN-23~CFG-SCN-24

실행 환경은 Windows/amd64, Go 1.26.5다. Parser와 3-node in-process runtime 시험을 실행했다.

```sh
go test ./cmd/narumq -run 'TestParseClusterConfigRuntimeMetrics|TestRunClusterModeInProcess' -count=1 -v
go test ./cmd/narumq -run 'TestParseClusterConfig|TestClusterConfigFlagIsExclusive|TestRunRejectsRuntimeMetricsFlagCombinations|TestRunWithContextClusterConfigLoading' -count=1
go build ./...
go vet ./cmd/narumq
go run ./tools/verify traceability
go run ./tools/verify markdown-links
go run ./tools/verify local
```

| 확인 | 결과 |
| --- | --- |
| `TestParseClusterConfigRuntimeMetrics`와 `TestRunClusterModeInProcess` | 통과. Parser 사례가 모두 통과했고, node 1의 JSONL 파일에서 `schemaVersion` 1 표본 2개 이상을 읽었다. |
| 설정 parser와 cluster flag 회귀 | 통과 |
| Build와 vet | 통과 |
| Local gate | 위 `go run ./tools/verify local`에서 `local: PASS`, `coverage: PASS`. |


<a id="api-09-mqttlimits-execution"></a>
### API-09 mqttLimits 실행 기록

대상 시나리오: CFG-SCN-25~CFG-SCN-26, CFG-SCN-28

실행 환경은 Windows/amd64, Go 1.26.5다. 아래 명령은 현재 작업 트리에서 실행했다.

```sh
go test ./cmd/narumq -run '^(TestParseClusterConfigClusterPolicy|TestClusterTenantQuotaControllerLogsCommittedMismatchOnce)$' -count=1 -v
go test ./cmd/narumq ./internal/adapters/metrics ./internal/adapters/http/management ./tests/contracts/management-api-v1 -count=1
go build ./...
go test ./tests/integration -run '^TestDynamicClusterPolicyProcessAcceptance$' -count=1 -timeout=20m
go test ./tests/integration -run '^(TestDynamicClusterClientBlocklistProcessAcceptance|TestDynamicClusterMQTTTLSClientCertificateProcessAcceptance)$' -count=1 -v -timeout 20m
go test ./internal/adapters/tcp -count=1 -run '^(TestServerSetConnectionLimitsAppliesToNewConnectionsOnly|TestTCPConnAckAboveClientMaximumPacketSizeClosesWithoutSending)$'
go test ./tests/contracts/management-api-v1 ./tests/contracts/audit-event-v1 -count=1
go run ./tools/verify traceability
go run ./tools/verify markdown-links
go run ./tools/verify local
```

| 확인 | 결과 |
| --- | --- |
| Parser, reconciliation, snapshot/backup, Management API, audit와 OpenAPI contract package 시험 | `PASS`; `cmd/narumq`, `metrics`, `management`, `management-api-v1` 모두 통과 |
| Build | `PASS`; `go build ./...` |
| CFG-SCN-28 3-node process acceptance | `PASS`; policy PUT/retry/conflict, old/new connection limits, all-node state, restart와 drift |
| Existing TCP limit regression | `PASS`; connection limit changes affect new connections only, oversized CONNACK rejection |
| Existing cluster security regressions | `PASS`; client blocklist와 MQTT TLS client certificate |
| Audit event contract regression | `PASS`; optional policy service가 포함된 router |
| Local gate | 위 `go run ./tools/verify local`에서 `local: PASS`, `coverage: PASS`. |

<a id="cluster-plaintext-tcp-g3-execution"></a>
### Cluster 평문 TCP(G3) 실행 기록

대상 시나리오: CFG-SCN-29

실행 환경은 Windows/amd64, Go 1.26.5다. Production 코드는 바꾸지 않고 parser 시험을 추가한 작업 트리다. CI 비교 기준은 `84d3a41b8b6feede6ae2856508c111c5f61674fd`였다.

```sh
go test ./cmd/narumq -count=1 -v -run '^(TestParseClusterConfigAcceptsNonLoopbackMQTTTCPListener|TestValidateClusterConfigReportsFirstInvalidField)$'
go test ./cmd/narumq -count=1 -run '^(TestParseClusterConfig.*|TestValidateClusterConfig.*)$'
go test ./internal/application/connect -count=1 -run '^(TestConnectRecordsAuthenticationDenialsWithoutChangingConnAck|TestCmmConnectRejectsCredentialModeMismatchesWithoutMutation)$'
go test ./tests/integration -count=1 -timeout 15m -v -run '^TestDynamicClusterPasswordAuthenticationProcessAcceptance$'
go build ./...
go run ./tools/verify markdown-links
go run ./tools/verify traceability
go run ./tools/verify local
go run ./tools/verify ci --base 84d3a41b8b6feede6ae2856508c111c5f61674fd
```

| 확인 | 결과 |
| --- | --- |
| Non-loopback parser와 security config 필수 경계 | 통과. Wildcard IPv4·IPv6, IPv4 주소와 DNS 주소 4개 subtest 및 `missing security file`이 통과했다. |
| Cluster config parser 회귀 | 통과 |
| CONNECT 인증 거절 회귀 | 통과 |
| 3-node 평문 TCP password acceptance | 통과 |
| Build | 통과 |
| Local gate | `local: PASS`, `coverage: PASS` |
| CI gate | 위 `--base 84d3a41b8b6feede6ae2856508c111c5f61674fd` 명령에서 `ci: PASS` |


<a id="g5-duration-millis-execution"></a>
### G5 duration Millis 실행 기록

대상 시나리오: CFG-SCN-30

실행 환경은 Windows/amd64, Go 1.26.5다. JSON duration key 변경, API field rename, 현재·이전 release 경계를 확인했다.

```sh
go test ./cmd/narumq -count=1 -v -run '^(TestParseSecurityConfig_Rejections|TestParseSecurityConfig_TenantLimitsMaxWaitMillis|TestParseSecurityConfig_TenantLimitsRejectInvalidProfiles|TestClusterManagementAccessConfiguration|TestPositiveMillisDuration)$'
go test ./cmd/narumq -count=1
go test ./internal/adapters/http/management ./internal/application/management ./tests/contracts/... -count=1 -v -run 'Drain|Rebalance|Contract|Node'
go test ./tests/benchmark/bp02 ./tests/benchmark/bp03 ./tests/benchmark/bp04 ./tests/benchmark/bp06 ./tests/benchmark/bp07 ./tests/chaos/cluster-storage-v1 ./tests/compatibility/mqtt-clients-v1 -count=1
go build ./... && go vet ./...
go test ./tests/integration -count=1 -timeout 30m -v -run '^(TestDynamicClusterManagementRBACProcessAcceptance|TestDynamicClusterCredentialExpiryProcessAcceptance|TestDynamicClusterNodeDrainProcessAcceptance|TestDynamicClusterConnectionRebalanceProcessAcceptance|TestDynamicClusterManagementAuditProcessAcceptance)$'
NARUMQ_RELEASE_COMPAT=1 GOMAXPROCS=4 go test ./tests/integration -count=1 -timeout 30m -v -run '^TestDynamicClusterReleaseCompatibilityProcessAcceptance$'
NARUMQ_RELEASE_COMPAT=1 GOMAXPROCS=4 go test ./tests/integration -count=1 -timeout 30m -v -run '^(TestDynamicClusterReleaseCompatibilityProcessAcceptance|TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance|TestClusterBackupRestoreHistoricalReleaseProcessAcceptance)$'
NARUMQ_RELEASE_COMPAT=1 GOMAXPROCS=4 go test ./tests/integration -run '^(TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance|TestClusterBackupRestoreHistoricalReleaseProcessAcceptance)$' -count=1 -v -timeout 30m
NARUMQ_RELEASE_COMPAT=1 GOMAXPROCS=4 go test ./tests/integration -run '^(TestDynamicClusterReleaseCompatibilityProcessAcceptance|TestClusterBackupRestoreReleaseWindowProcessAcceptance)$' -count=1 -v -timeout 30m
go test ./tests/integration -count=1 -run '^TestDashboardProvisioningMatchesContracts$'
go run ./cmd/narumq -tcp-listen 127.0.0.1:18831 -security-config .artifacts/verification/g5-duration-millis/legacy-security.json
go run ./cmd/narumq -tcp-listen 127.0.0.1:18832 -security-config .artifacts/verification/g5-duration-millis/current-security.json
```
Historical source는 `58a95f801e1467379db2eb6f3454ae44e954418a`다. Legacy fixture 변환에서 이전 binary가 지원하지 않는 `requestRate`·`authFailureRate`를 제거한 뒤 historical acceptance가 통과했다. Dashboard provisioning은 기준 commit `23005e1a0d819a357507ce173b8d0f89cf40cf19`와 대조해 datasource/query 형식을 유지하고 duration selector만 바꿨다.

| 확인 | 결과 |
| --- | --- |
| Millis parser와 security config 경계 | 통과. 0·최대값, 새 key, legacy key, 문자열·음수·상한 초과를 확인했다. |
| `cmd/narumq` 전체 시험 | 통과 |
| Management API와 contract regression | 통과 |
| Migrated benchmark·chaos·compatibility fixture 시험 | 통과 |
| Build와 vet | 기존 build·vet 통과. 이번 변경의 `go vet ./tests/integration`도 통과했다. |
| Management RBAC, credential expiry, drain, rebalance, audit process acceptance | 통과 |
| N-1/N release compatibility process acceptance | 통과; release window archive restore 포함 |
| Historical release acceptance | 통과. 위 historical source에서 archive의 `HEAD -> historical N-1: non-zero exit and target directory absent`를 확인했다. |
| Dashboard provisioning contract | 통과. Panel 6의 datasource/query 형식은 기준 checkout과 같고 `uptimeMs` selector만 `uptimeMillis`로 바꿨다. |
| Legacy JSON CLI smoke | 예상대로 종료 코드 1과 `unknown field "cacheTtl"`을 반환했다. |
| Current JSON CLI smoke | Listener 시작 log를 확인했다. Managed process로 종료했다. |
| Local gate | 통과; coverage 포함 |
| CI gate | 통과; coverage PASS, mutation efficacy `100%`, mutant coverage `98.85%`(6 not covered) |

<a id="g7-config-view-and-tenant-quotas-execution"></a>
### G7 config view·ClusterPolicy tenant quota 실행 기록

대상 시나리오: CFG-SCN-27, CFG-SCN-31~CFG-SCN-38

실행 환경은 Windows/amd64, Go 1.26.5다. 이 기록은 적용 설정 조회, node-local route 경계, ClusterPolicy quota feature gate와 precedence를 확인한다.
검증 대상은 G7에서 수정한 작업 트리다. 이 작업에서 commit은 만들지 않았다.

`Policy.Rules()`의 복사 동작과 process 응답 ACL 규칙을 확인하는 시험을 포함했다.

```sh
go test ./internal/domain/security ./internal/application/membership ./internal/adapters/raft/records ./internal/adapters/http/management ./cmd/narumq -count=1
go test ./tests/contracts/... -count=1
go test ./tests/integration -run '^(TestDynamicClusterNodeConfigurationProcessAcceptance|TestDynamicClusterPolicyProcessAcceptance|TestDynamicClusterPolicyTenantQuotasProcessAcceptance|TestDynamicClusterTenantStorageQuotaProcessAcceptance|TestDynamicClusterManagementRBACProcessAcceptance)$' -count=1 -timeout=30m
go run ./tools/verify markdown-links
go run ./tools/verify local
```

| 확인 | 결과 |
| --- | --- |
| Domain·membership·Raft records·Management API·configuration parser 시험 | 통과 (`go test ./internal/domain/security ./internal/application/membership ./internal/adapters/raft/records ./internal/adapters/http/management ./cmd/narumq -count=1`) |
| Contract suite | 통과 (`go test ./tests/contracts/... -count=1`) |
| G7 node configuration·policy·tenant quota process acceptance | 선택한 5개 시험 통과 |
| Local gate | 통과; coverage 포함 |
| CI gate | 미실행. G7 local gate를 실행했다. 전체 CI merge gate는 `docs/TODO.md` §1.9에서 별도로 추적한다. |

#### ACL 응답 계약 보정 실행 기록

초기 config response는 ACL 규칙 수만 반환했다. 승인된 A1/A2 계약은 `security.identityCounts`와 설정 순서의 `security.acl[]` 항목을 요구한다. MQTT JWT identity 수는 `identityCounts.jwt`에 두고, JWT configuration에서는 count를 제외한다. Management OIDC 응답은 바꾸지 않았다.

다음 검증은 이 계약 보정 뒤 현재 작업 트리에서 실행했다.

```sh
go test ./internal/domain/security ./internal/application/membership ./internal/adapters/raft/records ./internal/adapters/http/management ./cmd/narumq -count=1
go test ./tests/contracts/... -count=1
go test ./tests/integration -run '^TestDynamicClusterNodeConfigurationProcessAcceptance$' -count=1 -timeout=30m
go run ./tools/verify markdown-links
go run ./tools/verify local
```

| 확인 | 결과 |
| --- | --- |
| Focused package suites | 통과. `Policy.Rules()`의 복사·순서, runtime ACL 매핑과 Management adapter를 포함한다. |
| Management contract suite | 통과. OpenAPI에서 새 `identityCounts`와 ACL rule array 응답을 확인했다. |
| Node configuration process acceptance | 통과. 58개 ACL rule의 첫 항목·마지막 항목, identity count와 private key redaction을 확인했다. |
| Local gate | 통과; coverage 포함 |
| CI gate | 미실행. 전체 CI merge gate는 `docs/TODO.md` §1.9에서 별도로 추적한다. |

<a id="g8-standalone-identifiers-execution"></a>
### G8 standalone 식별자 실행 기록

대상 시나리오: CFG-SCN-39

실행 환경: Windows/amd64, Go 1.26.5. 아래 검증은 현재 작업 트리에서 실행했다.

민감도 확인에서는 `cmd/narumq/main.go`의 `NodeID`를 임시로 `narumq-node-2`로 바꿔 시험이 실패하는지 확인했다. 이후 `narumq-node-1`로 복구하고 시험을 다시 실행했다.

CLI smoke service는 아래 명령으로 시작했다. 응답을 확인한 뒤 service를 종료했다.

```sh
go test ./cmd/narumq -run '^TestRunWithContextServesManagementHTTP$' -count=1 -v
# 같은 명령을 NodeID 임시 변경 상태와 복구 뒤에도 실행했다.
# 다음 명령은 background service로 시작하고 HTTP 확인 뒤 종료한다.
go run ./cmd/narumq -tcp-listen 127.0.0.1:18883 -allow-insecure-anonymous-mqtt -http-listen 127.0.0.1:18081
curl.exe -sS http://127.0.0.1:18081/api/v1/cluster
curl.exe -sS -o NUL -w "%{http_code}\n" http://127.0.0.1:18081/api/v1/nodes/1
go run ./tools/verify markdown-links
go run ./tools/verify local
```

| 확인 | 결과 |
| --- | --- |
| 집중 시험 | 통과. Cluster ID와 leader node ID, `narumq-node-1`의 node 응답과 decimal ID `1`의 `404`를 확인했다. |
| 민감도 확인 | 통과. `NodeID`를 `narumq-node-2`로 바꾼 상태에서 시험이 leader node ID 불일치로 실패했다. 값을 복구한 뒤 시험이 통과했다. |
| Standalone CLI smoke | 통과. `/api/v1/cluster`는 `narumq-cluster-1`과 `narumq-node-1`을 반환했고 `/api/v1/nodes/1`은 `404`를 반환했다. 확인 뒤 process를 종료했다. |
| Local gate | `local: PASS`, `coverage: PASS` |
| CI gate | 미실행. 전체 CI merge gate는 `docs/TODO.md` §1.9에서 별도로 추적한다. |

<a id="websocket-execution"></a>
### MQTT-10 WebSocket/WSS 실행 기록

대상 시나리오: CFG-SCN-40~CFG-SCN-47

실행 환경은 Windows/amd64, Go 1.26.5다. 검증은 현재 작업 트리에서 실행했다. CI는 실행하지 않았다.

```sh
go test ./cmd/narumq -run '^TestRunWithContextStartsAndStopsWebSocketListeners$' -count=1 -v
go test ./internal/adapters/websocket ./internal/adapters/tcp ./internal/adapters/metrics ./internal/adapters/http/management ./cmd/narumq -count=1 -race
go test ./tests/integration -run 'WebSocket' -count=1 -v
go test ./tests/integration -run '^TestDynamicClusterWebSocketProcessAcceptance$' -count=1 -v -timeout 10m
go build ./... && go vet ./...
go run ./tools/verify traceability
go run ./tools/verify markdown-links
GOMAXPROCS=4 go run ./tools/verify local
```

| 확인 | 결과 |
| --- | --- |
| Standalone listener startup | 통과. WS와 WSS loopback socket이 연결을 받았고 context 취소 뒤 `runWithContext`가 nil을 반환했다. |
| Adapter·composition race suite | 통과. WebSocket, TCP, metrics, management adapter와 `cmd/narumq` 시험이 race detector를 통과했다. |
| WebSocket wire integration | 통과. MQTT 3.1.1/5, cross-transport, password, mTLS, keepalive, blocklist, frame boundary, live close, admission·metrics 시나리오를 확인했다. mTLS에서 인증서가 없는 client의 TLS handshake warning은 예상한 거절 로그다. |
| Dynamic cluster process acceptance | 통과. 세 node 사이 WS publish/subscribe와 applied-config 응답을 확인했다. |
| Real binary smoke | 통과. `go build -o .artifacts/mqtt10-ws-smoke/narumq.exe ./cmd/narumq`로 binary를 만들었다. `.artifacts/mqtt10-ws-smoke/narumq.exe -ws-listen 127.0.0.1:18083 -security-config .artifacts/mqtt10-ws-smoke/security.json`을 background service로 시작했다. `go run .artifacts/mqtt10-ws-smoke/client.go`는 `CONNACK return code=0`을 받았다. `.artifacts/mqtt10-ws-smoke/narumq.exe -ws-listen 0.0.0.0:18083 -security-config .artifacts/mqtt10-ws-smoke/security.json`은 종료 코드 1과 `-ws-listen must use literal loopback IP 127.0.0.0/8 or ::1`을 반환했다. 임시 binary, security config, client source는 smoke 뒤 제거했으므로 명령을 그대로 다시 실행할 수는 없다. |
| Build·vet | 통과 (`go build ./... && go vet ./...`). |
| Local gate | `local: PASS`, `coverage: PASS` |
| CI gate | 미실행. 저장소 전체 CI merge gate는 `docs/TODO.md` §1.9에서 별도로 추적한다. |
