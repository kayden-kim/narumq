# Tenant와 security 검증 기록

이 문서는 [Tenant와 security 스펙](../specs/tenancy-and-security.md)의 과거 실행 증거다. 아래 통과는 당시 작업 트리의 결과이며 현재 코드의 통과를 뜻하지 않는다. 현재 연결은 테스트 소스의 `Scenarios:` 메타 정보, 현재 결과는 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서를 따른다. 별도 commit이 없는 실행은 해당 날짜의 작업 트리 대상이며, 기록에 없는 버전·환경은 추정하지 않았다.

## 1. TEN-06·TEN-07 실행 기록

대상 시나리오: SEC-SCN-024, SEC-SCN-025, SEC-SCN-026, SEC-SCN-027, SEC-SCN-028

2026-10-05 Windows x64, Go 1.26.5의 작업 트리에서 실행했다.

| 검증 | 명령 | 결과 |
| --- | --- | --- |
| 빌드·vet와 수용 시험 | 수용 명령 A(아래) | PASS; MQTT 5 하위 시험 6개와 실제 binary 3-node 시험 |
| 변경 package 회귀 | `go test ./internal/application/tenantlimit ./internal/adapters/memory/sessionstore ./internal/domain/routing ./internal/application/connect ./internal/application/subscribe ./internal/adapters/tcp ./internal/adapters/metrics ./cmd/narumq -count=1` | 8개 package PASS |
| 기존 보안·listener·저장 quota 회귀 | 회귀 명령 B(아래) | PASS |
| Local subscription 경쟁 재현 | `go test ./internal/application/subscribe -run '^TestSubscribeTenantQuotaSerializes' -count=1 -v` | **수정 전 FAIL**; 한도 1에서 두 add가 모두 성공하고 대기 중 replacement가 완료된 remove를 덮어썼다. |
| 직렬화 보완 뒤 회귀·race·수용 | 직렬화 명령 C(아래) | PASS |
| 최종 전체 local gate | `GOMAXPROCS=4 go run ./tools/verify local` | PASS; coverage 포함 |

초기 local 실행의 benchmark restart sampler `exit status 1` 실패는 `GOMAXPROCS=4 go test ./tests/benchmark/internal/environment -run '^TestRunClusterTargetKeepsPublishedOwnerPortAcrossBrokerRestart$' -count=1 -v` 단독 실행에서 통과했다. 원인은 확정하지 않았다. Timer 보존 시험은 `SessionKey`별 비교로 고친 뒤 `go test ./internal/application/connect -run '^TestConnectTenantQuotaPreservesDetachedTimers$' -count=100`을 통과했다.

최종 coverage 증거는 `.artifacts/verification/coverage.out`과 `.artifacts/verification/coverage-summary.json`이다. 전체 line coverage는 85.97%, function coverage는 97.40%이며 core package 기준도 통과했다. Threshold는 바꾸지 않았다. Branch coverage는 승인된 instrumentation 도구가 설정되지 않아 `not_evaluated`다. 이번 결과는 local 실행 증거이며 CI·release의 branch coverage gate를 대신하지 않는다.

수용 명령 A:

```sh
go build ./... && go vet ./... && GOMAXPROCS=4 go test ./tests/integration -run '^(TestMQTTTenantLimits|TestDynamicClusterTenantLimitsProcessAcceptance)$' -count=1 -v -timeout 20m
```

회귀 명령 B:

```sh
GOMAXPROCS=4 go test ./tests/integration -run '^(TestMQTTSecurity|TestMQTTListenerAdmission.*|TestDynamicClusterTenantStorageQuotaProcessAcceptance)$' -count=1 -v -timeout 20m
```

직렬화 명령 C:

```sh
go build ./... &&
go test ./internal/application/subscribe -run '^TestSubscribeTenantQuotaSerializes' -count=1 -v &&
go test -race ./internal/application/subscribe -count=1 &&
GOMAXPROCS=4 go test ./tests/integration -run '^(TestMQTTTenantLimits|TestDynamicClusterTenantLimitsProcessAcceptance)$' -count=1 -v -timeout 20m
```

MQTT 5 시험은 연결 상한 2, session 상한 3, subscription 상한 2를 확인했다. 17-byte payload는 상한 16을 넘어 PUBACK `0x97`로 거절되며 subscriber에 전달되지 않았다. Rate 시험은 실제 clock과 여러 연결의 tenant 공통 bucket을 사용했다. Rate 2/s와 `maxWait=1s`에서 burst 2건 뒤의 성공 ACK는 400ms 이상 늦었고, 추가 3건 중 1건은 `0x97`로 거절됐다. Tenant-b의 override 0은 tenant-a의 한도와 독립적으로 동작했다.

별도 TCP 시험은 고정된 policy clock과 실제 timer로 같은 연결의 세 번째 ACK 지연과 다섯 번째 `0x97`을 확인한다. QoS별 MQTT 5·3.1.1 응답, pending QoS 2 DUP의 과금 제외, shutdown 중 대기 취소, Receive Maximum 우선순위와 거절 ACK 뒤 슬롯 회수를 함께 확인한다.

실제 binary 시험은 committed subscription projection을 세 node에서 확인한 뒤 node3의 SUBACK `0x80`을 확인했다. Node1에서 CONNACK `0x03`으로 거절된 client가 node2에서는 연결됐다. 각 거절 node의 `/metrics` counter가 1이었다.

## 2. SEC-11·NFR-17 실행 기록

대상 시나리오: SEC-SCN-046, SEC-SCN-049, SEC-SCN-050

2026-10-08 저장소 루트에서 Windows x64와 Go 1.26.5로 실행했다.

| 검증 | 명령 | 결과 |
| --- | --- | --- |
| 감사 회귀 package | `go test ./internal/application/audit ./internal/adapters/http/management ./internal/adapters/auditlog ./cmd/narumq ./tests/contracts/audit-event-v1 ./internal/adapters/metrics -count=1` | PASS. 6개 package. 중첩된 잘못된 UTF-8과 unexported embedded struct의 exported field, panic의 HTTP 상태 500, Windows rotation 대상 보존, 다른 volume 경로 허용, schema contract와 metric 시험이 통과했다. |
| Audit metric contract | `go test ./internal/adapters/metrics -count=1` | PASS. Fixed outcome series, unknown outcome와 readiness gauge 시험이 통과했다. |
| 실제 3-process cluster | `go test ./tests/integration -run '^TestDynamicClusterManagementAuditProcessAcceptance$' -count=1 -timeout=10m -v` | PASS. Node 1 drain은 `503 audit_unavailable`, metric은 publisher ready `0`·mutation rejected `1`이었고 MQTT CONNECT는 성공했다. Node 2의 거절·활성화 실패 event는 request ID가 일치했다. CLI 검증은 2건 통과, 변조 사본은 `audit.jsonl:1`에서 거절했다. |
| Historical release compatibility | `NARUMQ_RELEASE_COMPAT=1 go test ./tests/integration -run 'TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance|TestClusterBackupRestoreHistoricalReleaseProcessAcceptance' -count=1 -timeout 60m` | 두 시험 PASS |
| 전체 Go package build | `go build ./...` | PASS. |
| Go vet | `go vet ./...` | PASS. |
| Traceability·architecture | `go run ./tools/verify traceability && go run ./tools/verify architecture` | PASS. |
| Full local gate | `GOMAXPROCS=4 go run ./tools/verify local` | PASS; `.artifacts/verification/coverage-summary.json`에 line 85.12%, function 97.43% |

초기 BP-07 digest 실패는 working tree의 CRLF 때문이었다. LF 규칙과 BP-06 digest 고정값을 보완한 뒤 통과했다. [Digest 기준](../specs/performance-and-verification.md#3-benchmark-공통-계약)을 따른다.

### 2026-10-11 Broker 내부 감사 consumer 계약 시험

저장소 루트에서 Windows x64와 Go 1.26.5로 실행했다. 시험은 schema에 맞는 미지 `action`·`reason`과 `ROLLOUT_CONTROLLER` event가 broker 내부 감사 저장·검증·조회 경로에서 보존되는지 확인한다. 외부 SIEM의 동작은 확인하지 않는다.

| 검증 | 명령 | 결과 |
| --- | --- | --- |
| 미지 action/reason 단독 시험 | `go test ./tests/contracts/audit-event-v1 -run '^TestAuditConsumersPreserveUnknownActionAndReason$' -count=1 -v` | PASS. 저장된 3개 event의 schema, action·reason·producer 값, hash chain, 최신순 조회와 원문 event bytes를 확인했다. 두 action 필터 조회도 각각 1건을 반환했다. |
| Action allowlist 민감도 확인 | 같은 단독 시험 명령(unknown action을 제외하는 임시 query allowlist 적용) | 예상 FAIL. `ROLLOUT_CONTROLLER` action 필터 조회가 0건을 반환했다. 임시 변경을 원복한 뒤 단독 시험은 PASS했다. |
| 감사 계약 package 전체 | `go test ./tests/contracts/audit-event-v1 -count=1` | PASS. |
| Vet | `go vet ./tests/contracts/audit-event-v1` | PASS |

## 3. NFR-19 자동 penetration test 실행 기록

대상 시나리오: SEC-SCN-052

2026-10-09 Windows x64, Go 1.26.5(`windows/amd64`)의 작업 트리에서 실행했다. 자동 공격 시험은 사전 시험이며 NFR-19 독립 침투 시험은 `미검증`이다. 현재 위협·위험 채택 상태는 [Threat model](../architectures/threat-model.md)을 따른다.

| 검증 | 명령 | 결과 |
| --- | --- | --- |
| 실제 3-node 3회 반복 | `go test ./tests/integration -run '^TestDynamicClusterTenantIsolationPenetrationProcessAcceptance$' -count=3 -v -timeout 20m` | 3회 모두 PASS |
| 민감도 확인 | `go test ./tests/integration -run '^TestDynamicClusterTenantIsolationPenetrationProcessAcceptance$/^T-01_T-05_WildcardWithInjectedTenantClaim$' -count=1 -v -timeout 20m` | 예상 FAIL; B identity를 `tenant-a`로 바꾸자 bAll에서 A payload의 unexpected PUBLISH(kind=3) 감지 |
| 원복 뒤 전체 공격 시험 | `go test ./tests/integration -run '^TestDynamicClusterTenantIsolationPenetrationProcessAcceptance$' -count=1 -v -timeout 20m` | 13개 subtest PASS |
| 전체 local gate | `go run ./tools/verify local` | `coverage: PASS`, `local: PASS` |

민감도 확인은 목표한 도청 감지까지 도달하도록 초기 projection 기대값도 임시로 합쳤다. Binding과 기대값은 원복했고 broker 코드는 바꾸지 않았다.

전체 local gate의 coverage 산출물은 `.artifacts/verification/coverage.out`, `.artifacts/verification/coverage-summary.json`과 `.artifacts/verification/branch/manifest.json`이다. CI·독립 보안 시험의 실행 증거는 아니다.

MQTT 3.1.1의 다른 tenant Client ID 요청은 CONNACK `0x05`로 거절됐다. `GET /api/v1/clients/tenant-a/b-all`은 `404`였다. `$SYS`는 ACL로 허용한 A client가 발행하고 A만 수신했다. Shared subscription projection은 underlying filter `jobs/#`였다. MQTT 3.1.1 persistent session은 B가 node 3에서 session-present=true로 재개하고 offline payload를 수신했다. A의 같은 Client ID는 session-present=false였으며 B payload를 받지 않았다.

독립 penetration test와 production 환경 시험은 이 실행에 포함되지 않는다. 현재 threat model 승인·위험 처리 상태를 이 과거 실행으로 판정하지 않는다.

## 4. SEC-11 MQTT data-plane denial audit 실행 기록

대상 시나리오: SEC-SCN-047, SEC-SCN-048

2026-10-09 저장소 루트에서 Windows x64와 Go 1.26.5 (`windows/amd64`)로 실행했다. 아래 결과는 이 작업의 SEC-11 변경과 관련된 시험이다.

| 검증 | 명령 | 결과 |
| --- | --- | --- |
| Application·TCP·metric·schema package | `go test ./internal/application/audit ./internal/application/connect ./internal/application/subscribe ./internal/adapters/tcp ./internal/adapters/metrics ./tests/contracts/audit-event-v1 -count=1` | PASS. 6개 package의 관련 시험이 통과했다. |
| Recorder race | `go test -race ./internal/application/audit -run 'DataPlane' -count=20` | PASS. 20회 반복에서 data-plane recorder race가 없었다. |
| Dynamic cluster process acceptance | `go test ./tests/integration -run '^TestDynamicCluster(MQTTDenialAudit|ManagementAudit|TenantIsolationPenetration)ProcessAcceptance$' -count=1 -v -timeout 20m` | PASS. MQTT denial 시험은 invalid credential, 거절 Client ID, SUBSCRIBE와 PUBLISH 거절 4건을 기록하고 schema·hash chain·field redaction·source IP·suppressed/dropped metric을 확인했다. Management audit와 13개 NFR-19 subtest도 통과했다. |
| Build·vet | `go build ./... && go vet ./...` | PASS. |
| Architecture 문서 검증 | `go run ./tools/verify architecture` | PASS. |
| 전체 local gate | `GOMAXPROCS=4 go run ./tools/verify local` | PASS. Branch instrumentation, 전체 package·integration, race, fuzz, vet, traceability, architecture와 coverage gate가 통과했다. 마지막 출력은 `coverage: PASS`, `local: PASS`다. |
| Diff coverage | `go run ./tools/verify coverage --profile .artifacts/verification/coverage.out --report .artifacts/verification/diff-summary.json --base HEAD` | PASS. |

첫 전체 local gate는 `TestClusterConcurrentOriginsReadBurstCompletesAfterEgressDrain`에서 peer rejection 1건으로 실패했다. 해당 시험의 단독 normal·race 실행과 artifact 재생성 뒤의 전체 local gate는 통과했다.

Data-plane recorder는 node별 초당 20건과 queue capacity 256을 적용한다. MQTT 응답은 기록 실패나 한도 초과 때문에 바뀌지 않는다. NFR-19의 독립 penetration test와 threat model 승인은 이 실행 증거에 포함되지 않는다.

## 5. SEC-05 password authentication 실행 기록

대상 시나리오: SEC-SCN-014, SEC-SCN-015, SEC-SCN-016, SEC-SCN-017

2026-10-09 Windows x64, Go 1.26.5(`windows/amd64`)의 작업 트리에서 실행했다.

| 검증 | 명령 | 결과 |
| --- | --- | --- |
| Provider·CLI·CONNECT·TCP package | `go test ./internal/adapters/authentication ./cmd/narumq ./internal/application/connect ./internal/adapters/tcp -count=1` | 4개 package PASS |
| Authentication race | `go test -race ./internal/adapters/authentication ./internal/application/connect ./cmd/narumq -count=1 -timeout 20m` | 3개 package PASS |
| TLS·cluster process acceptance | `go test ./tests/integration -run '^(TestMQTTTLS_PasswordAuthentication|TestDynamicClusterPasswordAuthenticationProcessAcceptance|TestDynamicClusterMQTTDenialAuditProcessAcceptance)$' -count=1 -v -timeout 20m` | 3개 시험 PASS |
| Secret-output scan | `go test ./tests/integration -run '^TestDynamicClusterPasswordAuthenticationProcessAcceptance$' -count=1 -v -timeout 20m` | PASS; metric·audit JSONL·process log에 password와 Argon2id PHC hash 없음 |
| Architecture | `go run ./tools/verify architecture` | PASS. |
| Full local gate | `go run ./tools/verify local` | `coverage: PASS`, `local: PASS` |

`TestMQTTTLS_PasswordAuthentication`는 MQTT 3.1.1·5의 password CONNECT를 실제 TLS socket에서 확인했다. 3-node process acceptance는 각 node에서 성공·잘못된 password·unknown username·허용되지 않은 Client ID를 확인했다. 인증 Metric은 node별 `success=2`, `invalid_credentials=4`, `client_id_not_allowed=2`였고, denial audit에는 `authMethod=password`가 기록됐다. Metric 응답·audit JSONL·process log에서 시험용 password와 Argon2id PHC hash가 나오지 않는지 검사했다. 설정·fixture 임시 파일·process log에는 원문 password가 없는지도 확인했다.

SEC-05 구현 acceptance는 완료했다. Credential 회전·폐기와 만료 연결 종료는 [스펙 §7](../specs/tenancy-and-security.md#7-credential-회전폐기와-차단)과 이 문서의 [§7](#7-sec-07-credential-회전폐기-실행-기록)에서 다룬다. MQTT 5 `AUTH` 재인증은 제공하지 않는다. SEC-12 blocklist는 [스펙 §7.1](../specs/tenancy-and-security.md#71-clientidentityip-차단sec-12)과 이 문서의 [§8](#8-sec-12-blocklist-실행-기록)을 따른다.

## 6. SEC-10 실행 기록

대상 시나리오: SEC-SCN-031, SEC-SCN-032, SEC-SCN-033

2026-10-10 저장소 루트에서 실행했다. 아래 결과는 SEC-10 구현, named credential fixture와 Dashboard regression의 검증이다.

| 검증 | 명령 | 결과 |
| --- | --- | --- |
| JWT/JWKS verifier와 설정 | `go test ./internal/adapters/authentication ./cmd/narumq -run 'JWT|JWKS|SecurityConfig' -count=1` | PASS. |
| Cluster 설정·credential 적재 | `go test ./cmd/narumq -count=1` | PASS. |
| Management RBAC와 authentication | `go test ./internal/adapters/http/management -count=1` | PASS. |
| Audit query application·file reader·handler | `go test ./internal/application/audit ./internal/adapters/auditlog ./internal/adapters/http/management -count=1` | PASS. |
| Audit query race | `go test -race ./internal/application/audit ./internal/adapters/auditlog ./internal/adapters/http/management -count=1` | PASS. |
| Management API와 audit event contracts | `go test ./tests/contracts/... -count=1` | PASS. |
| 실제 3-node RBAC acceptance | `go test ./tests/integration -run '^TestDynamicClusterManagementRBACProcessAcceptance$' -count=1 -v -timeout 20m` | PASS |
| Process 회귀 | `go test ./tests/integration -run '^(TestDynamicClusterTenantIsolationPenetrationProcessAcceptance|TestDynamicClusterConnectionRebalanceProcessAcceptance|TestDashboardProvisioningMatchesContracts)$' -count=1 -v -timeout 20m` | PASS. |
| Dashboard Docker E2E | `NARUMQ_DASHBOARD_E2E=1 NARUMQ_DASHBOARD_E2E_HOLD=90s go test ./tests/integration -run '^TestDashboardStackProcessAcceptance$' -count=1 -v -timeout 20m` | PASS |
| Architecture | `go run ./tools/verify architecture` | PASS |
| Full local gate | `go run ./tools/verify local` | `coverage: PASS`, `local: PASS` |

실제 3-node RBAC acceptance는 named ReadOnly·Operator API key, SecurityAdmin OIDC subject binding과 ClusterAdmin break-glass를 확인했다. OIDC token의 `ClusterAdmin` role claim은 설정된 `SecurityAdmin` 권한을 바꾸지 않았다. Operator rebalance, audit filter·cursor·actor·role과 node-local hash chain도 통과했다. Dashboard E2E는 named ReadOnly key로 cluster GET을 허용하고 backup·drain mutation을 거절했다.

Production IdP·deployment, CI merge gate와 독립 침투 시험은 이 실행에 포함되지 않았다. 현재 위협·위험 결정은 threat model을 따른다.

## 7. SEC-07 credential 회전·폐기 실행 기록

대상 시나리오: SEC-SCN-034, SEC-SCN-035, SEC-SCN-036, SEC-SCN-037, SEC-SCN-038, SEC-SCN-039

2026-10-10 저장소 루트에서 Windows x64, Go 1.26.5 (`windows/amd64`)로 실행했다.

| 검증 | 명령 | 결과 |
| --- | --- | --- |
| Regression before fix | `go test ./internal/application/connect -run '^TestCredentialRevocationRejectsBeforeOwnershipSideEffects$' -count=1 -v` | 예상 FAIL로 재현했다. 폐기 credential과 checker 오류 모두 `Accepted=true`, `OwnershipEpoch=7`로 반환됐다. Disallowed Client ID 시험에서는 checker가 호출되지 않았다. |
| CONNECT·TCP regression after fix | `go test ./internal/application/connect ./internal/adapters/tcp -count=1` | PASS. 2개 package가 통과했다. CONNECT 거절의 session·ownership·Will 무변경과 TCP의 post-ownership recheck를 확인했다. |
| 실제 3-node Dynamic cluster | `go test ./tests/integration -run '^TestDynamicClusterCredential(Revocation|Expiry)ProcessAcceptance$' -count=1 -v -timeout 20m` | PASS; 모든 node의 revoked CONNECT 거절, MQTT 3.1.1 close·5 `0x87`, 새 credential 유지, 복구 뒤 재인증과 revoked Will 억제. JWT expiry는 두 protocol 종료와 Will 전달 |
| Build·vet | `go build ./... && go vet ./...` | PASS |
| Traceability·architecture | `go run ./tools/verify traceability && go run ./tools/verify architecture` | PASS |
| Full local gate | `GOMAXPROCS=4 go run ./tools/verify local` | PASS; coverage 포함 |

Regression test는 수정 전 CONNECT가 ownership을 얻고 허용되는 문제를 재현했다. 수정 뒤에는 checker가 credential을 revoke했다고 하거나 checker 오류를 내면 `ConnectContext`가 Client ID 검사와 session·ownership 작업 전에 거절한다. Test는 repository load/save, ownership 호출, queue 등록·제거, timer와 superseded Will publication이 없음을 확인한다. TCP adapter의 ownership 뒤 재검사는 유지한다.

`internal/adapters/tls/listener_test.go::TestExtractVerifiedClientCertificateDTO`는 verified certificate의 `NotAfter`가 DTO에 복사되는지 확인한다. `internal/adapters/authentication/mtls_test.go::TestMTLSAuthenticationProviderMatchesConfiguredSelectorKinds`는 provider의 `Result.ExpiresAt`을 확인한다. `internal/adapters/tcp/server_credential_lifecycle_test.go::TestTCPCredentialExpiryDisconnectsAndProcessesWill`은 별도로 `Principal.ExpiresAt` timer와 protocol별 close·Will 동작을 확인한다. 실제 TLS handshake에서 certificate expiry까지 이어지는 process acceptance는 실행하지 않았다. `internal/application/connect/dataplane_audit_test.go::TestCredentialRevocationSuppressesImmediateAndDelayedWills`는 폐기된 credential의 즉시·지연 Will 억제와 denial audit을 확인한다.

Process acceptance는 Dynamic 3-node cluster에서 password credential을 revoke하고 node별 새 MQTT 3.1.1·5 CONNECT를 거절했다. 이미 열린 연결은 즉시 close 또는 MQTT 5 `0x87` DISCONNECT를 받았다. Subscriber는 revoked credential의 Will을 받지 않았다. 새 credential은 계속 연결됐고, restore 뒤 이전 credential도 다시 연결됐다. JWT expiry acceptance는 MQTT 3.1.1·5에서 연결을 종료하고 두 Will을 전달했다.

저장소 acceptance는 production IdP, secret manager, certificate 발급·회전 절차, deployment rollout controller, production 환경 또는 독립 penetration test를 검증하지 않는다. MQTT 5 `AUTH` 재인증은 제공하지 않는다. 이 저장소에는 실행 결과를 연결할 CI run이 없어 local verification만 기록했다. 운영 절차는 [credential 회전·폐기 how-to](../how-to/credential-rotation-and-revocation.md)에 있다.

## 8. SEC-12 blocklist 실행 기록

대상 시나리오: SEC-SCN-040, SEC-SCN-041, SEC-SCN-042, SEC-SCN-043, SEC-SCN-044, SEC-SCN-045

2026-10-10 저장소 루트에서 Windows x64와 Go 1.26.5 (`windows/amd64`)로 실행했다. `verify ci`는 이 workstation에서 실행한 저장소 gate이며 hosted CI run을 뜻하지 않는다.

| 검증 | 명령 | 결과 |
| --- | --- | --- |
| SEC-12 domain·application·adapter·contract package | `go test ./internal/domain/security ./internal/domain/session ./internal/application/membership ./internal/application/connect ./internal/adapters/raft/records ./internal/adapters/tcp ./internal/adapters/tls ./internal/adapters/metrics ./internal/adapters/http/management ./cmd/narumq ./tests/contracts/... -count=1` | PASS. Blocklist 값 검증·match 우선순위, membership service와 Raft codec, CONNECT 경계, TCP 회수, TLS fingerprint, management API·audit contract 시험이 통과했다. |
| 실제 cluster·TLS acceptance | `go test ./tests/integration -run '^(TestDynamicClusterClientBlocklistProcessAcceptance|TestMQTTTLSBlockedCertificateFingerprintRejected)$' -count=1 -v -timeout 20m` | PASS; 3-node Client ID·username·IP 전파, tenant 분리, 양 protocol 거절·기존 연결 종료·Will 억제·unblock·audit와 TLS leaf fingerprint 거절 |
| SEC-07 credential process 회귀 | `go test ./tests/integration -run '^TestDynamicClusterCredential(Revocation|Expiry)ProcessAcceptance$' -count=1 -v -timeout 20m` | PASS |
| Build·vet | `go build ./... && go vet ./...` | PASS. |
| Traceability·architecture | `go run ./tools/verify traceability && go run ./tools/verify architecture` | PASS |
| Full local gate | `powershell.exe -NoProfile -Command "$env:GOMAXPROCS='4'; go run ./tools/verify local"` | `coverage: PASS`, `local: PASS` |
| Local CI gate | `go run ./tools/verify ci --base HEAD` | PASS; mutation 516 killed, 0 lived, 6 not covered, efficacy 100%, mutator coverage 98.85% |

최초 coverage gate 실패 뒤 canonical 값 오류·comparator 순서·unknown kind·policy filter 시험을 추가했다. Threshold는 바꾸지 않았다.

실행 증거는 저장소 acceptance와 local verification 범위다. Hosted CI, production 환경, 독립 penetration test와 threat model 승인은 이 실행에 포함되지 않는다. NFR-19는 별도 release gate로 유지한다.

## 9. TEN-09 noisy-neighbor 실행 기록

대상 시나리오: SEC-SCN-030

2026-10-11 저장소 루트에서 Windows x64, Go 1.26.5 (`windows/amd64`)로 실행했다.

| 실행 | baseline p50/p99/max | noisy p50/p99/max | p99 비율 | noisy received/상한 per round | offered |
| --- | --- | --- | ---: | --- | ---: |
| 1 | 11.6598/28.7616/53.7386ms | 11.7026/30.5298/38.8162ms | 1.061 | 263/264, 263/264, 263/264, 262/263, 263/264 | 1314 |
| 2 | 11.6791/28.6557/37.0045ms | 11.6323/26.0608/36.6205ms | 0.909 | 263/264, 263/264, 263/264, 263/264, 263/264 | 1315 |
| 3 | 11.7204/29.4892/35.7259ms | 11.7043/28.7731/45.3087ms | 0.976 | 263/264, 263/264, 263/264, 263/264, 263/264 | 1315 |
| 4 | 11.814/38.6218/94.3751ms | 11.7249/29.7111/34.5871ms | 0.769 | 263/264, 263/264, 262/263, 263/264, 264/265 | 1315 |
| 5 | 11.7185/24.574/34.5682ms | 11.8383/28.7883/74.3548ms | 1.171 | 263/264, 262/263, 263/264, 263/264, 263/264 | 1314 |
| 2026-10-15 isolated diagnostic (one run; not replacement for five-run evidence) | 11.5375/32.3042/54.604ms | 11.5115/23.6597/33.2267ms | 0.732 | 262/263, 263/264, 262/263, 263/264, 263/264 | 1313 |

2026-10-15 단독 진단 `GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterTenantNoisyNeighborProcessAcceptance$' -count=1 -v -timeout 10m`은 PASS였다. 같은 날 전체 local 실행의 p99 비율 `1.517 > 1.5` 실패를 해소한 결과는 아니다([실패 기록](drain-and-rolling-update.md#2-10-수용-시험-매핑-표와-보충-문단)). 기존 5회 수용이나 production gate를 대신하지 않는다.

| 검증 | 명령 | 결과 |
| --- | --- | --- |
| 실제 3-node 수용 시험 5회 | `go test ./tests/integration -run '^TestDynamicClusterTenantNoisyNeighborProcessAcceptance$' -count=5 -v -timeout 30m` | PASS 5/5. 매 round noisy 수신 50 이상·token bucket 상한 이하, 최대 p99 비율 1.171, noisy 연결 오류 없음 |
| Rate 제한 음성 대조 | 같은 시험에서 tenant override `messagesPerSecond`를 일시적으로 0으로 설정해 `-count=1` 실행 | 예상한 FAIL. 2.9902118초 동안 1492개를 수신해 계산 상한 200을 넘었다. 확인 뒤 설정을 50으로 복원했다. |
| Full local gate | `go run ./tools/verify local` | 당시 `coverage: PASS`, `local: PASS` |

이 local 수용 시험은 상대 p99 기준과 tenant rate limit 적용을 확인한다. Production tenant 부하, production 환경, 절대 지연 SLO와 독립 penetration test는 확인하지 않는다.

2026-10-18 [PR #9 실행 39077367789](https://github.com/kayden-kim/narumq/actions/runs/39077367789)에서 이 시험이 `ratio 2.712 > 1.5`로 실패했다. [ADR-0064](../decisions/ADR-0064-ci-gate-logic-only.md)에 따라 시험은 기본 gate 밖의 opt-in 수용 시험(`Acceptance: NARUMQ_LOAD_QUALITY`)이 됐다. 합격 기준(p99, 1.5배, 5 round, 구간당 500개 표본)은 바꾸지 않았다. 실패 원인과 이동 근거는 [성능 및 검증 기록 §15](performance-and-verification.md#15-ci-gate-논리-검증-한정-2026-10-18)에 있다. 이후 이 시험의 통과 증거는 아래 명령의 결과다. 아래 실행은 위 5회 수용 기록을 바꾸지 않는다.

| 명령 | 결과 |
| --- | --- |
| `NARUMQ_LOAD_QUALITY=1 go run ./tools/verify acceptance --run TenantNoisyNeighbor` (Windows x64, Go 1.26.5) | `acceptance: PASS`. 시험 45초. baseline p50/p99/max 13.4453/37.1534/68.5345ms, noisy 12.9238/31.1943/59.0191ms, p99 비율 0.840. noisy 수신/상한은 round마다 263/264, 263/264, 263/264, 263/264, 264/265이고 offered는 1316이다. |
| `NARUMQ_LOAD_QUALITY=1 go run ./tools/verify acceptance --run 'TenantNoisyNeighbor\|TestN1PersistentClose\|TestRunAgainstInProcessNaruMQ'` (같은 host, 네 시험을 한 번에 실행) | `acceptance: 4 tests passed`, `acceptance: PASS`. TEN-09는 37.31초. baseline p50/p99/max 10.2928/25.8698/49.8811ms, noisy 10.2014/23.8598/32.9465ms, p99 비율 0.922. noisy 수신/상한은 round마다 263/264, 263/264, 264/265, 262/263, 263/264이고 offered는 1315다. |

## 10. 완료 판정과 범위 기록

대상 시나리오: SEC-SCN-014~SEC-SCN-017, SEC-SCN-030, SEC-SCN-034~SEC-SCN-052

이 문서의 수용은 각 절에 적은 로컬 시험 범위다. TEN-08은 [스펙 §5.1](../specs/tenancy-and-security.md#51-저장-quota), TEN-09는 [스펙 §5.3](../specs/tenancy-and-security.md#53-noisy-neighbor-완화ten-09), credential·차단은 [스펙 §7](../specs/tenancy-and-security.md#7-credential-회전폐기와-차단)을 따른다. Production 인프라·IdP·독립 침투 시험을 완료한 것으로 확대하지 않는다.
2026-10-16 결정으로 제품 감사 기능·포맷·hash chain·로컬 7일 buffer의 수용과 외부 SIEM 운영 보관을 분리했다. Rollout 감사 producer의 EKS 실측은 `rollout-durable-audit-result.json`에 기록됐다.

## 11. 미실행 기록

SEC-SCN-051의 node-local buffer·조회 권한은 [§12](#12-감사-buffer와-조회-권한-회귀-시험-2026-10-17)에서 확인했다. 외부 SIEM의 접근 제어와 최소 365일 실제 보관 관찰은 운영 인프라(Day-2 Ops) 요건이며 미실행이다. 2026-10-16 사용자 결정에 따라 제품 수용 차단 조건에서 분리했고, 저장소 자동 시험으로 통과를 판정하지 않는다.

## 12. 감사 buffer와 조회 권한 회귀 시험 (2026-10-17)

대상 시나리오: SEC-SCN-051

대상 범위: 현재 작업 트리의 테스트 추가다. 제품 코드는 바꾸지 않았다. 환경은 Windows/amd64다.

실행 명령: `GOMAXPROCS=2 go test -p=2 -count=1 ./internal/adapters/auditlog ./internal/adapters/http/management -run "TestAuditBufferPreservesSevenDaysAcrossRotationAndRestart|TestAuditQueryOnlySecurityAdminAndClusterAdminCanRead" -v`.

결과: 두 회귀 시험을 포함한 focused 실행 통과.

`TestAuditBufferPreservesSevenDaysAcrossRotationAndRestart`는 8일 전부터 현재까지의 event를 실제 디스크에 기록한다. Rotation 파일의 수정 시각도 8일 전으로 바꾼다. 재시작과 새 event append 뒤 모든 event가 조회되는지 확인한다. 이는 시간 경과에 따른 자동 삭제가 없는 현재 node-local buffer의 회귀 시험이다. 실제 7일 동안 실행한 운영 시험이나 디스크 용량 보장은 아니다.

`TestAuditQueryOnlySecurityAdminAndClusterAdminCanRead`는 audit 조회 API에서 `SecurityAdmin`·`ClusterAdmin`만 `200`을 받는지 확인한다. `ReadOnly`·`Operator`는 `403`이며 저장소를 읽지 않는다.

이 로컬 시험은 §11의 외부 SIEM 검증을 대신하지 않는다.
