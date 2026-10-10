# 실행 옵션과 설정 스펙

`contract`는 파일이나 명령행에 적는 내용만 보고 process 시작 단계에서 수락하거나 거절하는 규칙을 소유한다. 여기에는 flag와 key의 이름, 자료형, 필수 여부, 생략할 때의 기본값, 허용 범위, 항목 사이의 정적 검증과 엄격한 파싱 규칙이 포함된다. `spec`은 실행 중인 시스템이 설정을 어떻게 사용하는지를 소유한다. 여기에는 실행 모드 선택, 적재 순서, 시작 실패, 변경을 반영하는 경로, node-local과 cluster-wide 적용 범위가 포함된다. 각 값이 MQTT나 Management API 동작에 주는 효과는 기존 도메인 spec이 계속 소유한다. 예를 들어 Receive Maximum 초과 시 DISCONNECT 동작은 MQTT spec이 소유한다.

| 항목 | 값 |
| --- | --- |
| 상태 | `Draft` |
| 관련 요구사항 | [PRD](../requirements/PRD.md) API-05, API-09, SEC-02, SEC-05, MQTT-09, MQTT-11, CLU-09, CLU-12, TEN-06~TEN-08 |
| 관련 아키텍처 문서 | [Project structure 코드 지도](../architectures/project-structure.md) |
| 관련 계약 | [Process configuration v1](../contracts/process-configuration-v1.md) |
| 관련 ADR | [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md), [ADR-0029](../decisions/ADR-0029-replication-factor-write-quorum-profile.md), [ADR-0030](../decisions/ADR-0030-publish-backpressure-admission.md), [ADR-0034](../decisions/ADR-0034-node-local-tenant-connection-and-traffic-limits.md), [ADR-0043](../decisions/ADR-0043-management-api-rate-limit.md), [ADR-0049](../decisions/ADR-0049-mqtt-credential-revocation.md), [ADR-0051](../decisions/ADR-0051-cluster-update-strategy.md), [ADR-0052](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md) (Proposed, 목표 구조) |

## 1. 범위와 외부 주체

- 외부 주체: 운영자, 배포 도구, build pipeline
- 포함: 실행 모드 선택, 설정 적재와 검증 순서, 시작 실패, 현재 설정 변경 반영 경로, node-local과 cluster-wide 적용 범위
- 제외: 형식·범위·기본값은 [Process configuration v1](../contracts/process-configuration-v1.md)이 소유한다. 설정값이 MQTT·API·tenant 동작에 주는 효과는 해당 도메인 spec이 소유한다. MQTT listener의 보안 mode 규칙은 [Tenant와 security §2.3](tenancy-and-security.md#23-connect-credential과-strict-security-configuration)을 따른다. 목표 구조는 [ADR-0052](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)가 소유한다.

## 2. 실행 모드

| 모드 | 진입 조건 | 결과 |
| --- | --- | --- |
| password hash | `-hash-password` | stdin에서 password를 읽고 hash를 stdout에 쓴 뒤 종료한다. |
| audit verify | `-audit-verify <dir>` | audit 디렉터리를 읽기 전용으로 검증하고 결과를 출력한 뒤 종료한다. |
| cluster restore | `-cluster-config`와 `-cluster-restore` | data를 seed하고 종료한다. Listener를 열지 않는다. |
| dynamic cluster | `-cluster-config`만 지정 | 실행을 유지한다. |
| fixed-voter peer-only | `-raft-peer-config` | 실행을 유지한다. |
| standalone | 앞의 모드가 아니며 listener flag를 하나 이상 지정 | 실행을 유지한다. |
| 비활성 | 앞의 모드가 아니며 listener flag가 없음 | `MQTT TCP and management HTTP listeners disabled`를 log에 남기고 종료 코드 0으로 끝난다. |

시스템은 다음 순서로 모드를 판정한다: `-hash-password` → `-audit-verify` → `-cluster-config` 또는 `-cluster-restore` → `-raft-peer-config` → standalone. 허용 조합은 [Process configuration v1 §1.2](../contracts/process-configuration-v1.md#12-모드-판정-순서와-허용-조합)를 따른다. 진입 조건과 결과는 `cmd/narumq/main.go`의 `runWithContext`에 근거한다.

## 3. 시작, 실행, 종료 흐름

1. `run`은 SIGINT와 SIGTERM handler를 등록한다. `runWithContext`는 flag를 parse하고 실행 모드를 선택한다.
2. Password hash, audit verify, cluster restore 같은 일회성 모드는 작업을 마친 뒤 listener 없이 종료한다.
3. Standalone mode는 flag를 검증하고 security config와 authentication provider를 준비한 뒤 MQTT listener를 연다. Management listener와 runtime service를 구성한 뒤 실행한다. Listener 일부를 연 다음 실패하면 이미 연 listener를 닫는다.
4. Dynamic cluster mode는 `runWithContext`에서 cluster config를 읽는다. `runClusterMode`는 config와 peer TLS를 검증하고 Raft 상태를 연다. Bootstrap 확인을 위해 peer listener를 먼저 열 수 있다. Peer transport를 준비한 뒤 MQTT security config와 provider를 읽고 MQTT·management listener를 연다. 이 단계에서 실패하면 이미 연 자원을 닫는다.
5. Fixed-voter peer-only mode는 peer config와 TLS를 검증한 뒤 peer listener와 Raft node를 연다.
6. 실행 중 SIGINT 또는 SIGTERM이 오면 `run`의 context가 취소된다. Cluster runtime은 node를 draining 상태로 전환한 뒤 service를 종료한다.

Standalone mode에서는 security config를 MQTT listener보다 먼저 적재한다. Dynamic cluster mode에서는 bootstrap 확인 또는 Raft 복구를 먼저 진행할 수 있다. Cluster mode의 MQTT security config 적재 순서는 [Tenant와 security §2.3](tenancy-and-security.md#23-connect-credential과-strict-security-configuration)을 따른다. 근거는 `cmd/narumq/main.go`의 `runWithContext`, `cmd/narumq/cluster_runtime.go`의 `runClusterMode`와 `clusterRuntime.serve`, `cmd/narumq/raft_peer_config.go`의 `runRaftPeerOnly`다.

설정을 바꿔도 실행 중인 process에는 자동 반영되지 않는다. Reload signal은 없다.

### 3.1 현재 설정 변경 반영 경로

| 설정 범주 | 예 | 반영 방법 | 적용 범위 | 기준 문서 |
| --- | --- | --- | --- | --- |
| standalone flag | listener, timeout, queue 한도 | process를 재시작한다. | node-local | [Process configuration v1](../contracts/process-configuration-v1.md) |
| cluster config 일반 field | node별 주소와 listener 설정 | rolling restart를 한다. | node-local | [ADR-0051](../decisions/ADR-0051-cluster-update-strategy.md) |
| cluster config bootstrap identity field | `clusterId`, `nodeId`, `role`, bootstrap identity | 첫 시작 뒤 바꿀 수 없다. `cluster.meta`와 다른 값이면 시작을 거절한다. | 변경 불가 | [Process configuration v1 §1.3](../contracts/process-configuration-v1.md), [Raft membership v2 §1.4](../contracts/raft-membership-v2.md#14-combined-cluster-configuration) |
| `tenantQuotas` | versioned tenant storage quota profile | version을 올린 설정으로 rolling restart를 한다. Commit된 version이 cluster 전체의 기준이 된다. | cluster-wide | [Tenant §5.1](tenancy-and-security.md#51-저장-quota), [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md) |
| `replication` | `factor`, `writeQuorum` | 3/2만 허용하므로 바꿀 수 없다. | 변경 불가 | [ADR-0029](../decisions/ADR-0029-replication-factor-write-quorum-profile.md) |
| security config | ACL, `tenantLimits`, 인증 identity, secret | rolling restart를 한다. Rollout 중 node마다 값이 다를 수 있다. | node-local | [ADR-0049](../decisions/ADR-0049-mqtt-credential-revocation.md), [ADR-0034](../decisions/ADR-0034-node-local-tenant-connection-and-traffic-limits.md) |
| build identity | release version, build feature | binary를 교체한다. | node별 binary | [Process configuration v1 §1.8](../contracts/process-configuration-v1.md#18-build-identity) |
| 운영 상태 변경 | feature activation, credential revocation, blocklist, drain, rebalance, membership | 기존 전용 Management API를 사용한다. Mutation은 cluster에 commit한 뒤 적용한다. | cluster-wide | [Management API spec](management-api-and-observability.md), [Cluster spec](cluster-session-and-delivery.md), [Tenant와 security spec](tenancy-and-security.md), [Drain spec](drain-and-rolling-update.md) |
| Backup | cluster backup 생성·restore | 전용 API 또는 `-cluster-restore`를 사용한다. Backup 생성은 commit된 상태를 읽으며 cluster 상태를 변경하지 않는다. | cluster state의 archive | [Cluster backup v3](../contracts/cluster-backup-v3.md) |

`tenantQuotas`는 정적 설정이지만 더 높은 version이 Raft command로 commit된 뒤 cluster-wide 기준이 된다. 그 밖의 cluster config와 security config는 node마다 적재하므로 rollout 중 값이 다를 수 있다.

## 4. 오류와 복구

| 조건 | 외부 결과 | 상태 변경 | 복구 |
| --- | --- | --- | --- |
| 정의되지 않은 flag | Parse 오류로 시작하지 않는다. | 없음 | 유효한 flag를 사용해 다시 시작한다. |
| 서로 충돌하는 모드 flag | 시작 오류를 반환한다. | 없음 | 모드에 맞는 flag만 지정해 다시 시작한다. |
| 설정 파일이 contract 규칙을 위반함 | Parser가 거절하면 시작 오류를 반환한다. 입력 형식과 정적 검증 규칙은 [Process configuration v1](../contracts/process-configuration-v1.md)을 따른다. | 없음 | 설정을 고친 뒤 다시 시작한다. |
| secret 파일이 없거나 비어 있음 | 시작 오류를 반환한다. | 없음 | secret 파일을 고친 뒤 다시 시작한다. |
| listener 일부가 열린 뒤 실패함 | 시작 오류를 반환한다. Process는 이미 연 listener를 닫는다. | 열린 listener를 정리한다. | 원인을 고친 뒤 다시 시작한다. |

시작 또는 runtime 오류는 stderr에 JSON log `narumq startup or runtime failure`와 `error` field를 남긴다. Process는 종료 코드 1로 끝난다. 재시도하려면 설정이나 실행 환경을 고친 뒤 다시 시작한다. 오류 형식의 상세 규칙은 [Process configuration v1 §2](../contracts/process-configuration-v1.md#2-오류)를 따른다.

## 5. 항상 지켜야 할 규칙과 보장하지 않는 범위

- 시스템은 token, key, password, password hash를 log에 남겨서는 안 된다.
- Release build는 설정용 환경 변수를 읽지 않는다.
- 설정 파일 변경은 실행 중인 process에 자동 반영되지 않는다.
- 이 spec은 node 사이의 일반 설정 일치를 보장하지 않는다. 현재 예외는 commit된 `tenantQuotas`다.
- 이 spec은 Proposed [ADR-0052](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)의 목표 구조가 구현되었다고 보장하지 않는다. 단, §1.5의 중복 key 거절 규칙과 standalone `-http-auth-token-file` secret 파일 규칙은 채택해 구현했다.

## 6. 수용 시험

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 | 시험 위치 | 결과 | 실행 기록 |
| --- | --- | --- | --- | --- | --- | --- |
| `CFG-SCN-01` | SEC-05 | `-hash-password`와 다른 flag 또는 위치 인수를 함께 지정한다. | 시스템은 조합을 거절한다. | `cmd/narumq.TestHashPasswordFlagIsExclusive` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-02` | CLU-09 | `-cluster-config`와 허용되지 않는 flag 또는 위치 인수를 함께 지정한다. | 시스템은 조합을 거절한다. | `cmd/narumq.TestClusterConfigFlagIsExclusive` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-03` | CLU-09 | `-raft-peer-config`와 다른 flag를 함께 지정한다. | 시스템은 조합을 거절한다. | `cmd/narumq.TestRaftPeerOnlyRejectsOtherFlags` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-04` | API-05 | `-audit-verify`의 경로를 비우거나 다른 flag·위치 인수를 함께 지정한다. | 시스템은 실행을 거절한다. | `cmd/narumq.TestAuditVerifyFlagRequiresExclusiveDirectory` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-05` | MQTT-09, API-05 | listener가 없는 기본 실행을 시작한다. | 시스템은 지정된 비활성 log를 남기고 종료 코드 0으로 끝난다. | `cmd/narumq.TestMainNoListenerPath`, `cmd/narumq.TestRun_Defaults` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-06` | SEC-02, SEC-05, MQTT-09 | TLS listener의 client-auth mode와 security binding 조합을 바꾼다. | 시스템은 허용된 인증 조합만 시작한다. | `cmd/narumq.TestRunWithContextMQTTSecurityMode` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-07` | MQTT-09 | runtime metrics output과 interval을 하나만 지정하거나 잘못된 interval을 지정한다. | 시스템은 flag 조합을 거절한다. | `cmd/narumq.TestRunRejectsRuntimeMetricsFlagCombinations` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-08` | MQTT-09, MQTT-11 | 생성 용량에 0, TCP max에 음수, TLS rate에 상한 초과값 또는 TCP listener 없이 TCP limit을 지정한다. | 시스템은 잘못된 생성 용량·범위·flag 조합을 거절한다. | `cmd/narumq.TestRunWithContextRejectsConstructionBounds` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-09` | CLU-09, CLU-12, TEN-08 | Cluster config에 중복·알 수 없는·누락 key, 뒤따르는 JSON value 또는 잘못된 값을 넣는다. | Parser는 설정을 거절한다. | `cmd/narumq.TestParseClusterConfigRejectsDuplicateUnknownMissingAndTrailingJSON` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-10` | CLU-09 | 존재하지 않는 cluster config 파일을 지정한다. | 시스템은 시작을 거절한다. | `cmd/narumq.TestRunWithContextClusterConfigLoading` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-11` | SEC-02, SEC-05 | Security config에 지원하지 않는 field, 중복 key, 잘못된 형식 또는 인증·ACL 오류를 넣는다. | Parser는 설정을 거절한다. | `cmd/narumq.TestParseSecurityConfig_Rejections` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-12` | CLU-09 | Peer-only config에 알 수 없는 field, 중복 key, 뒤따르는 value 또는 중복 peer ID를 넣는다. | Parser는 알 수 없는 field, 중복 key, 뒤따르는 value와 중복 peer ID를 거절한다. | `cmd/narumq.TestParseRaftPeerConfigStrict` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-13` | TEN-08 | `tenantQuotas`를 생략하거나 strict profile을 지정한다. | 생략 시 built-in version 0을 사용하고, 명시 profile은 strict하게 검증한다. | `cmd/narumq.TestConfiguredTenantQuotaProfileUsesBuiltinOrStrictConfig` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-14` | TEN-08 | Local `tenantQuotas`가 commit된 profile과 맞지 않는다. | 시스템은 committed profile을 유지하고 mismatch를 한 번 log에 남긴다. | `cmd/narumq.TestClusterTenantQuotaControllerLogsCommittedMismatchOnce` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-15` | CLU-11 | Build identity에 release version과 feature 목록을 지정한다. | 시스템은 feature를 정렬·중복 제거하고 유효성을 검사한다. | `cmd/narumq.TestResolveBuildIdentity` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-16` | MQTT-01 | Cluster MQTT listener를 여러 개 열 때 뒤쪽 listener 생성이 실패한다. | 시스템은 앞서 연 listener를 닫는다. | `cmd/narumq.TestClusterMQTTListenersBindConfiguredEndpointsAndClosePartialOpen` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-17` | CLU-09 | Peer-only 시작이 각 실패 지점에서 중단된다. | 시스템은 시작 중 만든 자원을 반환한다. | `cmd/narumq.TestRunRaftPeerOnlyStartupFailuresReleaseResources` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-18` | CLU-14 | `-cluster-restore`만 지정한다. | 시스템은 `-cluster-config`가 필요하다고 거절한다. | `cmd/narumq.TestClusterConfigFlagIsExclusive` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |
| `CFG-SCN-19` | SEC-10 | `-http-auth-token-file`에 없는 파일이나 공백만 있는 파일을 지정한다. 또는 삭제된 `-http-auth-token`을 지정한다. 또는 유효한 token 파일을 지정한다. | 앞의 세 입력은 시작을 거절한다. 유효한 파일을 쓰면 앞뒤 공백을 제거한 token으로 `/api/v1` Bearer 인증을 요구한다. | `cmd/narumq.TestRunWithContextManagementAuthTokenFile` | 통과 | 2026-10-10 [실행 기록](#configuration-test-execution) |

<a id="configuration-test-execution"></a>
### 설정 시험 실행 기록

실행 환경은 Windows/amd64, Go 1.26.5다. 다음 명령은 `cmd/narumq`의 설정·시작 경계 시험을 실행했다.

```sh
go test ./cmd/narumq -count=1 -run '^(TestHashPasswordFlagIsExclusive|TestClusterConfigFlagIsExclusive|TestRaftPeerOnlyRejectsOtherFlags|TestAuditVerifyFlagRequiresExclusiveDirectory|TestMainNoListenerPath|TestRun_Defaults|TestRunWithContextMQTTSecurityMode|TestRunRejectsRuntimeMetricsFlagCombinations|TestRunWithContextRejectsConstructionBounds|TestParseClusterConfig.*|TestValidateClusterConfig.*|TestClusterManagementAccessConfiguration|TestRunWithContextClusterConfigLoading|TestParseSecurityConfig.*|TestParseRaftPeerConfig.*|TestRejectDuplicateJSONKeys|TestConfiguredTenantQuotaProfileUsesBuiltinOrStrictConfig|TestClusterTenantQuotaControllerLogsCommittedMismatchOnce|TestResolveBuildIdentity|TestClusterMQTTListenersBindConfiguredEndpointsAndClosePartialOpen|TestRunRaftPeerOnlyStartupFailuresReleaseResources|TestRunRejectsManagementRateFlagsOutOfRange|TestValidateTLSAuthenticationBindings.*|TestRunWithContextManagementAuthTokenFile|TestRunWithContextReportsManagementListenFailure)$' -v
```

결과는 `PASS`다. 설정·시작 경계 시험과 G9 token-file 시험이 통과했다.

CLI 기본 경로도 smoke 실행했다. `go run ./cmd/narumq`는 `MQTT TCP and management HTTP listeners disabled`를 기록하고 정상 종료했다.

중복 key 시작 거절 smoke도 실행했다. Security: `go run ./cmd/narumq -tcp-listen 127.0.0.1:0 -security-config .artifacts/verification/g4-duplicate-key/dup-security.json`은 종료 코드 1과 `load security config: check security config JSON keys: duplicate JSON key`를 반환했다. Peer-only: `go run ./cmd/narumq -raft-peer-config .artifacts/verification/g4-duplicate-key/dup-peer.json`은 종료 코드 1과 `load raft peer config: check raft peer config JSON keys: duplicate JSON key`를 반환했다. 코드에서 security 설정 적재는 `openMQTTListener`보다 먼저, peer config 적재는 `net.Listen`보다 먼저 실행됨을 확인했다.

G9 CLI smoke는 Windows/amd64, Go 1.26.5에서 실행했다. `go run ./cmd/narumq -http-listen 127.0.0.1:18080 -http-auth-token x`는 종료 코드 1과 `flag provided but not defined: -http-auth-token`을 반환했다. `.artifacts/verification/g9-token-file/management.token`의 token으로 시작한 server는 인증 없는 `/api/v1/cluster` 요청에 `401`, 파일 token을 담은 `Authorization: Bearer` 요청에 `200`을 반환했다. Server를 종료했으며 process log에는 token 값이 없었다. Windows shell에서 `curl -o /dev/null`은 `401` 출력 뒤 종료 코드 23을 반환했다. 두 요청은 response body를 출력하는 `curl -s -w` 호출로 다시 확인했다.

## 7. 미결정 사항

- `SPEC-TBD-02`와 `SPEC-TBD-03`의 확정 위치는 이 spec이다. 이번 문서는 새 제품 값을 정하지 않는다.
- 목표 설정 구조와 API-09 정책은 [ADR-0052](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)(Proposed)를 따른다.
