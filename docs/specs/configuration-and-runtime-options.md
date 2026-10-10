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
4. Dynamic cluster mode는 `runWithContext`에서 cluster config를 읽는다. `runClusterMode`는 config와 peer TLS를 검증하고 Raft 상태를 연다. Bootstrap 확인을 위해 peer listener를 먼저 열 수 있다. Peer transport를 준비한 뒤 MQTT security config와 provider를 읽는다. TLS listener가 있으면 MQTT security config를 읽은 직후 `mqtt.tls.clientAuth`와 `mtlsIdentities` 조합을 standalone과 같은 규칙으로 검사한다. 이어서 MQTT·management listener를 연다. `runtimeMetrics`가 있으면 management server를 만든 뒤 sampler를 만들고 `clusterRuntime.serve`가 다른 service와 함께 실행한다. Sampler는 시작 즉시 한 행, 설정한 간격마다 한 행, 종료할 때 마지막 한 행을 기록한다. Sampler 오류는 다른 service 오류처럼 node를 draining으로 바꾸고 process를 종료한다. Cluster restore mode는 `runtimeMetrics`를 사용하지 않는다. 이 단계에서 실패하면 이미 연 자원을 닫는다.
5. Fixed-voter peer-only mode는 peer config와 TLS를 검증한 뒤 peer listener와 Raft node를 연다.
6. 실행 중 SIGINT 또는 SIGTERM이 오면 `run`의 context가 취소된다. Cluster runtime은 node를 draining 상태로 전환한 뒤 service를 종료한다.

Standalone mode에서는 security config를 MQTT listener보다 먼저 적재한다. Dynamic cluster mode에서는 bootstrap 확인 또는 Raft 복구를 먼저 진행할 수 있다. Cluster mode의 MQTT security config 적재 순서는 [Tenant와 security §2.3](tenancy-and-security.md#23-connect-credential과-strict-security-configuration)을 따른다. 근거는 `cmd/narumq/main.go`의 `runWithContext`, `cmd/narumq/cluster_runtime.go`의 `runClusterMode`와 `clusterRuntime.serve`, `cmd/narumq/raft_peer_config.go`의 `runRaftPeerOnly`다.

설정을 바꿔도 실행 중인 process에는 자동 반영되지 않는다. Reload signal은 없다.

### 3.1 현재 설정 변경 반영 경로

| 설정 범주 | 예 | 반영 방법 | 적용 범위 | 기준 문서 |
| --- | --- | --- | --- | --- |
| standalone flag | listener, timeout, queue 한도 | process를 재시작한다. | node-local | [Process configuration v1](../contracts/process-configuration-v1.md) |
| cluster config 일반 field | node별 주소와 listener 설정 | rolling restart를 한다. | node-local | [ADR-0051](../decisions/ADR-0051-cluster-update-strategy.md) |
| Cluster runtime metrics | `runtimeMetrics` | rolling restart를 한다. | node-local | [Process configuration v1 §1.3](../contracts/process-configuration-v1.md#13-cluster-config-json), [ADR-0052](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md) |
| `clusterPolicy.mqttLimits` | Cluster MQTT의 다섯 한도 | Feature 활성 전에는 local 값, 활성 뒤 version 1 이상 commit을 마치면 committed 값을 쓴다. 새 연결부터 적용한다. | cluster-wide | [ADR-0052 §1.1.1](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#111-1차-구현-계약-mqttlimits), [Process configuration v1](../contracts/process-configuration-v1.md) |
| `tenantQuotas` | versioned tenant storage quota profile | version을 올린 설정으로 rolling restart를 한다. Commit된 version이 cluster 전체의 기준이 된다. | cluster-wide | [Tenant §5.1](tenancy-and-security.md#51-저장-quota), [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md) |
| `replication` | `factor`, `writeQuorum` | 3/2만 허용하므로 바꿀 수 없다. | 변경 불가 | [ADR-0029](../decisions/ADR-0029-replication-factor-write-quorum-profile.md) |
| security config | ACL, `tenantLimits`, 인증 identity, secret | rolling restart를 한다. Rollout 중 node마다 값이 다를 수 있다. | node-local | [ADR-0049](../decisions/ADR-0049-mqtt-credential-revocation.md), [ADR-0034](../decisions/ADR-0034-node-local-tenant-connection-and-traffic-limits.md) |
| build identity | release version, build feature | binary를 교체한다. | node별 binary | [Process configuration v1 §1.8](../contracts/process-configuration-v1.md#18-build-identity) |
| 운영 상태 변경 | feature activation, credential revocation, blocklist, drain, rebalance, membership | 기존 전용 Management API를 사용한다. Mutation은 cluster에 commit한 뒤 적용한다. | cluster-wide | [Management API spec](management-api-and-observability.md), [Cluster spec](cluster-session-and-delivery.md), [Tenant와 security spec](tenancy-and-security.md), [Drain spec](drain-and-rolling-update.md) |
| Backup | cluster backup 생성·restore | 전용 API 또는 `-cluster-restore`를 사용한다. Backup 생성은 commit된 상태를 읽으며 cluster 상태를 변경하지 않는다. | cluster state의 archive | [Cluster backup v3](../contracts/cluster-backup-v3.md) |

`tenantQuotas`는 정적 설정이지만 더 높은 version이 Raft command로 commit된 뒤 cluster-wide 기준이 된다. 그 밖의 cluster config와 security config는 node마다 적재하므로 rollout 중 값이 다를 수 있다.
`ClusterPolicy`는 committed version 1 이상이 있으면 그 `mqttLimits`를 모든 node에 적용한다. 그렇지 않으면 각 node는 local `clusterPolicy.mqttLimits`를, 그것도 없으면 기본값을 사용한다. Reconciler는 feature 활성 뒤 local version이 committed version보다 높을 때만 새 정책을 제안한다. Local version이 낮거나 같은 version의 내용이 다르면 committed 값을 유지하고 mismatch gauge와 Warn log를 갱신한다. 설정은 연결 수락 때 capture하므로 정책 변경은 새 연결부터 반영한다.

`runtimeMetrics`는 node 정적 설정이다. 값을 바꾸려면 해당 node를 rolling restart한다. 설정한 node만 JSONL 표본을 기록한다.

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
- `clusterPolicy.mqttLimits`는 versioned committed policy가 있을 때 node 간 일치한다. 일반 cluster config와 security config의 나머지 값은 node마다 적재하므로 rollout 중 다를 수 있다.
- 이 spec은 Proposed [ADR-0052](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)의 나머지 정책 항목이 구현되었다고 보장하지 않는다. `mqttLimits` 범위만 G1·API-09의 채택 사항이다.

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
| `CFG-SCN-20` | CLU-09 | Cluster config에서 `mqtt.tls`를 생략하거나 각 필드에 잘못된 값·형식을 넣는다. | Parser는 TLS listener와 `mqtt.tls` 불일치, 누락 필드, 잘못된 `clientAuth`, CA 경로와 `minVersion`을 거절한다. | `cmd/narumq.TestParseClusterConfigMQTTTLS` | 통과 | [Cluster MQTT TLS 실행 기록](#cluster-mqtt-tls-g2-g10-execution) |
| `CFG-SCN-21` | SEC-02 | `mqtt.tls.clientAuth`와 `mtlsIdentities` 조합을 바꾼다. | 시스템은 standalone과 같은 인증 조합만 허용한다. | `cmd/narumq.TestClusterMQTTSecurityValidatesTLSClientAuthBindings` | 통과 | [Cluster MQTT TLS 실행 기록](#cluster-mqtt-tls-g2-g10-execution) |
| `CFG-SCN-22` | SEC-02 | 3-node cluster에 유효한, 없는, 미등록 client certificate를 제시한다. MQTT 3.1.1과 MQTT 5, TLS 1.2와 1.3, 허용·미등록 Client ID도 확인한다. | 유효한 인증서와 binding은 연결된다. `require`에서 인증서가 없거나 TLS 버전이 낮으면 handshake를 거절한다. 미등록 identity와 Client ID는 MQTT 3.1.1에서 CONNACK `0x05`, MQTT 5에서 CONNACK `0x87`로 거절한다. | `tests/integration.TestDynamicClusterMQTTTLSClientCertificateProcessAcceptance` | 통과 | [Cluster MQTT TLS 실행 기록](#cluster-mqtt-tls-g2-g10-execution) |
| `CFG-SCN-23` | PERF-08 | Cluster config에서 `runtimeMetrics`를 생략·지정하거나 누락 key, 범위 밖 interval, 빈 경로, `directory`·`auditDirectory`와 겹치는 경로, 알 수 없는 key를 넣는다. | 생략하면 비활성이다. 유효하면 값을 적재한다. 나머지는 거절한다. | `cmd/narumq.TestParseClusterConfigRuntimeMetrics` | 통과 | [실행 기록](#cluster-runtime-metrics-g6-execution) |
| `CFG-SCN-24` | PERF-08 | 3-node in-process cluster에서 node 1에만 `runtimeMetrics`(`intervalMillis` 50)를 지정한다. | node 1의 출력 파일에 `schemaVersion` 1인 JSON 행이 2개 이상 기록된다. | `cmd/narumq.TestRunClusterModeInProcess` | 통과 | [실행 기록](#cluster-runtime-metrics-g6-execution) |
| `CFG-SCN-25` | API-09 | `clusterPolicy`를 생략하거나 version, 다섯 한도 값, 필수 key와 unknown key를 바꾼다. | 생략은 허용한다. 유효한 값은 읽고, 0·상한 초과·누락·알 수 없는 key는 거절한다. | `cmd/narumq.TestParseClusterConfigClusterPolicy` | 통과 | [API-09 mqttLimits 실행 기록](#api-09-mqttlimits-execution) |
| `CFG-SCN-26` | API-09 | 기본값·local config·committed policy precedence와 higher/lower/same-version reconciliation을 확인한다. | Committed version이 우선한다. Higher local version만 제안하고 불일치 설정은 mismatch로 남긴다. | `cmd/narumq.TestEffectiveClusterMQTTLimitsPrecedence`, `cmd/narumq.TestClusterPolicyControllerReconcilesAndLogsMismatchOnce` | 통과 | [API-09 mqttLimits 실행 기록](#api-09-mqttlimits-execution) |
| `CFG-SCN-27` | API-09 | Feature가 활성화된 ClusterPolicy를 snapshot과 backup archive로 저장·복원한다. | Restored state에서 같은 version과 MQTT 한도를 읽는다. | `cmd/narumq.TestClusterBackupArchivePreservesClusterPolicy` | 통과 | [API-09 mqttLimits 실행 기록](#api-09-mqttlimits-execution) |
| `CFG-SCN-28` | API-09 | 3-node cluster에서 policy PUT, 재시도·충돌, 신규·기존 연결, node restart와 drift를 확인한다. | 모든 node가 committed version을 적용한다. 기존 연결은 이전 한도를 유지하고 재시작 node는 committed 값과 mismatch를 보고한다. | `tests/integration.TestDynamicClusterPolicyProcessAcceptance` | 통과 | [API-09 mqttLimits 실행 기록](#api-09-mqttlimits-execution) |
| `CFG-SCN-29` | SEC-01 | Cluster config의 `mqtt.tcpListenAddress`에 `0.0.0.0:1884`, `[::]:1884`, `10.0.0.5:1884`, `mqtt.example:1884`를 넣는다. `mqtt.securityConfigFile`을 비운다. 인증 방식별로 credential·certificate 없는 CONNECT를 보낸다. 3-node cluster 평문 TCP에서 password 인증을 확인한다. | 네 주소를 그대로 적재한다. 빈 security config 경로는 거절한다. JWT 설정에서 credential 없는 MQTT 5 CONNECT는 `0x87`로 거절한다. mTLS 전용 설정에서 certificate 없는 CONNECT는 MQTT 3.1.1 `0x05`, MQTT 5 `0x87`로 거절한다. Cluster TCP는 유효한 password만 연결한다. | `cmd/narumq.TestParseClusterConfigAcceptsNonLoopbackMQTTTCPListener`, `cmd/narumq.TestValidateClusterConfigReportsFirstInvalidField`, `internal/application/connect.TestConnectRecordsAuthenticationDenialsWithoutChangingConnAck`, `internal/application/connect.TestCmmConnectRejectsCredentialModeMismatchesWithoutMutation`, `tests/integration.TestDynamicClusterPasswordAuthenticationProcessAcceptance` | 통과 | [Cluster 평문 TCP(G3) 실행 기록](#cluster-plaintext-tcp-g3-execution) |
| `CFG-SCN-30` | SEC-02, TEN-07, API-02 | Command-line flag, security config, cluster OIDC config와 Management API에 duration 값을 넣는다. Security config의 새 JWT·`tenantLimits` key에는 integer millisecond를 넣고 legacy key, string, 음수와 상한 초과도 시험한다. API request·response에서 새 field 이름을 확인하고 legacy 이름을 보낸다. | Flag는 Go duration 문자열을 유지한다. JSON duration은 `Millis` 접미사와 integer millisecond를 사용한다. Legacy config key와 API field는 거절한다. | `cmd/narumq.TestPositiveMillisDuration`, `cmd/narumq.TestParseSecurityConfig_Rejections`, `cmd/narumq.TestParseSecurityConfig_TenantLimitsMaxWaitMillis`, `cmd/narumq.TestParseSecurityConfig_TenantLimitsRejectInvalidProfiles`, `cmd/narumq.TestClusterManagementAccessConfiguration`, `tests/contracts/management-api-v1` | 통과 | [G5 duration 표기 실행 기록](#g5-duration-millis-execution) |

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

<a id="cluster-mqtt-tls-g2-g10-execution"></a>
### Cluster MQTT TLS(G2·G10) 실행 기록

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

저장소 검증은 다음 결과를 반환했다.

| Gate | 실행 명령 | 결과 |
| --- | --- | --- |
| Traceability | `go run ./tools/verify traceability` | `PASS`; P0 `69/81` mapped, exceptions `12` |
| Markdown links | `go run ./tools/verify markdown-links` | `PASS`; 152 files, 2,153 local links |
| Local | `GOMAXPROCS=4 go run ./tools/verify local` | `local: PASS`, `coverage: PASS`; branch-instrument, architecture, fuzz, package tests와 traceability 통과 |

<a id="cluster-runtime-metrics-g6-execution"></a>
### Cluster runtime metrics(G6) 실행 기록

실행 환경은 Windows/amd64, Go 1.26.5다. 구현 전 test-first 실행은 `RuntimeMetrics` field와 `runtimeMetricsInterval` method가 없어 build 단계에서 실패했다.

```sh
go test ./cmd/narumq -run TestParseClusterConfigRuntimeMetrics -count=1
```

구현 뒤 parser와 3-node in-process runtime 시험이 통과했다.

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
| Traceability | `PASS`; P0 mapped `69/81`, exceptions `12` |
| Markdown links | `PASS`; `go run ./tools/verify markdown-links`에서 문서 152개와 로컬 링크 2,159개를 확인했다. |
| Local gate | `go run ./tools/verify local`에서 `local: PASS`, `coverage: PASS`. Branch-instrument, architecture, fuzz, package tests와 traceability가 통과했다. |


<a id="api-09-mqttlimits-execution"></a>
### API-09 mqttLimits 실행 기록

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
| CFG-SCN-28 3-node process acceptance | `PASS` (8.58s); policy PUT/retry/conflict, old/new connection limits, all-node state, restart와 drift |
| Existing TCP limit regression | `PASS` (0.551s); connection limit changes affect new connections only, oversized CONNACK rejection |
| Existing cluster security regressions | `PASS`; `TestDynamicClusterClientBlocklistProcessAcceptance` (10.06s) and `TestDynamicClusterMQTTTLSClientCertificateProcessAcceptance` (1.54s) |
| Audit event contract regression | `PASS`; router constructor callsite includes the new optional policy service |
| Traceability | `PASS`; P0 mapped `70/81`, exceptions `11` |
| Markdown links | `PASS`; 152 documents, 2,177 local links |
| Local gate | `go run ./tools/verify local`: `local: PASS` (193.88s), `coverage: PASS`; branch instrumentation, traceability, architecture, fuzz, package and race checks passed. |

<a id="cluster-plaintext-tcp-g3-execution"></a>
### Cluster 평문 TCP(G3) 실행 기록

실행 환경은 Windows/amd64, Go 1.26.5다. 현재 작업 트리에서 production 코드는 바꾸지 않고 parser 시험만 추가했다. 첫 local gate는 `cmd/narumq/cluster_config_test.go`의 gofmt 검사에서 중단됐다. `gofmt -w cmd/narumq/cluster_config_test.go`를 실행한 뒤 parser 시험과 local gate를 재실행했다. 현재 checkout에는 remote-tracking branch가 없어서 `git merge-base HEAD HEAD`의 결과 `84d3a41b8b6feede6ae2856508c111c5f61674fd`를 CI `--base`로 사용했다.

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
| 3-node 평문 TCP password acceptance | 통과 (9.41s) |
| Build | 통과 |
| Markdown links | 첫 실행은 G3 anchor 부재로 2개 링크 오류를 반환했다. Anchor 추가 후 재실행은 `PASS` (152 files, 2,182 local links). |
| Traceability | `PASS`; P0 mapped `70/81`, exceptions `11` |
| Local gate | 첫 실행은 `cmd/narumq/cluster_config_test.go`의 gofmt 사전 검사에서 중단됐다. Gofmt 적용 뒤 `local: PASS` (194.34s), `coverage: PASS`; branch-instrument, traceability, fuzz, architecture와 전체 package·integration 시험이 통과했다. |
| CI gate | `go run ./tools/verify ci --base 84d3a41b8b6feede6ae2856508c111c5f61674fd`: `ci: PASS` (1541.36s). |


<a id="g5-duration-millis-execution"></a>
### G5 duration Millis 실행 기록

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
go test ./tests/integration -count=1 -run '^TestDashboardProvisioningMatchesContracts$'
go run ./cmd/narumq -tcp-listen 127.0.0.1:18831 -security-config .artifacts/verification/g5-duration-millis/legacy-security.json
go run ./cmd/narumq -tcp-listen 127.0.0.1:18832 -security-config .artifacts/verification/g5-duration-millis/current-security.json
```
기준 비교는 detached checkout commit `23005e1a0d819a357507ce173b8d0f89cf40cf19`에서 수행했다. Historical release acceptance 두 시험은 현재 checkout과 기준 checkout에서 모두 `load cluster config: cluster config has invalid JSON or field types`로 실패했다. Dashboard contract 시험은 panel 6의 datasource/query 형식을 기준 checkout에 맞추고 duration selector만 바꾼 뒤 현재와 기준 checkout에서 통과했다. 기준 checkout은 비교 뒤 제거했다.

| 확인 | 결과 |
| --- | --- |
| Millis parser와 security config 경계 | 통과. 0·최대값, 새 key, legacy key, 문자열·음수·상한 초과를 확인했다. |
| `cmd/narumq` 전체 시험 | 통과 |
| Management API와 contract regression | 통과 |
| Migrated benchmark·chaos·compatibility fixture 시험 | 통과 |
| Build와 vet | 통과 |
| Management RBAC, credential expiry, drain, rebalance, audit process acceptance | 통과 |
| N-1/N release compatibility process acceptance | 통과. |
| Historical release acceptance | `TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance`와 `TestClusterBackupRestoreHistoricalReleaseProcessAcceptance`는 현재와 기준 checkout에서 모두 `load cluster config: cluster config has invalid JSON or field types`로 실패했다. 기준 checkout에서 같은 실패를 확인해 이 기존 문제는 수정하지 않았다. |
| Dashboard provisioning contract | 통과. Panel 6의 datasource/query 형식은 기준 checkout과 같고 `uptimeMs` selector만 `uptimeMillis`로 바꿨다. |
| Legacy JSON CLI smoke | 예상대로 종료 코드 1과 `unknown field "cacheTtl"`을 반환했다. |
| Current JSON CLI smoke | Listener 시작 log를 확인했다. Managed process로 종료했다. |
| Markdown links | 통과 (152 files, 2,186 local links) |
| Traceability | 통과 (P0 mapped `70/81`, exceptions `11`) |
| Local gate | 통과 (206.30s). Branch instrumentation, traceability, fuzz, architecture, package·race 시험과 coverage가 모두 통과했다. |
| CI gate | 통과 (1484.27s). Coverage PASS, mutation efficacy `100%`, mutant coverage `98.85%`(6 not covered). |

## 7. 미결정 사항

- `SPEC-TBD-02`와 `SPEC-TBD-03`의 확정 위치는 이 spec이다. 이번 문서는 새 제품 값을 정하지 않는다.
- 목표 설정 구조와 API-09 정책은 [ADR-0052](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)(Proposed)를 따른다.
