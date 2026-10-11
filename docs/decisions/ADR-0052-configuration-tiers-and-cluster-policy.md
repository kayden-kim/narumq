# ADR-0052: 설정을 범주로 나누고 cluster 정책은 commit 상태를 기준으로 한다

| 항목 | 값 |
| --- | --- |
| 상태 | `Proposed`. §1.1의 `ClusterPolicy` commit·version·reconciliation·`GET/PUT /api/v1/cluster/policy`·metric과 §1.2의 `mqttLimits` 적용 시점은 2026-10-10 사용자 결정으로 채택했다(G1·API-09). §1.4의 `cluster-policy` feature gate도 `mqttLimits`에 한해 채택했다. 기존 `TenantQuota` proposal 억제와 top-level `tenantQuotas`·`clusterPolicy` 충돌 거절은 §1.4에 명시한 동작을 유지한다. §1.5의 설정 parser 중복 key 거절 규칙과 secret 파일 규칙 중 standalone `-http-auth-token-file` 전환은 같은 날 채택했다(G4·G9). §1.5의 Cluster MQTT TLS(`mqtt.tls`)와 MQTT·peer 인증서 분리 규칙도 채택했다(G2·G10). Cluster runtime metrics의 node 정적 설정 `runtimeMetrics`도 채택했다(G6). §1.5의 Cluster 평문 MQTT TCP 주소 허용 규칙도 2026-10-10 사용자 결정으로 채택했다(G3). §1.5의 JSON duration 표기 규칙도 같은 날 채택했다(G5). §1.1.2 설정 조회(G7-a)와 §1.1.3 `tenantQuotas` policy(G7-b)는 2026-10-11 사용자 결정으로 채택했다. §1.6의 standalone 고정 식별자 규칙은 2026-10-11 사용자 결정으로 채택했다(G8). 코드 동작은 바꾸지 않았다. 나머지 정책 항목은 `Proposed`로 남는다. |
| 결정일 | 2026-10-11 |
| 결정자 | NaruMQ 프로젝트 사용자 |
| 관련 요구사항 | API-09, API-08, UI-01~UI-10, SEC-07, TEN-06~TEN-08, CLU-11 |
| 관련 스펙·계약 | [실행 옵션과 설정 스펙](../specs/configuration-and-runtime-options.md), [Process configuration v1](../contracts/process-configuration-v1.md), [Management API v1](../contracts/management-api-v1.md), [replicated-state-v1](../contracts/replicated-state-v1.proto) |
| 관련 아키텍처 문서 | [Cluster architecture](../architectures/cluster-architecture.md), [Management HTTP adapter module design](../architectures/management-http-module.md) |
| 관련 ADR | [ADR-0017](ADR-0017-tenant-storage-quota.md), [ADR-0032](ADR-0032-feature-activation-approval-policy.md), [ADR-0034](ADR-0034-node-local-tenant-connection-and-traffic-limits.md), [ADR-0043](ADR-0043-management-api-rate-limit.md), [ADR-0045](ADR-0045-operations-dashboard-grafana.md), [ADR-0049](ADR-0049-mqtt-credential-revocation.md), [ADR-0051](ADR-0051-cluster-update-strategy.md) |
| 이 ADR이 대체하는 이전 ADR | 없음. Accepted가 되면 ADR-0034의 cluster 설정 원천과 ADR-0049의 security config 전체 정적 설정 중 ACL·`tenantLimits` 범위를 대체한다. |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

이 Proposed ADR은 설정을 다섯 범주로 나눈다.

| 범주 | 예 | 변경 방법과 console |
| --- | --- | --- |
| A. 실행 명령 | `-hash-password`, `-audit-verify`, `-cluster-restore` | CLI 전용이다. |
| B. Build identity | `ldflags`로 넣는 release version과 feature 목록 | binary를 교체한다. |
| C. Node 정적 설정 | node identity, data·audit directory, listen·advertised 주소, TLS·secret 파일 경로, MQTT 인증 identity(`jwt`, `identities`, `passwordIdentities`, `mtlsIdentities`), delivery·control queue capacity, cluster runtime metrics(`runtimeMetrics.outputFile`, `runtimeMetrics.intervalMillis`) | restart 또는 rolling restart로 반영한다. Console은 읽기만 한다. Secret은 존재 여부만 보여 준다. |
| D. Cluster 정책 | `mqttLimits`, `listenerAdmission`, `managementRateLimit`, `publishBackpressure`, `tenantQuotas`, `tenantLimits`, `acl` | Dynamic cluster에서 console로 변경할 수 있다. Raft commit 뒤 적용한다. |
| E. Runtime 상태 | membership, feature activation, drain, rebalance, credential revocation, blocklist, backup | 기존 전용 API를 사용한다. 이 범주는 설정 파일이 아니다. |

### 1.1 Cluster 정책의 권한과 reconciliation

Dynamic cluster에서 Raft에 commit된 `ClusterPolicy`와 단조 증가하는 `version`(`uint64`)이 적용된 정책의 권위다. `-cluster-config`의 선택 object `clusterPolicy`는 versioned desired value를 제공한다. Commit 전에는 설정 파일만으로 cluster 전체 값을 바꾸지 않는다.

`ClusterPolicy`는 아래 정책을 담는다.

| 정책 | 값 |
| --- | --- |
| `mqttLimits` | `maxPacketSize`, `receiveMaximum`, `connectTimeoutMillis`, `packetReadTimeoutMillis`, `writeTimeoutMillis` |
| `listenerAdmission` | TCP·TLS listener별 `maxConnections`, `connectionRate` |
| `managementRateLimit` | `requestRate`, `authFailureRate` |
| `publishBackpressure` | `minFreeBytes`, `maxCommitLagMillis` |
| `tenantQuotas` | versioned tenant storage quota |
| `tenantLimits` | tenant connection, session, subscription, message·byte rate와 payload limit |
| `acl` | tenant, role, effect, action과 topic filter를 묶는 ACL rule |

Node는 시작할 때 local `clusterPolicy`와 committed value를 비교한다.

1. 아직 `ClusterPolicy`가 commit되지 않았으면 version 0으로 본다. Feature가 활성화되지 않았거나 committed version이 0이면 node는 현재처럼 자기 파일의 값을 사용한다.
2. Local version이 committed version보다 높으면 새 `ClusterPolicy` commit을 제안한다.
3. Local version이 낮거나 같은 version의 내용이 다르면 제안하지 않는다. Commit된 value를 유지한다. `narumq_cluster_policy_config_mismatch` gauge를 `1`로 설정하고 Warn log를 한 번 남긴다.
4. Version과 내용이 모두 같으면 mismatch가 아니다.
5. 제안한 value는 commit된 뒤에만 적용한다. Runtime 기준과 다른 파일 값은 낮은 version으로 commit 상태를 되돌리지 않는다.

Cluster는 `narumq_cluster_policy_committed`와 `narumq_cluster_policy_version` gauge도 제공한다. `narumq_cluster_policy_committed`는 정책이 commit됐는지, `narumq_cluster_policy_version`은 적용 version을 나타낸다.

Management API는 아래 경로로 정책을 조회하고 변경한다.

| Method | Path | 권한 | 결과 |
| --- | --- | --- | --- |
| `GET` | `/api/v1/cluster/policy` | `ReadOnly` | `version`과 전체 `ClusterPolicy`를 반환한다. |
| `PUT` | `/api/v1/cluster/policy` | `ClusterAdmin` | `If-Match: "<version>"`과 `Idempotency-Key`가 필요하다. 성공하면 version을 1 올린 value를 commit한다. 현재 version이 다르면 `409 conflict`를 반환한다. |

Mutation은 audit action `cluster.policy.update`로 기록한다. Audit detail에는 이전 version과 새 version을 넣는다. HTTP status와 JSON schema는 [Management API v1 contract §1](../contracts/management-api-v1.md#1-phase-1-operation-profile)와 [OpenAPI JSON](../contracts/management-api-v1.openapi.json)에서 확정한다.

### 1.1.1 1차 구현 계약 (`mqttLimits`)
+
첫 구현은 `mqttLimits`만 저장한다. 다른 §1.1 정책 항목은 replicated state와 API에 포함하지 않는다.
+
| JSON field | Type | 허용 범위 | 기본값 |
| --- | --- | --- | --- |
| `maxPacketSize` | `uint32` | `1..268435455` | `1048576` |
| `receiveMaximum` | `uint32` | `1..65535` | `64` |
| `connectTimeoutMillis` | `uint32` | `1..4294967295` | `10000` |
| `packetReadTimeoutMillis` | `uint32` | `1..4294967295` | `30000` |
| `writeTimeoutMillis` | `uint32` | `1..4294967295` | `5000` |
+
`clusterPolicy`가 없는 설정과 committed policy version 0은 위 기본값을 사용한다. 적용 값은 committed version 1 이상, local `clusterPolicy`, 기본값 순서로 고른다. 변경은 새 연결부터 적용하며 기존 연결은 accept 시점의 한도를 유지한다. Control·delivery queue 256은 node 정적 설정이다.
+
요청 JSON은 `{"mqttLimits":{...}}`이며 object와 다섯 field를 모두 요구한다. 알 수 없는 key, 누락 값과 범위 밖 값은 `400 invalid_request`다.
+
`GET /api/v1/cluster/policy`는 `ReadOnly` 권한을 요구하고 `ETag: "<version>"`을 반환한다. Commit 전 응답은 version `0`, policy `null`이다. Feature 활성 여부와 관계없이 조회할 수 있다.
+
`PUT /api/v1/cluster/policy`는 `ClusterAdmin` 권한을 요구한다. 정확히 하나의 `Idempotency-Key`와 `If-Match: "<version>"`을 요구한다. `If-Match`는 선행 0이 없는 uint64 decimal이어야 하며 `math.MaxUint64`는 허용하지 않는다. 성공 응답은 다음 version과 policy를 반환하고 `ETag`를 갱신한다. Feature 비활성은 `409 feature_inactive`, stale version은 non-retryable `409 cluster_policy_version_conflict`, 최근 key와 다른 요청 내용은 `409 idempotency_conflict`다. Quorum 실패는 기존 retryable `503` 규칙을 따른다.
+
Idempotency state는 최근 적용한 update의 key 하나만 보관한다. 같은 key와 같은 이전 version·새 version·정책은 상태 변경 없이 재생한다. 오래된 key 재사용은 version CAS에서 거절한다.
+
성공 mutation은 `cluster.policy.update` audit event를 남긴다. Target은 `cluster_policy`와 cluster ID다. `details`에는 `previousVersion`과 `newVersion`을 기록한다.

### 1.1.2 적용 설정 조회 (`G7-a`)

Dynamic cluster는 `GET /api/v1/nodes/{nodeId}/config`로 해당 node가 실제로 적용하는 설정을 조회한다. 이 API는 node 정적 설정과 cluster-wide committed policy의 적용 출처를 함께 보여 준다. Standalone에는 route를 제공하지 않는다.

이 route는 `ClusterAdmin` 권한을 요구한다. Node 고유 정보와 특정 node만 대상으로 하는 API는 `/api/v1/nodes/{nodeId}/...`를 사용한다. 요청을 받은 node의 `nodeId`와 path의 canonical decimal ID가 다르면 `409 node_not_local`을 반환한다. API는 원격 node에 proxy하지 않으므로 조회 대상 node에 직접 요청해야 한다. Raft commit으로 cluster 전체에 적용되는 `/api/v1/nodes/{nodeId}/drain`은 이 규칙에서 제외한다.

응답에는 build·active feature, node·management·MQTT listener 설정, TLS 인증서와 key 파일의 존재 여부, cluster policy reconciliation, effective MQTT limits와 tenant quotas, replication·publish backpressure·runtime metrics, security policy를 포함한다. Secret 원문과 secret file 경로, password hash, token, identity 목록은 반환하지 않는다. Secret은 설정 여부나 개수만 반환한다. OIDC·JWT의 JWKS URL은 URL userinfo를 제거한다.

`tenantQuotas.source`는 `none`, `replicatedProfile`, `clusterPolicy` 중 하나다. `value`는 적용된 quota profile이며 출처가 `none`이면 `null`이다. 상세 JSON schema는 [Management API v1 contract](../contracts/management-api-v1.md)와 OpenAPI가 정한다.

### 1.1.3 ClusterPolicy tenant quota (`G7-b`)

`ClusterPolicy`는 선택 field `tenantQuotas`로 versioned tenant storage quota를 담을 수 있다. 이 field를 쓰려면 `cluster-policy`와 별도로 build feature `cluster-policy-tenant-quotas`를 활성화해야 한다. 해당 feature가 비활성화된 동안 reconciler는 quota가 포함된 policy를 제안하지 않고, Management API PUT은 `409 feature_inactive`를 반환한다.

운영자는 `PUT /api/v1/cluster/policy`에서 기존 `mqttLimits`와 함께 `tenantQuotas`를 갱신한다. Request body는 최대 512 KiB다. `tenantQuotas`를 `null`로 보내거나 생략하면 policy가 quota를 관리하지 않는다. 이때 legacy committed `TenantQuota` profile이 적용된다. Policy quota가 있으면 그 quota가 legacy profile보다 우선하며 admission rejection에는 policy version을 기록한다. Policy field를 제거한 새 version이 commit되면 legacy profile로 돌아간다.

`tenantQuotas`가 저장된 뒤에는 해당 field를 모르는 build로 rollback할 수 없다. Feature gate, replicated schema와 release 호환 규칙은 [Replicated-state v1](../contracts/replicated-state-v1.md), [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md)에서 정한다. 설정 파일의 strict shape는 [Process configuration v1](../contracts/process-configuration-v1.md)에서 정한다.

GitOps 파일은 desired value의 원천이다. Console이 API로 정책을 바꾸면 committed value와 파일 사이에 drift가 생길 수 있다. Metric으로 drift를 표시한다. 운영자는 GET 결과를 같은 version으로 저장소에 반영해 차이를 없앤다.

Standalone에는 commit state가 없다. Standalone은 파일과 CLI 값만 사용한다. Runtime 설정 변경 API는 지원하지 않는다.

### 1.2 적용 시점

- `mqttLimits`는 새 연결부터 적용한다. 기존 연결은 CONNACK에서 광고한 값을 유지한다.
- 다른 cluster 정책은 commit을 apply한 뒤 시작하는 검사부터 적용한다.
- restart가 필요한 값은 `ClusterPolicy`에 넣지 않는다.

### 1.3 보안 설정

이 1차 Dynamic cluster 구현의 `ClusterPolicy`는 `mqttLimits`와 선택적 `tenantQuotas`를 포함한다. ACL과 `tenantLimits` 이행은 범위 밖이며 현재 security config에 남는다. Authentication identity와 secret은 node 정적 설정에 남기며 Raft log와 backup archive에 복제하지 않는다.

Standalone에서는 ACL과 `tenantLimits`를 계속 security config에서 읽는다. Credential rotation은 [ADR-0049](ADR-0049-mqtt-credential-revocation.md)의 rolling restart 경계를 유지한다.

### 1.4 Feature gate와 이행

새 replicated `ClusterPolicy` command는 build feature `cluster-policy`가 있어야 한다. 운영자는 [ADR-0032](ADR-0032-feature-activation-approval-policy.md)의 절차에 따라 이 feature를 활성화한다.

Feature가 활성화되기 전에는 각 node가 현재 파일 값을 사용한다. 활성화 뒤에도 version 1 이상 `ClusterPolicy`가 commit되기 전까지는 version 0이며 각 node가 자기 파일 값을 사용한다.

기존 `TenantQuota` command는 과거 log replay와 policy field가 없을 때의 호환성을 위해 유지한다. `cluster-policy`가 활성화되어도 legacy controller는 `TenantQuota` 변경을 제안하지 않는다. `cluster-policy-tenant-quotas`가 활성화된 뒤 `ClusterPolicy.tenantQuotas`로 quota를 변경한다. top-level `tenantQuotas`와 `clusterPolicy`를 함께 지정하면 시작을 거절한다. Policy field를 생략하면 legacy `TenantQuota` committed profile이 계속 적용된다.

### 1.5 형식과 Cluster MQTT TLS

- Command-line flag는 Go duration 문자열을 쓴다. JSON의 duration 값은 `Millis` 접미사가 붙은 field 이름과 음수가 아닌 정수 millisecond를 쓴다. 이 규칙은 2026-10-10 사용자 결정으로 채택했다(G5). Security config의 JWT, cluster `management.oidc`와 `tenantLimits.maxWait` duration field를 이 형식으로 전환했다.
- 설정 parser는 중복 key, 알 수 없는 key와 뒤따르는 JSON value를 거절한다. 이 규칙은 2026-10-10 사용자 결정으로 먼저 채택했다. Security config와 peer-only config parser에도 적용했다(G4).
- Secret은 파일로만 받는다. Standalone `-http-auth-token`은 `-http-auth-token-file`로 바꾼다. 이 규칙은 2026-10-10 사용자 결정으로 채택해 구현했다(G9).
- Environment variable은 설정 원천으로 사용하지 않는다.
- Cluster 평문 MQTT TCP(`mqtt.tcpListenAddress`)는 loopback 제한 없이 유효한 host:port를 허용한다. Wildcard `0.0.0.0`, `[::]`와 DNS 이름도 포함한다. 필수 `mqtt.securityConfigFile`은 authentication method를 하나 이상 요구한다. 따라서 credential이나 client certificate가 없는 CONNECT는 거절한다. Literal loopback 제한은 standalone `-tcp-listen`에만 적용한다. 평문 TCP는 username·password와 JWT를 암호화하지 않는다. 이 노출 위험은 운영자가 network 경계에서 관리한다. 이 규칙은 2026-10-10 사용자 결정으로 채택했다(G3).
- Cluster MQTT TLS에는 node 정적 설정인 `mqtt.tls` object를 추가한다. 이 object는 `certFile`, `keyFile`, `caFile`, `clientAuth`, `minVersion`을 가진다.
- MQTT client 인증서와 Raft peer 인증서는 분리한다.
- `tlsListenAddress`가 있으면 `mqtt.tls`가 필요하며 `peerTLS`로 대체하지 않는다.
- Cluster runtime metrics는 node 정적 설정 `runtimeMetrics` object(`outputFile`, `intervalMillis`)로 지정한다. `-cluster-config`와 runtime metrics flag는 함께 쓸 수 없다(G6).

### 1.6 Standalone과 console

Standalone은 개발과 단일 node 검증 경계로 유지한다. 기존 flag 기반 실행은 유지하되, §1.5의 secret 파일 flag 변경은 적용한다.
Standalone Management API는 cluster ID `narumq-cluster-1`과 node ID `narumq-node-1`을 고정값으로 사용한다. 이 값을 바꾸는 flag나 설정은 제공하지 않는다. Standalone은 개발과 단일 node 검증 경계이므로 여러 standalone process를 하나의 Dashboard나 Prometheus에서 구분하는 기능은 지원 범위가 아니다. Standalone node ID는 decimal이 아니므로 Dynamic cluster의 canonical decimal node ID 규칙과 Dashboard의 decimal `node_id` 규칙을 적용하지 않는다. 이 규칙은 2026-10-11 사용자 결정으로 채택했다(G8).


Console은 Management API만 사용한다. 이 ADR은 UI 기술을 정하지 않는다. 전용 console을 만들려면 custom web UI 제외를 정한 ADR-0045 §7을 대체하는 별도 ADR이 필요하다.

## 2. 배경

현재 설정 구조에는 다음 차이가 있다.

| ID | 현재 상태와 코드 근거 |
| --- | --- |
| G1 | Cluster MQTT의 packet, timeout, control·delivery queue 값은 `cluster_runtime.go`의 `clusterDefault*`와 `runClusterMode`에 고정되어 있다. 2026-10-10 해소: `mqttLimits`는 committed `ClusterPolicy`와 `/api/v1/cluster/policy`로 조정한다. Control·delivery queue 256은 node 정적 설정으로 남는다. |
| G2 | `clusterMQTTListeners`는 Raft peer certificate와 key를 재사용한다. MQTT TLS 설정에는 client CA와 client-auth가 없어 handshake에서 client certificate를 요청하지 않는다. 2026-10-10 해소: `mqtt.tls`가 MQTT 인증서와 client-auth를 설정하고 peer 인증서와 분리한다. |
| G3 | Standalone `-tcp-listen`은 literal loopback만 허용한다. Cluster `mqtt.tcpListenAddress`는 wildcard를 포함한 유효한 host:port를 허용한다. 2026-10-10 해소: cluster 평문 TCP는 secure mode에서 loopback 제한 없이 허용한다고 명문화했다. 코드 동작은 바꾸지 않았다. |
| G4 | `security_config.go`와 `raft_peer_config.go`는 중복 JSON key를 검사하지 않는다. Cluster config parser는 검사한다. 2026-10-10 해소: 세 parser가 모두 중복 key를 거절한다. |
| G5 | Flag와 security config는 Go duration을 쓴다. Cluster config는 millisecond 정수를 쓴다. 2026-10-10 해소: command-line flag만 Go duration 문자열을 유지하고 모든 JSON duration field는 `…Millis` 정수 millisecond를 쓴다. |
| G6 | Runtime metrics flag는 standalone에서만 지원한다. 2026-10-10 해소: cluster는 `runtimeMetrics` object를 사용한다. |
| G7 | 실제 적용 설정 조회 API와 `tenantQuotas` 변경 API는 없다. 2026-10-11 해소: Dynamic cluster 전용 설정 조회 API와 `ClusterPolicy.tenantQuotas` 변경 API를 제공한다. |
| G8 | Standalone Management API는 cluster ID `narumq-cluster-1`과 node ID `narumq-node-1`을 사용한다. 2026-10-11 해소: 고정값을 standalone 규칙으로 명문화했다(§1.6). 코드 동작은 바꾸지 않았다. |
| G9 | Standalone은 2026-10-10 해소: `-http-auth-token`은 제거하고 `-http-auth-token-file`만 받는다. Cluster는 `clusterAdminTokenFile`을 사용한다. |
| G10 | Cluster MQTT TLS에는 minimum TLS version과 client-auth를 설정하는 field가 없다. 2026-10-10 해소: `mqtt.tls.minVersion`은 TLS 1.2 또는 1.3을, `mqtt.tls.clientAuth`는 `none`, `request`, `require`를 설정한다. |
G1·API-09의 `mqttLimits`와 G7의 설정 조회·`tenantQuotas` 범위는 채택해 구현한다. G8은 현재 동작을 명문화했다. 다른 정책 범위와 일반 `ClusterPolicy` 항목은 아직 채택하지 않았다. 현재 동작은 [실행 옵션과 설정 스펙](../specs/configuration-and-runtime-options.md)에 기록한다.

## 3. 결정 요인

1. API-09가 요구하는 GitOps와 runtime 변경 사이의 명확한 권한.
2. Cluster node가 같은 committed policy를 사용해야 한다.
3. Secret이 Raft log와 backup archive에 남지 않아야 한다.
4. ADR-0017의 versioned quota 적용 방식을 재사용해야 한다.
5. GitOps 변경 이력을 확인할 수 있어야 한다.

## 4. 선택 근거

Commit된 `ClusterPolicy`를 적용 권위로 두면 node가 같은 값을 사용한다. Version이 낮거나 충돌하는 파일은 현재 commit을 되돌리지 않는다. Higher version만 proposal로 보내므로 GitOps가 의도한 변경과 실제 적용 상태를 구분할 수 있다.

Node 주소, listener, TLS certificate와 identity는 node-local 경계에 둔다. ACL과 `tenantLimits`는 Dynamic cluster에서 같은 commit된 정책으로 적용한다. Authentication identity와 secret은 파일에 남겨 복제 log와 backup에서 제외한다.

ADR-0017은 versioned `tenantQuotas` command로 local config와 committed state를 비교한다. 이를 cluster 정책의 기본 reconciliation model로 확장하면 새 정책과 API가 같은 version 경계를 사용할 수 있다. 기존 `TenantQuota` command는 replay 호환성을 위해 남긴다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| GitOps 파일이 기준이고 console은 조회만 한다. | 설정 변경이 Git 이력에만 남는다. | Runtime policy를 즉시 바꿀 수 없다. | 승인된 API-09 목표는 GitOps와 runtime 변경을 모두 다룬다. |
| API만 기준으로 쓰고 파일은 bootstrap 뒤 사용하지 않는다. | 적용 값의 권위가 단순하다. | Desired state가 Git에서 관리되지 않는다. | GitOps 추적성을 잃는다. |
| 파일 변경을 hot reload한다. | 재시작 없이 변경할 수 있다. | Node마다 다른 시각에 다른 정책을 적용할 수 있다. | Commit 이전 일부 적용을 허용하지 않는다. |
| Authentication identity까지 commit 정책에 넣는다. | 보안 설정이 node 간 일치한다. | Password hash와 credential 정보가 Raft log와 backup에 남는다. | Secret과 인증 자료는 node 정적 설정에 둔다. |

## 6. 결과

### 긍정적 결과

- 설정과 runtime 상태의 소유 경계를 구분한다.
- Dynamic cluster는 commit된 cluster policy를 같은 기준으로 적용한다.
- 오래된 선언 파일이 최신 committed policy를 낮추지 않는다.
- GitOps version과 runtime 변경의 drift를 관찰할 수 있다.

### 부정적 결과와 비용

- 운영자는 GitOps 파일과 runtime API 두 경로를 관리해야 한다.
- Drift metric을 감시해야 한다.
- Commit된 policy 변경은 cluster 전체에 영향을 준다.

### 후속 작업

1. `clusterPolicy`를 반영할 Process configuration contract v2 또는 새 contract revision을 만든다.
2. `ClusterPolicy` replicated-state schema와 Raft command를 추가한다.
3. Management API contract와 OpenAPI를 갱신한다.
4. 전용 console이 필요하면 ADR-0045 §7을 대체하는 console ADR을 작성한다.

## 7. 검증

| 기준 | 확인 방법 | 합격 기준 | 현재 증거 |
| --- | --- | --- | --- |
| API-09 version 충돌·idempotency | `TestClusterPolicyApplyFeatureCASAndLatestKeyReplay`, `TestClusterPolicyServiceUpdateReplayConflictAndRead`, `TestClusterPolicyHTTPRejectsInvalidRequestAndMapsErrors` 실행 | Feature gate, CAS, 최신 idempotency key 재생과 stale `If-Match` 오류를 확인한다. | 통과. 세부 실행 기록은 [API-09 mqttLimits 실행 기록](../specs/configuration-and-runtime-options.md#api-09-mqttlimits-execution)에 있다. |
| API-09 cluster 적용 | `TestDynamicClusterPolicyProcessAcceptance` 실행 | 3개 node가 같은 committed `ClusterPolicy` version과 값을 적용하고 기존 연결의 MQTT 한도를 유지한다. | 통과 (3-node process acceptance 8.499초). [실행 기록](../specs/configuration-and-runtime-options.md#api-09-mqttlimits-execution). |
| API-09 feature activation·restore | `TestClusterBackupArchivePreservesClusterPolicy`, `TestDynamicClusterPolicyProcessAcceptance` 실행 | Feature 활성 전 local 설정을 사용하고 snapshot·backup 복구 뒤 committed policy를 유지한다. | 통과. Backup archive 시험과 3-node restart·drift 결과는 [API-09 mqttLimits 실행 기록](../specs/configuration-and-runtime-options.md#api-09-mqttlimits-execution)에 있다. |
| 설정 parser 중복 key 거절(G4) | `TestParseSecurityConfig_Rejections`, `TestParseRaftPeerConfigStrict`, `TestRejectDuplicateJSONKeys` 실행 | Security·peer-only config의 top-level, 중첩 object, 배열 원소 object에서 중복 key를 거절한다. | 2026-10-10 [설정 시험 기록](../specs/configuration-and-runtime-options.md#configuration-test-execution)에서 통과했다. |
| JSON duration 형식(G5) | `TestPositiveMillisDuration`, `TestParseSecurityConfig_Rejections`, `TestParseSecurityConfig_TenantLimitsMaxWaitMillis`, `TestClusterManagementAccessConfiguration` 실행 | Security config가 integer millisecond와 `Millis` field 이름만 허용하고 legacy string field를 거절한다. Management API의 duration field rename과 이전 이름 거절도 확인한다. | 통과. [G5 duration 표기 실행 기록](../specs/configuration-and-runtime-options.md#g5-duration-millis-execution)에서 결과를 확인한다. |
| Secret 파일 flag(G9) | `TestRunWithContextManagementAuthTokenFile` 실행 | 명령행 token flag와 없는·빈 파일을 거절하고, 공백을 제거한 파일 token으로 인증한다. | 2026-10-10 [설정 시험 기록](../specs/configuration-and-runtime-options.md#configuration-test-execution)에서 통과했다. |
| Cluster MQTT TLS(G2·G10) | `TestParseClusterConfigMQTTTLS`, `TestClusterMQTTListenersBindConfiguredEndpointsAndClosePartialOpen`, `TestClusterMQTTSecurityValidatesTLSClientAuthBindings`, `TestDynamicClusterMQTTTLSClientCertificateProcessAcceptance` 실행 | Parser 검증, TLS 인증 조합, peer·MQTT 인증서 분리와 3-node mTLS 동작을 확인한다. | 통과. [설정 스펙 실행 기록](../specs/configuration-and-runtime-options.md#cluster-mqtt-tls-g2-g10-execution)에서 결과를 확인한다. |
| Cluster runtime metrics(G6) | `TestParseClusterConfigRuntimeMetrics`, `TestRunClusterModeInProcess` 실행 | Parser 경계를 검증하고 cluster node가 JSONL 표본을 기록한다. | 통과. [설정 스펙 실행 기록](../specs/configuration-and-runtime-options.md#cluster-runtime-metrics-g6-execution)에서 결과를 확인한다. |

## 8. 재검토 조건

- Tenant 위임 권한(TEN-10)이 cluster policy 편집을 요구한다.
- Node마다 다른 cluster policy 값이 필요해진다.
