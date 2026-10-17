# Process configuration v1 계약

이 계약은 `narumq` process가 시작할 때 받는 명령행 옵션과 설정 파일의 형식·정적 검증·호환성 범위를 정한다. 이 계약은 같은 release의 binary와 설정 파일 조합에 적용한다.

| 항목 | 값 |
| --- | --- |
| 이름과 버전 | `narumq-process-configuration` 1.0.0 |
| 상태 | `Draft` |
| 소유자 | `NaruMQ 프로젝트` |
| 만드는 쪽(Producer) | 운영자, 배포 도구, build pipeline |
| 사용하는 쪽(Consumer) | `cmd/narumq` process |
| 방향 | process 시작 때 한 번 읽는 입력 |
| 스키마 기준 파일 | 이 문서의 표. JSON Schema는 없다. |
| 기준 스펙 | [실행 옵션과 설정 스펙](../specs/configuration-and-runtime-options.md) |
| 관련 요구사항 | API-05, API-09, SEC-02, SEC-05, MQTT-09, MQTT-10, MQTT-11, CLU-09, CLU-12, TEN-06~TEN-08 |
| 관련 결정 | [ADR-0057](../decisions/ADR-0057-mqtt-websocket-listener.md) |
| 호환성 범위 | 같은 release의 binary와 설정 파일 |
| 관련 시험 | §5 |

## 1. 형식과 의미

`contract`는 파일이나 명령행에 적는 내용만 보고 process 시작 단계에서 수락하거나 거절하는 규칙을 소유한다. 여기에는 flag와 key의 이름, 자료형, 필수 여부, 생략할 때의 기본값, 허용 범위, 항목 사이의 정적 검증과 엄격한 파싱 규칙이 포함된다. `spec`은 실행 중인 시스템이 설정을 어떻게 사용하는지를 소유한다. 여기에는 실행 모드 선택, 적재 순서, 시작 실패, 변경을 반영하는 경로, node-local과 cluster-wide 적용 범위가 포함된다. 각 값이 MQTT나 Management API 동작에 주는 효과는 기존 도메인 spec이 계속 소유한다. 예를 들어 Receive Maximum 초과 시 DISCONNECT 동작은 MQTT spec이 소유한다.

### 1.1 명령행 옵션

출처는 `cmd/narumq/main.go`의 `runWithContext`다. 기본값은 별도 표기가 없으면 해당 flag 선언값이다.

| Flag | 자료형 | 기본값 | 허용 값·검증 | 사용 모드 |
| --- | --- | --- | --- | --- |
| `-hash-password` | bool | `false` | 다른 flag를 명시하면 거절한다. 위치 인수도 허용하지 않는다. | password hash 전용 |
| `-audit-verify` | string | `""` | 비어 있으면 안 된다. 다른 flag와 위치 인수를 거절한다. | audit 검증 전용 |
| `-cluster-config` | string | `""` | 파일 경로가 필요하다. `-cluster-restore` 외의 flag와 위치 인수를 거절한다. | dynamic cluster, cluster restore |
| `-cluster-restore` | string | `""` | `-cluster-config`가 필요하고 archive 경로가 비어 있으면 안 된다. | cluster restore |
| `-raft-peer-config` | string | `""` | 파일 경로가 필요하다. 다른 flag와 위치 인수를 거절한다. | fixed-voter peer-only |
| `-tcp-listen` | string | `""` | 주소가 있으면 literal `127.0.0.0/8` 또는 `::1` loopback이어야 한다. | standalone MQTT TCP |
| `-tls-listen` | string | `""` | 주소가 있으면 TLS listener를 연다. | standalone MQTT TLS |
| `-tls-cert` | string | `""` | TLS 또는 WSS listener를 열면 `BuildTLSConfig`가 필수로 검사한다. | standalone MQTT TLS/WSS |
| `-tls-key` | string | `""` | TLS 또는 WSS listener를 열면 `BuildTLSConfig`가 필수로 검사한다. | standalone MQTT TLS/WSS |
| `-tls-ca` | string | `""` | client auth가 `request` 또는 `require`이면 `BuildTLSConfig`가 필요 여부와 PEM을 검사한다. | standalone MQTT TLS/WSS |
| `-tls-client-auth` | string | `none` | `none\|request\|require`. `none`이면 `mtlsIdentities`가 없어야 한다. `request`이면 mTLS binding과 JWT 또는 password authentication이 필요하다. `require`이면 mTLS binding이 하나 이상 필요하다. | standalone MQTT TLS/WSS |
| `-tls-min-version` | string | `1.2` | `1.2\|1.3`만 허용한다. | standalone MQTT TLS/WSS |
| `-ws-listen` | string | `""` | 주소가 있으면 literal `127.0.0.0/8` 또는 `::1` loopback이어야 한다. `-security-config`가 필요하다. | standalone MQTT WebSocket |
| `-wss-listen` | string | `""` | 주소가 있으면 WSS listener를 연다. 기존 `-tls-*` 설정을 사용한다. | standalone MQTT WSS |
| `-tcp-max-connections` | int | `0` | 범위는 `0..1,000,000`. 0은 제한을 끈다. 0이 아닌 값은 `-tcp-listen`이 필요하다. | standalone TCP listener admission |
| `-tcp-connection-rate` | int | `0` | 범위는 `0..1,000,000`. 0은 제한을 끈다. 0이 아닌 값은 `-tcp-listen`이 필요하다. | standalone TCP listener admission |
| `-tls-max-connections` | int | `0` | 범위는 `0..1,000,000`. 0은 제한을 끈다. 0이 아닌 값은 `-tls-listen`이 필요하다. | standalone TLS listener admission |
| `-tls-connection-rate` | int | `0` | 범위는 `0..1,000,000`. 0은 제한을 끈다. 0이 아닌 값은 `-tls-listen`이 필요하다. | standalone TLS listener admission |
| `-ws-max-connections` | int | `0` | 범위는 `0..1,000,000`. 0은 제한을 끈다. 0이 아닌 값은 `-ws-listen`이 필요하다. | standalone WebSocket listener admission |
| `-ws-connection-rate` | int | `0` | 범위는 `0..1,000,000`. 0은 제한을 끈다. 0이 아닌 값은 `-ws-listen`이 필요하다. | standalone WebSocket listener admission |
| `-wss-max-connections` | int | `0` | 범위는 `0..1,000,000`. 0은 제한을 끈다. 0이 아닌 값은 `-wss-listen`이 필요하다. | standalone WSS listener admission |
| `-wss-connection-rate` | int | `0` | 범위는 `0..1,000,000`. 0은 제한을 끈다. 0이 아닌 값은 `-wss-listen`이 필요하다. | standalone WSS listener admission |
| `-max-packet-size` | int | `1048576` | 범위는 `1..268435455`다. | standalone MQTT |
| `-receive-maximum` | int | `64` | 범위는 `1..65535`다. | standalone MQTT |
| `-packet-read-timeout` | Go duration | `30s` | 0보다 커야 한다. `tcp.NewServer`가 0 이하 값을 거절한다. | standalone MQTT |
| `-connect-timeout` | Go duration | `10s` | 0보다 커야 한다. `tcp.NewServer`가 0 이하 값을 거절한다. | standalone MQTT |
| `-write-timeout` | Go duration | `5s` | 0보다 커야 한다. `tcp.NewServer`가 0 이하 값을 거절한다. | standalone MQTT |
| `-control-queue-capacity` | int | `16` | 0보다 커야 한다. `tcp.NewServer`가 0 이하 값을 거절한다. | standalone MQTT |
| `-delivery-queue-capacity` | int | `64` | 0보다 커야 한다. `deliveryqueue.New`가 0 이하 값을 거절한다. 양수 상한은 없다. | standalone MQTT |
| `-retained-message-capacity` | int | `1024` | 0보다 커야 한다. | standalone MQTT |
| `-retained-byte-capacity` | int | `16777216` | 0보다 커야 한다. | standalone MQTT |
| `-http-listen` | string | `""` | `-http-auth-token-file`을 지정하지 않으면 literal loopback만 허용한다. | standalone Management HTTP |
| `-http-auth-token-file` | string | `""` | 비어 있지 않으면 파일을 읽는다. 앞뒤 공백을 제거한 내용이 비어 있으면 시작을 거절한다. | standalone Management HTTP |
| `-http-request-rate` | int | `50` | 범위는 `1..1,000,000`이다. | standalone Management API |
| `-http-auth-failure-rate` | int | `5` | 범위는 `1..1,000,000`이다. | standalone Management API |
| `-runtime-metrics-output` | string | `""` | `-runtime-metrics-interval`과 함께 지정해야 한다. | standalone runtime metrics |
| `-runtime-metrics-interval` | Go duration | `0` | `-runtime-metrics-output`과 함께 지정해야 한다. 0보다 커야 하고 listener가 하나 이상 있어야 한다. | standalone runtime metrics |
| `-security-config` | string | `""` | MQTT listener를 열 때 필요하다. `-allow-insecure-anonymous-mqtt`와 함께 쓸 수 없다. | standalone MQTT |
| `-allow-insecure-anonymous-mqtt` | bool | `false` | `-tcp-listen`이 필요하다. `-tcp-listen`은 literal loopback이어야 한다. `-tls-listen`, `-ws-listen`, `-wss-listen`과 함께 쓸 수 없다. `-security-config`와도 함께 쓸 수 없다. | standalone 개발·benchmark 전용 |

Listener admission flag 검증은 `validateListenerAdmissionFlags`가 수행한다. TCP/TLS/WS/WSS 각 admission 제한은 대응 listener를 요구한다. `BuildTLSConfig`는 TLS/WSS server certificate와 key를 요구한다. `request`와 `require`는 client CA를 요구한다.

Cluster mode에서는 `-runtime-metrics-*` flag 대신 cluster config의 `runtimeMetrics` object로 runtime metrics를 설정한다. `-cluster-config` 단독 규칙은 유지한다.

### 1.2 모드 판정 순서와 허용 조합

`cmd/narumq`는 다음 순서로 모드를 판정한다.

1. `-hash-password`
2. `-audit-verify`
3. `-cluster-config` 또는 `-cluster-restore`
4. `-raft-peer-config`
5. standalone

`-hash-password`, `-audit-verify`, `-raft-peer-config`는 다른 flag를 명시하면 거절한다. 기본값과 같은 값을 명시해도 거절한다. `-cluster-config`는 `-cluster-restore`만 함께 쓸 수 있다. 실행 모드 flag를 사용한 모드는 위치 인수를 거절한다. 정의되지 않은 flag는 parse 오류다.

### 1.3 Cluster config JSON


<a id="13-cluster-config-json"></a>

`-cluster-config <path>`는 MQTT, 관리 API와 동적 Raft 멤버십을 함께 구성하는 엄격한 JSON 모드다. 이 옵션 외의 다른 명령행 옵션이나 위치 인수는 허용하지 않으며, 기존 `-raft-peer-config <path>`와 함께 사용할 수 없다. 기존 피어 전용 config의 스키마와 디스크 동작은 바꾸지 않는다. 설정 파일에 알 수 없는 key, 중복 key, 뒤따르는 JSON 값 또는 필수 필드 누락이 있으면 process 시작을 거절한다. `releaseVersion`과 `features`는 build identity에서 오므로 JSON key로 지정하면 시작을 거절한다. 상대 경로는 process의 현재 작업 디렉터리를 기준으로 한다.

| 필드 | JSON 자료형 | 필수 조건 |
| --- | --- | --- |
| `mode` | 문자열 | 정확히 `"cluster"` |
| `clusterId` | 문자열 | UTF-8 `1..200`바이트 |
| `nodeId` | `uint64` | 양수여야 하며 로컬 인증서에 바인딩된 식별 정보와 일치해야 함 |
| `directory` | 문자열 | 영속 경로; 초기 부트스트랩 또는 새 가입 노드에서만 비어 있을 수 있음 |
| `listenAddress` | 문자열 | 로컬 피어 TCP bind 주소 `host:port` |
| `advertisedAddress` | 문자열 | 피어가 접속할 수 있는 `host:port`; 와일드카드 주소 금지 |
| `localDnsName` | 문자열 | 인증서 DNS SAN 및 commit된 로컬 식별 정보와 일치 |
| `clusterProtocolMajor`, `clusterProtocolMinor` | `uint32` | 정확히 `2`, `0` |
| `stateSchemaMajor`, `stateSchemaMinor` | `uint32` | 이 Draft에서는 정확히 `1`, `0` |
| `role` | 문자열 | 정확히 `"bootstrap"` 또는 `"joiner"` |
| `bootstrapMembers` | 멤버 객체 배열 | bootstrap에서는 서로 다른 식별 정보 3개; joiner에서는 빈 배열 |
| `seedAddresses` | 문자열 배열 | bootstrap에서는 빈 배열; joiner에서는 정규 DNS `host:port` 주소 1~16개. TLS SNI를 DNS 이름에 바인딩하므로 IP 리터럴은 거절 |
| `peerTLS` | 객체 | `certFile`, `keyFile`, `caFile` 필수; 인증서와 키가 일치하고 CA가 모든 피어를 검증해야 함 |
| `management` | 객체 | `listenAddress`, `clusterAdminTokenFile`, `auditDirectory` 필수. Token은 break-glass `ClusterAdmin` opaque secret이다. `auditDirectory`는 비어 있지 않은 경로여야 하며 `directory`와 같거나 서로 포함해서는 안 되고, 각 노드에서 달라야 한다. 선택 정수 필드 `requestRate`, `authFailureRate`는 각각 `1..1,000,000`; 생략하면 `50`, `5`를 사용한다. 선택 `apiKeys`는 `{id,keyFile,roles}` 객체 배열이며 `id`는 `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`와 일치하고 고유해야 하며, `keyFile`은 유효한 경로여야 하고 `roles`는 비어 있지 않은 고유한 `ReadOnly`, `Operator`, `SecurityAdmin`, `ClusterAdmin` 값만 포함해야 한다. Key 파일 내용은 앞뒤 공백 제거 후 비어 있지 않아야 하며 cluster admin token 및 다른 API key와 달라야 한다. 선택 `oidc`는 JWT verifier field(`issuer`, `audience`, `algorithms`, `jwksUrl`, `cacheTtlMillis`, `refreshCooldownMillis`, `clockSkewMillis`, `httpTimeoutMillis`)와 비어 있지 않은 `identities` 배열로 구성한다. 네 duration field는 `uint32` millisecond 정수이며 각각 `1..4294967295`다. 각 identity는 비어 있지 않고 최대 250 rune인 고유 `subject`와 비어 있지 않은 고유 역할 배열을 가져야 한다. JWT 필드는 MQTT `jwt` 설정과 같은 검증 규칙을 적용하며 `jwksUrl`은 HTTPS 또는 loopback HTTP여야 한다. OIDC 권한은 JWT claim이 아니라 정적 subject-to-roles binding에서 결정한다. |
| `mqtt` | 객체 | `tcpListenAddress`, `tlsListenAddress`, `securityConfigFile`가 필요하다. 주소가 비어 있으면 해당 listener를 사용하지 않는다. `tcpListenAddress`는 loopback 제한 없이 wildcard(`0.0.0.0`, `[::]`)와 DNS 이름을 포함한 유효한 host:port를 허용한다. Standalone `-tcp-listen`의 literal loopback 제한은 적용하지 않는다. 선택 정수 field `tcpMaxConnections`, `tcpConnectionRate`, `tlsMaxConnections`, `tlsConnectionRate`는 0~1000000이며 생략하면 0(제한 없음)이다. 해당 listener 주소가 비어 있으면 그 listener의 두 값은 0이어야 한다. 선택 object `webSocket`에는 필수 string `listenAddress`, `tlsListenAddress`와 선택 정수 `maxConnections`, `connectionRate`, `tlsMaxConnections`, `tlsConnectionRate`가 있다. 주소는 비어 있거나 유효한 host:port여야 한다. 각 제한은 0~1000000이며 생략하면 0이고, 0이 아닌 제한에는 대응 listen address가 필요하다. `webSocket` object를 생략하면 WS/WSS listener는 비활성이다. `tlsListenAddress` 또는 `webSocket.tlsListenAddress`가 있으면 `tls`가 필요하고, 둘 다 비어 있으면 `tls`를 지정할 수 없다. 선택 object `tls`는 `certFile`, `keyFile`, `caFile`, `clientAuth`, `minVersion`을 모두 요구한다. `certFile`과 `keyFile`은 공백이 없는 비어 있지 않은 경로여야 한다. `clientAuth`는 `none`, `request`, `require` 중 하나다. `none`이면 `caFile`은 빈 문자열이어야 한다. `request` 또는 `require`이면 `caFile`은 공백이 없는 비어 있지 않은 경로여야 한다. `minVersion`은 `1.2` 또는 `1.3`이어야 한다. 인증서·키 쌍과 client CA 파일은 listener 시작 때 읽고 검증한다. |
| `replication` | 객체(선택) | 생략하면 복제 계수 3과 쓰기 쿼럼 2를 사용한다. 지정하면 정확히 `{factor:int, writeQuorum:int}`여야 하며 3/2만 허용한다. |
| `publishBackpressure` | 선택 object | `minFreeBytes`, `maxCommitLagMillis`는 `uint64`다. Object가 있으면 두 key가 모두 필요하다. `publishBackpressure` object를 생략하면 기본값은 각각 `1073741824`, `1000`이다. `minFreeBytes` 범위는 `0..1<<50`이며 0은 disk admission을 끈다. `maxCommitLagMillis` 범위는 `100..10000`이다. |
| `runtimeMetrics` | 선택 object | `outputFile`(string)과 `intervalMillis`(`uint64`)가 필요하다. Object가 있으면 두 key가 모두 필요하다. 생략하면 기록하지 않는다. `outputFile`은 비어 있지 않고 앞뒤 공백이 없는 경로여야 하며 `directory` 또는 `management.auditDirectory`와 겹치면 안 된다. 시작할 때 파일을 만들거나 비운다. 부모 디렉터리는 자동으로 만들지 않는다. `intervalMillis` 범위는 `1..3600000`이다. JSONL 형식은 standalone `-runtime-metrics-output`과 같다(`schemaVersion` 1). |
| `tenantQuotas` | 선택 object | Legacy committed `TenantQuota` profile을 제안한다. `version`은 `uint64`이며 1 이상이다. Object가 있으면 `version`, `default`, `tenants`가 필요하다. `default`와 각 `tenants[]` 원소의 `retainedBytes`, `offlineQueueMessages`, `offlineQueueBytes`, `storageBytes`는 모두 `uint64`이며 필수다. 각 `tenants[]` 원소에는 string `tenantId`도 필요하다. 생략하면 built-in profile version 0을 사용한다. |
| `clusterPolicy` | 선택 object | `version`은 `uint64`이며 1 이상이다. `version`, `mqttLimits`가 필요하고 `tenantQuotas`는 선택이다. `mqttLimits`에는 `maxPacketSize`, `receiveMaximum`, `connectTimeoutMillis`, `packetReadTimeoutMillis`, `writeTimeoutMillis`가 모두 필요하며 `uint32`다. 범위는 각각 `1..268435455`, `1..65535`, `1..4294967295`, `1..4294967295`, `1..4294967295`다. `tenantQuotas`에는 `default`와 `tenants`가 필요하고 `version`은 허용하지 않는다. 각 quota object는 `retainedBytes`, `offlineQueueMessages`, `offlineQueueBytes`, `storageBytes`를 `uint64`로 모두 지정한다. `tenants[]`에는 `tenantId`를 추가하고 ID 오름차순으로 지정한다. |

`tenantQuotas`와 `clusterPolicy`는 함께 지정할 수 없다. Policy 안에 quota를 넣을 때는 `clusterPolicy.tenantQuotas`를 사용한다. `cluster-policy`와 `cluster-policy-tenant-quotas`는 별도 build feature다. `cluster-policy-tenant-quotas`가 활성화되기 전에는 정책 quota를 적용하지 않는다. Committed `ClusterPolicy.tenantQuotas`가 있으면 legacy `TenantQuota`보다 우선한다. Policy에서 quota field를 생략하면 committed legacy profile로 돌아간다. `cluster-policy` feature가 활성화된 뒤에는 새 legacy `TenantQuota` command를 제안하지 않는다. 기존 committed legacy profile은 policy quota가 없을 때 fallback으로 유지한다.

`ValidateTenantQuotaProfile`는 네 한도 값이 모두 양수인지 확인한다. 최대값은 retained bytes `16,777,216`, offline queue messages `8,192`, offline queue bytes `33,554,432`, storage bytes `50,331,648`다([ADR-0017 §1](../decisions/ADR-0017-tenant-storage-quota.md#1-결정)). `tenants`는 최대 1,024개다. 각 `tenantId`는 UTF-8 1~128 bytes이며 `tenancy.NewTenantID`를 통과해야 한다. Tenant ID는 오름차순이어야 하며 중복할 수 없다. `clusterPolicy.tenantQuotas`에도 같은 limits와 ID 검증을 적용한다. `clusterPolicy.tenantQuotas` 자체에는 profile version이 없으며 outer `clusterPolicy.version`을 사용한다.

`-cluster-config`에는 다음 형태의 `tenantQuotas` profile을 선택적으로 지정한다. tenant별 항목은 `tenantId` 순으로 적용된다.

```json
{
  "tenantQuotas": {
    "version": 1,
    "default": {
      "retainedBytes": 16777216,
      "offlineQueueMessages": 8192,
      "offlineQueueBytes": 33554432,
      "storageBytes": 50331648
    },
    "tenants": [
      {
        "tenantId": "acme",
        "retainedBytes": 1048576,
        "offlineQueueMessages": 256,
        "offlineQueueBytes": 1048576,
        "storageBytes": 4194304
      }
    ]
  }
}
```
`clusterPolicy.tenantQuotas`는 outer `clusterPolicy.version`을 profile version으로 사용한다. JSON에는 quota profile version을 넣지 않는다.

```json
{
  "clusterPolicy": {
    "version": 2,
    "mqttLimits": {
      "maxPacketSize": 1048576,
      "receiveMaximum": 64,
      "connectTimeoutMillis": 10000,
      "packetReadTimeoutMillis": 30000,
      "writeTimeoutMillis": 5000
    },
    "tenantQuotas": {
      "default": {
        "retainedBytes": 16777216,
        "offlineQueueMessages": 8192,
        "offlineQueueBytes": 33554432,
        "storageBytes": 50331648
      },
      "tenants": []
    }
  }
}
```

`checkClusterJSON`는 중복 key, 알 수 없는 key, 필수 key 누락과 뒤따르는 JSON value를 거절한다. `parseClusterConfig`는 형식과 정적 검증을 수행한다. `releaseVersion`과 `features`는 build identity에서만 가져온다.

Cluster mode에서 `clusterPolicy` 또는 version 1 이상 committed policy가 없을 때 쓰는 MQTT 기본값은 다음과 같다.

| 항목 | 기본값 |
| --- | --- |
| Remaining Length | `1048576` |
| Receive Maximum | `64` |
| packet read timeout | `30s` |
| CONNECT timeout | `10s` |
| write timeout | `5s` |
| control queue | `256` |
| delivery queue | `256` |
| Peer TLS | TLS 1.3 mutual TLS로 고정한다. |

`clusterPolicy`가 있으면 local `mqttLimits`를 사용한다. Feature가 활성화되고 committed version이 1 이상이면 committed `mqttLimits`가 우선한다. 활성화된 `cluster-policy-tenant-quotas` feature와 committed quota field가 함께 있으면 그 quota도 모든 node에 적용한다. Policy에 quota field가 없으면 committed legacy `TenantQuota` profile을 사용한다. Control·delivery queue 값은 policy에 포함하지 않는다.

`bootstrapMembers`의 각 항목은 정확히 `{memberId:uint64,address:string,dnsSan:string,certificateSha256:string}` 형식이다. 멤버 ID, 주소, DNS SAN과 인증서 지문의 제약은 [Raft membership v2 §1.1](raft-membership-v2.md#11-확정된-멤버-식별-정보)을 따른다. 세 bootstrap 구성은 `memberId` 기준으로 바이트 단위 정렬을 사용하고 같은 집합을 포함해야 한다. 각 멤버의 인증서 지문과 SAN은 해당 항목과 일치해야 한다. 모든 bootstrap 노드는 같은 빌드 릴리스와 feature 집합을 사용해야 한다. `seedAddresses`는 임의의 검색 서버가 아니라 피어 승인 listener를 가리킨다. 각 host는 소문자 DNS 이름이어야 하며 TLS server name으로 사용되고 접속한 seed 인증서 SAN과 일치해야 한다. ClusterAdmin token은 지정한 파일에서 읽는다. 원본 token은 JSON이나 로그에 포함하지 않는다.

Bootstrap 예시다. `<64 lowercase hex>`는 인증서 지문에서 가져오는 구성 값이며, 그대로 허용되는 리터럴 값이 아니다. 예시의 릴리스와 feature는 바이너리 빌드 식별 정보에서 가져온다.
```json
{
  "mode": "cluster",
  "clusterId": "sample-cluster",
  "nodeId": 1,
  "directory": "data/node1",
  "listenAddress": "127.0.0.1:19001",
  "advertisedAddress": "127.0.0.1:19001",
  "localDnsName": "node1.example",
  "clusterProtocolMajor": 2,
  "clusterProtocolMinor": 0,
  "stateSchemaMajor": 1,
  "stateSchemaMinor": 0,
  "role": "bootstrap",
  "bootstrapMembers": [
    {"memberId": 1, "address": "127.0.0.1:19001", "dnsSan": "node1.example", "certificateSha256": "<64 lowercase hex>"},
    {"memberId": 2, "address": "127.0.0.1:19002", "dnsSan": "node2.example", "certificateSha256": "<64 lowercase hex>"},
    {"memberId": 3, "address": "127.0.0.1:19003", "dnsSan": "node3.example", "certificateSha256": "<64 lowercase hex>"}
  ],
  "seedAddresses": [],
  "peerTLS": {"certFile": "certs/node1.crt", "keyFile": "certs/node1.key", "caFile": "certs/ca.crt"},
  "management": {
    "listenAddress": "127.0.0.1:18080",
    "clusterAdminTokenFile": "secrets/cluster-admin.token",
    "apiKeys": [{"id": "dashboard", "keyFile": "secrets/dashboard.key", "roles": ["ReadOnly"]}],
    "auditDirectory": "data/node1-audit"
  },
  "mqtt": {"tcpListenAddress": "", "tlsListenAddress": "127.0.0.1:1883", "securityConfigFile": "security.json", "tls": {"certFile": "certs/mqtt.crt", "keyFile": "certs/mqtt.key", "caFile": "", "clientAuth": "none", "minVersion": "1.2"}}
}
```

### 1.4 Security config JSON

Object 안에서 같은 이름의 key가 다시 나오면 거절한다. 대소문자까지 같은 문자열일 때만 중복으로 본다. `security_config.go`의 `parseSecurityConfig`는 `DisallowUnknownFields`, 뒤따르는 값 거절과 `config_json.go`의 `rejectDuplicateJSONKeys` 중복 key 검사를 수행한다.

```json
{
  "jwt": {
    "issuer": "https://issuer.example",
    "audience": "narumq",
    "algorithms": ["RS256"],
    "jwksUrl": "https://issuer.example/.well-known/jwks.json",
    "cacheTtlMillis": 900000,
    "refreshCooldownMillis": 30000,
    "clockSkewMillis": 30000,
    "httpTimeoutMillis": 5000
  },
  "identities": [
    {
      "issuer": "https://issuer.example",
      "subject": "device-1",
      "tenantId": "tenant-a",
      "credentialId": "device-1-key",
      "allowedClientIds": ["device-1"],
      "roles": ["publisher"]
    }
  ],
  "mtlsIdentities": [
    {
      "identityKind": "commonName",
      "identity": "device-1-cert",
      "tenantId": "tenant-a",
      "subject": "device-1-cert",
      "credentialId": "device-1-cert-key",
      "allowedClientIds": ["device-1-cert"],
      "roles": ["publisher"]
    }
  ],
  "acl": [
    {
      "tenantId": "tenant-a",
      "role": "publisher",
      "effect": "allow",
      "action": "publish",
      "topicFilter": "sensors/#"
    }
  ]
}
```

`mtlsIdentities` 원소는 `identityKind`, `identity`, `tenantId`, `subject`, `credentialId`, `allowedClientIds`, `roles` field만 사용한다. `identityKind`는 `commonName`, `dnsSan`, `emailSan` 중 하나다. Identity 비교는 대소문자를 구분하는 exact match다. 인증서 하나가 어떤 entry와도 맞지 않으면 unmapped로 처리한다. 둘 이상의 entry와 맞으면 ambiguous로 처리한다. `NotAfter`는 새 principal의 만료 시각으로 사용한다. Trust chain, 인증서 유효 기간과 client-auth EKU는 TLS handshake에서 먼저 검증한다.

Issuer와 audience는 각각 하나만 허용한다. JWT identities의 `(issuer, subject)` binding, mTLS identities의 `(identityKind, identity)` selector, `allowedClientIds`와 `roles`는 각각 고유해야 한다. Password identities는 같은 `subject`를 공유할 수 있지만 `username`과 `credentialId`는 고유해야 한다. JWT·password·mTLS 전체에서 `credentialId`는 UTF-8 1~512 bytes이며 중복될 수 없다. ACL `effect`는 `allow|deny`, `action`은 `publish|subscribe|unsubscribe|will`만 허용한다. 중복 ACL rule과 잘못된 MQTT filter는 시작을 거절한다. `jwksUrl`은 절대 `https` URL이어야 한다. Deterministic test를 위해 IP literal loopback host의 HTTP만 예외로 허용한다. `cacheTtlMillis`, `refreshCooldownMillis`, `clockSkewMillis`, `httpTimeoutMillis`는 각각 `uint32` millisecond 정수이며 `1..4294967295`여야 한다.

Security config에는 `jwt`, nonempty `passwordIdentities`, `mtlsIdentities` 중 하나 이상의 authentication method가 필요하다. `identities`는 `jwt`가 있어야 한다.

### Username/password identity

`passwordIdentities`가 하나 이상 있으면 password authentication을 활성화한다. 각 entry는 `username`, `passwordHash`, `tenantId`, `subject`, `credentialId`, `allowedClientIds`, `roles` field만 사용한다. Plaintext `password` field는 거절한다. Username은 비어 있지 않은 case-sensitive exact key이며 configuration 안에서 중복할 수 없다. Password identities는 같은 `subject`를 공유할 수 있지만 각 `credentialId`는 JWT·mTLS identity를 포함해 고유해야 한다. `credentialId`는 UTF-8 1~512 bytes여야 한다. Tenant·Client ID·role의 필수·중복 검사는 JWT/mTLS binding과 같은 기준을 따른다. JWT와 비어 있지 않은 password identity 목록을 함께 설정하면 거절한다. Password 목록이 없거나 비어 있으면 password authentication을 활성화하지 않는다.

`passwordHash`는 다음 Argon2id PHC profile만 허용한다.

```text
$argon2id$v=19$m=19456,t=2,p=1$<salt>$<digest>
```

Salt는 16 bytes, digest는 32 bytes다. 둘 다 padding 없는 canonical standard base64로 인코딩한다. Version, parameter, 길이 또는 encoding이 다르면 시작을 거절한다. Profile은 19 MiB, 2회, 1 lane이다.

### `tenantLimits`

`-security-config` JSON의 선택 object `tenantLimits`는 standalone과 cluster가 함께 사용한다.

```json
"tenantLimits": {
  "maxWaitMillis": 1000,
  "default": {
    "maxConnections": 1000, "maxSessions": 2000, "maxSubscriptions": 10000,
    "messagesPerSecond": 500, "bytesPerSecond": 1048576, "maxPayloadBytes": 262144
  },
  "tenants": [ { "tenantId": "acme", "maxConnections": 10, "messagesPerSecond": 0 } ]
}
```

- `default`의 모든 field는 선택이다. 생략하거나 0이면 해당 차원을 제한하지 않는다.
- `tenants[]`는 `tenantId`가 필요하고 `tenancy.NewTenantID`로 검증한다. 중복 tenant는 거절하며 최대 4,096개다. 생략한 field는 `default` 값을 상속한다. 명시한 0은 해당 tenant의 제한을 끈다.
- `maxConnections`, `maxSessions`, `maxSubscriptions`, `messagesPerSecond`의 허용 범위는 `0..1,000,000`이다.
- `bytesPerSecond`의 허용 범위는 `0..1,099,511,627,776`(`1<<40`)이다.
- `maxPayloadBytes`의 허용 범위는 `0..268435455`다.
- `maxWaitMillis`는 `uint32` millisecond 정수이며 `0..10000`이다. 생략하면 `1000`이다. `0`은 대기 없이 거절한다.
- 알 수 없는 field, 범위 밖 값, 음수와 문자열 duration은 시작을 거절한다.

### 1.5 Peer-only config JSON

`cmd/narumq/raft_peer_config.go`의 `raftPeerConfigFile`과 `parseRaftPeerConfig`가 기준이다.

| Field | JSON 자료형 | 필수 조건·검증 |
| --- | --- | --- |
| `clusterId` | string | 공백 제거 뒤 비어 있으면 안 된다. |
| `nodeId` | uint64 | `1..3`이어야 한다. |
| `directory` | string | 공백 제거 뒤 비어 있으면 안 된다. |
| `listenAddress` | string | 공백 제거 뒤 비어 있으면 안 된다. |
| `localDnsName` | string | 공백 제거 뒤 비어 있으면 안 된다. |
| `certFile` | string | 공백 제거 뒤 비어 있으면 안 된다. |
| `keyFile` | string | 공백 제거 뒤 비어 있으면 안 된다. |
| `caFile` | string | 공백 제거 뒤 비어 있으면 안 된다. |
| `peers` | object array | 자기 자신을 제외한 나머지 두 voter가 각각 한 번씩 있어야 한다. |
| `peers[].id` | uint64 | `1..3`이며 `nodeId`와 달라야 한다. 중복 ID는 거절한다. |
| `peers[].address` | string | 공백 제거 뒤 비어 있으면 안 된다. |
| `peers[].dnsName` | string | 공백 제거 뒤 비어 있으면 안 된다. |

알 수 없는 key, 중복 key와 뒤따르는 JSON value는 거절한다. 중복 key는 같은 object 안에서 대소문자까지 같은 이름이 다시 나온 경우다. `parseRaftPeerConfig`가 strict decoder, 뒤따르는 값 검사와 `rejectDuplicateJSONKeys` 중복 key 검사를 수행하고, `validateRaftPeerConfig`가 필수 값과 고정 voter 집합을 확인한다.

### 1.6 Secret 파일

Cluster config의 `management.clusterAdminTokenFile`과 `apiKeys[].keyFile`은 앞뒤 공백을 제거한 내용을 사용한다. 파일 내용은 비어 있으면 안 된다. Cluster admin token과 API key, 서로 다른 API key는 같은 값을 사용할 수 없다. 필드와 필수 조건은 [§1.3](#13-cluster-config-json)을 따른다.

Standalone `-http-auth-token-file`도 앞뒤 공백을 제거한 내용을 사용하고, 비어 있으면 시작을 거절한다. Secret 값을 명령행 인수로 받는 flag는 없다.

### 1.7 환경 변수

Release build는 설정용 환경 변수를 읽지 않는다. Build tag `narumq_failpoint`로 빌드한 시험용 binary만 `NARUMQ_FAILPOINT`와 `NARUMQ_FAILPOINT_CONTROL`을 읽는다. 이 시험용 binary는 운영 설정으로 지원하지 않는다. 출처는 `cmd/narumq/cluster_failpoint_enabled.go`다.

### 1.8 Build identity

Build identity는 다음 `ldflags` 형식으로 지정한다.

```sh
go build -ldflags "-X main.buildReleaseVersion=<semver> -X main.buildFeatures=<comma list>"
```

기본값은 release version `0.0.0`이다. 항상 포함하는 feature는 `client-blocklist`, `cluster-observability-v1`, `cluster-policy`, `cluster-policy-tenant-quotas`, `connection-rebalance`, `credential-revocation`, `proposal-recovery`, `session-metadata`, `shared-subscription-reroute`, `snapshot-stream`이다. `resolveBuildIdentity`는 추가 feature를 합치고 정렬·중복 제거한 뒤 `release.ValidFeatureSet`으로 검증한다. 출처는 `cmd/narumq/build_identity.go`와 `internal/application/membership/build_features.go`다.

### 1.9 표기 규칙

Duration 표기는 입력 형식에 따라 다르다.

- Flag는 Go duration 문자열을 쓴다. 예: `10s`.
- JSON duration 값은 `Millis` 접미사가 붙은 field 이름과 음수가 아닌 정수 millisecond를 쓴다. Security config와 cluster config에 같은 규칙을 적용한다. 각 field의 허용 범위는 해당 설정 절을 따른다.

G5 규칙은 채택했다. ADR-0052의 다른 제안은 `Proposed`로 남는다. 상대 경로는 process의 현재 작업 디렉터리를 기준으로 한다.

## 2. 오류

시작에 실패하면 process는 stderr에 JSON log `narumq startup or runtime failure`를 남긴다. JSON의 `error` field에 오류를 넣는다. Process는 종료 코드 1로 끝난다.

| 대표 오류 문구 | 조건 |
| --- | --- |
| `-allow-insecure-anonymous-mqtt requires a loopback-only -tcp-listen and cannot be used with -tls-listen, -ws-listen, or -wss-listen` | 개발 익명 모드의 listener 조합이 허용 범위 밖이다. |
| `-ws-listen must use literal loopback IP 127.0.0.0/8 or ::1` | Standalone 평문 WebSocket 주소가 literal loopback이 아니다. |
| `-ws-max-connections must be between 0 and 1000000`, `-ws-connection-rate must be between 0 and 1000000` | WebSocket admission 값이 허용 범위를 벗어났다. |
| `-ws-max-connections and -ws-connection-rate require -ws-listen` | WebSocket admission 제한에 listener 주소가 없다. |
| `-wss-max-connections must be between 0 and 1000000`, `-wss-connection-rate must be between 0 and 1000000` | WSS admission 값이 허용 범위를 벗어났다. |
| `-wss-max-connections and -wss-connection-rate require -wss-listen` | WSS admission 제한에 listener 주소가 없다. |
| `mqtt webSocket listener addresses must be empty or valid host:port values` | Cluster WebSocket listener 주소가 유효하지 않다. |
| `missing required field "listenAddress"` | `mqtt.webSocket` object에 필수 `listenAddress`가 없다. |
| `mqtt webSocket connection limits must be between 0 and 1000000` | Cluster WebSocket admission 값이 허용 범위를 벗어났다. |
| `mqtt webSocket connection limits require listenAddress` | Cluster WS admission 제한에 listener 주소가 없다. |
| `mqtt webSocket tls connection limits require tlsListenAddress` | Cluster WSS admission 제한에 listener 주소가 없다. |
| `mqtt tls requires tlsListenAddress or webSocket.tlsListenAddress` | TLS profile은 설정했지만 TLS listener 주소가 없다. |
| `mqtt tls is required when tlsListenAddress or webSocket.tlsListenAddress is set` | TLS listener 주소가 있지만 TLS profile이 없다. |
| `-security-config is required when an MQTT listener is enabled` | MQTT listener를 열 때 security config가 없고 개발 익명 예외도 사용하지 않는다. |
| `-cluster-restore requires -cluster-config` | restore만 지정했다. |
| `-cluster-config cannot be combined with %s` | `-cluster-config`와 허용되지 않는 다른 flag를 함께 지정했다. |
| `-runtime-metrics-output and -runtime-metrics-interval must be provided together` | runtime metrics flag 하나만 지정했다. |
| `runtimeMetrics outputFile is required` | Cluster runtime metrics 경로가 비어 있거나 앞뒤 공백이 있다. |
| `runtimeMetrics intervalMillis must be between 1 and 3600000` | Cluster runtime metrics 간격이 허용 범위를 벗어났다. |
| `runtimeMetrics outputFile must not overlap directory or management auditDirectory` | 출력 경로가 cluster data 또는 management audit 경로와 겹친다. |
| `-http-request-rate and -http-auth-failure-rate must be between 1 and 1000000` | standalone Management API rate가 허용 범위 밖이다. |
| `read -http-auth-token-file: %w` | standalone token 파일을 읽지 못했다. |
| `-http-auth-token-file is empty` | token 파일의 공백 제거 결과가 비어 있다. |
| `load cluster config: %w` | cluster config를 읽거나 검증하지 못했다. |
| `load security config: %w` | security config를 읽거나 검증하지 못했다. |

오류 문구는 호환성 대상이 아니다.

## 3. 호환성 규칙

Consumer는 알 수 없는 key를 거절한다. 선택 key를 추가하면 그 key를 아는 새 binary와는 호환된다. 이전 binary는 새 key를 거절하므로 설정은 binary와 함께 배포해야 한다. 이 규칙은 [ADR-0030 §1 9항](../decisions/ADR-0030-publish-backpressure-admission.md#1-결정)을 따른다.

`Draft` 단계에서는 flag·key의 삭제와 이름 변경을 같은 버전에 반영할 수 있다. 이때 변경 내용을 아래 목록에 기록한다. `Active` 이후에는 위 규칙을 따른다.

- 2026-10-10: standalone `-http-auth-token`을 삭제하고 `-http-auth-token-file`로 대체했다([ADR-0052 §1.5](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#15-형식과-cluster-mqtt-tls), G9).
- 2026-10-10: Security config JWT, cluster `management.oidc`와 `tenantLimits.maxWait` duration key를 `Millis` 정수 field로 바꿨다. 이전 이름과 Go duration 문자열은 거절한다([ADR-0052 §1.5](../decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#15-형식과-cluster-mqtt-tls), G5).

## 4. 버전과 지원 단계

| 상태 | 조건 | 지원 종료·대체 |
| --- | --- | --- |
| `Draft` | 현재 상태다. | `Active` 전환 조건은 계약 시험 통과다. |
| `Active` | `tests/contracts` 전용 contract 시험이 §1 표를 실제 parser와 대조하고 통과해야 한다. | Breaking change는 새 major 버전으로 전환한다. |

## 5. 예제와 계약 시험

Cluster config와 security config 예제는 [§1.3](#13-cluster-config-json)과 [§1.4](#14-security-config-json)에 있다.

| 주체·시험 위치 | 확인할 계약 규칙 | 결과 | 실행 기록 |
| --- | --- | --- | --- |
| `cmd/narumq`: `TestParseClusterConfigValidRoles`, `TestParseClusterConfigRejectsInvalidInputs`, `TestParseClusterConfigRejectsDuplicateUnknownMissingAndTrailingJSON`, `TestParseClusterConfigRejectsUnreadableAndStructurallyInvalidJSON`, `TestValidateClusterConfigReportsFirstInvalidField` | Cluster config의 role, 필수·허용 필드, 형식, 중복·뒤따르는 값과 첫 오류 | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestValidateClusterConfigAcceptsManagementRateBoundaries`, `TestClusterManagementAccessConfiguration` | Management rate 경계, API key와 OIDC 구성 | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestParseClusterConfigReplicationProfile`, `TestParseClusterConfigPublishBackpressure`, `TestParseClusterConfigTenantQuotaProfile`, `TestParseClusterConfigRejectsMalformedTenantQuotaJSON` | 선택 cluster config object와 strict 정적 검증 | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestParseSecurityConfig_Valid`, `TestParseSecurityConfig_Rejections`, `TestParseSecurityConfig_NilOrUnreadableReader`, `TestParseSecurityConfig_PasswordIdentityRejections`, `TestParseSecurityConfig_MTLSValidation`, `TestParseSecurityConfig_TenantLimitsInheritance`, `TestParseSecurityConfig_TenantLimitsRejectInvalidProfiles`, `TestParseSecurityConfigRejectsIncompleteAuthenticationAndACL` | Security config authentication, ACL, password, mTLS, tenant limits, 중복 key와 거절 경계 | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestParseRaftPeerConfigStrict`, `TestParseRaftPeerConfigRejectsMissingFields` | Peer-only config strict parsing, 중복 key와 필수 값 | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestRejectDuplicateJSONKeys` | 공통 중복 key 검사의 중첩·배열·형제 object·대소문자 경계 | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestResolveBuildIdentity` | Build identity 정렬, 중복 제거와 feature 검증 | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestRunRejectsManagementRateFlagsOutOfRange`, `TestRunWithContextRejectsConstructionBounds`, `TestValidateTLSAuthenticationBindingsRequireMode`, `TestValidateTLSAuthenticationBindingsRequestMode` | Management rate 경계, queue 생성 한도, TCP max·TLS rate 범위와 TCP listener 필수 조건, TLS 인증 binding | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestHashPasswordFlagIsExclusive`, `TestClusterConfigFlagIsExclusive`, `TestRaftPeerOnlyRejectsOtherFlags`, `TestAuditVerifyFlagRequiresExclusiveDirectory` | 실행 모드 flag의 배타성·필수 인수 | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestMainNoListenerPath`, `TestRun_Defaults`, `TestRunWithContextMQTTSecurityMode`, `TestRunRejectsRuntimeMetricsFlagCombinations` | 비활성·기본 실행, MQTT 보안 mode와 runtime metrics flag 조합 | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestRunWithContextManagementAuthTokenFile` | Standalone Management token 파일의 존재·내용·Bearer 인증과 삭제된 flag | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestConfiguredTenantQuotaProfileUsesBuiltinOrStrictConfig`, `TestClusterTenantQuotaControllerLogsCommittedMismatchOnce`, `TestClusterMQTTListenersBindConfiguredEndpointsAndClosePartialOpen`, `TestRunRaftPeerOnlyStartupFailuresReleaseResources` | Quota profile, committed mismatch, partial listener 정리와 peer-only 시작 실패 | 통과 | 2026-10-10 [실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |
| `cmd/narumq`: `TestParseClusterConfigMQTTTLS`, `TestClusterMQTTListenersBindConfiguredEndpointsAndClosePartialOpen`, `TestClusterMQTTSecurityValidatesTLSClientAuthBindings`, `tests/integration`: `TestDynamicClusterMQTTTLSClientCertificateProcessAcceptance` | `mqtt.tls` 필수 key·strict validation, listener 자원 정리, client-auth binding과 3-node TLS·mTLS 동작 | 통과 | [Cluster MQTT TLS 실행 기록](../verification/configuration-and-runtime-options.md#cluster-mqtt-tls-g2-g10-execution) |
| `cmd/narumq`: `TestPositiveMillisDuration`, `TestParseSecurityConfig_Rejections`, `TestParseSecurityConfig_TenantLimitsMaxWaitMillis`, `TestParseSecurityConfig_TenantLimitsRejectInvalidProfiles`, `TestClusterManagementAccessConfiguration` | Millis 정수 경계, security config의 새 key·legacy key·문자열 거절, tenant max wait 범위, cluster OIDC 설정 | 통과 | [G5 duration 표기 실행 기록](../verification/configuration-and-runtime-options.md#g5-duration-millis-execution) |
| `cmd/narumq`: `TestParseClusterConfigClusterPolicyTenantQuotas` | `clusterPolicy.tenantQuotas`의 required·unknown·version field, 범위와 tenant ID·정렬 경계 | 통과 | [G7 config·quota 실행 기록](../verification/configuration-and-runtime-options.md#g7-config-view-and-tenant-quotas-execution) |
| `cmd/narumq`: `TestRunWithContextWebSocketFlags`, `TestParseClusterConfigWebSocket` | WS/WSS listener flag, admission 범위·binding, 선택 `mqtt.webSocket` object와 TLS profile binding | 미실행 | [설정 시험 실행 기록](../verification/configuration-and-runtime-options.md#configuration-test-execution) |

별도 `tests/contracts` 전용 시험은 실행하지 않았다. Process configuration v1은 `Draft`를 유지한다.

## 6. 초안을 확정하기 전에 해결할 사항

- 별도 contract 시험이 §1 표와 실제 parser를 대조해야 `Active`로 전환할 수 있다.
- 채택한 G1~G10의 완료 범위와 나머지 Proposed 범위는 [docs/TODO.md §1.10](../TODO.md#110-실행-옵션설정-구조)과 연결된 설정 스펙의 수용 매핑을 따른다.
