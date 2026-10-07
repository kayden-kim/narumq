# Tenant와 security 스펙

이 문서는 MQTT·관리 요청을 tenant에 귀속하고, topic·상태·자원을 격리하며, 인증·인가·감사를 일관되게 적용하는 외부 동작을 정의한다.

| 항목 | 값 |
| --- | --- |
| 상태 | Active (Phase 1 security and tenant isolation slice) |
| 적용 범위 | Phase 1에서 TEN-01~TEN-05, SEC-02~SEC-04, SEC-06과 SEC-08의 JWKS fail-closed 동작을 구현한다. Dynamic cluster의 tenant별 저장 quota(TEN-08)는 [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md) 범위에서 제공한다. TEN-06·TEN-07의 node-local 연결·session·subscription·PUBLISH traffic 한도는 §5.2를 따른다. Management plane의 활성 mutation 8개와 401·403 거절 감사는 §8과 [ADR-0039](../decisions/ADR-0039-management-audit-log.md)를 따른다. Blocklist, 기존 연결 재인증, MQTT data-plane 감사와 rollout controller 감사는 후속 범위다. Metrics는 인증 성공·실패, session gauge, 저장 quota 및 tenant 한도 거절을 포함하며, 상세 observability 정책은 §8과 Management API 스펙을 따른다. |
| 근거 | [PRD 9.3절, 9.4절과 11.4절](../requirements/PRD.md), [RFC 8725](https://www.rfc-editor.org/rfc/rfc8725.html) |
| 관련 spec | [MQTT protocol과 session](mqtt-protocol-and-session.md), [Management API](management-api-and-observability.md) |
| 관련 Architecture | [Security boundaries](../architectures/security-boundaries.md), [System overview](../architectures/system-overview.md) |
| 관련 Contract | [Audit event v1](../contracts/audit-event-v1.md), [Management API v1](../contracts/management-api-v1.md) |

## 1. 인증 결과 계약

인증 adapter는 성공 시 다음 논리 값을 반환해야 한다. 외부 provider의 원본 자료형을 이 경계 밖으로 노출해서는 안 된다.

| 필드 | 의미 |
| --- | --- |
| `tenantId` | 신뢰된 설정 또는 credential binding에서 결정한 tenant |
| `subject` | issuer 안에서 검증된 principal 식별자 |
| `credentialId` | 회전·폐기와 감사를 위한 비밀이 아닌 식별자 |
| `allowedClientIds` | 정확한 값 또는 승인된 pattern으로 표현한 허용 범위 |
| `roles` | 관리 plane의 RBAC role 목록 |
| `expiresAt` | 재인증 또는 연결 재평가에 쓰는 만료 시각 |

client가 username, token claim, MQTT property 또는 topic으로 제출한 `tenantId`를 독립적인 신뢰 근거 없이 사용해서는 안 된다. 인증 성공 뒤에도 tenant와 clientId binding, 차단 목록과 listener 정책을 확인해야 한다.

## 2. MQTT 연결 인증

```mermaid
flowchart TD
    A["TLS listener"] --> B["TLS version·cipher·server certificate"]
    B --> C["선택한 인증 방식"]
    C["선택한 인증 방식"] --> D["JWT/JWKS 또는 검증된 client certificate"]
    D --> E["tenant·subject·clientId binding"]
    E --> F["blocklist와 connection quota"]
    F --> G["default-deny ACL 준비"]
    G --> H["session ownership 요청"]
```

어느 단계든 실패하면 session ownership을 요청해서는 안 된다. 실패는 protocol version이 허용하는 CONNACK reason 또는 연결 종료로 표현하고 인증 실패 Metric을 기록해야 한다.

### 2.1 TLS와 mTLS

- TLS listener는 TLS 1.2 이상만 허용해야 한다.
- 허용 protocol과 cipher suite는 listener별 allowlist로 관리해야 한다.
- mTLS는 configured trust anchor, 유효 기간, key usage와 identity mapping을 모두 검증해야 한다.
- 신뢰하지 않는 CA, 만료·폐기 인증서 또는 identity 불일치는 거절해야 한다.
- node 간 연결은 별도 trust domain의 mTLS와 member allowlist를 모두 통과해야 한다.
- MQTT client mTLS의 verified certificate는 `commonName`, `dnsSan`, `emailSan` selector 중 설정된
  하나와 case-sensitive exact match되어 하나의 static binding을 선택해야 한다. zero match는
  unmapped, multiple match는 ambiguous로 fail-closed한다.
- TLS handshake가 trust chain, validity와 client-auth EKU를 확인하고, application authentication
  adapter가 certificate identity를 tenant·subject·credential·allowed Client ID·roles로 변환한다.
  인증서 원문과 chain은 application/domain 경계로 넘기지 않는다.

### 2.2 JWT와 JWKS

JWT validator는 다음 순서를 지켜야 한다.

1. CONNECT Password의 UTF-8 compact JWT를 읽고, configured algorithm allowlist에 없는 `alg`이면 서명 검증 전에 거절한다.
2. 필수 `kid`를 확인하고, cache의 JWKS key set에서 후보 key를 찾는다.
3. 후보 key로 서명을 검증한다. `kid`와 서명 검증은 configured issuer의 key set 안에서만 수행한다.
4. `iss`를 configured issuer와 정확히 비교하고, `aud`가 configured audience를 포함하는지 확인한다.
5. 비어 있지 않은 `sub`와 `exp`를 확인하고, 선택적인 `nbf`를 configured clock skew와 함께 검증한다.
6. 검증된 `(iss, sub)`를 정적 binding에 대조해 tenant, credential, 허용 Client ID와 roles를 결정한다. Token claim은 이 값을 덮어쓸 수 없다.

JWKS cache는 `fetchedAt`, TTL 만료 시각과 마지막 refresh 시각을 함께 소유한다. 비어 있거나 만료된 cache는 하나의 동시 fetch만 수행하는 mandatory refresh가 성공해야 인증을 진행한다. 유효 cache에서 알려지지 않은 `kid`는 `refreshCooldown`이 허용할 때 하나의 coalesced early refresh를 수행한다. early refresh가 실패하면 해당 `kid`는 거절하지만, 만료되지 않은 기존 key set으로 알려진 `kid`를 검증하는 동작은 유지한다. cooldown 안의 알려지지 않은 `kid`는 I/O 없이 거절한다. 성공한 fetch는 key set, `fetchedAt`, TTL과 refresh-attempt 시각을 원자적으로 교체하고, 실패한 fetch는 TTL을 연장하지 않는다. TTL이 끝난 뒤 refresh가 실패하면 fail-closed로 거절한다.

### 2.3 CONNECT credential과 strict security configuration

MQTT CONNECT는 configured authentication method 중 정확히 하나를 선택한다.

- verified client certificate가 있으면 mTLS를 선택하고 MQTT `Username`과 `Password` flag를 모두
  보내지 않아야 한다. mTLS provider는 certificate selector를 static binding에 대조해 tenant,
  subject, credential ID, allowed Client IDs와 roles를 결정한다. 인증서가 unmapped, ambiguous
  또는 application-level `NotAfter` 만료이면 JWT로 fallback하지 않고 거절한다.
- verified client certificate가 없고 JWT provider가 있으면 `HasUsername=true`와
  `HasPassword=true`를 모두 보내야 한다. Username은 비어 있어도 되며 identity로 사용하지 않고,
  Password에는 하나의 UTF-8 compact JWT만 넣는다.
- mTLS와 JWT credential을 함께 제출하면 거절한다. 두 방식의 identity를 합치거나 우선순위를
  추론하지 않는다.
- provider가 하나도 없는 경우에만 개발용 anonymous CONNECT를 허용한다.

credential, certificate identity, Client ID binding 또는 Will ACL이 실패하면 MQTT 3.1.1 CONNACK
`0x05`, MQTT 5 CONNACK `0x87`을 반환하고 연결을 닫는다. 실패 시 session, timer, route,
queue, retained message와 Will을 변경하지 않는다.

TLS listener(`-tcp-listen` 또는 `-tls-listen`)를 여는 Broker 시작에는 `-security-config <path>`가
필수다. 파일은 알 수 없는 field와 두 번째 JSON value를 거절하는 strict decoder로 한 JSON value만
읽는다. JWT 설정의 duration은 양수인 Go duration string이어야 한다.
`tenantLimits.maxWait`는 §5.2의 `0s`~`10s`를 따른다. 이 설정의 decode와 JWT/JWKS·mTLS
provider 생성은 listener socket을 열기 전에 끝나야 하며, 누락·충돌·오류가 있으면 시작을
실패시킨다. TLS listener의 `request|require` client authentication은 최소 하나의
`mtlsIdentities` binding을 요구하고, binding이 있으면 `client-auth none`을 사용할 수 없다.
`request`는 JWT provider가 함께 configured된 경우에만 certificate 없는 client를 허용하며,
`require`는 모든 TLS client에 verified certificate를 요구한다.

유일한 개발·benchmark 예외는 `-tcp-listen`과 `-allow-insecure-anonymous-mqtt`를 함께 사용하고
`-tls-listen`을 비워 둔 경우다. `-tcp-listen`은 literal `127.0.0.0/8` 또는 `::1` loopback이어야
하며, `-security-config`와 예외 flag를 함께 사용할 수 없다. MQTT listener 없이 예외 flag만 주거나
TLS listener와 함께 주는 경우도 거절한다. 이 예외는 production security mode가 아니며 Phase 1
보안 완료 증거로 사용할 수 없다. MQTT listener가 없는 management-only 실행에는
`-security-config`를 요구하지 않는다.

```json
{
  "jwt": {
    "issuer": "https://issuer.example",
    "audience": "narumq",
    "algorithms": ["RS256"],
    "jwksUrl": "https://issuer.example/.well-known/jwks.json",
    "cacheTtl": "15m",
    "refreshCooldown": "30s",
    "clockSkew": "30s",
    "httpTimeout": "5s"
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

`mtlsIdentities` entry는 `identityKind`, `identity`, `tenantId`, `subject`, `credentialId`,
`allowedClientIds`, `roles` field만 사용한다. `identityKind`는 `commonName`, `dnsSan`,
`emailSan` 중 하나이며 identity 비교는 case-sensitive exact match다. 하나의 certificate가
zero entry와 match하면 unmapped, 둘 이상의 entry와 match하면 ambiguous다. `NotAfter`는
새 principal의 만료 시각으로 사용하며 trust chain·certificate validity·client-auth EKU는
TLS handshake에서 먼저 검증한다. URI SAN/SPIFFE·fingerprint selector, revocation·rotation,
기존 연결 종료·reauthentication, blocklist·audit과 cluster-peer mTLS는 이 인증 mapping slice에
포함하지 않는다.

이 slice는 issuer와 audience를 각각 정확히 하나만 허용한다. algorithms, JWT identities,
mTLS identities, credential IDs, tenant IDs, `(issuer, subject)` binding, `(identityKind,
identity)` selector, allowed Client IDs와 roles는 비어 있지 않고 각각 중복되지 않아야 한다.
JWT credential ID와 mTLS credential ID도 서로 중복될 수 없다. ACL `effect`는 `allow|deny`,
`action`은 `publish|subscribe|unsubscribe|will`만 허용한다. 중복 ACL rule과 malformed MQTT
filter는 startup에서 거절한다. `jwksUrl`은 절대 `https` URL이어야 하며, deterministic test를
위해 IP-literal loopback host의 HTTP만 예외로 허용한다. `cacheTtl`, `refreshCooldown`,
`clockSkew`, `httpTimeout`은 모두 양수여야 한다. Reload 동작은 이 slice에 포함하지 않는다.


### 2.4 Username/password

이 방식이 활성화되면 password 검증에 salt를 포함한 승인된 password hashing scheme을 사용해야 한다. 평문이나 복호화 가능한 password를 저장해서는 안 된다. 실패 응답은 계정 존재 여부를 구분하지 않아야 한다.

## 3. Tenant namespace

외부 MQTT topic과 filter 표기는 변경하지 않는다. Broker 내부에서는 tenant와 namespace를 문자열 prefix로 이어 붙이지 않고, 비교 가능한 typed value로 함께 표현한다.

- `NewTenantID(raw string) (TenantID, error)`는 비어 있거나 유효하지 않은 tenant를 거절한다.
- `NewSessionKey(tenantID TenantID, clientID string) (SessionKey, error)`는 tenant와 비어 있지 않은 Client ID를 결합하며 field를 외부에서 만들 수 없게 한다.
- `NewTopic(tenantID TenantID, name string) (Topic, error)`는 concrete MQTT topic을 검증한다.
- `NewTopicFilter(tenantID TenantID, filter string) (TopicFilter, error)`는 MQTT filter 또는 shared subscription을 검증한다.

각 value는 private field와 read-only `TenantID()`, `ClientID()`, `Name()`, `Filter()` accessor만 제공한다. 서로 다른 tenant의 같은 외부 이름은 비교에서 절대 같은 값이 되지 않는다. Cross-tenant 비교는 거절한다. 향후 persistence·wire adapter만 unsigned-varint byte length 뒤에 UTF-8 tenant와 topic/filter bytes를 직렬화할 수 있으며, 이 slice에는 string compatibility key나 내부 prefix encoding을 두지 않는다.

Topic/filter validation과 MQTT wildcard matcher는 하나만 유지한다. Shared subscription은 먼저 `$share/{ShareName}/{TopicFilter}`로 분해하고 ACL과 containment/intersection 비교에는 underlying filter만 사용한다. `$` system topic의 첫 level wildcard 제한을 포함한 기존 MQTT matching 의미를 그대로 적용한다. Unicode normalization, case folding, slash 병합은 하지 않는다.

Namespace binding은 publish, subscribe, unsubscribe, retained, shared subscription, Will과 session key에 동일하게 적용한다. Client가 제출한 tenant claim, topic prefix 또는 이중 encoding으로 tenant를 선택하거나 변경할 수 없다.

## 4. ACL

ACL 입력은 `tenantId`, `subject`, `clientId`, action, canonical topic/filter, listener와 연결 속성이다.

| Action | 판정 시점 | 거절 결과 |
| --- | --- | --- |
| `publish` | message를 durable log나 route에 넣기 전 | MQTT 5 `Not authorized`가 허용되면 반환하고, 아니면 연결을 종료할 수 있다. |
| `subscribe` | route를 만들기 전 | 해당 SUBACK 항목을 거절한다. |
| `unsubscribe` | target session과 filter를 바꾸기 전 | protocol이 허용하는 거절 또는 연결 종료 |
| `will` | CONNECT에서 등록할 때와 실제 발행 직전 | 등록 또는 발행을 거절하고 감사 가능한 사유를 남긴다. |

- 일치하는 allow가 없으면 deny다.
- 명시적 deny는 allow보다 우선해야 한다.
- policy backend 오류 또는 timeout은 deny로 처리해야 한다.
- policy 변경의 적용 version을 연결과 감사 이벤트에서 확인할 수 있어야 한다.
- 기존 연결 재평가·재인증 정책은 credential 종류별로 문서화해야 한다.

### 4.1 Phase 1 ACL 판정과 Will

ACL은 authenticated principal이 가진 모든 role의 tenant-local rule을 대상으로 평가한다. matching deny가 하나라도 있으면 거절하고, deny가 없으며 matching allow가 있으면 허용하고, 둘 다 없으면 default deny다.

- `publish`와 `will`은 concrete `Topic`이 rule `TopicFilter`와 match하는지 평가한다.
- `subscribe`와 `unsubscribe`는 요청한 모든 topic을 포함하는 rule filter가 있어야 allow가 된다. 요청 filter와 deny rule이 한 topic이라도 교차하면 거절한다.
- Shared subscription syntax는 먼저 parse하고 underlying filter로 위 두 판정을 수행한다.
- CONNECT Will은 등록 시점과 실제 publication 직전에 같은 principal snapshot으로 다시 authorize한다. delayed Will의 authorization 실패는 broker state를 변경하지 않고 Will을 버린다.

`Principal`과 `SessionKey`의 tenant와 Client ID가 일치하지 않으면 모든 authorization은 거절한다. Principal은 CONNECT 시점에 session에 snapshot으로 저장하며 기존 연결은 live TCP connection이나 timer map에서 identity를 다시 읽지 않는다.

## 5. Quota와 rate limit

모든 quota는 tenant별로 계산하고 한 tenant의 초과가 다른 tenant의 정상 traffic을 직접 거절하게 해서는 안 된다.

| 범주 | 최소 제한 단위 | 초과 동작 |
| --- | --- | --- |
| 연결 상태 | connection, session, subscription | 새 생성 거절. 기존 정상 상태를 임의 삭제하지 않는다. |
| traffic | message rate, byte rate, payload size | bounded backpressure 후 protocol reason으로 거절하거나 연결 종료 |
| 저장 | offline queue message/bytes, retained bytes, 전체 저장량 | 새 durable state를 거절하고 기존 state를 보존한다. Dynamic cluster의 tenant별 저장 quota와 초과 동작은 §5.1을 따른다. |
| 선택 Metric | exact topic 등록 수, TTL | wildcard 거절, 상한 초과 등록 거절 |

저장 quota 상한과 초과 정책은 [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md)에 따른다. Cluster는 초과 시 eviction 없이 새 state만 거절해야 하며, quota 축소 뒤에도 기존 state를 보존하고 usage를 줄이는 변경을 허용해야 한다.

### 5.1 저장 quota

Dynamic cluster는 tenant별 retained bytes, offline queue와 저장 총량을 제한해야 한다. 한 tenant의 초과가 다른 tenant에 설정된 한도를 바꾸지 않는다. 적용 차원, byte 계산, global limit 우선순위와 profile version은 [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md)을 따른다.

| 설정 key | 집계 대상 | cluster-wide 최대값 |
| --- | --- | ---: |
| `retainedBytes` | 해당 tenant의 retained value 계산 bytes 합 | 16 MiB |
| `offlineQueueMessages` | QoS 0 offline message와 durable delivery outbound message(QUEUED·inflight) 수 | 8,192 |
| `offlineQueueBytes` | 위 message들의 계산 bytes 합 | 32 MiB |
| `storageBytes` | retained, QoS 0 offline, durable delivery outbound·inbound QoS 2 bytes 합 | 48 MiB |

`-cluster-config`에는 다음 형태의 `tenantQuotas` profile을 선택적으로 지정한다. 명시 profile의 `version`은 1 이상이며, tenant별 항목은 `tenantId` 순으로 적용된다.

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

Profile은 더 높은 version의 Raft command가 commit된 뒤 모든 replica에 적용된다. 설정에 tenant 항목이 없으면 `default`를 사용한다. `tenantQuotas`를 생략하면 각 dimension의 cluster-wide 최대값인 version 0 built-in profile이 사용된다. 새 저장이 quota를 넘으면 retained/offline mutation은 기존 capacity 결과로 거절한다. Durable delivery는 [Cluster §4.5](cluster-session-and-delivery.md#45-publish-backpressure-admission)의 ingress 사전 판정에서 전역·tenant 한도 초과 publish를 거절한다. MQTT 5는 `0x97`, MQTT 3.1.1은 연결 종료를 반환한다. Tenant 초과는 `narumq_tenant_quota_rejections_total`에 기록한다. Apply 시점 경합은 기존 `Saturated` 처리다. Session당 한도와 RELEASE 시점 포화도 target을 건너뛰는 기존 동작을 유지한다.

Proposer node는 `narumq_tenant_quota_rejections_total{tenant,dimension}` counter를 올리고 같은 tenant·dimension 경보 log를 10초 rate-limit한다. Tenant label은 최대 1,024개까지 보존하고 나머지는 `_overflow` label로 집계한다. Profile commit 여부, version과 local config mismatch는 각각 `narumq_tenant_quota_profile_committed`, `narumq_tenant_quota_profile_version`, `narumq_tenant_quota_config_mismatch`로 노출한다. Committed profile이 복구되기 전에는 cluster MQTT readiness를 허용하지 않는다.
### 5.2 연결 상태와 traffic 한도

`-security-config` JSON에 선택 object `tenantLimits`를 둔다. Standalone과 cluster가 같은 schema를 사용한다. `tenantLimits`를 생략하면 tenant 한도가 없다.

```json
"tenantLimits": {
  "maxWait": "1s",
  "default": {
    "maxConnections": 1000, "maxSessions": 2000, "maxSubscriptions": 10000,
    "messagesPerSecond": 500, "bytesPerSecond": 1048576, "maxPayloadBytes": 262144
  },
  "tenants": [ { "tenantId": "acme", "maxConnections": 10, "messagesPerSecond": 0 } ]
}
```

- **default.** 모든 field는 선택이다. 생략하거나 0이면 그 차원은 제한하지 않는다.
- **tenants[].** `tenantId`는 필수이고 `tenancy.NewTenantID`로 검증한다. 같은 tenant가 두 번 나오면 거절한다. 최대 4,096개다. 생략한 field는 `default` 값을 상속한다. 명시한 0은 그 tenant에서 제한을 끈다.
- **허용 범위.**
  - `maxConnections`, `maxSessions`, `maxSubscriptions`, `messagesPerSecond`: 0~1,000,000.
  - `bytesPerSecond`: 0~1,099,511,627,776(1<<40).
  - `maxPayloadBytes`: 0~268,435,455.
  - `maxWait`: Go duration, `0s`~`10s`. 생략하면 `1s`다. `0s`는 대기 없이 바로 거절한다.
  - 알 수 없는 field, 범위 밖 값, 음수, 잘못된 duration은 startup에서 거절한다.

한도는 인증된 principal(`principal.Subject() != ""`)에만 적용한다. Security config가 없는 loopback 개발 모드와 anonymous session에는 한도가 없다.

| 차원 (`dimension`) | 집계 대상 | 검사 시점 | 초과 응답 MQTT 5 / 3.1.1 |
| --- | --- | --- | --- |
| `connections` | 이 node session repository에서 `Connected()==true`인 tenant lifecycle 수 | CONNECT에서 `session.Open` 성공 뒤, ownership 획득 전. 기존 lifecycle이 이미 연결 중이면 증가로 보지 않는다. | CONNACK `0x97` / CONNACK `0x03`. State를 바꾸지 않는다. |
| `sessions` | 이 node session repository의 tenant lifecycle 수(연결 중 + detached) | 같은 시점. 기존 lifecycle이 없을 때만(`!found`) 검사한다. Resume과 clean start 대체는 새 session이 아니다. | 같음 |
| `subscriptions` | Route table에서 tenant에 bind된 session의 shared·non-shared subscription 수. Cluster에서는 committed projection 전체다. | SUBSCRIBE filter마다 ACL 검사 뒤, commit 전. 같은 session에 같은 filter가 이미 있으면(replace) 검사하지 않는다. | 해당 filter의 SUBACK `0x97` / `0x80`. 다른 filter는 독립 처리한다. |
| `payload_size` | PUBLISH payload byte 길이 | TCP `readLoop`에서 decode 직후, dispatch 전. 대기 없음. | QoS 1 PUBACK `0x97`, QoS 2 PUBREC `0x97`, QoS 0 DISCONNECT `0x97` 뒤 종료 / 종료 |
| `message_rate` | Tenant의 초당 PUBLISH 수 (node별 token bucket) | 같은 위치. 최대 `maxWait`까지 대기한다. | QoS 1 PUBACK `0x97`, QoS 2 PUBREC `0x97`, QoS 0 DISCONNECT `0x96` 뒤 종료 / 종료 |
| `byte_rate` | Tenant의 초당 payload byte (node별 token bucket) | 같은 위치 | `message_rate`와 같음 |

연결, session, rate는 node별로 센다. Subscription은 각 node의 route table에 있는 tenant 전체 수로 판정한다. Standalone에서는 해당 node의 수이고, cluster에서는 committed projection 기준 cluster 전체 수다. 같은 node의 제한 대상 SUBSCRIBE·UNSUBSCRIBE는 판정부터 commit과 route 변경까지 직렬화한다. Replacement와 removal이 경쟁해도 local 한도를 넘기지 않는다. 다른 node에서 동시에 SUBSCRIBE하면 projection 반영 시점 때문에 잠시 한도를 넘을 수 있다. Replicated schema, feature gate, Raft command는 바꾸지 않는다.

#### 검사 순서와 과금

- **검사 순서.**
  - CONNECT: `connections` → `sessions`.
  - PUBLISH: Receive Maximum → `payload_size` → rate. 그 뒤 기존 ACL·cluster·backpressure 검사가 이어진다. 따라서 ACL로 거절될 PUBLISH도 rate token을 쓴다.
- **과금 대상.** 새 PUBLISH만 과금한다. QoS 0은 항상 새 PUBLISH다. QoS 1/2는 `admitInboundPublish`가 처음 예약한 Packet ID일 때만 과금한다. 미완료 ID의 재전송은 과금하지 않는다. PUBREL, 서버 Will, retained 재전송, cluster forward는 과금하지 않는다.

#### Token bucket과 대기

- **Token bucket.** 용량과 초당 보충량은 설정 rate와 같다. 시작할 때 bucket은 가득 차 있다. Clock이 뒤로 가면 보충하지 않는다.
  - 예약하면 token에서 message 1개와 payload byte를 뺀다. Token은 음수가 될 수 있다. 이 음수가 대기열 역할을 한다.
  - 대기 시간은 `max(부족 message/messagesPerSecond, 부족 byte/bytesPerSecond)`다.
  - 대기 시간이 `maxWait`를 넘으면 token을 빼지 않고 거절한다. 두 차원이 모두 넘으면 `message_rate`를 보고한다.
- **대기.** Rate 초과 시 해당 연결의 읽기를 최대 `maxWait`(기본 1s, 최대 10s)만큼 멈추고 token을 기다린다. 대기 중에는 연결의 다음 packet을 읽지 않으므로 TCP 수준 backpressure가 생긴다. 연결이 닫히면 대기를 즉시 끝낸다. Token이 충분하지 않으면 거절한다.

#### Metric과 log

- Counter `narumq_tenant_limit_rejections_total{tenant,dimension}`을 둔다. `dimension`은 위 6개 값만 허용한다.
- `tenant` label은 기존 저장 quota metric과 같은 1,024개 tenant 집합을 공유한다. 집합을 넘으면 `_overflow`로 모은다.
- 거절마다 counter를 올린다. Warn log `tenant limit rejected a request`(`tenant`, `dimension`)는 tenant·dimension마다 10초에 한 번만 남긴다.


## 6. 관리 plane RBAC

| Role | 최소 권한 |
| --- | --- |
| `ReadOnly` | 허용 범위의 cluster·tenant 상태와 Metric 조회 |
| `Operator` | drain, client disconnect·redirect와 운영 mutation |
| `SecurityAdmin` | credential, ACL, blocklist와 감사 조회 |
| `ClusterAdmin` | cluster 전체 설정과 tenant 관리 |
| delegated tenant role | 자기 tenant의 credential, ACL과 quota만 관리 |

권한은 resource와 action마다 확인해야 한다. 단순히 UI에서 control을 숨기는 것으로 권한 검사를 대신해서는 안 된다. 목록과 상세 조회에서도 tenant scope를 적용해 IDOR를 막아야 한다.

## 7. Credential 회전·폐기와 차단

- 새 credential을 추가한 뒤 기존 credential을 겹쳐 허용하는 회전 구간을 설정할 수 있어야 한다.
- 폐기는 configured propagation 목표 안에 모든 node에 반영해야 한다.
- 강제 재인증 또는 기존 connection 종료 여부는 credential 정책에 따라 일관되게 적용해야 한다.
- `clientId`, username, certificate identity와 IP 차단은 P1이다. 적용 결과와 source policy version을 조회할 수 있어야 한다.
- node가 최신 보안 policy를 확인할 수 없고 cache TTL도 지났다면 새 인증을 허용해서는 안 된다.

## 8. 감사와 비밀정보

Dynamic cluster의 활성 Management API mutation 8개와 인증 `401`·인가 `403` 거절은 audit event를 생성한다. Event 형식과 action·target·actor·reason·details는 [Audit event v1 계약 §5](../contracts/audit-event-v1.md#5-broker-producer-범위)를 따른다. MQTT CONNECT 인증 실패와 ACL 거절은 이 범위에서 감사하지 않는다.

Management mutation은 실행 전에 node-local publisher의 준비 상태를 확인해야 한다. Publisher를 쓸 수 없으면 handler를 호출하지 않고 retryable `503 audit_unavailable`을 반환한다. Mutation 뒤 event 기록이 실패하면 작업을 되돌리지 않고 error log와 Metric으로 드러낸다.

Event는 `management.auditDirectory`의 JSONL 파일에 append하며 각 줄을 fsync한다. 이전 줄의 SHA-256을 연결하는 hash chain은 손상과 연결된 줄의 누락을 검출한다. Broker는 크기 기준으로만 rotation하고 파일을 삭제하지 않는다. 외부 collector는 rotation 파일 중 가장 오래된 연속 prefix만 삭제할 수 있다. 보존 기간, 검색과 접근 권한은 외부 SIEM 책임이다.

Standalone mode에는 감사 publisher를 연결하지 않는다. Standalone은 이 계약의 Dynamic cluster mutation을 제공하지 않으며, standalone 401·403 event도 기록하지 않는다.

Audit directory는 필수 cluster 설정이다. Raft data directory와 같거나 서로 포함하면 startup을 거절하고, 각 node는 별도 directory를 사용한다. `narumq -audit-verify <directory>`는 저장된 chain을 읽기 전용으로 검증한다.

Bearer token, API key, body, query와 원문 secret은 event에 기록하지 않는다. Management credential은 공유 값이므로 actor subject는 credential 종류를 나타내며 개인 식별을 보장하지 않는다. 초당 20개를 넘는 거절 event는 저장하지 않고 suppression Metric으로만 센다.

MQTT CONNECT 인증 실패와 ACL 거절, rollout controller producer, SIEM consumer의 unknown action/reason 검증은 후속 범위다. Hash chain은 파일 전체를 다시 쓰는 권한 보유자 공격을 막지 않으므로 SIEM 보관과 접근 통제를 대체하지 않는다.

## 9. 수용 시험과 범위

| ID | 수용 기준 | 시험 |
| --- | --- | --- |
| SEC-01 | TLS 1.2 이상, TLS 1.0/1.1 거절, AEAD cipher allowlist와 TLS/mTLS listener 왕복을 검증한다. | `tests/integration/mqtt_tls_test.go`의 `TestMQTTTLS_ConnectionAndDelivery`, `TestMQTTTLS_RejectInsecureTLSVersion` |
| SEC-02 | algorithm allowlist, required `kid`, cached JWKS signature, exact issuer, audience membership, subject, exp/nbf와 clock skew를 순서대로 검증하고 key rotation을 fail-closed로 처리한다. MQTT listener startup은 `-security-config`를 요구하며, 유일한 예외는 literal loopback `-tcp-listen`과 `-allow-insecure-anonymous-mqtt`의 development·benchmark 조합이다. | `internal/adapters/authentication`의 JWT/JWKS cache 시험, `cmd/narumq/main_test.go`의 `TestRunWithContextMQTTSecurityMode`와 `TestRunWithContextSecurityConfig` |
| SEC-03 | trusted CA와 mapped certificate 성공, untrusted CA·TLS 만료 certificate·application-level `NotAfter` 만료 certificate 거절을 검증한다. | `tests/integration/mqtt_tls_test.go`의 `TestMQTTTLS_mTLS_TrustedMappedCertificate`, `TestMQTTTLS_mTLS_UntrustedCA_Rejected`, `TestMQTTTLS_mTLS_ExpiredCert_Rejected`, `internal/adapters/authentication`의 certificate expiry 시험 |
| SEC-04 | 인증서 CN/SAN exact mapping, unmapped·ambiguous identity, dual credential 거절을 검증하고, mTLS와 JWT 인증 결과의 exact allowed Client ID가 아니면 MQTT 3.1.1 `0x05` 또는 MQTT 5 `0x87`로 거절하고 state를 만들지 않는다. | `tests/integration/mqtt_tls_test.go`의 mTLS principal acceptance/rejection 시험, `internal/application/connect`, `tests/integration/mqtt_ingress_test.go`의 `TestMQTTSecurity` |
| SEC-06 | publish, subscribe, unsubscribe와 Will은 deny > allow > default-deny를 적용하고 거절 전에는 mutation을 수행하지 않는다. | `internal/domain/security`, application mutation-order 시험, `tests/integration/mqtt_ingress_test.go`의 `TestMQTTSecurity` |
| SEC-08 | JWKS가 비어 있거나 TTL 만료 후 refresh에 실패하면 fail-closed한다. Metrics와 audit sink은 이 slice의 범위가 아니다. | `internal/adapters/authentication`의 `TestJWKSBootstrapFailureCollapses` 등 시험 |
| SEC-09 | node 간 복제와 internal transport mTLS를 분리 검증한다. | 별도 cluster transport acceptance suite |
| TEN-01 | 유효한 JWT와 정적 `(issuer, subject)` binding 없이 CONNECT가 성공하지 않고, token claim이 tenant를 선택하지 못한다. | `internal/adapters/authentication`의 JWT 검증 시험, `tests/integration/mqtt_ingress_test.go`의 `TestMQTTSecurity` |
| TEN-02 | Client ID uniqueness와 session ownership의 key가 `(tenant, clientId)`이며 같은 Client ID를 다른 tenant가 사용할 수 있다. | `internal/domain/session`, `internal/adapters/memory/sessionstore`, `tests/integration/mqtt_ingress_test.go`의 `TestMQTTSecurity` |
| TEN-03 | 같은 외부 topic과 filter가 tenant별 typed value로 격리되며 retained·shared·Will에도 같은 binding이 적용된다. | `internal/domain/tenancy`, `internal/domain/routing`, `tests/integration/mqtt_ingress_test.go`의 `TestMQTTSecurity` |
| TEN-04 | ACL은 tenant-local canonical typed value를 기준으로 평가하고 내부 prefix injection이나 이중 encoding으로 우회할 수 없다. | `internal/domain/security` policy 시험, `tests/integration/mqtt_ingress_test.go`의 `TestMQTTSecurity` |
| TEN-05 | system topic, retained, shared subscription, Will과 session이 tenant 경계를 넘지 않는다. | tenancy/routing/session 시험, `tests/integration/mqtt_ingress_test.go`의 `TestMQTTSecurity` |
| TEN-06 | Tenant별 연결·session 제한은 node별로, subscription 제한은 route table의 tenant 수로 적용한다. 한 tenant의 초과는 다른 tenant를 거절하지 않으며, 초과 응답과 state 무변경을 확인한다. 같은 node의 동시 add와 replace/remove는 직렬화한다. | `internal/application/connect`, `internal/application/subscribe`의 `TestSubscribeTenantQuotaSerializesLocalAdmission`, `TestSubscribeTenantQuotaSerializesReplacementAndRemoval`, `internal/adapters/memory/sessionstore`, `internal/domain/routing`, `tests/integration/mqtt_tenant_limits_test.go` (`TestMQTTTenantLimits`), `tests/integration/cluster_tenant_limits_process_test.go` (`TestDynamicClusterTenantLimitsProcessAcceptance`) |
| TEN-07 | Tenant별 payload 크기·message rate·byte rate를 제한한다. Rate 초과는 최대 `maxWait`까지 대기하고, MQTT 응답과 다른 tenant의 독립성을 확인한다. | `internal/application/tenantlimit`, `internal/adapters/tcp/server_tenant_limits_test.go`, `internal/adapters/metrics`, `cmd/narumq/security_config_test.go`, `cmd/narumq/tenant_limits_test.go`, `tests/integration/mqtt_tenant_limits_test.go` (`TestMQTTTenantLimits`) |
| SEC-11 | Dynamic cluster의 활성 Management API mutation 8개와 401·403 거절을 node-local JSONL hash chain에 기록한다. Audit publisher가 unavailable이면 mutation handler를 실행하지 않고 `503 audit_unavailable`을 반환한다. | `internal/adapters/http/management/audit_test.go`, `tests/contracts/audit-event-v1/contract_test.go::TestAuditEventMatchesSchemaAndFileChain`(8개 mutation, 거절·실패), `tests/integration/cluster_audit_process_test.go::TestDynamicClusterManagementAuditProcessAcceptance` |
| NFR-17 | 기록된 event의 줄 순서·hash 연결·변조 검출, 중첩 details의 잘못된 UTF-8 거절, handler panic의 감사 HTTP 상태 500, Windows rotation 충돌 보호와 audit/Raft directory 경계를 확인한다. 보존 기간 값과 SIEM 접근 권한은 외부 결정으로 미정이다. | `internal/application/audit/service_test.go`, `internal/adapters/http/management/audit_test.go`, `internal/adapters/auditlog/file_publisher_test.go`, `internal/adapters/auditlog/persist_windows_test.go`, `cmd/narumq/cluster_config_test.go`, `cmd/narumq/main_test.go::TestAuditVerifyCommandVerifiesAndLeavesFilesReadOnly`, `tests/contracts/audit-event-v1/contract_test.go`, `tests/integration/cluster_audit_process_test.go::TestDynamicClusterManagementAuditProcessAcceptance` |

SEC-03/SEC-04의 certificate-to-tenant acceptance와 TEN-06·TEN-07의 §5.2 범위를 구현했다. TEN-08 저장 quota는 §5.1과 ADR-0017의 채택 범위를 따른다. TEN-09의 noisy-neighbor p99 기준과 TEN-10은 이번 수용 범위에 포함하지 않는다.

MQTT data-plane 거절 감사, rollout controller producer, 보존 기간 값과 SIEM 접근 권한은 후속 범위다. 위 표 밖의 SEC-05, SEC-07, SEC-10, SEC-12와 TEN-08~TEN-10의 미채택 범위는 후속 acceptance scope다. TEN-08의 §5.1·ADR-0017 채택 범위는 다시 미구현으로 열지 않는다.

### 9.1 TEN-06·TEN-07 실행 증거

2026-10-05 저장소 루트에서 Windows x64와 Go 1.26.5로 실행했다. 아래 시간은 해당 명령 묶음의 wall time이며, 하위 시험 시간은 Go 출력값이다.

| 검증 | 명령 | 결과·시간 |
| --- | --- | --- |
| 빌드·vet와 두 수용 시험 | 수용 명령 A(아래) | PASS, 19.90s. MQTT 5 하위 시험 6개 PASS(1.66s). 실제 binary 3-node 시험 PASS(6.35s). |
| 변경 package 회귀 | `go test ./internal/application/tenantlimit ./internal/adapters/memory/sessionstore ./internal/domain/routing ./internal/application/connect ./internal/application/subscribe ./internal/adapters/tcp ./internal/adapters/metrics ./cmd/narumq -count=1` | PASS, 19.01s. 8개 package가 통과했다. |
| 기존 보안·listener·저장 quota 회귀 | 회귀 명령 B(아래) | PASS, 15.02s. 보안 4개 하위 시험, listener 2개 시험, 실제 binary 저장 quota 시험이 통과했다. |
| 전체 local gate 첫 실행 | `GOMAXPROCS=4 go run ./tools/verify local` | FAIL, 191.83s. `tests/benchmark/internal/environment`의 `TestRunClusterTargetKeepsPublishedOwnerPortAcrossBrokerRestart`에서 stats sampler가 `exit status 1`로 종료했다. Integration(108.732s), race, fuzz, traceability와 architecture는 통과했다. Coverage 판정 단계에는 도달하지 못했다. |
| Benchmark restart sampler 단독 확인 | `GOMAXPROCS=4 go test ./tests/benchmark/internal/environment -run '^TestRunClusterTargetKeepsPublishedOwnerPortAcrossBrokerRestart$' -count=1 -v` | PASS, 16.52s(시험 14.11s). 해당 코드를 바꾸지 않았다. 단독 통과만으로 첫 실패의 원인을 확정하지 않는다. |
| 전체 local gate 두 번째 실행 | `GOMAXPROCS=4 go run ./tools/verify local` | FAIL, 147.56s. 새 `TestConnectTenantQuotaPreservesDetachedTimers`가 unordered snapshot 두 개를 순서대로 비교해 실패했다. Lifecycle을 `SessionKey`로 조회해 비교하도록 시험을 고쳤다. Broker 코드는 바꾸지 않았다. Benchmark 환경 package와 integration(99.278s)은 통과했다. |
| Timer 시험 100회와 직렬화 보완 전 전체 local gate | `gofmt -w internal/application/connect/tenant_limits_test.go && go test ./internal/application/connect -run '^TestConnectTenantQuotaPreservesDetachedTimers$' -count=100 && GOMAXPROCS=4 go run ./tools/verify local` | PASS, 묶음 전체 184.96s. Timer 시험 100회 PASS(0.520s). Unit, integration, race, fuzz, vet, traceability, architecture와 coverage gate가 모두 통과했다. |
| Local subscription 경쟁 재현(수정 전) | `go test ./internal/application/subscribe -run '^TestSubscribeTenantQuotaSerializes' -count=1 -v` | FAIL, 5.12s(시험 package 0.561s). 한도 1에서 두 add가 모두 성공했다. 대기 중인 replacement가 완료된 remove를 덮어썼다. |
| 직렬화 보완 뒤 build·회귀·race·수용 시험 | 직렬화 명령 C(아래) | PASS, 묶음 전체 43.00s. 경쟁 회귀 2개 PASS(2.509s), subscribe race PASS(3.612s), MQTT 5 수용 시험 6개 PASS(1.63s), 실제 binary 3-node 시험 PASS(6.87s). |
| 직렬화 보완 뒤 최종 전체 local gate | `GOMAXPROCS=4 go run ./tools/verify local` | PASS, 171.69s. Unit, integration(98.231s), race, fuzz, vet, traceability, architecture와 coverage gate가 모두 통과했다. |

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
gofmt -w internal/application/subscribe/subscribe.go internal/application/subscribe/tenant_limits_test.go &&
go build ./... &&
go test ./internal/application/subscribe -run '^TestSubscribeTenantQuotaSerializes' -count=1 -v &&
go test -race ./internal/application/subscribe -count=1 &&
GOMAXPROCS=4 go test ./tests/integration -run '^(TestMQTTTenantLimits|TestDynamicClusterTenantLimitsProcessAcceptance)$' -count=1 -v -timeout 20m
```

MQTT 5 시험은 연결 상한 2, session 상한 3, subscription 상한 2를 확인했다. 17-byte payload는 상한 16을 넘어 PUBACK `0x97`로 거절되며 subscriber에 전달되지 않았다. Rate 시험은 실제 clock과 여러 연결의 tenant 공통 bucket을 사용했다. Rate 2/s와 `maxWait=1s`에서 burst 2건 뒤의 성공 ACK는 400ms 이상 늦었고, 추가 3건 중 1건은 `0x97`로 거절됐다. Tenant-b의 override 0은 tenant-a의 한도와 독립적으로 동작했다.

별도 TCP 시험은 고정된 policy clock과 실제 timer로 같은 연결의 세 번째 ACK 지연과 다섯 번째 `0x97`을 확인한다. QoS별 MQTT 5·3.1.1 응답, pending QoS 2 DUP의 과금 제외, shutdown 중 대기 취소, Receive Maximum 우선순위와 거절 ACK 뒤 슬롯 회수를 함께 확인한다.

실제 binary 시험은 committed subscription projection을 세 node에서 확인한 뒤 node3의 SUBACK `0x80`을 확인했다. Node1에서 CONNACK `0x03`으로 거절된 client가 node2에서는 연결됐다. 각 거절 node의 `/metrics` counter가 1이었다.

초기 수용 시험 실행은 신규 시험 코드의 type 오류와 process helper의 CONNECT credential flag 오류로 실패했다. 시험 코드를 고친 뒤 위 명령 묶음이 통과했다. 실패를 broker 동작의 통과 증거로 계산하지 않는다.

### 9.2 SEC-11 / NFR-17 실행 증거

2026-10-08 저장소 루트에서 Windows x64와 Go 1.26.5로 실행했다.

| 검증 | 명령 | 결과 |
| --- | --- | --- |
| 감사 회귀 package | `go test ./internal/application/audit ./internal/adapters/http/management ./internal/adapters/auditlog ./cmd/narumq ./tests/contracts/audit-event-v1 ./internal/adapters/metrics -count=1` | PASS. 6개 package. 중첩된 잘못된 UTF-8과 unexported embedded struct의 exported field, panic의 HTTP 상태 500, Windows rotation 대상 보존, 다른 volume 경로 허용, schema contract와 metric 시험이 통과했다. |
| Audit metric contract | `go test ./internal/adapters/metrics -count=1` | PASS. Fixed outcome series, unknown outcome와 readiness gauge 시험이 통과했다. |
| 실제 3-process cluster | `go test ./tests/integration -run '^TestDynamicClusterManagementAuditProcessAcceptance$' -count=1 -timeout=10m -v` | PASS, 3.92s. Node 1의 drain mutation은 `503 audit_unavailable`로 거절됐다. `/metrics`에서 `narumq_audit_publisher_ready 0`과 `mutation_rejected` 1을 확인했고 MQTT CONNECT는 성공했다. Node 2의 두 event는 401 거절과 feature 활성화 실패이며 response request ID와 일치했다. CLI는 2개 record를 검증했고 변조한 사본은 `audit.jsonl:1`에서 거절했다. |
| Historical release compatibility | `NARUMQ_RELEASE_COMPAT=1 go test ./tests/integration -run 'TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance|TestClusterBackupRestoreHistoricalReleaseProcessAcceptance' -count=1 -timeout 60m` | PASS, 82.671s. 두 historical process acceptance 시험이 통과했다. |
| 전체 Go package build | `go build ./...` | PASS. |
| Go vet | `go vet ./...` | PASS. |
| Traceability·architecture | `go run ./tools/verify traceability && go run ./tools/verify architecture` | PASS. |
| Full local gate | `GOMAXPROCS=4 go run ./tools/verify local` | FAIL, 125.04s. Traceability, architecture, fuzz와 integration package(88.407s)가 통과했다. `go test ./...`는 BP-07 smoke profile digest assertion에서 실패해 coverage gate에 도달하지 못했다. |

Full local gate는 통과하지 않았다. `tests/benchmark/bp07/profile_test.go::TestSmokeProfileIdentityAndDigest`는 digest `e1f305e1e5c38e4b00e161bfce733c46cd393075517241978594470fb508a7fb`를 기대한다. 현재 `tests/benchmark/bp07/testdata/smoke.json`의 SHA-256은 `1d543973107a0ae57697b0eb56cb63b69a056b67cffca5c6a11bd1fb103b9446`다. 이 fixture와 test assertion은 audit 변경 범위 밖이므로 수정하지 않았다.
