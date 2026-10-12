# TLS 및 보안 정책 설정 가이드

| 항목 | 내용 |
| :--- | :--- |
| **대상 독자** | NaruMQ 브로커의 TLS 암호화, 테넌트 인증, ACL 규칙을 구성하는 인프라 및 보안 엔지니어 |
| **문서 목적** | TLS 1.2+ 리스너 구성, Argon2id 비밀번호 해시 생성, `security.json` 보안 파일 작성법을 이해한다. |
| **관련 스펙** | [Tenant와 security 스펙 (GitHub)](https://github.com/kayden-kim/narumq/blob/main/docs/specs/tenancy-and-security.md) |

---

## 1. TLS 1.2+ 리스너 설정

프로덕션 환경에서는 반드시 TLS로 통신을 암호화해야 합니다.

### 인증서 파일 준비
- `server.crt`: 브로커 도메인(SAN)이 포함된 X.509 서버 인증서 (PEM 형식)
- `server.key`: 서버 비밀키 (PEM 형식, `chmod 600` 적용 필수)
- `ca.crt` (mTLS 상호 인증 시 필요): 클라이언트 인증서를 서명한 Root CA 인증서

### 실행 플래그
```sh
./bin/narumq \
  -tls-listen 0.0.0.0:8883 \
  -tls-cert /opt/narumq/certs/server.crt \
  -tls-key /opt/narumq/certs/server.key \
  -tls-min-version 1.2 \
  -security-config /opt/narumq/etc/security.json
```

---

## 2. 비밀번호 해시 생성 (`-hash-password`)

NaruMQ는 보안을 위해 원문 비밀번호 저장을 금지하며, **Argon2id** 알고리즘 해시만 허용합니다. `narumq` 바이너리의 내장 도구를 사용하여 해시를 생성합니다.

```sh
./bin/narumq -hash-password
```

비밀번호 입력 프롬프트가 표시되면 설정할 비밀번호를 입력합니다.

**출력 예시:**
```text
$argon2id$v=19$m=65536,t=3,p=4$c29tZXNhbHQ...$Rk7c...
```
생성된 문자열 전체를 `security.json`의 `passwordHash` 값에 복사합니다.

---

## 3. 보안 정책 파일 (`security.json`) 작성

보안 설정 파일은 브로커 시작 시 `-security-config` 플래그로 지정합니다.

### 비밀번호 인증 구성 예제
```json
{
  "passwordIdentities": [
    {
      "username": "device-sensor-01",
      "passwordHash": "$argon2id$v=19$m=65536,t=3,p=4$c29tZXNhbHQ...$Rk7c...",
      "tenantId": "tenant-iot",
      "subject": "device-01",
      "credentialId": "cred-pw-001",
      "allowedClientIds": ["device-sensor-01", "sensor-01-*"],
      "roles": ["client"]
    }
  ],
  "acl": [
    {
      "tenantId": "tenant-iot",
      "subjectPattern": "device-01",
      "topicPattern": "sensors/device-01/#",
      "allowPublish": true,
      "allowSubscribe": true
    }
  ]
}
```

### 주요 항목 설명
- `username`: MQTT 클라이언트가 CONNECT 패킷에 보낼 로그인 계정명입니다.
- `tenantId`: 클라이언트가 속할 격리된 테넌트 식별자입니다.
- `allowedClientIds`: 해당 계정으로 접속할 수 있는 ClientId 목록 또는 와일드카드 패턴입니다.
- `roles`: 관리 권한(`admin`, `operator`, `client`)입니다.
- `acl`: 토픽 접근 제어 규칙입니다. 명시된 규칙 외의 발행·구독은 Default-Deny 원칙에 따라 모두 차단됩니다.

---

## 4. JWT 기반 인증 구성 예제

OAuth2/OIDC IDP(Keycloak, Auth0 등)와 연동할 때 사용합니다. (*주의: `jwt`와 `passwordIdentities`는 상호 배타적입니다.*)

```json
{
  "jwt": {
    "issuer": "https://auth.example.com",
    "audience": "narumq-broker",
    "jwksUrl": "https://auth.example.com/.well-known/jwks.json",
    "refreshCooldownMillis": 5000
  },
  "identities": [
    {
      "issuer": "https://auth.example.com",
      "subject": "sub-service-account-01",
      "tenantId": "tenant-enterprise",
      "credentialId": "cred-jwt-001",
      "allowedClientIds": ["service-*"],
      "roles": ["client"]
    }
  ],
  "acl": [
    {
      "tenantId": "tenant-enterprise",
      "subjectPattern": "*",
      "topicPattern": "enterprise/#",
      "allowPublish": true,
      "allowSubscribe": true
    }
  ]
}
```

---

## 5. 관리 API 토큰 설정 (`-http-auth-token-file`)

HTTP 관리 인터페이스(`/api/v1`)를 루프백 외의 IP로 개방할 경우 반드시 인증 토큰 파일을 지정해야 합니다.

1. 난수 토큰 파일 생성:
   ```sh
   openssl rand -hex 32 > /opt/narumq/etc/token.secret
   chmod 600 /opt/narumq/etc/token.secret
   ```
2. 실행 시 플래그 지정:
   ```sh
   -http-listen 0.0.0.0:8080 \
   -http-auth-token-file /opt/narumq/etc/token.secret
   ```
3. API 호출 시 HTTP 요청 헤더에 전달:
   ```sh
   curl -H "Authorization: Bearer <TOKEN_SECRET>" http://broker:8080/api/v1/cluster
   ```

---

## 6. 다음 단계

- HTTP 엔드포인트 세부 명세는 [관리 REST API 가이드](./management-api.md)를 참조하십시오.
- Prometheus 메트릭 모니터링은 [메트릭 및 헬스 프로브 가이드](./observability.md)를 확인하십시오.
