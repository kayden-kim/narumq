# 자주 묻는 질문 및 오류 해결 (Troubleshooting)

| 항목 | 내용 |
| :--- | :--- |
| **대상 독자** | NaruMQ 브로커 기동 또는 클라이언트 통신 중 오류가 발생한 운영자 및 개발자 |
| **문서 목적** | 대표적인 오류 증상별 원인을 파악하고 신속하게 조치하는 해결 절차를 안내한다. |

---

## 1. 브로커 기동 오류 (Broker Startup Failures)

### 증상 1: `security-config is required` 오류로 프로세스 종료
- **원인:** 보안-기본(Security-by-Default) 정책에 따라 외부 인터페이스 바인딩 시 보안 설정 없이 기동하는 것을 차단했습니다.
- **해결 방법:**
  - 로컬 개발 목적이라면 IP를 루프백(`127.0.0.1`)으로 지정하고 `-allow-insecure-anonymous-mqtt` 플래그를 추가하십시오.
  - 외부 서비스 목적이라면 `-tls-listen`과 함께 `-security-config <경로>`를 제공하십시오.

### 증상 2: `bind: address already in use`
- **원인:** 브로커가 사용하려는 포트(`1883`, `8883`, `8080` 등)를 기존 프로세스(다른 브로커, 웹 서버 등)가 점유하고 있습니다.
- **해결 방법:**
  - Linux/macOS: `sudo lsof -i :1883` 또는 `netstat -tuln | grep 1883`으로 점유 PID를 찾아 종료하십시오.
  - Windows: `netstat -ano | findstr :1883`으로 PID를 확인 후 작업 관리자에서 종료하십시오.

### 증상 3: `BuildTLSConfig: cert and key are required`
- **원인:** `-tls-listen` 또는 `-wss-listen`을 지정했으나 `-tls-cert` 및 `-tls-key` 파일 경로를 누락했습니다.
- **해결 방법:** 유효한 PEM 형식의 인증서 파일과 개인키 파일 경로를 지정하십시오.

---

## 2. 클라이언트 연결 오류 (Client Connection Failures)

### 증상 1: `CONNACK 0x05` 또는 `0x87` (Not Authorized)
- **원인:**
  1. 클라이언트가 접속한 계정의 `allowedClientIds` 목록에 현재 접속한 ClientId가 포함되지 않았습니다.
  2. 해당 계정 또는 IP가 차단 목록(Blocklist)에 등록되었습니다.
- **해결 방법:** `security.json`의 해당 사용자 항목에서 `allowedClientIds` 패턴에 현재 ClientId가 매칭되는지 확인하십시오.

### 증상 2: `CONNACK 0x04` 또는 `0x86` (Bad User Name or Password)
- **원인:**
  1. Username 또는 비밀번호가 일치하지 않습니다.
  2. 비밀번호 해시가 올바른 Argon2id 형식(`$argon2id$...`)이 아닙니다.
  3. JWT 토큰의 유효기간(`exp`)이 만료되었거나 서명 검증에 실패했습니다.
- **해결 방법:** `./bin/narumq -hash-password`를 사용해 해시를 다시 생성하거나 JWT 토큰의 만료 시간을 갱신하십시오.

### 증상 3: TLS 핸드셰이크 실패 (`x509: certificate signed by unknown authority`)
- **원인:** 자체 서명(Self-signed) 인증서를 사용 중이며, 클라이언트가 신뢰하는 CA 목록에 서버의 CA가 포함되지 않았습니다.
- **해결 방법:** 클라이언트 연결 코드에서 브로커의 Root CA 인증서(`ca.crt`)를 명시적으로 신뢰 인증서로 지정하십시오.

---

## 3. 메시지 송수신 문제 (Pub/Sub Issues)

### 증상 1: 메시지를 발행했는데 구독자에게 도착하지 않음
- **원인:**
  1. **토픽 불일치:** MQTT 토픽은 대소문자를 구분합니다. (`sensors/temp` ≠ `Sensors/Temp`)
  2. **ACL 차단:** Default-Deny 정책으로 인해 해당 토픽에 대한 발행 또는 구독 권한이 없습니다.
- **확인 방법:** 브로커 표준 출력 로그에서 `ACL denial` 또는 `forbidden` 관련 경고 로그를 확인하십시오.

### 증상 2: 연결이 주기적으로 끊어지고 재연결이 반복됨
- **원인:** 두 개 이상의 클라이언트 애플리케이션이 **동일한 ClientId**를 사용하여 접속했습니다.
  - MQTT 프로토콜 규격에 따라 브로커는 동일한 ClientId로 새 연결이 들어오면 기존 연결을 즉시 강제 종료합니다.
- **해결 방법:** 각 디바이스나 프로세스 인스턴스마다 고유한 ClientId를 할당하십시오. (예: `device-sensor-01-uuid`)

---

## 4. 버그 리포트 및 지원 요청

위 내용으로 해결되지 않는 문제가 발생하면 GitHub Issues에 다음 정보를 포함하여 문의해 주십시오.

- NaruMQ 바이너리 버전 (`go version` 및 커밋 해시)
- 실행 시 사용한 명령행 플래그 (비밀 값 제외)
- 브로커 JSON 구조화 로그 출력 내용
- 재현 가능한 최소 클라이언트 코드

::: danger 비밀 값 마스킹 필수
이슈나 로그를 등록할 때 서버 개인키(`*.key`), 인증 토큰, 비밀번호 및 JWT 토큰 원문과 같은 민감한 자격 증명은 반드시 마스킹하거나 제거해야 합니다.
:::
