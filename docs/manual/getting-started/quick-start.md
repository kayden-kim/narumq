# 5분 빠른 시작 (Quick Start)

| 항목 | 내용 |
| :--- | :--- |
| **대상 독자** | 로컬 환경에서 NaruMQ를 빠르게 실행하고 메시지를 주고받으려는 개발자 |
| **목적** | 5분 안에 NaruMQ 바이너리를 빌드하고 로컬 개발 모드로 기동한 뒤 첫 MQTT 메시지를 발행·수신한다. |
| **필요 도구** | Go 1.26.5 이상, Git, MQTT 클라이언트 도구(예: `mosquitto_pub`, `mosquitto_sub` 또는 curl) |

---

## 1. 사전 준비

터미널에서 Go 컴파일러 버전을 확인합니다.

```sh
go version
```

출력 결과에 `go version go1.26.5` 이상의 버전이 표시되어야 합니다.

---

## 2. 1단계: 바이너리 빌드

저장소 루트 디렉터리에서 `cmd/narumq` 패키지를 빌드합니다.

::: code-group

```sh [Linux / macOS]
go build -o bin/narumq ./cmd/narumq
```

```powershell [Windows (PowerShell)]
go build -o bin/narumq.exe ./cmd/narumq
```

:::

빌드가 완료되면 `bin/` 디렉터리에 실행 가능한 `narumq` 바이너리가 생성됩니다.

---

## 3. 2단계: 로컬 개발 모드로 브로커 실행

NaruMQ는 보안-기본(Security-by-Default) 원칙에 따라, 보안 설정 없는 외부 공개 바인딩을 금지합니다. 로컬 개발 환경에서는 루프백 주소(`127.0.0.1`)와 `-allow-insecure-anonymous-mqtt` 플래그를 함께 전달하여 익명 접근을 허용합니다.

::: code-group

```sh [Linux / macOS]
./bin/narumq \
  -tcp-listen 127.0.0.1:1883 \
  -http-listen 127.0.0.1:8080 \
  -allow-insecure-anonymous-mqtt
```

```powershell [Windows (PowerShell)]
.\bin\narumq.exe `
  -tcp-listen 127.0.0.1:1883 `
  -http-listen 127.0.0.1:8080 `
  -allow-insecure-anonymous-mqtt
```

:::

브로커가 정상적으로 실행되면 다음과 같은 JSON 구조화 로그가 터미널에 출력됩니다.

```json
{"level":"INFO","msg":"MQTT TCP listener started","address":"127.0.0.1:1883","development_only":true,"qos0_only":true,"retained_supported":true}
{"level":"INFO","msg":"management HTTP listener started","address":"127.0.0.1:8080"}
```

---

## 4. 3단계: 브로커 상태 확인 (HTTP Probe)

별도의 터미널 창에서 HTTP 관리 엔드포인트를 호출하여 브로커 준비 상태를 확인합니다.

```sh
curl -i http://127.0.0.1:8080/health/ready
```

**기대 응답:**
```http
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{"status":"ok"}
```

---

## 5. 4단계: 첫 MQTT 메시지 발행 및 구독

MQTT CLI 도구(`mosquitto_sub`, `mosquitto_pub`)를 사용하여 메시지를 주고받습니다.

### 1) 터미널 A: 구독자 (Subscriber) 실행
`sensor/temperature` 토픽을 구독합니다.

```sh
mosquitto_sub -h 127.0.0.1 -p 1883 -t "sensor/temperature"
```

### 2) 터미널 B: 발행자 (Publisher) 실행
새 터미널에서 동일한 토픽으로 JSON 메시지를 전송합니다.

```sh
mosquitto_pub -h 127.0.0.1 -p 1883 -t "sensor/temperature" -m '{"temp": 24.5, "unit": "C"}'
```

터미널 A에 발행한 메시지(`{"temp": 24.5, "unit": "C"}`)가 즉시 수신되면 브로커가 정상 동작하는 것입니다.

---

## 6. 다음 단계

- 애플리케이션 코드로 연동하는 방법은 [클라이언트 개발자 가이드](../client-guide/connection-and-auth.md)를 참조하십시오.
- 프로덕션 환경에 맞는 TLS 인증서와 보안 설정을 적용하려면 [운영자 가이드: 보안 설정](../operator-guide/security.md)을 참조하십시오.
