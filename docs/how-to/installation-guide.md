# NaruMQ 소스 빌드 및 실행 방법

이 절차는 NaruMQ 바이너리를 소스 코드에서 빌드하고, 개발용 로컬 독립 실행(Standalone) 모드와 보안 단일 노드 모드(TLS 및 보안 정책 적용)로 프로세스를 기동하여 정상 동작을 확인한다. 본 저장소는 공식 배포 패키지(deb/rpm/helm 등)나 프로덕션 HA 인스톨러를 제공하지 않으며, 소스 빌드 기반 단일 프로세스의 실행 및 HTTP 관리 엔드포인트의 헬스 프로브와 메트릭 출력을 통해 브로커 상태를 검증하는 절차를 안내한다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | NaruMQ를 소스에서 빌드하고 로컬 또는 시험 환경에서 실행하려는 개발자 및 엔지니어 |
| 검증 상태 | Draft; Windows/amd64, Go 1.26.5 환경에서 소스 컴파일, 익명 개발 모드 로컬 기동, 헬스 프로브 및 메트릭 엔드포인트 응답 대조 완료 (TLS 보안 모드 및 클러스터 모드는 수동 절차 미실행) |
| 검증 환경·버전 | Windows/amd64, Go 1.26.5 |
| 최종 검증일 | 2026-09-26 |
| 관련 기준 문서 | [Project structure](../architectures/project-structure.md), [Broker core architecture](../architectures/broker-core.md), [Management API v1 Contract](../contracts/management-api-v1.md), [Cluster session and durable delivery 스펙](../specs/cluster-session-and-delivery.md) |

## 1. 대상과 영향

- 대상: NaruMQ 단일 브로커 프로세스(`cmd/narumq` 소스 빌드 바이너리).
- 네트워크 포트:
  - `1883`: 개발용 평문(Plaintext) MQTT 리스너 포트 (보안을 위해 루프백 주소 `127.0.0.1`에만 바인딩 허용).
  - `8883`: TLS(Transport Layer Security) MQTT 리스너 포트.
  - `8080`: 관리 HTTP API, 헬스 프로브 및 Prometheus 메트릭 수집 포트 (현재 루프백 주소만 허용).
- **CLI 실행 제약 및 보안 기본값:**
  - NaruMQ CLI는 보안-기본(Security-by-default) 정책에 따라 MQTT 리스너 활성화 시 `-security-config`를 필수로 요구합니다.
  - 예외적으로 로컬 개발 환경에서만 `-tcp-listen`을 루프백(`127.0.0.1`)으로 지정하고 `-allow-insecure-anonymous-mqtt` 플래그를 함께 주어야 합니다. 외부 공개 주소에 익명 바인딩을 시도하면 기동이 차단됩니다.
- **구현 경계 및 동작 범위:**
  - 현재 standalone 리스너(`cmd/narumq/main.go`)는 인메모리 세션, 딜리버리 큐 및 retained 저장소를 사용하며 기동 로그에 `"qos0_only": true, "retained_supported": true`를 출력합니다.
  - standalone 단일 프로세스는 영속적 세션 복구나 분산 QoS 1/2 내구성(Durable ACK)을 제공하지 않으며, 이는 개발 및 단일 노드 보안 검증을 위한 경계입니다. 분산 고가용성(HA) 및 영속 상태 복제는 클러스터 런타임 스파이크의 개발 범위에 해당합니다.
- 사용자 영향: 로컬 포트를 점유하므로 기존에 해당 포트를 사용하는 프로세스가 있다면 충돌이 발생할 수 있습니다.

## 2. 전제 조건

- [ ] Go 1.26.5 이상의 Go 툴체인이 시스템에 설치되어 있으며, 터미널에서 `go version`으로 확인 가능하다.
- [ ] 소스 코드를 복제(clone)할 수 있는 Git 환경이 준비되어 있다.
- [ ] 브로커가 바인딩할 로컬 포트(`1883`, `8080`, `8883`)가 다른 프로세스에서 사용 중이지 않다.
- [ ] 보안 모드를 실행할 경우, 서버 TLS 인증서(`server.crt`), 개인키(`server.key`) 및 보안 설정 JSON 파일(`security.json`)이 준비되어 있다.
- [ ] 비밀 값(개인키 파일 내용, 관리 API 토큰 등)을 터미널 명령어 인자나 공개 로그에 직접 노출하지 않는다.

## 3. 절차

모든 명령어는 저장소 루트 디렉터리(`narumq`)에서 실행한다.

### 1단계: 소스 코드 준비 및 의존성 검증

저장소 루트로 이동하여 필요한 외부 Go 모듈 의존성을 내려받고 무결성을 검증한다.

```sh
go mod download
go mod verify
```

기대 결과: 오류 메시지 없이 완료되며 `all modules verified`가 출력된다.

### 2단계: NaruMQ 바이너리 컴파일

`cmd/narumq` 패키지를 컴파일하여 실행 파일 바이너리를 생성한다.

```sh
go build -o bin/narumq ./cmd/narumq
```

Windows 환경에서는 다음과 같이 실행 파일을 생성한다.

```sh
go build -o bin/narumq.exe ./cmd/narumq
```

기대 결과: `bin/` 디렉터리에 실행 가능한 `narumq`(또는 `narumq.exe`) 바이너리가 생성된다.

### 3단계: 브로커 실행 모드 선택 및 기동

용도에 따라 다음 실행 모드 중 하나를 선택하여 실행한다.

#### 방식 A: 로컬 개발용 단일 노드 모드 (익명 연결 허용)

로컬 개발 및 간단한 기능 확인을 위해 루프백 주소(`127.0.0.1`)에서 익명 MQTT 연결을 허용하는 모드다. NaruMQ는 기본적으로 보안이 비활성화된 외부 바인딩을 차단하므로, 익명 모드는 반드시 루프백 IP와 `-allow-insecure-anonymous-mqtt` 플래그를 함께 지정해야 한다.

```sh
./bin/narumq -tcp-listen 127.0.0.1:1883 -http-listen 127.0.0.1:8080 -allow-insecure-anonymous-mqtt
```

기대 결과:
터미널에 다음과 같은 구조화 로그(JSON 형식)가 출력되며 프로세스가 대기 상태를 유지한다.
```json
{"level":"INFO","msg":"MQTT TCP listener started","address":"127.0.0.1:1883","development_only":true,"qos0_only":true,"retained_supported":true}
{"level":"INFO","msg":"management HTTP listener started","address":"127.0.0.1:8080"}
```
*(주의: standalone 리스너는 인메모리 저장소 기반이며 `qos0_only: true`로 동작합니다. 영속 세션 및 분산 HA는 지원하지 않습니다.)*

#### 방식 B: 보안 단일 노드 모드 (TLS 및 보안 설정 적용)

TLS 암호화 통신과 JWT(JSON Web Token), username/password 또는 mTLS(Mutual TLS, 상호 인증) 기반 인증 및 ACL(Access Control List, 접근 제어 목록) 정책을 적용하여 기동한다.

1. 보안 설정 파일(`config/security.json`)을 준비한다.
   ```json
   {
     "jwt": {
       "issuer": "https://auth.example.com",
       "audience": "narumq",
       "algorithms": ["RS256"],
       "jwksUrl": "https://auth.example.com/.well-known/jwks.json",
       "cacheTtl": "15m",
       "refreshCooldown": "30s",
       "clockSkew": "30s",
       "httpTimeout": "5s"
     },
     "identities": [
       {
         "issuer": "https://auth.example.com",
         "subject": "device-client-1",
         "tenantId": "tenant-iot",
         "credentialId": "device-client-1-key",
         "allowedClientIds": ["device-client-1"],
         "roles": ["sensor"]
       }
     ],
     "tenantLimits": {
       "maxWait": "1s",
       "default": {
         "maxConnections": 1000,
         "maxSessions": 2000,
         "maxSubscriptions": 10000,
         "messagesPerSecond": 500,
         "bytesPerSecond": 1048576,
         "maxPayloadBytes": 262144
       },
       "tenants": [
         { "tenantId": "tenant-iot", "maxConnections": 10, "messagesPerSecond": 0 }
       ]
     },
     "acl": [
       {
         "tenantId": "tenant-iot",
         "role": "sensor",
         "effect": "allow",
         "action": "publish",
         "topicFilter": "sensors/#"
       }
     ]
   }
   ```

   `tenantLimits`는 선택 설정이다. 위 수치는 실행 예시이며 제품 기본 보장이 아니다.
   배포할 한도를 직접 정한다. Tenant override에서 생략한 값은 `default`를 상속하고,
   명시한 0은 해당 제한을 끈다. 연결·session·rate는 node별로 집계한다.
   Cluster에서도 같은 security config schema를 사용한다. 설정 범위와 초과 응답은
   [Tenant 스펙 §5.2](../specs/tenancy-and-security.md#52-연결-상태와-traffic-한도)를 따른다.
   Password 방식은 위 JWT 예시의 `jwt`와 `identities`를 대체한다. 두 방식의 설정을 함께
   넣지 않는다. `mtlsIdentities`는 password 방식과 함께 사용할 수 있다. Password 방식의
   Username은 대소문자를 구분한다. `tenantId`, `subject`, `credentialId`,
   `allowedClientIds`, `roles`는 설정의 binding에서 정한다. TLS certificate와 MQTT
   Username·Password를 한 CONNECT에 함께 보내면 거절한다.

   **비밀번호 hash 생성과 설정**

   - 비밀번호 원문을 보호된 비밀 저장소에서 stdout으로 전달한다. 아래는 pipe 형태다.
     `secret-manager-command`를 조직에서 승인한 비밀 저장소 명령으로 바꾼다. 비밀번호를
     명령 인자, 환경 변수, 임시 파일에 저장하지 않는다.
     ```sh
     secret-manager-command | ./bin/narumq -hash-password
     ```

     PowerShell에서는 Windows 바이너리를 사용한다.
     ```powershell
     secret-manager-command | .\bin\narumq.exe -hash-password
     ```
     `-hash-password`는 stdin에서 비밀번호를 읽고 Argon2id PHC hash 한 줄만 stdout에 쓴다.
     입력 끝의 LF 또는 CRLF 한 줄을 제거한다. 이 flag는 `-security-config` 등 broker 실행
     flag와 함께 사용할 수 없다.

   - stdout의 hash를 접근이 제한된 보안 설정 관리 경로에 기록한다. Hash는 offline password
     추측에 쓰일 수 있으므로 이슈, 로그, 공개 저장소에 올리지 않는다. 설정에
     `passwordIdentities`를 추가한다. 아래에서 `passwordHash` 값은 실제 CLI 출력으로 바꾼다.
     ```json
     {
       "passwordIdentities": [
         {
           "username": "device-client-1",
           "passwordHash": "<REPLACE WITH -hash-password OUTPUT>",
           "tenantId": "tenant-iot",
           "subject": "device-client-1",
           "credentialId": "device-client-1-password",
           "allowedClientIds": ["device-client-1"],
           "roles": ["sensor"]
         }
       ]
     }
     ```

   - 설정의 ACL에 해당 tenant와 role의 허용 topic을 지정한다. TLS 인증서·키와
     `-security-config`를 사용해 broker를 시작한다. Password provider만 구성한 CONNECT는
     MQTT Username과 Password를 모두 보내야 한다.

2. TLS 인증서와 키를 지정하여 프로세스를 시작한다. 관리 리스너(`-http-listen`)는 로컬 루프백(`127.0.0.1`) 전용으로 바인딩되며, 명령줄에 인증 토큰 등 비밀 값을 노출하지 않고 로컬 익명 헬스 프로브(`/health/ready`) 및 메트릭(`/metrics`) 엔드포인트를 통해 상태를 안전하게 검증한다.
   ```sh
   ./bin/narumq \
     -tls-listen 127.0.0.1:8883 \
     -tls-cert certs/server.crt \
     -tls-key certs/server.key \
     -security-config config/security.json \
     -http-listen 127.0.0.1:8080
   ```

기대 결과: TLS 리스너(`127.0.0.1:8883`)가 시작되고 보안 번들이 로드되었다는 로그가 출력된다.

#### 방식 C: 분산 클러스터 모드 (개발 및 시험용 스파이크)

분산 합의 기반의 클러스터 기능은 현재 Phase 2 개발 및 검증 진행 중인 단계입니다. 프로덕션 HA 운영 배포물이 아니며, 개발용 피어 실행 및 동적 멤버십 변경 시험 절차는 다음 문서를 참조한다.
- [고정 voter Raft peer-only 실행 가이드](fixed-voter-raft-peer-only.md)
- [동적 클러스터 멤버십 변경 가이드](dynamic-cluster-membership.md)

#### Listener 연결 한도 설정

TCP와 TLS의 최대 연결 수와 초당 신규 연결 수는 따로 지정할 수 있다.
설정 범위와 초과 처리의 기준은 [MQTT 스펙 §7.1](../specs/mqtt-protocol-and-session.md#71-현재-상한과-기본값)을 따른다.

| Listener | 최대 연결 수 flag | 초당 신규 연결 수 flag |
| --- | --- | --- |
| TCP | `-tcp-max-connections` | `-tcp-connection-rate` |
| TLS | `-tls-max-connections` | `-tls-connection-rate` |

예를 들어 방식 A 명령에 `-tcp-max-connections 100 -tcp-connection-rate 20`을
추가하면 TCP listener에 두 한도를 적용한다. 해당 listener를 끄려면 주소를
비우고 그 listener의 두 한도도 0으로 설정한다.

Cluster mode에서는 flag를 함께 지정하지 않는다. Cluster JSON의 `mqtt`에
`tcpMaxConnections`, `tcpConnectionRate`, `tlsMaxConnections`, `tlsConnectionRate`를
넣는다. 필드 형식은 [cluster config 계약](../contracts/raft-membership-v2.md#14-combined-cluster-configuration)을 따른다.

초과 연결이 닫히면 `/metrics`의
`narumq_mqtt_listener_admission_rejected_total{transport,reason}`를 확인한다.
`transport`로 listener 종류를, `reason`으로 연결 수와 rate 초과를 구분한다.
거절한 socket에는 CONNACK을 보내지 않으며 거절 로그도 남기지 않는다.

Management API는 각 node에서 인증된 principal의 초당 요청 수와 burst를 기본 50으로 제한한다. 인증 실패는 RemoteAddr IP별로 초당 5회, burst 5회까지 허용한다. Standalone mode에서는 `-http-request-rate`와 `-http-auth-failure-rate`를 설정하고, Cluster mode에서는 `management.requestRate`와 `management.authFailureRate`를 설정한다. 설정 범위는 `1..1,000,000`이며 Cluster JSON에서 필드를 생략하면 각각 50과 5를 쓴다. 한도를 넘으면 HTTP `429`와 `Retry-After: 1`을 반환한다. Health probe와 `/metrics`는 제한하지 않는다. 자세한 정책은 [Management API 스펙 §1.5](../specs/management-api-and-observability.md#15-rate-limit)를 따른다.

### 4단계: 브로커 상태 및 엔드포인트 동작 확인

별도의 터미널 창을 열고 HTTP 엔드포인트를 호출하여 브로커의 준비 상태(Readiness)와 메트릭 수집을 검증한다.

1. 준비 상태 프로브(Readiness Probe) 확인:
   ```sh
   curl -s http://127.0.0.1:8080/health/ready
   ```
   기대 결과: HTTP 200 상태 코드와 함께 상태가 `UP`으로 응답한다.
   ```json
   {"data":{"status":"UP","reason":null,"observedAt":"..."}}
   ```

2. 프로메테우스(Prometheus) 메트릭 엔드포인트 확인:
   ```sh
   curl -s http://127.0.0.1:8080/metrics
   ```
   기대 결과: `narumq_connections_active`, `narumq_connections_total` 등 NaruMQ 표준 메트릭 텍스트가 출력된다.

3. MQTT 연결 시험 (선택 사항):
   표준 MQTT 클라이언트(예: `mosquitto_sub`, `mosquitto_pub`)를 사용하여 메시지 송수신을 확인한다.
   ```sh
   # 구독자 실행
   mosquitto_sub -h 127.0.0.1 -p 1883 -t "test/topic" -q 0 -V mqttv5

   # 다른 터미널에서 발행자 실행
   mosquitto_pub -h 127.0.0.1 -p 1883 -t "test/topic" -m "hello narumq" -q 0 -V mqttv5
   ```
   기대 결과: 구독자 터미널에 `hello narumq` 메시지가 정상 수신된다.

## 4. 검증

실행 결과가 다음 성공 조건을 충족하는지 대조한다. 본 절차의 실제 실행 검증은 Windows/amd64 환경에서 개발용 익명 모드 스모크 테스트로 확인하였으며, TLS 모드 및 클러스터 모드의 수동 기동 절차는 미실행 상태다.

| 관찰 대상 | 성공 조건 | 확인 방법 | 실제 결과 | 실행 증거 위치 |
| --- | --- | --- | --- | --- |
| 바이너리 컴파일 | 종료 코드 0으로 `bin/narumq` 바이너리 생성 | `go build -o bin/narumq ./cmd/narumq` | 통과 (Windows/amd64) | 로컬 파일 시스템 `bin/` |
| 개발 브로커 기동 (방식 A) | TCP 1883 및 HTTP 8080 리스너 로그 출력 | 터미널 stdout 로그 확인 | 통과 (Windows/amd64); `MQTT TCP listener started`, `management HTTP listener started` 출력 | 프로세스 표준 출력 |
| Readiness 프로브 | HTTP 200 및 `{"status":"UP"}` JSON 응답 | `curl -s http://127.0.0.1:8080/health/ready` | 통과 (Windows/amd64); `{"data":{"status":"UP",...}}` | HTTP 응답 본문 |
| 메트릭 수집 | OpenMetrics/Prometheus 형식 텍스트 출력 | `curl -s http://127.0.0.1:8080/metrics` | 통과 (Windows/amd64); `narumq_connections_active` 등 메트릭 출력 | HTTP 응답 본문 |
| 보안 단일 노드 모드 (방식 B) | TLS 리스너 바인딩 및 인증서 기반 접속 | TLS 및 보안 설정으로 기동 | 미실행; 본 가이드의 수동 실행 절차 미실행 (`cmd/narumq/main_test.go` 테스트 스위트에서만 확인) | `cmd/narumq/main_test.go` |
| 분산 클러스터 모드 (방식 C) | 3개 노드 Raft 합의 및 리더 선출 | 클러스터 설정으로 기동 | 미실행; 본 가이드의 수동 3노드 절차 미실행 ([fixed-voter-raft-peer-only.md](fixed-voter-raft-peer-only.md) 참조) | 관련 how-to 가이드 |

## 5. 중단 조건

다음 상황이 발생하면 즉시 프로세스 기동을 중단하고 원인을 조사한다.

- `bind: address already in use`: 대상 포트(1883, 8080, 8883)가 이미 다른 프로세스에 점유되어 있으므로, 포트를 변경하거나 충돌 프로세스를 종료하기 전까지 진행하지 않는다.
- `-allow-insecure-anonymous-mqtt requires a loopback-only -tcp-listen`: 익명 플래그를 사용하면서 `0.0.0.0` 등 외부 공개 주소에 바인딩하려는 경우 보안 위반으로 즉시 차단된다. 외부 통신이 필요한 경우 반드시 `-tls-listen` 및 `-security-config`를 사용해야 한다.
- `decode security config`: 보안 설정 JSON 파일의 문법이 올바르지 않거나 필수 항목이 누락된 경우.
- `configure TLS: open certFile`: 지정한 TLS 인증서 파일이나 비밀키 경로를 찾을 수 없거나 권한이 없는 경우.

## 6. 이전 상태로 되돌리는 방법

1. 실행 중인 터미널에서 `Ctrl+C`(SIGINT)를 입력하여 NaruMQ 프로세스를 안전하게 종료한다.
2. 컴파일된 바이너리를 안전하게 삭제한다. `bin/` 디렉터리에 다른 개인 산출물이 있는지 먼저 확인한 뒤 NaruMQ 빌드 바이너리만 삭제한다.
   ```sh
   rm -f bin/narumq bin/narumq.exe
   # bin/ 디렉터리가 비어 있고 다른 파일이 없다면 디렉터리 정리
   rmdir bin 2>/dev/null || true
   ```
3. 생성된 임시 설정 파일이나 테스트용 인증서가 있다면 해당 파일만 지정하여 정리한다.

## 7. 문제 전달

자체적으로 해결되지 않는 문제가 발생할 경우 다음 사항을 확인하여 담당자에게 문의한다.

- 전달 대상: NaruMQ 코어 유지보수 팀 및 인프라 담당자.
- 보존할 증거: 실행 명령어 전문, NaruMQ 표준 에러 및 로그 전체(JSON 형태), `go version` 출력 결과, `curl -v http://127.0.0.1:8080/health/ready` 상세 응답 헤더.
- 비밀 값 제외 규칙: 서버 개인키(`*.key`), 인증 토큰(`http-auth-token`), JWT 토큰 원문 등 민감한 자격 증명은 절대로 보고서나 이슈에 첨부하지 않고 마스킹 처리한다.
