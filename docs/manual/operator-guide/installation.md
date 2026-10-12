# 시스템 요구사항 및 바이너리 빌드

| 항목 | 내용 |
| :--- | :--- |
| **대상 독자** | NaruMQ 브로커를 서버 또는 클라우드 인프라에 직접 설치하고 운영하려는 시스템 엔지니어 |
| **문서 목적** | 소스 코드로부터 운영용 NaruMQ 바이너리를 컴파일하고, 권장 디렉터리 구조 및 systemd 서비스를 구성한다. |
| **빌드 기준** | Go 1.26.5 |

---

## 1. 시스템 요구사항

NaruMQ는 단일 정적 바이너리로 컴파일되어 외부 런타임 종속성 없이 동작합니다.

| 자원 구분 | 최소 요구사항 | 프로덕션 권장 사양 |
| :--- | :--- | :--- |
| **운영체제** | Linux (커널 5.4 이상), macOS, Windows | Linux (Ubuntu 22.04 LTS / 24.04 LTS 권장) |
| **아키텍처** | x86_64 (amd64) 또는 ARM64 | amd64 / arm64 |
| **CPU 코어** | 1 Core | 2 Cores 이상 |
| **메모리 (RAM)** | 512 MB | 2 GB 이상 |
| **빌드 툴체인** | Go 1.26.5 이상, Git | Go 1.26.5 이상 |

---

## 2. 소스 코드 다운로드 및 의존성 검증

저장소를 복제하고 모듈 무결성을 확인합니다.

```sh
git clone https://github.com/kayden-kim/narumq.git
cd narumq

# 모듈 다운로드 및 체크섬 검증
go mod download
go mod verify
```

출력 결과에 `all modules verified`가 표시되면 무결성 검증이 완료된 것입니다.

---

## 3. 프로덕션 바이너리 빌드

디버그 심볼을 제거하고 바이너리 크기를 최적화하여 빌드합니다.

```sh
# Linux / macOS
mkdir -p bin
go build -ldflags="-s -w" -o bin/narumq ./cmd/narumq

# 실행 권한 확인 및 도움말 출력
./bin/narumq -help
```

---

## 4. 권장 서버 디렉터리 레이아웃

Linux 서버 배포 시 다음과 같은 파일 시스템 구조를 권장합니다.

```text
/opt/narumq/
├── bin/
│   └── narumq                  # 실행 바이너리
├── etc/
│   ├── security.json           # 테넌트 및 인증 보안 설정
│   └── token.secret            # 관리 HTTP API 인증 토큰
├── certs/
│   ├── server.crt              # TLS 서버 인증서
│   ├── server.key              # TLS 서버 개인키 (chmod 600)
│   └── ca.crt                  # mTLS 클라이언트 CA 인증서
└── data/                       # 감사 로그 및 런타임 데이터
```

---

## 5. Linux systemd 서비스 등록 예제

운영체제 부팅 시 브로커가 자동 기동되고 장애 시 재시작되도록 systemd 유닛 파일을 생성합니다.

### 1) 서비스 파일 생성 (`/etc/systemd/system/narumq.service`)
```ini
[Unit]
Description=NaruMQ Distributed MQTT Broker
After=network.target

[Service]
Type=simple
User=narumq
Group=narumq
WorkingDirectory=/opt/narumq
ExecStart=/opt/narumq/bin/narumq \
  -tls-listen 0.0.0.0:8883 \
  -tls-cert /opt/narumq/certs/server.crt \
  -tls-key /opt/narumq/certs/server.key \
  -security-config /opt/narumq/etc/security.json \
  -http-listen 127.0.0.1:8080 \
  -http-auth-token-file /opt/narumq/etc/token.secret

Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### 2) 서비스 활성화 및 시작
```sh
sudo systemctl daemon-reload
sudo systemctl enable narumq
sudo systemctl start narumq

# 실행 상태 확인
sudo systemctl status narumq
```

---

## 6. 다음 단계

- 상세한 CLI 플래그 및 런타임 옵션은 [프로세스 및 CLI 설정 가이드](./configuration.md)를 참조하십시오.
- TLS 인증서 및 `security.json` 작성법은 [TLS 및 보안 정책 설정](./security.md)을 확인하십시오.
