# NaruMQ 개요 및 지원 범위

| 항목 | 내용 |
| :--- | :--- |
| **대상 독자** | NaruMQ를 처음 도입하거나 평가하는 모든 개발자 및 시스템 엔지니어 |
| **문서 목적** | NaruMQ의 제품 특징, 지원 프로토콜 규격, 현재 구현 경계를 파악한다. |
| **적용 버전** | NaruMQ 0.5 (Go 1.26.5 기반 소스 빌드) |

---

## 1. NaruMQ 소개

NaruMQ는 컨테이너 및 클라우드 환경에 최적화된 분산 MQTT 브로커입니다. 장기 연결(Long-lived connection) 기반인 MQTT 프로토콜은 컨테이너 재시작, 파드(Pod) 이동, 롤링 업데이트 환경에서 연결이 끊어지거나 메시지가 유실되기 쉽습니다. NaruMQ는 클라우드 네이티브 환경에서도 안정적인 연결 생애주기와 메시지 무결성을 제공하는 것을 목표로 합니다.

---

## 2. 핵심 가치

1. **보안 기본 정책 (Security-by-Default):**
   - 개발 편의를 위한 루프백(`127.0.0.1`) 익명 모드를 제외하고, 외부 네트워크 바인딩 시 TLS와 보안 설정 파일(`-security-config`)을 필수로 요구합니다.
   - 접근 권한 정책은 명시적 허용이 없으면 거부하는 Default-Deny 원칙을 따릅니다.
2. **관측성과 운영 표준 준수:**
   - Prometheus 호환 메트릭(`/metrics`), Kubernetes 프로브(`/health/live`, `/health/ready`), JSON 기반의 REST 관리 API(`/api/v1`)를 제공합니다.
3. **엄격한 소프트웨어 아키텍처:**
   - 클린 아키텍처(Ports and Adapters) 원칙을 적용하여 비즈니스 정책을 외부 I/O와 분리하고, 높은 테스트 커버리지로 품질을 검증합니다.

---

## 3. 프로토콜 지원 규격 및 구현 경계

NaruMQ는 현재 개발 로드맵에 따라 기능을 단계적으로 확장하고 있습니다. 사용 목적에 맞춰 지원 범위를 확인하십시오.

### 지원 프로토콜 규격

- **MQTT 프로토콜:** MQTT v3.1.1 및 MQTT v5.0 코어 패킷 사양을 지원합니다.
  - 패킷: `CONNECT`, `CONNACK`, `PUBLISH`, `PUBACK`, `SUBSCRIBE`, `SUBACK`, `UNSUBSCRIBE`, `UNSUBACK`, `PINGREQ`, `PINGRESP`, `DISCONNECT`
- **전송 계층(Transport):**
  - 표준 TCP (로컬 개발 전용)
  - TLS 1.2 및 TLS 1.3 암호화 통신 (운영 권장)
  - MQTT over WebSocket 및 WSS (WebSocket 엔드포인트 `/mqtt`, subprotocol `mqtt`)
  - HTTP/1.1 REST 관리 API v1
  - Prometheus / OpenMetrics 호환 메트릭 수집 엔드포인트

### 현재 런타임 구현 경계

| 런타임 구분 | 지원 기능 | 미지원 또는 주의 사항 |
| :--- | :--- | :--- |
| **단일 노드 standalone (`cmd/narumq`)** | • 루프백 개발 익명 모드<br>• TLS 기반 단일 노드 보안 실행<br>• 프로세스 메모리 내 QoS 0, 1, 2 핸드셰이크 지원<br>• 인메모리 세션 및 메시지 라우팅<br>• 관리 REST API 및 메트릭 수집 | • 프로세스 재시작 시 인메모리 세션·큐 초기화 (비영속 메모리 런타임)<br>• 다중 노드 분산 복제 및 디스크 내구성(Durable ACK) 미제공 |
| **클러스터 런타임 (Phase 2 Preview)** | • Raft 합의 기반 피어 통신<br>• 동적 멤버십 변경 시험 절차<br>• 오프라인 백업 및 복원 절차 | • 프로덕션 무중단 롤링 배포 스펙은 Draft 상태<br>• 개발용 수동 시험 절차로 제공 |

---

## 4. 다음 단계

- NaruMQ를 로컬 환경에서 5분 만에 빌드하고 실행하려면 [5분 빠른 시작](./quick-start.md)을 참조하십시오.
- 애플리케이션에서 메시지를 발행하고 구독하는 방법은 [클라이언트 개발자 가이드](../client-guide/connection-and-auth.md)를 확인하십시오.
- 브로커 빌드 및 프로덕션 환경 설정은 [브로커 운영자 가이드](../operator-guide/installation.md)를 확인하십시오.
