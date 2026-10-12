---
layout: home

hero:
  name: NaruMQ
  text: 사용자 매뉴얼
  tagline: 연결은 이동해도, 메시지는 남는다. Kubernetes 친화적 분산 MQTT 브로커
  actions:
    - theme: brand
      text: 5분 빠른 시작
      link: /getting-started/quick-start
    - theme: alt
      text: 클라이언트 개발자 가이드
      link: /client-guide/connection-and-auth
    - theme: alt
      text: 브로커 운영자 가이드
      link: /operator-guide/installation

features:
  - title: 보안 기본 정책 (Security-by-Default)
    details: 외부 네트워크 바인딩 시 TLS와 인증 설정을 필수로 요구하며, Default-Deny 원칙으로 비인가 접근을 차단합니다.
  - title: 컨테이너 및 관측성 친화적
    details: Prometheus 호환 /metrics 엔드포인트와 Kubernetes readiness/liveness 프로브, RESTful 관리 API를 제공합니다.
  - title: 분산 합의 및 고가용성 (Phase 2 Preview)
    details: Raft 합의 알고리즘 기반의 세션 소유권 관리 및 멀티 노드 복제 런타임을 개발하고 있습니다.
---

## 매뉴얼 구성 및 대상 독자

NaruMQ 사용자 매뉴얼은 역할에 따라 필요한 내용을 빠르게 찾을 수 있도록 독자를 구분하여 제공합니다.

| 가이드 | 대상 독자 | 다루는 주요 내용 |
| :--- | :--- | :--- |
| **[시작하기](./getting-started/overview.md)** | 모든 사용자 | NaruMQ 소개, 지원 규격, 현재 구현 경계, 5분 빠른 시작 |
| **[클라이언트 개발자 가이드](./client-guide/connection-and-auth.md)** | MQTT 애플리케이션 개발자 | 브로커 연결, 인증(JWT/비밀번호), 메시지 발행·구독, Go·Python·Node.js 연동 예제 |
| **[브로커 운영자 가이드](./operator-guide/installation.md)** | 시스템 및 인프라 엔지니어 | 바이너리 빌드, CLI 실행 플래그, TLS 및 보안 정책 설정, REST API, Prometheus 모니터링 |
| **[클러스터 런타임 프리뷰](./cluster-preview/cluster-preview.md)** | 분산 시스템 평가자 | Phase 2 Raft 클러스터 런타임 현황 및 시험 실행 안내 |
| **[문제 해결](./troubleshooting/troubleshooting.md)** | 운영자 및 개발자 | 연결 거부, TLS 핸드셰이크 실패, 인증 오류 등 자주 묻는 질문 |

---

## 현재 지원 상태 및 구현 경계

NaruMQ는 로드맵에 따라 단계별로 검증을 진행하고 있습니다. 운영 환경 도입 시 아래 지원 범위를 확인하십시오.

- **Phase 1 (단일 노드 보안 런타임 - 검증 완료):**
  - 루프백 개발용 익명 평문 모드 및 외부 공개용 TLS 1.2+ 보안 단일 노드 실행을 지원합니다.
  - standalone 프로세스는 프로세스 메모리 내에서 QoS 0, 1, 2 프로토콜 핸드셰이크를 모두 지원합니다. 단, 디스크 영속성이나 다중 노드 합의가 아닌 인메모리(RAM) 세션 및 큐를 사용하므로 프로세스 재시작 시 대기 중인 상태가 유실됩니다. (영속적 분산 복제 내구성은 Phase 2 대상)
  - 관리 HTTP API(`/api/v1`) 및 헬스 프로브(`/health/live`, `/health/ready`), Prometheus 메트릭(`/metrics`)을 지원합니다.
- **Phase 2 (Raft 분산 클러스터 런타임 - 진행 중 / Preview):**
  - Raft 합의 알고리즘 기반의 피어 전송, 리더 선출, 동적 멤버십 및 백업 복원 기능이 개발 단계(Preview)로 제공됩니다.
  - 프로덕션 무중단 롤링 배포 및 분산 영속 QoS 1/2 내구성 기능은 현재 시험 검증 단계입니다.
