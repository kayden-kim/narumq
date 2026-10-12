# 메트릭 및 헬스 프로브 가이드

| 항목 | 내용 |
| :--- | :--- |
| **대상 독자** | NaruMQ 브로커의 상태를 모니터링하고 Kubernetes에 연동하는 SRE 및 인프라 엔지니어 |
| **문서 목적** | Kubernetes 헬스 프로브(`/health/*`), Prometheus 메트릭 수집(`/metrics`), 핵심 모니터링 지표를 이해한다. |
| **관련 스펙** | [Management API와 observability 스펙 (GitHub)](https://github.com/kayden-kim/narumq/blob/main/docs/specs/management-api-and-observability.md) |

---

## 1. Kubernetes 헬스 프로브 연동

NaruMQ는 컨테이너 생애주기 관리를 위해 세분화된 HTTP 프로브 엔드포인트를 제공합니다.

| 엔드포인트 | 용도 | 설명 및 동작 |
| :--- | :--- | :--- |
| **`GET /health/live`** | Liveness Probe | 프로세스가 정상 가동 중인지 확인합니다. 실패 시 Kubernetes가 컨테이너를 재시작합니다. |
| **`GET /health/ready`** | Readiness Probe | 브로커가 클라이언트 연결을 수락할 준비가 되었는지 확인합니다. 미준비 시 로드밸런서 트래픽에서 제외됩니다. |
| **`GET /health/startup`**| Startup Probe | 프로세스 초기화(설정 검증, 스토리지 준비) 완료 여부를 확인합니다. |

### Kubernetes Pod 매니페스트 설정 예시
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: narumq-broker
spec:
  containers:
    - name: narumq
      image: narumq:latest
      ports:
        - name: mqtt-tls
          containerPort: 8883
        - name: http-mgmt
          containerPort: 8080
      livenessProbe:
        httpGet:
          path: /health/live
          port: http-mgmt
        initialDelaySeconds: 5
        periodSeconds: 10
      readinessProbe:
        httpGet:
          path: /health/ready
          port: http-mgmt
        initialDelaySeconds: 2
        periodSeconds: 5
```

---

## 2. Prometheus 메트릭 수집 (`/metrics`)

NaruMQ는 OpenMetrics / Prometheus 호환 표준 메트릭을 개방합니다.

- **엔드포인트:** `http://<broker-host>:8080/metrics`
- **수집 방식:** Prometheus 서버가 주기적으로 HTTP GET 요청을 보내 풀(Pull/Scrape) 방식으로 수집합니다.

### Prometheus 설정 (`prometheus.yml`)
```yaml
scrape_configs:
  - job_name: 'narumq'
    scrape_interval: 15s
    static_configs:
      - targets: ['narumq-broker:8080']
```

---

## 3. 핵심 모니터링 지표

장애 감지 및 성능 모니터링에 필수적인 주요 메트릭입니다.

| 메트릭 이름 | 유형 | 설명 및 모니터링 포인트 |
| :--- | :--- | :--- |
| `narumq_connections_active` | Gauge | 현재 유지 중인 활성 MQTT 연결 수입니다. 급격한 감소는 네트워크 단절을 의심해야 합니다. |
| `narumq_messages_published_total` | Counter | 브로커가 수신한 총 발행 메시지 수입니다. |
| `narumq_messages_delivered_total` | Counter | 브로커가 구독자에게 전달한 총 메시지 수입니다. |
| `narumq_auth_failures_total` | Counter | 인증 실패 횟수입니다. 급증 시 무차별 대입 공격이나 설정 오류를 나타냅니다. |
| `narumq_acl_denials_total` | Counter | ACL 인가 거부 횟수입니다. 비인가 클라이언트의 비정상 발행/구독 시도를 감지합니다. |
| `narumq_http_request_duration_seconds` | Histogram | 관리 API 요청 처리 지연 시간(Latency) 분포입니다. |

---

## 4. 구조화 로그 (Structured Logging)

NaruMQ는 모든 운영 및 보안 이벤트를 표준 출력(stdout)으로 JSON 형식의 구조화 로그를 기록합니다.

```json
{"level":"INFO","ts":"2026-10-12T10:00:00.123Z","msg":"MQTT TCP listener started","address":"127.0.0.1:1883","development_only":true,"qos0_only":true,"retained_supported":true}
{"level":"WARN","ts":"2026-10-12T10:05:12.456Z","msg":"authentication failed","reason":"invalid_password","remote_addr":"192.168.1.100:54321"}
```

Fluent Bit, Vector, Logstash 등의 로그 수집기를 통해 Elasticsearch나 Loki로 손쉽게 수집 및 분석할 수 있습니다.

---

## 5. 다음 단계

- 다중 노드 클러스터 구성 및 시험에 대한 내용은 [클러스터 런타임 프리뷰](../cluster-preview/cluster-preview.md)를 확인하십시오.
- 발생 가능한 문제와 해결 방법은 [문제 해결 가이드](../troubleshooting/troubleshooting.md)를 참조하십시오.
