# 관리 REST API 가이드

| 항목 | 내용 |
| :--- | :--- |
| **대상 독자** | NaruMQ 브로커의 상태를 조회하고 외부 관측/관리 도구와 연동하는 운영자 및 백엔드 개발자 |
| **문서 목적** | Management API v1의 인증 방식, 핵심 엔드포인트(`/api/v1`), 페이징 및 Rate Limit 정책을 파악한다. |
| **기준 계약** | [Management API v1 Contract (GitHub)](https://github.com/kayden-kim/narumq/blob/main/docs/contracts/management-api-v1.md) |

---

## 1. 개요 및 인증

NaruMQ는 브로커 상태 점검과 클러스터 관리를 위한 HTTP/JSON 기반 REST API v1을 제공합니다.

- **기본 포트:** `8080` (기본값)
- **API 프리픽스:** `/api/v1`
- **인증 방식:** HTTP `Authorization: Bearer <TOKEN>` 헤더 전달
  - 브로커 기동 시 `-http-auth-token-file`로 지정한 비밀 토큰을 사용합니다.
- **요청 속도 제한 (Rate Limit):**
  - 기본 한도: 초당 최대 50건 (Burst 50건)
  - 인증 실패 한도: 초당 최대 5건 (무차별 대입 공격 방어)
  - 한도 초과 시 `429 Too Many Requests`를 반환합니다.

---

## 2. 핵심 엔드포인트 목록

### 1) 클러스터 및 노드 조회

#### `GET /api/v1/cluster`
클러스터의 전반적인 상태와 리더 노드 정보를 조회합니다.

```sh
curl -s -H "Authorization: Bearer secret-token" http://127.0.0.1:8080/api/v1/cluster
```

**응답 예시:**
```json
{
  "clusterId": "narumq-cluster-1",
  "leaderNodeId": "narumq-node-1",
  "activeNodes": 1,
  "totalSessions": 12,
  "replicationFactor": 1,
  "writeQuorum": 1
}
```

#### `GET /api/v1/nodes`
클러스터에 참여 중인 브로커 노드 목록을 조회합니다.

#### `GET /api/v1/nodes/{nodeId}`
특정 노드의 상세 상태(가동 시간, CPU/메모리, 연결된 클라이언트 수)를 조회합니다.
### 노드 drain

#### `POST /api/v1/nodes/{nodeId}/drain`
지정 노드의 신규 연결 수락을 제한하고 기존 연결을 drain하여 종료를 준비합니다. 요청에서 운영 필드를 생략하면 `connectionRatePerSecond=100`, `loadBalancerPropagationDelayMillis=30000`, `timeoutMillis=600000`을 적용합니다.

---

### 2) 클라이언트 및 세션 조회

#### `GET /api/v1/clients`
현재 브로커에 연결된 MQTT 클라이언트 세션 목록을 조회합니다.

- **쿼리 파라미터:**
  - `limit`: 반환할 최대 항목 수 (기본 50, 최대 100)
  - `cursor`: 페이징 커서
  - `tenantId`: 특정 테넌트로 필터링

```sh
curl -s -H "Authorization: Bearer secret-token" \
  "http://127.0.0.1:8080/api/v1/clients?limit=10"
```

**응답 예시:**
```json
{
  "data": [
    {
      "tenantId": "tenant-iot",
      "clientId": "device-sensor-01",
      "connected": true,
      "protocolVersion": "5.0",
      "connectedAt": "2026-10-12T10:00:00Z",
      "remoteAddr": "192.168.1.50:42124"
    }
  ],
  "nextCursor": ""
}
```

---

### 3) 구독 및 Retained 메시지 조회

#### `GET /api/v1/subscriptions`
현재 브로커에 등록된 모든 토픽 구독(Subscription) 목록을 조회합니다.

#### `GET /api/v1/retained`
브로커 메모리에 보관 중인 Retained 메시지의 메타데이터(토픽, 페이로드 크기, 발행 시각)를 조회합니다. 보안을 위해 페이로드 원문은 노출되지 않습니다.

---

## 3. 공통 오류 응답 형식

API 오류 발생 시 RFC 7807 스타일의 표준 JSON 형식으로 반환됩니다.

| HTTP 상태 코드 | 오류 코드 (`code`) | 원인 및 조치 사항 |
| :--- | :--- | :--- |
| **401 Unauthorized** | `unauthorized` | 인증 토큰 누락 또는 유효하지 않은 토큰 |
| **403 Forbidden** | `insufficient_role` | 요청한 작업에 필요한 RBAC 권한 부족 |
| **404 Not Found** | `resource_not_found`| 요청한 노드 ID 또는 클라이언트 세션이 존재하지 않음 |
| **429 Too Many Requests** | `rate_limit_exceeded`| API 요청 속도 한도 초과 (잠시 후 재시도 필요) |

---

## 4. 다음 단계

- 메트릭 수집 및 Kubernetes 헬스 프로브 설정은 [메트릭 및 헬스 프로브 가이드](./observability.md)를 확인하십시오.
