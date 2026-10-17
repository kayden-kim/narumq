# ADR-0034: Node-local tenant connection and traffic limits

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (승인된 작업 계획; TEN-06·TEN-07 node-local scope) |
| 결정일 | 2026-10-05 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD TEN-06·TEN-07](../requirements/PRD.md) |
| 관련 스펙·데이터 계약 | [Tenant §5.2](../specs/tenancy-and-security.md#52-연결-상태와-traffic-한도), [MQTT §7.1](../specs/mqtt-protocol-and-session.md#71-현재-상한과-기본값), [Management §7](../specs/management-api-and-observability.md#7-metric-계약) |
| 관련 아키텍처 | [Broker core](../architectures/broker-core.md), [Project structure](../architectures/project-structure.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

TEN-06·TEN-07의 연결 상태와 PUBLISH traffic 한도를 다음 구조로 구현한다.

1. 연결·session·rate는 node-local 상태를 사용한다. Subscription은 기존 route table을 사용한다. 같은 node의 제한 대상 SUBSCRIBE·UNSUBSCRIBE는 판정부터 route 변경까지 직렬화한다. Cluster의 다른 node에서 발생한 동시 SUBSCRIBE는 committed projection 기준 근사 판정이다.
2. Standalone과 cluster는 `-security-config`의 선택 object `tenantLimits`를 함께 사용한다. 인증된 principal만 적용 대상이다.
3. Rate는 tenant별 token bucket으로 예약한다. 음수 token은 이미 예약한 대기량을 나타낸다. Bounded wait를 넘는 요청은 token을 차감하지 않고 거절한다.
4. CONNECT는 ownership 획득 전에, SUBSCRIBE는 commit 전에 판정한다. TCP PUBLISH는 Receive Maximum 뒤에 판정하며 fresh Packet ID만 과금한다. Cluster lane에 넘기기 전에 판정을 마친다.
5. 거절 counter는 저장 quota와 tenant label 집합을 공유한다. 경보 log는 tenant·dimension별로 제한한다.
6. Replicated schema, Raft command와 feature gate는 바꾸지 않는다. TEN-09의 p99 기준은 이 결정에 포함하지 않는다.

승인된 작업 계획의 설정·동작 계약을 아래에 기록한다. 현재 동작의 기준 문서는 [Tenant 스펙 §5.2](../specs/tenancy-and-security.md#52-연결-상태와-traffic-한도)다.

### 설정

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
2026-10-10 구현 메모: G5에서 `maxWait`를 `maxWaitMillis`로 바꿨다. 이전 Go duration 문자열은 거절한다. 현재 계약은 [Process configuration v1 §1.4](../contracts/process-configuration-v1.md#14-security-config-json)에 있다.

한도는 인증된 principal(`principal.Subject() != ""`)에만 적용한다. Security config가 없는 loopback 개발 모드와 anonymous session에는 한도가 없다.

### 차원과 초과 응답

| 차원 (`dimension`) | 집계 대상 | 검사 시점 | 초과 응답 MQTT 5 / 3.1.1 |
| --- | --- | --- | --- |
| `connections` | 이 node session repository에서 `Connected()==true`인 tenant lifecycle 수 | CONNECT에서 `session.Open` 성공 뒤, ownership 획득 전. 기존 lifecycle이 이미 연결 중이면 증가로 보지 않는다. | CONNACK `0x97` / CONNACK `0x03`. State를 바꾸지 않는다. |
| `sessions` | 이 node session repository의 tenant lifecycle 수(연결 중 + detached) | 같은 시점. 기존 lifecycle이 없을 때만(`!found`) 검사한다. Resume과 clean start 대체는 새 session이 아니다. | 같음 |
| `subscriptions` | Route table에서 tenant에 bind된 session의 shared·non-shared subscription 수. Cluster에서는 committed projection 전체다. | SUBSCRIBE filter마다 ACL 검사 뒤, commit 전. 같은 session에 같은 filter가 이미 있으면(replace) 검사하지 않는다. | 해당 filter의 SUBACK `0x97` / `0x80`. 다른 filter는 독립 처리한다. |
| `payload_size` | PUBLISH payload byte 길이 | TCP `readLoop`에서 decode 직후, dispatch 전. 대기 없음. | QoS 1 PUBACK `0x97`, QoS 2 PUBREC `0x97`, QoS 0 DISCONNECT `0x97` 뒤 종료 / 종료 |
| `message_rate` | Tenant의 초당 PUBLISH 수 (node별 token bucket) | 같은 위치. 최대 `maxWait`까지 대기한다. | QoS 1 PUBACK `0x97`, QoS 2 PUBREC `0x97`, QoS 0 DISCONNECT `0x96` 뒤 종료 / 종료 |
| `byte_rate` | Tenant의 초당 payload byte (node별 token bucket) | 같은 위치 | `message_rate`와 같음 |

연결, session, rate는 node별로 센다. Subscription은 각 node의 route table에 있는 tenant 전체 수로 판정한다. Standalone에서는 해당 node의 수이고, cluster에서는 committed projection 기준 cluster 전체 수다. 같은 node의 제한 대상 SUBSCRIBE·UNSUBSCRIBE는 판정부터 commit과 route 변경까지 직렬화한다. Replacement와 removal이 경쟁해도 local 한도를 넘기지 않는다. 다른 node에서 동시에 SUBSCRIBE하면 projection 반영 시점 때문에 잠시 한도를 넘을 수 있다. Replicated schema, feature gate, Raft command는 바꾸지 않는다.

### 검사 순서와 과금

- **검사 순서.**
  - CONNECT: `connections` → `sessions`.
  - PUBLISH: Receive Maximum → `payload_size` → rate. 그 뒤 기존 ACL·cluster·backpressure 검사가 이어진다. 따라서 ACL로 거절될 PUBLISH도 rate token을 쓴다.
- **과금 대상.** 새 PUBLISH만 과금한다. QoS 0은 항상 새 PUBLISH다. QoS 1/2는 `admitInboundPublish`가 처음 예약한 Packet ID일 때만 과금한다. 미완료 ID의 재전송은 과금하지 않는다. PUBREL, 서버 Will, retained 재전송, cluster forward는 과금하지 않는다.

### Token bucket과 대기

- **Token bucket.** 용량과 초당 보충량은 설정 rate와 같다. 시작할 때 bucket은 가득 차 있다. Clock이 뒤로 가면 보충하지 않는다.
  - 예약하면 token에서 message 1개와 payload byte를 뺀다. Token은 음수가 될 수 있다. 이 음수가 대기열 역할을 한다.
  - 대기 시간은 `max(부족 message/messagesPerSecond, 부족 byte/bytesPerSecond)`다.
  - 대기 시간이 `maxWait`를 넘으면 token을 빼지 않고 거절한다. 두 차원이 모두 넘으면 `message_rate`를 보고한다.
- **대기.** Rate 초과 시 해당 연결의 읽기를 최대 `maxWait`(기본 1s, 최대 10s)만큼 멈추고 token을 기다린다. 대기 중에는 연결의 다음 packet을 읽지 않으므로 TCP 수준 backpressure가 생긴다. 연결이 닫히면 대기를 즉시 끝낸다. Token이 충분하지 않으면 거절한다.

### Metric과 log

- Counter `narumq_tenant_limit_rejections_total{tenant,dimension}`을 둔다. `dimension`은 위 6개 값만 허용한다.
- `tenant` label은 기존 저장 quota metric과 같은 1,024개 tenant 집합을 공유한다. 집합을 넘으면 `_overflow`로 모은다.
- 거절마다 counter를 올린다. Warn log `tenant limit rejected a request`(`tenant`, `dimension`)는 tenant·dimension마다 10초에 한 번만 남긴다.

## 2. 배경

TEN-06은 tenant별 connection, session, subscription quota를, TEN-07은 tenant별 message rate, byte rate, payload size 제한을 요구한다. Cluster 전역 집계와 구성을 바꾸지 않고, 한 tenant의 초과가 다른 tenant의 정상 traffic을 거절하지 않는 범위를 제공한다. TEN-09(noisy-neighbor p99 한도)는 합의 수치가 없어 열린 항목으로 남긴다.

## 3. 결정 요인

1. tenant별 연결·session·subscription·traffic 한도를 정의하고 초과 결과를 MQTT 응답으로 확인할 수 있어야 한다.
2. 연결, session, rate의 집계 범위는 node별로 유지한다.
3. Cluster subscription은 committed projection으로 판정하되 schema·feature activation·Raft command 변경을 요구하지 않아야 한다.
4. 한 tenant의 초과가 다른 tenant의 정상 traffic을 거절하지 않아야 한다.
5. 설정은 standalone과 cluster가 같은 경로와 schema를 사용해야 한다.
6. Rate 제한은 짧은 burst에 즉시 연결을 끊지 않도록 bounded wait를 제공해야 한다.

## 4. 선택 근거

`-security-config`의 선택 object로 제한 값을 한 경로에서 관리하면 standalone과 cluster에서 같은 기본값과 tenant override를 사용한다. 인증된 principal에만 적용해 anonymous 개발 연결에 새 설정 의존성을 만들지 않는다. 연결과 session을 node-local하게 세면 replicated schema 변경과 cluster-wide feature activation 절차를 피한다. Subscription은 기존 route table의 committed projection을 사용해 cluster 전체에 가까운 판정을 제공한다. Token bucket은 burst를 허용하면서 `maxWait`를 넘는 부하를 제한한다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Cluster 전역 replicated session·subscription 집계 | 모든 node에서 단일 전역 상한을 적용한다. | Replicated schema와 feature activation이 필요하다. | Schema·feature activation 비용 때문에 제외했다. |
| `-cluster-config` 또는 standalone flag | 실행 모드별 설정을 명확히 분리한다. | 같은 tenant별 제한과 override를 두 경로에서 유지해야 한다. | Tenant별 override를 두 경로로 유지해야 해서 제외했다. |
| 즉시 거절 | 구현과 응답이 단순하다. | 짧은 burst에도 연결 종료가 늘어난다. | 짧은 burst에도 종료가 늘어나 제외했다. |

## 6. 결과

### 긍정적 결과

- Tenant별 연결 상태와 PUBLISH traffic 한도를 설정해 초과 요청을 제한한다.
- MQTT 5 reason code로 지원 가능한 초과를 알리고, MQTT 3.1.1에서는 기존 프로토콜 한계에 맞게 거절한다.
- 한 tenant의 한도 초과가 다른 tenant의 한도를 바꾸지 않는다.

### 부정적 결과와 비용

- Cluster의 실제 연결·session 상한은 "한도 × node 수"다.
- Subscription 판정은 다른 node의 동시 SUBSCRIBE에 대해 근사다. 같은 node의 제한 대상 add·replace·remove는 직렬화한다.
- 원격 takeover 뒤 이전 node에 남은 detached lifecycle은 그 node의 session 수에 계속 포함된다. 만료되거나 삭제될 때까지 포함되며, Session Expiry가 무한인 session은 계속 남는다.
- Rate 대기 중에는 같은 연결의 PUBREL·PINGREQ 처리도 최대 `maxWait`만큼 늦어진다.
- Subscription 한도가 설정된 요청은 use case mutex를 공유한다. 한 local commit이 늦으면 다른 제한 대상 tenant의 subscription 요청도 기다린다. 한도가 없는 요청은 이 mutex를 사용하지 않는다.
- TEN-09 수치는 이 결정 범위 밖이다.

## 7. 검증

| 결정·수용 기준 | 검증 위치 | 실행 결과 |
| --- | --- | --- |
| TEN-06: 연결·session·subscription 제한과 다른 tenant 격리 | `internal/application/connect`, `internal/application/subscribe`의 `TestSubscribeTenantQuotaSerializesLocalAdmission`, `TestSubscribeTenantQuotaSerializesReplacementAndRemoval`, `internal/adapters/memory/sessionstore`, `internal/domain/routing`, `TestMQTTTenantLimits`, `TestDynamicClusterTenantLimitsProcessAcceptance` | 2026-10-05 PASS. 직렬화 보완 뒤 경쟁 회귀 2개 2.509s, subscribe race 3.612s, MQTT 5 수용 시험 1.63s, 실제 binary 3-node 수용 시험 6.87s. |
| TEN-07: payload·message rate·byte rate 제한, bounded wait와 MQTT 응답 | `internal/application/tenantlimit`, `internal/adapters/tcp/server_tenant_limits_test.go`, `internal/adapters/metrics`, `cmd/narumq/security_config_test.go`, `cmd/narumq/tenant_limits_test.go`, `TestMQTTTenantLimits` | 2026-10-05 PASS. 변경 package 8개 회귀 19.01s, 보완 뒤 MQTT 5 수용 시험 1.63s, 최종 전체 local gate 171.69s. |

실행 환경은 Windows x64와 Go 1.26.5다. 수용 시험은 `go build ./... && go vet ./... && GOMAXPROCS=4 go test ./tests/integration -run '^(TestMQTTTenantLimits|TestDynamicClusterTenantLimitsProcessAcceptance)$' -count=1 -v -timeout 20m`으로 실행했다. 명령 묶음은 19.90s에 통과했다. 변경 package와 기존 보안·listener·저장 quota의 회귀 명령, 결과와 초기 실패 이력은 [Tenant 검증 기록 §1](../verification/tenancy-and-security.md#1-ten-06ten-07-실행-기록)에 기록했다.

Timer 시험은 `go test ./internal/application/connect -run '^TestConnectTenantQuotaPreservesDetachedTimers$' -count=100`으로 100회 확인했다(PASS, 0.520s). 이후 local subscription 경쟁 두 건을 재현했고, 보완 뒤 build·회귀·race·수용 명령 묶음이 43.00s에 PASS했다. 최종 `GOMAXPROCS=4 go run ./tools/verify local`은 171.69s에 PASS했다. 전체 line/function coverage는 85.97%/97.40%이며 기존 threshold와 core 기준을 통과했다. Branch coverage는 도구 미설정으로 측정하지 않았다. 초기 local gate 실패와 경쟁 재현 실패는 위 실행 기록에서 보존한다.

## 8. 재검토 조건

- Cluster 전역 connection·session quota가 필요해지면 replicated state와 feature activation의 비용을 포함해 집계 방식을 다시 결정한다.
- Subscription 경합으로 허용 가능한 한도를 반복해서 넘으면 강한 일관성 비용과 비교해 판정 방식을 다시 검토한다.
- 운영 요구가 TEN-09의 p99 한도를 확정하면 별도 결정으로 범위와 지표를 정한다.
