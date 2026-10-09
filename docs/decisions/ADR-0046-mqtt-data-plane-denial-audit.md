# ADR-0046: MQTT data-plane 거절 감사

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-09 |
| 결정자 | 사용자 |
| 관련 요구사항 | [SEC-11, NFR-17](../requirements/PRD.md#94-인증과-보안) |
| 관련 스펙·계약 | [Tenant와 security](../specs/tenancy-and-security.md), [Audit event v1](../contracts/audit-event-v1.md) |
| 관련 아키텍처 문서 | [Threat model](../architectures/threat-model.md), [Security boundaries](../architectures/security-boundaries.md), [Project structure](../architectures/project-structure.md) |
| 이 ADR이 대체하는 이전 ADR | [ADR-0039](ADR-0039-management-audit-log.md) §1 4항의 MQTT data-plane 제외 범위만 대체한다. 나머지 결정은 유지한다. |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

1. Dynamic cluster node는 기존 node-local `management.auditDirectory`의 `audit.jsonl` hash chain에 MQTT data-plane 거절을 기록한다. Management event와 같은 `AuditPublisher`를 사용한다. Event는 Raft에 저장하지 않는다. Standalone mode는 감사 publisher가 없으므로 동작을 바꾸지 않는다.
2. 감사 실패는 MQTT 응답을 바꾸지 않는다. Event 기록은 별도 node-local 한도인 초당 20개와 capacity 256의 bounded 비동기 queue를 사용한다. Queue 포화, 한도 초과, 종료 뒤 도착한 event는 버리고 Metric으로 센다. MQTT 요청 경로는 publisher와 queue를 기다리지 않는다.
3. CONNECT 인증 실패, Client ID binding 실패, CONNECT Will ACL 거절, PUBLISH·SUBSCRIBE·UNSUBSCRIBE ACL 거절, 지연 Will 발행 시점의 ACL 거절을 기록한다. 빈 Client ID, 지원하지 않는 protocol, tenant 한도·저장 quota, cluster readiness, listener admission, protocol error와 principal snapshot이 없는 Will은 기록하지 않는다.
4. Tenant·subject·credential ID·roles·Client ID·source IP host·topic 또는 filter는 원문으로 기록한다. Payload, password, token과 인증서 원문은 기록하지 않는다. 필드 길이는 계약의 rune 한도를 적용하고, 하나라도 자르면 `details.truncated=true`를 기록한다.
5. Data-plane event는 schema v1의 `DENIED` event다. `requestId`는 `eventId`와 같고 `policyVersion`은 비운다. Dynamic cluster의 기존 producer identity를 사용한다. Audit event v1 JSON Schema와 contract version은 변경하지 않는다.

## 2. 배경

ADR-0039는 management mutation의 audit admission을 fail-closed로 정하고 MQTT data-plane 거절을 제외했다. SEC-11의 남은 범위는 CONNECT와 ACL 거절의 추적이다. 이 경로는 MQTT 결과를 감사 storage 상태에 결합하지 않으면서 기존 node-local chain을 재사용해야 한다.

## 3. 결정 요인

1. Audit storage 장애가 CONNECT·ACL 응답을 바꾸지 않아야 한다.
2. MQTT 거절 폭주가 management 감사 기록을 밀어내지 않아야 한다.
3. Caller를 block하지 않고 memory 사용량을 제한해야 한다.
4. 원문 식별자를 기록하되 payload와 credential secret을 제외해야 한다.
5. 기존 event schema, publisher와 hash chain을 재사용해야 한다.

## 4. 선택 근거

전용 limiter와 bounded queue는 MQTT 거절량을 제한하고 management denial limit와 분리한다. Worker는 요청 경로 밖에서 기존 publisher를 호출하므로 MQTT 응답은 disk 지연과 무관하다. Publisher가 같은 node-local file을 쓰므로 기존 hash-chain verifier가 data-plane event도 검증한다. 원문 식별자는 조사에 필요한 값을 보존하며, 접근 통제는 외부 SIEM의 책임으로 남긴다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| CONNECT 거절을 fail-closed 처리 | 거절 누락을 막는다. | Disk 장애가 MQTT 장애로 번진다. | MQTT 응답은 감사 기록 실패와 독립이어야 한다. |
| Management 거절 한도 공유 | Limiter가 하나로 단순하다. | MQTT 폭주가 management 감사를 밀어낸다. | 분리된 초당 20개 한도를 사용한다. |
| HMAC pseudonym | 감사 파일에 원문 식별자를 남기지 않는다. | Secret 설정과 key 회전 정책이 필요하다. | 이번 범위에서 추가 secret lifecycle을 도입하지 않는다. |
| 동기 fsync 기록 | 응답 전에 durable event를 확인할 수 있다. | MQTT 경로가 storage 지연을 직접 기다린다. | Fail-open·non-blocking 정책과 맞지 않는다. |

## 6. 결과

### 긍정적 결과

- MQTT data-plane 거절을 management event와 같은 node-local hash chain에서 검증할 수 있다.
- Queue와 별도 한도로 memory와 event rate를 제한한다.
- 기존 schema v1과 Management audit 동작을 유지한다.

### 부정적 결과와 비용

- 초당 20개를 넘는 거절은 개수만 Metric으로 남고 개별 event는 기록되지 않는다.
- 원문 식별자 보호는 SIEM 접근 통제에 의존한다.
- Node 간 전역 event 순서는 보장하지 않는다.

### 후속 작업

- Rollout controller producer, SIEM consumer의 unknown action/reason 검증, 보존 기간과 SIEM 접근 권한은 별도 범위다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Data-plane event가 schema·hash chain에 맞고 secret을 제외한다. | [Audit event v1 contract test](../specs/tenancy-and-security.md#9-수용-시험과-범위), 실제 3-node process test | 다섯 action의 schema·chain 통과, process event field와 secret 제외 확인 | 구현 검증 뒤 [Tenant와 security §9](../specs/tenancy-and-security.md#9-수용-시험과-범위)에 기록 |
| Rate·queue 한도와 shutdown 경계가 bounded·fail-open이다. | `internal/application/audit/dataplane_test.go` | 한도 초과·queue 포화·종료 뒤 event가 거절 응답을 바꾸지 않고 Metric을 기록 | 구현 검증 뒤 기록 |

## 8. 재검토 조건

- MQTT 거절량이나 queue capacity가 운영 요구를 충족하지 못하는 경우
- 운영 정책이 감사 저장 실패를 MQTT 거절 결과에 결합하는 경우
- SIEM이 원문 식별자 대신 pseudonym을 요구하는 경우
