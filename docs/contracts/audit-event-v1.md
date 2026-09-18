# Audit event v1 Contract

이 문서는 NaruMQ broker와 rollout controller가 보안 감사 저장소로 보내는 관리·보안 event 형식을 정의한다. 정확한 payload는 [JSON Schema](audit-event-v1.schema.json)가 기준이다.

| 항목 | 값 |
| --- | --- |
| 이름과 version | `narumq-audit-event` 1.0.0 |
| 상태 | Draft |
| Producer | NaruMQ broker, NaruMQ rollout controller |
| Consumer | Audit store adapter, Security Admin, SIEM integration |
| 방향 | append-only event publish |
| 기준 artifact | [audit-event-v1.schema.json](audit-event-v1.schema.json) |
| 기준 spec | [Tenant와 security](../specs/tenancy-and-security.md), [Management API와 observability](../specs/management-api-and-observability.md) |
| 관련 Architecture | [Security boundaries](../architectures/security-boundaries.md), [System overview](../architectures/system-overview.md) |
| 관련 requirement | SEC-11, NFR-16~NFR-17, API-08 |
| 검증 | `tests/contracts/audit-event-v1` 예정. 현재 미구현 |

## 1. Event 의미

Event 하나는 actor가 target에 action을 시도한 결과 하나를 나타낸다. Mutation 성공뿐 아니라 권한 거절과 policy 변경 실패도 기록한다.

- `eventId`는 producer가 생성하는 전역 고유 불투명 ID다.
- `occurredAt`은 결과가 확정된 UTC 시각이다.
- `requestId`는 Management API response와 구조화 로그를 연결한다.
- `result`는 `ALLOWED`, `DENIED`, `FAILED` 중 하나다.
- `reason`은 안정된 machine-readable code다.
- `details`는 schema가 허용하는 JSON object지만 secret을 포함해서는 안 된다.

## 2. 개인정보와 비밀정보

- Password, bearer token, session cookie, private key와 원문 secret을 넣어서는 안 된다.
- IP, username, certificate identity와 clientId는 정책에 따라 pseudonym 또는 masked value로 기록할 수 있다.
- `actor.subject`와 target ID는 감사 권한이 있는 consumer만 복원 가능한 scope로 제한한다.
- Payload body, retained payload와 MQTT message payload를 기록해서는 안 된다.

## 3. 전달과 중복

Producer는 event를 append-only sink에 최소 한 번 전달할 수 있다. Consumer는 `eventId`로 중복을 제거해야 한다. Event 순서는 전역 보장하지 않는다. 같은 `requestId` 안에서는 `occurredAt`과 `eventId`로 정렬할 수 있다.

Audit sink 장애가 management mutation 자체를 실패시킬지 여부는 security operation policy에서 결정한다. 어떤 정책이든 event loss와 buffer overflow를 Metric·alert로 드러내야 한다.

## 4. 호환성

- Optional top-level field 추가는 consumer가 unknown field를 무시할 때 호환된다.
- Required field, enum과 기존 의미 변경은 새 major가 필요하다.
- `action`과 `reason`은 open string code다. Consumer는 모르는 값을 저장하고 표시해야 한다.
- Schema validation 실패 event는 정상 stream에 쓰지 않고 별도 오류 경로와 Metric으로 보낸다.

## 5. Draft blocker

- 감사 보존 기간과 위변조 방지 backend 확정
- Broker·controller producer contract test
- SIEM consumer의 unknown action/reason 검증
