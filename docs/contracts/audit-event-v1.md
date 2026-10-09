# Audit event v1 Contract

이 문서는 NaruMQ broker와 rollout controller가 보안 감사 저장소로 보내는 관리·보안 event 형식을 정의한다. 정확한 payload는 [JSON Schema](audit-event-v1.schema.json)가 기준이다.

| 항목 | 값 |
| --- | --- |
| 이름과 version | `narumq-audit-event` 1.0.0 |
| 상태 | Active (broker management-plane producer) |
| Producer | NaruMQ broker, NaruMQ rollout controller |
| Consumer | Audit store adapter, Security Admin, SIEM integration |
| 방향 | append-only event publish |
| 기준 artifact | [audit-event-v1.schema.json](audit-event-v1.schema.json) |
| 기준 spec | [Tenant와 security](../specs/tenancy-and-security.md), [Management API와 observability](../specs/management-api-and-observability.md) |
| 관련 Architecture | [Security boundaries](../architectures/security-boundaries.md), [System overview](../architectures/system-overview.md) |
| 관련 requirement | SEC-11, NFR-16~NFR-17, API-08 |
| 검증 | `tests/contracts/audit-event-v1` |

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

Audit sink 장애 정책은 [ADR-0039](../decisions/ADR-0039-management-audit-log.md)에 따른다. Mutation 실행 전에 publisher를 사용할 수 없으면 mutation을 거절하며, 실행 뒤 publish 실패는 결과를 되돌리지 않고 Metric·log로 드러낸다.

## 4. 호환성

- Optional top-level field 추가는 consumer가 unknown field를 무시할 때 호환된다.
- Required field, enum과 기존 의미 변경은 새 major가 필요하다.
- `action`과 `reason`은 open string code다. Consumer는 모르는 값을 저장하고 표시해야 한다.
- Schema validation 실패 event는 정상 stream에 쓰지 않고 별도 오류 경로와 Metric으로 보낸다.

## 5. Broker producer 범위

이 계약 revision의 Broker producer는 Dynamic cluster management API의 활성 mutation 8개와 Management API의 인증 `401`, 인가 `403`, rate-limit `429` 거절을 기록한다. Standalone mode와 MQTT data plane은 producer 범위가 아니다.

| Method·path | `action` | `target.type` / `target.id` |
| --- | --- | --- |
| `POST /api/v1/cluster/members` | `cluster.member.stage` | `cluster` / cluster ID; 요청 해석 뒤 `cluster_member` / member ID |
| `DELETE /api/v1/cluster/members/{memberId}` | `cluster.member.remove` | `cluster_member` / `memberId` |
| `POST /api/v1/cluster/features/{feature}` | `cluster.feature.activate` | `cluster_feature` / `feature` |
| `POST /api/v1/nodes/{nodeId}/drain` | `node.drain.start` | `node` / `nodeId` |
| `DELETE /api/v1/nodes/{nodeId}/drain` | `node.drain.cancel` | `node` / `nodeId` |
| `POST /api/v1/cluster/rebalance` | `cluster.rebalance.start` | `cluster` / cluster ID |
| `DELETE /api/v1/cluster/rebalance` | `cluster.rebalance.cancel` | `cluster` / cluster ID |
| `POST /api/v1/cluster/backups` | `cluster.backup.create` | `cluster` / cluster ID |

Mutation 성공은 `ALLOWED`와 `succeeded`, mutation 실패는 `FAILED`와 응답 error code를 사용한다. 인증 거절은 `DENIED`와 `unauthorized`, 인가 거절은 `DENIED`와 `forbidden`을 사용한다. Event의 `occurredAt`은 mutation 결과가 확정된 UTC 시각이다. HTTP 응답이 이미 기록된 뒤 Publisher 오류가 발생해도 응답이나 완료한 mutation을 되돌리지 않는다.

Rate limit 거절은 `DENIED`와 `rate_limited`를 사용한다. 인증 실패 IP 한도 거절의 subject는 `unauthenticated`이고 roles를 생략한다.

Actor subject는 인증을 끈 local mode에서 `local-unauthenticated`, 일치한 Bearer token에서 `management-bearer-token`, 일치한 API key에서 `management-api-key`, 인증 실패에서 `unauthenticated`다. 성공 인증에는 현재 role을 `roles`에 기록하며 401에는 roles를 생략한다. 공유 credential이므로 subject는 개인을 식별하지 않는다.

`details`는 `httpStatus`(정수)와 `method`를 포함한다. Mutation은 `Idempotency-Key` 값이 하나이고 허용 형식일 때만 `idempotencyKey`를 포함한다. Token, API key, body와 query 값은 기록하지 않는다. 인증·인가·rate-limit 거절은 같은 node별 초당 최대 20개 한도를 공유한다. 초과 event는 기록하지 않고 `denial_suppressed` Metric만 증가시킨다.

## 6. Node-local 기록 형식

각 node는 필수 설정 `management.auditDirectory`에 `audit.jsonl`을 쓴다. 한 줄은 다음 필드를 이 순서로 가진 compact JSON이다.

```json
{"seq":1,"prevHash":"<64 lowercase hex>","event":{},"hash":"<64 lowercase hex>"}
```

첫 줄의 `prevHash`는 64개의 `0`이다. 각 줄의 `hash`는 `SHA-256(decimal(seq) + "\n" + prevHash + "\n" + compact event JSON)`의 소문자 16진수 표현이다. 각 줄을 쓴 뒤 파일 `fsync`를 완료해야 publish가 성공한다.

크기 한도를 넘으면 현재 active 파일을 `audit-<20자리 zero-padded 첫 seq>.jsonl`로 rotation한다. Broker는 파일을 삭제하지 않는다. 외부 collector는 가장 오래된 rotation 파일부터 끊기지 않는 prefix만 삭제할 수 있다. 더 오래된 rotation 파일을 남긴 채 중간이나 더 최신 파일을 삭제해서는 안 된다. Active `audit.jsonl`은 수정하거나 삭제해서는 안 된다. 각 node는 서로 다른 audit directory를 사용해야 하며, audit directory는 Raft `directory`와 같거나 서로 포함되어서는 안 된다.

Process 재시작 시 active 파일의 끝이 줄바꿈으로 끝나지 않으면 마지막 완전한 줄 뒤의 bytes를 잘라낸 뒤 동기화한다. 완전한 줄은 JSON, 순번, 이전 hash와 계산한 hash를 검증한다. Rotation 파일의 torn tail은 오류다. `narumq -audit-verify <directory>`는 저장된 chain을 읽기 전용으로 검증한다. 성공하면 `files`, `records`, `first_seq`, `last_seq`, `last_hash`, `torn_tail`을 출력한다. `torn_tail`은 active 파일만 나타내며, verifier는 파일을 수정하지 않는다.

## 7. 남은 범위

- 보존 기간 값과 SIEM 접근 권한은 외부 보안 운영 책임이며 값은 미정이다.
- Rollout controller producer는 이 Broker producer 범위에 포함하지 않는다.
- SIEM consumer의 unknown action/reason 검증은 별도 범위다.
- MQTT CONNECT 인증 실패와 ACL 거절 감사는 후속 범위다.
