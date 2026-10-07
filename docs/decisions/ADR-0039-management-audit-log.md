# ADR-0039: Management plane 감사 로그

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-08 |
| 결정자 | 사용자 |
| 관련 요구사항 | [SEC-11, NFR-17](../requirements/PRD.md#94-인증과-보안), [API-08](../requirements/PRD.md#95-metric과-관리-api) |
| 관련 스펙·데이터 계약 | [Tenant와 security](../specs/tenancy-and-security.md), [Management API와 observability](../specs/management-api-and-observability.md), [Audit event v1](../contracts/audit-event-v1.md) |
| 관련 아키텍처 | [Security boundaries](../architectures/security-boundaries.md), [Project structure](../architectures/project-structure.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

1. 각 Dynamic cluster node는 `management.auditDirectory`에 node-local JSONL 감사 로그를 쓴다. 각 줄은 이전 줄의 SHA-256을 연결하며, 줄마다 `fsync`한다. 외부 collector가 rotation된 파일을 SIEM으로 전달한다. 감사 이벤트를 Raft에 저장하지 않는다.
2. Management mutation은 실행 전에 publisher의 준비 상태를 확인한다. Publisher를 쓸 수 없으면 mutation을 실행하지 않고 retryable `503 audit_unavailable`을 반환한다. Mutation 뒤 이벤트 기록이 실패해도 이미 실행한 작업은 되돌리지 않는다. Metric과 error log로 실패를 드러낸다.
3. Broker는 보존 기간, 검색과 접근 권한을 제공하지 않는다. 외부 SIEM이 이 책임을 맡는다. Broker는 파일 크기로만 rotation하며 파일을 삭제하지 않는다. 외부 collector는 가장 오래된 rotation 파일부터 끊기지 않는 prefix만 삭제할 수 있다. 조회 API를 추가하지 않는다.
4. 감사 범위는 management plane이다. MQTT CONNECT 인증 실패와 ACL 거절은 후속 범위다.
5. `management.auditDirectory`는 필수 cluster 설정이다. Raft `directory`와 같거나 서로 포함하는 경로를 거절한다. 각 node는 서로 다른 감사 directory를 사용한다.

## 2. 배경

SEC-11, NFR-17, API-08은 관리 작업의 추적과 위변조 검출을 요구한다. 기존 감사 이벤트 계약은 저장 방식, 장애 정책과 보존 책임이 정해지지 않아 Draft였다. 결정은 Management API의 결과를 node-local durable record로 남기며 Raft 상태 형식은 변경하지 않는다.

## 3. 결정 요인

1. 관리 mutation 전에 감사 기록을 남길 수 있는지 확인해 감사 공백을 막는다.
2. 감사 I/O 장애가 MQTT data plane이나 Raft state schema에 영향을 주지 않게 한다.
3. 비밀정보를 제외하고 node-local 파일에서 순서와 변경 흔적을 검증할 수 있어야 한다.
4. Broker가 보존·검색·권한 관리용 별도 서비스를 만들지 않는다.

## 4. 선택 근거

파일은 각 node의 local management mutation을 기록하며 매 append를 동기화한다. Hash chain은 연속된 줄의 삭제·변경을 검출한다. 크기 기반 rotation은 파일을 보존하고 collector가 전달한 뒤 회전 파일을 정리할 수 있게 한다. 삭제는 가장 오래된 연속 prefix로 제한해 남은 chain의 연속성을 보존한다. Publisher 준비 확인을 mutation 실행 전에 수행해 fail-closed 요구를 만족한다. 기록 실패가 mutation 이후 발생하면 이미 확정된 결과를 되돌리지 않고 관측 신호를 남긴다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 구조화 로그 stream만 사용 | 기존 로그 수집 경로를 재사용한다. | append-only·fsync·연결 hash를 보장하지 않는다. | 위변조 검출 계약을 충족하지 않는다. |
| Raft 복제 record | cluster node 사이에 이벤트를 복제한다. | Raft schema와 mutation 경계를 변경하고, 감사 경로 장애를 cluster 상태에 결합한다. | 사용자 결정에서 Raft schema 변경을 제외했다. |
| Raft data directory 안 자동 경로 | 설정 필드가 줄어든다. | 데이터와 감사 파일의 권한·수명 주기가 결합되고, 데이터 경로 손상 시 감사 경계도 함께 영향을 받는다. | 필수 별도 `auditDirectory`와 경로 겹침 거절을 선택했다. |
| Fail-open | 감사 저장소 장애 중 mutation을 계속 허용한다. | 장애 동안 감사가 빠진 변경이 발생할 수 있다. | 사용자 결정은 mutation admission에서 fail-closed다. |

## 6. 결과

### 긍정적 결과

- Management mutation과 권한 거절을 request ID, actor, action, target, result와 HTTP 결과로 연결한다.
- Local audit log의 줄별 hash와 fsync로 변경·누락을 검증할 수 있다.
- Audit I/O 문제는 cluster 시작이나 MQTT data plane을 중단하지 않는다. Management mutation은 기록을 보장할 수 없을 때 거절한다.

### 부정적 결과와 비용

- Node별 파일을 collector가 수집해야 하며 node 간 전역 순서는 보장하지 않는다.
- 파일은 Broker에서 자동 삭제되지 않으므로 외부 collector 운영과 disk 용량 관리가 필요하다.
- 줄마다 `fsync`하므로 management mutation 결과에 local storage 지연이 추가된다.
- Hash chain은 파일 소유자가 전체 로그와 hash를 다시 작성하는 공격을 막지 않는다. SIEM 보관과 접근 제한은 외부 책임이다.

### 후속 작업

- 보존 기간 값과 SIEM 접근 권한은 외부 보안 운영에서 정한다.
- Rollout controller producer, SIEM consumer의 unknown action/reason 검증, MQTT data-plane 거절 감사는 별도 범위다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Append chain은 손상과 중간 누락을 검출하며, oldest rotation prefix를 삭제한 뒤에도 남은 chain을 검증한다. | `internal/adapters/auditlog/file_publisher_test.go` (`TestVerifyDirectoryAllowsDeletedOldestRotation`), `cmd/narumq/main_test.go`, `tests/contracts/audit-event-v1/contract_test.go::TestAuditEventMatchesSchemaAndFileChain`, 실제 `narumq -audit-verify` 실행 | 정상 로그와 oldest prefix 제거 통과, 변조·중간 줄 삭제 거절, 파일명과 줄 번호 출력 | [Tenant와 security §9](../specs/tenancy-and-security.md#9-수용-시험과-범위) |
| Audit unavailable은 management mutation을 막는다. | `internal/adapters/http/management/audit_test.go`, `tests/integration/cluster_audit_process_test.go::TestDynamicClusterManagementAuditProcessAcceptance` | Mutation handler 미호출, retryable `503 audit_unavailable`; 조회와 data plane은 유지 | [Tenant와 security §9](../specs/tenancy-and-security.md#9-수용-시험과-범위) |
| Wire event가 계약 schema를 만족하고 secret을 제외한다. | `tests/contracts/audit-event-v1/contract_test.go::TestAuditEventMatchesSchemaAndFileChain` | 실제 event schema 통과, unknown field 거절, bearer token 파일에 없음 | [Audit event v1](../contracts/audit-event-v1.md) |

## 8. 재검토 조건

- SIEM이 rotation 파일 이외의 수집 방식을 요구하는 경우
- Node-local storage가 관리 mutation의 요구 지연이나 용량을 충족하지 못하는 경우
- 운영 정책이 보존 기간, 접근 권한 또는 event 조회를 Broker 책임으로 바꾸는 경우
- MQTT data-plane 거절 감사를 활성 범위에 넣는 경우
