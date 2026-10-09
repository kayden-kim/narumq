# ADR-0049: MQTT credential 회전과 폐기

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (SEC-07 적용 범위) |
| 결정일 | 2026-10-10 |
| 결정자 | 승인된 SEC-07 작업 계획 |
| 관련 요구사항 | [SEC-07](../requirements/PRD.md#94-인증과-보안) |
| 관련 스펙·계약 | [Tenant와 security §2.3·§2.4·§7·§9](../specs/tenancy-and-security.md), [Management API v1](../contracts/management-api-v1.md), [Audit event v1](../contracts/audit-event-v1.md), [Replicated-state v1](../contracts/replicated-state-v1.md), [Cluster backup v3](../contracts/cluster-backup-v3.md) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md), [Broker core](../architectures/broker-core.md), [Management HTTP module](../architectures/management-http-module.md) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

SEC-07은 security configuration의 rolling restart 회전, Dynamic cluster의 복제된 credential 폐기, 기존 MQTT 연결 종료와 credential 만료 처리를 지원한다. 운영 중 security configuration reload와 MQTT 5 `AUTH` 재인증은 제공하지 않는다.

회전은 새 credential을 기존 credential과 함께 security configuration에 추가한 뒤 기존 rolling restart로 반영한다. Client가 새 credential로 전환한 뒤 기존 `credentialId`를 cluster API로 폐기한다. JWT는 짧은 `exp`와 JWKS key 회전을 함께 사용한다. `(issuer, subject)` binding은 하나만 허용하므로 해당 binding의 JWT credential을 폐기하면 그 binding을 사용하는 token이 모두 거절된다. Password identity는 같은 `subject`를 공유할 수 있지만 `username`과 `credentialId`는 유일해야 한다.

폐기는 `credentialId` 전체에 적용한다. 폐기된 ID는 `DELETE`로 복구할 때까지 모든 CONNECT를 거절한다. 회전에는 새 `credentialId`를 사용한다. 복제 session metadata 형식은 바꾸지 않는다.

Dynamic cluster에서 폐기 command를 commit해 apply한 각 node는 해당 credential의 연결을 즉시 종료하고 연결의 Will을 폐기한다. JWT `exp` 또는 인증서 `NotAfter`에 도달한 연결도 종료한다. 만료에 따른 종료는 기존 비정상 종료 Will 규칙을 따른다. Client는 새 credential로 재접속한다.

| 설계 항목 | 고정값 |
| --- | --- |
| Feature token | `credential-revocation`; 기본 비활성화, 기존 `FeatureActivation`으로 활성화 |
| 최대 폐기 entry | 4096 (`MaxCredentialRevocations`) |
| `credentialId` 길이 | UTF-8 1~512 bytes (`MaxCredentialIDBytes = 512`); audit target의 512-rune 한도와 일치 |
| 폐기 연결 종료 | MQTT 5는 이미 CONNACK을 보낸 연결에 DISCONNECT `0x87` 뒤 2초 후 강제 close. MQTT 3.1.1 또는 CONNACK 전 연결은 즉시 close. Will은 폐기 |
| 만료 연결 종료 | MQTT 5는 DISCONNECT `0xA0` 뒤 2초 후 close. MQTT 3.1.1은 즉시 close. Will은 `DisconnectAbnormal` 규칙 |
| 만료 시각 | `Principal.ExpiresAt()` 그대로 사용. JWT `clockSkew`는 CONNECT 검증에만 적용. Password(`time.UnixMilli(math.MaxInt64)`)와 익명(`Subject()==""`)은 timer 없음 |
| CONNECT 거절 사유 | 인증 Metric과 data-plane audit에 `credential_revoked`; CONNACK은 기존 `notAuthorizedReason`(3.1.1 `0x05`, 5 `0x87`) |
| Management route | `GET /api/v1/security/credential-revocations`, `POST`·`DELETE /api/v1/security/credential-revocations/{credentialId}` |
| 권한 | `PermissionCredentialAdmin`: `SecurityAdmin` 또는 `ClusterAdmin`. 세 route 모두 이 권한. 405 분기는 기존 `PermissionRead` |
| Audit action | `security.credential.revoke`, `security.credential.restore`; target type `mqtt_credential`, target parameter `credentialId` |
| 범위 | 폐기 API는 Dynamic cluster 전용. Standalone에는 폐기 route, management mutation 또는 관련 audit event가 없다. 만료 종료는 Standalone에도 적용 |

## 2. 배경

SEC-07은 기존 rolling restart, replicated `credentialId` 폐기와 JWT `exp`·certificate `NotAfter` 연결 종료를 제공한다. MQTT 5 `AUTH` 재인증과 hot reload는 제공하지 않는다. 저장소의 process acceptance는 실제 3-node runtime을 검증하며 production IdP, certificate lifecycle과 deployment rollout은 별도 검증 대상이다. Threat model R-05는 이 배포 검증이 끝날 때까지 잔여 위험으로 둔다.

## 3. 결정 요인

1. 폐기는 모든 node에서 같은 commit 결과를 기준으로 적용해야 한다.
2. 새 credential을 먼저 배포하고 이전 credential은 사용 전환 뒤 폐기할 수 있어야 한다.
3. 기존 principal과 replicated session metadata 형식을 유지해야 한다.
4. 폐기와 복구는 권한 검사와 감사를 거쳐야 한다.
5. standalone의 기존 관리 범위와 JWT·password·mTLS 인증 경계를 유지해야 한다.

## 4. 선택 근거

기존 rolling restart는 security configuration을 모든 node에 반영하는 운영 경로다. 신·구 credential을 함께 배포하면 client를 새 credential로 전환할 수 있고, 이후 replicated `credentialId` 폐기로 구 credential의 새 인증과 기존 연결을 회수할 수 있다. Hot reload controller를 추가하지 않아 설정 변경과 replicated 폐기 상태의 두 가지 갱신 경로를 만들지 않는다.

복제된 폐기 entry는 node가 commit된 상태를 apply한 시점에 CONNECT와 연결 종료에 사용한다. Cluster의 새 CONNECT는 quorum commit된 ownership 경계를 거친 뒤 TCP adapter에서 폐기 상태를 다시 확인한다. Quorum에 도달하지 못한 node는 새 연결을 수락하지 않는다. `credentialId`를 폐기 단위로 고정하면 token 발급 시각 해석이나 credential 형식별 폐기 규칙이 필요하지 않다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Security configuration hot reload | 재시작 없이 credential 변경을 적용한다 | file watcher, 동시 갱신과 node별 설정 차이 처리 경로가 새로 생긴다 | 승인된 결정은 기존 rolling restart를 사용하며 운영 중 reload를 제공하지 않는다 |
| 발급 시각(`iat`/`NotBefore`) 기준 폐기 | ID별 폐기 목록을 저장하지 않을 수 있다 | 각 인증 형식의 시각 semantics가 달라지고 하나의 binding에서 token 일부만 폐기하는 규칙이 생긴다 | 폐기 단위를 `credentialId` 전체로 고정하고 credential을 새 ID로 회전한다 |
| Node-local 폐기 목록 파일 | 구현이 단순하고 cluster schema 변경이 없다 | node별 파일 동기화와 장애 복구가 필요하며 폐기 상태가 cluster 전체에 수렴하지 않는다 | 폐기를 committed replicated state로 적용한다 |

## 6. 결과

### 긍정적 결과

- 폐기 상태는 committed cluster state이며 snapshot과 backup restore에 보존된다.
- 폐기 적용 node는 기존 연결을 종료하고 Will을 억제한다.
- credential 만료 시점은 principal의 기존 `ExpiresAt()`에서 정한다.
- 폐기와 복구는 별도 permission과 audit action을 사용한다.

### 부정적 결과와 비용

- Feature를 활성화하면 strict replicated-state schema를 모르는 이전 build로 rollback할 수 없다.
- Credential rotation은 rolling restart와 client 전환을 필요로 한다.
- Revocation entry는 복구할 때까지 최대 4096개를 차지한다.
- JWT binding은 `(issuer, subject)`당 하나이므로 그 binding에 연결된 모든 JWT가 함께 폐기된다.

### 후속 작업

- SEC-12의 client·username·certificate identity·IP blocklist는 별도 요구사항으로 남는다.
- Production IdP와 deployment rollout 절차는 이 ADR의 구현 acceptance 범위가 아니다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Replicated revocation은 feature gate, capacity, snapshot·backup 경계를 지킨다 | Membership·Raft records unit/contract 시험과 backup checksum 시험 | 비활성 feature 기록 거절, 한도·정렬 검증, snapshot round trip과 digest 일치 | [Tenant와 security §9.7](../specs/tenancy-and-security.md#97-sec-07-실행-증거) |
| 폐기는 CONNECT와 기존 연결에 모든 node에서 적용되고 Will을 폐기한다 | Dynamic 3-node process acceptance | 모든 node의 새 CONNECT가 거절되고 활성 연결이 정해진 DISCONNECT/close를 받으며 Will이 발행되지 않는다 | [Tenant와 security §9.7](../specs/tenancy-and-security.md#97-sec-07-실행-증거) |
| Expiry는 protocol별 종료와 기존 Will 규칙을 따른다 | 실제 MQTT 3.1.1·5 connection process test | 만료 시 MQTT 5 `0xA0`, 3.1.1 close, Will 발행 결과 확인 | [Tenant와 security §9.7](../specs/tenancy-and-security.md#97-sec-07-실행-증거) |
| Management 권한과 감사가 API 계약과 일치한다 | HTTP handler·OpenAPI·audit contract·process 시험 | 권한, response schema, revoke/restore action·target 일치 | [Tenant와 security §9.7](../specs/tenancy-and-security.md#97-sec-07-실행-증거) |

## 8. 재검토 조건

- 회전·폐기 요구가 운영 중 reload 또는 credential 일부만 폐기하는 semantics로 바뀌면 별도 ADR로 결정한다.
- `credentialId` 한도 또는 revoked entry 한도를 바꿔야 하면 replicated-state contract와 backup verifier를 함께 변경한다.
- Standalone 폐기, MQTT 5 `AUTH` 재인증 또는 SEC-12 blocklist가 필요하면 범위와 상태 형식을 별도로 결정한다.
