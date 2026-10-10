# ADR-0050: MQTT client·identity·IP blocklist

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (SEC-12 적용 범위) |
| 결정일 | 2026-10-10 |
| 결정자 | 승인된 SEC-12 작업 계획 |
| 관련 요구사항 | [SEC-12](../requirements/PRD.md#94-인증과-보안) |
| 관련 스펙·계약 | [Tenant와 security §7.1·§9](../specs/tenancy-and-security.md), [Management API v1](../contracts/management-api-v1.md), [Audit event v1](../contracts/audit-event-v1.md), [Replicated-state v1](../contracts/replicated-state-v1.md), [Cluster backup v3](../contracts/cluster-backup-v3.md), [Release compatibility v1](../contracts/cluster-release-compatibility-v1.md) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md), [Broker core](../architectures/broker-core.md), [Management HTTP module](../architectures/management-http-module.md) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

SEC-12는 Dynamic cluster 전용의 replicated MQTT CONNECT blocklist를 제공한다. 관리자는 client ID, CONNECT Username 원문, 검증된 leaf certificate fingerprint 또는 source IP CIDR로 새 CONNECT를 차단하고, block 적용 시 일치하는 기존 연결을 종료한다. Standalone에는 blocklist route·matcher·audit event를 제공하지 않는다.

| 설계 항목 | 고정값 |
| --- | --- |
| Feature token | `client-blocklist`; 기본 build feature로 광고하고 기존 feature API로 활성화한다. 활성화 전에는 관련 command·snapshot field를 기록하지 않는다. |
| Wire kind | `clientId`=1, `username`=2, `certificateSha256`=3, `ipPrefix`=4 |
| Wire action | `block`=1, `unblock`=2 |
| Replicated fields | Command field 26; snapshot collection field 17; `blocklist_version` field 18 |
| Client ID key | Tenant-scoped `(tenantId, clientId)`; anonymous cluster tenant는 `default`다. |
| Client ID 경계 | Tenant 1..128 bytes이며 `tenancy.NewTenantID`를 통과; client ID는 UTF-8 1..256 bytes |
| Username key | MQTT CONNECT Username 원문, 정확히 일치하고 대소문자를 구분하며 `HasUsername`일 때만 비교; UTF-8 1..256 bytes, U+0000 금지 |
| Certificate key | 검증된 leaf certificate DER의 SHA-256, 정규식 `^[0-9a-f]{64}$`에 맞는 소문자 hex |
| IP key | `netip.ParsePrefix`가 성공하고 이미 masked이며 IPv4-mapped IPv6가 아니고 `Prefix.String()`이 입력과 같은 canonical IPv4/IPv6 CIDR; 단일 IP는 `/32` 또는 `/128` |
| Kind별 tenant | `tenantId`는 `clientId` entry에서만 허용한다. |
| Capacity | 전체 kind 합계 최대 4096 entry (`MaxBlocklistEntries`) |
| Policy version | Entry 집합을 실제로 바꾼 마지막 blocklist command의 Raft index; 초기값 0. Idempotent block 및 없는 entry의 unblock은 version을 바꾸지 않는다. Snapshot에 보존한다. |
| Canonical snapshot order | `(kind, tenantId, value)` 오름차순 |
| 전파 목표 | 성공한 관리 API 응답 뒤 5초 안에 모든 node가 새 CONNECT를 거절하고 matching 기존 연결을 종료한다. |
| CONNECT 거절 | MQTT 5 CONNACK `0x8A` (Banned); MQTT 3.1.1 CONNACK `0x05`; data-plane reason `blocked` |
| 기존 연결 종료 | Established MQTT 5는 DISCONNECT `0x98`을 보낸 뒤 2초 후 강제 close; MQTT 3.1.1 및 CONNACK 전 연결은 즉시 close. Will은 폐기한다. |
| Audit | Mutation action `security.blocklist.add`·`security.blocklist.remove`; target type `mqtt_blocklist_entry`; target ID는 `BlockEntry.Key()` 형식 `clientId:<tenant>/<client>`, `username:<v>`, `certificateSha256:<v>`, `ipPrefix:<v>`다. |
| Scope exclusions | Standalone route·matcher·audit, 이미 끊긴 session의 지연 Will 억제, entry TTL은 제공하지 않는다. |

Client ID key는 tenant를 포함하므로 다른 tenant의 같은 client ID를 서로 독립적으로 차단한다. Anonymous cluster CONNECT는 tenant `default`로 판정한다. Username은 인증 유형과 무관하게 원문 CONNECT field를 사용한다. Certificate fingerprint는 검증된 leaf DER에서 계산하며 CN·SAN을 차단 key로 삼지 않는다. IP prefix는 자동 정규화하지 않고 canonical 형식만 허용한다.

### 최신성 설계

Cluster CONNECT는 항상 ownership을 Raft로 commit하며 `clusterRuntime.Commit`은 local apply 완료를 기다린다. 따라서 ownership commit이 반환될 때 그 전에 commit된 blocklist 변경은 이미 해당 node의 local TCP 색인에 반영되어 있다. TCP는 `bindSession`과 CONNACK 직전의 두 시점에 `server.mu` 아래에서 blocklist를 다시 검사한다. Apply도 같은 mutex 아래에서 활성 연결을 scan하고 일치 연결을 회수한다. 이 동일 lock 경계가 경쟁 순서를 닫으므로 CONNECT마다 별도의 barrier read를 추가하지 않는다. Application의 사전 검사는 local committed state를 사용해 비싼 인증과 ownership 부작용을 줄인다.

## 2. 배경

SEC-12는 clientId, username, certificate, IP blocklist와 차단 전파가 목표 시간 안에 모든 node에 반영되는 것을 요구한다. SEC-07은 replicated credential revocation, CONNECT rejection 및 기존 연결 종료의 cluster 경로를 이미 제공한다. 이 결정은 같은 Dynamic cluster 적용 경계를 확장하고, 선택 key의 의미와 schema compatibility를 고정한다.

## 3. 결정 요인

1. 차단 상태와 snapshot·backup은 cluster 전체에서 동일한 committed state를 따라야 한다.
2. Selector는 tenant 범위, 인증 결과 또는 주소 표현에서 모호하지 않고 exact matching이어야 한다.
3. 성공 응답 이후 5초 안에 각 node의 새 CONNECT와 이미 연결된 client에 적용해야 한다.
4. 기존 CONNECT의 ownership 및 TCP session 경쟁 경로를 보호하되 불필요한 CONNECT latency를 더하지 않는다.
5. Feature activation 전 wire bytes를 유지하고 strict decode 및 rollback 경계를 보존해야 한다.
6. Mutation은 기존 permission·audit 경로를 사용하고 no-op 요청이 policy version을 허위로 전진시키지 않아야 한다.

## 4. 선택 근거

Blocklist를 replicated cluster state로 두면 leader 변경, snapshot 및 backup 뒤에도 모든 node가 동일한 policy를 사용한다. `clientId`에 tenant를 결합하면 한 tenant의 ID가 다른 tenant에 영향을 주지 않으며, anonymous cluster 연결의 기존 `default` tenant 규칙도 유지한다. Username 원문과 `HasUsername` 조건은 MQTT CONNECT 의미를 보존한다. 검증된 leaf DER fingerprint는 certificate identity를 안정적으로 지목하며, canonical CIDR은 IPv4와 IPv6 범위 모두를 정확히 표현한다.

Feature activation은 모든 non-retired member가 field를 이해한 뒤에만 새 기록을 허용한다. 그 전에는 command field 26 및 snapshot field 17·18을 쓰지 않는다. 활성화 후에는 새 형식을 모르는 build로 rollback할 수 없다. `blocklist_version`은 entry set이 실제 변경된 command의 Raft index이므로 운영자가 각 node GET으로 적용 결과의 수렴을 비교할 수 있다. Snapshot ordering과 4096-entry ceiling은 deterministic state와 bounded storage를 보장한다.

성공 API 응답은 replicated command의 commit/apply 뒤 반환된다. 그 뒤 각 node는 같은 색인을 갱신하고 일치하는 established connection을 회수한다. MQTT protocol별 rejection 및 close 동작은 client가 차단 원인을 받고 기존 session의 Will이 발행되지 않게 한다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Certificate CN/SAN selector | 사람이 인증서 이름을 읽고 입력하기 쉽다 | CN/SAN은 인증서별로 고유하지 않을 수 있고 이름·검증 규칙에 종속된다 | 사용자가 검증된 leaf DER SHA-256 fingerprint를 선택했다. |
| Exact IP만 허용하고 CIDR은 제외 | 저장·비교 형식이 단순하다 | 네트워크 범위를 표현할 수 없어 운영자가 주소마다 entry를 관리해야 한다 | 사용자는 IPv4/IPv6 CIDR을 선택했으며 단일 주소도 `/32`·`/128`로 표현한다. |
| CONNECT마다 Raft barrier read | 매 시점에 최신 committed state를 읽는 명시적 경계가 된다 | CONNECT마다 cluster read와 latency를 추가한다 | Ownership은 Raft commit 뒤 local apply가 완료되고, 동일 TCP mutex 아래의 bindSession·CONNACK 재검사와 apply scan이 경쟁을 막으므로 latency 비용을 감수할 필요가 없다. |

## 6. 결과

### 긍정적 결과

- 차단 entry와 policy version은 replicated state이며 canonical snapshot과 backup checksum에 보존된다.
- Client ID는 tenant-scoped이고 username·certificate·IP selector는 고정된 exact semantics를 갖는다.
- 실제 state 변경만 version을 올려 node 간 apply 상태를 비교할 수 있다.
- 새 CONNECT와 일치하는 기존 연결이 Dynamic cluster의 각 node에서 함께 차단된다.
- 기존 permission, management audit, strict schema 및 feature activation 규칙을 재사용한다.

### 부정적 결과와 비용

- Feature activation은 rollback window를 종료한다. 활성화 후 blocklist field를 모르는 이전 build로 rollback할 수 없다.
- Entry는 명시적으로 unblock할 때까지 유지되고 전체 4096개 제한을 공유한다. TTL은 없다.
- Standalone 운영자는 이 API나 matcher를 사용할 수 없다.
- 이미 연결이 끊긴 session에서 지연 발행될 Will은 blocklist 변경으로 억제하지 않는다.

### 운영

Feature activation을 완료한 뒤 해당 selector의 POST로 차단한다. 각 node의 GET 결과에서 `policyVersion`이 같은지 확인하고, 이후 DELETE로 해제한다. 관리 API 성공 응답 뒤 5초 이내의 전파 목표를 적용한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Entry 경계와 canonical state가 재현 가능하다 | Domain·membership·Raft codec unit 및 contract 시험 | 네 kind의 입력 검증, capacity·정렬·feature gate·snapshot version 검증 | [Tenant와 security §9](../specs/tenancy-and-security.md) |
| 모든 node에서 새 CONNECT와 기존 연결에 적용된다 | Dynamic 3-node process acceptance 및 TCP 경쟁 시험 | API 성공 뒤 5초 내 각 node의 새 연결 거절, matching 연결 종료, Will 폐기 | [Tenant와 security §9](../specs/tenancy-and-security.md) |
| 운영 결과와 version·audit 계약이 일치한다 | Management handler·OpenAPI·audit contract 시험 | 경로, selector, policyVersion, 권한 및 audit action/target 일치 | [Tenant와 security §9](../specs/tenancy-and-security.md) |
| Backup·release compatibility가 strict schema를 유지한다 | Snapshot/backup checksum 및 mixed-release acceptance | typed snapshot/digest 보존; inactive feature bytes 유지; active feature 미지원 build 거절 | [Replicated-state v1](../contracts/replicated-state-v1.md), [Cluster backup v3](../contracts/cluster-backup-v3.md), [Release compatibility v1](../contracts/cluster-release-compatibility-v1.md) |

## 8. 재검토 조건

- SEC-12 범위에 Standalone route·matcher·audit을 포함해야 하면 별도 범위와 상태 저장 정책을 결정한다.
- Certificate selector를 fingerprint 이외의 식별자로 바꾸거나 IP 범위 semantics를 바꿔야 하면 key·canonicalization 및 replicated-state contract를 함께 재검토한다.
- 5초 전파 목표나 TCP 경쟁 경계가 바뀌면 최신성 설계와 수용 기준을 다시 검토한다.
- 이미 끊긴 session의 delayed Will 억제 또는 entry TTL이 필요해지면 별도 lifecycle semantics를 결정한다.
