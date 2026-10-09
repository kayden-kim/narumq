# Raft 멤버십 v2 인터페이스 계약

이 초안은 통합 클러스터 런타임의 멤버 승인, 멤버십 작업과 관리 경계를 정의한다. 고정 투표자 방식 피어 전송 v1은 그대로 유지한다. v2가 존재한다는 사실만으로 운영 환경 지원이나 릴리스 간 호환성을 보장하지 않는다. 타입이 지정된 복제 로그와 스냅샷 레코드의 기준은 [replicated-state-v1.proto](replicated-state-v1.proto)다.

| 항목 | 값 |
| --- | --- |
| 이름과 버전 | `narumq-raft-membership` 2.0.0 |
| 상태 | Draft |
| 소유자 | NaruMQ 프로젝트 |
| 생산자 | ClusterAdmin API, 가입 중인 broker, commit된 Raft 멤버 등록부 |
| 사용자 | 클러스터 리더, 가입 중인 broker, 인증된 피어 전송 계층, 관리 클라이언트 |
| 전달 방향 | 관리 HTTP 요청·응답, TLS 1.3 피어 승인 핸드셰이크, Raft 스트림, 메시지 전달 스트림 |
| 스키마 기준 파일 | 이 문서와 [replicated-state-v1.proto](replicated-state-v1.proto) |
| 기준 동작 명세 | [클러스터 세션과 영속 전달](../specs/cluster-session-and-delivery.md) §1, §11 |
| 관련 요구사항 | CLU-01~CLU-04, CLU-06~CLU-11, SEC-09~SEC-11, NFR-07~NFR-10 |
| 호환성 범위 | 통합 클러스터 런타임 프로토콜 `2.0`, 상태 스키마 `1.0`; N/N-1 호환성을 보장하지 않음 |
| 채택 범위 | 2026-10-02에 사용자는 동적 Raft 인바운드의 제한된 수신 대기 정책을 채택했다. [ADR-0024](../decisions/ADR-0024-joiner-voter-volume-loss-admission.md)는 가입 노드의 투표자 볼륨 손실 시 승인 보호를 추가로 채택한다. [ADR-0029](../decisions/ADR-0029-replication-factor-write-quorum-profile.md)는 복제 계수 3, 쓰기 쿼럼 2 프로필과 제거 보호를 채택한다. 이 범위 밖의 Draft·운영 수용 상태는 바꾸지 않는다. |
| 관련 시험 | 클러스터 스펙 §11 멤버십 매핑; 현재 구현·시험 미완료 |

## 1. 멤버십 식별 정보와 버전 협상

### 1.1 확정된 멤버 식별 정보

확정된 멤버 레코드에는 피어 식별 정보, 프로토콜·스키마 버전, 수명 주기 상태와 변경 가능한 릴리스·기능 빌드 프로필 필드가 저장된다.

| 필드 | 자료형 | 필수 제약 |
| --- | --- | --- |
| `memberId` | 양수 `uint64` | `0` 금지. 제거된 ID는 tombstone으로 유지하고 재사용 금지 |
| `clusterId` | UTF-8 문자열 | 1~200바이트; 클러스터에서 동일 |
| `address` | UTF-8 `host:port` 문자열 | 1~255바이트; 포트 `1..65535`; 와일드카드 listener 주소 금지 |
| `dnsSan` | DNS 이름 | 1~253 ASCII 바이트; IP 리터럴 금지 |
| `certificateSha256` | 32바이트 원시 값 | TLS 리프 인증서 DER의 SHA-256 |
| `releaseVersion` | 정규 `MAJOR.MINOR.PATCH` 문자열 | 1~64 ASCII 바이트; 퇴역하지 않은 각 멤버와 주 버전이 같고 부 버전 차이가 최대 1이어야 함; 변경 가능한 commit 빌드 프로필 |
| `features` | 정렬된 고유 문자열 집합 | 정규식 `^[a-z][a-z0-9-]{0,62}$`를 따르는 불투명 토큰 최대 16개; 변경 가능한 commit 빌드 프로필 |
| `clusterProtocolMajor`, `clusterProtocolMinor` | `uint32` | v2 프로필은 정확히 `2.0` |
| `stateSchemaMajor`, `stateSchemaMinor` | `uint32` | 초기 Draft 프로필은 정확히 `1.0` |
| `phase` | enum | `STAGED`, `LEARNER`, `CATCHING_UP`, `VOTER`, `LEAVING`, `RETIRED` |
| `operationId` | 소문자 UUID 문자열 | 변경 진행 중에는 비어 있지 않아야 함; 안정된 기존 투표자·학습자는 비어 있을 수 있음 |

`releaseVersion`은 앞에 0이 없는 정규 `MAJOR.MINOR.PATCH` 형식이며 각 구성 요소는 `uint32` 10진수다. `v` 접두사, 사전 릴리스, 빌드 접미사는 허용하지 않는다. 릴리스 호환성은 주 버전이 같고 부 버전 차이가 1 이하일 때 성립하며 패치 버전은 비교하지 않는다. `releaseVersion`과 `features`는 변경 가능한 프로필이므로 멤버 식별 정보 비교에서 제외하고 별도의 commit된 프로필 갱신으로 변경한다. 클러스터 ID, 인증서 바인딩, 주소, 프로토콜·스키마, 멤버십 단계의 기존 식별 정보 검사와 정확한 일치 검사는 유지한다.

TLS 1.3 mTLS는 먼저 CA 인증서 체인과 확장 키 사용(EKU)을 검증한다. 피어 어댑터는 TLS 리프 인증서의 DNS SAN과 SHA-256을 commit된 식별 정보와 대조한다. 또한 클러스터 ID, 멤버 ID, 광고 주소, 프로토콜·상태 버전, 단계와 operation ID를 commit된 등록 정보와 대조한다. 값이 다르거나 지원하지 않는 버전이거나 hello 형식이 잘못되었거나 ID가 등록되지 않았으면, Raft payload를 `Step`에 전달하기 전에 연결을 닫는다.

Raft 프레임의 TLS·hello 식별 정보는 **직접 전송한 피어**를 인증한다. `etcd/raft`가 `MsgProp`, `MsgReadIndex`, `MsgTransferLeader`를 리더에게 전달할 때 protobuf `From`에는 **원래 요청한 멤버 ID**가 남을 수 있다. 이 세 메시지 유형에 한해서 Raft 어댑터는 인증된 직접 송신자와 원래 요청자가 모두 현재 commit된 `ConfState`에 속하는지 확인한 뒤 전달한다. 나머지 Raft 메시지는 protobuf `From`과 인증된 직접 송신자가 반드시 같아야 한다. 전송 계층은 protobuf payload를 수정하지 않는다.

클러스터 프로토콜의 주 버전은 정확히 `2`, 부 버전은 `0`이어야 한다. 현재 Draft에서 상태 스키마 주·부 버전도 정확히 `1.0`이어야 한다. 릴리스는 같은 주 버전에서 부 버전 차이가 최대 1인 경우만 허용한다. 주 버전이 다르거나 부 버전 차이가 2 이상이면 `compatibility_unsupported`로 거절한다. 클러스터 프로토콜과 스키마는 계속 정확히 일치해야 한다.

### 1.2 멤버십 단계와 안전한 진행 순서

`MemberPhase`는 멤버십 등록 정보의 commit된 상태다. 가입은 다음 단계 순서로만 진행한다.

```text
STAGED → LEARNER → CATCHING_UP → VOTER
```

- `STAGED`는 식별 정보, 인증서 바인딩, 릴리스·스키마 버전과 idempotency key가 Raft 애플리케이션 로그에 commit된 상태다. 아직 Raft 투표자가 아니다.
- `LEARNER`는 `ConfChangeV2` 학습자 추가가 commit되고 `RawNode.ApplyConfChange`가 적용된 상태다.
- `CATCHING_UP`은 학습자가 snapshot과 commit된 로그 후속분을 적용하는 상태다.
- `VOTER`는 로그 동기화를 확인한 뒤 별도의 `ConfChangeV2` 승격이 commit된 상태다.
- 계획된 제거는 `VOTER → LEAVING → RETIRED` 순서로 진행한다. 모든 세션 소유권을 후계자에게 이전하고 복제된 영속 큐와 처리 중인 상태를 이어받을 수 있게 한 뒤에만 `RETIRED`로 바꾸거나 투표자를 제거한다.
- `ABORT_STAGE`는 학습자로 전환하기 전의 stage만 취소한다. 이미 `ConfChangeV2`가 commit된 학습자를 heartbeat 시간 초과로 제거하는 데 사용할 수 없다.
- `RETIRED` 식별 정보와 ID tombstone은 snapshot에 남는다. ID를 다시 stage할 수 없다.
- 하나의 클러스터 그룹에서는 멤버십 작업을 한 번에 하나만 진행한다. 로컬 proposal 수락은 commit을 뜻하지 않는다. 관리 성공 응답은 stage 또는 현재 단계가 commit된 뒤에만 보낸다.
- Heartbeat 시간 초과는 장애 관측일 뿐 제거 명령이 아니다. 예기치 않은 장애가 발생해도 운영자가 승인하지 않은 투표자 집합 변경은 금지한다.

Raft 어댑터는 `ConfChangeV2`가 commit된 Ready entry일 때만 `RawNode.ApplyConfChange`를 호출한다. 애플리케이션 멤버십 레코드와 파생 등록 정보는 같은 commit entry 순서를 따른다. 복구할 때는 snapshot과 commit된 WAL 후속분에서 `ConfState`, stage record, phase, operation ID와 tombstone을 재구성한다. Replay 뒤 애플리케이션 index가 기존 타입 지정 snapshot index보다 앞서 있으면, 피어 전송을 시작하기 전에 replay가 반영된 새 타입 지정 snapshot을 저장한다.

**계획된 제거의 세션 인계:** 리더는 계획된 제거 중 대상 멤버가 소유한 모든 세션의 후계자로 남은 투표자 중 member ID가 가장 작은 멤버를 선택하고 `OwnershipActionTransfer`(epoch+1) 명령을 commit한다. 소유권 이전은 권위 있는 `DeliverySessionState`를 바꾸지 않는다. 새 소유자는 `ATTACH`를 사용해 대기 메시지와 인바운드·아웃바운드 QoS 상태를 복구한다. 탈퇴 대상 노드의 로컬 소켓은 takeover로 안전하게 닫는다(`CloseSessionForTakeover`). 후계 투표자가 없거나(`no_successor_voter`), epoch가 소진되었거나(`ownership_epoch_exhausted`), 고아 구독이 있으면 제거를 실패 시 차단하고 `REMOVING` 상태를 유지한다. 탈퇴 대상 노드는 `LEAVING` 상태에서 새 세션 승인을 거절한다.

### 1.3 관리 API

기존 `/api/v1` 조회 API는 유지하고 아래 변경·상태 조회 작업을 추가로 제공한다. 변경 요청에는 `ClusterAdmin`, 작업 조회에는 `ReadOnly` role이 필요하다. 요청과 응답은 `application/json`을 사용하며 공통 오류 응답 형식과 `X-Request-Id`는 [관리 API v1](management-api-v1.md)을 따른다.

#### `POST /api/v1/cluster/members`

가입 노드를 stage한다. 본문은 엄격한 JSON이어야 한다. 알 수 없는 필드, 중복 필드, 잘못된 자료형 또는 뒤따르는 JSON 값이 있으면 `400 invalid_request`를 반환한다.

```json
{
  "memberId": 4,
  "address": "node4.example:19004",
  "dnsSan": "node4.example",
  "certificateSha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "releaseVersion": "1.0.0",
  "clusterProtocolMajor": 2,
  "clusterProtocolMinor": 0,
  "stateSchemaMajor": 1,
  "stateSchemaMinor": 0
}
```

필수 header인 `Idempotency-Key`는 UTF-8이 아닌 출력 가능한 ASCII `0x21..0x7e`를 사용해야 하며, 길이는 `1..128`이고 공백은 허용하지 않는다. 같은 key와 같은 정규 요청은 같은 작업을 반환한다. 같은 key에 다른 요청을 보내면 conflict다. Key와 operation ID는 commit된 stage record에 저장한다.

새 stage는 식별 정보, ID tombstone 여부, 인증서 바인딩, 프로토콜·상태 버전, 현재 리더와 quorum을 확인한 뒤 log에 commit한다. stage record가 commit된 뒤에만 `202 Accepted` 응답을 반환한다. 응답 본문은 다음과 같다.

HTTP 요청에 `clusterId`를 포함하지 않는다. Service는 기존 commit된 멤버 등록 정보의 cluster identity를 후보 레코드에 적용한다. 요청자는 다른 cluster identity를 선택할 수 없다.

```json
{
  "data": {
    "operationId": "00000000-0000-4000-8000-000000000004",
    "action": "ADD",
    "memberId": 4,
    "phase": "STAGED",
    "committedIndex": 41
  }
}
```

commit된 요청을 재시도하면 HTTP `200`과 현재 commit된 phase를 반환한다. `202`는 동기 작업이 완료되었다는 뜻이 아니다. Quorum에 commit된 stage와 계속 진행 중인 작업을 뜻한다.

#### `DELETE /api/v1/cluster/members/{memberId}`

활성 투표자의 계획된 탈퇴 작업을 시작한다. `memberId`는 정규 양의 10진수 `uint64`여야 한다. 필수 `Idempotency-Key`와 작업 레코드 규칙은 POST와 같다. 요청 본문은 없다. 응답은 POST와 같은 작업 스키마를 사용하며 `action="REMOVE"`와 최초 작업의 `phase="REMOVING"`을 반환한다. 멤버 수명 주기는 `VOTER → LEAVING → RETIRED`다. `REMOVING`은 소유권을 이전한 뒤 새 소유자에서 영속 세션 상태를 재개하고 구성에서 멤버를 제거하는 작업이다. `SUCCEEDED` 전에는 투표자가 제거된 것으로 간주하지 않는다.

#### `GET /api/v1/cluster/membership-operations/{operationId}`

`ReadOnly` role로 commit된 작업 레코드를 읽는다. UUID 형식이 아닌 ID는 `400 invalid_request`, 알 수 없는 UUID는 `404 resource_not_found`를 반환한다. 리더가 없어도 로컬의 commit된 상태를 조회할 수 있다. 응답의 `phase`는 마지막으로 관찰된 commit 단계만 표시한다.

#### 작업 응답

`phase`는 `STAGED`, `LEARNER`, `CATCHING_UP`, `PROMOTING`, `TRANSFERRING`, `REMOVING`, `SUCCEEDED`, `ABORTED` 중 하나다. `committedIndex`는 해당 작업 단계를 기록한 Raft commit index다. proposal index나 로컬 queue에만 들어 있는 proposal index가 아니다. 응답에 피어 주소, 인증서, token 또는 원시 오류를 포함하지 않는다.

#### HTTP 오류 매핑

| 상태 | `error.code` | 조건 | 재시도 가능 |
| ---: | --- | --- | --- |
| 400 | `invalid_request` | 관리 요청의 형식이 잘못되었거나 정규 형식이 아님 | 아니요 |
| 401 | `unauthorized` | management 인증 정보가 없거나 잘못됨 | 아니요 |
| 403 | `forbidden` | `ClusterAdmin` role이 부족함 | 아니요 |
| 404 | `resource_not_found` | 작업 또는 멤버를 찾을 수 없음 | 아니요 |
| 409 | `member_id_reserved` | ID가 active, staged 또는 retired 이력에 있음 | 아니요 |
| 409 | `member_identity_conflict` | ID가 다른 식별 정보에 이미 연결됨 | 아니요 |
| 409 | `idempotency_conflict` | 같은 key가 다른 정규 요청에 사용됨 | 아니요 |
| 409 | `membership_change_in_progress` | 다른 멤버십 전이가 terminal phase에 도달하지 않음 | 예, 기존 전이가 끝난 뒤 |
| 409 | `replication_factor_unsatisfied` | 제거 후 투표자 수가 `replicationFactor`보다 적어짐 | 아니요 |
| 409 | `not_leader` | 변경 요청이 현재 리더가 아닌 endpoint에 도달함 | 예, 리더를 확인한 뒤 |
| 409 | `feature_not_supported_by_all_members` | Feature activation 요청에서 퇴역하지 않은 멤버 모두가 해당 feature를 광고하지 않음 | 아니요 |
| 422 | `compatibility_unsupported` | 릴리스, cluster protocol 또는 schema 조합이 지원되지 않음 | 아니요 |
| 503 | `quorum_unavailable` | 새 quorum barrier 또는 commit을 완료할 수 없음 | 예 |
| 500 | `internal_error` | 내부 service 또는 storage 오류를 정제해 반환함 | 아니요 |

리더가 없거나 quorum을 확보하지 못한 상태에서 변경 요청을 처리하면 stage나 구성 entry를 남기지 않는다. 오류 응답의 message는 민감 정보를 제거한 고정 문구여야 하며 내부 원인, TLS secret과 payload를 노출하지 않는다.

<a id="14-combined-cluster-configuration"></a>

### 1.4 통합 클러스터 구성

`-cluster-config <path>`는 MQTT, 관리 API와 동적 Raft 멤버십을 함께 구성하는 엄격한 JSON 모드다. 이 옵션 외의 다른 명령행 옵션이나 위치 인수는 허용하지 않으며, 기존 `-raft-peer-config <path>`와 함께 사용할 수 없다. 기존 피어 전용 config의 스키마와 디스크 동작은 바꾸지 않는다. 설정 파일에 알 수 없는 key, 중복 key, 뒤따르는 JSON 값 또는 필수 필드 누락이 있으면 process 시작을 거절한다. `releaseVersion`은 빌드 identity에서 가져오므로 이 key를 JSON에 넣으면 알 수 없는 필드로 처리해 시작을 거절한다. 상대 경로는 process의 현재 작업 디렉터리를 기준으로 한다.

| 필드 | JSON 자료형 | 필수 조건 |
| --- | --- | --- |
| `mode` | 문자열 | 정확히 `"cluster"` |
| `clusterId` | 문자열 | UTF-8 `1..200`바이트 |
| `nodeId` | `uint64` | 양수여야 하며 로컬 인증서에 바인딩된 식별 정보와 일치해야 함 |
| `directory` | 문자열 | 영속 경로; 초기 부트스트랩 또는 새 가입 노드에서만 비어 있을 수 있음 |
| `listenAddress` | 문자열 | 로컬 피어 TCP bind 주소 `host:port` |
| `advertisedAddress` | 문자열 | 피어가 접속할 수 있는 `host:port`; 와일드카드 주소 금지 |
| `localDnsName` | 문자열 | 인증서 DNS SAN 및 commit된 로컬 식별 정보와 일치 |
| `clusterProtocolMajor`, `clusterProtocolMinor` | `uint32` | 정확히 `2`, `0` |
| `stateSchemaMajor`, `stateSchemaMinor` | `uint32` | 이 Draft에서는 정확히 `1`, `0` |
| `role` | 문자열 | 정확히 `"bootstrap"` 또는 `"joiner"` |
| `bootstrapMembers` | 멤버 객체 배열 | bootstrap에서는 서로 다른 식별 정보 3개; joiner에서는 빈 배열 |
| `seedAddresses` | 문자열 배열 | bootstrap에서는 빈 배열; joiner에서는 정규 DNS `host:port` 주소 1~16개. TLS SNI를 DNS 이름에 바인딩하므로 IP 리터럴은 거절 |
| `peerTLS` | 객체 | `certFile`, `keyFile`, `caFile` 필수; 인증서와 키가 일치하고 CA가 모든 피어를 검증해야 함 |
| `management` | 객체 | `listenAddress`, `clusterAdminTokenFile`, `auditDirectory` 필수. Token은 break-glass `ClusterAdmin` opaque secret이다. `auditDirectory`는 비어 있지 않은 경로여야 하며 `directory`와 같거나 서로 포함해서는 안 되고, 각 노드에서 달라야 한다. 선택 정수 필드 `requestRate`, `authFailureRate`는 각각 `1..1,000,000`; 생략하면 `50`, `5`를 사용한다. 선택 `apiKeys`는 `{id,keyFile,roles}` 객체 배열이며 `id`는 `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`와 일치하고 고유해야 하며, `keyFile`은 유효한 경로여야 하고 `roles`는 비어 있지 않은 고유한 `ReadOnly`, `Operator`, `SecurityAdmin`, `ClusterAdmin` 값만 포함해야 한다. Key 파일 내용은 앞뒤 공백 제거 후 비어 있지 않아야 하며 cluster admin token 및 다른 API key와 달라야 한다. 선택 `oidc`는 JWT verifier 필드(`issuer`, `audience`, `algorithms`, `jwksUrl`, `cacheTtl`, `refreshCooldown`, `clockSkew`, `httpTimeout`)와 비어 있지 않은 `identities` 배열로 구성한다. 각 identity는 비어 있지 않고 최대 250 rune인 고유 `subject`와 비어 있지 않은 고유 역할 배열을 가져야 한다. JWT 필드는 MQTT `jwt` 설정과 같은 검증 규칙을 적용하며 `jwksUrl`은 HTTPS 또는 loopback HTTP여야 한다. OIDC 권한은 JWT claim이 아니라 정적 subject-to-roles binding에서 결정한다. |
| `mqtt` | 객체 | `tcpListenAddress`, `tlsListenAddress`, `securityConfigFile` 필수; 주소가 비어 있으면 해당 listener를 사용하지 않는다. 선택 정수 필드 `tcpMaxConnections`, `tcpConnectionRate`, `tlsMaxConnections`, `tlsConnectionRate`는 0~1000000이며 생략하면 0(제한 없음)이다. 해당 listener 주소가 비어 있으면 그 listener의 두 값은 0이어야 한다. |
| `replication` | 객체(선택) | 생략하면 복제 계수 3과 쓰기 쿼럼 2를 사용한다. 지정하면 정확히 `{factor:int, writeQuorum:int}`여야 하며 3/2만 허용한다. |

`bootstrapMembers`의 각 항목은 정확히 `{memberId:uint64,address:string,dnsSan:string,certificateSha256:string}` 형식이며 §1.1과 같은 식별 제약을 따른다. 세 bootstrap 구성은 `memberId` 기준으로 바이트 단위 정렬을 사용하고 같은 집합을 포함해야 한다. 각 멤버의 인증서 지문과 SAN은 해당 항목과 일치해야 한다. 모든 bootstrap 노드는 같은 빌드 릴리스와 feature 집합을 사용해야 한다. `seedAddresses`는 임의의 검색 서버가 아니라 피어 승인 listener를 가리킨다. 각 host는 소문자 DNS 이름이어야 하며 TLS server name으로 사용되고 접속한 seed 인증서 SAN과 일치해야 한다. ClusterAdmin token은 지정한 파일에서 읽는다. 원본 token은 JSON이나 로그에 포함하지 않는다.

Bootstrap 예시다. `<64 lowercase hex>`는 인증서 지문에서 가져오는 구성 값이며, 그대로 허용되는 리터럴 값이 아니다. 예시의 릴리스와 feature는 바이너리 빌드 식별 정보에서 가져온다.
```json
{
  "mode": "cluster",
  "clusterId": "sample-cluster",
  "nodeId": 1,
  "directory": "data/node1",
  "listenAddress": "127.0.0.1:19001",
  "advertisedAddress": "127.0.0.1:19001",
  "localDnsName": "node1.example",
  "clusterProtocolMajor": 2,
  "clusterProtocolMinor": 0,
  "stateSchemaMajor": 1,
  "stateSchemaMinor": 0,
  "role": "bootstrap",
  "bootstrapMembers": [
    {"memberId": 1, "address": "127.0.0.1:19001", "dnsSan": "node1.example", "certificateSha256": "<64 lowercase hex>"},
    {"memberId": 2, "address": "127.0.0.1:19002", "dnsSan": "node2.example", "certificateSha256": "<64 lowercase hex>"},
    {"memberId": 3, "address": "127.0.0.1:19003", "dnsSan": "node3.example", "certificateSha256": "<64 lowercase hex>"}
  ],
  "seedAddresses": [],
  "peerTLS": {"certFile": "certs/node1.crt", "keyFile": "certs/node1.key", "caFile": "certs/ca.crt"},
  "management": {
    "listenAddress": "127.0.0.1:18080",
    "clusterAdminTokenFile": "secrets/cluster-admin.token",
    "apiKeys": [{"id": "dashboard", "keyFile": "secrets/dashboard.key", "roles": ["ReadOnly"]}],
    "auditDirectory": "data/node1-audit"
  },
  "mqtt": {"tcpListenAddress": "", "tlsListenAddress": "127.0.0.1:1883", "securityConfigFile": "security.json"}
}
```

`role="bootstrap"`은 데이터 디렉터리가 비어 있고 로컬 ID를 포함한 bootstrap 식별 정보가 정확히 3개여야 한다. 모든 초기 노드 구성은 같은 `clusterId`와 동일한 정규 bootstrap 식별 정보 집합을 포함해야 한다. 모든 bootstrap 노드는 같은 빌드 릴리스와 feature를 사용해야 한다. 비어 있지 않은 디렉터리를 덮어써서는 안 된다.

`role="bootstrap"`으로 시작하면서 데이터 디렉터리가 비어 있거나 아직 없으면, §1.5의 `NMQBOOT2` 확인을 나머지 bootstrap 멤버 모두에 대해 먼저 완료해야 한다. 확인에 성공하기 전에는 Raft, 관리 또는 MQTT listener를 열어서는 안 된다.

`role="joiner"`는 정규 DNS `seedAddresses`(`host:port`, IP 리터럴 금지) 하나 이상, 이미 발급된 양수 `nodeId`, 빈 `bootstrapMembers`, 로컬 `advertisedAddress`·DNS SAN·인증서 지문, 새 디렉터리 또는 빈 디렉터리를 요구한다. `RawNode.Bootstrap`을 호출해서는 안 된다. 먼저 시드 승인을 수행하고 정확히 일치하는 commit된 `STAGED` 레코드를 확인해야 한다. `seedAddresses`는 검색·부트스트랩 접점이며 피어를 추가할 권한을 주지 않는다.

같은 volume을 사용하는 가입 노드가 재시작하면 영속 snapshot/WAL에서 commit된 로컬 식별 정보를 복구한다. 복구된 `LEARNER`·`CATCHING_UP`·`VOTER` 단계는 기존 seed loop에서 새 식별 정보 승인 없이 이어간다. 기존 투표자가 해당 기록을 잃으면 같은 ID를 새 가입 노드로 승인하지 않고 새 ID로 교체한다. Seed runtime은 commit된 `VOTER` 후보를 투표자 수와 관계없이 거절한다([ADR-0024](../decisions/ADR-0024-joiner-voter-volume-loss-admission.md)).

모든 모드는 `peerTLS` 인증서·키·CA, 로컬 인증서 DNS SAN과 지문의 일치, `management.clusterAdminTokenFile`, 빌드 릴리스와 feature 식별 정보, 프로토콜·스키마 버전, 영속 `directory`를 요구한다. MQTT listener를 사용하면 MQTT TLS·보안 구성도 필요하다. 제거된 JSON key `releaseVersion`은 알 수 없는 필드이므로 시작을 거절한다. Admin token은 secret 파일에서 읽으며 로그에 남기거나 응답으로 반환하지 않는다. Standalone runtime은 이 모드를 불러오지 않으며 기존 동작을 유지한다. Cluster management의 선택 `apiKeys`·`oidc`와 그 정적 검증 규칙은 §1.4를 따른다.

#### 로컬 식별 정보의 영속 메타데이터

통합 모드는 로컬 식별 정보를 `cluster.meta` v2 형식으로 저장한다. 피어 전용 `NMQRAFT1` v1 형식은 해석하거나 이관하지 않는다. 체크섬 앞의 v2 payload 필드는 다음 순서다. ASCII 매직 값 `NMQCLST2` 8바이트; 형식 버전 `uint8` 값 `2`; 클러스터 ID 길이 `uint16`과 UTF-8 bytes; 로컬 멤버 ID `uint64`; 로컬 TLS 리프 DER의 SHA-256 32바이트; 실행 모드 `uint8`(`1=bootstrap`, `2=joiner`); bootstrap 투표자 수 `uint16`; 오름차순으로 정렬한 bootstrap 멤버 ID `uint64` 값. CRC32C `uint32` big-endian 체크섬은 앞선 모든 바이트를 포함한다. Bootstrap의 투표자 수는 `3`이며 구성된 초기 투표자 집합과 일치해야 한다. Joiner의 투표자 수는 `0`이어야 한다. 알 수 없는 버전, 형식이 잘못되었거나 정규 형식이 아닌 바이트, 체크섬 불일치, 현재 클러스터 ID·노드 ID·인증서 지문·실행 모드·bootstrap 집합의 불일치는 시작을 fail-closed 방식으로 거절한다.

`RawNode`를 열기 전에 임시 파일을 만들고 메타데이터 전체를 쓴 뒤 파일 `Sync`, 원자적 rename, 상위 디렉터리 `Sync` 순으로 저장한다. 일치하는 v2 메타데이터가 없는 비어 있지 않은 디렉터리는 거절한다. v1 피어 전용 디렉터리를 자동으로 업그레이드하지 않는다. 멤버 작업 이력과 commit된 식별 정보 등록부는 로컬 메타데이터가 아니라 타입이 지정된 Raft 애플리케이션 상태에 저장한다.

Windows에서는 `MoveFileExW`와 `MOVEFILE_WRITE_THROUGH`로 rename한다. 다른 지원 플랫폼에서는 rename 뒤 상위 디렉터리를 동기화한다.

<a id="15-peer-transport-v2-handshake-and-framing"></a>

### 1.5 피어 전송 v2 핸드셰이크와 프레이밍

v2 피어 listener는 TLS 1.3 mTLS를 요구한다. v1은 `NMQRAFT1`을 사용하며 변경하지 않는다. v2는 통합 클러스터 모드에서만 동작한다. 모든 정수는 부호 없는 big-endian 형식이다. 문자열 길이는 UTF-8 바이트 수로 기록한다. 메모리를 할당하기 전에 문자열 길이와 내용을 검증하고, 문자열은 정규 형식이어야 한다. DNS 이름은 ASCII만 사용한다.

<a id="seed-admission-exchange"></a>

#### 시드 승인 교환

가입 노드는 Raft 메시지 스트림을 열기 전에 시드 TLS 연결에서 길이 prefix가 붙은 `NMQJOIN2` 요청을 보낸다. 요청·응답 프레임의 최대 크기는 64 KiB다. prefix는 big-endian `uint32`이며 payload 크기 한도에 포함하지 않는다.

요청 payload 필드는 다음 순서로 인코딩한다.

| 필드 | 인코딩 | 한도 |
| --- | --- | --- |
| 매직 값 | ASCII `NMQJOIN2` 8 bytes | 정확한 값 |
| Cluster 프로토콜 | major와 minor 각각 `uint16` | `2,0` |
| Cluster ID | `uint16` 길이 + UTF-8 bytes | `1..200` |
| 후보 member ID | `uint64` | 양수 |
| 광고 주소 | `uint16` 길이 + UTF-8 bytes | `1..255`, `host:port` |
| DNS SAN | `uint16` 길이 + ASCII bytes | `1..253` |
| 인증서 지문 | 32 bytes | 피어 TLS 리프 DER의 SHA-256 |
| 릴리스 버전 | `uint16` 길이 + UTF-8 bytes | `1..64` |
| 상태 스키마 | major와 minor 각각 `uint16` | 지원 버전과 정확히 일치 |

수락 응답도 길이 prefix가 붙은 `NMQJOIN2` 프레임이다. Prefix는 big-endian `uint32`이고 최대 payload 크기는 64 KiB다. Payload 필드는 다음 순서로 인코딩한다. ASCII magic 8 bytes; major/minor 각각 `uint16`(`2,0`); 상태 `uint8`(`1` 수락, `0` 거절); 수락된 경우에만 cluster ID 길이 `uint16`과 bytes; operation ID 길이 `uint16`과 bytes(안정된 `VOTER`만 비어 있을 수 있으며, 그 외에는 정규 36-byte UUID); 후보 member ID `uint64`; 후보 인증서 지문 32 bytes; 후보 DNS SAN 길이 `uint16`과 bytes; 후보의 commit된 phase `uint8`(`STAGED=1`, `LEARNER=2`, `CATCHING_UP=3`, `VOTER=4`); 상태 스키마 major/minor 각각 `uint16`; endpoint 수 `uint16`(후보를 뺀 기존 투표자 3개로 고정); 이어서 각 endpoint를 member ID 오름차순으로 정렬해 ID `uint64`, 주소 길이 `uint16`과 bytes, DNS SAN 길이 `uint16`과 bytes, 인증서 지문 32 bytes, phase `uint8`(`VOTER=4`) 순으로 인코딩한다.
응답 phase가 `STAGED`인 동안 가입 노드는 같은 시드 승인 요청을 반복하되 Raft 스트림을 열어서는 안 된다. 시드가 `LEARNER` 또는 `CATCHING_UP`으로 commit된 식별 정보를 반환하면 가입 노드는 그 식별 정보를 로컬 전송에 적용하고 기존 투표자 3개와 대조·조정한다. 현재 seed runtime은 commit된 `VOTER` 후보를 거절한다. 기존 wire decoder는 이전 시드가 보내는 `VOTER` 응답을 표현할 수 있지만, 로컬 transport가 `STAGED`이면 식별 정보나 피어를 설치하기 전에 이를 거절한다. 이미 승인된 learner·catching-up transport는 기존 phase 전이를 처리할 수 있다. 이는 새 투표자 승인이나 영속 이력이 복구되었다는 증거가 아니다. 시드 응답 자체는 멤버십을 commit하지 않는다.

<a id="empty-directory-bootstrap-probe"></a>

#### 빈 디렉터리의 부트스트랩 확인

추가된 `NMQBOOT2` 교환은 같은 TLS 1.3 mTLS 피어 listener를 사용한다. 요청과 응답에는 big-endian `uint32` payload 길이 prefix가 붙으며, payload 한도는 1024 bytes다.

요청 payload 필드는 다음 순서로 인코딩한다.

| 필드 | 인코딩 | 한도 |
| --- | --- | --- |
| 매직 값 | ASCII `NMQBOOT2` 8 bytes | 정확한 값 |
| 프로토콜 | major와 minor 각각 `uint16` | `2,0` |
| Cluster ID | `uint16` 길이 + UTF-8 bytes | `1..200` |
| 확인 노드 member ID | `uint64` | 양수 |

응답 payload는 정확히 29 bytes이며 다음 순서로 인코딩한다.

| 필드 | 인코딩 | 규칙 |
| --- | --- | --- |
| 매직 값 | ASCII `NMQBOOT2` 8 bytes | 정확한 값 |
| 프로토콜 | major와 minor 각각 `uint16` | `2,0` |
| 상태 | `uint8` | `0` 거절, `1` EMPTY, `2` READY, `3` PROGRESSED |
| Term | `uint64` | 상태가 `0` 또는 `1`이면 0 |
| Commit index | `uint64` | 상태가 `0` 또는 `1`이면 0 |

디코딩 실패, cluster ID 불일치, 확인 노드 ID와 로컬 ID의 동일성, 알 수 없는 확인 노드 또는 알려진 식별 정보와 지문·DNS SAN이 다른 TLS client 인증서가 있으면 상태 `0`을 반환한다. 진행 처리기가 없는 실행 중 transport도 상태 `0`을 반환한다. 부트스트랩 전 확인에서는 유효하게 구성된 피어가 `EMPTY`를 받는다. 실행 중인 응답자는 `Term > 1` 또는 `CommitIndex > 3`이면 `PROGRESSED`를 반환한다. 단, 확인 노드가 같은 부트스트랩 집합에 속한다고 기억하는 경우는 예외다. 그 외에는 `READY`를 반환한다. 실행 중 상태인 `PROGRESSED`와 `READY` 응답에는 실제 term과 commit index가 포함된다.

확인 노드는 구성된 모든 피어의 응답을 기다리며, 하나라도 `PROGRESSED`를 반환하면 시작을 거절한다. `EMPTY`를 반환한 피어를 process memory에 기억한다. 해당 피어에서 첫 Raft 프레임을 받으면 그 피어에 대한 허용을 해제하며, 포워딩 프레임은 이를 해제하지 않는다. 안전 결정과 배포 제약은 [ADR-0022](../decisions/ADR-0022-bootstrap-volume-loss-guard.md)에 따른다.

확인 노드는 TLS 핸드셰이크 뒤 `ServerName`을 피어 DNS SAN으로 설정하고 서버 리프 인증서 지문을 검증한다.

#### 인증된 Raft hello

v2 피어 listener는 같은 TLS 1.3 mTLS 연결 포트에서 시드 승인과 Raft 피어 연결을 처리한다. `NMQJOIN2`와 `NMQRAFT2` payload는 모두 big-endian `uint32` 길이 prefix를 앞에 붙인다. Listener는 8-byte magic을 기준으로 분배하기 전에 프레임을 읽고 크기를 제한한다. `NMQRAFT2` hello payload는 최대 1024 bytes다. 검증된 hello보다 먼저 application Raft 프레임을 보내서는 안 된다.

| 필드 | 인코딩 | 한도 |
| --- | --- | --- |
| 매직 값 | ASCII `NMQRAFT2` 8 bytes | 정확한 값 |
| 프로토콜 | major와 minor 각각 `uint16` | 정확히 `2,0` |
| Cluster ID | `uint16` 길이 + bytes | UTF-8 `1..200` |
| 멤버 ID | `uint64` | 양수이며 로컬 ID가 아님 |
| 광고 주소 | `uint16` 길이 + bytes | UTF-8 `1..255` |
| DNS SAN | `uint16` 길이 + bytes | ASCII `1..253` |
| 인증서 지문 | 32 bytes | SHA-256 |
| 릴리스 버전 | `uint16` 길이 + bytes | UTF-8 `1..64` |
| Cluster 프로토콜 버전 | major와 minor 각각 `uint16` | 정확히 `2,0` |
| 상태 스키마 버전 | major와 minor 각각 `uint16` | 협상된 지원 버전과 정확히 일치 |
| 멤버 phase | `uint8` | Raft 스트림 hello는 `LEARNER=2`, `CATCHING_UP=3`, `VOTER=4`, `LEAVING=5`만 허용한다. `STAGED`는 시드 승인에서만 사용하고 `RETIRED`는 절대 허용하지 않는다. |
| 작업 ID | ASCII 36 bytes 또는 빈 값 | `LEARNER`, `CATCHING_UP`, `LEAVING`은 정규 UUID; 안정된 `VOTER`는 빈 값 |

양 끝점은 각 Raft hello 필드를 commit된 등록 정보 및 TLS 식별 정보와 대조한다. `STAGED`는 `NMQJOIN2` 시드 승인 교환에서만 허용하며 Raft 스트림 단계로는 사용할 수 없다. 학습자는 학습자 추가 구성이 commit된 뒤, 투표자는 승격이 commit된 뒤에만 Raft 메시지를 교환할 수 있다. 인증서·클러스터 불일치, 미지원 릴리스·스키마, 알 수 없는 ID, 오래된 작업 ID, 잘못된 단계, 형식이 잘못되었거나 뒤에 데이터가 붙은 바이트, hello 불일치가 있으면 프레임을 전달하기 전에 socket을 닫는다.

#### Raft 메시지 프레임과 크기 한도

v2 hello 뒤의 Raft 프레임은 v1과 같은 프레임 구조를 사용한다. `uint32` 부호 없는 big-endian payload 길이 뒤에 직렬화된 Raft protobuf를 붙인다. Payload는 `1..1,048,576`바이트여야 한다. 길이가 0이거나 한도를 넘으면 payload 메모리를 할당하기 전에 연결을 닫는다. 프레임이 잘리면 전달하지 않고 연결을 닫는다. Payload의 protobuf `From`과 `To`는 인증된 전송 식별 정보와 로컬 노드 ID에 일치해야 한다.

각 활성 피어는 송신 FIFO와 수신 대기열을 서로 독립적으로 유지한다. 각 대기열은 프레임 최대 32개와 프레임화한 데이터 4 MiB로 제한한다. 쓰기 중인 송신 메시지도 한도에 포함한다. 송신 대기열이 가득 차면 `ErrPeerBackpressure`를 즉시 반환하고 기존 연결과 대기열을 보존한다. Driver는 해당 피어만 Raft 도달 불가 또는 snapshot 실패로 표시하고 다른 피어 전송은 계속한다.

동적 driver는 새 로컬 ReadIndex를 발행할 때 `DynamicTCPPeerTransport.OutboundCapacity(peerID)`가 반환하는 피어별 잔여 메시지 수 `Messages`와 프레임화한 byte 수 `Bytes`를 참고한다. `OutboundReady()`의 쓰기 완료 알림은 여러 완료를 하나로 합칠 수 있다. 따라서 알림 수를 용량 회수량으로 간주하지 않고 현재 용량을 다시 읽는다. 현재 쓰기 중인 메시지도 송신 사용량에 포함한다. 이 제어는 wire 형식, 한도, FIFO 또는 `Send`의 즉시 거절 정책을 바꾸지 않으며, 보내지 못한 프레임의 재시도도 새로 추가하지 않는다. Driver는 로컬 요청이 막혀 기다리는 동안에도 피어 응답, tick과 Ready를 처리한다. `TestDynamicOutboundCapacityRemainsChargedUntilWireCompletion`은 실제 socket 쓰기가 끝나기 전에 용량을 반환하지 않고, 완료 뒤 FIFO 프레임 처리와 용량 회수·깨움이 일어나는지 확인한다.

사용자가 채택한 동적 Raft 수신 정책은 대기열이 가득 찼을 때 연결을 바로 닫지 않고 TCP 읽기를 멈춘 뒤 driver의 `Complete`가 공간을 만들 때까지 기다리는 것이다. 수신 작업은 길이를 검증한 뒤 payload 메모리를 할당하기 전에 메시지 수와 프레임화한 byte 공간을 예약한다. 읽는 중인 프레임과 driver가 인수했지만 아직 `Complete`하지 않은 프레임도 32개·4 MiB 한도에 포함한다. 길이 prefix를 기다리는 동안의 기존 유휴 대기 동작은 유지한다. 유효한 prefix를 받은 뒤 공간을 기다리는 시간과 payload를 읽는 시간에는 같은 5초 제한 시간을 적용한다. 제한 시간 초과, context 취소, identity revision 변경, 연결 교체 또는 종료가 발생하면 대기를 해제하고 예약을 반환하며 이전 identity의 프레임을 전달하지 않는다. 고정 투표자 v1, 전달 포워딩 스트림, 명령별 최신 증명과 quorum 확인 전 ACK 금지 동작은 바꾸지 않는다. 성능 효과는 고정 BP-03으로 별도 검증한다. 정책을 채택했다는 사실만으로 병목이 해소되었다고 판단하지 않는다.

시험 연결: `internal/adapters/tcp/peer_transport_flow_test.go`의 `TestDynamicRaftInboundBackpressureResumesFIFO`는 메시지 수·byte 포화, lease 완료 전 payload 읽기 차단, 완료 뒤 FIFO 재개를 확인한다. `TestDynamicRaftInboundWaitUnblocksOnLifecycleChange`는 취소, identity revision 변경, 종료, 연결 교체와 writer 오류 때 대기가 해제되는지 확인한다. `TestDynamicRaftInboundReservationReturnsOnTruncatedPayload`는 읽는 중 예약과 잘린 payload의 자원 회수를 확인한다. `TestDynamicRaftInboundCapacityDeadlineRejectsWithoutReservation`과 `TestDynamicRaftInboundInvalidLengthIsNeverStaged`는 제한 시간 초과나 잘못된 길이에서 예약·전달이 발생하지 않는지 확인한다. 실행 결과와 한계는 [성능 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)에 기록한다.

`TestDynamicRaftInboundCancellationBeforeStagingReturnsReservation`는 payload를 다 읽은 직후 Go context가 취소되어도 프레임을 대기열에 넣지 않고 예약을 반환하는지 확인한다.

`TestDynamicRaftResetUnblocksActiveCapacityWait`는 reader가 실제 용량 대기 select에 진입한 뒤 `ResetPeer`를 호출했을 때 deadline까지 기다리지 않고 종료하는지, 기존 수신 대기 메시지 수와 byte 수가 손상되지 않는지 확인한다. `ResetPeer`는 연결을 무효화하는 같은 mutex 경계에서 용량 대기 신호를 보낸다.

`TestDynamicPeerReconcileAndQueueBounds`는 송신 queue가 가득 찬 뒤에도 연결에서 byte를 주고받을 수 있는지, identity를 다시 바인딩하면 연결이 닫히는지 확인한다. Reconciliation은 commit된 피어 ID·주소·인증서 등록 정보를 원자적으로 교체한다. 변경할 수 없는 identity는 그대로인데 commit된 단계나 operation ID가 바뀌면 연결을 닫고 다시 인증한다. 이때 대기 중인 송신 Raft 메시지는 보존하고 이전 identity revision의 수신 대기 데이터는 버린다. 해당 revision에서 읽기 loop가 가져온 프레임도 거절한다. v2 어댑터는 대기 중인 프레임을 합친 `InboundReady` 신호로 알리고 `TakeInbound` lease로 제공한다. 클러스터 driver는 `Step` 뒤에 `Complete`를 호출한다. Reconciliation은 새 lease 발급을 막고 진행 중인 lease가 끝날 때까지 기다린다. 이어서 identity revision을 높이고 남은 이전 revision의 수신 데이터를 버린 뒤 반환한다. 불변 identity를 다시 바인딩하거나 피어를 제거하면 해당 피어를 폐기하고 두 대기열의 사용량을 정리한다. 알 수 없는 피어는 거절한다. v2는 프레임별 I/O 제한 시간 5초와 재시도 간격 100ms를 유지한다. 대기열 한도와 Raft entry 처리 중 한도는 보관된 log 크기를 제한하지 않는다.

#### 전달 포워딩 스트림

전달 포워딩 스트림은 같은 TLS 1.3 mTLS listener에서 추가로 제공된다. 인증된 hello는 매직 값이 정확히 `NMQFWRD2`라는 점을 제외하면 위의 `NMQRAFT2` hello와 필드 및 검증 방식이 같다. commit된 식별 정보, 클러스터·릴리스·스키마, 단계와 operation ID 검사를 동일하게 적용한다. `LEARNER`, `CATCHING_UP`, `VOTER`, `LEAVING`만 허용한다. 기존 연결 방향 규칙에 따라 member ID가 낮은 노드는 연결을 수락하고 높은 노드는 연결을 건다. 수락 측은 자체 `NMQFWRD2` hello로 응답한다. 양쪽 hello가 모두 검증되기 전에는 데이터 프레임을 전달하지 않는다.

hello 뒤의 프레임은 `uint32` 부호 없는 big-endian payload 길이 뒤에 payload를 붙여 인코딩한다. 길이 prefix는 payload 한도에 포함하지 않는다. Payload 길이는 `1..1,179,648`바이트여야 한다. 4-byte 길이 prefix를 기다리는 동안에는 유휴 제한 시간을 두지 않는다. Prefix를 읽고 검증한 뒤 payload에 5초 I/O 제한 시간을 적용한다. 길이가 0이거나 한도를 넘거나 payload가 잘리면 stream을 닫는다.

v1 전달 메시지 payload는 다음 순서로 인코딩한다.

| 필드 | 인코딩 | 한도 |
| --- | --- | --- |
| 형식 버전 | `uint8` | 정확히 `1` |
| 세션 ID | `uint16` 바이트 길이 + 불투명한 routing ID 바이트(UTF-8이 아닐 수 있음) | `1..65,535` |
| Topic | `uint16` 바이트 길이 + UTF-8 바이트 | `1..65,535` |
| Flags | `uint8` | bit 0은 Retain; 나머지 bit는 모두 0 |
| 만료 시각 | Unix millisecond 단위 `uint64` | `0`은 만료 없음 |
| Payload | `uint32` 바이트 길이 + 바이트 | 전체 payload 프레임 최대 `1,179,648`바이트 |

모든 정수는 부호 없는 big-endian이다. Session ID는 내부 routing용 불투명 식별 정보이며 UTF-8로 디코딩하지 않는다. Topic 문자열은 유효한 UTF-8이어야 한다. Payload는 이진 데이터다. 뒤에 추가 바이트를 허용하지 않는다. Decoder는 지원하지 않는 버전, 유효하지 않은 topic UTF-8, 비어 있거나 한도를 넘는 문자열, 알 수 없는 flag bit, 잘리거나 길이가 맞지 않는 데이터, 뒤에 추가된 바이트, 전체 프레임 한도를 넘는 payload를 거절한다. 디코딩 결과는 payload 바이트의 복사본을 소유한다.

각 피어는 양방향에 독립된 FIFO 송신 대기열과 수신 staging 대기열을 가진다. 각 대기열은 프레임 최대 1,024개와 프레임화한 데이터 8 MiB로 제한한다. 전달 대기열이 가득 차도 Raft 스트림이나 포워딩 스트림을 재설정하지 않고 해당 전달을 거절한다. Raft 대기열은 별도의 기존 한도인 프레임 32개·4 MiB와 포화 동작을 유지한다. Forward frame은 포워딩 수신 측에만 전달하며 Raft `Step`으로 보내지 않는다. 수신 application은 commit된 로컬 소유권·세션 등록 정보와 더 이상 일치하지 않는 frame을 버릴 수 있다. 확인 응답이나 재시도는 없다.

포워딩 스트림은 `NMQRAFT2` hello, Raft 메시지 프레임 바이트, 멤버십 레코드, snapshot, backup 또는 클러스터 프로토콜 `2.0`을 변경하지 않는다.

### 1.6 복제 레코드와 스냅샷

명령·스냅샷 형식, 크기 한도, 정렬과 정규 바이트는 [복제 상태 v1 §1](replicated-state-v1.md#1-형식과-의미)을 따른다. 이 계약은 멤버십 식별 정보, 전이와 전송 경계를 정의한다.

## 2. 오류

| 오류 코드 | 조건 | 재시도 가능 | 상태 변경 |
| --- | --- | --- | --- |
| `invalid_request` | 형식이 잘못되었거나 정규 형식이 아닌 관리 요청 | 아니요 | 없음 |
| `unauthorized` / `forbidden` | 관리 API 인증 또는 역할 검사 실패 | 아니요 | 없음 |
| `member_id_reserved` | ID가 활성, stage 완료 또는 퇴역된 이력에 있음 | 아니요 | 없음 |
| `member_identity_conflict` | 같은 ID에 이미 다른 식별 정보가 연결됨 | 아니요 | 없음 |
| `idempotency_conflict` | 같은 key를 다른 요청 본문에 재사용함 | 아니요 | 없음 |
| `membership_change_in_progress` | 다른 전이가 아직 종료 단계에 도달하지 않음 | 기존 전이가 끝난 뒤 가능 | 없음 |
| `replication_factor_unsatisfied` | 제거하면 투표자 수가 `replicationFactor`보다 적어짐 | 아니요 | 없음 |
| `not_leader` | 변경 요청이 리더가 아닌 엔드포인트에 도달함 | 리더를 확인한 뒤 가능 | 없음 |
| `quorum_unavailable` | stage나 phase를 commit할 수 없음 | quorum 복구 뒤 가능 | commit되지 않은 레코드를 성공으로 알리지 않음 |
| `compatibility_unsupported` | 릴리스가 같은 주 버전에서 부 버전 차이 1 이내가 아니거나 프로토콜·스키마가 정확히 `2.0`·`1.0`이 아님 | 아니요 | 없음 |
| `feature_not_supported_by_all_members` | 활성화하려는 feature를 퇴역하지 않은 모든 멤버가 광고하지 않음 | 아니요 | 활성화하지 않음 |
| `peer_identity_rejected` | TLS·hello 식별 정보 불일치 | 운영자가 식별 정보를 수정하기 전까지 불가 | Raft 프레임이 `Step`에 전달되지 않음 |
| `incompatible_state_schema` | [복제 상태 v1 §2](replicated-state-v1.md#2-오류)에 정의된 스키마 거절 조건 | 아니요 | 해당 계약의 실패 시 적용·복구 거부 경로를 따르며 READY 상태가 되지 않음 |
| `feature_inactive` | `session-metadata` 활성화가 commit되기 전에 세션 메타데이터 payload를 제안함 | 아니요 | 메타데이터나 소유권을 변경하지 않음 |
| `session_metadata_capacity_exceeded` | 클러스터 전체 Will·principal 바이트가 8 MiB를 초과함 | 아니요 | 메타데이터나 소유권을 변경하지 않음 |
| `session_metadata_invalid` | 세션 메타데이터 payload 또는 상태가 정의된 제약을 위반함 | 아니요 | 메타데이터나 소유권을 변경하지 않음 |

## 3. 호환성 규칙

- 멤버십 v2는 `raft-peer-transport-v1` 또는 v1 `NMQRAFT1` 바이트를 변경하지 않는다.
- `NMQSTATE1`은 복제 상태 v1 인코딩이 아니며 자동으로 이관하거나 변환하지 않는다.
- 복제 상태 디코딩은 엄격하다. [복제 상태 v1 §3](replicated-state-v1.md#3-호환성-규칙)을 따른다. 세션 메타데이터는 `session-metadata`가 commit된 뒤에만 제안·저장한다.
- 퇴역하지 않은 모든 멤버가 해당 feature를 광고한 경우에만 feature 활성화를 commit한다. 활성화된 feature를 지원하지 않는 멤버가 있으면 readiness를 허용하지 않는다.
- 필드 번호와 enum 값은 재사용하지 않는다. Wire 또는 상태 형식을 깨뜨리는 변경에는 새 계약 major와 명시적인 이관·복구 계획이 필요하다.
- TLS, 핸드셰이크 식별 정보와 commit된 stage·구성 검사를 마치기 전에는 v2 피어 데이터를 허용하지 않는다.
- 추가된 `NMQFWRD2` 메시지 전달 스트림은 `NMQRAFT2` 바이트와 클러스터 프로토콜 `2.0`을 유지한다.
- `NMQBOOT2`는 같은 listener에 추가되며 `NMQJOIN2`·`NMQRAFT2` 바이트와 클러스터 프로토콜 `2.0`을 유지한다. 이를 인식하지 못하는 피어는 socket을 닫고 확인 노드는 계속 기다린다.

## 4. 버전 및 지원 상태

| 상태 | 조건 | 대체 |
| --- | --- | --- |
| Draft | 릴리스·프로필 구현과 혼합 바이너리 수용 증거가 아직 부족함 | 패키지, 프로세스, 로컬 검증 gate를 통과할 때까지 Draft 유지 |
| Active | 런타임, OpenAPI, 타입이 지정된 상태 artifact와 N-1↔N 증거가 이 계약과 일치함 | 정의된 항목 없음 |

## 5. 예시와 계약 시험

§1의 JSON·프레임 예시는 정규 필드의 의미를 정의한다. 릴리스 파싱과 프로필 갱신은 `internal/application/membership`, 타입이 지정된 codec과 snapshot 검증은 `internal/adapters/raft/records`, 피어 호환성은 `internal/adapters/tcp`, 관리 API 생산자는 `internal/adapters/http/management`, API 시험 fixture는 `tests/contracts/management-api-v1`, 다중 프로세스 수용 시험은 `tests/integration/cluster_release_compat_process_test.go`에서 확인한다.

## 6. Draft의 남은 조건

- 패키지 시험, 생성된 protobuf 시험, 혼합 바이너리 프로세스 시험, 저장소 local·architecture gate를 실행한다.
- 롤백 기간의 운영 절차와 rollout controller 사전 검사를 별도로 정의한다. 이 계약 변경에는 사전 검사가 구현되지 않았다.
- 백업 아카이브 복원은 [ADR-0021](../decisions/ADR-0021-backup-archive-release-window.md)의 릴리스 범위를 따른다. 과거 소스 버전과의 호환성은 아직 검증하지 않았다.
- 같은 소스에서 만든 합성 바이너리만으로는 과거 소스 버전 사이의 wire 호환성을 입증할 수 없다.
