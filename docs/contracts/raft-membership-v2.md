# Raft membership v2 인터페이스 계약(Contract)

이 초안은 combined cluster runtime의 member admission, membership operation과 management 경계를 정의한다. Fixed-voter peer transport v1은 그대로 유지하며, v2의 존재만으로 production 지원이나 release 간 호환성이 생기지 않는다. Typed replicated log·snapshot record는 [replicated-state-v1.proto](replicated-state-v1.proto)를 기준으로 한다.

| 항목 | 값 |
| --- | --- |
| 이름과 버전 | `narumq-raft-membership` 2.0.0 |
| 상태 | Draft |
| 소유자 | NaruMQ 프로젝트 |
| 만드는 쪽(Producer) | ClusterAdmin API, joining broker, committed Raft member registry |
| 사용하는 쪽(Consumer) | Cluster leader, joining broker, authenticated peer transport, management clients |
| 방향 | Management HTTP request/response, TLS 1.3 peer admission handshake, Raft stream, delivery-forwarding stream |
| 스키마 기준 파일 | 이 문서와 [replicated-state-v1.proto](replicated-state-v1.proto) |
| 기준 동작 명세 | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md) §1, §11 |
| 관련 요구사항 | CLU-01~CLU-04, CLU-06~CLU-11, SEC-09~SEC-11, NFR-07~NFR-10 |
| 호환성 범위 | Combined cluster runtime protocol `2.0`, state schema `1.0`; no N/N-1 claim |
| 채택 범위 | 2026-10-02 사용자가 dynamic Raft inbound의 bounded 수신 대기를 채택했다. [ADR-0024](../decisions/ADR-0024-joiner-voter-volume-loss-admission.md)는 joiner voter volume-loss admission 보호를 추가로 채택한다. 이 두 범위 밖의 Draft·production 수용 상태는 바꾸지 않는다. |
| 관련 시험 | Cluster spec §11 membership mapping; 현재 구현·시험 미완료 |

## 1. Membership identity와 version negotiation

### 1.1 Committed member identity

A committed member record stores peer identity, protocol/schema and lifecycle state, plus mutable release/features build profile fields.

| 필드 | 자료형 | 필수 제약 |
| --- | --- | --- |
| `memberId` | positive `uint64` | `0` 금지. 제거된 ID는 tombstone으로 유지하고 재사용 금지 |
| `clusterId` | UTF-8 string | 1~200 bytes; cluster 내 동일 |
| `address` | UTF-8 host:port string | 1~255 bytes; port `1..65535`; wildcard listener 주소 금지 |
| `dnsSan` | DNS name | 1~253 ASCII bytes; IP literal 금지 |
| `certificateSha256` | 32 raw bytes | TLS leaf certificate DER의 SHA-256 |
| `releaseVersion` | canonical `MAJOR.MINOR.PATCH` string | 1..64 ASCII bytes; same major and minor distance at most 1 from each non-retired member; mutable committed build profile |
| `features` | sorted unique string set | At most 16 opaque tokens matching `^[a-z][a-z0-9-]{0,62}$`; mutable committed build profile |
| `clusterProtocolMajor`, `clusterProtocolMinor` | `uint32` | v2 profile은 정확히 `2.0` |
| `stateSchemaMajor`, `stateSchemaMinor` | `uint32` | 초기 Draft profile은 정확히 `1.0` |
| `phase` | enum | `STAGED`, `LEARNER`, `CATCHING_UP`, `VOTER`, `LEAVING`, `RETIRED` |
| `operationId` | lower-case UUID string | 변경 진행 중에는 non-empty; 안정된 기존 voter/learner에는 empty 가능 |

`releaseVersion`은 선행 0 없는 `MAJOR.MINOR.PATCH` 형식이며 각 부분은 `uint32` decimal이다. `v` 접두어, pre-release 및 build suffix는 허용하지 않는다. Release compatibility는 같은 major이고 minor 차이가 1 이하일 때 true이며 patch는 무시한다. `releaseVersion`과 `features`는 mutable profile이므로 member identity 비교에서 제외하고 별도 committed profile update로 바꾼다. Cluster ID, certificate binding, address, protocol/schema와 membership phase의 기존 identity·exact 검사는 유지한다.

TLS 1.3 mTLS가 CA chain과 EKU를 먼저 검증한다. Peer adapter는 TLS leaf certificate의 DNS SAN과 SHA-256을 committed identity와 대조한다. Peer adapter는 cluster ID, member ID, advertised address, protocol/state version, phase와 operation ID도 committed registry와 대조한다. 불일치, 미지원 version, malformed hello 또는 등록되지 않은 ID는 Raft payload를 `Step`에 전달하기 전에 connection을 닫는다.

Raft frame의 TLS·hello identity는 **직접 보낸 peer**를 인증한다. `etcd/raft`가 `MsgProp`, `MsgReadIndex`, `MsgTransferLeader`를 leader에게 전달할 때 protobuf `From`에는 **원래 요청한 member ID**가 남을 수 있다. 이 세 유형에 한해서만 Raft adapter는 인증된 직접 송신자와 원래 요청자가 모두 현재 committed `ConfState`에 속하는지 확인한 뒤 전달한다. 그 외 Raft message는 protobuf `From`과 인증된 직접 송신자가 반드시 같아야 한다. Transport는 protobuf payload를 수정하지 않는다.

Cluster protocol major는 정확히 `2`, minor는 `0`이어야 한다. State schema major/minor도 현재 Draft에서 정확히 `1.0`만 받는다. Release는 동일 major의 인접 minor까지만 허용한다. 다른 major 또는 minor 차이가 2 이상이면 `compatibility_unsupported`로 거절한다. Cluster protocol과 schema는 계속 exact match다.

### 1.2 Membership phase와 안전한 순서

`MemberPhase`는 membership register의 committed 상태다. Join phase는 다음 순서로만 진행한다.

```text
STAGED → LEARNER → CATCHING_UP → VOTER
```

- `STAGED`는 identity, certificate binding, release/schema version과 idempotency key가 Raft application log에 commit된 상태다. 아직 Raft voter가 아니다.
- `LEARNER`는 `ConfChangeV2` learner addition이 commit되고 `RawNode.ApplyConfChange`가 적용된 상태다.
- `CATCHING_UP`은 learner가 snapshot과 committed log tail을 적용 중인 상태다.
- `VOTER`는 log catch-up 확인 뒤 별도의 `ConfChangeV2` promotion이 commit된 상태다.
- Planned removal은 `VOTER → LEAVING → RETIRED`다. 모든 session ownership을 successor로 이전하고 replicated durable queue·inflight state를 이어받을 수 있게 한 뒤에만 `RETIRED`로 바꾸거나 voter를 제거한다.
- `ABORT_STAGE`는 learner로 전환하기 전의 stage만 취소한다. 이미 `ConfChangeV2`가 commit된 learner를 heartbeat timeout으로 제거하는 용도로 사용할 수 없다.
- `RETIRED` identity와 ID tombstone은 snapshot에 남는다. ID는 다시 stage할 수 없다.
- 하나의 cluster group에서는 한 membership operation만 진행한다. Local proposal acceptance는 commitment가 아니며 management 성공 응답은 stage 또는 현재 phase의 commit 뒤에만 보낸다.
- Heartbeat timeout은 failure observation일 뿐 remove command가 아니다. Unexpected failure에서 operator가 승인하지 않은 voter-set 변경은 금지한다.

`ConfChangeV2`가 committed Ready entry일 때만 Raft adapter가 `RawNode.ApplyConfChange`를 호출한다. Application membership record와 derived registry는 같은 committed entry order를 소비한다. Recovery는 snapshot plus committed WAL tail에서 ConfState, staged record, phase, operation ID와 tombstone을 재구성한다. Replay로 application index가 기존 typed snapshot index보다 앞서면, peer transport를 시작하기 전에 replay 완료 상태로 새 typed snapshot을 저장한다.

**Planned removal session handoff:** planned removal 진행 중 리더는 대상 member가 소유한 모든 세션에 대해 남은 voter 중 가장 작은 member ID를 successor로 선택하여 `OwnershipActionTransfer` (epoch+1) 명령을 commit한다. 소유권 이전은 authoritative `DeliverySessionState`를 바꾸지 않고, 새 owner는 ATTACH로 queued message와 inbound/outbound QoS 상태를 복구한다. 탈퇴 대상 노드의 로컬 소켓은 takeover로 안전하게 닫힌다(`CloseSessionForTakeover`). successor voter가 없거나(`no_successor_voter`), epoch가 고갈되었거나(`ownership_epoch_exhausted`), 고아 구독이 감지되면(`orphan_subscription`) removal은 fail-closed 원칙에 따라 차단되고 `REMOVING` 상태를 유지한다. 탈퇴 대상 노드는 `LEAVING` 상태 동안 신규 세션 admission을 거절한다.

### 1.3 Management API

기존 `/api/v1` query API는 유지하고 아래 mutation/status operation을 additive로 제공한다. Mutation은 `ClusterAdmin`, operation query는 `ReadOnly` role을 요구한다. Request와 response는 `application/json`이며 공통 오류 envelope와 `X-Request-Id`는 [Management API v1](management-api-v1.md)을 따른다.

#### `POST /api/v1/cluster/members`

Joiner를 stage한다. Body는 strict JSON이며 unknown field, duplicate field, 잘못된 type와 trailing JSON value는 `400 invalid_request`다.

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

Required header `Idempotency-Key`는 UTF-8이 아닌 printable ASCII `0x21..0x7e`, 길이 `1..128`이며 공백은 허용하지 않는다. 동일 key·동일 canonical request는 같은 operation을 반환한다. 같은 key·다른 request는 conflict다. Key와 operation ID는 committed stage record에 저장된다.

새 stage는 identity, ID tombstone 여부, certificate binding, protocol/state version, current leader와 quorum을 확인한 다음 log에 commit한다. `202 Accepted` response는 stage record가 commit된 뒤에만 반환한다. Body:

`clusterId`는 HTTP request에 포함하지 않는다. Service는 기존 committed member registry의 cluster identity를 적용해 후보 record를 만든다. Caller가 다른 cluster identity를 선택할 수 없다.

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

동일한 committed request의 retry는 HTTP `200`과 현재 committed phase를 반환한다. `202`는 동기 완료가 아니라 quorum-committed stage와 계속 진행 중인 operation을 의미한다.

#### `DELETE /api/v1/cluster/members/{memberId}`

Active voter의 planned leave operation을 시작한다. `memberId`는 canonical positive decimal `uint64`이고, 필수 `Idempotency-Key` 및 operation record 규칙은 POST와 같다. Request body는 없다. Response는 POST와 동일한 operation schema에서 `action="REMOVE"`, 최초 operation `phase="REMOVING"`을 반환한다. Member lifecycle은 `VOTER → LEAVING → RETIRED`다. `REMOVING`은 ownership transfer 뒤 durable session state를 새 owner에서 재개하고 configuration removal을 진행하는 operation을 뜻하며, `SUCCEEDED` 전에는 voter가 제거된 것으로 간주하지 않는다.

#### `GET /api/v1/cluster/membership-operations/{operationId}`

`ReadOnly`로 committed operation record를 읽는다. UUID 형식이 아닌 ID는 `400 invalid_request`, 모르는 UUID는 `404 resource_not_found`다. 조회는 leader가 없어도 local committed state에서 가능하다. 응답의 `phase`는 마지막으로 관찰된 committed phase만 표시한다.

#### Operation response

`phase`는 `STAGED`, `LEARNER`, `CATCHING_UP`, `PROMOTING`, `TRANSFERRING`, `REMOVING`, `SUCCEEDED`, `ABORTED` 중 하나다. `committedIndex`는 해당 operation phase를 기록한 Raft committed index이며, proposal index나 locally queued proposal이 아니다. Response는 peer 주소, certificate, token 또는 raw error를 포함하지 않는다.

#### HTTP error mapping

| Status | `error.code` | 조건 | Retryable |
| ---: | --- | --- | --- |
| 400 | `invalid_request` | JSON, path, header, field, duplicate/unknown field 오류 | false |
| 401 | `unauthorized` | 누락·잘못된 management credential | false |
| 403 | `forbidden` | `ClusterAdmin` role 부족 | false |
| 404 | `resource_not_found` | 모르는 operation 또는 member | false |
| 409 | `member_id_reserved` | ID가 active, staged, 또는 retired history에 존재 | false |
| 409 | `member_identity_conflict` | 같은 ID/key에 저장된 identity와 request가 다름 | false |
| 409 | `idempotency_conflict` | 같은 key가 다른 canonical request에 이미 사용됨 | false |
| 409 | `membership_change_in_progress` | 다른 membership operation이 terminal phase가 아님 | true |
| 409 | `not_leader` | mutation endpoint가 current leader가 아님 | true |
| 409 | `feature_not_supported_by_all_members` | Feature activation 시 non-retired member 중 feature를 광고하지 않는 member가 있음 | false |
| 422 | `compatibility_unsupported` | release, cluster protocol 또는 schema 조합 미지원 | false |
| 503 | `quorum_unavailable` | Fresh quorum barrier/commit이 불가능 | true |
| 500 | `internal_error` | redacted unexpected service/storage error | false |

No-leader/quorum mutation은 어떤 stage/configuration entry도 남기지 않는다. Error envelope의 message는 sanitized stable text이며 internal cause, TLS secret과 payload를 노출하지 않는다.

### 1.4 Combined cluster configuration

`-cluster-config <path>`는 MQTT, management와 dynamic Raft membership을 함께 조립하는 strict JSON mode다. Flag 자체 외의 다른 CLI flag나 positional argument를 허용하지 않으며 기존 `-raft-peer-config <path>`와 상호 배타적이다. 기존 peer-only config의 schema와 disk semantics는 바꾸지 않는다. Config file에 알 수 없는 key, 중복 key, trailing value 또는 missing required field가 있으면 process startup을 거절한다. `releaseVersion`은 build identity에서 오므로 JSON에 이 key를 넣으면 unknown field로 startup을 거절한다. Relative path는 process current working directory 기준이다.

| Field | JSON type | Required rule |
| --- | --- | --- |
| `mode` | string | exactly `"cluster"` |
| `clusterId` | string | UTF-8 `1..200` bytes |
| `nodeId` | uint64 | positive; must match local cert-bound identity |
| `directory` | string | persistent, empty only for initial bootstrap or new joiner |
| `listenAddress` | string | local peer TCP bind `host:port` |
| `advertisedAddress` | string | reachable peer `host:port`; never wildcard |
| `localDnsName` | string | certificate DNS SAN and committed local identity |
| `clusterProtocolMajor`, `clusterProtocolMinor` | uint32 | exactly `2`, `0` |
| `stateSchemaMajor`, `stateSchemaMinor` | uint32 | exactly `1`, `0` for this Draft |
| `role` | string | exactly `"bootstrap"` or `"joiner"` |
| `bootstrapMembers` | array of member objects | exactly 3 unique identities for bootstrap; empty for joiner |
| `seedAddresses` | array of strings | empty for bootstrap; `1..16` unique canonical DNS `host:port` addresses for joiner; IP literals are rejected so TLS SNI is bound to a DNS name |
| `peerTLS` | object | required `certFile`, `keyFile`, `caFile`; cert/key match and CA must verify all peers |
| `management` | object | required `listenAddress`, `clusterAdminTokenFile`; token is opaque secret input |
| `mqtt` | object | required `tcpListenAddress`, `tlsListenAddress`, `securityConfigFile`; empty address disables that listener |

Each `bootstrapMembers` element has exactly `{memberId:uint64,address:string,dnsSan:string,certificateSha256:string}` with the same identity constraints as §1.1. All three bootstrap configs use bytewise-canonical sorting by `memberId` and the same set. A member's own certificate fingerprint and SAN must match its entry. All bootstrap nodes must use the same build release and feature set. `seedAddresses` address the peer admission listener, not an arbitrary discovery authority; each host is a lowercase DNS name used for the TLS server name and must match the contacted seed certificate SAN. ClusterAdmin token contents are read from the referenced file; the raw token is not part of JSON or logs.

Bootstrap example (`<64 lowercase hex>` is a configuration value supplied from the certificate fingerprint, not a literal accepted value). The example's release and features come from the binary's build identity:
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
  "management": {"listenAddress": "127.0.0.1:18080", "clusterAdminTokenFile": "secrets/cluster-admin.token"},
  "mqtt": {"tcpListenAddress": "", "tlsListenAddress": "127.0.0.1:1883", "securityConfigFile": "security.json"}
}
```

Bootstrap requires `role="bootstrap"`, an empty data directory and exactly three bootstrap identities including the local ID. All initial node configs must carry the same `clusterId` and identical canonical bootstrap identity set. Bootstrap nodes must use the same build release and features. Bootstrap must not overwrite any non-empty directory.

An empty or missing directory with `role="bootstrap"` first completes the `NMQBOOT2` probe in §1.5 against every other bootstrap member and must not open Raft, management or MQTT listeners before it succeeds.

Joiner requires `role="joiner"`, one or more configured canonical DNS `seedAddresses` (`host:port`, never an IP literal), an already-issued positive `nodeId`, empty `bootstrapMembers`, its local `advertisedAddress`/DNS SAN/certificate fingerprint, and a new/empty directory. It must not call `RawNode.Bootstrap`; it first performs seed admission and proves an exactly matching committed `STAGED` record. `seedAddresses` are discovery/bootstrap contacts, not an authority to add peers.

같은 volume의 joiner 재시작은 persisted snapshot/WAL에서 committed local identity를 복구한다. 복구된 `LEARNER`·`CATCHING_UP`·`VOTER`는 기존 seed loop에서 새 identity 승인 없이 재개한다. 기존 voter가 그 기록을 잃으면 같은 ID를 새 joiner로 승인하지 않으며 새 ID로 교체한다. Seed runtime은 committed `VOTER` 후보를 voter 수와 무관하게 거절한다([ADR-0024](../decisions/ADR-0024-joiner-voter-volume-loss-admission.md)).

All modes require `peerTLS` cert/key/CA, local certificate DNS SAN and fingerprint equality, explicit `management.clusterAdminTokenFile`, build release and feature identity, protocol/schema versions, persistent `directory`, and MQTT TLS/security configuration if an MQTT listener is enabled. The removed JSON key `releaseVersion` is an unknown field and rejects startup. Admin token contents are read as a secret file and never logged or returned. Standalone runtime does not load this mode and is unchanged.

#### Persistent local identity metadata

Combined mode stores local identity in `cluster.meta` format v2; peer-only `NMQRAFT1` format v1 is never interpreted or migrated. Exact v2 payload before checksum is: 8-byte ASCII magic `NMQCLST2`; uint8 format version `2`; uint16 cluster-ID byte length and UTF-8 bytes; uint64 local member ID; 32-byte SHA-256 of the local TLS leaf DER; uint8 launch mode (`1=bootstrap`, `2=joiner`); uint16 bootstrap voter count; then sorted ascending uint64 bootstrap member IDs. A CRC32C uint32 big-endian checksum covers all preceding bytes. Bootstrap requires count `3` and the configured initial voter set; joiner requires count `0`. Unknown version, malformed/noncanonical bytes, checksum mismatch, or any mismatch with current cluster ID, node ID, certificate fingerprint, launch mode, or bootstrap set fails startup closed.

Create metadata through a temporary file, write fully, file `Sync`, atomic rename, then containing-directory `Sync`, before opening `RawNode`. A non-empty directory without matching v2 metadata is rejected; a v1 peer-only directory is never upgraded implicitly. Member operation history and committed identity registry live in typed Raft application state, not local metadata.

Windows performs the rename with `MoveFileExW` and `MOVEFILE_WRITE_THROUGH`; other supported platforms sync the containing directory after rename.

### 1.5 Peer transport v2 handshake and framing

The v2 peer listener requires TLS 1.3 mTLS. V1 uses `NMQRAFT1` and is unchanged. V2 operates only in combined cluster mode. All integers are unsigned big-endian. Strings use UTF-8 byte lengths, are validated before allocation, and must be canonical. DNS names are ASCII.

#### Seed admission exchange

A joiner sends a length-prefixed `NMQJOIN2` request on a seed TLS connection before opening a Raft message stream. Maximum request/response frame is 64 KiB; prefix is uint32 big-endian and is excluded from the payload limit.

Request payload fields, in exact order:

| Field | Encoding | Limit |
| --- | --- | --- |
| Magic | 8 bytes ASCII `NMQJOIN2` | exact |
| Cluster protocol | uint16 major, uint16 minor | `2,0` |
| Cluster ID | uint16 length + UTF-8 bytes | `1..200` |
| Candidate member ID | uint64 | positive |
| Advertised address | uint16 length + UTF-8 bytes | `1..255`, host:port |
| DNS SAN | uint16 length + ASCII bytes | `1..253` |
| Certificate fingerprint | 32 bytes | SHA-256 of peer TLS leaf DER |
| Release version | uint16 length + UTF-8 bytes | `1..64` |
| State schema | uint16 major + uint16 minor | exact supported version |

Accepted response bytes are also length-prefixed `NMQJOIN2` frames (uint32 big-endian, maximum 64 KiB). Payload fields in order: 8-byte magic; uint16 major/minor (`2,0`); uint8 status (`1` accepted, `0` rejected); for accepted only, uint16 cluster-ID length+bytes; uint16 operation-ID length and bytes (empty only for stable `VOTER`, otherwise canonical 36-byte UUID); uint64 candidate member ID; 32-byte candidate certificate fingerprint; uint16 candidate DNS SAN length+bytes; uint8 committed candidate phase (`STAGED=1`, `LEARNER=2`, `CATCHING_UP=3`, or `VOTER=4`); uint16 state-schema major/minor; uint16 endpoint count (exactly the three incumbent voters, excluding the candidate); then each endpoint sorted by ascending member ID as uint64 ID, uint16 address length+bytes, uint16 DNS SAN length+bytes, 32-byte certificate fingerprint, and uint8 phase (`VOTER=4`).
The joiner repeats the idempotent admission request while its response is `STAGED`; it must not open a Raft stream in that phase. Once a seed reports a committed `LEARNER` or `CATCHING_UP` identity, the joiner applies that identity to its local transport and reconciles the three incumbent peers. Current seed runtime rejects committed `VOTER` candidates. The unchanged wire decoder can still represent a `VOTER` response from an older seed, but a local `STAGED` transport rejects it before identity or peer installation. An already admitted learner/catching-up transport may interpret the existing phase progression; this is not fresh voter admission or proof of recovered durable history. A seed response does not itself commit membership.

#### Empty-directory bootstrap probe

The additive `NMQBOOT2` exchange uses the same TLS 1.3 mTLS peer listener. Each request and response has a uint32 big-endian payload-length prefix; the payload limit is 1024 bytes.

Request payload fields, in exact order:

| Field | Encoding | Limit |
| --- | --- | --- |
| Magic | 8 bytes ASCII `NMQBOOT2` | exact |
| Protocol | uint16 major, uint16 minor | `2,0` |
| Cluster ID | uint16 length + UTF-8 bytes | `1..200` |
| Prober member ID | uint64 | positive |

Response payload is exactly 29 bytes, in this order:

| Field | Encoding | Rule |
| --- | --- | --- |
| Magic | 8 bytes ASCII `NMQBOOT2` | exact |
| Protocol | uint16 major, uint16 minor | `2,0` |
| Status | uint8 | `0` rejected, `1` EMPTY, `2` READY, `3` PROGRESSED |
| Term | uint64 | zero for status `0` or `1` |
| Commit index | uint64 | zero for status `0` or `1` |

Decode failure, cluster ID mismatch, prober ID equal to local ID, unknown prober, or a TLS client certificate whose fingerprint or DNS SAN differs from the known identity returns status `0`. A running transport without a progress handler also returns status `0`. During pre-bootstrap probing, a valid configured peer receives `EMPTY`. A running responder returns `PROGRESSED` when `Term > 1` or `CommitIndex > 3`, unless it remembers the prober as a co-bootstrap peer; otherwise it returns `READY`. Both running statuses include actual term and commit index.

The prober waits for every configured peer and refuses startup if any reports `PROGRESSED`. It remembers peers that answered `EMPTY` in process memory. The first received Raft frame from such a peer clears its allowance; forwarding frames do not. The safety decision and deployment constraints are in [ADR-0022](../decisions/ADR-0022-bootstrap-volume-loss-guard.md).

The prober sets TLS `ServerName` to the peer DNS SAN and verifies the server leaf fingerprint after the handshake.

#### Authenticated Raft hello

A v2 peer listener handles seed admission and Raft peers on the same TLS 1.3 mTLS connection port. Both `NMQJOIN2` and `NMQRAFT2` payloads use a uint32 big-endian length prefix before the payload, so the listener reads and bounds the frame before dispatching on its 8-byte magic. `NMQRAFT2` hello payloads are at most 1024 bytes; no application Raft frame may precede a validated hello.

| Field | Encoding | Limit |
| --- | --- | --- |
| Magic | 8 bytes ASCII `NMQRAFT2` | exact |
| Protocol | uint16 major, uint16 minor | exact `2,0` |
| Cluster ID | uint16 length + bytes | UTF-8 `1..200` |
| Member ID | uint64 | positive, not local ID |
| Advertised address | uint16 length + bytes | UTF-8 `1..255` |
| DNS SAN | uint16 length + bytes | ASCII `1..253` |
| Certificate fingerprint | 32 bytes | SHA-256 |
| Release version | uint16 length + bytes | UTF-8 `1..64` |
| Cluster protocol version | uint16 major, uint16 minor | exact `2,0` |
| State schema version | uint16 major, uint16 minor | exact negotiated support |
| Member phase | uint8 | Raft-stream hello accepts only `LEARNER=2`, `CATCHING_UP=3`, `VOTER=4`, or `LEAVING=5`; `STAGED` uses seed admission only; `RETIRED` is never admitted |
| Operation ID | 36 ASCII bytes or empty | canonical UUID for `LEARNER`, `CATCHING_UP`, `LEAVING`; empty for stable `VOTER` |

Both endpoints compare every Raft-hello field to their committed registry and TLS identity. `STAGED` is admitted only on the `NMQJOIN2` seed-admission exchange; it is never a valid Raft-stream phase. A learner can exchange Raft messages only after the committed learner-add configuration; a voter only after committed promotion. Certificate mismatch, wrong cluster, unsupported release/schema, unknown ID, stale operation ID, wrong phase, malformed/trailing bytes or hello mismatch closes the socket before frame dispatch.

#### Raft message frames and bounds

After the v2 hello, Raft frames use the same envelope shape as v1: `uint32 big-endian payload length | serialized Raft protobuf`. Payload is `1..1,048,576` bytes. Zero or oversized lengths close the connection before payload allocation. Truncated frames close without dispatch. The payload's protobuf `From` and `To` must match authenticated transport identity and local node ID.

Each active peer retains independently bounded outbound FIFO and inbound staging, each at most 32 messages and 4 MiB framed bytes; an in-progress outbound write counts. Outbound 포화는 즉시 `ErrPeerBackpressure`를 반환하고 기존 연결과 queue를 보존한다. Driver는 해당 peer만 Raft unreachable/snapshot-failure로 보고하며 다른 peer의 전송은 계속한다.

Dynamic driver의 새 local ReadIndex 발행은 `DynamicTCPPeerTransport.OutboundCapacity(peerID)`가 반환하는 peer별 남은 `Messages`·framed-byte `Bytes` snapshot을 참고한다. `OutboundReady()`의 write 완료 알림은 여러 완료를 하나로 합칠 수 있으므로 알림 횟수를 credit로 쓰지 않고 항상 현재 용량을 다시 읽는다. 현재 write는 계속 outbound 사용량에 포함한다. 이 생성 제어는 wire format·상한·FIFO·`Send`의 즉시 거절 정책을 바꾸거나 unsent frame의 새 재시도를 추가하지 않는다. Driver는 blocked local request를 기다리는 동안 peer 응답·tick·Ready를 처리한다. `TestDynamicOutboundCapacityRemainsChargedUntilWireCompletion`은 실제 socket write가 끝나기 전 용량을 돌려주지 않고 완료 뒤 FIFO frame과 용량 회수·wake를 확인한다.

사용자가 채택한 dynamic Raft inbound 정책은 queue 포화에서 연결을 즉시 닫는 대신 TCP 읽기를 멈추고 driver의 `Complete`로 공간이 생길 때까지 기다리는 것이다. Reader는 길이를 검증한 뒤 payload allocation 전에 count·framed-byte 공간을 예약하며, 읽는 중인 frame과 driver가 인수했지만 `Complete`하지 않은 frame도 32개·4 MiB 한도에 포함한다. 길이 prefix의 idle 대기는 기존 동작을 유지한다. 유효 prefix를 받은 뒤의 공간 대기와 payload 읽기는 같은 5초 deadline을 공유한다. Timeout·context 취소·identity revision 변경·연결 교체·shutdown에서는 대기를 해제하고 예약을 반환하며 이전 identity frame을 dispatch하지 않는다. Fixed-voter v1과 delivery-forwarding stream, command별 fresh proof·quorum-before-ACK은 바꾸지 않는다. 이 선택의 성능 효과는 고정 BP-03로 별도 검증하며 채택 자체를 병목 해소의 증거로 보지 않는다.

시험 연결: `internal/adapters/tcp/peer_transport_flow_test.go`의 `TestDynamicRaftInboundBackpressureResumesFIFO`는 count·byte 포화, lease 완료 전 payload 차단과 완료 후 FIFO 재개를 확인한다. `TestDynamicRaftInboundWaitUnblocksOnLifecycleChange`는 취소·identity revision 변경·shutdown·연결 교체·writer failure에서 대기 해제를, `TestDynamicRaftInboundReservationReturnsOnTruncatedPayload`는 읽는 중인 예약과 truncated payload의 회수를, `TestDynamicRaftInboundCapacityDeadlineRejectsWithoutReservation`과 `TestDynamicRaftInboundInvalidLengthIsNeverStaged`는 timeout·잘못된 길이의 무예약·무dispatch를 확인한다. 실제 실행·한계는 [성능 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)에 연결한다.

`TestDynamicRaftInboundCancellationBeforeStagingReturnsReservation`는 payload 읽기 완료 직후 context가 취소된 경우에도 frame을 queue에 넣지 않고 예약을 반환하는 경계를 확인한다.

`TestDynamicRaftResetUnblocksActiveCapacityWait`는 reader가 실제 공간 대기 select에 진입한 뒤 `ResetPeer`를 호출해 deadline을 기다리지 않고 종료하며 기존 staged count·bytes를 훼손하지 않는지 확인한다. `ResetPeer`는 연결을 무효화하는 같은 mutex 경계에서 capacity wake를 signal한다.

`TestDynamicPeerReconcileAndQueueBounds`는 outbound 포화 뒤에도 연결에서 byte를 교환할 수 있고 identity rebind에서는 연결이 닫히는지 확인한다. Reconciliation atomically swaps the committed peer-ID/address/certificate registry. A committed phase or operation-ID update for an unchanged immutable identity closes and re-authenticates the connection, preserves queued outbound Raft messages, discards inbound staging from the prior identity revision, and rejects read-loop frames captured under that revision. The v2 adapter exposes staged frames through a coalesced `InboundReady` signal and a `TakeInbound` lease; the cluster driver calls `Complete` after `Step`. Reconciliation blocks new leases, waits for any active lease to complete, then advances the identity revision and discards remaining old-revision staging before returning. An immutable-identity rebind or peer removal retires the peer and drains both queue accounting. Unknown peers are rejected. The 5-second per-frame I/O timeout and 100-ms retry interval are retained for v2; neither queue cap nor Raft entry inflight cap limits retained log size.

#### Delivery forwarding stream

The delivery-forwarding stream is additive on the same TLS 1.3 mTLS listener. Its authenticated hello has the same fields and validation as the `NMQRAFT2` hello above, except that the exact 8-byte magic is `NMQFWRD2`. The same committed identity, cluster/release/schema, phase and operation-ID checks apply. Only `LEARNER`, `CATCHING_UP`, `VOTER` and `LEAVING` are accepted. The existing dial rule applies: the lower member ID accepts and the higher member ID dials. The acceptor responds with its own `NMQFWRD2` hello; neither endpoint dispatches data frames before both hellos validate.

After the hello, frames use `uint32 big-endian payload length | payload`; the prefix is not included in the payload limit. Payload length is `1..1,179,648` bytes. While waiting for the 4-byte length prefix, the reader has no idle deadline. After reading and validating the prefix it applies the 5-second I/O deadline to the payload. Zero, oversized or truncated frames close the stream.

The v1 forwarded-delivery payload is encoded in this exact order:

| Field | Encoding | Limit |
| --- | --- | --- |
| Format version | `uint8` | exactly `1` |
| Session ID | `uint16` byte length + opaque routing-ID bytes (may be non-UTF-8) | `1..65,535` |
| Topic | `uint16` byte length + UTF-8 bytes | `1..65,535` |
| Flags | `uint8` | bit 0 is Retain; all other bits are zero |
| Expiry | `uint64` Unix milliseconds | `0` means no expiry |
| Payload | `uint32` byte length + bytes | total payload frame at most `1,179,648` bytes |

All integers are unsigned big-endian. The session ID is an opaque internal routing identifier and is not decoded as UTF-8. Topic text must be valid UTF-8. Payload bytes are binary. No trailing bytes are allowed. A decoder rejects unsupported versions, invalid topic UTF-8, empty/oversized strings, unknown flag bits, truncated or inconsistent lengths, trailing bytes and a payload over the total frame bound. Decoding owns a copy of the payload bytes.

Each peer has independent FIFO outbound and inbound-staging forwarding queues in each direction, each bounded to 1,024 frames and 8 MiB of framed bytes. A full forward queue rejects the delivery without resetting the Raft stream or the forwarding stream. Raft queues retain their separate existing 32-frame/4-MiB cap and full-queue behavior. Forward frames are delivered only to the forwarding receiver and never reach Raft `Step`. Receiver application may drop a frame whose committed local ownership/session registration no longer matches; there is no acknowledgement or retry.

The forwarding stream does not alter `NMQRAFT2` hello or Raft message frame bytes, membership records, snapshots, backups or cluster protocol `2.0`.

### 1.6 Replicated records and snapshot

`CommandEnvelope` uses protobuf `proto3` schema major/minor and exactly one payload. It supports `MembershipRecord`, `OwnershipChange`, `SubscriptionChange`, `DeliveryChange`, `RetainedChange`, `OfflineChange`, `MemberProfileChange`, `FeatureActivation` and `SessionMetadataChange`. `MemberProfileChange` carries `member_id`, canonical `release_version` and sorted `features`; it updates only mutable build profile fields. `FeatureActivation` carries an opaque feature token and is applied only when every non-retired member advertises it. `MemberIdentity` carries the committed release/features profile; `ReplicatedStateSnapshot.active_features` is a sorted feature set. Session metadata behavior and invariants are defined in [ADR-0025](../decisions/ADR-0025-replicated-session-metadata-and-owner-loss-recovery.md) and [Cluster §4.3](../specs/cluster-session-and-delivery.md#43-session-metadata-복제와-owner-손실-복구); acceptance mapping and execution evidence are in [Cluster §11.24](../specs/cluster-session-and-delivery.md#1124-session-metadata-복제와-owner-손실-복구-수용-매핑).
`OwnershipChange.session` is optional and carries the session metadata for a CONNECT acquisition or transfer. When present, it is valid only with `ACQUIRE` or `TRANSFER`; ownership and its new `CONNECTED` metadata record apply atomically in the same committed command, replacing any prior record and pending Will. It MUST be omitted when `session-metadata` is inactive. `SessionMetadataChange` carries the fenced `DETACH`, `CLAIM_WILL` or `DISCARD` transition; the feature MUST be active for metadata-bearing proposals and records. Every build advertises `session-metadata`, but no metadata payload is proposed or stored until its committed `FeatureActivation`; inactive clusters retain existing behavior and wire bytes.

Each snapshot `membership_history` record stores the exact `committed_index` of its committed transition. The command payload must leave that field zero because the Raft entry index is unknown when proposed; the application records the actual index while applying the committed entry. Snapshot validation requires `0 < committed_index <= last_included_index`. Restored operation status reports the latest transition's persisted index, never the snapshot boundary.

Snapshot의 `lastIncludedIndex`는 Raft가 적용한 마지막 index와 일치해야 한다. 리더 선출로 생긴 빈 Raft entry처럼 application event가 없는 commit도 typed state의 적용 index를 전진시키되 member·operation 내용은 바꾸지 않는다. 그래야 learner 추가 직후 저장한 snapshot의 `ConfState`와 typed snapshot index가 같고, 새 learner가 해당 snapshot에서 복구할 수 있다.

`DeliverySessionState` is the replicated per-session aggregate keyed by `(tenantId, clientId)`. It contains the monotonically increasing `next_sequence`, ordered `DeliveryMessage` entries and packet-ID-ordered `InboundMessage` entries. A delivery message stores topic, binary payload, effective QoS, retain, expiry, packet ID and delivery phase; packet ID zero denotes `QUEUED`. Inbound state stores the publisher-side QoS 2 payload before PUBREL and a payload-free tombstone after RELEASE. ATTACH/DETACH/DISCARD and SEND/ADVANCE/COMPLETE commands mutate this aggregate using exact owner/epoch fencing. Stale commands consume their committed ordering position but do not mutate the aggregate.

`SnapshotEnvelope` contains schema major/minor, `lastIncludedIndex`, `objectCount`, SHA-256 and typed `ReplicatedStateSnapshot`. `objectCount` counts repeated entries across members, their feature values, ownerships, subscriptions, delivery sessions and their messages/inbound entries, membership history, retained values, offline sessions and their queued messages, active feature values, and session metadata records. Digest input is the deterministic protobuf serialization of `ReplicatedStateSnapshot` without the envelope. Any unknown field, unknown required enum/action, duplicate singular field, duplicate oneof, malformed UTF-8, noncanonical byte sequence, unsupported version, count mismatch or digest mismatch fails apply/recovery/readiness closed. Release compatibility does not relax this strict unknown-field codec.

### 1.7 Limits, ordering and canonical bytes

| Scope | Limit / rule |
| --- | --- |
| `CommandEnvelope` | maximum 1 MiB serialized bytes |
| `SnapshotEnvelope` | maximum 64 MiB serialized bytes |
| Cluster ID | 1..200 UTF-8 bytes |
| Address | 1..255 UTF-8 bytes, valid host:port |
| DNS SAN | 1..253 ASCII DNS bytes |
| release version | canonical `MAJOR.MINOR.PATCH`, each component a no-leading-zero `uint32` decimal; at most 64 ASCII bytes |
| idempotency key | 1..128 printable ASCII bytes, no spaces |
| tenant ID | 1..128 UTF-8 bytes |
| client ID | 1..256 UTF-8 bytes |
| topic/filter | 1..65,535 valid MQTT UTF-8 bytes; filter validated by MQTT filter rules |
| certificate fingerprint | exactly 32 bytes in protobuf; lowercase 64-hex at HTTP JSON boundary |
| Durable delivery QoS and packet ID | `DeliveryMessage.qos` is 1 or 2; packet ID is zero exactly for `QUEUED` and nonzero for inflight; phase must agree with QoS |
| Inbound QoS 2 state | packet ID is `1..65535`; unreleased entries retain payload, released tombstones have empty payload and a valid `released_index` |
| Subscription QoS / retain handling | QoS `0..2`; retain handling `0..2` |
| Durable delivery limits | at most 64 messages and 64 inbound entries per session; at most 4,096 entries and 16 MiB across the cluster; at most 1,024 targets per publish |
| Message expiry | unsigned Unix milliseconds; `0` means no expiry |
| MQTT publish payload | at most 1,000,000 bytes and enclosing command still <=1 MiB |
| Snapshot entries | at most 1,000,000 overall, including nested delivery messages/inbound entries and offline messages; at most 4,096 session metadata records; 64 MiB serialized snapshot bound applies first |
| Session metadata state | At most 8 MiB combined Will and principal bytes across the cluster, where each record contributes topic + payload + principal subject + credential ID + role bytes; no tenant storage quota charge |
| Session metadata Will | Valid MQTT topic of `1..65,535` UTF-8 bytes, payload at most 65,535 bytes, QoS `0..2`; principal only with a Will |
| Session metadata principal | Subject and credential ID each valid UTF-8 `1..1,024` bytes; expiry positive; at most 64 unique roles, each valid UTF-8 `1..256` bytes |
| Session metadata snapshot key | Records sorted and unique by bytewise UTF-8 `(tenant_id, client_id)`; each record has an ownership row and `record.epoch <= ownership.epoch`; any record requires active `session-metadata` |

Deterministic protobuf serialization is mandatory. Proto3 default-valued scalar fields are omitted; fields use ascending field-number order. Repeated `members` sort by `member_id`; each member's `features` sort lexically with no duplicates; `active_features` sort lexically with no duplicates; `ownerships` by bytewise UTF-8 `(tenant_id, client_id)`; `subscriptions` by bytewise `(tenant_id, client_id, filter)`; `delivery_sessions` by bytewise `(tenant_id, client_id)`; `membership_history` by bytewise `operation_id`; each delivery session's `messages` sort by ascending sequence and `inbound` by ascending nonzero packet ID. Duplicate canonical keys are invalid. A consumer verifies canonical bytes by parsing, enforcing duplicate/unknown-field rejection, deterministic re-serialization and byte equality. Removed field numbers and enum values are reserved permanently.

The following reference vectors use schema `1.0`, proto3 deterministic field ordering, omitted scalar defaults, and the fixture values stated in each name. Hex bytes are grouped only for readability; concatenation is the exact encoding. They are contract fixtures, not yet producer/consumer test evidence.
| Fixture | Semantic values |
| --- | --- |
| `stage_join` | operation `00000000-0000-4000-8000-000000000004`; key `k1`; member 4; cluster `c`; address `n:1`; DNS `n`; 32 bytes `0x11`; release `1.0.0`; protocol `2.0`; state schema `1.0` |
| `ownership_acquire` | operation `00000000-0000-4000-8000-000000000001`; tenant `t`; client `c`; expected epoch 0; owner 1; epoch 1; action `ACQUIRE` |
| `subscription_add` / `subscription_remove` | same operation/tenant/client/owner/epoch as ownership; filter `#`; QoS 1; actions 1/2 |
| `delivery_publish` | operation/tenant/client as ownership; `PUBLISH`, proposed time 1, topic `a`, payload `x`, publish QoS 1 |
| `empty_snapshot` | envelope schema `1.0`, index 1, object count 0, empty typed state; state digest is SHA-256 of empty bytes |

```text
stage_join (187 bytes)
08 01 1A 24 30 30 30 30 30 30 30 30 2D 30 30 30
30 2D 34 30 30 30 2D 38 30 30 30 2D 30 30 30 30
30 30 30 30 30 30 30 34 52 90 01 0A 24 30 30 30
30 30 30 30 30 2D 30 30 30 30 2D 34 30 30 30 2D
38 30 30 30 2D 30 30 30 30 30 30 30 30 30 30 30
34 10 01 1A 02 6B 31 22 62 08 04 12 01 63 1A 03
6E 3A 31 22 01 6E 2A 20 11 11 11 11 11 11 11 11
11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11
11 11 11 11 11 11 11 11 32 05 31 2E 30 2E 30 38
02 48 01 58 01 62 24 30 30 30 30 30 30 30 30 2D
30 30 30 30 2D 34 30 30 30 2D 38 30 30 30 2D 30
30 30 30 30 30 30 30 30 30 30 34

ownership_acquire (54 bytes)
08 01 1A 24 30 30 30 30 30 30 30 30 2D 30 30 30
30 2D 34 30 30 30 2D 38 30 30 30 2D 30 30 30 30
30 30 30 30 30 30 30 31 5A 0C 0A 01 74 12 01 63
20 01 28 01 30 01

subscription_add (59 bytes)
08 01 1A 24 30 30 30 30 30 30 30 30 2D 30 30 30
30 2D 34 30 30 30 2D 38 30 30 30 2D 30 30 30 30
30 30 30 30 30 30 30 31 62 11 0A 01 74 12 01 63
18 01 20 01 2A 01 23 30 01 50 01

subscription_remove (59 bytes)
08 01 1A 24 30 30 30 30 30 30 30 30 2D 30 30 30
30 2D 34 30 30 30 2D 38 30 30 30 2D 30 30 30 30
30 30 30 30 30 30 30 31 62 11 0A 01 74 12 01 63
18 01 20 01 2A 01 23 30 01 50 02

delivery_publish (62 bytes)
08 01 1A 24 30 30 30 30 30 30 30 30 2D 30 30 30 30
2D 34 30 30 30 2D 38 30 30 30 2D 30 30 30 30 30 30
30 30 30 30 30 31 82 01 13 0A 01 74 12 01 63 28 01
30 01 3A 01 61 42 01 78 80 01 01


empty_snapshot (40 bytes)
08 01 18 01 2A 20 E3 B0 C4 42 98 FC 1C 14 9A FB
F4 C8 99 6F B9 24 27 AE 41 E4 64 9B 93 4C A4 95
99 1B 78 52 B8 55 32 00
```

## 2. Errors

| Error code | Condition | Retryable | State change |
| --- | --- | --- | --- |
| `invalid_request` | malformed or noncanonical management input | no | none |
| `unauthorized` / `forbidden` | management authentication/role failure | no | none |
| `member_id_reserved` | active, staged or retired ID | no | none |
| `member_identity_conflict` | ID bound to other identity | no | none |
| `idempotency_conflict` | key reused with another body | no | none |
| `membership_change_in_progress` | another transition is active | yes after existing transition ends | none |
| `not_leader` | mutation reaches non-leader endpoint | yes after leader discovery | none |
| `quorum_unavailable` | stage/phase cannot commit | yes after quorum recovery | no uncommitted record acknowledged |
| `compatibility_unsupported` | release is outside the same-major one-minor window, or protocol/schema is not exact `2.0`/`1.0` | no | none |
| `feature_not_supported_by_all_members` | active feature is not advertised by every non-retired member | no | no activation applied |
| `peer_identity_rejected` | TLS/hello identity mismatch | no until operator fixes identity | no Raft frame reaches `Step` |
| `incompatible_state_schema` | unknown/invalid typed record, schema or snapshot | no | no READY; corrupted state is not rewritten |
| `feature_inactive` | Session metadata payload proposed before committed `session-metadata` activation | no | no metadata or ownership mutation |
| `session_metadata_capacity_exceeded` | Cluster-wide Will/principal bytes exceed 8 MiB | no | no metadata or ownership mutation |
| `session_metadata_invalid` | Session metadata payload or state violates its defined constraints | no | no metadata or ownership mutation |

## 3. Compatibility rules

- Membership v2 never changes `raft-peer-transport-v1` or v1 `NMQRAFT1` bytes.
- `NMQSTATE1` is not a replicated-state v1 encoding and receives no implicit migration or conversion.
- Unknown fields are rejected by the strict replicated-state schema `1.0` codec. Release compatibility does not make unknown fields acceptable. Session metadata wire payloads and snapshot records are used only after `session-metadata` is committed active on every non-retired member; before activation, metadata is neither proposed nor stored, preserving existing inactive-cluster behavior and wire bytes.
- Feature activation is committed only when every non-retired member advertises the feature; an unsupported active feature prevents readiness.
- Field numbers and enum values are never reused. A breaking wire/state change requires a new contract major and explicit migration/recovery plan.
- No v2 peer data is admitted before TLS, handshake identity and committed stage/configuration checks complete.
- The additive `NMQFWRD2` delivery-forwarding stream preserves `NMQRAFT2` bytes and cluster protocol `2.0`.
- `NMQBOOT2` is additive on the same listener and preserves `NMQJOIN2`/`NMQRAFT2` bytes and cluster protocol `2.0`; a peer that does not recognize it closes the socket and the prober keeps waiting.

## 4. Version and support status

| Status | Condition | Replacement |
| --- | --- | --- |
| Draft | Release/profile implementation and mixed-binary acceptance evidence are incomplete | Remains Draft until package, process and local verification gates pass |
| Active | Runtime, OpenAPI and typed state artifacts plus N-1↔N evidence match this contract | None defined |

## 5. Examples and contract tests

The JSON and frame examples in §1 define canonical field semantics. Release parsing/profile updates are tested in `internal/application/membership`, typed codec and snapshot validation in `internal/adapters/raft/records`, peer compatibility in `internal/adapters/tcp`, management producers in `internal/adapters/http/management`, API fixtures in `tests/contracts/management-api-v1`, and multi-process acceptance in `tests/integration/cluster_release_compat_process_test.go`.

## 6. Draft blockers

- Run package, generated-protobuf, mixed-binary process and repository local/architecture gates.
- Define rollback-window operations and rollout-controller preflight separately; preflight is not implemented by this contract change.
- Backup archive restore follows the [ADR-0021](../decisions/ADR-0021-backup-archive-release-window.md) release window; historical-source compatibility remains unverified.
- Same-source synthetic builds do not establish wire compatibility between historical source revisions.
