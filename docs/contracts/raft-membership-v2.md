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
| 관련 시험 | Cluster spec §11 membership mapping; 현재 구현·시험 미완료 |

## 1. Membership identity와 version negotiation

### 1.1 Committed member identity

A member record는 다음 값을 하나의 identity로 commit한다.

| 필드 | 자료형 | 필수 제약 |
| --- | --- | --- |
| `memberId` | positive `uint64` | `0` 금지. 제거된 ID는 tombstone으로 유지하고 재사용 금지 |
| `clusterId` | UTF-8 string | 1~200 bytes; cluster 내 동일 |
| `address` | UTF-8 host:port string | 1~255 bytes; port `1..65535`; wildcard listener 주소 금지 |
| `dnsSan` | DNS name | 1~253 ASCII bytes; IP literal 금지 |
| `certificateSha256` | 32 raw bytes | TLS leaf certificate DER의 SHA-256 |
| `releaseVersion` | UTF-8 string | 1~64 bytes; 빈 값 금지 |
| `clusterProtocolMajor`, `clusterProtocolMinor` | `uint32` | v2 profile은 정확히 `2.0` |
| `stateSchemaMajor`, `stateSchemaMinor` | `uint32` | 초기 Draft profile은 정확히 `1.0` |
| `phase` | enum | `STAGED`, `LEARNER`, `CATCHING_UP`, `VOTER`, `LEAVING`, `RETIRED` |
| `operationId` | lower-case UUID string | 변경 진행 중에는 non-empty; 안정된 기존 voter/learner에는 empty 가능 |

TLS 1.3 mTLS가 CA chain과 EKU를 먼저 검증한다. Peer adapter는 TLS leaf certificate의 DNS SAN과 SHA-256을 committed identity와 대조한다. Peer adapter는 cluster ID, member ID, advertised address, protocol/state version, phase와 operation ID도 committed registry와 대조한다. 불일치, 미지원 version, malformed hello 또는 등록되지 않은 ID는 Raft payload를 `Step`에 전달하기 전에 connection을 닫는다.

Raft frame의 TLS·hello identity는 **직접 보낸 peer**를 인증한다. `etcd/raft`가 `MsgProp`, `MsgReadIndex`, `MsgTransferLeader`를 leader에게 전달할 때 protobuf `From`에는 **원래 요청한 member ID**가 남을 수 있다. 이 세 유형에 한해서만 Raft adapter는 인증된 직접 송신자와 원래 요청자가 모두 현재 committed `ConfState`에 속하는지 확인한 뒤 전달한다. 그 외 Raft message는 protobuf `From`과 인증된 직접 송신자가 반드시 같아야 한다. Transport는 protobuf payload를 수정하지 않는다.

Cluster protocol major는 정확히 `2`, minor는 `0`이어야 한다. State schema major/minor도 현재 Draft에서 정확히 `1.0`만 받는다. `releaseVersion`은 모든 committed member와 bytewise exact match여야 하며 mixed-release 운영을 지원하지 않는다. 더 큰 major·minor 또는 다른 release/schema는 `compatibility_unsupported`로 거절한다. 이 exact match rule은 release N/N-1 compatibility를 약속하지 않는다.

### 1.2 Membership phase와 안전한 순서

`MemberPhase`는 membership register의 committed 상태다. Join phase는 다음 순서로만 진행한다.

```text
STAGED → LEARNER → CATCHING_UP → VOTER
```

- `STAGED`는 identity, certificate binding, release/schema version과 idempotency key가 Raft application log에 commit된 상태다. 아직 Raft voter가 아니다.
- `LEARNER`는 `ConfChangeV2` learner addition이 commit되고 `RawNode.ApplyConfChange`가 적용된 상태다.
- `CATCHING_UP`은 learner가 snapshot과 committed log tail을 적용 중인 상태다.
- `VOTER`는 log catch-up 확인 뒤 별도의 `ConfChangeV2` promotion이 commit된 상태다.
- Planned removal은 `VOTER → LEAVING → RETIRED`다. 모든 session owner transfer와 inbound/outbound checkpoint를 successor 쪽에서 commit하기 전에는 `RETIRED`로 바꾸거나 voter를 제거하지 않는다.
- `ABORT_STAGE`는 learner로 전환하기 전의 stage만 취소한다. 이미 `ConfChangeV2`가 commit된 learner를 heartbeat timeout으로 제거하는 용도로 사용할 수 없다.
- `RETIRED` identity와 ID tombstone은 snapshot에 남는다. ID는 다시 stage할 수 없다.
- 하나의 cluster group에서는 한 membership operation만 진행한다. Local proposal acceptance는 commitment가 아니며 management 성공 응답은 stage 또는 현재 phase의 commit 뒤에만 보낸다.
- Heartbeat timeout은 failure observation일 뿐 remove command가 아니다. Unexpected failure에서 operator가 승인하지 않은 voter-set 변경은 금지한다.

`ConfChangeV2`가 committed Ready entry일 때만 Raft adapter가 `RawNode.ApplyConfChange`를 호출한다. Application membership record와 derived registry는 같은 committed entry order를 소비한다. Recovery는 snapshot plus committed WAL tail에서 ConfState, staged record, phase, operation ID와 tombstone을 재구성한다. Replay로 application index가 기존 typed snapshot index보다 앞서면, peer transport를 시작하기 전에 replay 완료 상태로 새 typed snapshot을 저장한다.

**Planned removal session handoff:** planned removal 진행 중 리더는 대상 member가 소유한 모든 세션에 대해 남은 voter 중 가장 작은 member ID를 successor로 선택하여 `OwnershipActionTransfer` (epoch+1) 명령을 commit한다. Commit된 소유권 이전은 state machine에서 persistent subscription의 ownerNodeID와 epoch를 함께 갱신하며, 탈퇴 대상 노드의 로컬 소켓은 takeover로 안전하게 닫힌다(`CloseSessionForTakeover`). 탈퇴 대상 member에 속한 미완료 checkpoint가 존재하거나(`checkpoint_handoff_unsupported`), successor voter가 없거나(`no_successor_voter`), epoch가 고갈되었거나(`ownership_epoch_exhausted`), 고아 구독이 감지되면(`orphan_subscription`) removal은 fail-closed 원칙에 따라 차단되고 `REMOVING` 상태를 유지한다. 탈퇴 대상 노드는 `LEAVING` 상태 동안 신규 세션 admission을 거절한다.

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

Active voter의 planned leave operation을 시작한다. `memberId`는 canonical positive decimal `uint64`이고, 필수 `Idempotency-Key` 및 operation record 규칙은 POST와 같다. Request body는 없다. Response는 POST와 동일한 operation schema에서 `action="REMOVE"`, 최초 operation `phase="REMOVING"`을 반환한다. Member lifecycle은 `VOTER → LEAVING → RETIRED`다. `REMOVING`은 ownership/checkpoint transfer와 configuration removal을 진행 중인 operation을 뜻하며, `SUCCEEDED` 전에는 voter가 제거된 것으로 간주하지 않는다.

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
| 422 | `compatibility_unsupported` | release, cluster protocol 또는 schema 조합 미지원 | false |
| 503 | `quorum_unavailable` | Fresh quorum barrier/commit이 불가능 | true |
| 500 | `internal_error` | redacted unexpected service/storage error | false |

No-leader/quorum mutation은 어떤 stage/configuration entry도 남기지 않는다. Error envelope의 message는 sanitized stable text이며 internal cause, TLS secret과 payload를 노출하지 않는다.

### 1.4 Combined cluster configuration

`-cluster-config <path>`는 MQTT, management와 dynamic Raft membership을 함께 조립하는 strict JSON mode다. Flag 자체 외의 다른 CLI flag나 positional argument를 허용하지 않으며 기존 `-raft-peer-config <path>`와 상호 배타적이다. 기존 peer-only config의 schema와 disk semantics는 바꾸지 않는다. Config file에 알 수 없는 key, 중복 key, trailing value 또는 missing required field가 있으면 process startup을 거절한다. Relative path는 process current working directory 기준이다.

| Field | JSON type | Required rule |
| --- | --- | --- |
| `mode` | string | exactly `"cluster"` |
| `clusterId` | string | UTF-8 `1..200` bytes |
| `nodeId` | uint64 | positive; must match local cert-bound identity |
| `directory` | string | persistent, empty only for initial bootstrap or new joiner |
| `listenAddress` | string | local peer TCP bind `host:port` |
| `advertisedAddress` | string | reachable peer `host:port`; never wildcard |
| `localDnsName` | string | certificate DNS SAN and committed local identity |
| `releaseVersion` | string | `1..64` UTF-8 bytes |
| `clusterProtocolMajor`, `clusterProtocolMinor` | uint32 | exactly `2`, `0` |
| `stateSchemaMajor`, `stateSchemaMinor` | uint32 | exactly `1`, `0` for this Draft |
| `role` | string | exactly `"bootstrap"` or `"joiner"` |
| `bootstrapMembers` | array of member objects | exactly 3 unique identities for bootstrap; empty for joiner |
| `seedAddresses` | array of strings | empty for bootstrap; `1..16` unique canonical DNS `host:port` addresses for joiner; IP literals are rejected so TLS SNI is bound to a DNS name |
| `peerTLS` | object | required `certFile`, `keyFile`, `caFile`; cert/key match and CA must verify all peers |
| `management` | object | required `listenAddress`, `clusterAdminTokenFile`; token is opaque secret input |
| `mqtt` | object | required `tcpListenAddress`, `tlsListenAddress`, `securityConfigFile`; empty address disables that listener |

Each `bootstrapMembers` element has exactly `{memberId:uint64,address:string,dnsSan:string,certificateSha256:string}` with the same identity constraints as §1.1. All three bootstrap configs use bytewise-canonical sorting by `memberId` and the same set. A member's own certificate fingerprint and SAN must match its entry. `seedAddresses` address the peer admission listener, not an arbitrary discovery authority; each host is a lowercase DNS name used for the TLS server name and must match the contacted seed certificate SAN. ClusterAdmin token contents are read from the referenced file; the raw token is not part of JSON or logs.

Bootstrap example (`<64 lowercase hex>` is a configuration value supplied from the certificate fingerprint, not a literal accepted value):
```json
{
  "mode": "cluster",
  "clusterId": "sample-cluster",
  "nodeId": 1,
  "directory": "data/node1",
  "listenAddress": "127.0.0.1:19001",
  "advertisedAddress": "127.0.0.1:19001",
  "localDnsName": "node1.example",
  "releaseVersion": "1.0.0",
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

Bootstrap requires `role="bootstrap"`, an empty data directory and exactly three bootstrap identities including the local ID. All initial node configs must carry the same `clusterId` and identical canonical bootstrap identity set. Bootstrap must not overwrite any non-empty directory.

Joiner requires `role="joiner"`, one or more configured canonical DNS `seedAddresses` (`host:port`, never an IP literal), an already-issued positive `nodeId`, empty `bootstrapMembers`, its local `advertisedAddress`/DNS SAN/certificate fingerprint, and a new/empty directory. It must not call `RawNode.Bootstrap`; it first performs seed admission and proves an exactly matching committed `STAGED` record. `seedAddresses` are discovery/bootstrap contacts, not an authority to add peers.

All modes require `peerTLS` cert/key/CA, local certificate DNS SAN and fingerprint equality, explicit `management.clusterAdminTokenFile`, release and schema versions, persistent `directory`, and MQTT TLS/security configuration if an MQTT listener is enabled. Admin token contents are read as a secret file and never logged or returned. Standalone runtime does not load this mode and is unchanged.

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
The joiner repeats the idempotent admission request while its response is `STAGED`; it must not open a Raft stream in that phase. Once a seed reports a committed `LEARNER`, `CATCHING_UP`, or `VOTER` identity, the joiner applies that identity to its local transport and reconciles the three incumbent peers. This lets the joiner learn the committed configuration/log without accepting a Raft hello for an uncommitted learner. A seed admission response is phase-aware but does not itself commit membership.

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

After the v2 hello, Raft frames use the same envelope shape as v1: `uint32 big-endian payload length | serialized Raft protobuf`. Payload is `1..1,048,576` bytes. Zero, oversized or truncated frames close the connection before payload allocation/dispatch. The payload's protobuf `From` and `To` must match authenticated transport identity and local node ID.

Each active peer retains v1's independently bounded queue policy: outbound FIFO and inbound staging each at most 32 messages and 4 MiB framed bytes; an in-progress outbound write counts. A full queue rejects immediately and resets/reports only that peer. Reconciliation atomically swaps the committed peer-ID/address/certificate registry. A committed phase or operation-ID update for an unchanged immutable identity closes and re-authenticates the connection, preserves queued outbound Raft messages, discards inbound staging from the prior identity revision, and rejects read-loop frames captured under that revision. The v2 adapter exposes staged frames through a coalesced `InboundReady` signal and a `TakeInbound` lease; the cluster driver calls `Complete` after `Step`. Reconciliation blocks new leases, waits for any active lease to complete, then advances the identity revision and discards remaining old-revision staging before returning. An immutable-identity rebind or peer removal retires the peer and drains both queue accounting. Unknown peers are rejected. The 5-second per-frame I/O timeout and 100-ms retry interval are retained for v2; neither queue cap nor Raft entry inflight cap limits retained log size.

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

`CommandEnvelope` uses protobuf `proto3` schema major/minor and exactly one payload: `MembershipRecord`, `OwnershipChange`, `SubscriptionChange` or `SessionCheckpoint`. `MembershipRecord` stores operation ID, action, persisted idempotency key, member identity, optional removal target, and `expected_conf_state_index` for compare-before-change; HTTP `committedIndex` is derived from the actual applied Raft entry index. `OwnershipChange` carries `(tenantId, clientId, expectedEpoch, ownerNodeId, epoch, action)`. `SubscriptionChange` carries `(tenantId, clientId, ownerNodeId, epoch, filter, qos, noLocal, retainAsPublished, retainHandling, add/remove)`. `SessionCheckpoint` contains typed inbound/outbound publish packet plus protocol delivery phase, not opaque Go bytes.

Each snapshot `membership_history` record stores the exact `committed_index` of its committed transition. The command payload must leave that field zero because the Raft entry index is unknown when proposed; the application records the actual index while applying the committed entry. Snapshot validation requires `0 < committed_index <= last_included_index`. Restored operation status reports the latest transition's persisted index, never the snapshot boundary.

Snapshot의 `lastIncludedIndex`는 Raft가 적용한 마지막 index와 일치해야 한다. 리더 선출로 생긴 빈 Raft entry처럼 application event가 없는 commit도 typed state의 적용 index를 전진시키되 member·operation 내용은 바꾸지 않는다. 그래야 learner 추가 직후 저장한 snapshot의 `ConfState`와 typed snapshot index가 같고, 새 learner가 해당 snapshot에서 복구할 수 있다.

`PublishPacket` is the exact current `mqtt.Publish` domain subset: MQTT protocol version (`4` for 3.1.1, `5` for 5.0), DUP, QoS, retain, packet ID, topic, binary payload and optional message-expiry interval. QoS is `0..2`; packet ID is zero exactly for QoS 0 and nonzero for QoS 1/2. When `has_message_expiry_interval=false`, the stored interval is zero; otherwise it is seconds. `InboundCheckpoint` and `OutboundCheckpoint` map to `delivery.Inbound` and `delivery.Outbound` phases and accept only QoS 1/2 entries whose packet ID, direction and phase are consistent. The ownership/subscription command semantics follow `takeover.StateMachine`: committed ordering advances even when a syntactically valid stale epoch is rejected; mutation itself applies only when exact `(ownerNodeId, epoch)` matches.

`SnapshotEnvelope` contains schema major/minor, `lastIncludedIndex`, `objectCount`, SHA-256 and typed `ReplicatedStateSnapshot`. `objectCount` is the total number of top-level repeated entries across members (including `RETIRED` tombstones), ownerships, subscriptions, session checkpoints, and membership-history records; a checkpoint is one object regardless of its number of inflight entries. Digest input is the deterministic protobuf serialization of `ReplicatedStateSnapshot` without the envelope. Any unknown field, unknown required enum/action, duplicate singular field, duplicate oneof, malformed UTF-8, noncanonical byte sequence, unsupported version, count mismatch or digest mismatch fails apply/recovery/readiness closed.

### 1.7 Limits, ordering and canonical bytes

| Scope | Limit / rule |
| --- | --- |
| `CommandEnvelope` | maximum 1 MiB serialized bytes |
| `SnapshotEnvelope` | maximum 64 MiB serialized bytes |
| Cluster ID | 1..200 UTF-8 bytes |
| Address | 1..255 UTF-8 bytes, valid host:port |
| DNS SAN | 1..253 ASCII DNS bytes |
| release version | 1..64 UTF-8 bytes |
| idempotency key | 1..128 printable ASCII bytes, no spaces |
| tenant ID | 1..128 UTF-8 bytes |
| client ID | 1..256 UTF-8 bytes |
| topic/filter | 1..65,535 valid MQTT UTF-8 bytes; filter validated by MQTT filter rules |
| certificate fingerprint | exactly 32 bytes in protobuf; lowercase 64-hex at HTTP JSON boundary |
| `PublishPacket.protocol_version` | exactly `4` (MQTT 3.1.1) or `5` (MQTT 5.0) |
| QoS and packet ID | QoS `0..2`; packet ID zero iff QoS 0 |
| Subscription QoS / retain handling | QoS `0..2`; retain handling `0..2` |
| Checkpoint | only QoS 1/2; phase must be valid for packet direction and QoS |
| Message expiry | uint32 seconds; absent flag requires zero interval |
| MQTT publish payload | at most 1,000,000 bytes and enclosing command still <=1 MiB |
| top-level snapshot entries | at most 1,000,000 across all repeated fields; 64 MiB serialized snapshot bound applies first |

Deterministic protobuf serialization is mandatory. Proto3 default-valued scalar fields are omitted; fields use ascending field-number order. Repeated `members` sort by `member_id`; `ownerships` by bytewise UTF-8 `(tenant_id, client_id)`; `subscriptions` by bytewise `(tenant_id, client_id, filter)`; `checkpoints` by bytewise `(tenant_id, client_id)`; `membership_history` by bytewise `operation_id`; each checkpoint's `inbound` and `outbound` sort independently by ascending nonzero packet ID. Duplicate canonical keys are invalid. A consumer verifies canonical bytes by parsing, enforcing duplicate/unknown-field rejection, deterministic re-serialization and byte equality. Removed field numbers and enum values are reserved permanently.

The following reference vectors use schema `1.0`, proto3 deterministic field ordering, omitted scalar defaults, and the fixture values stated in each name. Hex bytes are grouped only for readability; concatenation is the exact encoding. They are contract fixtures, not yet producer/consumer test evidence.
| Fixture | Semantic values |
| --- | --- |
| `stage_join` | operation `00000000-0000-4000-8000-000000000004`; key `k1`; member 4; cluster `c`; address `n:1`; DNS `n`; 32 bytes `0x11`; release `v1`; protocol `2.0`; state schema `1.0` |
| `ownership_acquire` | operation `00000000-0000-4000-8000-000000000001`; tenant `t`; client `c`; expected epoch 0; owner 1; epoch 1; action `ACQUIRE` |
| `subscription_add` / `subscription_remove` | same operation/tenant/client/owner/epoch as ownership; filter `#`; QoS 1; actions 1/2 |
| `checkpoint` | operation/tenant/client/owner/epoch as above; MQTT 5 PUBLISH QoS 1, packet 7, topic `a`, payload `x`; inbound QoS1-await-acceptance and outbound awaiting-PUBACK |
| `empty_snapshot` | envelope schema `1.0`, index 1, object count 0, empty typed state; state digest is SHA-256 of empty bytes |

```text
stage_join (184 bytes)
08 01 1A 24 30 30 30 30 30 30 30 30 2D 30 30 30
30 2D 34 30 30 30 2D 38 30 30 30 2D 30 30 30 30
30 30 30 30 30 30 30 34 52 8D 01 0A 24 30 30 30
30 30 30 30 30 2D 30 30 30 30 2D 34 30 30 30 2D
38 30 30 30 2D 30 30 30 30 30 30 30 30 30 30 30
34 10 01 1A 02 6B 31 22 5F 08 04 12 01 63 1A 03
6E 3A 31 22 01 6E 2A 20 11 11 11 11 11 11 11 11
11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11
11 11 11 11 11 11 11 11 32 02 76 31 38 02 48 01
58 01 62 24 30 30 30 30 30 30 30 30 2D 30 30 30
30 2D 34 30 30 30 2D 38 30 30 30 2D 30 30 30 30
30 30 30 30 30 30 30 34

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

checkpoint (88 bytes)
08 01 1A 24 30 30 30 30 30 30 30 30 2D 30 30 30
30 2D 34 30 30 30 2D 38 30 30 30 2D 30 30 30 30
30 30 30 30 30 30 30 31 6A 2E 0A 01 74 12 01 63
18 01 20 01 2A 10 0A 0C 08 05 18 01 28 07 32 01
61 3A 01 78 10 01 32 10 0A 0C 08 05 18 01 28 07
32 01 61 3A 01 78 10 01

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
| `compatibility_unsupported` | release/protocol/schema not exactly supported | no | none |
| `peer_identity_rejected` | TLS/hello identity mismatch | no until operator fixes identity | no Raft frame reaches `Step` |
| `incompatible_state_schema` | unknown/invalid typed record, schema or snapshot | no | no READY; corrupted state is not rewritten |

## 3. Compatibility rules

- Membership v2 never changes `raft-peer-transport-v1` or v1 `NMQRAFT1` bytes.
- `NMQSTATE1` is not a replicated-state v1 encoding and receives no implicit migration or conversion.
- Unknown fields are rejected in Draft schema v1; no mixed release or N/N-1 interoperability is claimed.
- Field numbers and enum values are never reused. A breaking wire/state change requires a new contract major and explicit migration/recovery plan.
- No v2 peer data is admitted before TLS, handshake identity and committed stage/configuration checks complete.
- The additive `NMQFWRD2` delivery-forwarding stream preserves `NMQRAFT2` bytes and cluster protocol `2.0`; it does not claim mixed-release or N/N-1 interoperability.

## 4. Version and support status

| Status | Condition | Replacement |
| --- | --- | --- |
| Draft | Design approved; implementation and membership/restart/readiness tests incomplete | Remains Draft until all producer/consumer contract tests and cluster acceptance evidence pass |
| Active | Exact runtime, OpenAPI and typed state artifacts plus evidence match this contract | None defined |

## 5. Examples and contract tests

The JSON and frame examples in §1 define canonical field semantics, but are not runtime evidence. The Draft has no producer/consumer contract tests yet. Planned locations: v2 hello and queue tests under `internal/adapters/tcp`, typed state codec tests under `internal/adapters/raft`, management producers under `internal/adapters/http/management`, API consumer tests under `tests/contracts/management-api-v1`, and multi-process acceptance under `tests/integration`.

## 6. Draft blockers

- Implement v2 peer admission and atomically reconciled dynamic peer registry without changing v1.
- Implement committed membership application, crash/restart replay, typed state codec, snapshot checksum and generated or reviewed wire vectors.
- Implement management mutations/status, strict configuration loader and producer/consumer API tests.
- Run actual TLS 3-process replacement, lost-response retry, transition-boundary crash/restart, MQTT route/readiness and quorum-loss tests.
- Define an actual release compatibility matrix only after N and N-1 runtime artifacts exist; this Draft does not claim one.
