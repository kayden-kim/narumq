# Cluster session과 durable delivery 스펙

이 문서는 여러 NaruMQ node가 membership, session ownership, routing과 durable state를 외부에서 일관되게 제공하는 동작을 정의한다. Storage와 consensus target은 ADR에서 결정하며, target 선택만으로 runtime 수용이 완료되지는 않는다.

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 관련 요구사항 | CLU-01~CLU-14, NFR-07~NFR-10, NFR-15 |
| 근거 | [PRD 8장, 9.2절과 11.2절](../requirements/PRD.md) |
| 관련 Architecture | [Cluster architecture](../architectures/cluster-architecture.md), [EKS on EC2 deployment](../architectures/eks-ec2-deployment.md) |
| 관련 결정 | [ADR-0001](../decisions/ADR-0001-core-implementation-language.md), [ADR-0002](../decisions/ADR-0002-cluster-consensus-library.md), [ADR-0003](../decisions/ADR-0003-durable-ack-barrier.md), [ADR-0004](../decisions/ADR-0004-session-ownership-and-fencing.md), [ADR-0007](../decisions/ADR-0007-aws-production-deployment-platform.md), [ADR-0008](../decisions/ADR-0008-raft-peer-transport.md), [ADR-0009](../decisions/ADR-0009-raft-production-state-profile.md), [ADR-0011](../decisions/ADR-0011-cluster-backup-restore-v1.md) (historical v1 boundary), [ADR-0012](../decisions/ADR-0012-retained-replication-and-cluster-backup-v2.md) (retained scope), [ADR-0013](../decisions/ADR-0013-offline-queue-replication-and-cluster-backup-v3.md), [ADR-0014](../decisions/ADR-0014-cross-node-live-qos0-forwarding.md), [ADR-0015](../decisions/ADR-0015-cluster-durable-qos-delivery.md) (cluster QoS 1/2 durable delivery scope), [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md) (TEN-08 storage quota scope), [ADR-0018](../decisions/ADR-0018-release-compatibility-window.md) (N/N-1 release profile and feature activation), [ADR-0019](../decisions/ADR-0019-untargeted-durable-publish-quorum-barrier.md) (target 0 command 생략 범위), [ADR-0027](../decisions/ADR-0027-detach-forward-and-owner-crash-qos0-boundary.md), [ADR-0060](../decisions/ADR-0060-shared-subscription-durable-owner-loss-reroute.md) (공유 구독 durable 전달의 owner 손실) |
| 관련 Contract | [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md), [Raft peer transport v1](../contracts/raft-peer-transport-v1.md), Draft [Cluster backup v3](../contracts/cluster-backup-v3.md), Superseded [Cluster backup v2](../contracts/cluster-backup-v2.md), Superseded [Cluster backup v1](../contracts/cluster-backup-v1.md) |
| 해설 | [세션 소유권과 fencing](../explainers/session-ownership-and-fencing.md), [EKS 고가용성과 장애 도메인](../explainers/eks-ha-failure-domains.md) |

## 1. Cluster membership

1. 기준 배포는 quorum에 참여하는 node 3개로 구성해야 한다.
2. node는 `JOINING` 중 discovery, member 인증, log/snapshot 동기화와 committed persistent subscription에서 route projection을 rebuild하는 작업을 마쳐야 한다.
3. 초기 동기화와 route projection rebuild가 끝나고 quorum을 확인하기 전에는 MQTT traffic readiness를 성공으로 반환해서는 안 된다.
4. join, 정상 leave와 재시작은 수동 데이터 수정 없이 재시도할 수 있어야 한다.
5. 비인가 member와 지원하지 않는 cluster protocol version은 membership에 추가해서는 안 된다.
6. data directory가 없거나 빈 bootstrap voter는 다른 bootstrap voter 모두에게 진행 여부를 확인한 뒤에만 Raft를 bootstrap한다. 어느 voter라도 term 1·commit 3을 넘었고 이 voter를 함께 시작한 peer로 허용하지 않으면 시작하지 않고 `cluster bootstrap refused` 오류로 종료한다. 확인 중에는 management·MQTT listener를 열지 않는다.
7. 승격된 joiner voter가 영속 기록을 잃으면 같은 ID를 seed admission으로 다시 참여시키지 않는다. Seed runtime은 committed VOTER 후보를 거절하며 fresh STAGED client는 이전 seed의 VOTER 응답도 설치하지 않는다. 정상 same-volume 재시작은 persisted committed local identity로 재개한다([ADR-0024](../decisions/ADR-0024-joiner-voter-volume-loss-admission.md)).

외부 cluster 조회는 최소한 node별 `nodeId`, lifecycle state, release version, member role, readiness, replication health와 마지막 오류를 제공해야 한다. `replicationHealth`는 조회 node 자신에 대해서는 local Raft storage 상태를, 원격 peer 행에 대해서는 조회 node에서 관찰한 peer 연결 상태를 나타내며 원격 peer의 storage 상태를 뜻하지 않는다. `lastErrorCode`는 내부 오류 원문 대신 안정된 오류 code를 제공한다. `replicationQueue`는 조회 node에서 해당 원격 peer로 보내는 outbound 대기열을 나타낸다. 정확한 field 형식은 [Management API v1 Contract](../contracts/management-api-v1.md)과 OpenAPI 문서를 따른다.

## 2. Session identity와 ownership

session key는 `(tenantId, clientId)`다. 각 key에는 한 시점에 쓰기 권한을 가진 owner가 최대 하나만 존재해야 한다.

```mermaid
stateDiagram-v2
    [*] --> Unowned
    Unowned --> Acquiring: "인증된 CONNECT"
    Acquiring --> Owned: "quorum commit + 새 epoch"
    Acquiring --> Unowned: "quorum/권한 실패"
    Owned --> Transferring: "새 CONNECT 또는 drain"
    Transferring --> Owned: "새 owner + 더 큰 epoch"
    Owned --> Unowned: "session expiry/삭제"
```

ownership record는 최소 `sessionKey`, `ownerNodeId`, 단조 증가 `epoch`, 상태와 `commit identifier`를 가져야 한다. 이 Phase 2 evidence에서 `commit identifier`는 ownership aggregate가 opaque committed-command stream을 소비할 때 부여하는 application committed-command sequence position이다. 이는 consensus adapter의 Raft `StatusDTO.CommitIndex`나 snapshot index와 다르다. [ADR-0009](../decisions/ADR-0009-raft-production-state-profile.md)는 protobuf encoding family만 선택했으며 exact persisted schema는 별도 contract에서 정한다.

Phase 2의 첫 단계인 replicated ownership core는 다음 관찰 계약을 만족해야 한다.
1. 최초 acquire의 `expectedEpoch`는 `0`이어야 하며 성공 시 `epoch`는 `1`이 된다.
2. 이후 모든 ownership 획득 및 reconnect는 현재 `epoch`와 일치하는 `expectedEpoch`를 요구하며 성공 시 `epoch`가 정확히 `1` 단조 증가한다.
3. `epoch`가 `uint64` 최대값에 도달하면 wrap-around하지 않고 mutation을 영구 중단한다.
4. quorum commit 없는 proposal은 어떤 node의 state machine에도 반영되지 않는다.
5. majority takeover 후 이전 `(ownerNodeId, epoch)`의 인가 요청은 거절되어야 한다.
6. restart 및 snapshot replay 후에도 마지막 application committed-command sequence position과 epoch가 정확히 보존된다.
7. 이미 quorum에 commit된 동일 ownership acquire command를 commit 응답 유실 후 재시도하면, retry도 다음 application committed-command sequence position으로 commit되어 `stale_epoch`로 결정적으로 거절되어야 한다. 이때 `ownerNodeId`, `epoch`와 성공한 ownership record의 `commit identifier`는 바뀌지 않아야 한다.

### 2.1 Fencing 규칙

- session aggregate를 바꾸는 acquire 이외의 command의 의미상 envelope 필드는 `sessionKey`, `ownerNodeId`, `epoch`와 operation-specific subscription 또는 inflight mutation으로 한정한다. 이 이름은 필요한 의미 정보이며 serialized field나 byte format을 정하지 않는다.
- committed apply는 `ownerNodeId == currentOwnerNodeId`이고 `epoch == currentEpoch`인 경우에만 acquire 이외의 session mutation을 인가한다.
- ownership record가 없거나 owner가 다르거나 epoch가 다르면 stale/not-authorized mutation으로 결정적으로 거절한다.
- 성공한 `Acquire`만 owner와 epoch를 설치하거나 증가시킬 수 있다. Subscription 또는 inflight mutation은 epoch가 현재 값보다 크더라도 새 owner나 epoch를 설치할 수 없다.
- 문법적으로 유효하지만 stale한 committed mutation은 application committed-command sequence position을 소비하지만 ownership, persistent subscription, QoS inflight와 route projection은 바꾸지 않는다.
- 새 owner는 더 큰 epoch가 quorum commit된 뒤에만 client traffic을 처리해야 한다.
- 이전 owner는 takeover를 관찰한 즉시 read·write와 delivery를 중단하고 connection을 종료해야 한다.
- node가 더 새로운 owner/epoch를 적용하면 이전 owner/epoch로 이미 bind된 연결은 종료하고, 아직 bind하지 않은 이전 epoch의 CONNECT는 session binding과 성공 CONNACK 전에 거절해야 한다.
- 연결 정리와 session bind 전에 거절된 CONNECT 정리는 해당 연결이 획득한 ownership epoch를 사용한다. 현재 local session epoch가 다르면 timer, repository, delivery, Will을 변경하기 전에 정리를 중단한다. epoch가 일치하면 기존 local session 정리 절차를 실행하고, replicated mutation은 committed owner/epoch 검증을 다시 거친다.
- 원격 takeover apply는 이전 node의 application `ownershipEpochs`를 새 epoch로 덮어쓰지 않는다. 이전 연결은 `DisconnectTakeover`로 local lifecycle을 정리하고 Will을 억제하며 집계하지 않는다. 이전 epoch로 제안된 DETACH·DISCARD 등 replicated session mutation은 committed owner/epoch 검증에서 거절된다.
- 인증으로 tenant-scoped session identity가 만들어지기 전 ingress는 raw MQTT Client ID로 pending CONNECT를 묶을 수 있다. committed ownership fence는 group 안에서 tenant별로 분리하므로 같은 raw Client ID를 쓰는 다른 tenant의 CONNECT를 거절하지 않는다.
- timeout만으로 ownership을 추정하거나 임대 만료 전 stale owner write를 허용해서는 안 된다.

CONNECT를 받은 ingress node와 Raft leader가 다를 수 있다. Ingress node는 ownership proposal을 현재 leader까지 전달해야 하며, follower는 인증된 peer transport를 통한 Raft proposal forwarding을 사용할 수 있다. Quorum commit을 확인한 뒤에만 성공 CONNACK을 반환한다. Leader 또는 quorum가 없으면 CONNECT를 거절하고 node-local ownership을 설치해서는 안 된다.

Production error/DTO와 serialized bytes는 [ADR-0009](../decisions/ADR-0009-raft-production-state-profile.md)가 선택한 encoding family에 맞춰 별도 replicated-state contract에서 정한다. ADR-0009는 exact message schema나 wire bytes를 정의하지 않는다.

### 2.2 Session aggregate와 route projection

`sessionKey`마다 ownership record, persistent subscription, inbound QoS inflight와 outbound QoS inflight를 하나의 논리적 replicated session aggregate로 취급한다. 한 `sessionKey`에 대한 command는 같은 committed ordering을 공유하고 하나의 aggregate 경계에서 원자적으로 적용해야 한다. Subscription 또는 inflight 변경 결과를 ownership 권한 판정 결과와 분리해 외부에 노출해서는 안 된다.

Route metadata는 committed persistent subscription에서만 계산하는 node-local disposable projection이다. 이 projection은 authoritative committed subscription state에서 다시 만들 수 있어야 하며, node는 routing readiness를 광고하기 전에 rebuild를 완료해야 한다. Route projection은 ownership 또는 fencing authority로 사용할 수 없다.

WAL·snapshot target, protobuf encoding family와 단일 group profile은 scoped [ADR-0009](../decisions/ADR-0009-raft-production-state-profile.md)에 기록한다. Exact replicated-state message schema는 [replicated-state-v1.proto](../contracts/replicated-state-v1.proto)에 정의한다. N-1 compatibility와 production storage 보장은 검증되지 않았다. Detached persistent session의 QoS 0 offline queue 경계는 §4.1과 [ADR-0013](../decisions/ADR-0013-offline-queue-replication-and-cluster-backup-v3.md)이 정하고, QoS 1/2 durable message·inflight state와 전달 경계는 §4.2와 [ADR-0015](../decisions/ADR-0015-cluster-durable-qos-delivery.md)가 정한다.
### 2.3 Takeover ordering

- Outbound `SEND`, `ADVANCE`, `COMPLETE`는 각각 PUBLISH, PUBREL 또는 packet identifier 해제의 wire effect보다 먼저 commit해야 한다. Successor의 더 큰 epoch `Acquire`가 commit되면 이전 owner의 후속 command는 exact owner/epoch check에서 거절되며, successor는 ATTACH로 committed delivery state를 재개한다. 이 ordering 규칙만으로 drain lifecycle이 충족되지는 않으며, broker drain contract는 [Drain spec](drain-and-rolling-update.md)을 따른다.

동시 CONNECT 또는 partition 시험에서 성공 owner가 둘로 관찰되면 spec 위반이다.

## 3. Routing

1. 어느 READY node에 publisher와 subscriber가 연결돼도 cluster 전체의 matching subscription으로 전달해야 한다.
2. route key는 tenant 격리가 끝난 canonical topic/filter를 사용해야 한다.
3. Route-affecting subscription mutation은 authoritative fenced command로 `sessionKey`, `ownerNodeId`, `epoch`와 subscription option을 전달한다. Committed apply 뒤 route projection은 committed persistent subscription state에서 파생하며, 별도의 authorized route mutation으로 처리하지 않는다.
4. 이전 owner 또는 stale owner/epoch의 route-affecting subscription add·remove mutation은 거절해야 한다.
5. route propagation 지연은 측정 가능해야 한다. 지연 중에도 권한 또는 tenant 경계를 우회해서는 안 된다.
6. shared subscription member 선택은 tenant와 group 경계를 지켜야 하며, unavailable owner를 계속 선택해서는 안 된다. Ingress node는 owner node가 현재 이 node의 peer 연결 상태로 도달할 수 없는 member를 primary에서 제외하고 `Fallbacks` 끝으로 보낸다. Owner가 local node이거나 transport가 모르는 node인 member는 도달 가능으로 본다. 도달 가능한 member가 없으면 전체 member에서 고른다. 이 선호는 node-local이며 ownership·epoch와 replicated state를 바꾸지 않는다. Timeout으로 owner를 실패 처리하지 않는다([ADR-0060](../decisions/ADR-0060-shared-subscription-durable-owner-loss-reroute.md)). Durable QoS 1/2와 QoS 0 선택이 같은 순서를 쓴다.

### 3.1 Live cross-node QoS 0 전달

`-cluster-config` mode에서 ingress node는 local committed route projection으로 publish 대상과 각 candidate의 effective QoS, Retain 및 shared-subscription 선택을 계산한다. Candidate마다 다음 순서를 적용한다.

1. Detached persistent session은 §4.1의 Raft offline `ENQUEUE` 경로를 먼저 사용한다.
2. Connected candidate의 committed owner가 local node와 다르면 QoS 0 delivery를 owner node로 forwarding한다.
3. Owner가 local node이거나 ownership row가 없으면 node-local delivery queue에 넣는다.

Detached 확인을 먼저 해야 한다. 연결이 끊긴 session은 이전 owner의 ownership row를 계속 가질 수 있으므로, 원격 전달보다 offline `ENQUEUE`가 우선한다. Ingress는 local committed ownership을 읽으며 publish message마다 Raft read barrier를 수행하지 않는다. QoS 0 PUBLISH ingress admission은 local storage가 정상이고 runtime과 projection이 초기화되어 있으며 projection index가 applied index 이상일 때만 허용한다. Driver가 Ready batch를 처리 중이면 cached readiness snapshot을 사용해 local projection refresh를 기다리되, `clusterReadTimeout`(2초) 뒤에도 처리가 끝나지 않거나 storage health가 `DEGRADED`/`FAILED`이면 기다림을 끝내고 delivery 전에 연결을 닫는다. 그 밖의 uninitialized·failed·stale projection은 즉시 거절한다. Quorum만 사용할 수 없는 경우에는 local projection이 정상인 QoS 0 경로를 막지 않는다.

Forwarding은 인증된 peer listener의 별도 `NMQFWRD2` stream을 사용한다. Stream queue는 방향·peer마다 최대 1,024 frame 및 8 MiB이며, Raft queue와 독립적이다. Queue admission 이후 receiver ACK나 retry는 없다. 따라서 전달은 at-most-once이고 FIFO는 같은 `(ingress node, owner node)` 쌍 안에서만 보장된다. Peer queue가 가득 차면 해당 candidate는 `Saturated`, 전달 lane/peer를 사용할 수 없으면 `Unavailable`로 처리하고 shared-subscription 선택은 다음 candidate를 시도한다.

Receiver는 자기 local committed ownership이 해당 session의 owner라고 나타내고 session이 local delivery queue에 등록된 경우에만 enqueue한다. Ownership이 다르거나 session이 등록되지 않으면 조용히 버린다. Receiver queue가 가득 차면 local queue saturation과 같이 해당 subscriber connection을 닫는다. Will publish도 동일한 routing·forwarding 경로를 사용한다. QoS 1/2는 §4.2의 Raft 경로를 사용한다.

이 경로의 live message는 Raft log, snapshot, backup 또는 detached offline queue에 기록하지 않는다.

## 4. Replicated state

다음 상태는 configured replication factor와 write quorum을 적용해야 한다.

| 상태 | 복구 계약 |
| --- | --- |
| persistent subscription | owner 장애 뒤 같은 filter와 options를 복구한다. |
| 수신·송신 QoS inflight | packet identifier, 단계와 재전송 의미를 복구한다. |
| durable offline message | expiry·quota 안에서 전달을 재개한다. |
| retained message | tenant와 canonical topic별 최신 값을 복구한다. |
| session metadata | expiry, Will, owner epoch와 client protocol 정보를 복구한다. |
| ownership·route metadata | stale owner를 fence하고 routing을 다시 구성한다. |

replica 수가 configured write quorum보다 적으면 durable write를 성공 처리해서는 안 된다. replication lag 또는 disk pressure가 한도를 넘으면 publish admission을 늦추거나 거절해야 하며, 이미 성공 ACK한 durable state를 임의로 drop해서는 안 된다. (§4.4, §4.5)

### 4.1 Offline message capture와 전달

Cluster mode에서 persistent session(MQTT 3.1.1 CleanSession=0 또는 MQTT 5 CleanStart=0 및 Session Expiry Interval > 0)이 연결을 종료한 뒤부터 다음 재접속 전까지의 구간을 **detached** 상태로 취급한다. Detached session에 도착한 QoS 0 message만 Raft committed state machine에 저장한다. Connected session은 local owner의 node-local queue를 사용하거나, 다른 node가 owner이면 §3.1에 따라 live QoS 0 delivery를 forward한다. QoS 1/2 offline entry와 durable ACK의 동작은 §4.2에서 정한다.

1. **Key와 복제 상태:** 각 `(tenantId, clientId)` session key마다 `OfflineSessionState`를 둔다. 필드는 `tenantId`, `clientId`, 단조 증가 `epoch`, `sessionExpiresAtUnixMillis`, 단조 증가 `nextSequence`와 `messages[]`다. 각 `OfflineMessage`는 `sequence`, `topic`, `payload`, `qos(=0)`, `retain`, `expiresAtUnixMillis`를 가진다. Message identity는 session 안에서 1부터 단조 증가하는 `sequence`이며 FIFO 순서다.
2. **명령:** `OfflineChange`는 세 action을 가진다.
   - `DETACH`: owner node가 persistent session disconnect 시 제안한다. `(ownerNodeId, epoch)`가 현재 ownership과 정확히 일치해야 한다(아니면 `stale_epoch`). Record가 없으면 `nextSequence=1`로 생성한다. 동일 epoch record가 이미 있으면 no-op `Applied`다. 더 작은 epoch record가 있으면 기존 message를 보존하고 epoch를 올린 뒤 이어 붙인다. Local queue 잔여 FIFO message는 최대 64개, `topic+payload` 합계 최대 768 KiB까지 함께 싣는다.
   - `ENQUEUE`: 어느 node든 publish 대상이 detached session이면 제안한다. `ownerNodeId=0`, message 정확히 1개다. Record가 존재하고 `record.epoch == command.epoch`여야 한다(아니면 `offline_session_missing`).
   - `CLAIM`: CONNECT가 새 ownership epoch를 commit한 직후 새 owner가 제안한다. `(ownerNodeId, epoch)`가 현재 ownership과 같아야 한다(아니면 `stale_epoch`). Record를 삭제하고 그 message들을 apply 결과로 반환한다. Record가 없으면 `Found=false`다.
3. **전달 의미:** at-most-once다. CLAIM commit이 record를 먼저 지우고 그 다음 local queue로 전달한다. CLAIM 성공 뒤 전송 전 crash하면 해당 QoS 0 message는 유실될 수 있다. 자동 재시도는 하지 않는다. Commit timeout 뒤 늦게 commit된 ENQUEUE는 queue에 남는다.
4. **결정적 한도:** Apply는 session당 64 message, 전체 tenant 합산 4,096 message·16 MiB, payload당 1,000,000 bytes를 제한한다. DETACH 한 command의 `topic+payload` 합계는 768 KiB 이하여야 한다. 크기는 `len(tenantId)+len(clientId)+len(topic)+len(payload)`의 합이다. 한도를 넘으면 command 전체를 `offline_capacity_exceeded`로 거절하고 기존 state를 보존한다(같은 apply의 만료 purge는 유지). Detached session record 수는 ownership row 수를 넘지 않으므로 별도 상한을 두지 않는다. Tenant quota profile이 있으면 QoS 0 offline message와 durable delivery outbound message를 합산한 tenant usage에 `offline_queue_messages`와 `offline_queue_bytes`를 추가 적용한다. Global limits가 먼저이며 구체적인 quota 차원 순서와 profile 적용은 [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md)을 따른다.
5. **만료:** 모든 offline command apply는 먼저 proposer 시각(`proposedAtUnixMillis`) 기준으로 `0 < expiresAt <= proposedAt`인 message와 `0 < sessionExpiresAt <= proposedAt`인 session record를 지운다. Local replica clock은 읽지 않는다. `sessionExpiresAt`은 disconnect 시각 + MQTT 5 Session Expiry Interval이며, 만료가 없으면 0이다.
6. **Ownership 연동:** Ownership `Release`가 apply되면 같은 key의 offline record도 지운다. Transfer/acquire는 record를 보존하며 새 owner의 CLAIM이 가져간다. 모든 offline record는 `record.epoch` 이상의 ownership row를 가져야 한다.
7. **SessionPresent:** `CleanStart=0` CONNECT의 CLAIM이 `Found=true`이면 다른 node로 reconnect해도 CONNACK `SessionPresent=1`이고 message를 sequence FIFO로 전달한다. `CleanStart=1`이면 CLAIM으로 record를 지우고 message는 버린다.
8. **Fallback:** DETACH가 quorum/compatibility/stale-epoch 이유로 실패하면 local queue를 그대로 둔다. CLAIM이 실패하면 CONNECT를 ownership-unavailable reason으로 거절하고 local session 변경을 rollback한다. 기존 local session이 없었던 시도는 새 delivery queue 등록과 tenant route binding도 제거한다. 기존 local session이 있으면 CLAIM 성공 전까지 기존 expiry/WILL timer를 유지하고, 실패 시 기존 timer와 session 상태를 보존한다. ENQUEUE 실패는 해당 대상만 `Saturated`(capacity) 또는 `Unavailable`(그 외)로 보고한다.
9. **알려진 한계:** Feature 비활성 시 owner node가 연결 중에 crash하면 DETACH가 없어 이후 message는 capture되지 않는다. Feature 활성 시 replicated session metadata로 owner 재시작 뒤 expiry와 session 복구를 수행한다(§4.3). Detach 전환 중 owner node에서 publish된 QoS 0 message는 유실하지 않는다(11항). 다른 node에서 forward된 QoS 0 delivery는 전환 중이거나 local queue 제거 뒤 도착하면 §3.1의 at-most-once에 따라 유실될 수 있다. 연결 중 owner가 offline DETACH commit 전에 끝나면 그 local queue는 복구하지 않는다. DETACH commit 뒤 crash하면 record와 이후 ENQUEUE를 보존한다. Feature 활성 시 owner 재시작 recovery가 빈 DETACH를 commit해 capture를 재개한다. 이미 전달한 message를 다시 보내지 않는다. 이 경계는 [ADR-0027](../decisions/ADR-0027-detach-forward-and-owner-crash-qos0-boundary.md)을 따른다. ENQUEUE는 대상 session마다 commit 1회다.
10. **Schema와 profile gate:** Replicated-state schema는 `1.0`을 유지하고 unknown field를 거절한다. Offline command는 fresh read barrier 뒤 모든 non-retired member의 cluster protocol/state schema가 exact match이고 release가 같은 major의 인접 minor window 안에 있는지 확인한 뒤 제안한다.
11. **Detach 전환 보호:** [ADR-0026](../decisions/ADR-0026-detach-transition-local-qos0-gate.md)에 따라 전환 중 owner-node local publish는 DETACH 결과를 기다린 뒤 다시 판정한다. 성공하면 offline ENQUEUE로 보내고 실패하면 유지된 local queue로 보낸다. 최대 대기는 10초이며 이를 넘기면 해당 대상을 `Unavailable`로 보고한다. 캡처한 message와 같은 publisher의 이후 message 순서는 FIFO다. Local queue 제거 직후 도착한 publish도 offline 경로를 한 번 재판정한다. 각 판정은 새 현재 시각으로 기존 absolute Message Expiry deadline을 검사한다. 대기 중 만료되면 ENQUEUE·forward·local enqueue를 실행하지 않고 전달 결과에도 넣지 않는다. Forward 경로는 기다리지 않는다. 기존 capacity·expiry·commit timeout 경계는 유지한다.

### 4.2 QoS 1/2 durable delivery

Cluster mode에서 publisher가 보낸 QoS 1/2 PUBLISH 및 QoS 1/2 Will의 처리는 Raft quorum 경계를 따른다. QoS 1 client PUBLISH는 retained mutation이 있으면 이를 먼저 commit한다. Prepare 시점 ingress route projection에 durable target 후보가 하나 이상 있으면 `publish_qos=1` delivery command를 commit한 뒤 effective QoS 0 대상 lane을 처리하고 PUBACK을 보낸다. 후보가 없으면 delivery PUBLISH command 없이 fresh, proposal 없는 quorum ReadIndex barrier를 기다리고, 그 index까지 local apply와 route projection이 끝난 뒤 QoS 0 lane을 처리하고 PUBACK을 보낸다. QoS 2 client PUBLISH 수신은 target 수와 무관하게 publisher session inbound 상태를 RECEIVE로 commit한 뒤 PUBREC을 보낸다. QoS 1/2 Will은 publisher inbound RECEIVE 없이 전달하며, retained mutation이 있으면 기존 순서로 먼저 commit한다. Target 후보가 있으면 `publish_qos=1/2` durable PUBLISH command를 commit하고 없으면 같은 fresh ReadIndex barrier를 통과한 뒤 QoS 0 lane을 처리한다. Will에는 publisher ACK가 없다. PUBREL 수신은 target 수와 무관하게 retained state와 `publish_qos=2`인 RELEASE를 commit하고 QoS 0 대상 lane을 처리한 뒤 PUBCOMP을 보낸다. RELEASE가 이미 완료된 tombstone을 찾거나 packet을 찾지 못하면 QoS 0 lane 없이 성공 PUBCOMP을 보낸다.

Client QoS 1/2 PUBLISH ingress admission은 local projection readiness만 먼저 확인하며 별도 quorum readiness barrier를 수행하지 않는다. Admission은 Ready drain이 진행 중일 때 atomic cached readiness snapshot을 확인하고 route projection refresh를 기다리며, storage health는 그 대기와 독립적으로 확인한다. `DEGRADED`/`FAILED` storage 또는 `clusterReadTimeout`(2초) 내에 끝나지 않은 Ready drain은 전달 전에 연결을 닫는다. Ready 처리 중이 아닐 때 projection이 초기화되지 않았거나 rebuild에 실패했거나 applied index보다 뒤처지면 즉시 닫는다. Runtime은 staged proposal 결과를 Ready drain이 끝나고 refreshed projection이 최신 applied index를 반영한 뒤 해제한다. Fatal Ready 처리 오류는 admission-failed 상태를 기록한 뒤 sequence를 닫아 이후 PUBLISH admission이 실패하도록 하며, driver exit은 request queue에 남은 proposal과 driver-local waiter의 미적용·stage된 결과를 포함한 pending proposal 전체를 non-success error로 해제한다. Fresh quorum proof는 durable command의 bounded commit 경계에서 확인한다. 성공 응답 전에 quorum이 없거나 commit이 실패·timeout되면 PUBACK/PUBREC/PUBCOMP 성공 응답을 보내지 않고 연결을 닫는다. RECEIVE capacity 초과 또는 command 크기 초과 시 MQTT 5는 reason `0x97`을 보내고 MQTT 3.1.1은 연결을 닫는다. PUBLISH/RELEASE 대상별 durable queue가 포화되면 그 대상은 `Saturated`로 건너뛰지만 publish 전체는 성공할 수 있다. 이는 PRD CLU-12의 한도 도달 시 publish 제한 요구와 다른, 승인된 결정이다. 로컬 연결 중인 포화 대상은 기존 정책에 따라 연결을 닫으며, 재접속 시 queue 적재가 다시 시도된다.

리더를 아직 모르는 동안에는 새 ReadIndex를 Raft에 넘기지 않는다. Driver가 기존 미발행 FIFO head로 보관하며 inbound·tick 처리는 계속한다. 리더를 다시 알게 되면 같은 요청을 발행한다. 원래 context와 deadline을 유지하며 proof를 공유하거나 성공으로 간주하지 않는다.

발행한 read가 완료되기 전에 leader·term·role이 바뀌면 driver는 미완료 proof를 다시 발행 대상으로 돌린다. 새 proof는 현재 Raft의 리더가 분명하고 기존 outbound 예산이 있을 때만 발행한다. Request context에는 origin node ID와 발행마다 바뀌는 nonce를 넣는다. Caller의 원래 key·context·deadline과 FIFO commit 순서는 유지한다. 이전 발행의 ReadState는 새 proof를 완료할 수 없다. Proof를 다른 caller와 공유하지 않는다. Application 재제안은 아래의 `proposal-recovery` 활성화 경로에서만 허용한다.

발행한 proof의 ReadState가 `clusterReadProofRetryInterval`(`clusterReadTimeout`의 4분의 1, 500ms) 안에 오지 않으면 driver는 같은 방식으로 다시 발행한다. Peer FIFO가 가득 차면 leader의 proof 응답 frame이 거절될 수 있고, 송신 쪽은 이 frame을 다시 보내지 않기 때문이다. 이미 ReadState가 도착해 local apply와 projection을 기다리는 proof는 다시 발행하지 않는다. Context가 끝난 read도 다시 발행하지 않는다. 다시 발행할 outbound 여유가 없으면 이전 nonce를 유지한다. 새 proof는 위 규칙과 같이 원래 key·context·deadline과 FIFO commit 순서를 유지하며 이전 발행의 ReadState로 완료하지 않는다.

리더 이전이나 리더 선출 중 정상 forwarded proposal이 `raft.ErrProposalDropped`로 거절될 수 있다. 이 오류는 Raft log에 append하지 않았다는 뜻이다. Dynamic adapter는 이 경우 요청을 bounded FIFO에 보관한다. [ADR-0035](../decisions/ADR-0035-failover-aware-commit-lag-and-unsent-proposal-reclaim.md)에 따라 socket write를 한 번도 시작하지 않은 raft-lane `MsgProp`도 같은 FIFO로 회수한다. 대상은 연결 없는 peer의 outbox ring에 남은 frame과 transport가 `ErrPeerBackpressure`로 받지 않은 frame이다. Write를 시작한 `outCurrent`는 회수하지 않고 기존처럼 재연결 뒤 다시 보낸다. 리더와 저장 공간을 다시 사용할 수 있으면 같은 요청을 Raft에 넘긴다. 새 forwarded proposal도 보관 중인 요청 뒤에 정렬한다. 서로 다른 peer에 동시에 남은 proposal의 상대 순서와 FIFO overflow 때 남은 frame의 순서는 보장하지 않는다. 일반 leader-only `Propose`와 membership change의 계약은 바꾸지 않는다.

FIFO는 최대 64 message와 기존 `maxWALFramePayload-1024` byte 예산으로 제한한다. Ready 입력과 보관 요청의 합도 같은 byte 한도를 지킨다. 가득 차면 기존 `ErrBackpressure`로 거절한다. 인증된 inbound frame의 보관은 adapter가 이미 소유한 decoded 값으로 처리한다. Transport lease는 즉시 반환하며 Raft tick과 peer message 처리는 계속한다. Runtime membership reconciliation은 기존 stable checkpoint 조건을 유지하므로 보관분이 남아 있으면 기다린다. Raft의 선출·리더 이전 자체는 peer message와 tick으로 진행한다. 미처리 FIFO는 `StoragePending`에 포함해 checkpoint나 안전한 stop의 완료로 숨기지 않는다. 저장 장애와 Close는 기존 실패 계약을 따른다.

Follower가 보관 요청을 넘길 때는 `Config.ForwardedProposalCapacity`로 현재 리더의 outbound count·byte 여유를 확인한다. 기존 Ready·append·apply 처리가 idle일 때만 FIFO를 그 여유 안에서 발행한다. 그렇지 않으면 64개 보관 요청을 32개 peer FIFO로 한꺼번에 넘겨 다시 유실할 수 있다. 이 callback은 기존 observer처럼 composition root에서 제공하며 Node를 다시 호출하거나 대기하지 않는다. Production cluster는 TCP의 실제 여유에서 기존 2-frame·512-byte reserve를 뺀 값을 제공한다. Transport가 없는 Batch consumer는 callback 없이 기존처럼 전체 output을 소비할 수 있다. 이전 node가 다시 leader가 되기를 기다리지 않는다.

Leader peer에 연결되어 있지 않으면 forwarded FIFO flush credit은 0이다.

Test-only fault control을 쓰면 driver가 Node lock을 얻기 전에 해당 Ready turn의 isolation 파일을 읽는다. Callback은 이 snapshot만 확인하며 파일 I/O를 하지 않는다. 이 값은 fault 주입의 발행 제어에만 쓰며 fresh quorum proof를 저장하거나 공유하지 않는다.

Adapter 보관은 log append 전의 명확한 거절과 socket write를 시작하지 않은 frame만 다룬다. Write를 시작하지 않은 frame은 전송 결과를 알 수 없는 요청이 아니다. `proposal-recovery` 비활성 상태에서는 Raft가 이미 수락해 전송을 시작한 요청, 전송 결과를 알 수 없는 요청, ACK가 없는 요청을 재제안하지 않는다. 보관을 성공 commit이나 성공 MQTT ACK로 바꾸지 않는다. 원래 caller의 context와 deadline도 늘리지 않는다. 정상 거절을 malformed peer로 분류하거나 peer를 reset하지 않으며, frame·identity 검증 실패는 기존 `ErrInvalidMessage` 경로를 유지한다.

Caller deadline은 결과를 기다리는 시간의 상한이다. Adapter FIFO에는 원래 context·deadline이 전달되지 않으므로 deadline 만료만으로 보관 요청을 폐기하지 않는다. 이 구조에서는 timeout 뒤에도 요청이 나중에 commit될 수 있다. Timeout을 미적용의 증거로 쓰거나 새 OperationID로 자동 재제안하지 않는다. Timeout을 반환한 원래 호출에는 이후 성공 ACK를 보내면 안 된다. MQTT 재접속·재전송은 기존 PacketID 상태와 session ownership·epoch 규칙을 따른다.

Caller timeout 뒤에는 FIFO 재생 순서와 각 command의 1회 적용을 지킨다. 원래 `Wait`는 `ErrQuorumUnavailable`을 반환하고 늦은 결과는 버려진 waiter의 buffer에 남는다. TCP는 같은 오류에서 QoS 1/2 성공 ACK 없이 연결을 닫고 pending 값을 회수한다. 이 경계의 범위는 in-process 3-node runtime과 실제 dynamic Raft adapter·WAL·`BeginDelivery`·`Wait`이며, MQTT process 수준의 late commit 경합은 포함하지 않는다. 시나리오는 [§11.11](#1111-qos-12-durable-delivery-수용-매핑)에 있다.

`proposal-recovery`를 활성화하면 [ADR-0036](../decisions/ADR-0036-replicated-proposal-receipts-and-safe-reproposal.md)과 [ADR-0037](../decisions/ADR-0037-proposal-request-identity.md)에 따라 이미 전송한 application command도 재제안할 수 있다. Runtime은 command별 RequestID를 만들고 최초 fresh proof index와 원래 RequestID·OperationID·payload·digest를 유지한다. Leader·term 변경 뒤 새 nonce의 proof를 얻고 local apply·projection이 완료된 뒤 미완료 요청만 FIFO 순서로 재제안한다. Caller의 기존 deadline은 유지한다.

복제된 receipt는 최초 성공·conflict 결과를 보관한다. 같은 origin·RequestID·digest의 중복은 상태와 외부 효과를 반복하지 않는다. 같은 RequestID의 다른 payload나 최초 proof index는 conflict다. 같은 논리 OperationID를 쓰는 별도 RequestID는 서로 다른 command다. Origin별 retirement floor 이하의 proof index는 receipt 삭제 뒤에도 거절한다. Node별 receipt 최대 1024개, cluster 전체 최대 4096개, 결과 bytes 전체 최대 32 MiB다. Snapshot은 floor와 receipt를 함께 보존한다. 기존 64 MiB 직렬화 한도는 우선한다. Activation 전 baseline bytes와 일반 ConfChangeV2 경로는 유지한다.

Count 한도 초과 요청은 origin별 capacity fence로 기억한다. 결과 byte 공간은 mutation 전에 예약한다. 공간이 나중에 생겨도 처음 거절한 요청은 성공으로 바꾸지 않는다. 개별 receipt와 count 거절의 index 의미는 [ADR-0038](../decisions/ADR-0038-proposal-receipt-capacity-fences.md)을 따른다. 관련 시나리오는 [§11.33](#1133-안전한-proposal-복구-수용-매핑)에 있다.

`proposal-recovery`를 활성화하면 리더 kill 때 전송 중이던 PUBACK COMPLETE와 PUBREL RELEASE commit도 같은 재제안 경로를 탄다. 생존 node의 persistent subscriber 연결은 닫히지 않고, publisher는 같은 연결에서 PUBCOMP을 받는다. 두 command의 RequestID·OperationID와 payload는 바뀌지 않으며 원래 5초 deadline도 늘리지 않는다. 비활성 상태에서는 같은 commit이 5초 deadline에서 `ErrQuorumUnavailable`을 반환하고 연결을 닫는다. 위 비활성 계약은 그대로다. 따라서 N-1에서 계획 밖 disconnect 0을 요구하는 용량 시험은 `proposal-recovery`를 활성화한 cluster에서 실행해야 한다. 이 feature는 기본 비활성이며 활성화와 rollback 제약은 [ADR-0036](../decisions/ADR-0036-replicated-proposal-receipts-and-safe-reproposal.md)과 [feature activation 절차](../how-to/cluster-feature-activation.md)를 따른다. 비활성·활성 비교 시나리오는 [§11.33](#1133-안전한-proposal-복구-수용-매핑)에 있다.

각 `(tenantId, clientId)` session에는 Raft 복제 durable queue와 QoS inflight state가 있다. Apply 단계의 target 판정은 committed ownership과 subscription으로 다시 검증하며, 현재 owner/epoch에 묶인 matching subscription 가운데 QoS 1 이상이 있어야 한다. `DeliveryChange.publish_qos`는 PUBLISH/RELEASE에서 각각 1 또는 2이며 RELEASE는 2여야 한다. Apply는 이 값을 사용해 `min(publish_qos, matching subscription QoS 최대값)`을 대상의 effective QoS로 계산한다. 각 session message는 1부터 증가하는 sequence를 가져 FIFO로 복구된다. Session당 message 64개 및 inbound 64개, tenant 전체 합계 4,096 entries와 16 MiB, payload 1,000,000 bytes, command당 1,024 targets 제한을 둔다. Committed tenant quota profile은 offline queue와 durable delivery outbound message를 함께 집계하고 `offline_queue_messages`·`offline_queue_bytes`를 적용한다. `storage_bytes`에는 retained, offline, durable delivery outbound·inbound state를 포함한다. 전체 quota 규칙은 [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md)을 따른다.

Delivery command를 만들지 여부를 결정하는 target 후보는 prepare 시점 ingress projection의 primary 후보 가운데 effective QoS가 1 이상인 matching subscription이다. Session의 persistent 여부는 후보 포함 여부를 바꾸지 않으며, Apply는 기존대로 committed ownership과 subscription을 다시 검증한다. Ingress node에 아직 적용되지 않은 SUBSCRIBE는 publish보다 뒤에 정렬된다. Target 없는 경로에서 barrier가 실패하거나 timeout되면 성공 ACK와 QoS 0 lane 처리를 막는다. QoS 1 client PUBLISH는 기존 commit 실패와 같이 연결을 닫으며, Will에는 원래 publisher ACK가 없다. Barrier timeout은 5초다. 이 경로는 별도 release profile compatibility gate나 feature activation을 추가하지 않는다.

`NextSequence`는 wrap-around하지 않는다. `uint64` 최대값에 도달한 target은 새 message를 받지 않고 `Saturated`로 보고한다.

Owner는 outbound PUBLISH 쓰기 전에 SEND를 commit하고, subscriber의 PUBREC 뒤 PUBREL 쓰기 전에 ADVANCE를 commit한다. PUBACK/PUBCOMP 또는 실패 PUBREC 처리 뒤에는 COMPLETE를 commit한 뒤 packet identifier를 해제한다. 이전 owner의 명령은 새 ownership epoch에서 fencing된다. Reconnect/takeover 시 새 owner는 할당된 inflight를 packet identifier 순서로 재전송한다(PUBLISH는 `DUP=1`, PUBREL 대기 단계는 PUBREL). 이어 QUEUED message를 sequence 순서로 보내고, claimed offline QoS 0 message는 그 뒤에 보낸다. CleanStart는 durable state를 reset하며, CONNACK SessionPresent는 offline CLAIM 또는 durable ATTACH 중 하나가 기존 state를 찾았고 CleanStart가 아닌 경우에만 설정한다.

`SEND` commit에서 quorum을 일시적으로 사용할 수 없으면 replicated message와 local queue head를 소비하거나 승격해서는 안 된다. 이 사유만으로 subscriber TCP connection을 닫거나 PUBLISH를 보내지 않는다. `ErrRetryableUnavailable`은 SEND commit 결과를 확인할 수 없는 상태를 나타내며, local queue head와 pending Packet Identifier를 보존하고 같은 ID로 재시도한다. Writer는 최대 초당 한 번 자동 재시도하고, 다음 durable-delivery notification은 retry timer보다 먼저 처리를 깨울 수 있다. 일반 `ErrUnavailable`은 missing session/state를 뜻하므로 일시적 quorum 오류로 취급하지 않는다.

`ATTACH`는 command 전부터 `DeliverySessionState` record가 있으면 message/inbound가 비어 있어도 `Found=true`를 반환한다. `Reset=true`인 CleanStart는 delivery/inbound state와 committed persistent subscriptions를 지우고 `Found=false`를 반환하며, record가 있으면 `next_sequence`는 단조 증가를 유지한다.

공유 구독의 durable message 재배분은 `shared-subscription-reroute` feature 활성화 뒤의 동작이다([ADR-0060](../decisions/ADR-0060-shared-subscription-durable-owner-loss-reroute.md)). 모든 build는 이 feature를 광고하지만 `FeatureActivation`이 commit되기 전에는 `shared_filter`를 제안하거나 저장하지 않는다. 비활성 cluster의 동작과 wire bytes는 기존과 같다. 활성화 뒤 ingress는 group 선택의 결과인 durable target에 group을 포함한 전체 filter(`$share/<group>/<filter>`)를 `shared_filter`로 싣는다. 같은 client가 비공유 구독으로도 match하면 싣지 않는다. Apply는 대상이 현재 owner/epoch로 그 filter를 구독 중일 때만 message에 기록한다. 저장한 `shared_filter`의 byte 길이는 그 message의 byte 수에 더해 전체 16 MiB와 tenant quota에 포함한다.

Delivery record를 지우는 apply(CleanStart `Reset=true` ATTACH, DISCARD, session expiry purge, ownership Release)는 지우기 전에 `shared_filter`가 있고 만료되지 않은 message 가운데 `QUEUED`와 QoS 1 `AWAIT_PUBACK`을 같은 group의 다른 member queue로 옮긴다. QoS 2 `AWAIT_PUBREC`·`AWAIT_PUBCOMP`는 이미 보내기 시작한 message라서 옮기지 않는다(MQTT-4.8.2-5). 목적지는 같은 tenant에서 같은 filter를 현재 owner/epoch로 QoS 1 이상 구독 중인 다른 client다. 그 command를 제안한 owner node(CleanStart 재접속이면 재접속한 node)가 owner인 member를 먼저 고른다. 그 node는 방금 command를 보냈으므로 살아 있다. Expiry purge처럼 제안한 node를 알 수 없으면 이 단계를 건너뛴다. 각 단계에서 client ID 오름차순으로 정렬하고 지워지는 client의 다음부터 한 건씩 돌려 가며 고른다. Session당 64개, 전체·tenant 한도 또는 sequence가 가득 찬 member는 건너뛴다. 받을 member가 없으면 message를 버린다. 새 message는 새 sequence의 `QUEUED`이고 만료 시각과 `shared_filter`를 유지한다. QoS는 `min(원래 QoS, 목적지 구독 QoS)`이고 RETAIN은 저장된 RETAIN이 true이며 목적지가 Retain As Published를 요청했을 때만 true다. 한 message는 한 member에게만 간다. Persistent session의 member가 살아 있으면 queue는 그대로 남고 그 member가 돌아올 때 전달된다. 옮긴 message는 목적지 owner node가 기존 durable 알림 경로로 전달한다.

QoS 2 publisher inbound record는 PUBREL 전 상태 또는 완료 tombstone을 복구한다. Tombstone은 RELEASE 뒤 PUBCOMP가 유실되어 client가 PUBREL을 재시도할 때 성공 PUBCOMP을 보장하며, 같은 packet identifier의 새 RECEIVE, capacity eviction, ATTACH reset 또는 record 삭제까지만 유지된다. QoS 1 중복은 같은 connection에서 기존 inbound 상태로 억제하지만, reconnect 뒤 DUP 재전송은 새 PUBLISH로 처리될 수 있어 중복 전달이 가능하다. QoS 2는 publisher inbound state로 connection/node를 넘어 중복을 억제한다.

QoS 1/2 PUBLISH에서 effective QoS가 0인 subscriber는 durable queue가 아니라 기존 QoS 0 경로를 따른다. 이 at-most-once lane은 delivery commit 뒤 detached session이면 offline ENQUEUE, remote owner이면 `NMQFWRD2` forwarding, 그 밖에는 local queue를 사용한다. PUBACK/PUBCOMP 뒤 lane 실패 또는 ingress node crash로 전달이 유실될 수 있다. 이는 MQTT QoS 0 및 ADR-0013의 보장 범위와 같다.

SUBSCRIBE 중 retained message 재생과 DETACH 실패 뒤 남은 local QoS 0 message는 node-local 예외이며 owner crash/takeover 시 복구되지 않는다. Local QoS 0 message는 durable queue 뒤 순서로 유지되고 capacity 초과분은 잘릴 수 있다. Feature 비활성 시 connected session의 owner node가 crash하고 client가 돌아오지 않아 DETACH가 없으면 session expiry 갱신이 없을 수 있다. Feature 활성 시 owner restart recovery와 expiry 회수는 §4.3을 따른다. Record는 session당 64개까지 찬 뒤 이후 matching target은 `Saturated`로 건너뛰며 global quota도 고갈될 수 있다.

이 범위는 ADR-0015가 정한 cluster QoS 1/2 durable delivery 동작이다. §11.11은 detached session queue 전달, QoS inflight takeover, QoS 2 Will reconnect/takeover 전달, backup restore, ACK 뒤 process kill 및 quorum-loss 거절을 시나리오로 정의한다. Drain/`SAFE_TO_STOP`은 ADR-0015 범위에 포함되지 않으며 broker drain 동작과 process evidence는 [Drain spec §10](drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)에 별도로 기록한다. Production storage/EBS power-loss, BP-03, N-1 compatibility와 TEN-08 quota도 별도 gate다.
Target 없는 QoS 1 client PUBLISH와 QoS 1/2 Will의 delivery command 생략 및 quorum barrier는 [ADR-0019](../decisions/ADR-0019-untargeted-durable-publish-quorum-barrier.md)가 ADR-0015 §1–8을 제한된 범위에서 대체한다.

### 4.3 Session metadata 복제와 owner 손실 복구

이 절은 `session-metadata` feature 활성화 뒤의 동작이다. 모든 build는 이 feature를 광고하지만 `FeatureActivation`이 commit되기 전에는 session metadata를 제안하거나 저장하지 않는다. 비활성 cluster는 기존 동작과 wire bytes를 유지한다. 활성화는 영구적이며 release-profile feature 규칙에 따른다.

각 `(tenantId, clientId)`마다 committed `SessionMetadataState`를 두며 기록 epoch, MQTT protocol version(4 또는 5), retention(`IMMEDIATE`, `FINITE`, `INFINITE`), expiry interval, connected 상태, detached 시각, session 만료 시각, 남은 Will과 그 예정 시각, Will ACL 재검사용 principal(subject·credential ID·roles·만료)을 포함한다. Principal은 Will이 있을 때만 저장한다. Will과 principal의 cluster 전체 byte 합은 8 MiB(`MaxSessionWillBytes`) 이하이며 tenant storage quota에는 포함하지 않는다.

1. **CONNECT와 ownership:** CONNECT는 session metadata를 포함한 ownership `Acquire`/`Transfer` 하나를 commit해 새 `CONNECTED` record를 ownership과 원자적으로 설치한다(별도 metadata commit은 없다). 새 record는 이전 record와 pending Will을 대체한다. 예약 Will은 `suppressed_reconnected_within_delay`로 집계한다. 계획 인계 recovery 전에 재접속해 이전 epoch의 `CONNECTED` record를 대체하면, MQTT 3.1.1·Will Delay 0·retention `IMMEDIATE` Will은 재접속 node가 발행한다. 나머지는 같은 재접속 억제 사유로 집계한다. 같은 epoch의 live takeover는 기존 억제를 유지한다. `Session`은 `Acquire` 및 `Transfer`에서만 허용한다. Feature가 비활성이면 metadata proposal은 `feature_inactive`로 거절한다.
2. **유효성:** protocol version은 4 또는 5다. MQTT 3.1.1은 retention이 `IMMEDIATE` 또는 `INFINITE`, expiry interval 0, Will delay 0이어야 한다. MQTT 5는 `IMMEDIATE` iff interval 0, `INFINITE` iff interval `0xFFFFFFFF`, `FINITE` iff interval이 그 사이 값이다. Will topic은 유효하고 payload는 최대 65,535 bytes, QoS는 최대 2다. Principal은 Will이 없으면 허용되지 않으며, 있을 경우 subject·credential ID 각각 UTF-8 1..1,024 bytes, expiry 양수, roles 최대 64개, 각 role UTF-8 1..256 bytes이고 중복이 없어야 한다. 어긋난 session metadata는 `session_metadata_invalid`로 거절한다.
3. **Fenced metadata commands:** `DETACH`, `CLAIM_WILL`, `DISCARD`는 committed current ownership의 owner/epoch가 정확히 일치해야 하며 아니면 `stale_epoch`이고 state를 바꾸지 않는다. 모든 change는 identity 및 owner·epoch·proposal 시각이 유효해야 한다. `DETACH` disposition은 `DISCARD`, `CLAIM`, `SCHEDULE` 중 하나다. `SCHEDULE`은 0이 아닌 `WillDueAt >= ProposedAt`을 요구하며 다른 disposition은 Will due를 0으로 둔다. Session expiry는 0 또는 `ProposedAt`보다 커야 한다. `CLAIM_WILL`은 disposition과 두 시각이 0/unspecified이고, `DISCARD`는 disposition `DISCARD`/`CLAIM` 및 두 시각 0이어야 한다. Valid command에 record가 없으면 `Applied, Found=false`다.
4. **Apply와 Will disposition:** `DETACH`는 record epoch를 갱신하고 disconnected로 설정해 proposal 시각을 detached 시각으로, command의 session expiry를 만료 시각으로 저장한다. `DISCARD` disposition은 Will/principal을 버린다. `CLAIM`은 Will을 apply 결과로 반환한 뒤 Will/principal을 제거한다. `SCHEDULE`은 Will이 있으면 due 시각을 저장하며 Will이 없으면 due 0, principal 없음으로 만든다. `CLAIM_WILL`은 Will(없으면 nil)을 결과로 반환하고 Will/principal/due를 제거하되 connected 상태는 유지한다. `DISCARD` action은 record를 삭제하며 disposition `CLAIM`에서만 Will을 결과로 반환한다. 모든 transition은 global Will/principal bytes를 갱신한다. Bytes 한도 초과는 `session_metadata_capacity_exceeded`이며 실패 command는 기존 state를 보존한다.
5. **State 및 snapshot invariant:** Connected record는 detached·session expiry·Will due 시각이 모두 0이어야 한다. Disconnected record는 detached 시각 양수, retention 비-`IMMEDIATE`여야 하며 `FINITE`는 expiry가 detached 시각보다 크고 `INFINITE`는 expiry 0이어야 한다. Will 존재와 양수 Will due는 동치다. Snapshot record는 `(tenantId, clientId)` 순으로 정렬·유일하고 ownership row가 있으며 record epoch는 ownership epoch 이하이어야 한다. Metadata가 하나라도 있으면 `session-metadata` active feature가 필요하다. Record 개수는 snapshot object count에 포함한다. 전체 byte 합은 각 record Will이 없으면 0, 있으면 `len(topic)+len(payload)+len(subject)+len(credentialId)+Σlen(roles)`로 계산한다.
6. **Owner 재시작 recovery:** READY된 node는 자기 node가 owner인 metadata와 local 연결이 없는 session을 복구한다. Record epoch가 현재 ownership epoch와 같고 record가 `CONNECTED`이면 재시작 recovery 시각을 disconnect 시각으로 하는 비정상 종료로 처리한다. Will 규칙을 적용하고 expiry를 시작한다. Recovery 전에 local lifecycle이 connected이거나 이미 처리한 epoch 이상이면 건너뛴다.
7. **Handoff:** 새 client CONNECT 없이 drain 또는 planned removal로 record가 successor에 이전되면, 이전 epoch의 `CONNECTED` record에 서버 주도 비정상 종료 규칙을 적용한다. MQTT 3.1.1·Will Delay 0·retention `IMMEDIATE`이면 즉시 claim·발행한다. 나머지는 successor recovery 시각부터 Will Delay로 예약하며 session expiry로 제한한다. Expiry도 recovery 시각부터 시작한다. `DETACHED` record는 저장된 절대 Will due/session expiry 시각을 기준으로 timer를 다시 설정한다. Host를 영구히 잃은 owner의 session은 planned removal handoff 전까지 복구되지 않으며 그 handoff에서 같은 규칙으로 Will을 발행한다. [ADR-0028](../decisions/ADR-0028-planned-handoff-will-and-reason-metric.md)을 따른다.
8. **Will at-most-once 및 만료 회수:** Will은 Will을 claim하는 command가 commit된 뒤에만 claim한 node에서 발행한다. Claim commit 실패(예: quorum 없음)면 발행하지 않는다. Owner-local expiry timer는 metadata `DISCARD`와 기존 durable delivery `DISCARD`를 commit한다. Metadata `DISCARD` 또는 ownership `Release`는 metadata record를 제거하며 만료 때 ownership row를 `Release`하지 않아 epoch ABA를 막는다. Offline record는 기존 lazy purge가 제거한다. Disconnect·지연 Will 처리마다 metadata commit 1회가 추가된다. Quorum 손실 중 Will은 발행되지 않는다.
9. **조회와 backup:** 활성화 후 `/api/v1/clients` 및 `/api/v1/clients/{tenantId}/{clientId}`는 어느 node에서나 committed metadata를 반환한다. Backup에는 metadata를 typed snapshot과 whole-state digest에 포함하되 별도 kind summary를 추가하지 않는다. Archive format `3.0`을 유지한다. Feature 활성화 전에 연결된 session은 재접속 전까지 metadata가 없어 crash recovery나 cluster session 조회 대상이 아니다.

### 4.4 Replication factor와 write quorum profile

[ADR-0029](../decisions/ADR-0029-replication-factor-write-quorum-profile.md)에 따라 지원 profile은 replication factor 3 / write quorum 2 하나다. Replication factor는 유지할 최소 voter 수다. Write quorum은 durable write 성공에 필요한 최소 영속 replica 수다.

Cluster config의 선택 객체 `replication`을 생략하면 3/2다. 객체가 있으면 정수 key `factor`와 `writeQuorum`이 모두 있어야 한다. 3/2 외의 값, 누락 key와 unknown key는 startup에서 거절한다. Durable write는 기존 Raft 과반수 commit 뒤에만 성공한다. Voter가 3 이상이면 과반수도 2 이상이므로 write quorum을 충족한다.

Planned removal은 대상 외의 committed `VOTER`가 3 미만이면 non-retryable `409 replication_factor_unsatisfied`를 반환한다. 이 요청은 commit하지 않고 member와 operation을 바꾸지 않는다. Leader는 실제 Raft voter 집합도 다시 검사한다. 경합으로 남은 voter가 부족하면 session transfer와 removal ConfChange를 제안하지 않고 operation을 `REMOVING`에 둔다. 새 voter를 먼저 추가한 4→3 교체는 허용한다. Drain은 voter를 제거하지 않으므로 이 보호의 대상이 아니다.

`GET /api/v1/cluster`는 dynamic cluster의 `replicationFactor=3`, `writeQuorum=2`를 반환한다. Standalone은 1/1을 반환한다. Replicated schema·wire·apply 의미는 바꾸지 않는다. Mixed N-1/N removal 보호와 경합 operation의 자동 해제 부재는 ADR-0029 §1 항목 7의 결정한 한계다.

### 4.5 Publish backpressure admission

[ADR-0030](../decisions/ADR-0030-publish-backpressure-admission.md)을 따른다. Admission은 새 durable 저장을 제안하기 전에 압력과 저장 한도를 검사한다. Replicated apply 결과는 바꾸지 않는다.

- 대상은 QoS 1 client PUBLISH의 phased·inline 경로, QoS 2 첫 수신, payload가 있는 retained QoS 0이다. Retained QoS 1도 포함한다.
- QoS 2 RELEASE(PUBREL), 서버 Will, non-retained QoS 0의 live·offline·forward, 빈 payload의 retained 삭제와 SEND/ADVANCE/COMPLETE/ATTACH/DETACH/DISCARD는 제외한다.
- 검사 순서는 disk → commit lag → retained commit·route match → delivery capacity다. QoS 1 retained commit 뒤 capacity 거절은 retained mutation을 되돌리지 않는다. QoS 2 첫 수신은 retained를 commit하지 않고 route match와 capacity를 검사한다.
- Capacity는 local committed view에서 eligible target의 합계를 계산한다. 전역 delivery 4,096 entry·16 MiB와 tenant quota 초과는 ingress에서 publish를 거절한다. Session당 64개 및 sequence 소진 target은 기존 `Saturated` 처리다. Record를 만들거나 만료 purge를 미리 수행하지 않는다. 만료 message 때문에 보수적으로 거절할 수 있다. Apply 시점 경합과 RELEASE 시점 포화는 기존 결과를 유지한다.
- Commit backlog는 이 node의 `BeginDelivery` 미완료 command만 추적한다. 다른 `Commit` 경로는 추적하지 않는다. 나이 계산은 [ADR-0035](../decisions/ADR-0035-failover-aware-commit-lag-and-unsent-proposal-reclaim.md)를 따른다.
  1. Local Raft status의 `LeaderID != 0`이고 local NodeID와 같거나 dynamic transport의 `Status(LeaderID).Connected == true`이면 도달 가능한 leader다.
  2. 도달 가능한 leader가 없으면 `commit_lag`는 활성화되지 않는다.
  3. `(LeaderID, Term)`이 바뀌거나 도달 가능한 leader가 도달 불가능해지면 backlog `generation`을 1 증가시킨다.
  4. Raft에 제출한 순간의 generation을 entry에 기록한다. 현재 generation과 다른 제출 entry는 나이 계산에서 영구히 제외한다. 미제출 entry는 계속 포함한다.
  5. 포함된 가장 오래된 entry의 나이는 `now - max(startedAt, reachableSince)`다. `reachableSince`는 현재 leader에 도달할 수 있게 된 시각이다.
  6. 이 나이가 `maxCommitLagMillis` 이상이면 거절한다. `narumq_cluster_commit_backlog_oldest_seconds`도 같은 값을 출력한다.
  7. Failover 지연과 quorum 손실 뒤 leader가 물러난 구간은 기존 5초 `clusterDurableCommitTimeout`이 처리한다. 결과가 확정되지 않은 commit의 timeout과 연결 종료는 §4.2·ADR-0015 §13을 유지한다. Leader의 follower별 지연은 metric으로만 보고한다.
  8. Driver가 local apply와 projection을 완료하면 caller가 `Wait()`를 호출하기 전에도 해당 entry를 제거한다. Fresh proof·제출 실패나 driver 종료로 오류 결과를 확정한 entry도 제거한다. Begin 오류로 driver에 전달하지 못한 entry는 caller가 제거한다. Wait의 timeout·취소는 이미 수락된 proposal의 entry를 제거하지 않는다. 그 entry는 늦은 apply·projection 완료나 driver 종료까지 유지한다. 이전 generation의 entry를 나이에서 제외하는 규칙은 그대로다. 따라서 TCP의 순서 대기나 SEND batch의 앞선 요청 timeout은 이미 완료된 command의 backlog 나이를 늘리지 않는다.
- Disk는 local data directory filesystem을 시작 때와 1초마다 측정한다. `free < minFreeBytes`이면 진입한다. `free >= minFreeBytes + minFreeBytes/8`이면 해제한다. `minFreeBytes=0`이면 disk admission만 끄고 gauge는 유지한다. 측정 오류는 이전 상태를 유지한다. 초기 상태는 압력 없음이다. 연속 오류는 Warn을 한 번만 남긴다. ENOSPC는 기존 `FAILED` 처리다.

| 요청 | MQTT 5 거절 | MQTT 3.1.1 거절 |
| --- | --- | --- |
| QoS 1 PUBLISH | PUBACK `0x97`, 연결 유지 | 연결 종료 |
| QoS 2 첫 PUBLISH | PUBREC `0x97` | 연결 종료 |
| payload가 있는 retained QoS 0 | DISCONNECT `0x97` 뒤 종료 | 연결 종료 |

PUBACK에 `0x89`를 쓰지 않는다. PUBCOMP에 `0x97`이 없으므로 RELEASE를 막지 않는다. PUBACK·PUBREC `0x97`에는 거절 원인(`commit_lag`, `disk_free`, `delivery_entries`, `delivery_bytes`, `delivery_capacity`, `receive_capacity`, `retained_capacity`, `tenant_quota`)을 나타내는 Reason String을 붙인다. 형식은 [MQTT §7.2](mqtt-protocol-and-session.md#72-quota-계열-거절의-원인-표시)를 따른다.

`publishBackpressure`의 JSON 구조, 기본값과 허용 범위는 [Process configuration v1 §1.3](../contracts/process-configuration-v1.md)에서 정한다. 설정은 해당 field를 아는 binary와 함께 배포해야 한다. 이전 binary는 알 수 없는 key를 거절한다([Process configuration v1 §3](../contracts/process-configuration-v1.md#3-호환성-규칙)). Raft·peer wire, replicated schema와 apply 동작은 바뀌지 않으므로 feature gate는 추가하지 않는다. Reason String은 client에 보내는 MQTT 5 packet에만 붙는다.

| Metric | 값과 label |
| --- | --- |
| `narumq_publish_backpressure_rejections_total{reason}` | `commit_lag`, `disk_free`, `delivery_capacity`, `tenant_quota` counter를 항상 출력한다. |
| `narumq_publish_backpressure_active{reason}` | `commit_lag`, `disk_free`의 0/1 gauge를 항상 출력한다. |
| `narumq_storage_free_bytes` | 첫 측정 성공 뒤 local 여유 공간을 출력한다. |
| `narumq_cluster_commit_backlog_oldest_seconds` | `commit_lag` 판정에 쓰는 포함 entry의 최대 나이를 초 단위 소수점 여섯 자리로 출력한다. |
| `narumq_durable_delivery_saturated_targets_total` | Apply의 포화 target 수를 누적한다. |
| `narumq_raft_peer_replication_lag_entries{peer}` | Leader에서 non-retired peer별 `max(CommitIndex-match, 0)`를 출력한다. Follower에서는 series가 없다. |

Tenant quota 거절은 기존 `narumq_tenant_quota_rejections_total{tenant,dimension}`에도 기록한다. 거절 Warn은 reason별 10초에 한 번이다. 압력 진입은 Warn, 해제는 Info다. Readiness·lifecycle·`replicationHealth`·`lastErrorCode`는 바꾸지 않는다. Local disk 압력만 `storagePressure`에 추가한다. 다른 voter volume의 압력은 전파하지 않는다.

## 5. Publish commit과 delivery


```mermaid
sequenceDiagram
    participant P as Publisher node
    participant R as Replicated log/store
    participant O as Subscriber owner
    participant C as Subscriber
    P->>R: "message + delivery state, epoch"
    R-->>P: "configured durability barrier 완료"
    P-->>P: "성공 PUBACK/PUBREC 허용"
    R->>O: "committed message"
    O->>C: "PUBLISH"
    C-->>O: "PUBACK 또는 QoS 2 handshake"
    O->>R: "delivery progress, epoch"
```

- 성공 PUBACK/PUBREC은 PRD의 내구성 ACK 경계 뒤에만 반환해야 한다.
- matching durable subscriber(durable target)가 없으면 publisher ACK는 quorum 확인만 뜻하며 payload를 저장하지 않는다. 미래 subscriber 전달을 뜻하지 않는다.
- owner 장애 뒤 새 owner는 committed inflight 단계에서 handshake를 재개해야 한다.
- QoS 1 중복은 허용되지만 durable ACK 뒤 message gap은 허용되지 않는다.
- 공유 구독 group에 살아 있는 member가 있으면 durable ACK된 message는 어느 member에게든 전달돼야 한다. Ingress는 도달 불가 owner의 member를 고르지 않고(§3), 이미 그 member queue에 들어간 message는 record가 지워질 때 다른 member로 옮긴다(§4.2). Persistent session member의 queue는 그 member가 돌아올 때 전달되어도 된다.
- QoS 2는 MQTT receiver와의 protocol handshake에서 중복 전달을 막아야 한다. subscriber 애플리케이션의 정확히 한 번 처리는 보장하지 않는다.

## 6. Partition과 quorum loss

| 상태 | Majority/quorum side | Minority 또는 quorum 없는 side |
| --- | --- | --- |
| 기존 non-durable traffic | 설정 정책과 자원 한도 안에서만 처리할 수 있다. | 데이터 일관성을 해치지 않는 범위로 제한한다. |
| 새 session ownership | 획득할 수 있다. | 획득해서는 안 된다. |
| durable publish ACK | barrier 충족 시 반환한다. | 성공 ACK을 반환해서는 안 된다. |
| ownership transfer | 새 epoch를 commit할 수 있다. | 수행해서는 안 된다. |
| 관리 mutation | quorum이 필요한 변경만 commit한다. | `quorum_unavailable`로 실패해야 한다. |

partition이 복구되면 committed log가 기준이 된다. minority의 speculative state가 있었다면 폐기하고 route·session state를 다시 구성해야 한다. 자동 수렴 전 node를 READY로 표시해서는 안 된다.

## 7. Failure recovery

1. process crash 또는 host loss가 감지되면 다른 node가 quorum을 통해 새 epoch를 획득해야 한다.
2. 새 owner는 persistent session, subscription, inflight와 offline queue를 복구한 뒤 connection을 수용해야 한다.
3. 단일 node 장애에서 durable ACK가 끝난 message의 RPO는 0이어야 한다.
4. crash 중 snapshot 또는 log append가 중단돼도 마지막 유효 commit 이전 상태로 원자적으로 복구해야 한다.
   - Joiner voter의 기록 손실은 3-voter·승격 직후 4-voter 상태 모두 seed 단계에서 재참여를 막고 새 ID로 교체한다. Joiner 초기 metadata/WAL·listener 생성은 허용되나 성공 MQTT admission과 Raft 참여는 허용하지 않는다. 정상 NOT_READY 거절과 Raft panic을 구분한다.
5. 실제 disk full과 느린 I/O는 해당 node의 복제 상태, readiness와 commit 결과에 반영해야 한다. 저장 오류를 성공 commit으로 보고해서는 안 된다.
   - Local `replicationHealth`는 terminal Raft 실패 시 `FAILED`, WAL Ready 기록과 `fsync` 또는 snapshot 영속화 한 건이 1초 이상 걸리거나 진행 중인 시간이 1초에 도달하면 `DEGRADED`, 나머지는 `HEALTHY`다. 느린 작업이 끝난 뒤 10초 동안 다시 느린 영속화 작업이 없으면 `HEALTHY`로 돌아간다.
   - `DEGRADED`는 `/health/ready`를 503으로 만들고 liveness는 200으로 유지한다. `FAILED`는 `/health/ready`와 `/health/live`를 모두 503으로 만들며 readiness 원인은 `raft storage failed: no space left; restart required`, `raft storage failed: I/O error; restart required`, `raft node failed; restart required` 중 원인에 맞는 문자열이어야 한다. 느린 I/O readiness 원인은 `raft storage I/O is slow`다. `FAILED` node는 관리 HTTP 응답을 유지하고 MQTT 신규 연결을 거절하며, 저장 공간을 확보해도 process를 재시작하기 전에는 복구하지 않는다.
   - Storage health는 membership lifecycle `state`를 바꾸지 않는다. local `DEGRADED` 또는 `FAILED` node의 summary는 `ready=false`, 해당 readiness 원인과 `storagePressure`를 보고해야 한다.
   - Local data directory 여유 공간이 `publishBackpressure.minFreeBytes` 미만일 때도 `storagePressure=true`다. 해제는 기준값의 1/8 hysteresis를 따른다(§4.5). Readiness와 `replicationHealth`는 바꾸지 않는다.
   - `FAILED` node는 새로운 commit을 성공으로 반환해서는 안 된다. Quorum이 남아 있는 healthy node는 진행할 수 있고, quorum이 없으면 commit을 거절해야 한다. `DEGRADED`는 commit을 계속 시도할 수 있다.
   - `lastErrorCode`는 내부 오류 문자열 대신 `storage_no_space`, `storage_io_error`, `storage_slow`, `raft_node_failed`, `peer_disconnected`, `peer_queue_full`, `peer_invalid_message`, `peer_transport_error` 중 안정된 code만 제공한다. local slow-I/O code는 상태가 회복된 뒤에도 최근 원인으로 남는다.
   - 원격 node 행의 `replicationHealth`는 조회 node에서 관찰한 peer 연결 상태다. 연결되어 있으면 `HEALTHY`, 끊겼으면 `UNREACHABLE`이며, `lastErrorCode`는 연결 상태와 별개로 마지막 알려진 transport 오류를 안정된 code로 나타낸다. 이 값은 원격 node의 Raft 또는 storage health를 의미하지 않는다. `replicationQueue`는 조회 node에서 해당 peer로 보내는 outbound message 수, byte 수와 거절 수를 나타내며 실제 transport 제한을 넘지 않아야 한다. 원시 transport 오류 문자열은 API 응답에 포함하지 않는다.
6. volume을 잃은 voter는 같은 ID로 다시 bootstrap하지 않는다. 새 ID를 stage·join해 voter로 승격한 뒤 잃은 member를 planned removal로 제거한다. Removal은 잃은 member의 session ownership을 남은 voter로 이전한다.
7. Leader가 뒤처진 follower나 새 learner에게 보낼 `MsgSnap`이 Raft peer frame 한도(1 MiB)를 넘으면 node를 실패시키지 않는다. `snapshot-stream`이 비활성이면 보내지 않고 snapshot 전송을 실패로 Raft에 알린다. 같은 Ready의 다른 peer 메시지는 계속 보내고 거절은 peer별 30초에 한 번 `WARN`으로 남긴다([§11.36](#1136-1-mib-초과-snapshot-송신-수용-매핑)). 활성화 뒤에는 [Raft snapshot stream v1](../contracts/raft-snapshot-stream-v1.md)의 별도 mTLS chunk stream으로 보낸다. 수신자는 순서·CRC32C·전체 SHA-256을 확인한 뒤 Raft에 넘기고 송신자는 결과를 Raft에 알린다([§11.37](#1137-raft-snapshot-stream-수용-매핑), [ADR-0062](../decisions/ADR-0062-raft-snapshot-stream.md)).
   - 활성화 전 한계: 1 MiB 초과 snapshot은 전달하지 못한다. 대체 voter(6항)를 join하는 절차도 완료할 수 없다. 큰 snapshot으로 복구하려면 장애 전에 `snapshot-stream`을 활성화해야 한다. 장애 중 활성화는 ADR-0032의 모든 member READY 승인 조건을 충족하지 못할 수 있다.

## 8. Mixed-version compatibility

- Dynamic cluster release compatibility follows the [release compatibility contract](../contracts/cluster-release-compatibility-v1.md): same major and minor distance at most one; patch does not affect the compatibility decision. N-1 and N must safely interoperate for cluster wire, persisted state, snapshot and management behavior.
- Replicated state는 strict decode다. 새 field·command는 feature activation 뒤에만 기록하며 activation이 해당 feature의 rollback window를 끝낸다([Replicated-state v1 §3](../contracts/replicated-state-v1.md#3-호환성-규칙)).
- Rollback window가 끝나기 전에는 N-1이 해석하지 못하는 persisted state를 기록하는 irreversible migration을 해서는 안 된다. Window 종료 판단과 activation 승인 절차는 [Release contract §4.1](../contracts/cluster-release-compatibility-v1.md#41-운영-승인과-rollback-window-종료)을 따른다.
- N에서 N-1로 rollback할 때 이미 기록된 상태를 손상하거나 silent downgrade해서는 안 된다.
- 호환할 수 없는 변경은 rolling update를 시작하기 전에 preflight에서 거절해야 한다.
- 호환 matrix는 release artifact에 포함해야 한다.

Retained, offline, durable delivery, node drain and tenant quota replication require a fresh read barrier and a profile gate across all non-retired members. Cluster ID, cluster protocol and state schema remain exact; releases must share a major and fit the one-minor compatibility window. Member release and features are mutable committed profiles. After restart, a node is not READY until its committed profile matches its build identity. Feature advertisement is carried by committed member profile, not peer hello. `FeatureActivation` commits only when every non-retired member advertises the feature. A build that does not support an active feature is not READY and reports `incompatible_state_schema: ...`; its profile update is rejected. Periodic profile reconciliation keeps the last synchronized state while the sequential `Members` and `ActiveFeatures` reads are pending, then applies the decision only after both complete. A read error, wait, mismatch/proposal, local committed profile change or snapshot restore closes the gate. A local committed profile change or snapshot restore increments the revision, so completion from a pre-change read cannot reopen readiness. Unsupported feature activation or restored unsupported feature state latches incompatibility before readiness can reopen.

Session metadata는 `FeatureActivation`으로 `session-metadata`가 활성화된 뒤에만 제안한다. 비활성 상태에서는 session metadata payload를 포함하지 않는다.

## 9. Rebalance

[ADR-0033](../decisions/ADR-0033-connection-rebalance-operation.md)에 따라 `connection-rebalance` feature 활성화 뒤 운영자가 `POST /api/v1/cluster/rebalance`로 일회성 operation을 시작해야 한다. `maxDeviationConnections`는 절대 연결 수 `1..1_000_000`이며 rate·LB 전파 대기·timeout과 함께 필수다. 제품 기본값은 없다. GET은 committed 상태를 조회하고 DELETE는 Running operation을 취소해야 한다.

Participant는 START 시점 VOTER로 고정하며 최소 2개여야 한다. 각 participant는 CONNECT로 묶인 자기 연결 수를 보고해야 한다. `fairShare = ceil(total/n)`, `ceiling = fairShare + maxDeviationConnections`다. 모든 보고가 fresh이고 모든 연결 수가 ceiling 이하이며 shedding 중인 node가 없으면 COMPLETED로 commit해야 한다. 미보고 또는 나이가 15초를 넘는 보고는 stale이다. 같은 값도 5초마다 다시 보고해야 한다.

Fresh 보고에서 local 연결 수가 ceiling을 초과하면 신규 CONNECT admission과 readiness를 닫아야 한다. Readiness reason은 `connection rebalance shedding`이다. 설정한 LB 전파 대기 뒤 fairShare까지 rate 제한 TRANSFER를 수행해야 한다. 이전 뒤 5초간 재접속을 기다리고 admission을 열어야 한다. MQTT 5는 `DISCONNECT 0x9C`와 선택 Server Reference로 종료하고 MQTT 3.1.1은 packet 없이 닫아야 한다.

Running rebalance와 active drain은 apply에서 서로 배제해야 한다. Timeout은 `FAILED`와 `rebalance_timeout`으로 commit해야 한다. 취소는 `CANCELLED`로 commit하며 이미 이전한 epoch를 되돌리지 않는다. Completed·Failed의 취소는 `409 rebalance_not_cancellable`이다. Terminal 상태에서 rebalance admission은 열어야 한다. Participant 손실·제거는 stale 보고로 timeout이 될 수 있다. Client가 LB를 통해 재접속할 node는 broker가 통제하지 않는다.

Rebalance는 ownership fencing과 이미 복제된 delivery state를 보존해 successor가 새 epoch의 ATTACH로 재개하도록 해야 한다. QoS 전송·진행 상태는 wire effect보다 먼저 replicated command로 commit되며, rebalance를 위해 별도의 checkpoint authority를 두지 않는다.

## 10. Snapshot, backup과 restore

1. Snapshot은 포함한 commit index와 typed-state checksum을 가져야 한다. Finalize는 snapshot file sync, close, atomic rename, directory sync 순서이며, 그 경계에서 process crash가 발생해도 재시작 시 완전한 snapshot/WAL만 replay한다.
2. Backup은 commit 경계와 일치하는 typed snapshot, 그 뒤의 committed application/configuration 기록, schema/release metadata 및 checksum을 포함해야 한다. Backup capture는 fresh quorum/read barrier를 거쳐야 한다.
3. 전체 CLU-14 target에는 tenant, session ownership, persistent subscription, retained, QoS delivery/inflight, offline state와 committed tenant quota profile이 포함된다. Current [Cluster backup v3](../contracts/cluster-backup-v3.md)는 archive format `3.0`을 유지하고 required `inflight` kind summary에 delivery session의 outbound message와 inbound QoS 2 상태를 포함한다. Count는 모든 `DeliverySessionState`의 `len(messages)+len(inbound)` 합이며 digest는 `delivery_sessions`만 가진 typed snapshot을 사용한다. Tenant digest에도 delivery record tenant를 포함한다. Tenant quota profile은 replicated typed snapshot과 whole-state SHA-256에 포함하고 별도 kind summary를 만들지 않는다. Historical v2는 retained만, historical v1은 retained/offline 모두 제외했다. Linux/EBS power-loss와 retention/RPO/RTO는 여전히 미완료다.
4. Restore는 snapshot과 committed tail을 replay한 뒤 manifest의 commit-boundary count/checksum과 비교해야 한다. Format, identity, version, member identity 또는 summary가 일치하지 않으면 target storage를 바꾸기 전에 거절한다.
5. Current v3 restore는 동일 cluster·protocol/schema를 요구하고 producer와 모든 voter release는 restore build와 같은 major·minor 차이 1 이하인 N/N-1 window 안이어야 한다 ([ADR-0021](../decisions/ADR-0021-backup-archive-release-window.md)). Commit 경계 state의 active feature를 restore build가 지원하지 않으면 target storage 변경 전에 거절한다. 기존 strict decode·replay·summary 검증은 유지하며 모르는 state를 무시하지 않는다. 정확히 세 bootstrap voter ID와 동일 address/DNS SAN/certificate fingerprint를 요구하며 learner ID가 있으면 거절한다. V1/v2 archive, cross-cluster migration과 voter-set 변경은 지원하지 않는다. Restore는 member profile을 바꾸지 않으며 startup 뒤 build profile commit·재확인 전에는 READY가 되지 않는다.
6. Restore는 offline CLI로 voter별 빈 target directory를 seed한 뒤 종료한다. Durable restore marker가 남은 node는 일반 startup에서 READY가 되면 안 된다.
7. Retention 기간과 disaster-recovery RPO/RTO(`SPEC-TBD-09`)는 2026-10-13 사용자 결정으로 일 1회 정기 백업, 30일 보존, 전체 클러스터 재해 복구 RPO 24시간·RTO 1시간으로 정한다(단일 노드 장애는 3-node Raft quorum으로 RPO 0 유지). S3는 새 백업의 30일 보존 권위자이고 기존 PVC는 staging만 담당한다. Bucket은 same-region private이며 public access block, TLS-only policy, SSE-S3, 30일 lifecycle expiration, incomplete multipart upload 1일 abort를 적용한다.
   Full RTO는 source와 network가 분리된 fixture에서 restore start부터 세 voter의 readiness·quorum·복구 동작까지 측정해야 한다. 30일 경과 retention과 지속적 RPO/RTO는 열린 acceptance gate다.
8. 새 archive는 backup API의 기존 commit-index와 body/trailer SHA 검증을 통과한 뒤 persistent backup PVC의 비공개 임시 directory에 bounded JSON request record와 함께 준비한다. Request record는 UTC timestamp, commit index, full-file SHA-256, safe staged filename을 가진다. 두 파일이 모두 준비된 directory를 같은 PVC의 `<archive filename>.staged`로 원자적으로 rename해 게시한다. 업로더는 게시된 directory만 처리한다. 게시 전 Pod가 종료되면 불완전한 임시 directory는 업로드하지 않는다. 게시 후에는 archive와 request가 함께 살아남아 Pod/Job 교체 뒤에도 재시도된다. S3 활성 mode에서는 per-run ready/done/result marker를 Pod UID별 ephemeral directory에 두고, 각 새 backup 전에 request-backed pending archive를 먼저 업로드한다. S3 요청 실패나 checksum mismatch 때 archive와 request를 보존하고, matching `PutObject` SHA256 response 뒤에만 두 파일과 빈 게시 directory를 제거한다. Request record가 없는 legacy file은 자동 이전하지 않으며, 기존 `mtime +30` 로컬 정리 경계를 유지한다. 이는 Pod 종료 경계의 규칙이며 물리 전원 손실 내구성 증거를 대신하지 않는다.
   미게시 잔여의 복구·정리는 운영자가 모든 backup producer·uploader를 중지한 maintenance 상태에서만 수행한다. CronJob suspend만으로 수동 Job의 중지를 보장하지 않는다. `s3_uploader.py recover-stage <정확한 .staging-* 이름> --offline`은 한 요청·archive 쌍의 metadata와 전체 파일 SHA-256을 확인한 뒤 기존 게시 directory를 덮어쓰지 않고 함께 게시한다. 새 metadata나 commit index를 추정하지 않는다. 불완전·손상 잔여는 다른 유효한 백업과 필요한 증거를 보존한 뒤 `discard-stage <정확한 이름> --offline`으로 그 directory만 정리할 수 있다. 두 명령 모두 자동 스캔·나이 기준 삭제·AWS 호출을 하지 않는다. 경로 이탈, symlink·하위 directory·예상 밖 파일은 거절하고 원본을 남긴다. 동시에 다른 쓰기 작업을 실행하면 안 된다. 이 절차는 물리 전원 손실의 directory durability나 archive 복원 적합성을 보장하지 않는다.
9. Object key는 cluster prefix, UTC date, UTC timestamp, commit index와 full-file SHA-256을 포함한다. Response-loss retry는 같은 SHA-derived key에 같은 bytes만 다시 `PutObject`하며 `If-None-Match`를 사용하지 않는다. Writer IRSA trust는 `system:serviceaccount:narumq:narumq-backup`만 허용하고 해당 prefix의 `s3:PutObject`만 부여한다. Writer는 list/read/delete 권한을 갖지 않는다. 기본 `deploy/kubernetes/eks-ec2-ha` CronJob은 S3 설정이나 sidecar 없이 로컬 timestamp archive를 생성한다. Production shared Terraform root `deploy/terraform`의 optional `backup_s3_enabled`(default `false`)를 켠 뒤에만 `deploy/kubernetes/eks-ec2-ha-s3` overlay를 적용한다. Separate `deploy/terraform-acceptance-s3` acceptance root에는 이 변수가 없고 `main.tf`가 S3 module을 직접 호출하므로, 해당 fixture resources는 operator가 그 root를 명시적으로 apply할 때만 생성된다. 선택한 root가 필요한 resources를 설치한 뒤에만 S3 overlay를 적용한다. 이 overlay는 pinned AWS CLI Python native sidecar, S3 설정 ConfigMap 참조와 helper ConfigMap을 추가한다. AWS credential projection은 transfer sidecar에만 두고 broker container에는 AWS CLI나 runtime package installation을 추가하지 않는다.
10. 별도 restore IRSA trust는 `system:serviceaccount:narumq:narumq-backup-restore`만 허용한다. `s3:ListBucket`은 정확한 cluster prefix로 제한하며 `s3:GetObject`는 그 prefix object만 허용한다. S3 overlay 디렉터리의 on-demand list Job으로 operator가 exact content-addressed key를 선택하고, on-demand per-voter seed Jobs가 그 exact key를 내려받는다. 이 Job manifest는 기본과 S3 apply Kustomize package 모두에 포함하지 않는다. Download helper는 exact-key S3 checksum response, content length와 full-file SHA-256을 검증한 뒤 기존 offline `-cluster-restore` CLI에 전달한다. `deploy/kubernetes/eks-ec2-ha-restore-mode`는 S3 자원 없는 local restore overlay로 유지한다. S3 restore에는 `deploy/kubernetes/eks-ec2-ha-s3-restore-mode`를 사용한다. 이 overlay는 S3-enabled CronJob을 suspend하고 StatefulSet replicas를 0으로 유지하며 volumeClaimTemplate과 같은 이름의 세 빈 voter PVC를 제공한다. Non-empty target을 지우거나 덮지 않고 거절한다. Existing source/fixture PVC에 적용해서는 안 된다.
11. 원래 commit-index header를 갖지 않는 legacy PVC archive는 S3로 자동 이전하지 않고, `NMQBKUP3`를 파싱하거나 metadata를 추정하지 않는다. Legacy archive는 기존 local `mtime +30` cleanup 경계를 유지한다. S3 30일 retention과 연관 acceptance는 새 CronJob이 생성하고 업로드한 request-backed archive부터 시작한다.

## 11. 수용 시험

테스트 연결과 결과는 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서로 확인한다. 과거 실행 기록은 [검증 기록](../verification/cluster-session-and-delivery.md)에 있다. 시나리오 ID는 `CLU-SCN-NNN`이며 아래 표가 기준 정의다.

- 3-node에서 publisher와 subscriber를 서로 다른 node에 연결해 모든 QoS와 subscription 유형을 시험한다.
- 동시 CONNECT, owner kill, packet partition과 stale epoch write 거절을 검증한다.
- quorum 상실 중 새 ownership과 durable ACK가 성공하지 않는지 검증한다.
- 성공 durable ACK 직후 각 node를 kill해 message gap이 없는지 확인한다.
- disk full, snapshot finalize crash, archive/restore·restart/resync 중 consistency와 readiness를 확인한다. Production EBS/power-loss는 별도 미완료 기준이다.
- N-1↔N rolling upgrade와 rollback을 양방향으로 시험한다.

### 11.1 Phase 0 gate 4 deterministic spike mapping

아래 첫 표의 네 시나리오는 `internal/adapters/raft`의 in-process deterministic harness가 검증하는 제한된 증거다. 각 시나리오는 독립 temporary directory와 FIFO `MessageDTO` delivery를 사용한다. 이어지는 두 failure-bound 시험은 각각 WAL adapter unit test와 in-process cluster harness를 쓰는 부분 증거이며, 실제 디스크 장애나 production transport를 검증하지 않는다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-001 | CLU-01, CLU-02, CLU-06, CLU-07, CLU-09, CLU-10 | `leader failure preserves committed state` | node 1 장애 뒤 node 2가 증가한 term으로 선출되고 `[alpha,beta]`가 surviving node에 유지된다. |
| CLU-SCN-002 | CLU-01, CLU-02, CLU-06, CLU-07, CLU-09, CLU-10 | `quorum loss cannot commit` | quorum 없는 old leader가 `gamma`를 apply하지 않으며 반복 proposal은 `ErrBackpressure`로 수렴한다. |
| CLU-SCN-003 | CLU-01, CLU-02, CLU-06, CLU-07, CLU-09, CLU-10 | `WAL replay restores commit` | bootstrap snapshot 뒤 reopen한 node가 `[alpha]`를 복구하고 재가입 뒤 `[alpha,beta]`로 수렴한다. |
| CLU-SCN-004 | CLU-01, CLU-02, CLU-06, CLU-07, CLU-09, CLU-10 | `snapshot plus WAL tail restores state` | snapshot index 이후 WAL tail을 replay해 `[alpha,beta,gamma]`와 동일한 applied/commit 경계를 복구한다. |

#### WAL write failure와 slow-follower 제한의 부분 증거

아래 시험은 특정 어댑터 동작과 Raft의 in-process flow-control window에 관한 부분 증거다. `Open`의 `MaxInflightMsgs=16`은 entry가 포함된 고유 `MsgApp` 범위에 적용된다. entry가 없는 `MsgApp`은 commit index를 전달할 수 있어 이 계수에 포함하지 않는다. 측정값은 총 outbound 메시지 수, transport queue 크기 또는 retained Raft log 크기의 한계를 뜻하지 않는다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-005 | CLU-07, CLU-12 | `WAL write failure stops outbound` | bootstrap 이후 해당 node의 `wal.bin` write에만 주입한 `ENOSPC`를 WAL worker 완료에서 인수하면 `ErrFailed`를 반환하고 node는 terminal failed state로 전이한다. 실패 batch는 비고 applied command는 바뀌지 않는다. 주입 제거 후 `Campaign`, `Propose`, `ProcessReady`도 계속 `ErrFailed`이며 `Close`는 worker를 join하고 정상 작동한다. |
| CLU-SCN-006 | CLU-07, CLU-12 | `slow follower keeps outbound append window bounded` | 1↔3 및 2↔3 route를 막은 동안 node 1·2가 순서가 고유한 1 KiB command 64개를 모두 같은 순서로 적용하고 node 3은 적용하지 않는다. node 1→3의 미응답 entry-bearing `MsgApp` 고유 범위는 `Open`의 `MaxInflightMsgs=16`에 따라 16개 이하로 제한된다. route 복구 뒤 최대 20 logical tick 안에 세 node가 같은 command 순서로 수렴한다. |

### 11.2 Phase 2 replicated session ownership core mapping

아래 다섯 시나리오는 `internal/application/takeover`가 application committed-command 순서로 세션 상태를 적용하고 `internal/adapters/raft`가 opaque bytes를 운반하는 in-process deterministic 3-node harness의 제한된 증거다. Acquire bytes는 `takeover`의 experimental codec으로 해석하며, subscription·inflight bytes는 해당 시험 안에서만 typed DTO를 찾는 token으로 해석한다. NMQSTATE1 snapshot/WAL의 재시작 복구 증거는 acquire command에만 해당하고 새 mutation type의 persisted encoding·재시작 replay를 증명하지 않는다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-007 | CLU-04, CLU-05, CLU-10 | `concurrent acquire elects single owner` | 동일 `(tenantId, clientId)`에 동시 acquire 시 leader에서 먼저 commit된 owner만 epoch 1을 얻고 후속 제안은 `stale_epoch`로 거절되며 모든 node가 동일 상태로 수렴한다. |
| CLU-SCN-008 | CLU-04, CLU-05, CLU-10 | `minority cannot acquire or advance epoch` | partition된 minority node의 ownership proposal은 commit되지 않으며 majority에서 새 leader가 epoch 2를 commit한 뒤 takeover한다. partition 복구 시 minority는 majority의 epoch 2로 수렴하고 이전 epoch 1 owner의 authorize는 거절된다. |
| CLU-SCN-009 | CLU-04, CLU-05, CLU-10 | `restart recovers ownership epoch and command sequence` | snapshot과 WAL tail을 가진 node가 재시작 후 opaque command stream을 replay했을 때 restart 전과 동일한 session owner, epoch, application committed-command sequence position으로 복구된다. |
| CLU-SCN-010 | CLU-04, CLU-05, CLU-10 | `lost commit response retry preserves owner and epoch` | 동일한 opaque command payload 두 개가 모든 node에 commit되고 두 번째 retry가 `stale_epoch`로 거절되며, ownership은 node 1 / epoch 1 / sequence position 1에 남고 state machine은 application sequence position 2까지 진행한다. |
| CLU-SCN-011 | CLU-04, CLU-05, CLU-10 | `stale owner session mutations are fenced after takeover` | 5개 명령이 적용된 뒤 node 2/epoch 2 takeover의 ownership position은 5다. 미리 준비한 이전 owner의 persistent subscription 및 inbound/outbound QoS inflight 명령 세 개가 뒤이어 commit되어 3개 node 모두 application sequence 8까지 진행하지만 각각 `stale_epoch`로 거절된다. 구독 `orders/#` QoS 1, inbound `[10]`, outbound `[20]`과 각 node의 파생 route projection은 takeover 직후와 동일하게 유지된다. |

### 11.3 Phase 0 gate 4 production peer transport mapping

이 mapping은 Accepted [ADR-0008](../decisions/ADR-0008-raft-peer-transport.md)과 Active [Raft peer transport v1 contract](../contracts/raft-peer-transport-v1.md)의 고정 voter `{1,2,3}` 전송 경로만 닫는다. 실행 가능한 composition root는 `cmd/narumq -raft-peer-config`이며 일반 MQTT listener와 통합되지 않는다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-012 | CLU-01, CLU-02 | Contract wire vectors와 framing limits | Canonical hello/frame bytes, protocol version validation, zero/oversized/truncated frame 경계가 contract와 일치한다. |
| CLU-SCN-013 | CLU-01, CLU-02 | mTLS identity 및 invalid peer 입력 | Foreign CA, allowlisted 다른 identity 주장, cluster/version mismatch, protobuf envelope mismatch, malformed protobuf, oversized/truncated frame은 state/command에 적용되지 않는다. Production driver는 malformed protobuf를 보낸 peer 연결을 종료한다. |
| CLU-SCN-014 | CLU-01, CLU-02 | Bounded queue, backpressure와 quorum progress | Queue count/byte cap을 지킨다. Peer 3 outbound queue가 가득 차도 node 1·2는 election과 command apply를 진행하고, queue가 drain된 뒤 Raft가 peer traffic을 재생성한다. 같은 Ready batch의 거절된 peer 후속 frame은 건너뛰며 healthy peer 전송은 계속한다. Inbound staging overflow는 해당 connection을 닫고 cancellation은 queued-byte accounting을 비운다. 초과 inbound socket도 accept되지 않는다. |
| CLU-SCN-015 | CLU-01, CLU-02 | TLS 3-node replication과 reconnect | Node 3을 끊은 동안 majority가 progress하고, 재연결 후 Node 3이 commit된 command까지 catch up한다. |
| CLU-SCN-016 | CLU-01, CLU-02 | Peer-only CLI lifecycle | 3개 별도 peer-only process가 TLS election을 수행하고 WAL을 남긴다. Cancellation 뒤 listener/socket이 닫히며 MQTT `READY`는 출력되지 않는다. |

### 11.4 ADR-0009 design target과 module checksum 증거

[ADR-0009](../decisions/ADR-0009-raft-production-state-profile.md)은 네 가지 design target만 채택하며 runtime acceptance는 아니다. 아래 시나리오는 `go.etcd.io/raft/v3@v3.7.0`의 module checksum을 확인한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-017 | CLU-14 | `go mod verify`와 `go mod download -json go.etcd.io/raft/v3@v3.7.0`을 실행한다. | `go mod verify`는 `all modules verified`를 출력한다. Version은 `v3.7.0`, `Sum`은 `h1:BGzlwx07bLv8PW6OU5HObuz1y4hlPZUXA07pM1mPUh4=`, `GoModSum`은 `h1:6gX6T2X907DjnjsFLODnTxba77stjs84W9gTTI0GUNA=`이며 두 값은 `go.sum`과 ADR-0009의 선택값과 일치한다. |

### 11.5 Dynamic membership·restart convergence·MQTT readiness mapping

이 section은 production storage·disk-full·backup/restore를 판정하지 않는다. Backup v1의 별도 process evidence는 §11.7이며, durable QoS ACK·release N/N-1은 미완료 수용 범위다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-018 | CLU-01, CLU-02 | Three-voter replacement and retired ID | `{1,2,3}`에서 새 ID `4` learner를 catch up·promote하고 planned removal로 `{1,2,4}`가 된다. ID `3` 재사용은 거절되고 나머지 두 voter는 한 node loss 뒤 quorum을 유지한다. |
| CLU-SCN-019 | CLU-01, CLU-02 | Idempotent join/leave | Stage, promotion, leave/remove 각 응답을 유실한 뒤 같은 key로 재요청하면 같은 operation ID와 최종 voter set으로 수렴한다. 중복 promotion/config change는 없다. |
| CLU-SCN-020 | CLU-01, CLU-02 | Compatibility and identity rejection | Foreign CA, SAN/fingerprint/cluster/ID mismatch, unsupported release/protocol/schema, malformed hello/state record는 membership log에 commit되지 않고 `Step`에 도달하지 않는다. |
| CLU-SCN-021 | CLU-01, CLU-02 | Dialer-side certificate check before hello (ADR-0061) | 연결을 거는 쪽이 기대한 멤버의 인증서가 아니면 자신의 hello를 보내지 않고 닫는다. 같은 주소의 다른 멤버가 가진 정상 연결을 교체하지 않는다. |
| CLU-SCN-022 | CLU-01, CLU-02 | Learner TCP dial and peer-phase delivery boundary | Unchanged peer identity updates retain queued/in-progress outbound frames. `InboundReady` coalesces staged-frame notifications; `TakeInbound` leases one frame, and the cluster driver completes it after `Step`. Reconciliation blocks new leases and waits for active leases before advancing identity revision and discarding prior staging. Forwarded Raft requests preserve their original requester only when both it and the authenticated relay are committed members. |
| CLU-SCN-023 | CLU-01, CLU-02 | Transition-boundary crash/restart | ConfChangeV2 Ready persistence 전후, snapshot transfer 중, local finalize 후, promotion/remove 전후에서 같은 volume으로 재시작하면 committed ConfState·phase·operation·tombstone·typed state가 수동 수정 없이 수렴한다. 빈 Raft entry가 typed event 뒤에 오더라도 snapshot index는 Raft applied index와 일치한다. |
| CLU-SCN-024 | CLU-01, CLU-02 | Readiness and MQTT admission | Fresh join/restart, applied-index gap, route projection rebuild failure or quorum barrier failure 시 `/health/ready`는 503이고 신규 CONNECT는 긍정 CONNACK을 받지 않는다. Committed subscription projection과 fresh quorum proof 후에만 readiness가 200이 되고 cross-node routing이 일치한다. |
| CLU-SCN-025 | CLU-01, CLU-02 | Quorum loss and planned leave | quorum loss 또는 successor ownership transfer 실패 중 readiness와 admission이 내려가고 기존 voter removal은 commit되지 않는다. Transfer가 commit되면 replicated subscription과 durable delivery state를 새 owner가 다음 CONNECT의 ATTACH로 재개하고, 그 뒤에만 planned removal이 완료된다. |

### 11.6 Storage fault chaos mapping

이 시험은 용량 제한 tmpfs를 ENOSPC까지 채우고 FUSE에서 file/directory sync 지연·오류, 보류된 WAL fsync와 제어된 power cut을 주입한다. MQTT CONNECT/SUBSCRIBE 결과는 readiness와 admission 관찰이며 durable ACK가 아니다. 각 scenario는 applied index와 peer queue도 별도로 기록한다. Raft follower의 authenticated proposal forwarding은 `internal/adapters/raft.TestDynamicFollowerForwardsApplicationProposal`로 검증한다. §11.6의 durable ACK extension은 별도 MQTT PUBLISH·ACK·delivery sentinel을 확인한다. Held fsync의 cut-generation 회귀 시험은 release→remount→late-resume 순서에서 stale callback을 거부한다. FUSE journal의 rollback은 실제 전원 손실이나 production EBS 검증이 아니다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-026 | CLU-07, CLU-12 | `follower-disk-full` | Follower의 filesystem이 실제 ENOSPC에 도달하고 `FAILED`, `/health/ready`·`/health/live` 503, `storage_no_space`, `storagePressure=true`를 보인다. 관리 HTTP는 유지되고 Follower의 MQTT probe는 거절된다. Leader의 progress probe는 성공하고 outbound queue count/bytes가 제한 안에 있으며 queue cap 도달을 관찰한다. Leader와 healthy follower의 applied index가 증가한다. 공간 확보·restart 뒤 follower가 READY로 돌아와 leader를 따라잡고 fresh client와 원래 baseline client의 MQTT CONNECT/SUBSCRIBE가 수락된다. |
| CLU-SCN-027 | CLU-07, CLU-12 | `quorum-disk-full` | 두 follower가 실제 ENOSPC로 `FAILED`가 된 뒤 leader의 readiness는 503, `quorumAvailable=false`다. 20초 동안 leader MQTT progress probe는 성공하지 않고 applied index는 변하지 않으며 peer queue는 제한 안에 있다. 두 follower의 공간 확보·restart 뒤 세 node가 READY가 되고 applied index가 수렴하며 각 node의 MQTT probe와 원래 baseline client reconnect가 수락된다. |
| CLU-SCN-028 | CLU-07, CLU-12 | `follower-slow-io` | 별도 I/O probe가 FUSE write/`fsync` 지연을 확인한다. Follower는 `DEGRADED`, readiness 503, liveness 200, `storage_slow`, `storagePressure=true`를 보이고 MQTT admission을 거절한다. Leader progress probe는 성공하고 peer queue가 제한 안에 있다. Delay 제거와 10초 recovery window 뒤 follower는 `HEALTHY`가 되며 process 재시작은 없다. Recovery 후 fresh client와 원래 baseline client의 MQTT CONNECT/SUBSCRIBE가 수락된다. |
| CLU-SCN-029 | CLU-07, CLU-12 | `follower-sync-error` | Follower file `fsync` 실패 뒤 `FAILED`, `storage_io_error`, readiness/liveness 503 및 MQTT rejection을 보인다. Leader는 READY 상태로 progress를 계속하고 bounded peer queue를 유지한다. 오류가 남은 상태의 broker reopen은 fail-closed로 종료되거나 readiness 503을 유지해야 한다. 오류 제거·restart 뒤 follower가 applied index를 따라잡고 fault 중 ACK된 QoS 1/2를 sentinel 이전에 전달한다. |
| CLU-SCN-030 | CLU-07, CLU-12 | `follower-torn-power-loss` | Follower의 WAL fsync를 보류한 동안 healthy quorum이 durable QoS 1을 ACK한다. Follower 전원 차단에서 unsynced WAL tail을 torn 상태로 남기고, power-on·recovery 뒤 commit과 applied index를 수렴시키며 ACK된 payload를 한 번 전달한다. |
| CLU-SCN-031 | CLU-07, CLU-12 | `cluster-power-loss` | In-flight QoS 1 publisher가 네 건의 ACK를 받은 뒤 `client.publish`로 PUBLISH packet을 성공적으로 쓴다. Write 완료 뒤 barrier를 signal하고 `awaitAck` 전에 대기한다. Barrier 뒤 세 노드의 power cut을 동시에 시작하며 모든 cut 완료 전에는 전원을 켜지 않는다. 한 follower에는 WAL fsync 보류와 torn tail을, 나머지 노드에는 clean cut을 적용한다. 복구 뒤 readiness, leader 합의, applied-index 수렴과 persistent `SessionPresent`를 확인한다. 원래 leader와 복구 후 leader를 별도로 기록한다. Cut이 connection을 끊어 PUBACK 관찰을 중단한 해당 PUBLISH만 `unknown`으로 기록한다. 알려진 `timeout`/`closed` 결과는 unacked로 남기되 unknown evidence에 포함하지 않는다. 해당 payload는 0회 또는 1회 전달을 허용하고 중복은 거부한다. |

**ADR-0003 durable ACK 확장:** Runner는 durable subscriber의 persistent subscription을 세 node에 복제했는지 확인하고, follower disk-full·slow-I/O·sync-error에서 leader와 healthy follower가 fault 중 QoS 1/2를 성공 ACK한 뒤 sentinel 이전에 한 번 전달하는지 확인한다. Slow-I/O scenario의 durable subscriber는 fault target follower에 둔다. Quorum disk-full에서는 fault 전 QoS 1/2 publish를 ACK하고, quorum loss 뒤 QoS 1/2 no-ACK, persistent QoS 2 same-packet 재개 및 QoS 1 at-most-once를 확인한다. Torn-power-loss에서는 held WAL fsync 중 ACK된 QoS 1을 torn cut 뒤 복구한다. Cluster-power-loss에서는 세 node 동시 power-cut과 in-flight publisher를 거친 뒤 ACK된 QoS 1/2를 복구하고 persistent SessionPresent를 확인한다. 관련 시나리오는 [§11.13](#1113-adr-0003-durable-ack-fault-수용-매핑)에 있다.

### 11.7 Cluster backup/restore와 snapshot finalize 수용 매핑

이 수용 시험은 historical v2 archive의 commit 경계, identity와 local process restore/restart 범위를 확인한다. Retained-specific assertions는 §11.8에 연결하며, current archive와 offline queue는 §11.9를 따른다. Restore는 정확히 세 bootstrap voter를 요구하고 learner archive와 v1 bytes를 거절한다. Production EBS power-loss, retention/RPO/RTO와 archive N/N-1 compatibility는 미완료다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-032 | CLU-14 | Online boundary archive | ClusterAdmin API가 fresh quorum barrier 뒤 v2 binary archive를 반환한다. Commit header가 manifest commit과 같고 committed tail이 snapshot 뒤 archive에 포함된다. |
| CLU-SCN-033 | CLU-14 | Archive validation and identity | Corrupt/noncanonical/checksum·summary 오류, cluster/member ID·address/SAN/certificate·release/schema mismatch, learner 및 v1 archive는 target을 바꾸기 전에 거절한다. |
| CLU-SCN-034 | CLU-14 | Offline restore, crash/restart and resync | 같은 v2 archive를 세 bootstrap voter directory에 seed한다. Staged operation과 retained state가 유지되고, restored node crash/restart 뒤 quorum READY, 신규 joiner catch-up과 replay가 수렴한다. |
| CLU-SCN-035 | CLU-14 | Snapshot finalize process crash | Temp file Sync 뒤 rename 전 crash와 rename/directory sync 후 crash에서 process restart 뒤 committed membership과 readiness가 수렴한다. |
| CLU-SCN-036 | CLU-14 | Production volume boundary | EBS/전원 손실 경계에서 durable commit을 보존한다. |

### 11.8 Retained replication과 backup v2 수용 매핑

Retained key는 `(tenantId, exact Topic Name)`이다. 이 매핑의 cluster MQTT ingress 범위는 QoS 0 PUBLISH다. QoS 1/2 retained PUBLISH는 [§11.32](#1132-qos-12-retained-수용-매핑)에서 확인한다. Retained mutation은 Raft commit 뒤 다른 node의 replay projection에 반영되고, v2 snapshot/archive는 같은 committed retained state와 kind summary를 보존한다. Offline queue와 durable QoS 1/2 ACK는 이 acceptance 범위가 아니다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-037 | CLU-08, CLU-14 | Tenant-scoped retained state와 deterministic limits | 같은 topic의 다른 tenant 값은 분리된다. Replace/delete, proposal-time expiry, 전체 tenant 합산 1,024 object/16 MiB/1,000,000-byte payload limit, per-tenant quota 미제공과 canonical snapshot을 검증한다. |
| CLU-SCN-038 | CLU-08, CLU-14 | Commit-before-route와 projection recovery | Retained commit은 route matching보다 먼저 일어난다. Commit error에서 fan-out하지 않고, snapshot/startup projection은 committed state에서 rebuild한다. |
| CLU-SCN-039 | CLU-08, CLU-14 | Live cross-node retained replay | Node 1에서 retained `a=alpha`, `b=beta`를 publish하고 `b`를 삭제한 뒤 `c=gamma`를 publish한다. 다른 node가 정확히 `{a:alpha,c:gamma}`를 replay하며 삭제한 값은 나타나지 않는다. |
| CLU-SCN-040 | CLU-08, CLU-14 | v2 backup, restore, restart와 catch-up | Archive format major 2, retained summary count 2와 `not_replicated_kinds=["offline"]`를 확인한다. 세 voter restore, restored-node crash/restart와 learner catch-up 뒤 모두 `{a:alpha,c:gamma}`를 replay한다. |

### 11.9 Offline queue replication과 backup v3 수용 매핑

Offline key는 `(tenantId, clientId)`다. Cluster mode에서 persistent session이 detached 상태일 때 도착한 QoS 0 message를 Raft committed state machine에 복제하고, reconnect 때 새 owner가 CLAIM으로 가져간다. Backup v3(`NMQBKUP3`)는 retained와 offline kind summary를 필수로 포함하고 v1/v2 archive를 거절한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-041 | CLU-07, CLU-14 | Offline FIFO, fencing and capacity limits | Detach, enqueue, claim이 sequence 1..N 순서와 payload를 복구한다. Stale epoch/non-owner claim과 epoch 불일치 enqueue는 거절된다. Session당 64개, 전체 4,096개·16 MiB, payload 1,000,000 bytes 초과는 `offline_capacity_exceeded`로 거절되며 기존 state를 보존한다. Expiry purge는 proposer 시각 기준이다. |
| CLU-SCN-042 | CLU-07, CLU-14 | Connect and publish offline integration | Disconnect 시 persistent session pending queue를 DETACH하고 local queue를 제거한다. DETACH 실패 시 local queue를 보존한다. CleanStart=0 CONNECT의 CLAIM 성공 시 SessionPresent=true와 복구 queue를 설치한다. CLAIM 실패 시 CONNECT와 local session 변경을 rollback하며, 새 session이면 queue 등록 및 tenant route binding을 제거하고, 기존 session이면 queue·route·expiry timer를 보존한다. Detached 대상 publish는 Raft ENQUEUE를 호출한다. |
| CLU-SCN-043 | CLU-07, CLU-14 | Backup v3 and historical v1/v2 archive rejection | V3 archive format major 3, required `offline` kind count/digest와 offline tenant digest를 검증한다. Format 1과 format 2 archive는 `unsupported archive format N`으로 명시 거절하며 target directory를 변경하지 않는다. |
| CLU-SCN-044 | CLU-07, CLU-14 | Real-cluster cross-node offline enqueue and owner crash delivery | Node 1 detached subscriber로 Node 2가 publish한 QoS 0 message 세 개를 복제한다. Owner node 1 crash 뒤 Node 3으로 reconnect 시 CONNACK `SessionPresent=true`와 세 message가 FIFO 순서로 1회씩 도착한다. |
| CLU-SCN-045 | CLU-07, CLU-14 | Backup v3 restore, restart and joiner convergence | Node 1 재시작 후 네 번째 message가 offline queue에 든 상태에서 backup v3를 뜬다(format 3, retained count 2, offline count 3). 세 voter restore 후 restored node 2 reconnect에서 네 번째 message가 도착한다. 이후 발행된 다섯 번째 message는 joiner reconnect에서 도착한다. Restored node와 joiner의 backup summary가 동일 commit index에서 일치한다. |
| CLU-SCN-046 | CLU-07, CLU-14 | Live EKS backup와 offline seed 부분 검증 | 정기 backup CronJob(`0 2 * * *`) 배포와 local PVC `mtime +30` cleanup policy, 온라인 NMQBKUP3 backup integrity와 Commit Index 100, ConfigMap 설정을 이용한 clean-directory 3-voter offline seed 소요 시간을 확인한다. 전체 cluster RTO는 복원 node 기동·quorum 회복까지 포함한다. |
| CLU-SCN-047 | CLU-07, CLU-14 | EKS backup PVC retention script probe | 실제 backup PVC에 둔 단 하나의 빈 `backup-*.nrbak` probe를 30일보다 오래된 synthetic mtime으로 설정하면 배포된 CronJob script가 삭제하고, 기존 archive를 보존하며 새 backup을 생성한다. |
| CLU-SCN-048 | CLU-07, CLU-14 | S3 요청 기록 기반 백업 전송과 오프라인 복원 | 새 격리 시험 환경에서 새 아카이브를 S3에 업로드한다. `PutObject`의 SHA-256 응답이 요청 기록과 일치할 때만 PVC의 아카이브와 요청 기록을 삭제한다. 실패나 응답 유실 시 두 파일을 보존해 같은 키로 재시도한다. 복원 역할은 지정 prefix만 조회하고 정확한 객체 키를 내려받아 checksum과 전체 파일 SHA-256을 검증한 뒤 기존 오프라인 복원에 전달한다. |
| CLU-SCN-049 | CLU-07, CLU-14 | S3 archive/request 게시 중단 | 게시 전 종료는 업로드 가능한 불완전 archive를 남기지 않는다. 게시 후 종료는 archive와 요청을 함께 남긴다. 새 Pod UID의 Job이 pending archive를 먼저 전송하고 legacy archive는 보존한다. |
| CLU-SCN-050 | CLU-07, CLU-14 | 격리 EKS 실제 S3·IRSA·전체 복원 | 실제 S3 archive를 정확한 prefix/key로 목록 조회·다운로드하고, 기존 데이터 PVC는 보존한 채 새 PVC 3개에 seed한다. 세 voter 기동·Ready·quorum과 MQTT session/retained 복구까지 측정한다. |
| CLU-SCN-051 | CLU-07, CLU-14 | 미게시 staging의 명시적 offline 복구·정리 | 모든 backup writer를 중지한 maintenance에서 정확한 임시 directory 하나를 처리한다. 완성된 쌍은 metadata·SHA 검증 뒤 게시하고 다음 uploader가 원래 key로 전송한다. 불완전·손상 복구, 기존 게시 덮어쓰기, 경로 이탈·symlink·예상 밖 파일은 거절해 원본을 보존한다. 삭제는 `discard-stage --offline`의 명시적 선택에만 허용한다. |

### 11.10 Live cross-node QoS 0 forwarding 수용 매핑

Ingress는 cluster-wide committed route projection으로 remote candidate를 선택하고, bounded `NMQFWRD2` lane에 admitted 된 QoS 0 delivery를 subscriber owner로 보낸다. Receiver는 committed local ownership과 등록된 session queue를 확인한다. 전달은 at-most-once이며 Raft state에는 포함되지 않는다. QoS 1/2, stale-epoch end-to-end fencing과 quorum-loss durable ACK는 이 mapping의 범위 밖이다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-052 | CLU-03 | Forwarded-delivery codec and framing | Canonical v1 bytes가 opaque non-UTF-8 routing ID를 포함해 round-trip한다. Unknown version/flags, 잘못된 topic UTF-8, 길이 오류, trailing bytes와 oversized payload는 거절하며 idle stream은 length prefix를 기다리는 동안 만료되지 않는다. |
| CLU-SCN-053 | CLU-03 | Independent forward lane and bounds | `NMQFWRD2` delivery는 `TakeForward`에서만 수신하고 Raft는 기존 `TakeInbound` 경로를 유지한다. Raft 32-frame/4-MiB와 forwarding 1,024-frame/8-MiB queue는 독립적이다. |
| CLU-SCN-054 | CLU-03 | Publish candidate precedence and receiver validation | Detached candidate는 forwarder를 호출하지 않는다. Remote-handled는 local enqueue를 생략하고 unhandled는 local fallback을 사용한다. Full은 shared fallback의 `Saturated`가 된다. 만료, 잘못된 session/topic 및 QoS 0이 아닌 input은 enqueue하지 않는다. |
| CLU-SCN-055 | CLU-03 | Local subscriber queue outcome | Registered session에 enqueue한 뒤 connection을 깨운다. Full queue는 connection을 닫고 `ErrFull`, 등록되지 않은 session은 `ErrUnavailable`을 반환한다. |
| CLU-SCN-056 | CLU-03 | Local projection admission without quorum barrier | 이미 연결된 session의 QoS 0 PUBLISH는 local projection으로 처리하며 별도 quorum read를 하지 않는다. Projection이 초기화되지 않았거나 실패했거나 applied index보다 뒤처지면 기다리지 않고 전달 전에 거절한다. |
| CLU-SCN-057 | CLU-03 | Runtime owner validation and error mapping | Ingress는 다른 committed owner에게만 보낸다. Receiver는 local owner만 sink로 전달하며 malformed frame을 warning과 함께 버린다. Peer backpressure는 `Saturated`로 이어진다. |
| CLU-SCN-058 | CLU-03 | Three-node live delivery, order and ownership move | Node 1 publisher가 Node 2·3 subscriber에 각각 한 번 전달된다. Node 2 ingress는 local·remote fan-out을 수행한다. Subscriber가 Node 1로 reconnect하면 Node 3 publish가 새 owner에 도달한다. |

### 11.11 QoS 1/2 durable delivery 수용 매핑

ADR-0015의 cluster QoS 1/2 delivery scope를 다룬다. Persistent session이 detached된 동안의 QoS 1/2 queue, owner takeover 뒤 inflight 재개, backup v3 restore와 tombstone, 성공 ACK 뒤 process kill 및 quorum loss 중 성공 ACK 부재, QoS 2 Will reconnect/takeover 전달이 대상이다. Production storage·EBS power-loss, BP-03, N-1 compatibility, TEN-08 tenant quota와 drain/SAFE_TO_STOP는 이 범위 밖이다.

PUBLISH admission이 진행 중인 Ready drain을 만났을 때는 driver queue에 local projection checkpoint(새 quorum 조회가 아닌 현재 commit 경계의 projection 상태 확인)를 요청한다. Driver는 현재 commit index까지 local apply/projection이 끝난 경계에서 health와 applied index를 검사해 응답한다. 따라서 ingress가 10ms마다 짧은 drain 종료 구간을 우연히 찾아야 하는 대기는 제거한다. 최대 대기 2초와 대기 중 10ms 간격의 storage health 확인은 유지하며, driver 종료·projection 실패·applied-index gap은 거절한다. Durable 변경은 driver의 FIFO pending 목록에서 각 변경의 고유 ReadIndex proof를 확인한 뒤 입력 순서대로 proposal을 제출한다. 해당 Ready의 WAL sync·apply·projection·outbound 완료 뒤 proposal result를 반환하며, TCP 완료 worker는 그 결과 뒤에만 성공 ACK를 만든다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-059 | CLU-06, CLU-07, CLU-14 | Leadership transition의 fresh read | 리더를 모를 때는 발행을 보류한다. 발행 뒤 leader·term·role이 바뀌면 원래 deadline 안에서 새 proof를 확인한다. 이전 발행의 quorum 결과는 새 proof를 완료하지 않는다. |
| CLU-SCN-060 | CLU-06, CLU-07, CLU-14 | Append 전에 거절된 proposal의 새 leader 전달 | 리더를 모를 때 받은 정상 peer proposal을 보관한다. 리더 이전 중 local·batch·인증된 inbound proposal도 FIFO로 보관한다. 이전 노드가 follower인 상태에서 새 leader로 전달하고, 세 노드 모두 각 command를 한 번씩 같은 순서로 apply한다. Invalid identity와 malformed frame은 거절한다. |
| CLU-SCN-061 | CLU-06, CLU-07, CLU-14 | 보관 FIFO의 count·byte 상한과 downstream credit | 64개까지 보관한 뒤 65번째 요청은 거절한다. Ready input과 보관분의 합이 frame byte 예산을 넘으면 거절한다. Follower는 실제 count·byte credit 안에서만 전달한다. Credit이 부족하면 앞선 command까지만 commit하며, credit 복귀 뒤 남은 command가 한 번 commit한다. |
| CLU-SCN-062 | CLU-06, CLU-07, CLU-14 | Batch SEND 오류와 모든 suffix PacketID 보존 | 앞선 SEND의 commit 뒤 quorum 오류와 뒤 SEND의 성공이 겹쳐도 동일 ID 재시도로 수렴한다. TCP는 일부 prefix 전송 뒤 모든 suffix 예약을 지키고, durable sequence가 없는 local 메시지에도 ID를 중복 사용하지 않는다. |
| CLU-SCN-063 | CLU-06, CLU-07, CLU-14 | Durable state transitions, capacity, fencing and targets | Action별 전이, proposal-time expiry, stale epoch fencing, target 재검증, session/global saturation 시 나머지 target 처리, tombstone·snapshot 유효성을 확인한다. |
| CLU-SCN-064 | CLU-06, CLU-07, CLU-14 | Delivery command and snapshot codec | Action validation 경계, `publish_qos` 범위 및 RELEASE QoS 2 강제, nested canonical order와 1 MiB 초과 command 처리를 확인한다. |
| CLU-SCN-065 | CLU-06, CLU-07, CLU-14 | Backup v3 delivery state | Backup `inflight` count/digest에 delivery session message와 inbound tombstone이 포함되고, restore 뒤 같은 commit-boundary state가 takeover에 사용된다. 손상된 membership ID와 tail entry는 거부한다. |
| CLU-SCN-066 | CLU-06, CLU-07, CLU-14 | Backup v3 membership configuration tail | Snapshot 뒤의 learner 추가·promotion·제거 entry를 정렬해 replay하고, restore seed가 원래 3-voter 상태와 retired member를 보존한다. |
| CLU-SCN-067 | CLU-06, CLU-07, CLU-14 | Durable queue replacement | Durable outbound 교체, packet identifier 충돌, durable queued 우선순위, local queued 유지 및 capacity 절단을 확인한다. |
| CLU-SCN-068 | CLU-06, CLU-07, CLU-14 | Publish commit boundary and QoS 0 lane | QoS 1/2 durable target과 effective-QoS-0 lane 분리, QoS 2 Will의 publish QoS 보존, commit 실패 시 QoS 0 fan-out 금지, target 없는 QoS 1 PUBLISH/Will의 proposal-free ReadIndex barrier, RELEASE not-found 시 QoS 0 lane 생략, unauthorized QoS 2 RECEIVE의 commit 금지를 확인한다. |
| CLU-SCN-069 | CLU-06, CLU-07, CLU-14 | Resume, detach and rollback | Resume 뒤 durable delivery가 claimed offline QoS 0보다 먼저 오고, SessionPresent/CleanStart, reset-before-activation, durable ATTACH conflict rollback, detach expiry/discard를 확인한다. |
| CLU-SCN-070 | CLU-06, CLU-07, CLU-14 | MQTT ACK ordering and inbound recovery | QoS 1 PUBACK은 commit 뒤에만 오고 실패 시 성공 ACK 없이 종료한다. Target 없는 QoS 1은 barrier 대기 중 PUBACK을 보내지 않으며, 성공 완료 뒤에만 PUBACK을 보내고 실패하면 ACK 없이 연결을 종료한다. QoS 2 RECEIVE/PUBREC·RELEASE/PUBCOMP 순서, capacity reason, inbound recovery와 unknown/tombstone PUBREL 처리를 확인한다. |
| CLU-SCN-071 | CLU-06, CLU-07, CLU-14 | Durable promotion retry and Packet ID reuse | `ErrRetryableUnavailable` 경로에서 reserved Packet ID를 보존하고, 후속 notification 또는 timer retry 뒤 동일 Packet ID로 재시도하여 eventual PUBLISH를 보내며 다음 메시지에는 다른 Packet ID를 할당한다. |
| CLU-SCN-072 | CLU-06, CLU-07, CLU-14 | Publish readiness and durable commit boundary | QoS 1/2 PUBLISH ingress는 별도 full quorum-readiness preflight 없이 stable atomic view 또는 driver 소유 projection checkpoint를 사용한다. Ready 진행 중에도 최대 2초와 10ms 간격 storage-health 검사로 fail closed다. 변경마다 고유 ReadIndex proof를 요청하고 proof index까지 apply/projection한 뒤 FIFO 순서로 proposal을 제출한다. Proposal 결과는 해당 Ready의 WAL sync·apply/projection·outbound 완료 뒤 반환하며 이후 unrelated Ready 전체가 끝날 때까지 기다리지 않는다. 실패·cancel·driver exit은 queued checkpoint, pending fresh-proof 변경, staged·unapplied proposal 모두 non-success로 해제한다. TCP의 두 lane과 완료 worker는 같은 방향·ID 순서, 입력 제한, PacketID 예약과 commit 뒤 성공 ACK를 지킨다. |
| CLU-SCN-073 | CLU-06, CLU-07, CLU-14 | Caller timeout 뒤 late commit | Fresh proof 뒤 leader를 잊은 follower의 두 QoS 1 command를 adapter FIFO에 보관한다. 원래 `Wait`는 caller deadline 뒤 `ErrQuorumUnavailable`을 반환한다. Leader 복구 뒤 세 node가 FIFO 순서로 각 command를 한 번만 apply하고 늦은 결과는 버려진 waiter의 buffer에 남는다. 추가 8 turn에도 중복 command가 없다. TCP는 같은 오류에서 QoS 1/2 성공 ACK 없이 연결을 닫는다. |
| CLU-SCN-074 | CLU-06, CLU-07, CLU-14 | Runtime notification, recovery and fencing | Durable notifier의 `loadedThrough`, local sink 실패 뒤 reconnect 재투영, non-owner skip, missing message 처리, epoch detach, error mapping, committed command/configuration의 정렬 replay와 duplicate/malformed tail 거부, ownership epoch 전환과 subscription commit/fencing, proposal 없는 quorum ReadIndex barrier를 확인한다. |
| CLU-SCN-075 | CLU-06, CLU-07, CLU-14 | Detached queue, cross-node takeover and publisher recovery | Detached subscriber에 쌓인 QoS 1/2를 reconnect 뒤 FIFO로 재개하고, subscriber owner crash 뒤 QoS 1 PUBLISH 및 QoS 2 PUBREL을 이어 보내며 중복을 만들지 않는다. |
| CLU-SCN-076 | CLU-06, CLU-07, CLU-14 | ACK-then-crash and quorum loss | QoS 1 PUBACK 뒤 publisher process를 중단하고 persistent subscriber를 reconnect해 message가 복구되는지 확인한다. Target 없는 QoS 1 PUBLISH는 quorum이 있을 때 barrier 뒤 성공 PUBACK을 받고, quorum 상실 중에는 성공 PUBACK을 보내지 않는지 확인한다. |
| CLU-SCN-077 | CLU-06, CLU-07, CLU-14 | QoS 2 Will reconnect and takeover | 비정상 종료한 publisher의 QoS 2 Will이 detached QoS 2 subscriber reconnect 뒤 한 번 전달된다. Subscriber owner가 `AWAIT_PUBCOMP`에서 crash한 뒤 takeover는 같은 packet ID의 PUBREL만 재전송하고, sentinel 전에 중복이 없다. Clean-session DISCARD 뒤에도 인증된 tenant key로 Will을 route해 tenant 격리와 기존 PUBLISH session 검증을 유지한다. |

### 11.12 MQTT CONNECT ownership acquisition과 takeover 수용 매핑

이 절은 §2·§2.1의 quorum commit 뒤에만 성공 CONNACK을 보내고 takeover 뒤 이전 owner의 client connection을 종료해야 하는 규칙을 실제 3-process MQTT 경로에서 확인한다. `ownership-acquire-after-commit` failpoint는 ownership `Acquire`가 apply된 뒤 CONNECT local activation 전에 node 2를 종료해 CONNACK이 commit보다 먼저 반환되지 않는 경계를 확인한다.

Persistent subscription·inflight의 production encoding/replay와 route projection rebuild 수용은 §11.14에서 별도로 확인한다. 동시 CONNECT 및 stale-epoch end-to-end는 §11.15에 연결한다. Broker drain acceptance는 [Drain spec §10](drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)에 별도 기록한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-078 | CLU-04, CLU-05 | Live takeover | SessionPresent=1, 이전 owner socket 종료, 새 owner 전달 |
| CLU-SCN-079 | CLU-04, CLU-05 | Commit-before-CONNACK failpoint | Node 2 exit 86과 failpoint marker, CONNACK 없음, node 3 socket 종료, node 1 복구 reconnect와 전달 |
| CLU-SCN-080 | CLU-04, CLU-05 | Failpoint parse/boundary unit | failpoint 문자열을 parse하고 ownership acquire 경계에만 일치한다. |

### 11.13 ADR-0003 durable ACK fault 수용 매핑

이 매핑은 ADR-0003 §9의 process-crash, partition 및 storage-fault evidence를 연결한다. Process crash는 실제 프로세스를 종료하지만 OS·filesystem cache를 비우는 전원 손실 시험은 아니다. `WALAfterSync`는 `File.Sync`가 성공을 반환한 뒤 process를 종료하며 physical power-loss durability를 증명하지 않는다. 새 Docker/FUSE power-cut 시나리오는 held fsync와 journal rollback을 시험하지만 production filesystem/EBS 또는 실제 전원 손실의 증거가 아니다. QoS 1 reconnect retry는 ADR-0015 §1.13의 at-least-once 의미를 따라 새 command일 수 있어 중복을 허용한다. QoS 2는 동일 packet identifier 재개 후 중복 전달이 없어야 한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-081 | CLU-07, CLU-09 | ACK 뒤 replica kill | QoS 1/2 성공 ACK 직후 각 세 replica를 차례로 종료한다. 재접속 subscriber가 ACKed message를 정확히 한 번 받는다. Leader와 follower 역할 모두 두 QoS에서 시험한다. |
| CLU-SCN-082 | CLU-07, CLU-09 | WAL·commit crash matrix | `delivery-wal-before-write`, `delivery-wal-before-sync`, `delivery-wal-after-sync`, `delivery-commit-before-apply`, `delivery-commit-after-apply` 각각에 leader/follower와 QoS 1/2 조합을 주입한다. |
| CLU-SCN-083 | CLU-07, CLU-09 | Follower·leader partition | Isolated node는 quorum을 잃고 QoS 1 PUBACK/QoS 2 PUBREC를 성공 반환하지 않는다. Majority는 QoS 1/2를 계속 ACK한다. Heal 뒤 동일 QoS 2 packet을 재개해 한 번 전달하고 QoS 1은 at-most-once로 취급한다. |
| CLU-SCN-084 | CLU-07, CLU-09 | Storage-fault/power-loss durable ACK chaos | §11.6의 disk-full, quorum-loss, slow-I/O, sync-error, follower torn-power-loss와 cluster power-loss scenario에서 ACK·recovery 경계를 확인한다. Cluster publisher는 PUBLISH packet write 성공 뒤 in-flight barrier를 signal하고 `awaitAck` 전에 멈춘다. Cut으로 PUBACK 관찰이 끊긴 이 QoS 1 결과만 `unknown`이며, known timeout/closed 결과는 unacked로 남고 unknown evidence에서는 제외한다. Unknown payload delivery는 0회 또는 1회로 허용하되 중복은 거부한다. |

### 11.14 Persistent subscription·inflight replay와 route projection rebuild 수용 매핑

이 매핑은 production `replicated-state-v1` command codec과 typed snapshot을 거친 persistent subscription·QoS inflight 복구, 전체 process 재시작 뒤 route projection 재구성, dynamic joiner의 첫 READY 경계를 확인한다. Unit test는 index 9 snapshot 뒤 command tail을 재생하고, process test는 MQTT client의 관찰 결과와 node-local management projection을 확인한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-085 | CLU-03, CLU-06 | Production codec snapshot + WAL-tail replay | Snapshot index 9의 subscription option과 outbound QoS 1 `AWAIT_PUBACK` 상태를 복원하고 index 10~13 tail을 적용한다. Takeover 후 owner/epoch는 2/2이며 `opts/#`, `plain/+/x`, `late/+`만 남고 stale subscription·SEND는 상태를 바꾸지 않는다. Route rebuild 결과도 authoritative subscription과 일치한다. |
| CLU-SCN-086 | CLU-03, CLU-06 | Full cluster crash, WAL replay and ingress routing | 세 bootstrap process 전체 종료·재시작 뒤 두 persistent filter를 모두 복구한다. Node 1 ingress의 exact·wildcard publish가 각각 `exact-1`, `plus-1`로 재생되고, 제거한 `gone/#` 대상은 sentinel 앞에서 전달되지 않는다. |
| CLU-SCN-087 | CLU-03, CLU-06 | Joiner first READY has complete projection | Joiner의 첫 `/health/ready` 200을 관찰한 같은 probe에서 exact·wildcard subscription 두 개를 확인한다. Node 4 ingress가 rebuilt projection으로 subscriber를 찾아 `via-joiner`를 전달하고 세 bootstrap node의 snapshot index가 모두 증가한다. |
| CLU-SCN-088 | CLU-03, CLU-06 | Inflight and subscription WAL tail after join snapshot | Configuration snapshot 뒤 `SEND`와 `late/+` ADD가 snapshot index를 바꾸지 않는다. 네 process 재시작 뒤 subscription 세 개가 복원되고, 같은 packet ID의 DUP `inflight-1`과 새 `late-1`을 전달한다. |
| CLU-SCN-089 | CLU-03, CLU-06 | Unsubscribe and stale mutation absence | `replay/gone/#` UNSUBSCRIBE 뒤 projection에서 filter를 제거한다. Takeover 전 owner/epoch의 stale subscription과 SEND는 committed aggregate 및 route를 바꾸지 않는다. |

### 11.15 Cross-node delivery matrix·동시 CONNECT·stale-epoch 수용 매핑

이 절은 Cluster §3·§3.1·§4.2의 remote-owner delivery 조합과 §2·§2.1의 session ownership·fencing 결과를 연결한다. 세 process acceptance는 production MQTT 경로를 사용한다. 별도 drain/`SAFE_TO_STOP`, N-1 compatibility, production storage/EBS와 power-loss gate를 닫지 않는다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-090 | CLU-03, CLU-04, CLU-05 | Cross-node QoS × filter matrix | Node 1 publisher가 Node 2·3의 exact, single-level `+`, multi-level `#` subscriber에 QoS 0/1/2 각각 발행한다. 각 subscriber는 `p0`, `p1`, `p2`를 한 번씩 받고 delivery QoS가 `min(publish QoS, subscription QoS)`이며 QoS>0 packet ID가 0이 아니다. DUP/RETAIN은 모두 0이다. |
| CLU-SCN-091 | CLU-03, CLU-04, CLU-05 | Shared subscription with mixed QoS | MQTT 5 `$share` group의 Node 2·3 member에 QoS 1 세 건과 QoS 0 세 건을 모두 보낸 뒤에만 subscriber read/ACK를 시작한다. 여섯 topic은 각각 정확히 한 번 전달되고 두 member 모두 선택되며 추가 전달은 없다. |
| CLU-SCN-092 | CLU-03, CLU-04, CLU-05 | MQTT 5 retained subscription options | Retained publish is delivered to an MQTT 3.1.1 subscriber. RH 0/1 delivers one retained message, RH 2 delivers none, and repeating RH 1 does not replay. RAP 1/0 sets RETAIN 1/0. No Local suppresses self-publish but delivers remote publish. The No Local client setup retries MQTT 5 Server Unavailable (`0x88`) CONNACK for up to 15 seconds; all other reason codes fail immediately and persistent unavailability emits broker diagnostics. |
| CLU-SCN-093 | CLU-03, CLU-04, CLU-05 | Concurrent CONNECT ownership | Forced hold에서 두 node가 동일한 expected epoch를 읽은 뒤 하나만 CONNACK success를 받고, 다른 하나는 stale-epoch conflict를 받는다. 자연 경합 10회마다 정확히 한 accepted owner가 살아 있고 유효한 SUBSCRIBE를 처리한다. 자연 경합에서 먼저 commit한 연결이 local bind 또는 CONNACK 전에 더 큰 epoch의 takeover를 적용받으면 CONNACK 없이 닫힐 수 있다. 이 종료는 다른 연결 하나가 살아 있을 때만 허용하며, 그 밖의 연결 오류와 0·3 이외의 return code는 실패다. CONNACK을 한 byte라도 받은 뒤의 EOF는 부분 frame 오류이므로 허용하지 않는다. TCP regression은 epoch 1 commit 뒤 local bind 전 epoch 2 takeover를 적용해 stale 연결 bind를 거절하고 epoch 2 owner만 등록한다. |
| CLU-SCN-094 | CLU-03, CLU-04, CLU-05 | 연결 정리 epoch fence와 tenant 분리 | 현재 local epoch와 다르면 cleanup은 timer·repository·delivery·Will을 변경하지 않는다. 원격 takeover는 이전 node의 local epoch map을 바꾸지 않는다. 이전 연결은 `DisconnectTakeover`로 local Will을 폐기하며 집계하지 않는다. 이전 epoch의 replicated mutation은 committed owner/epoch 검사에서 거절된다. 같은 raw Client ID의 서로 다른 tenant fence는 독립적이다. |
| CLU-SCN-095 | CLU-03, CLU-04, CLU-05 | Stale subscription fencing | Takeover 뒤 이전 epoch에서 시작한 SUBSCRIBE는 success SUBACK을 내지 않고 새 projection에 stale filter를 추가하지 않는다. 현재 owner의 sentinel subscription은 계속 전달된다. |
| CLU-SCN-096 | CLU-03, CLU-04, CLU-05 | Stale inflight completion fencing | 이전 owner의 held `DeliveryActionComplete`는 takeover 뒤 거절된다. 새 owner와 이후 owner takeover는 같은 packet ID의 inflight PUBLISH를 DUP로 다시 전달하며 ACK 뒤 sentinel 전에 중복이 없다. |
| CLU-SCN-097 | CLU-03, CLU-04, CLU-05 | Ready-drain PUBLISH admission and projection regressions | PUBLISH는 stable atomic admission view 또는 driver checkpoint로 local projection을 확인한다. CONNECT/SUBSCRIBE의 별도 read-barrier readiness는 유지한다. Checkpoint는 현재 commit index까지 적용한 경계에서만 성공하고 2초 제한·storage health·fatal Ready 오류·driver exit에는 fail closed다. Durable write마다 새 ReadIndex context와 적용된 proof를 요구하며, 앞선 proof가 지연되어도 뒤 command가 추월하지 않는다. Proposal 결과는 해당 Ready의 sync·apply·projection·outbound 완료 뒤 반환한다. Tenant-scoped shared cursor와 RH 1 first-subscription 확인은 기존 회귀가 담당한다. |

### 11.16 Tenant storage quota 수용 매핑

이 mapping은 Dynamic cluster의 committed tenant quota profile, retained·offline·durable delivery state usage, over-limit 결과와 경보를 검증한다. Quota를 낮춰도 기존 state를 보존하고 usage를 줄이는 mutation을 허용한다. Linux/EBS power-loss, WAL torn-tail, retention/RPO/RTO와 N/N-1 compatibility는 이 acceptance에 포함되지 않는다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-098 | TEN-08, CLU-12 | Profile validation, versioning, storage accounting and snapshot replay | Profile validation/version, tenant isolation, retained/offline/delivery quota enforcement, retained shrink, offline CLAIM, delivery COMPLETE and restored usage/rejection are checked. |
| CLU-SCN-099 | TEN-08, CLU-12 | Replicated command/snapshot codec and conversion | Tenant quota command와 typed snapshot round-trip, nil/default-invalid profile, 정렬 오류 및 quota-aware object count mismatch를 거절한다. |
| CLU-SCN-100 | TEN-08, CLU-12 | Strict config, controller, committed-profile readiness and rejection report | Default profile, config version/limit validation, profile proposal rules, mismatch warning, rejection counter/rate-limited log and uncommitted-profile readiness are checked. |
| CLU-SCN-101 | TEN-08, CLU-12 | Bounded quota metrics | Tenant label escaping, 1,024-label limit and `_overflow`, allowed dimensions, rejection counter and three profile gauges are checked. |
| CLU-SCN-102 | TEN-08, CLU-12 | Three-node MQTT storage quota and rolling profile update | Retained quota excess preserves the old value and closes the MQTT 3.1.1 connection. Detached QoS 1 targets receive only up to the quota while publisher ACK succeeds. A node with lower local configuration still enforces the committed profile; rolling version update resolves mismatch. Restart preserves usage and retained state. |

### 11.17 N-1↔N rolling upgrade·rollback 수용 매핑

이 process acceptance는 같은 source를 `-ldflags`만 달리해 N-2=`1.0.0`, N-1=`1.1.0`, N=`1.2.0` binary로 build한다. 이는 release gate, profile activation, rollback, peer rejection과 양방향 snapshot 동작을 증명하지만 서로 다른 historical source revision 사이의 wire 차이를 증명하지 않는다. Rollout-controller preflight는 범위 밖이며 backup archive cross-release restore는 §11.19에서 검증한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-103 | CLU-11 | N-1 bootstrap and durable session baseline | 세 node가 N-1 profile로 READY가 되고 node 간 QoS 1 persistent session이 재개된다. |
| CLU-SCN-104 | CLU-11 | N-1 leader → N follower snapshot | N-1 cluster에 N follower를 넣어 snapshot catch-up, retained 최신 값, committed `1.2.0` profile과 feature advertisement를 확인한다. N-1 member가 남아 있으면 activation은 409다. |
| CLU-SCN-105 | CLU-11 | Drain and second node upgrade | Non-leader N-1 node가 drain에서 `SAFE_TO_STOP`에 도달한 뒤 N build로 READY가 된다. |
| CLU-SCN-106 | CLU-11 | N leader → N-1 follower snapshot after failure | N leader의 compacted state를 N-1 follower가 snapshot으로 복구하고 retained 값과 durable session delivery를 유지한다. |
| CLU-SCN-107 | CLU-11 | Rolling rollback and re-upgrade | N node를 하나씩 N-1로 되돌리고 durable session takeover를 확인한 뒤 다시 N으로 올린다. |
| CLU-SCN-108 | CLU-11 | N/N same-release snapshot transfer in both peer roles | All-N cluster에서 서로 다른 두 member를 차례로 중단·재시작한다. 각 member는 중단 전 applied index를 넘는 실제 설치 snapshot과 retained payload를 복구한다. |
| CLU-SCN-109 | CLU-11 | N-2 rejection | `1.0.0` node는 10초 동안 readiness 503이며 다른 node의 committed version은 `1.2.0`으로 유지된다. |
| CLU-SCN-110 | CLU-11 | Feature activation and rollback block | 모든 member가 `compatibility-probe`를 광고한 뒤 activation/replay가 200이다. N-1 rollback node는 `incompatible_state_schema: ...` reason으로 503이고 N build로 돌아오면 READY가 된다. |
| CLU-SCN-111 | CLU-11 | Release-profile readiness transitions | The controller retains synchronized state while both sequential reads are pending, then applies the combined result. Read errors, missing-member wait and profile proposal close the gate. A committed local profile change invalidates stale reads. A compatible snapshot restore closes the profile gate without latching incompatibility; unsupported activation or unsupported restored feature state latches incompatibility. |
| CLU-SCN-112 | CLU-11 | Snapshot invalidation wake and stale confirmation | Snapshot restore는 즉시 gate를 닫는다. 이전 revision의 pending 조회가 성공해도 다시 열리지 않는다. Controller는 주기 대기 없이 새 `Members`·`ActiveFeatures` fresh proof를 요청하며 둘 다 끝난 뒤에만 일치하는 profile을 승인한다. |
| CLU-SCN-113 | CLU-11 | Profile proposal 응답 없음 | Leader 손실 등으로 local `MemberProfileChange` proposal 결과가 오지 않아도 controller는 `clusterReleaseProfileCommitTimeout`(5초) 뒤 반환한다. Gate는 닫힌 채 유지하고 incompatibility를 latch하지 않는다. 다음 주기 reconcile이 다시 확인한다. |

### 11.18 비동기 WAL·application 완료 경계 수용 매핑

[ADR-0020](../decisions/ADR-0020-asynchronous-raft-wal-group-commit.md)은 저장 실행 구조만 채택한다. Pending 저장 중 RawNode 진행과 저장 뒤 ACK 경계를 분리하며, 단일 `ProcessReady` 반환은 저장 완료가 아니다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-114 | CLU-07 | Held fsync 중 tick | 저장 전에는 follower 응답과 apply가 없다. |
| CLU-SCN-115 | CLU-07 | Leader가 entry를 replication하는 경우 | local WAL 저장이 끝난 뒤에만 entry를 보낸다. |
| CLU-SCN-116 | CLU-07 | Commit-only sync, old-term overwrite, queue count·byte 상한 경계 | Raft node가 ADR-0020의 저장 완료·상한 경계를 지킨다. |
| CLU-SCN-117 | CLU-07 | 여러 CRC frame의 공동 sync와 write·sync 실패 | 실패하면 성공 완료를 반환하지 않는다. 성공한 frame은 reopen 뒤 FIFO payload로 복구된다. |
| CLU-SCN-118 | CLU-07 | 보류된 append와 선행 storage 오류 뒤 Close (`consumed-prior-error`, `unconsumed-prior-error`) | Close가 보류된 append를 저장하거나 실패를 보고한다. `unconsumed-prior-error`에서 Close 오류는 `ErrStorage`와 `os.ErrPermission`을 모두 포함한다. |

### 11.19 Backup archive N/N-1 restore 수용 매핑

같은 source를 `-ldflags`로 N-2=`1.0.0`, N-1=`1.1.0`, N=`1.2.0`으로 build한다. Gate와 restore mechanics만 검증하며 실제 historical-source wire/state 호환성이나 production storage를 증명하지 않는다. Archive format `3.0`, protocol `2.0`, schema `1.0`과 strict codec·boundary summary 검증은 유지한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-119 | CLU-11, CLU-14 | Upgrade restore N-1→N | 모든 committed profile이 N으로 수렴한 뒤 READY; retained `v1`, SessionPresent와 offline `one` 복구 |
| CLU-SCN-120 | CLU-11, CLU-14 | Downgrade restore N→N-1 | Feature 비활성 archive에서 profile이 N-1로 수렴; retained `v1`, SessionPresent와 offline `two` 복구 |
| CLU-SCN-121 | CLU-11, CLU-14 | Unsupported active feature 거절 | `compatibility-probe`를 모르는 N-1 restore는 non-zero exit, 지정 오류, target directory 미생성 |
| CLU-SCN-122 | CLU-11, CLU-14 | N-2 거절 | N archive를 N-2에서 복원하면 window 오류, target directory 미생성 |
| CLU-SCN-123 | CLU-11, CLU-14 | Unit window·feature·voter 경계 | 양방향·patch 허용, N-2·major·미지원 feature·window 밖 voter 거절; seed profile 보존 |
| CLU-SCN-124 | CLU-11, CLU-14 | Protocol·state schema exact 경계 | Manifest의 protocol major/minor와 schema major/minor를 각각 바꾸면 지정 protocol/schema 오류로 거절 |

### 11.20 Voter volume 손실 보호와 재시드 수용 매핑

[ADR-0022](../decisions/ADR-0022-bootstrap-volume-loss-guard.md)의 빈 directory bootstrap 보호와 [ADR-0024](../decisions/ADR-0024-joiner-voter-volume-loss-admission.md)의 joiner voter seed 보호·새 ID 교체를 검증한다. 로컬 process 종료·volume 삭제는 production Linux/EBS 손실 시험을 대신하지 않는다. Bootstrap은 exit 1·directory 미변경이고 joiner는 기존 NOT_READY·generic seed 거절 경로를 유지하므로 서로 다른 수용 기준이다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-125 | CLU-02, CLU-14 | Probe wire·TLS·listener 재사용 | 고정 bytes, configured peer만 EMPTY, cancel 뒤 listener 재사용 |
| CLU-SCN-126 | CLU-02, CLU-14 | Co-bootstrap 허용 해제 | 진행한 peer에 READY, Raft frame 수신 뒤 PROGRESSED, 초기 term/index READY, nil handler 거절 |
| CLU-SCN-127 | CLU-02, CLU-14 | Directory·all-peer barrier·취소 | 없는/빈 directory만 검사, 모든 peer 응답 대기, PROGRESSED 거절, context 취소 |
| CLU-SCN-128 | CLU-02, CLU-14 | 손실 voter 거절 | 같은 ID 재시작 exit 1·오류 로그, directory 미변경, 두 survivor READY |
| CLU-SCN-129 | CLU-02, CLU-14 | 새 ID 재시드 | `{1,2,4}` voter와 retired 3, joiner에서 retained `v1`, SessionPresent와 offline `kept` 복구 |
| CLU-SCN-130 | CLU-02, CLU-14 | 늦은 초기 peer 허용 | Node 3 probe 지연 중 management 미개방, 진행한 peer READY 응답, 세 node READY |
| CLU-SCN-131 | CLU-02, CLU-14 | Fresh joiner VOTER 응답 거절 | 실제 TLS에서 정합적인 VOTER 응답도 local STAGED identity·peer registry에 설치하지 않고 Raft 송신 차단 |
| CLU-SCN-132 | CLU-02, CLU-14 | Joiner voter 4-voter 빈 volume | `{1,2,3,4}`에서 같은 ID 빈-volume 재시작은 seed 거절·NOT_READY·긍정 CONNACK 없음·panic 없음, survivor membership 불변 |
| CLU-SCN-133 | CLU-02, CLU-14 | Joiner voter 3-voter 빈 volume | `{1,2,4}`에서도 같은 보호, 정상 same-volume recovery 유지 |
| CLU-SCN-134 | CLU-02, CLU-14 | Joiner 새 ID 재시드 | 최종 `{1,2,5}`·retired 4, 새 node에서 retained·SessionPresent·offline 복구와 same-volume restart, retired ID stage 거절 |
| CLU-SCN-135 | CLU-02, CLU-14 | Production Linux/EBS volume loss | 실제 영속 volume 손실 뒤 durable commit 보존과 새 ID 복구 |
| CLU-SCN-136 | CLU-02, CLU-14 | EKS disposable-emptyDir bootstrap guard (CLU-14) | member 3 빈 directory가 member 1의 PROGRESSED(term 6, commit index 100)를 받고 bootstrap을 거절해 exit 1; emptyDir 무변경 및 기존 broker 3/3 Ready |
| CLU-SCN-137 | CLU-02, CLU-14 | 격리 EKS 3-voter fixture에서 retained QoS 1 marker의 `PUBACK`과 완료된 기준 snapshot 3개를 확인한 뒤, voter의 EC2를 `--skip-os-shutdown`으로 stop/start한다 | 같은 EBS volume을 다시 연결해 기존 WAL·파일을 바꾸지 않고 replay로 복구한다. Pod 3개가 Ready가 되고 quorum이 회복되며 marker를 읽을 수 있다. |
| CLU-SCN-138 | CLU-02, CLU-14 | 같은 주소 새 ID 재시드 (ADR-0061) | 잃은 voter 3의 peer 주소·DNS SAN을 새 ID 4가 새 인증서로 이어받는다. 최종 voter `{1,2,4}`, retired 3, joiner에서 retained·SessionPresent·offline 복구, 이전 ID stage 재요청 `409 member_id_reserved` |
| CLU-SCN-139 | CLU-02, CLU-14 | 격리 EKS fixture에서 follower slot 하나의 실제 EBS volume을 잃고 새 ID로 재시드한다 | [EKS slot 재시드 기준](#eks-slot-재시드-기준-2026-10-16)을 모두 충족한다. |

#### EKS slot 재시드 기준 (2026-10-16)

[ADR-0061](../decisions/ADR-0061-eks-ordinal-member-slot.md)의 slot 교체 절차를 실제 EBS 손실에 적용한다. 대상은 follower slot 하나다. 판정은 아래 조건을 모두 충족할 때만 통과다.

1. 손실 주입: 장애 전에 같은 AZ의 여분 worker를 cordon한다. 손실 volume이 삭제되기 전에 다시 attach되어 이전 데이터로 시작하는 경로를 막는 시험 통제다. 대상 worker를 소유한 ASG가 대체 instance를 띄우면 cordon하지 않은 같은 AZ node가 생겨 같은 경로가 열린다. 그래서 그 ASG의 `Launch` process를 장애 전에 멈추고, 멈춘 시각과 해제 여부를 기록한다. 대상 worker EC2를 `terminate-instances --skip-os-shutdown`으로 종료한다. `terminated`를 확인한 뒤 기존 Pod를 해제한다. 대상 EBS volume을 삭제하고 AWS에서 삭제를 확인한다. 이후 PVC를 삭제하고 여분 worker의 cordon을 푼다.
2. 보호 확인: StatefulSet이 새 빈 PVC로 다시 만든 Pod는 이전 ID로 시작해 `cluster bootstrap refused` 오류와 exit 1로 끝나야 한다. 이 동안 남은 두 voter는 quorum을 유지하고 생존 Pod의 MQTT 표본은 모두 성공해야 한다.
3. 재시드: 새 ID를 stage하고 slot ConfigMap·Secret을 바꾼 뒤 Pod를 다시 만든다. 새 ID는 `VOTER`가 되고 이전 ID는 `RETIRED`가 되어야 한다. 3개 broker Ready와 quorum이 회복되어야 한다. 이전 ID의 stage 재요청은 `409 member_id_reserved`여야 한다.
4. Durable commit 보존: 관리 API는 commit index를 보여 주지 않는다. 그래서 손실 직전 생존 member의 `lastAppliedIndex` 최댓값을 기준으로 기록한다. 이 값은 commit된 index 이하다. 새 member의 `lastAppliedIndex`가 그 값 이상이어야 한다. [Durable stream oracle](drain-and-rolling-update.md#공통-durable-stream-oracle)이 통과해야 한다. Subscriber session은 손실 전 대상 member가 소유했다. 확인은 재시드 뒤 새 member Pod에서 한다. 손실 전에 쓴 retained marker를 새 member에서 같은 SHA-256으로 읽어야 한다.

이 시나리오는 실제 EBS volume 손실과 새 ID 재시드만 다룬다. 전체 voter 동시 전원 손실과 운영 환경에서 자연 발생한 WAL torn-tail은 다루지 않는다. 그래서 이 시나리오가 통과해도 CLU-14 전체를 닫지 않는다.

#### EKS 전체 voter 전원 손실·torn-tail·큰 snapshot 재시드 기준 (2026-10-16)

다음은 CLU-14의 EKS 시험 기준이다. 격리된 fixture의 실제 EBS ext4 volume을 사용한다. 각 도구는 `--execute --plan`을 요구한다. 실행 상태는 [검증 기록](../verification/cluster-session-and-delivery.md#2026-10-16-clu-14-완결-matrix-실행-결과)에서 확인한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-140 | CLU-14 | P1: 전체 voter 전원 손실 모델 | Durable QoS 1 발행 중 voter worker 3개를 **한 번의** `stop-instances --skip-os-shutdown` 요청으로 멈춘다. 같은 instance와 volume으로 시작한다. 기한 안에 Pod 3개 Ready와 quorum을 회복한다. PUBACK한 메시지가 모두 남아야 한다(RPO 0). Persistent session과 offline 메시지를 복구하고 retained marker의 SHA-256이 같아야 한다. |
| CLU-SCN-141 | CLU-14 | P1: 복구 진단과 통제 | Preflight에 image digest를 남긴다. 장애 뒤 voter마다 Pod UID, `restartCount`, 직전 container 종료 reason·exit code·시각, node state·readinessReason·uptime을 남긴다. `/health/ready`로 startup을 통과시키지 않는다. 재시작했거나 `DRAINING`이면 drain 기록을 읽는다. `COMPLETED`는 자동 해제를 기다린다. `FAILED`는 시험 실패다. 도구가 기록을 DELETE해서는 안 된다. ASG process를 복구하고 이후 교체가 없는지 관찰한다. |
| CLU-SCN-142 | CLU-14 | P1: 실제 tail 절단 관찰 | 전원 손실 뒤 voter 3개의 시작 로그를 읽고 실제 WAL tail 절단 기록의 유무를 남긴다. 절단이 없다는 결과도 유효한 관찰이다. 로그가 없거나 읽히지 않은 상태를 절단 없음으로 단정하지 않는다. |
| CLU-SCN-143 | CLU-14 | T1: 허용한 torn tail | Broker를 완전히 멈춘 뒤 같은 EBS ext4의 WAL에 ADR-0023이 절단을 허용한 tail을 주입한다. 재시작이 tail을 절단하고 Ready·quorum·marker·session을 복구해야 한다. |
| CLU-SCN-144 | CLU-14 | T2: 거절할 손상 | 절단 근거가 없는 final CRC 오류와 middle CRC 오류를 주입한다. Broker가 `corrupt raft WAL`로 시작을 거절하고 Ready가 되지 않아야 한다. 거절 뒤 WAL은 주입한 바이트와 같아야 한다. 사전 백업을 복원한 뒤 정상 복구해야 한다. |
| CLU-SCN-145 | CLU-14 | R1: 1 MiB 초과 snapshot 재시드 | ADR-0032 절차로 `snapshot-stream`을 활성화한다. 1 MiB를 넘는 retained 상태를 쓴 뒤 snapshot 주기 이상 entry를 추가한다. 기존 EBS 손실·새 ID 재시드 기준을 모두 통과해야 한다. Sender의 `raft snapshot stream completed`는 새 member를 가리키고 `total_bytes > 1048576`이어야 한다. Receiver의 수신 기록과 새 member의 retained SHA-256을 확인한다. |

P1은 전원 손실 **모델**이다. EC2의 OS shutdown은 건너뛰지만 EBS 내부 write cache 손실은 재현하지 않는다. T1·T2도 정지한 WAL에 바이트를 쓰는 주입 모델이다. 운영 환경에서 자연 발생한 torn write를 재현했다고 기록하지 않는다. 자연 발생 torn-tail의 빈도와 복구 관찰은 운영 관찰 항목이며 수용 시험의 선행 조건이 아니다.

### 11.21 Historical-source N-1↔N 호환성 수용 매핑

N-1·N-2 source는 commit `58a95f801e1467379db2eb6f3454ae44e954418a`다. 게시된 release가 아니라 사용자가 지정한 historical baseline(과거 source 기준점)이다. 이 source를 `git archive`로 추출하고 release identity `1.1.0`·`1.0.0`으로 build한다. N은 HEAD를 `1.2.0`과 feature `compatibility-probe`로 build한다. Production storage는 이 검증 범위에 포함하지 않는다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-146 | CLU-11 | Provenance·build | 지정 commit을 정확히 해석·추출하고 과거 source와 HEAD에서 각각 binary를 build |
| CLU-SCN-147 | CLU-11 | §11.17 시나리오 1–7 historical 재실행 | N-1 bootstrap profile, rolling upgrade·rollback, 양방향 installed snapshot, drain, N-2 거절 9회 이상, feature activation 차단과 미지원 rollback readiness 거절 |
| CLU-SCN-148 | CLU-11 | Historical N-1 archive를 HEAD로 restore | Retained `v1`, `SessionPresent=true`, offline `archive/offline/1`=`one` 복구 |
| CLU-SCN-149 | CLU-11 | HEAD archive를 historical N-1이 fail-closed로 거절 | `backup release or schema does not match cluster config`, non-zero exit와 target directory 미생성 |

### 11.22 WAL compaction·snapshot retention·torn-tail 수용 매핑

[ADR-0023](../decisions/ADR-0023-raft-wal-compaction-and-snapshot-retention.md)은 local snapshot이 확정될 때 이전 기록의 저장 공간을 회수하는 규칙을 정한다. WAL은 재시작 시 재생하는 영속 기록이며, tail은 snapshot index 뒤의 기록이다. Snapshot 파일 확정 → 영속 tail과 최신 HardState로 WAL 교체 → 이전 snapshot 삭제 순서를 지킨다. 이미 저장된 미commit entry도 tail에 보존한다. 최신 finalized snapshot 하나만 남기며 오래된 snapshot temp도 삭제한다. 더 큰 index의 temp는 유지한다.

Torn write는 끝나지 않은 마지막 쓰기다. Header보다 짧은 tail·부족한 payload는 절단한다. Zero-length header는 남은 영역 전체가 0일 때만 절단한다. 마지막 CRC 불일치 frame은 frame 자체의 파일 offset 기준 512-byte 구간(양 끝 부분 포함)에 전부 0인 구간이 있을 때만 절단한다. Frame 뒤의 zero padding만으로 손상 frame을 버리지 않는다. 근거 없는 마지막 CRC 오류, 중간 오류, decode 실패와 크기 한도 초과는 fail-closed다.

Opt-in process 시험은 각각 별도로 실행하며 skip을 통과로 계산하지 않는다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-150 | CLU-14 | Snapshot 이후 WAL tail·append 재개 | Index 5 뒤에는 6–10, index 8 뒤에는 9–10과 commit 10만 보존; snapshot record 없음; snapshot 하나·temp 없음; entry 11 append 뒤 commands 1–11·applied 11 복구 |
| CLU-SCN-151 | CLU-14 | Frame 크기 경계 | 60 KiB entry 40개에서 snapshot 1 뒤 entry 2–40을 여러 bounded frame으로 저장; 재시작 뒤 commands 40개 정확히 복구 |
| CLU-SCN-152 | CLU-14 | Crash 상태 복구 | Stale WAL temp 제거, snapshot 확정 후 이전 WAL·교체 후 prune 전 상태에서 commands·applied·commit 10 보존; 다음 snapshot에서 하나만 남김 |
| CLU-SCN-153 | CLU-14 | Compaction 오류 복구와 Windows 파일 공유 잠금 | Temp sync·rename·교체 후 directory sync 실패가 error 반환; close 안전; 다시 열면 commands 1–10·applied 10·temp 없음. Windows의 일시적 공유 잠금은 제한된 대기 안에 풀리면 atomic 교체·후속 append·복구가 성공한다. 지속되는 잠금은 error를 반환하며 이전 WAL의 영속 기록을 보존한다. |
| CLU-SCN-154 | CLU-14 | Torn-tail 근거와 corruption 거절 | Zero-filled tail·zero-sector frame은 valid prefix까지 절단, record 1개와 정확한 파일 크기; 근거 없는 CRC·zero padding만 있는 CRC·zero header 뒤 nonzero·middle corruption은 거절 |
| CLU-SCN-155 | CLU-14 | Consensus snapshot·handle 교체 | Snapshot index=AppliedIndex 하나, WAL identity 교체; gamma append 후 alpha/beta/gamma와 CommitIndex=AppliedIndex 복구 |
| CLU-SCN-156 | CLU-14 | Worker와 handle 교체 race 회귀 | 기존 Raft package의 저장·snapshot·복구 경계 유지, race 오류 없음 |
| CLU-SCN-157 | CLU-14 | 실제 3-node MQTT snapshot·restart | 두 snapshot 주기 뒤 node별 finalized 하나·old temp 없음·WAL 교체; 전체 재시작 뒤 retained payload 일치 |
| CLU-SCN-158 | CLU-14 | N/N-1와 historical rollback | 기존 release 시나리오 유지; HEAD가 교체한 WAL을 `58a95f8` N-1 binary가 읽는 scenario 5 통과 |
| CLU-SCN-159 | CLU-14 | Production storage와 backup retention | 실제 Linux/EBS 전원·volume 손실 및 승인된 retention·RPO/RTO |

### 11.23 Local ext4 filesystem 수용 매핑

사용자가 선택한 `local-ext4-v1`은 실제 Linux ext4 위의 로컬 검증 profile이다. Ext4는 Linux에서 파일을 디스크에 보관하는 파일시스템이다. 각 broker는 시험 전용 Docker volume에 만든 256 MiB disk image를 loop device(일반 파일을 디스크처럼 연결하는 Linux 기능)로 연결하고 `data=ordered`로 mount한다. 노드마다 image와 volume이 다르지만 host·kernel은 공유한다. 이는 독립 host/AZ 배치, EBS, filesystem cache를 잃는 전원 손실 또는 production 지원 profile을 뜻하지 않는다. 기본 `local-fuse-v1`의 여섯 simulation 시나리오는 유지한다.
Recovery-boundary fixture는 별도 1 GiB image에 `TMPDIR`를 두고 실행한다. 여기서 여러 node의 data directory는 같은 filesystem 안의 서로 다른 directory이며 독립 storage failure domain을 뜻하지 않는다. 기존 fault hook과 인위적 WAL 손상 입력은 recovery 규칙을 시험할 뿐 실제 block device의 sync 실패나 자연 발생 torn write 증거가 아니다.

CLI는 `--profile local-ext4-v1`으로 선택한다. Ext4가 실제로 mount되지 않거나 필수 시험이 skip되면 PASS로 처리하지 않는다. Manifest와 scenario event에는 profile·증거 종류, Linux kernel, 실제 filesystem/mount 옵션·loop device·image 크기 및 broker checksum을 기록한다. Fixture secret과 고객 payload는 공개 artifact에 넣지 않는다. Privileged container는 전용 loop image만 생성하며 기존 volume/image는 덮어쓰지 않는다. 정상 종료·오류·취소 때 시험이 소유한 mount·loop·container·volume만 정리하고 정리 실패도 run 실패로 기록한다.
Container 시작에는 build 뒤 검사한 immutable image ID(내용으로 식별되는 image 값)를 사용한다. 사람이 붙이는 image tag가 다른 run의 build로 바뀌어도 현재 run의 후속 container가 다른 image를 쓰지 않게 한다. Manifest에는 사람이 읽을 tag와 해당 ID를 함께 보존한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-160 | CLU-07, CLU-14 | Ext4 follower ENOSPC | 256 MiB filesystem이 실제 ENOSPC를 반환한다. Follower는 storage 오류·readiness 거절을 보이고 healthy quorum은 QoS 1/2를 ACK한다. 공간 회수·restart 뒤 ACK된 payload가 전달된다. |
| CLU-SCN-161 | CLU-07, CLU-14 | Ext4 quorum ENOSPC | 두 follower의 ENOSPC 뒤 quorum을 잃고 새 QoS 1/2 publish는 성공 ACK를 받지 않는다. 복구 뒤 기존 persistent session·QoS 2 exchange가 재개된다. |
| CLU-SCN-162 | CLU-07, CLU-14 | Ext4 전체 broker process crash | QoS 1/2 성공 ACK를 외부 runner가 관찰한 뒤 세 broker process를 SIGKILL한다. 세 exit 137을 확인한 다음 같은 mount에서 재시작하고 SessionPresent·ACK된 payload와 applied index 수렴을 확인한다. OS·filesystem cache는 유지되므로 power-loss evidence가 아니다. |
| CLU-SCN-163 | CLU-07, CLU-14 | Ext4 recovery boundaries | Ext4에 놓인 fixture에서 WAL compaction·오류 복구·torn-tail classifier, snapshot-finalize process crash, 실제 3-node WAL lifecycle, backup/restore와 voter volume-loss/reseed 기존 수용 시험을 실행한다. 인위적 corruption과 process crash를 자연 발생 torn write로 표현하지 않는다. Test 누락·skip·실패는 scenario 실패다. |

### 11.24 Session metadata 복제와 owner 손실 복구 수용 매핑

이 매핑은 [ADR-0025](../decisions/ADR-0025-replicated-session-metadata-and-owner-loss-recovery.md)의 feature gate, committed metadata lifecycle, owner restart·handoff recovery, management view 및 backup inclusion을 검증한다. `session-metadata` 비활성 상태에서 기존 동작과 wire bytes가 유지되는지 확인한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-164 | CLU-06, CLU-08 | Restore lifecycle validation | 유효하지 않은 복원 조합 7개를 거절하고 connected/disconnected lifecycle 복원 뒤 `Disconnect`·`Expire` 결과가 동일하다. |
| CLU-SCN-165 | CLU-06, CLU-08 | Feature gate, ownership atomicity, action disposition과 capacity | 비활성 metadata 제안이 `feature_inactive`이고 ownership 불변이다. CONNECT replacement는 pending Will을 취소하며 Session 없는 Transfer는 metadata를 보존한다. DETACH·CLAIM_WILL·DISCARD, stale epoch, retention 오류, bytes 한도와 Release capacity 회수를 확인한다. |
| CLU-SCN-166 | CLU-06, CLU-08 | Snapshot invariants and round-trip | Metadata bytes 재계산·정렬·deep copy가 유지되고 ownership 누락, epoch 역전, feature 미활성 및 state invariant 위반을 거절한다. |
| CLU-SCN-167 | CLU-06, CLU-08 | Codec compatibility and validation | Command·snapshot 왕복, 정렬·count·invalid boundary를 확인한다. 비활성 empty snapshot reference vector 및 Session 없는 OwnershipChange bytes는 기존과 같다. |
| CLU-SCN-168 | CLU-06, CLU-08 | Build feature, DTO bridge, runtime commit gate | 기본 feature 광고, ldflags merge/deduplicate와 empty-token rejection을 확인한다. 비활성 runtime은 metadata commit을 replicate하지 않고 session 없는 ownership commit을 하며 활성 runtime은 record를 생성한다. 다른 feature만 활성화되어도 metadata는 비활성이다. |
| CLU-SCN-169 | CLU-06, CLU-08 | Disconnect, Will claim, expiry and recovery transitions | Committed claim만 발행하며 commit 실패 시 억제한다. Inactive fallback, timer claim, expiry stale skip, 현재 epoch 비계획 recovery, 이전 epoch 표준 계획 인계, DETACHED absolute timer와 idempotence를 확인한다. |
| CLU-SCN-170 | CLU-06, CLU-08 | Cluster management view | Cluster source 사용 시 tenant/prefix/node/protocol filter, sort/pagination, exact GetClient lookup 및 `ok=false` local fallback을 확인한다. |
| CLU-SCN-171 | CLU-06, CLU-08 | Three-node owner crash, restart and expiry/capacity reclamation | Feature 활성화 뒤 owner crash의 committed metadata를 다른 node에서 조회한다. 재시작 뒤 crash Will은 한 번 발행된다. 별도 session의 client 재접속은 metadata와 current ownership의 epoch가 같은 상태에서 이전 Will을 대체하므로 기존 takeover 억제를 유지한다. 이는 metadata 없는 계획 인계 뒤 후계의 표준 Will 판단과 다르다. Delivery는 보존하며 expiry 뒤 조회는 404, 재접속은 SessionPresent=0이다. |
| CLU-SCN-172 | CLU-06, CLU-08 | Disconnect 없이 owner crash 중 delayed Will | TCP abrupt close 뒤 owner를 crash/restart한다. 저장된 absolute delay에 맞춰 Will이 한 번만 발행되고 중복이 없다. |
| CLU-SCN-173 | CLU-06, CLU-08 | Mixed-release inactive wire compatibility | Build feature가 없어 활성화가 거절되는 historical release와 feature-inactive N/N-1 process compatibility를 확인한다. |

### 11.25 Detach 전환 local QoS 0 보존 수용 매핑

이 매핑은 [ADR-0026](../decisions/ADR-0026-detach-transition-local-qos0-gate.md)의 owner-node local publish 보호만 검증한다. 다른 node의 forward delivery와 연결 중 owner crash는 보호 범위 밖이다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-174 | CLU-06 | Local enqueue 전환과 실패 재개 | 전환 중 local item은 상태 변경 없이 거절하고 durable item은 받는다. 실패 뒤 기존 FIFO에 이어 enqueue한다. |
| CLU-SCN-175 | CLU-06 | Commit 성공 뒤 제거와 대기자 해제 | 대기자는 nil로 끝나고 queue 조회·enqueue는 ErrUnavailable이다. |
| CLU-SCN-176 | CLU-06 | Remove의 대기자 해제 | Remove가 전환 대기자를 깨운다. |
| CLU-SCN-177 | CLU-06 | Publish 전환 재판정 | Commit 성공은 offline, 실패는 local로 전달한다. 제거 재판정·committer 부재·timeout 결과와 대기 중 expiry 직전·경계·이후를 확인한다. |
| CLU-SCN-178 | CLU-06 | Forward 비대기 | ErrDetaching을 ErrUnavailable로 바꾸고 기다리지 않는다. |
| CLU-SCN-179 | CLU-06 | Disconnect 성공 | Pending QoS 0 capture 뒤 FinishDetach(true)를 실행한다. |
| CLU-SCN-180 | CLU-06 | Disconnect 실패 | FinishDetach(false) 뒤 local queue를 유지한다. |
| CLU-SCN-181 | CLU-06 | BeginDetach 실패 | DETACH와 FinishDetach를 호출하지 않는다. |
| CLU-SCN-182 | CLU-06 | 실제 queue 동시 publish | DETACH에는 race/1, 이후 offline ENQUEUE에는 race/2만 기록한다. Publish는 Delivered이며 local queue는 제거된다. |

### 11.26 Detach 전환 forward·연결 중 owner crash 유실 경계 수용 매핑

이 매핑은 [ADR-0027](../decisions/ADR-0027-detach-forward-and-owner-crash-qos0-boundary.md)의 at-most-once 경계만 검증한다. Production storage와 CLU-14 전체 완료를 선언하지 않는다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-183 | CLU-06 | Failpoint parse·경계 | 두 값이 parse되고 자기 경계에서만 일치한다. |
| CLU-SCN-184 | CLU-06 | Forward 전환 중 비대기·유실 | DETACH가 막혀 있어도 두 frame 처리가 끝난다. 다른 session에는 전달하고 전환 중·뒤 forward는 DETACH와 ENQUEUE에 넣지 않는다. |
| CLU-SCN-185 | CLU-06 | DETACH commit 전 owner crash | Exit 86·marker, outage count 0, recovery 뒤 capture 재개, SessionPresent=1, live 재전달·중복 없음, sentinel 수신 |
| CLU-SCN-186 | CLU-06 | DETACH commit 뒤 owner crash | Exit 86·marker, outage count 1 보존, outage → after-recovery FIFO, SessionPresent=1, 중복 없음, sentinel 수신 |

### 11.27 Replication factor·write quorum profile 수용 매핑

이 매핑은 [ADR-0029](../decisions/ADR-0029-replication-factor-write-quorum-profile.md)의 3/2 profile을 검증한다. Production Linux/EBS storage 수용은 별도다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-187 | CLU-09 | Config profile | 생략=3/2, 3/2 허용, 그 밖의 값·누락 key·unknown key는 startup 거절 |
| CLU-SCN-188 | CLU-09 | 남은 voter 경계 | 4→3 허용, 3→2 거절, 이미 제거된 대상은 막지 않음 |
| CLU-SCN-189 | CLU-09 | Removal 사전 확인 | 3 voter에서 `replication_factor_unsatisfied`, commit 없음 |
| CLU-SCN-190 | CLU-09 | HTTP 결과 | 409, non-retryable |
| CLU-SCN-191 | CLU-09 | Runtime·API | summary 3/2, 3-voter removal 409, 4→3 removal 성공 |
| CLU-SCN-192 | CLU-09 | Leader removal 재검사 | Committed BeginRemove 뒤에도 남은 voter가 부족하면 transfer·ConfChange가 없고 operation은 REMOVING에 남는다. |
| CLU-SCN-193 | CLU-09 | Write quorum 미달 | quorum 손실·partition 중 성공 durable ACK 없음 |
| CLU-SCN-194 | CLU-09 | Contract | `ClusterData` 1.6.0 field와 example 검증 |

### 11.28 Publish backpressure 수용 매핑

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-195 | CLU-12 | Read-only capacity와 apply 불변 | 전역 entry·bytes·tenant 초과 구분, eligible 합계, session 포화, 상태 무변경 |
| CLU-SCN-196 | CLU-12 | 새 저장 gate와 제외 경로 | 압력·capacity 거절 전 commit 없음, RELEASE·Will·비내구 경로 유지 |
| CLU-SCN-197 | CLU-12 | TCP adapter 응답 | MQTT 5 `0x97`, QoS 1/2 연결 유지, MQTT 3.1.1 종료 |
| CLU-SCN-198 | CLU-12 | Backlog 나이·disk 진입과 해제·quota 경보 | Entry 제거 후 회복, 1/8 hysteresis, 연속 오류 Warn 1회, counter 기록 |
| CLU-SCN-199 | CLU-12 | Failover-aware backlog | 도달 불가 시 0, 재연결 시 나이 시작점 갱신, 이전 generation 제출분 영구 제외, 미제출분 포함 |
| CLU-SCN-200 | CLU-12 | Apply 완료 뒤 caller 순서 대기 | 실제 3-node Raft commit과 state machine apply 뒤 Wait 전에 admission 회복, 늦은 Wait의 결과 유지, 다른 미완료 entry 보존과 제출 전 취소의 driver 정리 |
| CLU-SCN-201 | CLU-12 | Write 전 proposal 회수 | Queue 순서·count·bytes 유지, 일반 frame·outCurrent·연결 peer 제외 |
| CLU-SCN-202 | CLU-12 | 회수한 forwarded proposal 재생 | Credit 복구 뒤 정확히 1회 apply, FIFO 공간·StoragePending 반영, 비-MsgProp 거절 |
| CLU-SCN-203 | CLU-12 | Strict config·metric·filesystem | 범위·누락·unknown 거절, 정확한 sample과 peer 교체, 실제 filesystem 조회 |
| CLU-SCN-204 | CLU-12 | Commit lag와 disk 포화·회복 | 두 subtest, 거절 topic 미전달, healthy node 성공, 회복 뒤 재시도, 정확한 topic 집합 |
| CLU-SCN-205 | CLU-12 | 실제 follower disk 압력 아래 publish | 거절 topic이 `UnackedDuringFault`에 있고 `missingAcked`·`duplicates`가 없다. |

### 11.29 Replicated-state v1 계약 수용 매핑

이 매핑은 [ADR-0031](../decisions/ADR-0031-replicated-state-schema-strict-evolution.md)과 [Replicated-state v1](../contracts/replicated-state-v1.md)의 strict decode·feature gate·기준 bytes를 검증한다. 게시 release별 process matrix와 production storage는 별도 gate다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-206 | CLU-11 | Vector producer/consumer | Producer 6개 bytes·semantic 일치; reject 6개 schema 분류 |
| CLU-SCN-207 | CLU-11 | 거절 분류 | Unknown·schema·미지원 payload는 sentinel; 손상·값 오류는 미분류 |
| CLU-SCN-208 | CLU-11 | Baseline 보존 | 기존 message·field·enum·reserved 보존, 새 field 번호 증가; vector 6개 baseline unknown 없음 |
| CLU-SCN-209 | CLU-11 | Inactive-feature producer guard | 실제 DTO bridge bytes가 baseline unknown 없이 decode; 실패 위치 보고 |

### 11.30 Feature activation 운영 승인 수용 매핑

이 매핑은 [ADR-0032](../decisions/ADR-0032-feature-activation-approval-policy.md)와 [Release contract §4.1](../contracts/cluster-release-compatibility-v1.md#41-운영-승인과-rollback-window-종료)의 운영 정책을 검증한다. [PowerShell 절차](../how-to/cluster-feature-activation.md)의 실제 명령 smoke와 기존 runtime 시험을 수동 정책 판정과 구분한다. API는 외부 승인 기록을 검사하지 않는다. Production storage·retention/RPO/RTO, rollout controller와 미래 게시 release별 matrix는 완료 범위 밖이다.

#### 수동 정책 판정

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-210 | CLU-11 | Feature 광고가 한 member에 없음 | 승인·POST 보류 |
| CLU-SCN-211 | CLU-11 | 승인 완료, 아직 POST 전 | Window는 종료되지 않음 |
| CLU-SCN-212 | CLU-11 | 승인 뒤 member/build 집합 변화 | 기존 승인 무효, 재확인·재승인 |
| CLU-SCN-213 | CLU-11 | POST timeout 뒤 GET에 token 없음 | 결과 불명, rollback 금지 |
| CLU-SCN-214 | CLU-11 | POST 오류 뒤 성공 GET에 token 있음 | Commit 확인, window 종료 |
| CLU-SCN-215 | CLU-11 | 현재 409지만 이전 결과 불명 POST 존재 | 이전 요청까지 실패했다고 판정하지 않음 |
| CLU-SCN-216 | CLU-11 | HTTP 200+active token, 이후 동일 token replay | Window 닫힘 유지 |
| CLU-SCN-217 | CLU-11 | Activation 뒤 이전 미지원 build 복구 요청 | 지원 build로 전진 복구; 이전 backup restore는 별도 DR와 데이터 손실 경계 |

#### Runtime·명령 수용

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-218 | CLU-11 | Non-retired 전체 광고·profile 변경 | Staged 미지원 차단, Retired 제외, active feature profile 보호 |
| CLU-SCN-219 | CLU-11 | HTTP 권한·token·409와 OpenAPI | 기존 API·권한·응답 계약 유지 |
| CLU-SCN-220 | CLU-11 | 실제 PowerShell·curl·jq 명령 | Summary·features·각 node local health·readiness 200, 광고 filter exit 0, IDs 1/2/3·release 1.2.0·초기 active 빈 집합 |
| CLU-SCN-221 | CLU-11 | Activation·replay·재조회 | Body 없는 POST 200, active filter exit 0, replay/GET activeFeatures 정확히 compatibility-probe 하나, 세 readiness 200 |
| CLU-SCN-222 | CLU-11 | Synthetic·historical release/backup | Rolling·activation 전 rollback·snapshot, 미지원 member 409, activation/replay 200, 미지원 N-1 READY 503·지원 N 회복, 미지원 archive 거절·target 미생성 |

### 11.31 연결 rebalance 수용 매핑

기준은 [ADR-0033](../decisions/ADR-0033-connection-rebalance-operation.md)과 §9다. Production 부하 성능과 LB의 재접속 배치 보장은 이 local 수용 범위 밖이다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-223 | CLU-13 | 수렴식·fresh·local shed 전이 | 편차·stale·shedding·LB 대기·settle 경계 일치 |
| CLU-SCN-224 | CLU-13 | 복제 상태·상호 배제·snapshot·feature | 결정적 전이·깊은 복사·feature 없는 snapshot 거절·baseline 유지 |
| CLU-SCN-225 | CLU-13 | API·OpenAPI·TCP 연결 계수 | 필수 입력·권한·오류·CONNECT 전 socket 제외 |
| CLU-SCN-226 | CLU-13 | 실제 3-node 취소·수렴·session 재개 | 12/7/0에서 ceiling 이하, MQTT redirect·Session Present·QoS 1 재개, 취소 뒤 READY |

### 11.32 QoS 1/2 retained 수용 매핑

기준은 [MQTT §5](mqtt-protocol-and-session.md#5-retained-message), §4.2의 commit·ACK 순서와 §6의 partition·quorum loss 경계다. QoS 1/2 retained Will, retained capacity·backpressure(§11.28), production storage는 이 수용 범위 밖이다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-227 | CLU-08 | Commit 실패 시 delivery·fan-out 차단 | QoS 1 PUBLISH와 QoS 2 RELEASE 모두 입력 QoS로 retained replace를 시도한다. 오류 뒤 delivery command·quorum barrier·local fan-out이 없다. |
| CLU-SCN-228 | CLU-08 | QoS 1/2 commit·ACK·cross-node replay·QoS 하향 | ACK 뒤 모든 node의 저장 QoS·payload bytes가 일치한다. 새 subscriber는 topic 순서로 `min(stored, granted)` QoS와 RETAIN=true인 최신 값을 한 번씩 받는다. |
| CLU-SCN-229 | CLU-08 | Replace·delete | QoS 2 replace는 payload와 저장 QoS를 바꾼다. QoS 1 빈 payload는 retained 값을 삭제하고 live로 전달된다. 삭제한 값은 replay되지 않는다. |
| CLU-SCN-230 | CLU-08 | MQTT 5 Message Expiry | QoS 1/2 값은 8초 만료 전에 MQTT 3.1.1 subscriber에 replay된다. 만료 뒤 모든 node의 조회와 replay에서 제외된다. |
| CLU-SCN-231 | CLU-08 | 단일 node 손실 뒤 replay(CLU-08) | Publisher voter를 crash한 뒤 다른 node가 ACK된 최신 retained payload와 QoS를 replay한다. |
| CLU-SCN-232 | CLU-08 | Quorum 손실 중 성공 ACK·live fan-out 부재와 QoS 2 PUBREL 재시도 | QoS 1 PUBACK·QoS 2 PUBCOMP 성공을 차단한다. PUBREL 전후 실패한 입력은 live로 전달되지 않는다. Quorum 복구 뒤 persistent session을 재개하고 원래 Packet ID의 PUBREL로 pending 값을 한 번 전달·replay한다. Timeout된 QoS 1 retained는 §4.2에 따라 늦게 commit될 수 있으나 원래 호출의 성공 ACK나 live fan-out은 허용하지 않는다. |

### 11.33 안전한 proposal 복구 수용 매핑

2026-10-06 사용자는 NaruMQ의 안전한 자동 재제안과 EMQX 5.8.8 고정 유지를 선택했다. [ADR-0036](../decisions/ADR-0036-replicated-proposal-receipts-and-safe-reproposal.md)·[ADR-0037](../decisions/ADR-0037-proposal-request-identity.md)·[ADR-0038](../decisions/ADR-0038-proposal-receipt-capacity-fences.md)은 §4.2의 활성 feature 경로에 적용한다. 이전 §11.28의 backlog·write 전 회수는 유지한다. 이전 실패 artifact의 판정은 바꾸지 않는다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-233 | CLU-07 | 최초 결과·중복 PUBLISH·다른 digest·retirement | 중복은 처음 index·결과를 반환한다. Queue sequence를 다시 늘리지 않는다. 다른 digest와 GC 뒤 stale frame을 거절한다. |
| CLU-SCN-234 | CLU-07 | Conflict 이후 조건 변화·논리 OperationID 공유 | Ownership이 생겨도 기존 conflict를 성공으로 바꾸지 않는다. STAGE_JOIN·ADD_LEARNER는 별도 RequestID로 진행하며 처음 operation view는 보존한다. |
| CLU-SCN-235 | CLU-07 | Count·byte 한도와 복원 | Full ledger의 count fence를 복원한다. 큰 Will 결과의 한도 초과는 mutation 전에 거절한다. GC·restore 뒤 거절한 요청이 Will을 소비하지 않는다. Caller가 result payload를 바꿔도 receipt는 바뀌지 않는다. |
| CLU-SCN-236 | CLU-07 | 모든 historical result·wire digest·inactive baseline | Restore 뒤 historical result를 replay하면 원래 fields를 반환하고 현재 state를 바꾸지 않는다. Digest 변경·unknown field를 거절하고 기존 baseline vectors를 유지한다. |
| CLU-SCN-237 | CLU-07 | 실제 WAL·leader 손실·늦은 duplicate·disk restore | 이전 leader에 저장했지만 quorum 적용 전 손실된 요청을 복구한다. 늦은 duplicate 이후에도 메시지 1개·next sequence 2다. 실제 snapshot·archive·disk restore 뒤에도 처음 result index와 dedup을 보존한다. 미지원 restore build는 active feature를 거절한다. |
| CLU-SCN-238 | CLU-07 | 새 quorum proof 부재·caller 취소·late ACK | Replacement leader 선출 뒤 survivor 간 전송을 멈추면 성공 결과가 없다. Caller 취소 후 늦은 원래 frame은 한 번 적용할 수 있지만 성공 응답을 다시 보내지 않는다. |
| CLU-SCN-239 | CLU-07 | 리더 kill 때 전송 중이던 PUBACK COMPLETE: 활성·비활성 비교 | Subscriber 소속 ingress node가 Raft에 낸 COMPLETE proposal이 사라진 상태에서(`dropProposalPeers`, [성능 §7.3](performance-and-verification.md#73-cluster-failpoint-빌드)) 리더를 kill한다. 활성: subscriber 연결은 8초 동안 닫히지 않는다. 이어서 보낸 새 PUBLISH를 같은 연결로 받고, 세션을 이어 붙여도 첫 message를 다시 받지 않는다(COMPLETE 적용). 비활성: 같은 상태에서 5초 deadline 뒤 연결이 닫힌다. |
| CLU-SCN-240 | CLU-07 | 리더 kill 때 전송 중이던 PUBREL RELEASE: 활성·비활성 비교 | Publisher 소속 ingress node의 RELEASE proposal이 사라진 상태에서 리더를 kill한다. 활성: 같은 연결에서 kill 뒤 8초 안에 PUBCOMP을 받고 subscriber가 QoS 2 message를 한 번 받으며 2초 동안 두 번째 전달이 없다. 비활성: PUBCOMP 없이 연결이 닫힌다. |
| CLU-SCN-241 | CLU-07 | CPU 경쟁 아래 3-node process 반복(진단) | Persistent QoS 1 subscriber 6명과 약 25 msg/s publish 중 리더를 kill한다. 활성은 계획 밖 subscriber 종료와 `dispatch MQTT PUBACK: quorum unavailable`이 0이어야 한다. |
| CLU-SCN-242 | CLU-07 | 실제 MQTT SIGKILL·재접속·resync | 원래 5초 broker commit deadline, 성공 ACK durability와 NaruMQ 무예외 판정을 유지한다. |

### 11.34 Dynamic peer TCP 생존 확인 수용 매핑

기준은 [ADR-0059](../decisions/ADR-0059-dynamic-peer-tcp-liveness.md)와 [Raft membership v2](../contracts/raft-membership-v2.md#raft-메시지-프레임과-크기-한도)다. 연결 방향 규칙상 member ID가 낮은 node만 dial한다. 따라서 높은 ID의 node가 새 IP로 다시 시작하면, 낮은 ID 쪽이 이전 연결을 끝내야 새 node가 Raft 메시지를 받는다. 이 매핑은 응답 없이 사라진 이전 IP의 연결이 응용 송신 대기열 포화나 write deadline 실패를 기다리지 않고 끊기는지 확인한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-243 | CLU-02, CLU-10 | 같은 DNS 이름의 peer를 새 IP로 교체하고 이전 IP를 응답 없이 버림 | EKS 사건과 같게 leader가 node 2인 cluster만 사용한다. 다른 leader가 선출되면 새 cluster로 다시 시작한다. Node 1·2에서 node 3의 이전 IP 송수신을 DROP하고 node 3을 kill한다. 20초 뒤 같은 alias·volume·새 IP로 대체 node를 시작한다. 대체 node `/health/ready` 200이 `docker run` 반환 뒤 15초 이내여야 한다. |
| CLU-SCN-244 | CLU-02, CLU-10 | EKS에서 follower EC2를 OS 종료 없이 잃음 | preStop·drain이 실행되지 않는다. Leader는 이전 IP 연결에 확인받지 못한 송신 데이터가 생긴 뒤 약 10초 안에 그 연결을 닫는다. 기존 Pod 해제가 `deleted`이고, leader가 대체 node의 새 IP에 연결하며, 데이터 oracle이 통과한다. |

### 11.35 공유 구독 durable 전달의 owner 손실 수용 매핑

기준은 [ADR-0060](../decisions/ADR-0060-shared-subscription-durable-owner-loss-reroute.md), §3 6항, §4.2와 §5다. BP-02 capacity 시험(N-1)에서 owner node를 kill한 뒤 공유 group으로 보낸 QoS 1 message가 PUBACK을 받고도 어느 member에게도 가지 않았다. 이 매핑은 두 가지를 확인한다. 선택 단계가 죽은 owner의 member를 건너뛰는가? 그래도 그 member의 record가 지워질 때 message가 같은 group의 다른 member에게 옮겨지는가?

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-245 | CLU-03, CLU-07 | Owner node에 닿지 않는 member는 선택하지 않는다 | 도달 가능한 member만 primary가 되고 round-robin이 유지된다. 도달 불가 member는 fallback 끝에만 남는다. 다시 도달 가능해지면 원래 순서로 돌아온다. |
| CLU-SCN-246 | CLU-03, CLU-07 | 모든 owner에 닿지 않거나 owner를 모르면 기존 선택을 유지한다 | Group 전체가 도달 불가이면 기존 round-robin 순서와 같다. Owner 정보가 없는 session은 도달 가능으로 본다. |
| CLU-SCN-247 | CLU-03, CLU-07 | 공유 선택의 결과만 `shared_filter`를 싣는다 | 공유 group 대상은 전체 filter를 싣는다. 같은 client가 비공유 구독으로도 match하면 싣지 않는다. |
| CLU-SCN-248 | CLU-03, CLU-07 | Apply는 정확한 공유 구독에만 출처를 기록하고 feature·byte 한도를 지킨다 | 현재 owner/epoch로 그 filter를 구독 중일 때만 기록한다. Feature 비활성 상태의 `shared_filter`는 `feature_inactive`로 거절한다. Filter 길이가 byte 계산에 들어가고 snapshot 복원 뒤에도 같다. |
| CLU-SCN-249 | CLU-03, CLU-07 | CleanStart 재접속은 QUEUED와 QoS 1 미ACK message만 옮긴다 | QoS 2 진행 중 message와 만료 message는 옮기지 않는다. 한 message는 한 member에게만 간다. |
| CLU-SCN-250 | CLU-03, CLU-07 | 옮긴 message의 QoS·RETAIN은 목적지 구독을 따른다 | QoS는 `min(원래, 목적지)`다. RETAIN은 저장된 RETAIN과 목적지 Retain As Published가 모두 true일 때만 true다. |
| CLU-SCN-251 | CLU-03, CLU-07 | 제안한 node의 member를 먼저 고른다 | 죽은 node의 member가 다른 live node의 member보다 ring에서 먼저여도, 재접속을 제안한 node의 member가 message를 모두 받는다. |
| CLU-SCN-252 | CLU-03, CLU-07 | DISCARD와 ownership Release도 옮긴다 | Clean session 종료나 owner Release로 record가 지워져도 message가 남은 member로 간다. |
| CLU-SCN-253 | CLU-03, CLU-07 | Session expiry purge는 client ID 순서로 옮긴다 | 같은 apply에서 만료되는 member는 목적지가 되지 않는다. 남은 member가 만료 session의 message를 client ID 순서로 받는다. |
| CLU-SCN-254 | CLU-03, CLU-07 | 용량이 가득 찼거나 다른 member가 없으면 기존 결과와 같다 | 가득 찬 member는 건너뛴다. 받을 member가 없으면 버린다. 비공유 message는 건드리지 않는다. |
| CLU-SCN-255 | CLU-03, CLU-07 | Wire·snapshot 계약 | 잘못된 `shared_filter`를 거절한다. Feature가 활성이 아닌 snapshot의 `shared_filter`를 거절한다. 활성 상태의 round trip이 같다. |
| CLU-SCN-256 | CLU-03, CLU-07 | 3-node에서 leader를 kill해도 PUBACK한 QoS 1 message가 모두 member에게 간다 | Leader node에 member 2개, 생존 node에 member 2개를 둔다. Kill 뒤 40개를 생존 node에서 발행한다. PUBACK한 message는 모두 어느 member가 받아야 한다. Leader node의 member는 생존 node에 다시 접속한다. Clean member는 CleanStart=true로 다시 구독하고 persistent member는 session을 이어받는다. 선출 중 연결이 끊긴 persistent member는 session을 이어받아 queue를 받는다. |

### 11.36 1 MiB 초과 snapshot 송신 수용 매핑

기준은 §7 7항과 [Raft membership v2](../contracts/raft-membership-v2.md#raft-메시지-프레임과-크기-한도)다. 이 매핑은 feature 비활성 상태에서 큰 snapshot이 leader를 죽이지 않는 것만 확인한다. 활성 상태의 전송과 따라잡기는 [§11.37](#1137-raft-snapshot-stream-수용-매핑)에서 확인한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-257 | CLU-02 | 1 MiB 초과 `MsgSnap`은 송신하지 않고 node는 건강하다 | Leader가 1 MiB 초과 snapshot을 learner에게 보내야 하는 상황에서 `ProcessReady`가 오류를 반환하지 않는다. 1 MiB 초과 메시지가 batch에 없다. Learner의 진행 상태가 snapshot 대기에 머물지 않는다. 다른 voter와의 복제와 commit이 계속된다. 반복 거절에도 같은 peer의 `WARN`은 30초 안에 한 번뿐이다. |
| CLU-SCN-258 | CLU-02 | 경고 제한은 peer별 간격을 지킨다 | 같은 peer는 30초 안에 한 번만 통과하고, 다른 peer는 따로 센다. 간격이 지나면 다시 통과한다. |

### 11.37 Raft snapshot stream 수용 매핑

기준은 §7 7항, [ADR-0062](../decisions/ADR-0062-raft-snapshot-stream.md)와 [Raft snapshot stream v1](../contracts/raft-snapshot-stream-v1.md)이다. Feature는 명시적으로 활성화해야 한다. 비활성 상태의 wire·오류 동작은 §11.36과 같다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-259 | CLU-02 | Feature 활성화와 Raft 전달 | 비활성 상태에서는 기존 거절을 유지한다. 활성 상태에서는 4 MiB snapshot이 stream batch로 전달되고 learner가 복원한다. 실패 보고 뒤 Raft가 다시 시도한다. |
| CLU-SCN-260 | CLU-02 | Frame 한도·순서·무결성·재개 | 1 MiB 이하 frame과 512 KiB chunk를 쓴다. 끊김 뒤 같은 manifest는 offset에서 재개한다. CRC32C·digest·순서·overflow·oversized frame·모르는 identity를 거절한다. 송신 실패 이유를 안정된 code로 돌려준다. |
| CLU-SCN-261 | CLU-02 | Driver 비동기 송신과 결과 보고 | Peer별 한 전송만 실행한다. 성공·실패·취소를 Raft에 보고하고 driver를 깨운다. 고정 필드의 시작·완료·실패 로그를 남긴다. |
| CLU-SCN-262 | CLU-02 | 실제 process의 큰 snapshot catch-up | 3-voter cluster에서 feature를 활성화한다. Retained 700,000 byte 3개를 포함한 snapshot을 새 learner에 전송한다. 새 member가 voter가 되고 retained SHA-256이 모두 같아야 한다. 같은 volume 재시작 뒤 다시 Ready여야 한다. |
| CLU-SCN-263 | CLU-02 | Release compatibility와 rollback | N-1/N 혼합 운영·snapshot 교환·N-2 거절·활성화 뒤 rollback 거절을 기존 기준대로 확인한다. Commit된 member version은 feature API의 member profile에서 읽는다. |
| CLU-SCN-264 | CLU-02 | Historical-source release 회귀 | 지정 source `58a95f801e1467379db2eb6f3454ae44e954418a`와 현재 source의 혼합 운영·snapshot 교환·rollback을 확인한다. |

### 11.38 Fresh read proof 응답 유실 복구 수용 매핑

기준은 §4.2와 [ADR-0002 §1.2](../decisions/ADR-0002-cluster-consensus-library.md#12-dynamic-driver의-제한된-발행-제어-채택)다. Peer FIFO 용량은 예약이 아닌 snapshot이므로 leader의 proof 응답이 거절될 수 있다. 이 매핑은 응답이 유실된 뒤의 복구만 확인한다. 한꺼번에 요청한 read가 egress drain 뒤 모두 완료되는지는 [PRF-SCN-011](performance-and-verification.md)이 확인한다.

| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |
| --- | --- | --- | --- |
| CLU-SCN-265 | CLU-06, CLU-07, CLU-14 | Follower가 발행한 fresh read의 proof 응답이 유실된다 | 재발행 간격 전에는 proof를 다시 발행하지 않는다. 간격이 지나면 같은 key와 deadline으로 새 nonce의 proof를 발행하고 완료한다. 이전 발행의 ReadState는 새 proof를 완료하지 않는다. ReadState가 도착해 apply를 기다리는 proof, context가 끝난 read와 outbound 여유가 없는 proof는 이전 nonce를 유지한다. |
