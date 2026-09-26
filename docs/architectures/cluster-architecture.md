# Cluster architecture

NaruMQ cluster는 세 broker node가 같은 replicated state machine에 참여하는 목표 구조다. 각 node는 MQTT connection을 직접 수용한다. Session ownership과 durable write는 quorum commit을 거쳐 단일 owner와 복구 가능한 상태를 유지한다.

| 항목 | 값 |
| --- | --- |
| 관점 상태 | Target |
| 문서 상태 | Draft |
| 구현 증거 | 제한된 Phase 0/2 evidence: `internal/adapters/raft`, `internal/adapters/tcp`, `cmd/narumq -raft-peer-config`, `internal/application/takeover`, `tests/integration/raft_peer_transport_test.go`, `tests/integration/raft_consensus_test.go` |
| 범위 | Broker cluster의 runtime component와 상태 소유권 |
| 관련 요구사항 | [PRD](../requirements/PRD.md) CLU-01~CLU-14, MQTT-02~05, NFR-07~NFR-10 |
| 관련 스펙 | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md), [MQTT protocol과 session](../specs/mqtt-protocol-and-session.md) |
| 관련 결정 | [ADR-0002](../decisions/ADR-0002-cluster-consensus-library.md), [ADR-0003](../decisions/ADR-0003-durable-ack-barrier.md), [ADR-0004](../decisions/ADR-0004-session-ownership-and-fencing.md), [ADR-0008](../decisions/ADR-0008-raft-peer-transport.md), [ADR-0009](../decisions/ADR-0009-raft-production-state-profile.md) (Accepted, design target only), [ADR-0011](../decisions/ADR-0011-cluster-backup-restore-v1.md) (Accepted, v1 scope only). ADR-0002~0004는 Proposed |
| 관련 Contract | [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md), Active [Raft peer transport v1](../contracts/raft-peer-transport-v1.md), Draft [Replicated state v1](../contracts/replicated-state-v1.proto), Draft [Cluster backup v1](../contracts/cluster-backup-v1.md) |
| 최종 확인일 | 2026-09-26 |

## Runtime component view

아래 화살표는 runtime data·control flow다. 세 node는 같은 component 구조를 갖는다. Delivery의 client 송신 경로와 read-only query는 복잡도를 줄이기 위해 생략했다.

```mermaid
flowchart TB
    NLB["MQTT endpoint"]

    subgraph Cluster["NaruMQ 3-node cluster"]
        subgraph A["Broker node A"]
            IA["MQTT ingress"] --> UA["Application use cases"]
            UA --> SA["Session·routing·delivery policy"]
            SA -->|"command proposal"| RA["Raft core"]
            RA --> WA["WAL·snapshot A"]
            RA -->|"committed apply"| MA["Replicated state machine"]
            MA -->|"state view"| SA
        end

        subgraph B["Broker node B"]
            IB["MQTT ingress"] --> UB["Application use cases"]
            UB --> SB["Session·routing·delivery policy"]
            SB -->|"command proposal"| RB["Raft core"]
            RB --> WB["WAL·snapshot B"]
            RB -->|"committed apply"| MB["Replicated state machine"]
            MB -->|"state view"| SB
        end

        subgraph C["Broker node C"]
            IC["MQTT ingress"] --> UC["Application use cases"]
            UC --> SC["Session·routing·delivery policy"]
            SC -->|"command proposal"| RC["Raft core"]
            RC --> WC["WAL·snapshot C"]
            RC -->|"committed apply"| MC["Replicated state machine"]
            MC -->|"state view"| SC
        end

        RA <-->|"authenticated peer transport"| RB
        RB <-->|"authenticated peer transport"| RC
        RC <-->|"authenticated peer transport"| RA
    end

    NLB --> IA
    NLB --> IB
    NLB --> IC
```

Consensus module pin, file-backed WAL/snapshot과 sync target, application-state encoding family, 단일-group partition profile은 Accepted [ADR-0009](../decisions/ADR-0009-raft-production-state-profile.md)의 scoped design target이다. 이는 현재 구현의 production readiness, crash durability, durable ACK 또는 release 호환성을 승인하지 않는다. 고정 voter `{1,2,3}`의 transport v1은 Accepted [ADR-0008](../decisions/ADR-0008-raft-peer-transport.md)과 Active [contract](../contracts/raft-peer-transport-v1.md)에서 정한다.

## Raft core, cluster runtime과 제한된 acceptance evidence

`internal/adapters/raft`는 `go.etcd.io/raft/v3 v3.7.0`의 `RawNode`, stable WAL/snapshot persistence, committed command/configuration boundary와 typed application snapshot을 제공한다. `internal/adapters/raft/records`는 replicated-state protobuf와 cluster backup archive framing/checksum을 소유한다. NMQSTATE1 opaque command replay와 `internal/application/takeover`는 이전 deterministic ownership spike의 경계이며 production cluster backup format이 아니다.

`cmd/narumq -raft-peer-config`는 고정 `{1,2,3}` voter의 peer-only Raft와 mTLS TCP transport다. 이 mode는 MQTT listener, application proposal API 또는 cluster readiness를 제공하지 않는다. Dynamic cluster의 composition root는 별도 `cmd/narumq -cluster-config`다. 이 mode는 Raft, authenticated peer transport, membership state machine, Management API, route projection 및 fresh quorum을 요구하는 MQTT admission을 process 안에서 조립한다.

`POST /api/v1/cluster/backups`는 ClusterAdmin 전용으로 fresh quorum barrier 뒤 typed snapshot과 committed tail을 담은 versioned archive를 반환한다. `-cluster-restore`는 그 archive를 오프라인에서 검사·seed하고 종료한다. 현재 restore는 같은 cluster ID와 정확한 3 bootstrap voter identity를 요구하며 archive에 learner가 있으면 거절한다. 구현 위치는 [`cluster_backup.go`](../../cmd/narumq/cluster_backup.go), [`cluster_backup_restore.go`](../../cmd/narumq/cluster_backup_restore.go), [backup archive codec](../../internal/adapters/raft/records/backup.go)이고 process acceptance는 [Cluster §11.7](../specs/cluster-session-and-delivery.md#117-cluster-backuprestore와-snapshot-finalize-수용-매핑)에 있다.

이 evidence는 membership/readiness, commit-boundary archive, 3-voter offline seed와 local process crash/restart/resync만 검증한다. `offline`/`retained` message의 복제·backup, durable MQTT publish ACK, archive N/N-1 compatibility, retention/RPO/RTO 또는 production EBS power-loss를 승인하지 않는다. ADR-0002·0003·0004가 Proposed인 동안 전체 view도 production 또는 Verified 상태가 아니다.

## 상태 소유권

| 상태 | 권위 있는 위치 | Local copy의 역할 |
| --- | --- | --- |
| Membership과 configuration | Quorum-committed cluster state | Discovery·health cache |
| Session owner와 epoch | Replicated state machine | 현재 connection의 빠른 판정 |
| Persistent subscription | Replicated state | Node-local route projection 구성 |
| QoS 1·2 inflight와 handshake | Replicated state | Packet 송수신 진행 |
| Durable offline message | Replicated log·store | Owner의 delivery queue |
| Retained message | Replicated state | Topic lookup cache |
| Route metadata | Committed persistent-subscription state | Rebuildable node-local route projection |
| Active TCP connection과 socket buffer | Connection을 수용한 node | 복제하지 않는다. Reconnect로 교체한다. |
| Metric·log buffer | Node local, 외부 observability로 export | System of record가 아니다. |

Route metadata는 committed persistent subscription에서 계산하는 node-local disposable projection이다. Node는 routing readiness를 광고하기 전에 이를 rebuild해야 하며, projection은 ownership·checkpoint·drain barrier의 근거가 아니다. Physical rebuild mechanics와 route-index partition placement는 미결정이다.

## Durable publish 경로

아래 흐름은 구조적 경로다. 성공 ACK의 정확한 조건은 cluster spec과 ADR-0003을 기준으로 한다.

```mermaid
sequenceDiagram
    participant P as Publisher
    participant I as Ingress node
    participant O as Session·delivery policy
    participant R as Replication port
    participant Q as Raft quorum
    participant W as Stable storage

    P->>I: QoS 1/2 PUBLISH
    I->>O: 인증·ACL·quota를 통과한 command
    O->>R: message·delivery state proposal
    R->>Q: replicated command
    Q->>W: quorum member별 stable write
    W-->>Q: durability evidence
    Q-->>R: committed index
    R-->>O: commit receipt
    O-->>I: 성공 ACK 허용
    I-->>P: PUBACK 또는 PUBREC
```

Publisher ingress node와 consensus leader는 같지 않을 수 있다. Runtime은 Raft peer transport를 통해 proposal을 leader로 전달하고 commit 결과를 ingress에 돌려준다. MQTT policy는 Raft·transport 자료형에 종속되면 안 된다.

## Session ownership 경로

1. Connection을 수용한 node가 인증된 `(tenantId, clientId)`를 만든다.
2. Application이 현재 ownership record와 expected epoch를 확인한다.
3. Candidate owner가 새 epoch command를 replication port에 제안한다.
4. Quorum commit receipt를 받은 node만 session을 복구하고 traffic을 처리한다.
5. 현재 owner와 exact owner/epoch가 일치하는 모든 persistent subscription mutation과 inbound/outbound QoS inflight mutation만 authoritative session aggregate에 적용한다. 불일치 mutation은 최종 저장 경계에서 거절하며, route projection은 committed persistent subscription 결과에서만 파생한다.

동시 CONNECT, reconnect, drain과 rebalance는 같은 ownership primitive를 공유한다. 상태 전이와 오류 결과는 [Cluster session과 durable delivery 스펙](../specs/cluster-session-and-delivery.md)을 따른다.

Drain에서는 draining owner가 현재 epoch의 최종 subscription·inflight checkpoint를 commit한 뒤에만 successor의 더 큰 epoch ownership acquire를 commit한다. Acquire 이후 old owner의 route-affecting persistent subscription 또는 inbound/outbound QoS inflight mutation은 exact owner/epoch check에서 stale로 거절되고 authoritative aggregate를 바꾸지 않는다. Route projection은 committed persistent subscription 결과에서만 파생되며, 이를 rebuild한 뒤에만 readiness를 광고한다.

## Control path

| Control path | 구조적 역할 | 권위 경계 |
| --- | --- | --- |
| Join·leave | Member 후보를 검증하고 cluster membership을 변경 | Quorum commit |
| Readiness | Local sync, quorum, committed persistent-subscription에서 rebuild한 route projection과 replica 준비 상태를 집계 | Spec의 probe 조건 |
| Drain | Connection admission을 닫고 ownership을 이전 | `SAFE_TO_STOP` barrier |
| Snapshot·restore | Committed index 기준으로 state를 압축·복구 | Checksum과 schema compatibility |
| Mixed-version rollout | N-1↔N wire·state 호환성을 유지 | Release compatibility contract |

## Failure boundary

- 단일 process·Pod·host 손실은 남은 두 member의 quorum으로 복구하는 목표다.
- Minority partition은 새 ownership과 durable ACK를 만들지 않는다.
- Stable write quorum이 없으면 성공 PUBACK/PUBREC을 반환하지 않는다.
- TCP connection은 복제하지 않는다. Client reconnect와 session takeover로 복구한다.
- 단일 Region 전체 손실은 이 cluster view의 가용성 범위 밖이다.

Physical failure domain은 [EKS on EC2 deployment architecture](eks-ec2-deployment.md), fencing 원리는 [세션 소유권과 fencing](../explainers/session-ownership-and-fencing.md)에서 설명한다.

## 미결정 사항

아래 범위는 runtime 구현 또는 acceptance가 없거나 v1 backup profile의 제한을 넘어선다.

- Production WAL/snapshot lifecycle·retention, 지원 filesystem/volume의 power-loss 및 volume-loss guarantee
- Replicated-state schema의 migration·release compatibility vectors와 Active peer transport 밖 RPC encoding
- Route-index의 물리적 배치와 rebuild mechanics
- Full CLU-14: `offline`/`retained` state, archive retention, disaster-recovery RPO/RTO, cross-cluster migration 및 3-voter 제한 밖 restore
- Release N/N-1 compatibility matrix와 양방향 검증
