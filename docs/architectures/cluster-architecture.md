# Cluster architecture

NaruMQ cluster는 세 broker node가 같은 replicated state machine에 참여하는 목표 구조다. 각 node는 MQTT connection을 직접 수용한다. Session ownership과 durable write는 quorum commit을 거쳐 단일 owner와 복구 가능한 상태를 유지한다.

| 항목 | 값 |
| --- | --- |
| 관점 상태 | Target |
| 문서 상태 | Draft |
| 구현 증거 | 제한된 Phase 0/2 evidence: `internal/adapters/raft`, `internal/application/takeover`, `tests/integration/raft_consensus_test.go` |
| 범위 | Broker cluster의 runtime component와 상태 소유권 |
| 관련 요구사항 | [PRD](../requirements/PRD.md) CLU-01~CLU-14, MQTT-02~05, NFR-07~NFR-10 |
| 관련 스펙 | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md), [MQTT protocol과 session](../specs/mqtt-protocol-and-session.md) |
| 관련 결정 | [ADR-0002](../decisions/ADR-0002-cluster-consensus-library.md), [ADR-0003](../decisions/ADR-0003-durable-ack-barrier.md), [ADR-0004](../decisions/ADR-0004-session-ownership-and-fencing.md). 모두 Proposed |
| 관련 Contract | [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md), replicated state encoding은 예정 |
| 최종 확인일 | 2026-09-19 |

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

`etcd/raft`, WAL과 transport의 정확한 API는 Proposed [ADR-0002](../decisions/ADR-0002-cluster-consensus-library.md)와 후속 ADR의 대상이다. Diagram은 library 선택이 승인되었다는 뜻이 아니다.

## Phase 0/2 제한된 구현 증거

현재 repository는 `internal/adapters/raft`에서 `go.etcd.io/raft/v3 v3.7.0`의 `RawNode`와 deterministic logical-tick harness, opaque application command boundary(`BatchDTO.Commands`, `AppliedCommands`)와 NMQSTATE1 opaque command snapshot/WAL recovery를 검증한다. `internal/application/takeover`는 이 command bytes를 해석하고 application committed-command sequence position을 부여해 세션 소유권 상태 전이(single owner 선출, majority takeover, stale epoch 거절, restart 복구)를 검증한다. 이 evidence는 consensus core와 세션 소유권 상태 머신의 결정론적 동작 경계만 보여준다. `cmd/narumq`의 composition root에는 조립하지 않으며 production peer transport/TLS, replicated MQTT message/route state, durable ACK barrier와 membership workflow는 연결되지 않았다.

따라서 위 runtime view의 `Raft core`·`WAL·snapshot`·authenticated transport 흐름은 Target 구조이고, 현재 구현된 실제 경로는 [adapter source](../../internal/adapters/raft/node.go), [stable store](../../internal/adapters/raft/stable_store.go), [takeover state machine and codec](../../internal/application/takeover), [integration evidence](../../tests/integration/raft_consensus_test.go)의 제한된 in-process core다. ADR-0002와 ADR-0004가 Proposed인 동안 이 view를 production 또는 Verified 상태로 승격하지 않는다.
## 상태 소유권

| 상태 | 권위 있는 위치 | Local copy의 역할 |
| --- | --- | --- |
| Membership과 configuration | Quorum-committed cluster state | Discovery·health cache |
| Session owner와 epoch | Replicated state machine | 현재 connection의 빠른 판정 |
| Persistent subscription | Replicated state | Route index 구성 |
| QoS 1·2 inflight와 handshake | Replicated state | Packet 송수신 진행 |
| Durable offline message | Replicated log·store | Owner의 delivery queue |
| Retained message | Replicated state | Topic lookup cache |
| Route metadata | Committed session·subscription state에서 추적 가능한 cluster state | Node-local route index |
| Active TCP connection과 socket buffer | Connection을 수용한 node | 복제하지 않는다. Reconnect로 교체한다. |
| Metric·log buffer | Node local, 외부 observability로 export | System of record가 아니다. |

Route index의 정확한 복제·재구성 전략은 미결정이다. 어떤 방식을 선택해도 stale owner epoch가 delivery 권한을 유지해서는 안 된다.

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

Publisher ingress node와 consensus leader는 같지 않을 수 있다. Runtime forwarding은 허용하지만 MQTT policy가 Raft·transport 자료형에 종속되면 안 된다.

## Session ownership 경로

1. Connection을 수용한 node가 인증된 `(tenantId, clientId)`를 만든다.
2. Application이 현재 ownership record와 expected epoch를 확인한다.
3. Candidate owner가 새 epoch command를 replication port에 제안한다.
4. Quorum commit receipt를 받은 node만 session을 복구하고 traffic을 처리한다.
5. 이전 epoch의 route·inflight·delivery write는 최종 저장 경계에서 거절한다.

동시 CONNECT, reconnect, drain과 rebalance는 같은 ownership primitive를 공유한다. 상태 전이와 오류 결과는 [Cluster session과 durable delivery 스펙](../specs/cluster-session-and-delivery.md)을 따른다.

## Control path

| Control path | 구조적 역할 | 권위 경계 |
| --- | --- | --- |
| Join·leave | Member 후보를 검증하고 cluster membership을 변경 | Quorum commit |
| Readiness | Local sync, quorum, route와 replica 준비 상태를 집계 | Spec의 probe 조건 |
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

- Consensus module의 production version과 checksum
- WAL·snapshot storage engine과 `fsync` primitive
- Peer RPC protocol, framing과 node identity bootstrap
- Replicated state encoding contract
- Route index partition·replication·rebuild 방식
- 하나 또는 여러 consensus group을 사용하는 partition 전략
- Backup store와 disaster recovery RPO/RTO

각 선택은 별도 ADR 또는 contract가 승인될 때 이 view에 반영한다.
