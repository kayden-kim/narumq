# Cluster release compatibility v1 Contract

이 문서는 release N과 직전 지원 release N-1 node가 rolling upgrade·rollback 중 지켜야 할 cluster 호환성 범위를 정의한다. 실제 release 번호와 test artifact는 각 release matrix에서 이 template을 채운다.
[ADR-0009](../decisions/ADR-0009-raft-production-state-profile.md)는 versioned protobuf application-state encoding family를 design target으로 선택했지만 exact schema는 정하지 않는다. 이 Draft contract는 future persisted command/snapshot의 release N/N-1 compatibility를 다루며 아직 호환성을 검증하지 않았다. 이는 fixed-voter wire만 정의하는 Active [Raft peer transport v1](raft-peer-transport-v1.md)과 별도 경계다.

| 항목 | 값 |
| --- | --- |
| 이름과 version | `narumq-cluster-release-compatibility` 1.0.0 |
| 상태 | Draft |
| Producer | Release N·N-1 broker node와 rollout controller |
| Consumer | Release N·N-1 broker node와 rollout controller |
| 방향 | Cluster RPC, replicated log, snapshot, management·rollout status |
| 기준 artifact | 이 문서의 compatibility rule과 release matrix |
| 기준 spec | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md), [Drain과 rolling update](../specs/drain-and-rolling-update.md) |
| 관련 Architecture | [Cluster architecture](../architectures/cluster-architecture.md), [EKS on EC2 deployment](../architectures/eks-ec2-deployment.md) |
| 관련 requirement | CLU-11, NFR-15, DEP-18 |
| 관련 결정 | [ADR-0002](../decisions/ADR-0002-cluster-consensus-library.md), [ADR-0006](../decisions/ADR-0006-external-rollout-controller.md), [ADR-0009](../decisions/ADR-0009-raft-production-state-profile.md) |
| 검증 | `tests/compatibility/cluster-release-v1` 예정. 현재 미구현 |

## 1. 지원 범위

Release N은 N-1과 다음 조합을 지원해야 한다.

- N-1 node가 leader이고 N node가 follower인 mixed cluster
- N node가 leader이고 N-1 node가 follower인 mixed cluster
- N-1에서 N으로 한 node씩 upgrade
- N에서 N-1로 한 node씩 rollback
- Mixed cluster 중 leader failure와 재선출
- Mixed cluster 중 drain, session takeover와 snapshot 전송

N-2 이하와의 mixed cluster는 지원하지 않는다. Rollout preflight는 지원 범위를 벗어난 version 조합을 Pod 교체 전에 거절해야 한다.

## 2. Compatibility 축

| 축 | 같은 major에서 지켜야 할 규칙 |
| --- | --- |
| Cluster RPC | N과 N-1이 서로 필수 message를 decode하고 모르는 optional field를 무시한다. |
| Replicated command | N은 N-1이 이해하지 못하는 command를 mixed cluster에서 propose하지 않는다. |
| Persisted record | N이 쓴 record를 rollback한 N-1이 읽거나 명시적으로 feature-gated 상태로 남긴다. |
| Snapshot | Sender·receiver 양방향으로 schema와 checksum을 검증한다. |
| Membership | Node ID와 member generation의 의미를 바꾸지 않는다. |
| Management API | Rollout controller가 사용하는 v1 field와 error code를 유지한다. |
| Metric·health | Readiness와 drain state 의미를 release 사이에서 바꾸지 않는다. |

## 3. Version handshake

Cluster connection은 application traffic 전에 다음 논리 정보를 교환해야 한다.

| Field | Type | 의미 |
| --- | --- | --- |
| `releaseVersion` | non-empty string | Node release version |
| `clusterProtocolMajor` | unsigned integer | Breaking wire compatibility generation |
| `clusterProtocolMinor` | unsigned integer | Backward-compatible wire capability level |
| `stateSchemaMajor` | unsigned integer | Breaking persisted-state generation |
| `stateSchemaMinor` | unsigned integer | Backward-compatible state capability level |
| `features` | set of string | 명시적으로 활성화 가능한 capability |
| `minimumPeerRelease` | non-empty string | 이 node가 지원하는 가장 오래된 peer release |

정확한 RPC encoding은 transport ADR과 machine-readable contract에서 확정한다. 이 artifact가 없으면 본 contract를 Active로 전환할 수 없다.

Major가 다르거나 peer release가 범위를 벗어나면 join을 거절한다. Minor 차이는 필요한 feature가 교집합에 있을 때만 허용한다.

## 4. Feature activation

- 새 feature는 mixed cluster에서 기본 비활성화한다.
- 모든 quorum member가 feature를 광고하고 N-1 rollback이 불필요해진 뒤에만 persistent format을 활성화한다.
- Activation은 quorum에 commit된 cluster setting으로 기록한다.
- Activation 뒤 N-1이 읽을 수 없는 state를 쓰면 rollback 가능 상태를 끝낸 것으로 표시해야 한다.
- Rollback window 안에는 irreversible migration을 실행해서는 안 된다.

## 5. Persisted state와 snapshot

- 모든 record는 schema major/minor와 record type을 식별할 수 있어야 한다.
- Consumer는 같은 major의 모르는 optional field를 보존하거나 무시하되 기존 의미를 바꾸지 않는다.
- 모르는 required record type이나 major를 만나면 READY가 되지 않고 `incompatible_state_schema`를 보고한다.
- Snapshot manifest는 producer release, schema version, last included index, object count와 checksum을 포함해야 한다.
- Snapshot restore 뒤 object count와 checksum이 맞지 않으면 apply하지 않는다.

## 6. Release matrix

각 release N은 다음 표를 실제 version과 artifact link로 채워 release bundle에 포함한다.

| Current | Peer | Upgrade | Rollback | Leader 양방향 | Snapshot 양방향 | 상태 |
| --- | --- | --- | --- | --- | --- | --- |
| N | N | 필수 | 해당 없음 | 필수 | 필수 | 미검증 |
| N | N-1 | 필수 | 필수 | 필수 | 필수 | 미검증 |
| N | N-2 | 비지원 | 비지원 | 비지원 | 비지원 | 명시적 거절 |

`미검증` 행이 있으면 release gate를 통과할 수 없다.

## 7. Breaking change

다음 변경은 cluster protocol 또는 state schema major 변경 후보이다.

- 기존 required field 삭제·type·의미 변경
- 기존 node가 해석할 수 없는 required command 추가
- Snapshot checksum 또는 framing의 비호환 변경
- Epoch, commit index, node ID와 membership 의미 변경
- N-1 rollback 경로를 제거하는 persisted-state migration

Major 변경은 in-place mixed upgrade로 처리하지 않는다. 별도 migration plan, backup·restore 검증과 downtime 또는 bridge release가 필요하다.

## 8. Draft blocker

- Cluster RPC와 persisted record의 machine-readable encoding contract
- 실제 release N/N-1 version
- Leader·snapshot 양방향 compatibility harness
- Irreversible feature activation과 rollback window 운영 정책
