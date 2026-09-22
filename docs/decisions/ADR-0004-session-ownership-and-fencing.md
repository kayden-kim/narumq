# ADR-0004: Session ownership은 quorum-committed epoch로 fencing한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Proposed |
| 제안일 | 2026-08-16 |
| 결정일 | 미정 |
| 결정자 | 미정 |
| 관련 요구사항 | CLU-04~CLU-06, CLU-10, DEP-08~DEP-10, NFR-07~NFR-09 |
| 관련 스펙 | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md), [Drain과 rolling update](../specs/drain-and-rolling-update.md) |
| 관련 Architecture | [Cluster architecture](../architectures/cluster-architecture.md) |
| 관련 결정 | [ADR-0002](ADR-0002-cluster-consensus-library.md), [ADR-0003](ADR-0003-durable-ack-barrier.md) |
| 관련 Contract | Replicated state encoding v1 예정. Storage·RPC 결정 전에는 작성하지 않는다. |
| 관련 해설 | [세션 소유권과 fencing](../explainers/session-ownership-and-fencing.md) |

## 1. 결정

Session key `(tenantId, clientId)`마다 quorum에 commit된 ownership record를 하나 둔다. Record는 최소 다음 논리 값을 가진다.

- `sessionKey`
- `ownerNodeId`
- 단조 증가하는 unsigned `epoch`
- ownership state
- 마지막 committed index

새 owner는 ownership command가 quorum에 commit되고 이전 값보다 큰 epoch를 받은 뒤에만 session state를 읽고 쓸 수 있다.

Session, inflight, offline queue와 route를 바꾸는 모든 command는 session key와 epoch를 포함한다. Replicated state machine은 현재 epoch와 일치하지 않는 command를 `stale_epoch`로 거절한다.

Wall clock timeout이나 local connection 상태만으로 ownership을 획득했다고 판단하지 않는다. 시간 기반 lease는 liveness 보조 신호로 사용할 수 있지만 fencing authority가 될 수 없다.

## 2. 배경

Network partition, 긴 GC pause와 node 재시작은 이전 owner가 자신이 아직 유효하다고 오판하게 만든다. Session owner를 local memory나 timeout으로만 관리하면 두 node가 같은 QoS state와 route를 동시에 수정할 수 있다.

PRD는 partition 중에도 단일 활성 owner와 stale write 거절을 요구한다. 따라서 권한 판정은 연결 생존 여부가 아니라 quorum에 commit된 세대 값에 의존해야 한다.

## 3. 결정 요인

1. 어떤 partition에서도 성공 write owner가 둘이면 안 된다.
2. Storage와 route index에서 stale write를 최종 거절해야 한다.
3. Client reconnect와 drain takeover가 같은 mechanism을 사용해야 한다.
4. Crash 뒤 ownership과 inflight를 재구성할 수 있어야 한다.
5. 실제 clock 오차가 safety를 결정하면 안 된다.

## 4. Ownership 전이

1. Candidate node가 인증과 tenant/client binding을 검증한다.
2. 현재 ownership record와 session state version을 읽는다.
3. Candidate가 `Acquire(sessionKey, expectedEpoch, ownerNodeId)`를 제안한다.
4. State machine은 expected epoch가 현재 값과 같을 때만 epoch를 증가시킨다.
5. Commit receipt를 받은 candidate만 새 owner로 동작한다.
6. 이전 owner의 후속 write는 저장 경계에서 거절된다.
7. 새 owner는 committed session·inflight를 복구한 뒤 client traffic을 진행한다.

동시 candidate 중 하나만 expected epoch 비교에 성공해야 한다. 실패 candidate는 최신 record를 다시 읽거나 connection을 거절한다.

## 5. 자료형과 overflow

Epoch는 contract에서 unsigned 64-bit 논리 값으로 취급한다. Encoding은 cluster persisted-state contract에서 고정한다. Overflow에 도달하면 wrap하지 않고 해당 session mutation을 중단해 operator intervention을 요구한다.

`nodeId`는 cluster lifetime 동안 재사용하지 않는다. Pod name이나 IP를 영구 node ID로 사용하지 않는다.

## 6. 대안

| 대안 | 장점 | 제외 이유 |
| --- | --- | --- |
| Connection이 있는 node를 owner로 간주 | 단순하다. | Partition과 pause에서 이중 owner를 막지 못한다. |
| Clock 기반 lease만 사용 | 빠른 local 판정 | Clock skew와 stop-the-world pause가 stale write를 허용할 수 있다. |
| 중앙 leader memory에 owner map 저장 | 빠른 lookup | Leader crash 뒤 권한 근거를 복구할 수 없다. |
| Storage compare-and-swap만 사용 | 명시적 fencing 가능 | Consensus log·route·inflight와 별도 순서를 만들면 원자성이 깨진다. Storage adapter가 같은 committed epoch를 적용하는 방식은 사용한다. |

## 7. 결과

- Reconnect, takeover, rebalance와 drain이 같은 ownership primitive를 사용한다.
- 모든 session write에 epoch가 전파되어야 한다.
- Epoch 누락은 compile-time 또는 architecture test에서 차단해야 한다.
- Ownership 획득에 quorum latency가 추가된다.
- Minority는 새 session ownership을 얻지 못한다.

## 8. 검증

Phase 2 replicated session ownership core는 다음 검증 기준을 갖는다.

1. 같은 session key의 동시 CONNECT에서 성공 owner가 하나인지 확인한다(`TestReplicatedSessionOwnership/concurrent_acquire_elects_single_owner`).
2. Owner를 network partition한 뒤 majority에서 takeover하고 이전 owner write를 모두 거절한다(`TestReplicatedSessionOwnership/minority_cannot_acquire_or_advance_epoch`).
3. Commit 응답 유실과 client retry가 epoch를 불필요하게 중복 증가시켜도 safety를 유지하는지 확인한다(후속 과제).
4. Drain takeover 직후 이전 node의 route·inflight write를 거절한다(후속 과제).
5. Snapshot restore 뒤 epoch가 감소하거나 재사용되지 않는지 확인한다(`TestReplicatedSessionOwnership/restart_recovers_ownership_epoch_and_commit_index`).

항목 1, 2, 5는 in-process deterministic 3-node cluster와 takeover state machine에서 검증한다. 항목 3, 4와 실제 MQTT runtime/drain wiring이 검증되기 전까지 이 결정은 `Proposed` 상태를 유지한다.
## 9. 재검토 조건

- Consensus 구조가 여러 shard/group으로 바뀐다.
- Ownership acquire latency가 확정 SLO를 반복해서 위반한다.
- Session state가 다른 system of record로 이동해 같은 atomic boundary를 유지할 수 없다.
