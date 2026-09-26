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
| 관련 Contract | [`replicated-state-v1.proto`](../contracts/replicated-state-v1.proto)는 Draft schema다. generated codec은 구현됐지만, 공식 크기 제한·마이그레이션·릴리스 호환성 규칙을 포함한 contract는 아직 검토 중이다. |
| 관련 해설 | [세션 소유권과 fencing](../explainers/session-ownership-and-fencing.md) |

## 1. 결정

Session key `(tenantId, clientId)`마다 quorum에 commit된 ownership record를 하나 둔다. Record는 최소 다음 논리 값을 가진다.

- `sessionKey`
- `ownerNodeId`
- 단조 증가하는 unsigned `epoch`
- ownership state
- application committed-command sequence position

이 sequence position은 ownership aggregate가 opaque committed-command bytes를 소비하며 부여한다. `internal/adapters/raft`의 spike boundary는 bytes를 해석하지 않고 순서와 Raft progress만 보존하며, `internal/application/takeover`의 ownership codec과 aggregate가 command 의미와 sequence를 소유한다. 따라서 이 identifier는 adapter의 Raft `StatusDTO.CommitIndex` 또는 snapshot index가 아니다.

새 owner는 ownership command가 quorum에 commit되고 이전 값보다 큰 epoch를 받은 뒤에만 session state를 읽고 쓸 수 있다.

Ownership acquire를 제외한 session aggregate mutation command의 의미상 envelope 필드는 `sessionKey`, `ownerNodeId`, `epoch`와 operation-specific subscription 또는 inflight mutation으로 한정한다. 이 이름은 필요한 의미 정보이며 serialized field나 byte format을 정하지 않는다. Replicated state machine은 현재 ownership과 일치하지 않는 command를 stale/not-authorized mutation으로 거절한다.

### 1.1 Session aggregate와 route projection

`sessionKey`마다 ownership record, persistent subscription, inbound QoS inflight와 outbound QoS inflight를 하나의 논리적 replicated session aggregate로 취급한다. 이 aggregate가 저장소에서 하나의 물리 레코드여야 한다는 뜻은 아니다. 한 `sessionKey`에 대한 command는 같은 committed ordering을 공유하고 하나의 aggregate 경계에서 원자적으로 적용해야 한다. Subscription 또는 inflight 변경 결과를 ownership 권한 판정 결과와 분리해 외부에 노출해서는 안 된다.

Route metadata는 committed persistent subscription에서만 계산하는 node-local disposable projection이다. Node는 이 projection을 authoritative state에서 다시 만들 수 있어야 하며, routing readiness를 광고하기 전에 rebuild를 완료해야 한다. Route projection은 ownership, session checkpoint 또는 drain barrier의 근거가 아니다.

WAL·snapshot target, protobuf encoding family와 단일 consensus group은 scoped [ADR-0009](ADR-0009-raft-production-state-profile.md)에 기록한다. 이 ADR은 exact serialized bytes와 protobuf schema, production error/DTO, Active Raft peer transport v1 범위 밖의 peer compatibility, durable offline message 경계, production application port와 `cmd/narumq` 조립 방식을 정하지 않는다. 해당 contract와 runtime 검증은 미완료다.

### 1.2 Non-acquire mutation fencing

Committed apply는 `ownerNodeId == currentOwnerNodeId`이고 `epoch == currentEpoch`인 경우에만 acquire 이외의 session mutation을 인가한다. Ownership record가 없거나 owner가 다르거나 epoch가 다르면 stale/not-authorized mutation으로 결정적으로 거절한다.

Epoch를 설치하거나 증가시킬 수 있는 command는 성공한 `Acquire`뿐이다. Subscription 또는 inflight mutation은 epoch가 현재 값보다 크더라도 새 owner나 epoch를 설치할 수 없다.

문법적으로 유효하지만 stale한 committed mutation은 application committed-command sequence position을 소비한다. 그러나 ownership, persistent subscription, QoS inflight와 route projection은 바꾸지 않는다. Production error/DTO와 exact serialized bytes는 [ADR-0009](ADR-0009-raft-production-state-profile.md)가 정한 encoding family에 맞춰 별도 Draft contract에서 확정해야 한다.

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
6. 이전 owner 또는 다른 epoch의 후속 subscription·inflight write는 exact owner/epoch check에서 거절된다.
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
3. Commit 응답이 유실된 뒤 client가 동일한 acquire를 retry하면, retry도 다음 application committed-command sequence position으로 commit되지만 현재 epoch와 expected epoch가 달라 `stale_epoch`로 결정적으로 거절되고 owner, epoch와 성공한 ownership record의 sequence position은 바뀌지 않는지 확인한다(`TestReplicatedSessionOwnership/lost_commit_response_retry_preserves_owner_and_epoch`).
4. Takeover 뒤 이전에 인가되었던 이전 owner의 route-affecting persistent subscription mutation과 inbound/outbound QoS inflight mutation이 각각 commit되고, application committed-command sequence position을 소비하며, stale authorization을 반환하고, 3개 node 모두에서 authoritative aggregate와 파생 route projection을 변경하지 않는지 확인한다(`TestReplicatedSessionOwnership/stale_owner_session_mutations_are_fenced_after_takeover`).
5. Snapshot restore 뒤 epoch가 감소하거나 재사용되지 않는지 확인한다(`TestReplicatedSessionOwnership/restart_recovers_ownership_epoch_and_command_sequence`).

항목 1~5는 in-process deterministic 3-node cluster와 takeover state machine에서 검증한다. 항목 4의 subscription·inflight bytes는 이 시험 안의 typed-token registry로만 해석하며 production persisted encoding이나 재시작 복구를 검증하지 않는다. 실제 MQTT runtime/drain wiring이 검증되기 전까지 이 결정은 `Proposed` 상태를 유지한다.

## 9. 재검토 조건

- Consensus 구조가 여러 shard/group으로 바뀐다.
- Ownership acquire latency가 확정 SLO를 반복해서 위반한다.
- Session state가 다른 system of record로 이동해 같은 atomic boundary를 유지할 수 없다.
