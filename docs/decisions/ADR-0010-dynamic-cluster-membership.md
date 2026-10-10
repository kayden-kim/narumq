# ADR-0010: Cluster membership은 staged learner 전환과 영구 node ID를 사용한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (membership 설계만; runtime 미검증) |
| 결정일 | 2026-09-24 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD](../requirements/PRD.md#92-클러스터와-세션) CLU-01~CLU-02, CLU-04~CLU-11, NFR-07~NFR-10, SEC-09~SEC-11 |
| 관련 스펙·데이터 계약 | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md), Draft [Raft membership v2](../contracts/raft-membership-v2.md), Draft [Replicated state v1](../contracts/replicated-state-v1.proto) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md) |
| 상위 결정 | [ADR-0002](ADR-0002-cluster-consensus-library.md) (`Proposed` 유지), [ADR-0008](ADR-0008-raft-peer-transport.md), [ADR-0009](ADR-0009-raft-production-state-profile.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | [ADR-0009 §1.4](ADR-0009-raft-production-state-profile.md#14-consensus-group-partition-profile)의 고정 `{1,2,3}` voter assumption만 대체 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

이 결정은 membership과 MQTT admission의 설계만 채택한다. WAL·snapshot target, `etcd/raft` 선택, protobuf encoding family와 단일 Raft group에 대한 ADR-0009의 나머지 결론은 바꾸지 않는다. 구현 또는 acceptance 시험이 통과했다는 뜻은 아니다.

## 1. 결정

### 1.1 Group과 identity

- Production target은 하나의 Raft group과 steady-state voter 3개다. 초기 bootstrap은 operator가 지정한 정확히 3개의 고유한 양수 `uint64` ID로 수행한다.
- Voter 교체는 새롭고 이전에 사용되지 않은 양수 `uint64` ID를 사용한다. 기준 예시는 `{1,2,3}`에서 `{1,2,4}`로 바꾸는 것이다. 제거한 ID를 tombstone으로 영구 보존하고 재사용하지 않는다.
- Member identity는 `(clusterId, memberId, address, DNS SAN, SHA-256 certificate fingerprint, releaseVersion, cluster protocol version, state-schema version)`으로 결합한다. Certificate SAN과 fingerprint가 committed stage record와 다르면 해당 peer traffic을 `RawNode.Step` 전에 거절한다.
- 기존 `-raft-peer-config`와 Active Raft peer transport v1은 별도 fixed `{1,2,3}` peer-only compatibility profile로 유지한다. 이 profile을 dynamic membership 또는 combined MQTT cluster runtime으로 해석하지 않는다. v1 wire는 바꾸지 않는다.

### 1.2 승인과 변경 순서

- `ClusterAdmin`만 management API로 join stage 또는 planned remove를 승인한다. Joiner는 설정된 seed 주소를 통해 cluster identity와 quorum-committed stage record를 확인한 뒤 Raft peer traffic을 시작한다.
- 각 API mutation은 caller-supplied `Idempotency-Key`를 받는다. 해당 key와 생성된 operation ID를 stage record와 함께 replicated log에 commit한다. 같은 key·같은 request 재시도는 같은 operation을 반환하고, 같은 key의 다른 내용 또는 같은 ID의 다른 identity는 거절한다.
- Join 순서는 다음과 같다: ID·certificate·compatibility를 committed state에 durable stage → `ConfChangeV2`로 learner 추가 및 commit → snapshot과 log tail catch-up 확인 → 별도의 `ConfChangeV2`로 voter 승격 및 commit → 이전 voter의 session ownership·subscription·inflight checkpoint 이전 → 이전 voter 제거 및 commit.
- 정상 3-voter group에서는 한 번에 한 membership operation만 진행한다. 이전 operation이 terminal phase에 도달할 때까지 다음 변경을 거절한다. 성공 응답은 stage 또는 현재 operation phase가 commit된 뒤에만 보낸다.
- Planned leave는 모든 owning session의 successor ownership과 inbound/outbound QoS checkpoint commit을 먼저 요구한다. Transfer 또는 checkpoint가 실패하거나 quorum이 없으면 기존 voter를 유지한다.
- Heartbeat timeout, peer socket 종료 또는 liveness failure만으로 voter를 자동 제거하지 않는다. Unexpected failure와 planned replacement는 서로 다른 절차다.
- Lost HTTP response는 replicated operation record를 조회해 같은 operation을 재개한다. 재시도는 중복 stage, ID 재사용 또는 중복 promotion을 만들지 않는다.

### 1.3 Readiness와 MQTT 범위

- Cluster-mode readiness는 committed local voter role, snapshot/WAL replay 및 committed index 동기화, committed subscription에서 재구축한 route projection, persistence health와 fresh quorum `ReadIndex` barrier를 모두 요구한다. 재시작 후 barrier와 projection을 다시 통과하기 전에는 READY가 아니다.
- 증명된 quorum barrier는 짧은 deadline 동안만 유효하다. 실패한 다음 probe 또는 deadline 만료 시 readiness와 새 MQTT admission을 즉시 차단한다. Liveness는 quorum과 독립적이다.
- Cluster-mode CONNECT는 ownership acquire가 commit되기 전 긍정 CONNACK을 반환하지 않는다. SUBSCRIBE/UNSUBSCRIBE는 typed subscription command commit 뒤에만 SUBACK/UNSUBACK 성공을 반환한다.
- 이 membership acceptance slice에는 전체 durable publish acceptance가 포함되지 않았다. 당시 별도 replicated acceptance path가 없는 QoS 1/2 mutation은 positive PUBACK/PUBREC을 반환하지 않고 거절하거나 연결을 닫았다. 이후 QoS 1/2 durable path는 [ADR-0015](ADR-0015-cluster-durable-qos-delivery.md)와 [Cluster §11.11](../specs/cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑)에서 제한된 범위로 검증했다. Readiness는 membership과 route admission 준비를 뜻할 뿐 production QoS durability, WAL/fsync gate 또는 release N/N-1 support를 뜻하지 않는다.

### 1.4 Wire·state contract와 변경 승인

- New combined cluster mode는 별도의 Raft membership v2 handshake와 Draft [replicated-state v1 proto](../contracts/replicated-state-v1.proto)를 사용한다. Fixed peer transport v1 및 experimental `NMQSTATE1`과 silent conversion하지 않는다.
- Candidate는 stage 전에 Raft payload를 보내지 않는다. Peers는 mTLS chain·SAN, cluster ID, staged ID, certificate fingerprint, advertised address, release·protocol·schema version, phase와 operation ID를 확인한 뒤에만 frame을 `Step`에 전달한다.
- Unknown required record, unknown field, malformed/canonicality-invalid frame, 미지원 major 또는 정확히 협상되지 않은 minor는 Raft에 전달하지 않는다. N/N-1 호환성은 양방향 test matrix가 실제로 통과하기 전까지 주장하지 않는다.
- 전체 Ready와 configuration record는 stable persistence가 성공한 뒤에만 outbound 또는 acknowledgment 경계로 진행한다. `ConfChangeV2`는 committed entry일 때만 `RawNode.ApplyConfChange`로 적용한다. Proposal 수락은 commit이 아니다.

## 2. 배경

현재 Raft adapter와 peer-only command는 fixed `{1,2,3}` peer set을 사용하고, MQTT runtime은 자체 in-memory session/routing state로 분리되어 있다. [ADR-0009](ADR-0009-raft-production-state-profile.md)는 기존 fixed-voter assumption으로 한 consensus group을 설명하고, [ADR-0008](ADR-0008-raft-peer-transport.md)은 v1 identity allowlist를 정한다. Dynamic membership을 이 경계들에 암묵적으로 넣으면 disk identity, bootstrap과 transport contract가 서로 다르게 해석된다.

Membership 변경은 voter set과 quorum 계산을 바꾸므로 peer discovery나 heartbeat만으로 처리할 수 없다. Joiner를 먼저 independent cluster로 bootstrap하면 두 Raft group이 생길 수 있다. 따라서 identity를 durable stage하고 learner로 catch-up한 뒤 voter로 승격한다. Planned removal은 ownership·checkpoint 이전과 membership removal의 순서를 보장해야 한다.

## 3. 결정 요인

1. Join, replacement와 leave 중 quorum 안전성 및 retry idempotency를 유지한다.
2. Certificate와 Raft identity가 committed member identity에 결합되어야 한다.
3. Restart 뒤 operation phase, retired ID와 configuration state가 replay 가능해야 한다.
4. Readiness가 membership, log/snapshot, route projection, persistence와 quorum을 반영해야 한다.
5. 기존 v1 profile과 현재 NMQSTATE1 데이터의 의미를 바꾸지 않는다.
6. 실제 실행하지 않은 storage, QoS, release compatibility 기준을 완료했다고 표시하지 않는다.

## 4. 선택 근거

Learner는 기존 quorum 투표권을 바꾸지 않고 snapshot과 이후 log를 받을 수 있어 catch-up 이전에 새 voter를 구성원으로 인정하는 위험을 줄인다. `ConfChangeV2` commit을 유일한 configuration authority로 삼으면 voter set과 durable stage/operation record를 같은 committed ordering에서 복구할 수 있다. At-most-one in-flight change는 target 3-voter profile에서 동시 config transition 상태를 제한한다.

Persisted tombstone과 operation ID는 ID 재사용 및 응답 유실 retry에 대해 cluster-wide 기준을 제공한다. Certificate fingerprint와 SAN을 committed record와 비교하면 TLS trust-chain 검증만으로 다른 staged member의 identity를 주장하는 일을 막는다. Timeout 기반 자동 removal을 금지하면 transient partition이 무승인 membership change로 바뀌지 않는다.

Fresh `ReadIndex`와 route projection index를 readiness 조건으로 결합하면 stale process-local READY flag만으로 MQTT 연결을 수락하는 경로를 막는다. 이는 production QoS delivery와 durable ACK 전부를 구현했다는 주장이 아니다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Fixed `{1,2,3}` allowlist만 사용 | 현재 peer-only transport와 단순하게 맞는다. | 안전한 replacement, learner catch-up, removal과 ID retirement를 표현하지 못한다. | v1 compatibility profile로 한정하고 combined cluster membership으로 승격하지 않는다. |
| New member를 즉시 voter로 추가 | 단계가 짧다. | Catch-up 전 새 voter를 구성에 넣어 quorum progress와 election availability를 위험에 노출한다. | Learner catch-up 후 committed promotion을 선택한다. |
| Heartbeat timeout으로 자동 voter 제거 | 단일 실패에 자동 반응한다. | Partition과 실제 영구 장애를 구분하지 못하고 3-node quorum을 훼손할 수 있다. | 승인된 planned replacement/remove만 허용한다. |
| Joiner를 별도 bootstrap한 뒤 merge | local startup이 간단하다. | 독립 log/configuration history가 생기며 안전한 merge boundary가 없다. | Joiner는 committed stage 후 learner로 초기화하고 independent `Bootstrap`을 금지한다. |
| 모든 membership update를 별도 control-plane DB에 저장 | Raft log를 피한다. | 두 system-of-record 간 commit/recovery 불일치를 만든다. | Membership registry, operation phase와 tombstone은 기존 단일 Raft group의 committed state에 둔다. |

## 6. 결과

### 긍정적 결과

- Configuration, identity, operation phase와 retired ID가 quorum-committed history로 복구된다.
- Replacement는 learner catch-up, ownership transfer, checkpoint와 voter removal 순서가 명시된다.
- 기존 peer-only v1 및 standalone MQTT 동작이 별도 mode로 남는다.
- Readiness와 admission에 관찰 가능한 sync·projection·quorum 경계가 생긴다.

### 부정적 결과와 비용

- Management API, v2 handshake, membership registry, asynchronous ReadIndex, typed state codec, transport reconciliation과 crash/restart 시험이 추가로 필요하다.
- Membership과 management stage record 모두의 storage·snapshot replay 검증이 필요하다.
- Production-like filesystem crash/disk-full/backup, QoS 1/2 durable ACK 및 N/N-1 호환성은 이 결정으로 닫히지 않는다.

### 후속 작업

- Draft [raft-membership-v2 contract](../contracts/raft-membership-v2.md)와 [replicated-state-v1.proto](../contracts/replicated-state-v1.proto)를 구현 전에 검토·고정한다.
- Cluster spec §1/§11와 architecture view를 동기화하고 실제 수용시험 증거를 추가한다.
- 구현 뒤에도 통과하지 않은 membership, crash/restart, readiness 또는 compatibility 기준은 열어 둔다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Replacement가 committed learner→voter 순서를 따른다. | 실제 3-node TLS process에서 `{1,2,3}` → `{1,2,4}` 수행 | 단계별 committed history와 모든 survivor의 동일한 management-visible voter 집합; reused ID 거절 | PASS: `TestDynamicClusterMembershipProcessAcceptance`에서 survivor의 voter set `{1,2,4}`, retired ID 재사용 거절, quorum 손실 후 survivor readiness를 확인했다. `go test ./tests/integration -run '^TestDynamicClusterMembershipProcessAcceptance$' -count=1` |
| Lost response 재시도가 동일 operation을 재개한다. | Stage, promotion, remove 응답을 각각 유실시킨 뒤 같은 idempotency key로 재시도 | 동일 operation ID, 중복 ID·promotion 없음 | PASS: stage/promotion/removal 재시도에서 동일 operation ID와 최종 phase를 확인했다. `TestDynamicClusterMembershipProcessAcceptance` |
| Stage 전 candidate traffic과 mismatch traffic이 apply되지 않는다. | mTLS CA/SAN, cluster, ID, fingerprint, release/schema 조합을 바꿔 연결 | Invalid 조합은 configuration 또는 application state를 바꾸지 않고 `Step` 전에 거절 | PASS: `TestDynamicPeerRejectsTLSCertificateIdentityMismatchBeforeRaftDelivery`, `TestDynamicPeerRejectsForeignCAAndSANAndClusterMismatchBeforeRaftDelivery`, `TestDynamicClusterMembershipProcessAcceptance` |
| Restart가 각 transition boundary를 수렴시킨다. | ConfChangeV2 persistence 전후, snapshot 전송 중, promotion·remove 전후 crash/reopen | 수동 데이터 수정 없이 committed log, stage/tombstone, phase, `ConfState`와 app state 수렴 | PASS: build-tagged process test가 persistence/apply와 snapshot-install 경계를 강제 종료 후 재시작하여 voter set, phase와 retired identity 수렴을 확인했다. `go test ./tests/integration -run '^TestDynamicClusterMembershipTransitionCrashRecovery$' -count=1 -v` |
| Readiness는 quorum·projection sync 전에 닫힌다. | Fresh join/restart, route rebuild 실패, quorum loss와 barrier expiry를 실제 endpoint에서 관찰 | 조건 미충족 시 `/health/ready` 503과 신규 MQTT admission 거절; 모두 복구 후에만 200 | PASS: readiness snapshot unit cases, projection-rebuild failpoint process test, quorum-loss/removal-handoff process test에서 503/CONNECT 거절과 quorum 복구 후 READY를 확인했다. `go test ./cmd/narumq -run '^TestClusterReadinessGate$' -count=1`; `go test ./tests/integration -run '^(TestDynamicClusterMembershipTransitionCrashRecovery|TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum)$' -count=1 -v` |

## 8. 재검토 조건

- `etcd/raft` 설정이 learner, `ConfChangeV2`, committed `ApplyConfChange` 또는 ReadIndex 요구를 충족하지 못한다.
- 실제 fault test가 at-most-one operation 또는 3-voter replacement 순서의 안전성 부족을 보인다.
- Authentication, operator authorization 또는 새 schema의 security review에서 identity binding이 불충분하다고 확인된다.
- Workload evidence가 단일 group 또는 3-voter baseline을 바꾸도록 요구한다. 그 경우 별도 migration 및 failure-domain 결정을 기록한다.
