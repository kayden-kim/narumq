# Raft failure-bound 증거 이후 잔여 작업

이 목록은 [cluster 수용 시험](specs/cluster-session-and-delivery.md#11-수용-시험)과 [ADR-0002 §7](decisions/ADR-0002-cluster-consensus-library.md#7-검증)에서 이번 in-process WAL 쓰기 실패·느린 follower 시험만으로 닫히지 않은 작업을 추린 것이다. 프로젝트 전체 백로그나 새 요구사항이 아니다. 동작과 완료 판정의 기준은 링크한 스펙·결정 기록이며 이 목록이 대신하지 않는다.

## Production 경계와 설계 결정

- [x] [ADR-0002 §7 항목 5](decisions/ADR-0002-cluster-consensus-library.md#7-검증): 고정 voter `{1,2,3}`의 production peer-only transport, mTLS 인증·framing, bounded queue/backpressure와 Raft 재생성을 구현·검증했다. Evidence는 [Cluster §11.3](specs/cluster-session-and-delivery.md#113-phase-0-gate-4-production-peer-transport-mapping)에 연결한다. 이 항목은 ADR-0002의 Proposed 상태, dynamic membership 또는 전체 cluster 수용 완료를 뜻하지 않는다. 기존 시험의 미응답 entry-bearing `MsgApp` 범위 16개 이하는 전송 대기열 또는 전체 Raft log 한계가 아니다.
- [x] [Cluster architecture의 미결정 사항](architectures/cluster-architecture.md#미결정-사항): production consensus module version/checksum, WAL·snapshot storage engine 및 `fsync`, replicated state encoding, group partition 전략을 scoped design target으로 [ADR-0009](decisions/ADR-0009-raft-production-state-profile.md)에 결정·기록했다. `go mod verify`는 `all modules verified`였고, `go mod download -json go.etcd.io/raft/v3@v3.7.0`의 `Sum`·`GoModSum`은 `go.sum`과 일치한다. 이는 설계와 dependency checksum 확인만 완료한 것이며 production durability·snapshot/backup 복구·quorum ACK·N/N-1 compatibility나 전체 runtime 수용 완료를 뜻하지 않는다. 고정 voter transport v1의 peer RPC와 node identity 결정은 [ADR-0008](decisions/ADR-0008-raft-peer-transport.md)에 한정된다. [ADR-0002](decisions/ADR-0002-cluster-consensus-library.md)는 계속 `Proposed`다.
- [x] [Cluster membership](specs/cluster-session-and-delivery.md#1-cluster-membership)과 [ADR-0002 §7 항목 6](decisions/ADR-0002-cluster-consensus-library.md#7-검증): join·leave, member 인증·호환성 검사와 membership 변경 중 crash/restart 수렴을 구현·시험했다. 동기화, route projection rebuild 및 quorum 확인 전 MQTT readiness 거절을 검증했다. [Cluster §11.5](specs/cluster-session-and-delivery.md#115-dynamic-membershiprestart-convergencemqtt-readiness-mapping)에 수용 증거를 기록했으며 `go run ./tools/verify local`이 PASS였다(전체 production line coverage 91.59%). 다른 cluster production 수용 기준은 아래 열린 항목으로 유지한다.

## 장애·복구 수용 증거

- [x] [Failure recovery §7](specs/cluster-session-and-delivery.md#7-failure-recovery)과 [§11.6](specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping): Docker chaos run `storage-fault-20260925d`에서 실제 tmpfs ENOSPC, FUSE slow-I/O, replication health/readiness, commit 진행·중단, bounded peer queue, follower recovery와 MQTT admission을 검증했다. 실행 증거와 관측값은 [Cluster §11.6](specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)에 기록했다. `go run ./tools/verify local`도 PASS했다. 이 범위는 durable MQTT ACK, snapshot/backup/restore 또는 power-loss 보장을 완료하지 않는다.
- [ ] [Snapshot·backup·restore §10](specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore): 실제 snapshot crash, backup/restore 및 재동기화의 원자성, checksum, commit 경계와 readiness를 시험한다. 기존 NMQSTATE1 로컬 replay만으로 production 복구를 판정하지 않는다.
- [ ] [ADR-0003 §9](decisions/ADR-0003-durable-ack-barrier.md#9-검증): WAL write·`fsync`·commit 전후 crash, follower disk full·slow I/O·partition에서 잘못된 성공 ACK가 없는지와 성공 ACK 직후 각 replica kill에서 message gap 0을 검증한다.

## MQTT·session 결합 및 release 수용

- [ ] [Cluster §11.2](specs/cluster-session-and-delivery.md#112-phase-2-replicated-session-ownership-core-mapping)와 [ADR-0004 §8](decisions/ADR-0004-session-ownership-and-fencing.md#8-검증): in-process ownership core를 실제 MQTT CONNECT·session takeover·connection teardown·drain 경로에 연결하고, persistent subscription·inflight의 production encoding/replay와 route projection rebuild를 검증한다.
- [ ] [Cluster §4~5](specs/cluster-session-and-delivery.md#4-replicated-state)와 [ADR-0003](decisions/ADR-0003-durable-ack-barrier.md): persistent subscription, QoS inflight, offline·retained message 등 필요한 상태를 복제하고 QoS 1/2 성공 PUBACK/PUBREC을 quorum stable write·commit 뒤로 연결한다. 재전송에서 중복 durable command가 생기지 않는지 검증한다.
- [ ] [Cluster §11](specs/cluster-session-and-delivery.md#11-수용-시험): 서로 다른 node에 연결된 publisher/subscriber의 QoS·subscription 유형, 동시 CONNECT·stale epoch, quorum loss와 durable ACK 시나리오를 실제 cluster 경로에서 시험한다. 기존 deterministic spike를 E2E 완료로 간주하지 않는다.
- [ ] [Cluster §8 및 §11](specs/cluster-session-and-delivery.md#8-mixed-version-compatibility): N-1↔N rolling upgrade·rollback을 양방향으로 시험하고 wire·persisted-state·snapshot 호환성 증거를 확보한다.

각 항목을 마치면 해당 [수용 시험 mapping](specs/cluster-session-and-delivery.md#11-수용-시험)에 시험 위치·실행 결과를 연결하고 영향받은 결정·구조 문서의 범위와 상태를 갱신한다. 실행하지 않은 시험이나 미승인 `Proposed` 결정을 완료로 표시하지 않는다.
