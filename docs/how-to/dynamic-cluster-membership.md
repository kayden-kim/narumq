# 동적 Raft 클러스터를 시작하고 멤버를 추가하는 방법

이 절차는 초기 Raft cluster의 voter 세 개(voter는 합의 투표에 참여하는 node)를 시작하고 새 node를 quorum(현재 voter 과반수의 동의)으로 commit하는 `STAGED` identity로 등록하는 방법을 정의한다. 새 node는 먼저 learner(로그를 복제하지만 투표하지 않는 member)로 기록을 따라잡은 뒤 voter로 승격된다. Membership crash/restart, retained QoS 0 replay, v3 backup/restore·snapshot-finalize와 제한된 QoS 1/2 durable delivery process acceptance는 통과했다. Production power-loss, N/N-1과 전체 lifecycle 동등성은 미검증이므로 이 초안은 production 실행을 승인하지 않는다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | cluster를 구성하고 변경하는 운영자 |
| 검증 상태 | Draft |
| 검증 환경·버전 | 로컬 다중 프로세스 Go acceptance: membership·retained/offline·durable QoS 1/2 process tests 및 focused package suite |
| 최종 검증일 | 2026-09-27 |
| 관련 기준 문서 | [동적 멤버십 계약](../contracts/raft-membership-v2.md), [Cluster backup v3](../contracts/cluster-backup-v3.md), [Cluster backup/restore 절차](cluster-backup-restore.md), [cluster session 명세](../specs/cluster-session-and-delivery.md), [ADR-0010](../decisions/ADR-0010-dynamic-cluster-membership.md), [ADR-0012](../decisions/ADR-0012-retained-replication-and-cluster-backup-v2.md), [ADR-0013](../decisions/ADR-0013-offline-queue-replication-and-cluster-backup-v3.md), [ADR-0015](../decisions/ADR-0015-cluster-durable-qos-delivery.md), [cluster architecture](../architectures/cluster-architecture.md) |

## 1. 대상과 영향

- 대상: 동일한 `clusterId`와 bootstrap voter set을 사용하는 세 node 및 새 joiner
- 예상 사용자 영향: bootstrap과 멤버십 변경 중 quorum, 인증서, 네트워크 설정이 맞지 않으면 신규 MQTT 연결이 거절될 수 있다.
- 운영 경계: 제한된 process acceptance에서 durable QoS 1/2 detached delivery, ACK commit boundary, takeover와 backup/restore를 확인했다. Production power-loss, N/N-1, TEN-08 tenant quota, QoS 0 general live forwarding 및 전체 drain 동작까지 승인한 것은 아니다. Procedure는 계속 Draft이므로 production cluster에는 적용하지 않는다.

## 2. 전제 조건

- [ ] 모든 node가 cluster protocol `2.0`, state schema `1.0`을 지원해야 한다. Retained command는 fresh read barrier 뒤 모든 non-retired committed member의 정확한 release/protocol/schema가 일치할 때만 허용되며, mixed-release retained operation과 rolling downgrade는 지원하지 않는다.
- [ ] 세 bootstrap node의 `clusterId`, sorted `bootstrapMembers`, member ID, endpoint, DNS SAN(인증서에 기록된 허용 host 이름), certificate SHA-256 fingerprint가 서로 일치한다.
- [ ] 각 node의 TLS 1.3 peer certificate가 공통 CA(인증서 발급 기관)에서 검증되고, local DNS SAN과 fingerprint가 구성값에 일치한다.
- [ ] Joiner는 운영자가 승인한 미사용 positive `nodeId`, 자체 인증서와 새 데이터 디렉터리를 갖는다. retired ID를 재사용하지 않는다.
- [ ] Peer listener와 DNS 주소가 node 간 연결 가능하다. Joiner의 `seedAddresses`는 lowercase DNS `host:port`여야 하며 IP literal은 허용되지 않는다.
- [ ] 데이터 디렉터리는 node별로 분리된 영속 volume이다. 기존 디렉터리를 새 node에 복사하거나 `cluster.meta`, WAL(write-ahead log, Raft 변경 기록), snapshot을 수동 삭제하지 않는다.
- [ ] Management HTTP listener는 신뢰된 관리 네트워크 안에서만 접근 가능하다. 인증 token은 명령 history, 로그, ticket에 붙여 넣지 않는다. 원격으로 접근해야 하면 별도의 승인된 TLS 보호 경로를 사용한다.
- [ ] 아직 Draft 상태이므로 production cluster에는 적용하지 않는다.

## 3. 절차

실행 위치는 저장소 root이며 예시는 Bash 호환 shell 기준이다. 설치된 binary를 쓰면 `go run ./cmd/narumq`를 해당 binary 경로로 바꾼다. 전체 strict config schema와 예시는 [membership v2 contract §1.4](../contracts/raft-membership-v2.md#14-combined-cluster-configuration)를 따른다.

1. **Bootstrap 구성 파일을 세 node에 각각 만든다.** `role`은 `bootstrap`, `bootstrapMembers`는 정확히 같은 세 identity를 포함하고 `seedAddresses`는 빈 배열이어야 한다. 각 파일의 `nodeId`, `directory`, `listenAddress`, `advertisedAddress`, 인증서 경로, management 주소, MQTT 보안 파일은 해당 node 값으로 설정한다. 초기 bootstrap 디렉터리는 비어 있어야 한다.

   검사 기준: `clusterId` 및 bootstrap identity set은 세 config에서 동일하고, 각 config의 local certificate SAN/fingerprint는 local member entry와 같다.

2. **세 node를 각각 별도 터미널에서 시작한다.**

   ```sh
   go run ./cmd/narumq -cluster-config config/node1.json
   go run ./cmd/narumq -cluster-config config/node2.json
   go run ./cmd/narumq -cluster-config config/node3.json
   ```

   `-cluster-config`는 다른 CLI flag나 positional argument와 함께 쓸 수 없다. 각 process는 자신이 소유한 영속 디렉터리만 사용한다. 서로 다른 bootstrap identity set으로 node를 시작하지 않는다.

3. **모든 node의 readiness를 확인한다.** Management endpoint에서 `/health/ready`를 조회한다. Liveness 응답만으로 준비 완료를 판단하지 않는다. READY는 committed voter role, replay/catch-up, route projection 재구축, healthy persistence 및 fresh quorum `ReadIndex` barrier(현재 Raft quorum에서 읽기 일관성을 확인하는 경계)가 모두 성공한 상태여야 한다. 어느 node라도 READY가 아니거나 quorum을 확인할 수 없으면 join을 진행하지 않는다.

4. **관리 API로 joiner identity를 한 번 stage한다.** 멤버십 contract §1.3의 request body에 joiner의 member ID, peer address, DNS SAN, leaf certificate SHA-256, release/protocol/schema version을 넣는다. `POST /api/v1/cluster/members`에는 같은 request 재시도를 구분하는 `Idempotency-Key`와 `ClusterAdmin` bearer token을 보낸다. `202 Accepted`는 `STAGED` 기록이 commit되었으며 operation이 계속 진행 중이라는 뜻이다. Response의 `operationId`를 보존한다.

   응답을 받지 못했으면 **같은 key와 동일 body로** 재요청한다. 다른 key로 중복 stage하지 않는다. Quorum 오류가 발생하면 joiner를 시작하지 말고 기존 voter 수와 node readiness를 확인한다.

5. **Stage commit 이후 joiner를 시작한다.** Joiner config는 `role="joiner"`, `bootstrapMembers: []`, DNS 형식 `seedAddresses`, 이미 승인된 `nodeId`, 새 empty directory를 사용한다. Joiner는 독립 Raft cluster로 bootstrap하지 않는다.

   ```sh
   go run ./cmd/narumq -cluster-config config/node4.json
   ```
   Joiner는 committed `STAGED` 확인만으로 Raft stream을 열지 않는다. Seed에 같은 admission 요청을 반복해 조회하고, Raft configuration에 반영된 `LEARNER` identity를 확인한 뒤에만 기존 voter와 peer stream을 맺는다. 이 자동 재조회 중 bootstrap quorum이 사라지거나 process가 종료되면 같은 volume으로 재시작하고 operation 상태를 다시 확인한다.


6. **동일 operation을 관찰해 종료 phase를 확인한다.** 저장한 ID로 `GET /api/v1/cluster/membership-operations/{operationId}`를 반복 조회한다. `STAGED`, `LEARNER`, `CATCHING_UP`, `PROMOTING`은 중간 상태다. `SUCCEEDED` 전까지 join 완료로 취급하지 않는다. 응답의 `committedIndex`는 해당 phase가 commit된 index이며 제안이 queue에 들어간 index가 아니다.


7. **Restart는 같은 identity와 data directory로 수행한다.** Process를 종료하고 다시 시작할 때 기존 node의 같은 config, 인증서와 volume을 유지한다. Joiner 역할 node는 이미 voter로 commit된 뒤에도 startup 시 seed admission으로 현재 peer identity를 다시 얻으므로, `seedAddresses`에 든 seed 중 하나 이상이 접속 가능해야 한다. Recovery가 committed WAL replay로 typed snapshot보다 앞선 상태를 만들면 runtime은 peer transport를 열기 전에 replay 완료 상태의 새 typed snapshot을 저장한다. 바뀐 인증서나 손상된 WAL을 복구한다는 이유로 metadata를 삭제하거나 디렉터리를 비우지 않는다. 오류 시 진행을 멈추고 증거를 보존한다.

### 계획된 member 교체

계획된 제거에서 leader는 대상 member가 소유한 모든 session을 남은 voter 중 member ID가 가장 작은 node로 이전한다. 소유권 이전과 epoch 증가는 commit된 뒤 대상 voter를 제거한다. 적용 과정에서 persistent subscription의 소유 node와 epoch를 갱신하고 기존 연결을 takeover로 닫는다. 이전이 막히면 제거 operation은 `REMOVING` 상태에 머문다. Successor voter가 없거나 ownership epoch가 고갈됐거나 orphan subscription이 감지되면 제거를 차단한다. QoS 1/2 delivery state는 `(tenantId, clientId)` key로 quorum에 이미 복제되어 있으므로 별도 checkpoint handoff가 필요하지 않고, 새 owner는 다음 CONNECT에서 ATTACH로 이를 복구한다. `LEAVING` member는 새 session admission을 거절한다.

중단된 제거를 관찰하거나 재시도할 때는 같은 operation ID와 idempotency key를 사용한다. 막힌 소유권 이전을 우회하려고 voter를 강제로 제거하지 않는다.

## 4. 검증

다중 프로세스 TLS membership과 retained/v2 backup process acceptance에서 관찰한 범위만 기록한다. 전체 cluster의 다른 수용 기준과 production 사용 여부는 Draft로 유지한다. Membership evidence는 [Cluster §11.5](../specs/cluster-session-and-delivery.md#115-dynamic-membershiprestart-convergencemqtt-readiness-mapping), retained evidence는 [§11.8](../specs/cluster-session-and-delivery.md#118-retained-replication과-backup-v2-수용-매핑)에 연결한다.

| 관찰 대상 | 성공 조건 | 확인 명령·방법 | 실제 결과 | 실행 증거 위치 |
| --- | --- | --- | --- | --- |
| Bootstrap readiness | 세 node 모두 fresh quorum barrier 이후 READY | 각 관리 endpoint의 `GET /health/ready` | 확인: 세 process 모두 READY이고 cluster summary에서 quorum과 공통 leader를 확인했다. | `tests/integration/cluster_membership_process_test.go::TestDynamicClusterMembershipProcessAcceptance` |
| Join operation and idempotency | 동일 idempotency key 재시도가 같은 operation ID로 수렴하고 (stage 직후 및 promotion 완료 후), joiner가 learner catch-up 후 voter가 된다. | Stage API 및 `GET /api/v1/cluster/membership-operations/{operationId}` | 확인: stage 직후(STAGED) 및 promotion 완료 후(SUCCEEDED) 동일 key 재요청이 같은 operation ID를 반환했고 중복 config change/promotion 없이 joiner가 voter/READY 및 4-member summary에 도달했다. | `tests/integration/cluster_membership_process_test.go::TestDynamicClusterMembershipProcessAcceptance` |
| Restart convergence | 같은 volume 재시작 후 committed membership과 operation phase가 수렴한다. | `go test -race ./tests/integration -run '^TestDynamicClusterMembershipTransitionCrashRecovery$' -count=2`; current `go run ./tools/verify local` | PASS: 전용 race 반복 2회는 retained cutover 전 증거다. Current local gate도 전체 integration package를 race mode로 실행해 통과했다. 이전 cycle 3 `CATCHING_UP` timeout 이후 빈 Raft entry를 typed snapshot index에 반영했다. | `tests/integration/cluster_membership_failpoint_process_test.go::TestDynamicClusterMembershipTransitionCrashRecovery`; current local gate |
| Three-voter replacement and retired ID | Member 3 planned removal 후 active voter set이 `{1,2,4}`(LEADER/FOLLOWER)로 수렴하고, member 3은 RETIRED가 되며, retired ID 3 재사용이 거절되고, 1 voter loss 후 quorum을 유지한다. | Management API 및 process acceptance | 확인: member 3 planned removal 후 active node가 정확히 `{1,2,4}`(voter role: LEADER/FOLLOWER)이고 member 3이 RETIRED로 수렴함을 확인했다. retired ID 3 재사용은 409 `member_id_reserved`로 거절됐고, 1 voter crash 후 2 survivor가 quorum, 200 readiness 및 MQTT CONNECT 수락을 유지했다. | `tests/integration/cluster_membership_process_test.go::TestDynamicClusterMembershipProcessAcceptance` |
| Compatibility and identity rejection | Foreign CA, SAN/fingerprint/cluster ID 불일치, malformed hello, unsupported profile/schema 및 reserved ID는 log commit 및 Step에 도달하지 않는다. | Transport unit tests 및 Management API process acceptance | 확인: wire 레벨에서 foreign CA, SAN 불일치, cluster ID 불일치, malformed frame이 TLS/transport에서 거절되어 Step에 도달하지 않음을 확인했다. Management API에서 unsupported release/protocol/schema는 422 `compatibility_unsupported`로, reserved ID는 409로 거절되어 membership log에 commit되지 않았다. | `internal/adapters/tcp/peer_transport_v2_test.go`, `tests/integration/cluster_membership_process_test.go` |
| Quorum loss rejection | Quorum loss 중 readiness가 내려가고 removal은 commit되지 않는다. | Management readiness 및 removal API | 확인: 2 voter crash로 quorum loss 시 `/health/ready`가 503으로 떨어지고, removal 요청이 503 `quorum_unavailable`로 거절되어 commit되지 않으며, MQTT admission이 거절됨을 확인했다. | `tests/integration/cluster_membership_process_test.go::TestDynamicClusterMembershipProcessAcceptance` |
| MQTT admission and readiness barrier | 준비 전 CONNECT와 commit 전 subscription success ACK가 거절되고, readiness 회복 뒤 정책에 맞게 처리된다. | `TestClusterReadinessGate` 및 failpoint process acceptance | PASS: readiness unit case가 applied-index gap과 projection failure를 확인하고 process test가 route projection rebuild 실패 시 503/CONNECT 거절, 회복 뒤 READY/CONNECT 수락을 확인했다. | `cmd/narumq/cluster_runtime_test.go::TestClusterReadinessGate`; `tests/integration/cluster_membership_failpoint_process_test.go` |
| Peer identity refresh and learner TCP dial | Phase refresh keeps queued/in-progress outbound frames; old staged inbound frames cannot be leased; reconciliation waits for the current delivery to complete after `Step`. | `go test -race ./internal/adapters/tcp`; process diagnostic | PASS: adapter tests cover outbound preservation and inbound lease/reconciliation barrier. Earlier process diagnostics observed leader node 1 queue Raft messages for learner 4 while disconnected (queue reached 18); the TCP peer session later established and snapshot catch-up advanced. The code now preserves outbound frames across identity-preserving refresh and leases inbound delivery through `Step`. The observed symptom was a delayed dial, not evidence of a malformed address. | `internal/adapters/tcp/peer_transport_v2_test.go`; `tests/integration/cluster_membership_process_test.go` |
| MQTT session-resume CONNECT | Persistent session resumes with accepted CONNACK and delivery state. | `go test ./tests/integration -count=1` | PASS: integration package 전체가 통과했다. 이전 header EOF는 `connection.cleanup()`의 `unbindSession` 복구 뒤 재현되지 않았다. | `internal/adapters/tcp/server.go`; `tests/integration` |
| Retained replay and v2 archive recovery | QoS 0 retained replace/delete가 다른 node, restored voter, restarted voter와 learner catch-up 뒤 동일하게 replay되고 archive retained summary가 보존된다. | `TestClusterBackupRestoreProcessCrashAndResync`; §11.7의 process regression command | PASS: cross-node/restore/restart/joiner replay `{retained/a:alpha, retained/c:gamma}`, format major 2와 retained count 2를 확인했다. | `tests/integration/cluster_backup_restore_process_test.go::TestClusterBackupRestoreProcessCrashAndResync`; Cluster §11.8 |
| Local repository verification | Current retained/backup v2 changes pass the full local gate. | `go run ./tools/verify local` | PASS: 표준·race package suite, 제한 fuzz, traceability, architecture와 coverage gate가 통과했다. Mutation: delivery 25/25 kill, session/ownership 45/46 kill·1 timeout, lived/uncovered 0. Verifier returned `local: PASS`. | Cluster §11.8 |

실행 증거:

```text
go build ./...
go test ./internal/application/membership/... ./internal/adapters/raft/records/... ./internal/adapters/memory/retained/... ./internal/application/publish/... ./internal/application/subscribe/... ./internal/application/management/... ./internal/adapters/http/management/... ./cmd/narumq/... ./tests/contracts/... -count=1
go test ./tests/integration -run '^(TestClusterBackupRestoreProcessCrashAndResync|TestDynamicClusterMembershipProcessAcceptance|TestDynamicClusterMembershipTransitionCrashRecovery|TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum|TestSnapshotFinalizationCrashRecoveryAcrossProcessRestart)$' -count=1 -v
```

`go build ./...`, membership·retained process acceptance, focused package suite와 full `go run ./tools/verify local` gate가 PASS했다. Local gate에는 표준·race package suite, 제한 fuzz, traceability, architecture, coverage와 mutation gate가 포함되며, 별도 production power-loss나 offline/durable-delivery 보장은 포함하지 않는다. 전체 acceptance와 남은 production 경계는 [Cluster §11.8](../specs/cluster-session-and-delivery.md#118-retained-replication과-backup-v2-수용-매핑)에 연결한다.

## 5. 중단 조건

- TLS SAN/fingerprint, cluster ID, member ID, release/protocol/schema 불일치
- bootstrap member set 불일치, 비어 있지 않은 신규 데이터 디렉터리, 중복·retired ID
- Management API에서 `not_leader`, `quorum_unavailable`, identity/idempotency conflict 또는 미지원 compatibility 응답
- 기존 node 하나라도 readiness 실패, committed operation phase가 예상 상태에서 멈춤, peer 연결 또는 persistence 오류
- Joiner가 stage commit 전에 Raft peer stream을 시작하거나 별도 bootstrap을 시도함

중단 조건이 하나라도 발생하면 새 membership operation을 만들거나 기존 voter를 수동 제거하지 않는다. Node log, operation ID, committed index, health 응답과 관련 시각을 보존하되 token, private key, payload secret은 제외한다.

## 6. 이전 상태로 되돌리는 롤백 또는 복구

1. `STAGED` 이후 operation이 진행되지 않으면 동일 key와 동일 request만 재시도하고 원인을 확인한다. 새로운 stage key, 수동 tombstone 삭제, ID 재사용, 데이터 디렉터리 초기화는 복구 방법이 아니다.
2. Joiner가 learner가 된 뒤 응답을 잃었으면 같은 persistent directory로 재시작하고 동일 operation을 조회한다. Heartbeat timeout이나 process 종료만으로 learner/voter를 자동 삭제하지 않는다.
3. Planned removal은 successor ownership transfer가 commit된 뒤에만 허용된다. Durable delivery aggregate는 별도 checkpoint command 없이 새 owner가 다음 CONNECT에서 ATTACH로 재개한다. Quorum이나 successor transfer가 확인되지 않으면 `DELETE /api/v1/cluster/members/{memberId}`를 호출하지 말고 기존 voter를 유지한다. Active session handoff와 quorum loss 복구 acceptance는 `TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum`로 검증했다.
4. Restore 절차인 [Cluster backup/restore](cluster-backup-restore.md)는 v3 archive에 retained, offline 및 durable delivery state를 포함한다. Production volume power-loss는 검증하지 않으며, 이전 v1/v2 archive 또는 binary로 downgrade하는 경로는 지원하지 않는다.

## 7. 해결되지 않을 때 전달할 대상

- 전달 대상: cluster 운영 당직자와 NaruMQ 개발 담당자
- 보존할 증거: node ID, operation ID, committed index, phase, UTC 발생 시각, 비밀을 제거한 로그, readiness 응답, 배포 version
- 비밀 값 제외 규칙: bearer token, 개인키, 인증서 private data 및 MQTT payload를 로그·보고서에 포함하지 않는다.
