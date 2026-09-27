# ADR-0012: Retained state는 Raft로 복제하고 cluster backup v2에 포함한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (retained scope; archive format v2는 ADR-0013이 대체; 전체 CLU-14 완료 아님) |
| 결정일 | 2026-09-26 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD CLU-14](../requirements/PRD.md#92-클러스터와-세션), tenant-scoped retained state |
| 관련 스펙·데이터 계약 | [Cluster §4·§5·§10·§11.8](../specs/cluster-session-and-delivery.md), [Cluster backup v2](../contracts/cluster-backup-v2.md), [replicated-state-v1.proto](../contracts/replicated-state-v1.proto) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | [ADR-0011](ADR-0011-cluster-backup-restore-v1.md)의 archive format과 retained 포함 여부만 대체한다. v1의 boundary capture와 offline restore 결정은 이행 경계로 남긴다. |
| 이 결정을 대체한 새 결정 기록(ADR) | [ADR-0013](ADR-0013-offline-queue-replication-and-cluster-backup-v3.md) (archive format v2만 대체) |

## 1. 결정

1. Retained state의 권위 있는 사본은 Raft committed state machine에 둔다. Key는 `(tenantId, exact Topic Name)`이며 payload, publisher client ID, QoS와 expiry deadline을 저장한다. 동일한 topic이어도 tenant가 다르면 별도 값이다.
2. `Retain=true` PUBLISH는 route matching과 live fan-out보다 먼저 retained replace/delete command를 commit한다. ADR-0012 결정 당시 Cluster MQTT ingress는 QoS 0 PUBLISH만 지원했다. QoS 1/2 durable path는 후속 ADR-0015에서 별도로 정의됐다. Commit이 실패하면 live fan-out을 시작하지 않는다.
3. Retained state의 적용 한도는 모든 tenant를 합친 전체 기준이다: value 1,024개, 계산된 총 16 MiB, payload당 1,000,000 bytes. 크기는 tenant ID, topic, payload와 publisher routing ID의 byte 길이 합이며 tenant별 저장 quota는 제공하지 않는다. Capacity 판정과 proposal 시각 기준 만료 제거는 Raft apply에서 결정적으로 수행한다.
4. Node-local retained repository는 replay용 projection으로만 사용한다. Startup과 snapshot restore 뒤 committed state에서 재구성하고, incremental update도 committed state를 기준으로 반영한다. Projection을 재구성하지 못한 node는 readiness를 광고하지 않는다.
5. Replicated-state schema 번호는 `1.0`으로 유지한다. Draft wire contract에 retained command/state field를 추가하되 unknown field를 거절하는 기존 규칙을 유지한다. Retained command 전에 fresh quorum read barrier를 얻고 모든 non-retired committed member의 release, cluster protocol과 state schema가 local member와 정확히 일치하는지 검사한다. 불일치 상태에서는 retained command를 제안하지 않는다. Mixed-release retained operation과 rolling rollback은 지원하지 않는다.
6. Archive는 새 `cluster-backup-v2` format `2.0`(`NMQBKUP2`)를 사용한다. Typed retained state와 retained count/digest를 boundary summary에 포함하고 `not_replicated_kinds`는 `["offline"]`으로 둔다. v1 archive를 읽지 않고 명시적 unsupported-format 오류를 반환한다. Restore는 기존과 같이 같은 cluster identity의 빈 디렉터리, 정확히 세 bootstrap voter와 learner 없는 archive로 제한한다.
7. 이 결정은 offline queue replication/archive, QoS 1/2 durable publish ACK, 전체 CLU-14, release 간 archive compatibility, retention/RPO/RTO 또는 production EBS/power-loss 보장을 완료 처리하지 않는다.

## 2. 배경

ADR-0011의 archive v1은 commit-boundary snapshot과 offline voter restore를 제공했지만 `retained`와 `offline` message를 제외했다. 기존 retained 저장소는 process-local이어서 cluster node 사이에 값이 공유되지 않았고, node restart 뒤에도 복구되지 않았다. 이를 단순히 archive에만 추가하면 online publish와 다른 node의 replay가 일관된 값을 보지 못한다.

Replicated-state schema `1.0`은 아직 Draft 상태이며 unknown field를 거절한다. 따라서 한 node가 retained field를 쓴 뒤 이전 release node가 그 상태를 읽는 mixed-version path는 안전하지 않다. Schema 번호를 임의로 올리지 않고 retained feature 자체를 정확한 member-profile 일치 조건 뒤에 둔다. 이 선택은 현 작업에서 요청한 schema `1.0` 경계를 보존하지만, rolling upgrade를 지원한다는 뜻은 아니다.

Archive v2는 새 retained kind를 checksum/count summary에 포함한다. Archive v1 bytes와 새 schema를 같은 이름의 형식으로 조용히 해석하지 않도록 기존 v1 consumer/proto source는 clean cutover로 제거하고 v1 설명 문서는 historical superseded record로 유지한다.

## 3. 결정 요인

1. Cluster에 접속한 publisher의 retained 값은 commit 뒤 다른 ready node에서도 replay되어야 한다.
2. Tenant별 topic 격리를 유지하고 capacity/expiry 결과를 모든 replica에서 결정적으로 만들어야 한다.
3. Retained state가 snapshot, restart, backup, restore와 learner catch-up을 지나도 유지되어야 한다.
4. Unknown-field reject 규칙을 완화하거나 호환되지 않는 mixed-release proposal을 허용하지 않아야 한다.
5. 검증되지 않은 offline queue, durable ACK, power-loss와 운영 RPO/RTO를 완료로 표시하지 않아야 한다.

## 4. 선택 근거

Publish use case는 retained change를 먼저 committer에 전달하고 성공 뒤에만 route를 match한다. Cluster runtime은 tenant-scoped command를 Raft에 commit한다. Publisher node는 committed command를 local apply하고 replay projection을 갱신한 뒤 publish 경로를 계속 진행하며, 다른 node는 각자의 committed apply/catch-up 때 local projection을 갱신한다. Publish 완료는 모든 peer projection의 동기 완료를 기다리지 않는다. Empty retained payload는 tenant/topic key를 삭제하며 publisher live publication으로는 계속 전달된다. Process acceptance는 실제 MQTT QoS 0 publish, cross-node replay, replace/delete, backup summary, 세 voter restore, restored-node restart와 joiner catch-up 뒤의 retained replay를 확인한다.

Retained state의 deterministic limits와 proposal time은 leader와 follower가 같은 committed command를 같은 결과로 적용하게 한다. Local repository는 빠른 lookup을 제공하지만 복제 원본이 아니므로 startup/restore resync가 손실·중단된 projection을 복구할 수 있다. Exact member profile gate는 unknown field를 거절하는 schema에서 이전 release가 새 command/state를 조용히 해석하는 일을 막는다. 비용은 retained operation의 quorum/read-barrier 지연과 mixed-release 배포 중 retained write 거절이다.

Backup v2는 format major를 분리해 v1 reader가 v2 bytes를 잘못 해석하지 않게 한다. `retained` summary는 archive restore의 state count/digest 검증에 포함되지만 `offline` queue는 계속 제외한다. 따라서 archive v2는 전체 CLU-14의 completion marker가 아니다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Retained 값을 각 node의 memory에만 저장 | Raft command가 없어 publish 경로가 짧다. | Node별 replay가 달라지고 restart/restore에서 값이 사라진다. | Cross-node replay와 backup/restore 요구를 만족하지 않는다. |
| Backup v1을 유지하고 retained를 제외 | 기존 bytes와 reader를 유지한다. | Retained state는 archive와 restore 후 유실된다. | 요청된 retained backup scope를 만족하지 않는다. |
| Schema 번호를 `1.1`로 올리고 rolling migration을 함께 설계 | Wire change를 명시적 schema revision으로 표현할 수 있다. | Migration, compatibility matrix와 mixed-version replay 범위를 추가로 확정해야 한다. | 현 승인 범위는 schema `1.0` 유지와 정확한 profile gate다. |
| 이전 release node도 retained command를 수용 | Rolling update availability가 높다. | 이전 decoder가 unknown field를 거절하고 상태를 안전하게 해석할 수 없다. | Mixed-release operation은 fail-closed로 유지한다. |
| Offline queue도 같은 작업에서 복제·archive | CLU-14 state set 범위가 넓어진다. | QoS/session delivery와 durable ACK 경계를 함께 재설계·검증해야 한다. | 승인된 작업 범위를 넘어가므로 열린 CLU-14 항목으로 남긴다. |

## 6. 결과

### 긍정적 결과

- Tenant-scoped retained value가 quorum-committed state이며 다른 ready node, restored node와 catch-up joiner에서 같은 replay 결과를 제공한다.
- Snapshot과 archive summary가 retained state를 포함하므로 restore가 retained object count/digest를 검증한다.
- Expiry, count와 byte capacity가 deterministic apply 규칙을 사용한다. Projection은 복제 상태에서 다시 생성된다.
- v1 archive는 명시적으로 거절되어 old format을 새 contract처럼 해석하지 않는다.

### 부정적 결과와 비용

- Retained publish는 fresh read barrier와 Raft commit 비용을 추가하며 quorum/compatibility 오류가 나면 fan-out하지 않는다.
- Retained capacity는 node 간 동일한 1,024개·16 MiB 상한을 적용하며 MQTT 5 초과 요청은 `0x97`로 연결을 닫는다.
- Schema `1.0`의 retained field 추가 때문에 exact release profile 일치 전에는 retained command를 허용하지 않는다. Mixed-release rolling upgrade와 downgrade 경로는 없다.
- 이 결정에서 offline queue replication/archive는 scope 밖이었고 durable QoS 1/2 ACK도 당시 미검증이었다. Offline replication/archive는 후속 ADR-0013에서, QoS 1/2 durable delivery의 제한된 process acceptance는 ADR-0015 §7 및 Cluster §11.11에서 다뤘다. 전체 CLU-14, RPO/RTO와 production power-loss는 여전히 별도 미완료다.
- Format v1 archive는 이 release에서 복원할 수 없다. 이전 release에서 v2로 넘어가기 전 새 backup/restore를 완료해야 한다.

### 후속 작업

- Offline queue replication/archive는 v2 범위에서 제외했으며 후속 ADR-0013과 Cluster §11.9에서 구현·검증했다. 이 결과는 production power-loss나 전체 CLU-14 완료를 의미하지 않는다.
- Production-like Linux filesystem/EBS power-loss, volume loss와 backup retention/RPO/RTO를 검증한다.
- Release 간 archive compatibility matrix와 승인된 migration/rollback 정책을 결정한다.
- [x] Durable QoS 1/2 ACK와 retained QoS 1/2 network ingress는 후속 [ADR-0015](ADR-0015-cluster-durable-qos-delivery.md)에서 범위화하고 [Cluster §11.11](../specs/cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑)의 제한된 process acceptance로 검증했다. Production storage/WAL durability 및 N/N-1은 포함하지 않는다.

## 7. 검증

| 결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Tenant/topic key, expiry, limits, snapshot과 validation이 deterministic하다. | Membership state-machine 및 records codec tests | Tenant 경계, replace/delete, expiry, cap, canonical snapshot/codec round-trip 검증 | PASS: `go test ./internal/application/membership/... ./internal/adapters/raft/records/... -count=1` |
| Publish가 route보다 먼저 Raft commit하고 projection을 복구한다. | Publish/runtime focused tests | Commit failure에서 fan-out 없음; rebuild, incremental sync와 projection failure/readiness 확인 | PASS: focused publish·cmd tests; 전체 명령은 Cluster §11.8에 기록 |
| 실제 MQTT retained가 cross-node, archive, restore, restart와 joiner catch-up을 통과한다. | `go test ./tests/integration -run '^(TestClusterBackupRestoreProcessCrashAndResync|TestDynamicClusterMembershipProcessAcceptance|TestDynamicClusterMembershipTransitionCrashRecovery|TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum|TestSnapshotFinalizationCrashRecoveryAcrossProcessRestart)$' -count=1 -v` | Cross-node replay, replace/delete, v2 summary, offline restore, process restart와 joiner replay/convergence | PASS: `ok narumq/tests/integration 90.062s`; 상세는 [Cluster §11.8](../specs/cluster-session-and-delivery.md#118-retained-replication과-backup-v2-수용-매핑) |
| ADR-0012가 offline queue, production power-loss와 전체 CLU-14를 완료했는가? | Production 유사 filesystem/EBS 및 offline-state acceptance | v2에서 offline state는 의도적으로 제외했다. 후속 결정과 production recovery를 별도로 확인한다. | Offline QoS 0 replication/archive는 ADR-0013, QoS 1/2 durable delivery는 ADR-0015 §7 및 Cluster §11.11에서 후속 검증했다. Production power-loss, RPO/RTO와 전체 CLU-14는 미완료다. |
