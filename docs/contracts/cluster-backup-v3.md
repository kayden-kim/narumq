# Cluster backup v3 인터페이스 계약(Contract)

이 계약은 실행 중인 3-voter cluster에서 하나의 commit 경계를 담은 archive를 만들고, 검증된 archive로 동일 cluster의 빈 voter 저장 디렉터리를 오프라인 seed하는 형식을 정의한다. Retained state, detached persistent session의 QoS 0 offline queue, durable QoS 1/2 delivery session과 inflight state를 포함한다. [ADR-0021](../decisions/ADR-0021-backup-archive-release-window.md)은 N/N-1 restore 정책을 채택한다. Production storage acceptance, historical-source 호환성과 disaster-recovery 보존 정책은 이 계약만으로 완료되지 않는다.

| 항목 | 값 |
| --- | --- |
| 이름과 버전 | `cluster-backup-v3` 1.0.0 |
| 상태 | Draft |
| 소유자 | NaruMQ cluster |
| 만드는 쪽(Producer) | ClusterAdmin이 호출하는 `POST /api/v1/cluster/backups` |
| 사용하는 쪽(Consumer) | `narumq -cluster-config <file> -cluster-restore <archive>` |
| 방향 | Broker → binary archive → 오프라인 restore CLI |
| 스키마 기준 파일 | [cluster-backup-v3.proto](cluster-backup-v3.proto) |
| replicated-state schema | [replicated-state-v1.proto](replicated-state-v1.proto), schema `1.0` |
| 기준 동작 명세 | [Cluster §4.1·§4.2·§10](../specs/cluster-session-and-delivery.md), [§11.9·§11.11](../specs/cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑) |
| 관련 결정 | [ADR-0013](../decisions/ADR-0013-offline-queue-replication-and-cluster-backup-v3.md), [ADR-0015](../decisions/ADR-0015-cluster-durable-qos-delivery.md), [ADR-0021](../decisions/ADR-0021-backup-archive-release-window.md) |
| 관련 요구사항 | [PRD CLU-14](../requirements/PRD.md#92-클러스터와-세션) |
| 호환성 범위 | archive format `3.0`; 동일 `clusterId`, protocol/schema 및 현재 3개 bootstrap voter identity; producer·voter release는 restore build와 같은 major·minor 차이 1 이하 |
| 관련 시험 | Codec: `internal/adapters/raft/records`; retained/offline producer·consumer: Cluster §11.9; durable delivery/backup evidence: Cluster §11.11 |

## 1. 형식과 의미

Archive는 아래 byte 순서로 구성한다. 모든 protobuf frame은 deterministic encoding이어야 하며 unknown field, 비정규 encoding, 잘못된 frame 또는 commit 범위 밖 entry는 거절한다.

| 구성 | 형식 | 필수 여부·제약 | 의미 |
| --- | --- | --- | --- |
| Magic | 8 bytes `NMQBKUP3` | 필수 | Backup format major 3을 식별한다. |
| Manifest frame | big-endian `uint32` byte length, CRC32C, protobuf payload | 필수; payload 최대 64 KiB | format, cluster/member identity, release/protocol/schema, snapshot·commit index/term, voter·learner ID, entry 수와 commit 경계 state summary를 담는다. |
| Snapshot frame | big-endian `uint32` byte length, CRC32C, typed-state snapshot payload | 필수 | `snapshot_index`의 typed state와 Raft ConfState를 담는다. |
| Entry frames | 위와 같은 length·CRC32C frame의 `BackupEntry` protobuf | `entry_count`만큼 | Snapshot 뒤 commit 경계까지의 committed command/configuration 기록을 보존한다. No-op Raft entry는 application record로 인코딩하지 않는다. |
| Trailer | 32-byte SHA-256 | 필수 | Magic부터 마지막 frame까지, trailer 직전의 모든 byte를 해시한다. |

전체 archive는 1 GiB를 넘을 수 없고, committed entry 수는 1,000,000개를 넘을 수 없다. Consumer는 frame 길이와 CRC32C(Castagnoli), trailer digest, protobuf canonical encoding, format major/minor, snapshot index, entry count와 entry 범위를 검증한다. Unknown major/minor는 지원하지 않는다.

`boundary_state`는 `commit_index`에서 재구성한 전체 및 kind별 object count/SHA-256을 담는다. Required kind는 members, tenants, sessions, subscriptions, inflight, membership history, retained와 offline이다. `offline` summary의 count는 detached offline session record 수와 그 record에 든 queued message 수의 합이다. `inflight` summary의 count는 모든 `DeliverySessionState`에서 `len(messages)+len(inbound)`의 합이며 digest는 `delivery_sessions`만 가진 typed snapshot을 사용한다. Tenant digest는 ownership, subscription, delivery, retained, offline record의 tenant ID를 포함한다. Node drain record와 tenant quota profile은 별도 kind summary 없이 typed snapshot과 whole-state SHA-256에 포함된다. `not_replicated_kinds` field는 이 형식에 없다. |

Retained key는 `(tenantId, exact Topic Name)`이다. Offline key는 `(tenantId, clientId)`이며 각 detached persistent session의 message는 단조 증가 sequence로 FIFO 순서를 보존한다. Exact state transition, ownership fencing, at-most-once claim, expiry와 capacity는 [Cluster §4.1](../specs/cluster-session-and-delivery.md#41-offline-message-capture와-전달)을 따른다. Durable delivery session과 inbound/outbound QoS state는 [Cluster §4.2](../specs/cluster-session-and-delivery.md#42-qos-12-durable-delivery)와 [ADR-0015](../decisions/ADR-0015-cluster-durable-qos-delivery.md)를 따른다.

## 2. Producer와 consumer 동작

### Online backup

- `POST /api/v1/cluster/backups`는 body 없는 ClusterAdmin 요청이다. API는 fresh quorum/read barrier를 통과한 뒤 typed snapshot과 committed tail을 같은 commit boundary에 묶어 `application/octet-stream`으로 반환한다.
- `X-NaruMQ-Backup-Commit-Index`는 manifest의 `commit_index`다. `X-NaruMQ-Backup-SHA256`은 trailer digest로, 전체 응답에서 마지막 32 bytes를 제외한 byte의 SHA-256이다.
- Fresh quorum boundary를 만들 수 없으면 retryable `503 cluster_backup_unavailable`을 반환한다. HTTP 인증과 오류 envelope는 [Management API v1](management-api-v1.md)을 따른다.

### Retained, offline and durable delivery state

- Retained value, offline queue와 durable delivery state는 committed typed state이며 snapshot, backup summary와 restore에서 함께 보존한다. Restore된 state는 새 owner와 catch-up node가 사용할 수 있다.
- Cluster MQTT ingress는 QoS 0/1/2 PUBLISH를 지원한다. Detached persistent session의 matching QoS 0 delivery는 §4.1의 Raft offline state를 사용한다. Connected QoS 0은 §3.1의 live path를 사용하며, QoS 1/2 delivery와 inbound state는 §4.2의 durable delivery session에 기록한다.
- Disconnect 시 persistent session의 pending QoS 0 FIFO는 DETACH command로 capture한다. 새 ownership epoch의 CONNECT는 durable delivery state를 ATTACH로 복구하고 QoS 0 offline queue는 CLAIM으로 가져와 durable delivery 뒤에 전달한다.
- Offline limit는 session당 64 message, 전체 4,096 message·16 MiB, payload당 1,000,000 bytes, DETACH command당 topic+payload 768 KiB다. Tenant 저장 quota는 [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md)에 따라 retained, QoS 0 offline queue, durable delivery outbound·inbound state를 집계하며 새 state 초과를 거절한다.
- Replicated-state schema `1.0`은 unknown-field rejection을 유지한다. Replication proposal의 fresh read와 release-profile gate는 [Release compatibility v1](cluster-release-compatibility-v1.md)을 따른다.
- Node drain operation과 tenant quota profile은 replicated typed snapshot 및 whole-state SHA-256에 포함된다. Restore된 진행 중 drain operation은 저장된 phase와 original deadline에서 재개한다. Restored `COMPLETED` record는 node가 readiness를 다시 통과한 뒤 replicated `CLEAR`로 제거한다.

### QoS 1/2 durable delivery state

QoS 1/2 delivery session은 `ReplicatedStateSnapshot.delivery_sessions`에 저장된다. `inflight` summary는 outbound message와 inbound QoS 2 상태/tombstone 수를 함께 계산하며, 동일 commit boundary의 restore 뒤 ATTACH와 takeover에서 사용한다. 이 archive representation은 production WAL/fsync durability acceptance나 N-1 compatibility를 입증하지 않는다.


### Offline restore

- Restore는 실행 중인 cluster process가 아닌 **중지된 node**에서 수행한다. CLI는 config, format, identity, state와 checksum을 target에 쓰기 전에 검증한다. 성공하면 seed를 저장하고 종료하며 cluster listener를 시작하지 않는다.
- `-cluster-config`는 `role=bootstrap`이며 현재 voter 세 개를 정확히 포함해야 한다. Archive voter ID와 config bootstrap member ID는 같은 정렬된 집합이어야 하고 archive에 learner ID가 하나라도 있으면 restore를 거절한다.
- `clusterId`, cluster protocol과 state schema는 config와 같아야 한다. Producer와 모든 voter release는 restore build와 같은 major·minor 차이 1 이하이어야 한다. Window 밖 producer는 `backup producer release <producer> is outside the supported release window of <restore>`로 거절한다. Commit 경계 state에 restore build가 지원하지 않는 active feature가 있으면 `backup active feature "<feature>" is not supported by this release`로 거절한다. 두 검증은 target storage 변경 전에 수행한다. 각 bootstrap voter의 member ID, peer address, DNS SAN 및 certificate SHA-256 fingerprint도 committed member identity와 일치해야 한다. Archive source와 local `nodeId`는 voter 집합에 속해야 한다.
- 같은 archive를 각 voter의 별도 빈 data directory에 한 번씩 restore한다. 기존 data directory나 foreign file을 덮어쓰지 않는다.
- `restore-incomplete` marker가 남으면 일반 cluster startup은 실패한다. Marker나 restore 파일을 수동 삭제하지 않는다. 같은 archive와 올바른 config로 restore를 다시 실행하거나 운영 담당자에게 이관한다.
- Restore는 committed member profile을 다시 쓰지 않는다. Restore 완료 뒤 정상 `-cluster-config` 경로로 시작하며 snapshot/WAL replay와 fresh quorum 검증에 더해 build profile의 `MemberProfileChange` commit·재확인 전에는 READY가 되지 않는다.

## 3. 오류

| 오류 | 조건 | 재시도 가능 | 상태 변경 |
| --- | --- | --- | --- |
| `400 invalid_request` | Backup 요청 body 또는 유효하지 않은 요청 형식 | 아니오 | 없음 |
| `401` / `403` | ClusterAdmin 인증·권한 실패 | credential/권한 수정 후 | 없음 |
| `405 method_not_allowed` | 선언되지 않은 HTTP method | 아니오 | 없음 |
| `503 cluster_backup_unavailable` | Fresh quorum/read boundary 또는 backup capture 불가 | 가능 | 없음 |
| Restore CLI non-zero exit | 손상·지원하지 않는 format, boundary summary mismatch, identity/version mismatch, learner 포함 또는 durable write 실패 | 원인에 따라; marker가 있으면 같은 directory의 일반 startup 금지 | 검증 실패 전 target 불변; 쓰기 실패 뒤 marker가 startup을 차단 |
| Offline enqueue 거절 | Stale/missing session, exact-profile/quorum 오류 또는 offline capacity 초과 | 원인 해소 뒤 QoS 0 publisher가 다시 발행할 수 있음; delivery 보장은 없음 | Capacity conflict는 기존 non-expired state를 바꾸지 않는다. Apply에서 proposal 시각 기준 expired item은 제거할 수 있다. |

## 4. 호환성 규칙

- Archive major/minor `3.0`, cluster protocol `2.0`, replicated-state schema `1.0`만 지원한다.
- `NMQBKUP1`과 `NMQBKUP2` archive는 명시적으로 거절한다. 오류는 각각 `unsupported archive format 1; capture a new backup with this release`, `unsupported archive format 2; capture a new backup with this release`다. 각 archive는 이를 지원하는 이전 release에서만 복원한다.
- v2→v3 live migration과 converter는 제공하지 않는다. Cutover 전에 운영자는 persistent subscriber를 다시 연결해 기존 node-local queue를 drain해야 한다. Drain하지 않은 queue는 이전 process 종료와 함께 사라질 수 있다. 새 build의 replicated offline/durable delivery state는 빈 상태로 시작하며, v2 archive를 v3 reader로 변환하는 경로는 없다.
- Restore는 cross-cluster migration, voter 재구성, learner 복원을 지원하지 않는다. 현재 bootstrap voter ID와 member identity를 바꾸지 않는다.
- Retained, offline, delivery field 추가는 unknown field를 거절하는 schema `1.0`의 replicated-state wire format 변경이다. 같은 schema 번호만으로 release 호환을 추정하지 않는다. Archive·typed snapshot·committed tail·boundary summary를 기존 strict codec로 검증하며 알 수 없는 field·command·summary는 target storage 변경 전에 거절한다. Release window나 inactive feature가 decode 실패를 허용하지 않는다.
- N archive를 N-1에서 restore할 수 있다. 단 N-1이 모르는 active feature가 있으면 거절한다. N-2 이하·다른 major는 거절한다. Rolling rollback은 [ADR-0018](../decisions/ADR-0018-release-compatibility-window.md), archive restore는 [ADR-0021](../decisions/ADR-0021-backup-archive-release-window.md)을 따른다.
- ADR-0021 이전 restore build(`58a95f8` 포함)는 producer release exact 일치만 허용한다. 더 새 release의 archive를 그 build로 restore하면 `backup release or schema does not match cluster config`로 target 생성 전에 거절한다. N→N-1 archive restore는 N-1 build가 ADR-0021 이후일 때만 지원한다. 지정 historical baseline의 실행 증거는 [Cluster §11.21](../specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑)에 있다.
- Production Linux/EBS power-loss, WAL torn-tail, volume loss, retention/RPO/RTO와 BP-03는 이 archive contract로 완료되지 않는다. 지정 historical baseline 검증은 §11.21로 한정하며 이후 게시 release별 matrix는 별도다. PRD TEN-08 tenant storage quota는 [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md)과 [Cluster §11.16](../specs/cluster-session-and-delivery.md#1116-tenant-storage-quota-수용-매핑)에서 별도로 제공·검증한다.

### Archive producer/consumer release matrix

아래 행은 모든 voter도 consumer의 window 안에 있고 기존 identity·codec·summary 검증이 통과한다는 조건이다. `N/N-1`은 같은 major의 인접 minor를 뜻한다. 실행 증거는 [Cluster §11.19](../specs/cluster-session-and-delivery.md#1119-backup-archive-nn-1-restore-수용-매핑)에만 기록한다.

| Producer → consumer | Active feature 조건 | 결과·검증 범위 |
| --- | --- | --- |
| N-1 → N | Consumer가 모두 지원 | 허용; synthetic binary upgrade restore |
| N → N-1 | Consumer가 모두 지원 | 허용; feature 비활성 archive의 synthetic binary downgrade restore |
| N → N-1 | Consumer가 지원하지 않는 feature 존재 | Target 변경 전에 거절; synthetic binary process acceptance |
| 같은 minor, patch 차이 | Consumer가 모두 지원 | 허용; unit boundary |
| Minor 차이 2 이상 또는 다른 major | 관계없음 | Target 변경 전에 거절; N→N-2 process 및 양방향·major unit boundary |


## 5. 초안을 확정하기 전에 해결할 사항

- Production-like Linux filesystem/EBS power-loss, WAL torn-tail 및 volume-loss/reseed acceptance.
- `SPEC-TBD-09` retention 및 disaster-recovery RPO/RTO 목표.
- 지정 `58a95f8` historical producer/consumer 검증은 [Cluster §11.21](../specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑)에서 관리한다. 이후 실제 게시 release 사이의 matrix는 release마다 추가한다. Synthetic build acceptance는 [Cluster §11.19](../specs/cluster-session-and-delivery.md#1119-backup-archive-nn-1-restore-수용-매핑)와 구분한다.
- PRD TEN-08 tenant 저장 quota와 초과 경보는 [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md) 및 Cluster §11.16에서 관리한다.
- Durable delivery-session backup/restore process acceptance는 Cluster §11.11에서 `inflight` summary와 restored state를 포함해 통과했다. Production storage/EBS와 이후 실제 게시 release 사이의 compatibility는 별도 미완료 항목이다.
