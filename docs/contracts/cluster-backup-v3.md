# Cluster backup v3 인터페이스 계약(Contract)

이 계약은 실행 중인 3-voter cluster에서 하나의 commit 경계를 담은 archive를 만들고, 검증된 archive로 동일 cluster의 빈 voter 저장 디렉터리를 오프라인 seed하는 형식을 정의한다. Retained와 detached persistent session의 QoS 0 offline queue를 포함한다. Production storage, durable QoS 1/2 ACK, archive N/N-1 compatibility와 disaster-recovery 보존 정책은 이 계약만으로 완료되지 않는다.

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
| 기준 동작 명세 | [Cluster §4.1·§10](../specs/cluster-session-and-delivery.md), [§11.9](../specs/cluster-session-and-delivery.md#119-offline-queue-replication과-backup-v3-수용-매핑) |
| 관련 결정 | [ADR-0013](../decisions/ADR-0013-offline-queue-replication-and-cluster-backup-v3.md) |
| 관련 요구사항 | [PRD CLU-14](../requirements/PRD.md#92-클러스터와-세션) |
| 호환성 범위 | archive format `3.0`; 동일 `clusterId`, release/protocol/schema 및 현재 3개 bootstrap voter identity |
| 관련 시험 | Codec: `internal/adapters/raft/records`; producer·consumer/process evidence: Cluster §11.9 |

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

`boundary_state`는 `commit_index`에서 재구성한 전체 및 kind별 object count/SHA-256을 담는다. Required kind는 members, tenants, sessions, subscriptions, inflight, membership history, retained와 offline이다. `offline` summary의 count는 detached offline session record 수와 그 record에 든 queued message 수의 합이다. Offline record의 tenant ID도 tenant digest 입력에 포함한다. `not_replicated_kinds` field는 이 형식에 없다.

Retained key는 `(tenantId, exact Topic Name)`이다. Offline key는 `(tenantId, clientId)`이며 각 detached persistent session의 message는 단조 증가 sequence로 FIFO 순서를 보존한다. Exact state transition, ownership fencing, at-most-once claim, expiry와 capacity는 [Cluster §4.1](../specs/cluster-session-and-delivery.md#41-offline-message-capture와-전달)을 따른다.

## 2. Producer와 consumer 동작

### Online backup

- `POST /api/v1/cluster/backups`는 body 없는 ClusterAdmin 요청이다. API는 fresh quorum/read barrier를 통과한 뒤 typed snapshot과 committed tail을 같은 commit boundary에 묶어 `application/octet-stream`으로 반환한다.
- `X-NaruMQ-Backup-Commit-Index`는 manifest의 `commit_index`다. `X-NaruMQ-Backup-SHA256`은 trailer digest로, 전체 응답에서 마지막 32 bytes를 제외한 byte의 SHA-256이다.
- Fresh quorum boundary를 만들 수 없으면 retryable `503 cluster_backup_unavailable`을 반환한다. HTTP 인증과 오류 envelope는 [Management API v1](management-api-v1.md)을 따른다.

### Retained와 offline state

- Retained value와 offline queue는 committed typed state이며 snapshot, backup summary와 restore에서 함께 보존한다. Restore된 state는 새 owner와 catch-up node가 사용할 수 있다.
- Cluster MQTT ingress는 현재 QoS 0 PUBLISH만 지원한다. Detached persistent session에 매칭된 QoS 0 delivery는 Raft offline state로 enqueue한다. Connected session의 live path는 node-local이다.
- Disconnect 시 persistent session의 pending FIFO는 DETACH command로 capture한다. 이후 detached publish는 ENQUEUE로 기록한다. 새 ownership epoch의 CONNECT는 CLAIM을 commit해 queue를 삭제한 뒤 local delivery를 재개하므로 의미는 at-most-once다.
- Offline limit는 session당 64 message, 전체 4,096 message·16 MiB, payload당 1,000,000 bytes, DETACH command당 topic+payload 768 KiB다. Tenant별 quota는 이 contract에서 제공하지 않으며 PRD TEN-08의 P0 requirement는 미충족이다.
- Offline command는 schema `1.0`과 unknown-field rejection을 유지하며, fresh read barrier 뒤 모든 non-retired member의 release/protocol/schema가 정확히 일치하는 profile에서만 제안한다. 이 제한은 N/N-1 rolling compatibility를 의미하지 않는다.

### Offline restore

- Restore는 실행 중인 cluster process가 아닌 **중지된 node**에서 수행한다. CLI는 config, format, identity, state와 checksum을 target에 쓰기 전에 검증한다. 성공하면 seed를 저장하고 종료하며 cluster listener를 시작하지 않는다.
- `-cluster-config`는 `role=bootstrap`이며 현재 voter 세 개를 정확히 포함해야 한다. Archive voter ID와 config bootstrap member ID는 같은 정렬된 집합이어야 하고 archive에 learner ID가 하나라도 있으면 restore를 거절한다.
- `clusterId`, producer release, cluster protocol, state schema는 config와 같아야 한다. 각 bootstrap voter의 member ID, peer address, DNS SAN 및 certificate SHA-256 fingerprint도 committed member identity와 일치해야 한다. Archive source는 현재 voter여야 하며 local `nodeId`도 voter 집합에 속해야 한다.
- 같은 archive를 각 voter의 별도 빈 data directory에 한 번씩 restore한다. 기존 data directory나 foreign file을 덮어쓰지 않는다.
- `restore-incomplete` marker가 남으면 일반 cluster startup은 실패한다. Marker나 restore 파일을 수동 삭제하지 않는다. 같은 archive와 올바른 config로 restore를 다시 실행하거나 운영 담당자에게 이관한다.
- Restore 완료 뒤 각 node를 정상 `-cluster-config` 경로로 시작한다. READY는 snapshot/WAL replay와 fresh quorum/readiness 검증 뒤에만 반환한다.

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
- v2→v3 live migration과 converter는 제공하지 않는다. Cutover 전에 운영자는 persistent subscriber를 다시 연결해 기존 node-local queue를 drain해야 한다. Drain하지 않은 queue는 이전 process 종료와 함께 사라질 수 있다. 새 build의 replicated offline state는 빈 상태로 시작하며, v2 archive를 v3 reader로 변환하는 경로는 없다.
- Restore는 cross-cluster migration, voter 재구성, learner 복원을 지원하지 않는다. 현재 bootstrap voter ID와 member identity를 바꾸지 않는다.
- Offline field 추가는 unknown field를 거절하는 schema `1.0`의 replicated-state wire format 변경이다. 같은 schema 번호만으로 release 호환을 추정하지 않는다. Offline/retained proposal 전에 모든 non-retired member의 release/protocol/schema 정확한 일치가 필요하다.
- 이전 release로 rolling rollback 또는 v3 archive를 이전 release에서 restore하는 경로는 지원하지 않는다. N/N-1 archive compatibility는 별도 승인과 acceptance가 필요하다.
- Production Linux/EBS power-loss, WAL torn-tail, volume loss, retention/RPO/RTO, durable QoS 1/2 ACK와 TEN-08 tenant별 quota는 이 archive contract의 acceptance가 아니다.

## 5. 초안을 확정하기 전에 해결할 사항

- Production-like Linux filesystem/EBS power-loss, WAL torn-tail 및 volume-loss/reseed acceptance.
- `SPEC-TBD-09` retention 및 disaster-recovery RPO/RTO 목표.
- Release 간 archive producer/consumer compatibility matrix와 승인된 migration/rollback 정책.
- PRD TEN-08 P0의 tenant별 offline queue, retained bytes, 저장 용량 quota와 초과 경보.
- Durable QoS 1/2 offline delivery와 ACK barrier.
