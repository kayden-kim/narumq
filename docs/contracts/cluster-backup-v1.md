# Cluster backup v1 인터페이스 계약(Contract)

이 계약은 실행 중인 3-voter cluster에서 일관된 commit 경계의 archive를 만들고, 검증한 archive로 각 voter의 빈 저장 디렉터리를 오프라인에서 seed하는 형식을 정의한다. 이 계약은 전체 CLU-14 또는 disaster-recovery 운영 정책을 완료하지 않는다.

| 항목 | 값 |
| --- | --- |
| 이름과 버전 | `cluster-backup-v1` 1.0.0 |
| 상태 | Draft |
| 소유자 | NaruMQ cluster |
| 만드는 쪽(Producer) | ClusterAdmin이 호출하는 `POST /api/v1/cluster/backups` |
| 사용하는 쪽(Consumer) | `narumq -cluster-config <file> -cluster-restore <archive>` |
| 방향 | Broker → binary archive → 오프라인 restore CLI |
| 스키마 기준 파일 | [cluster-backup-v1.proto](cluster-backup-v1.proto) |
| 기준 동작 명세 | [Cluster §10](../specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore), [§11.7](../specs/cluster-session-and-delivery.md#117-cluster-backuprestore와-snapshot-finalize-수용-매핑) |
| 관련 요구사항 | [PRD CLU-14](../requirements/PRD.md#92-클러스터와-세션) |
| 호환성 범위 | archive format `1.0`; 동일 `clusterId`, release/protocol/schema, 정확히 현재 3개 bootstrap voter identity |
| 관련 시험 | Codec: `internal/adapters/raft/records.TestBackupArchiveRoundTripAndChecksums`; producer·consumer/process evidence: Cluster §11.7 |

## 1. 형식과 의미

Archive는 다음 byte 순서로 구성한다. 모든 protobuf 메시지는 deterministic encoding이어야 하며 알 수 없는 field, 비정규 encoding, 잘못된 frame 또는 commit 범위 밖 entry는 거절한다.

| 구성 | 형식 | 필수 여부·제약 | 의미 |
| --- | --- | --- | --- |
| Magic | 8 bytes `NMQBKUP1` | 필수 | Archive family를 식별한다. |
| Manifest frame | big-endian `uint32` byte length, CRC32C, protobuf payload | 필수; payload 최대 64 KiB | format, cluster/member identity, release/protocol/schema, snapshot·commit index/term, voter·learner ID, entry 수와 commit 경계 state summary를 담는다. |
| Snapshot frame | big-endian `uint32` byte length, CRC32C, versioned typed-state snapshot payload | 필수 | `snapshot_index`에 포함된 typed state와 Raft ConfState를 담는다. |
| Entry frames | 위와 같은 length·CRC32C frame의 `BackupEntry` protobuf | `entry_count`만큼 | snapshot 이후 commit 경계까지의 committed command/configuration 기록을 보존한다. No-op Raft entry는 application 기록으로 인코딩하지 않는다. |
| Trailer | 32-byte SHA-256 | 필수 | Magic부터 마지막 frame까지, trailer 직전의 모든 byte를 해시한다. |

전체 archive는 1 GiB를 넘을 수 없다. 각 frame의 길이와 CRC32C(Castagnoli), trailer digest, protobuf canonical encoding, archive format major/minor, snapshot index, entry count 및 entry 범위는 consumer가 모두 검증한다. Unknown major/minor는 지원하지 않는다.

Manifest의 `boundary_state`는 `commit_index`에서 재구성된 state의 전체 및 kind별 object count/SHA-256을 담는다. 현재 archive 종류는 members, session ownership, subscription, QoS inflight/checkpoints, membership history와 그 state에서 도출한 고유 tenant ID다. `not_replicated_kinds`는 `["offline", "retained"]`이며 offline queue와 retained message는 archive에 보존되지 않는다.

## 2. Producer와 consumer 동작

### Online backup

- `POST /api/v1/cluster/backups`는 body 없는 ClusterAdmin 요청이다. API는 fresh quorum/read barrier를 통과한 뒤 snapshot과 committed tail을 동일 commit boundary에 묶어 `application/octet-stream`으로 반환한다.
- `X-NaruMQ-Backup-Commit-Index`는 manifest의 `commit_index`다. `X-NaruMQ-Backup-SHA256`은 trailer digest로, 전체 응답에서 마지막 32 bytes를 제외한 byte의 SHA-256이다.
- fresh quorum boundary를 만들 수 없으면 retryable `503 cluster_backup_unavailable`을 반환한다. 자세한 HTTP authentication 및 error envelope는 [Management API v1](management-api-v1.md)을 따른다.

### Offline restore

- Restore는 실행 중인 cluster process가 아닌 **중지된 node**에서 실행한다. CLI는 config/format/identity/state/checksum을 target directory에 쓰기 전에 검증한다. 성공하면 seed를 저장하고 종료하며 cluster listener를 시작하지 않는다.
- `-cluster-config`는 `role=bootstrap`이며 정확히 현재 voter 세 개를 포함해야 한다. Archive voter ID와 config bootstrap member ID는 같은 정렬된 집합이어야 하고, archive에 learner ID가 하나라도 있으면 restore를 거절한다.
- `clusterId`, producer release, cluster protocol, state schema가 config와 같아야 한다. 각 bootstrap voter의 member ID, peer address, DNS SAN 및 certificate SHA-256 fingerprint도 archive의 committed member identity와 같아야 한다. Archive source는 현재 voter여야 하며 각 restore config의 local `nodeId`도 이 voter 집합에 속해야 한다.
- 동일 archive를 각 voter의 별도 빈 data directory에 한 번씩 restore한다. 기존 data directory나 foreign file을 백업 데이터로 덮어쓰지 않는다.
- Restore 중 `restore-incomplete` marker가 남으면 일반 cluster startup은 실패한다. Marker나 그 옆의 restore 파일을 수동 삭제하지 않는다. 동일 archive와 올바른 config로 restore를 다시 실행하거나, 운영 담당자에게 이관한다.
- restore 완료 후 각 node를 정상 `-cluster-config` 경로로 시작한다. READY는 snapshot/WAL replay와 fresh quorum/readiness 검증을 통과한 뒤에만 반환한다.

## 3. 오류

| 오류 | 조건 | 재시도 가능 | 상태 변경 |
| --- | --- | --- | --- |
| `400 invalid_request` | Backup 요청이 body를 포함하거나 요청 형식이 유효하지 않음 | 아니오 | 없음 |
| `401` / `403` | ClusterAdmin 인증·권한 실패 | credential/권한 수정 후 | 없음 |
| `405 method_not_allowed` | 선언되지 않은 HTTP method | 아니오 | 없음 |
| `503 cluster_backup_unavailable` | fresh quorum/read boundary 또는 backup capture 불가 | 가능 | 없음 |
| Restore CLI non-zero exit | archive 손상, boundary summary mismatch, identity/version mismatch, learner 포함 또는 durable write 실패 | 원인에 따라; marker가 있으면 같은 디렉터리 일반 startup 금지 | 검증 실패 전 target 불변; 쓰기 실패 뒤에는 `restore-incomplete`가 startup을 차단 |

## 4. 호환성 규칙

- Archive major/minor `1.0`, cluster protocol `2.0`, state schema `1.0`만 지원한다.
- Restore는 cross-cluster migration, voter 재구성, learner 복원을 지원하지 않는다. 현재 bootstrap voter ID와 각 identity를 바꾸지 않는다.
- 추가 required field, frame 변경 또는 summary 의미 변경은 호환성을 깨므로 새 format major와 migration 계획이 필요하다.
- `offline`/`retained` 포함 여부를 현재 계약의 의미로 오해하지 않는다. 해당 state를 보존하는 archive는 이 format의 지원 범위 밖이다.
- Restore process test는 staged command tail, seed startup, node crash/restart와 joiner resync를 검증한다. 이는 `CLU-14`의 retained/offline checksum, 장기 보존/RPO/RTO, power-loss 또는 production EBS guarantee를 충족했다는 뜻이 아니다.

## 5. 초안을 확정하기 전에 해결할 사항

- Offline queue와 retained state를 포함한 전체 CLU-14 object set, retention 정책, disaster-recovery RPO/RTO.
- Production-like Linux filesystem/EBS power-loss 및 volume-loss acceptance.
- Release 간 archive producer/consumer 호환성 matrix와 승인된 migration/rollback 정책.
