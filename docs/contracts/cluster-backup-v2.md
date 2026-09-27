# Cluster backup v2 historical interface contract

이 문서는 v2 archive 형식의 역사적 기록이다. [Cluster backup v3](cluster-backup-v3.md)가 현재 archive 계약을 대체한다. 현재 restore CLI는 v2 archive를 거절한다.

| 항목 | 값 |
| --- | --- |
| 이름과 버전 | `cluster-backup-v2` 1.0.0 |
| 상태 | Superseded |
| 소유자 | NaruMQ cluster |
| 만드는 쪽(Producer) | ClusterAdmin이 호출하는 `POST /api/v1/cluster/backups` |
| 사용하는 쪽(Consumer) | `narumq -cluster-config <file> -cluster-restore <archive>` |
| 방향 | Broker → binary archive → 오프라인 restore CLI |
| 스키마 기준 파일 | v2 proto source와 generated binding은 v3 cutover 때 제거했다. 현재 schema는 [cluster-backup-v3.proto](cluster-backup-v3.proto)다. |
| replicated-state schema | [replicated-state-v1.proto](replicated-state-v1.proto), schema `1.0` |
| 기준 동작 명세 | [Cluster §10](../specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore), [§11.7](../specs/cluster-session-and-delivery.md#117-cluster-backuprestore와-snapshot-finalize-수용-매핑), [§11.8](../specs/cluster-session-and-delivery.md#118-retained-replication과-backup-v2-수용-매핑) |
| 관련 결정 | [ADR-0012](../decisions/ADR-0012-retained-replication-and-cluster-backup-v2.md) |
| 관련 요구사항 | [PRD CLU-14](../requirements/PRD.md#92-클러스터와-세션) |
| 호환성 범위 | Historical archive format `2.0`; current v3 restore는 읽지 않는다. |
| 관련 시험 | Codec: `internal/adapters/raft/records`; producer·consumer/process evidence: Cluster §11.8 |

## 1. 형식과 의미

Archive는 아래 byte 순서로 구성한다. 모든 protobuf frame은 deterministic encoding이어야 하며 unknown field, 비정규 encoding, 잘못된 frame 또는 commit 범위 밖 entry는 거절한다.

| 구성 | 형식 | 필수 여부·제약 | 의미 |
| --- | --- | --- | --- |
| Magic | 8 bytes `NMQBKUP2` | 필수 | Backup format major 2를 식별한다. |
| Manifest frame | big-endian `uint32` byte length, CRC32C, protobuf payload | 필수; payload 최대 64 KiB | format, cluster/member identity, release/protocol/schema, snapshot·commit index/term, voter·learner ID, entry 수와 commit 경계 state summary를 담는다. |
| Snapshot frame | big-endian `uint32` byte length, CRC32C, typed-state snapshot payload | 필수 | `snapshot_index`의 typed state와 Raft ConfState를 담는다. |
| Entry frames | 위와 같은 length·CRC32C frame의 `BackupEntry` protobuf | `entry_count`만큼 | Snapshot 뒤 commit 경계까지의 committed command/configuration 기록을 보존한다. No-op Raft entry는 application record로 인코딩하지 않는다. |
| Trailer | 32-byte SHA-256 | 필수 | Magic부터 마지막 frame까지, trailer 직전의 모든 byte를 해시한다. |

전체 archive는 1 GiB를 넘을 수 없고, committed entry 수는 1,000,000개를 넘을 수 없다. Consumer는 frame 길이와 CRC32C(Castagnoli), trailer digest, protobuf canonical encoding, format major/minor, snapshot index, entry count와 entry 범위를 검증한다. Unknown major/minor는 지원하지 않는다.

`boundary_state`는 `commit_index`에서 재구성한 전체 및 kind별 object count/SHA-256을 담는다. Archive state kind는 members, state에서 도출한 tenant ID, session ownership, subscriptions, QoS inflight/checkpoints, membership history와 retained message다. `not_replicated_kinds`는 `["offline"]`이며 offline queue는 archive에 보존되지 않는다. Retained summary는 저장된 retained object 수와 결정적 state digest를 기록한다.

Retained key는 `(tenantId, exact Topic Name)`이다. 같은 topic이라도 tenant가 다르면 독립된 값이다. Replace는 해당 key의 마지막 값을 교체하고 zero-byte PUBLISH는 삭제한다. Archive restore 뒤 retained 값은 새 구독의 replay에 제공된다.

## 2. Producer와 consumer 동작

### Online backup

- `POST /api/v1/cluster/backups`는 body 없는 ClusterAdmin 요청이다. API는 fresh quorum/read barrier를 통과한 뒤 typed snapshot과 committed tail을 같은 commit boundary에 묶어 `application/octet-stream`으로 반환한다.
- `X-NaruMQ-Backup-Commit-Index`는 manifest의 `commit_index`다. `X-NaruMQ-Backup-SHA256`은 trailer digest로, 전체 응답에서 마지막 32 bytes를 제외한 byte의 SHA-256이다.
- Fresh quorum boundary를 만들 수 없으면 retryable `503 cluster_backup_unavailable`을 반환한다. HTTP 인증과 오류 envelope는 [Management API v1](management-api-v1.md)을 따른다.

### Retained replication

- Cluster backup v2가 기록한 시점의 MQTT ingress는 QoS 0 PUBLISH를 지원했다. `Retain=true`이면 publish use case가 route matching과 live fan-out보다 먼저 retained mutation을 Raft committed state에 기록했으며, commit failure에서는 fan-out을 시작하지 않았다. 이후 QoS 1/2 durable delivery 경로는 별도 [ADR-0015](../decisions/ADR-0015-cluster-durable-qos-delivery.md)로 정의됐다.
- Retained value는 `(tenantId, exact Topic Name)`에 속하며 payload, publisher client ID, QoS와 expiry deadline을 포함한다. 적용 한도는 모든 tenant를 합친 전체 기준으로 value 1,024개, 계산된 총 16 MiB, payload당 1,000,000 bytes다. 총 크기는 tenant ID, topic, payload와 publisher routing ID의 byte 길이를 합산하며 tenant별 retained-storage quota는 제공하지 않는다.
- MQTT 5 Message Expiry deadline은 ingress에서 계산해 retained value에 저장한다. Local replay projection은 `List(now)`에서 만료 값을 제거한다. Raft 권위 state는 다음 retained command를 apply할 때 그 command의 proposal 시각으로 만료 entry를 결정적으로 제거하므로, 그 전에는 만료 entry가 snapshot/archive와 용량 한도에 남을 수 있다.
- Commit 뒤 publisher node는 local apply 중 committed value를 retained repository에 반영한다. 다른 node는 committed command를 apply하거나 snapshot catch-up할 때 local projection을 갱신한다. Publish completion은 모든 peer projection 갱신을 기다리지 않는다. Local repository는 replay 조회용 projection(복제 원본에서 다시 만들 수 있는 조회용 사본)이며 권위 있는 값은 Raft state다. Startup 또는 snapshot restore 뒤 projection을 committed state에서 다시 만든다. Projection 재구성이 실패하면 readiness를 허용하지 않는다.
- Retained command는 unknown field를 거절하는 schema `1.0` state에 추가됐다. Retained commit 전 fresh read barrier에서 모든 non-retired committed member의 release, cluster protocol과 state schema가 local member와 정확히 일치하는지 확인한다. 일치하지 않으면 mutation을 제안하지 않는다. Retained 기능에 대한 mixed-release membership과 rolling upgrade는 지원하지 않는다.
- Retained count/byte limit를 넘으면 해당 값으로 replace하지 않고 live fan-out도 시작하지 않는다. Apply는 proposal 시각에 이미 만료된 entry를 먼저 제거할 수 있다. MQTT 5 ingress는 `0x97`(`Quota exceeded`) DISCONNECT를 보내고 연결을 닫는다. 이 v2 contract가 기록한 당시에는 Cluster ingress에서 QoS 1/2 PUBLISH를 지원하지 않았다. 현재 durable QoS 1/2 scope는 [ADR-0015](../decisions/ADR-0015-cluster-durable-qos-delivery.md)와 [Cluster §4.2](../specs/cluster-session-and-delivery.md#42-durable-qos-1-2-delivery)를 참조한다.

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
| Retained publish 거절 | Quorum/compatibility 실패 또는 retained capacity 초과 | 원인 해소 뒤 client 재시도 가능 | Live fan-out을 시작하지 않는다. Capacity conflict는 expired entry를 정리할 수 있으며, commit 대기 timeout이면 proposal 결과가 불확실할 수 있다. |

## 4. 호환성 규칙

- Archive major/minor `2.0`, cluster protocol `2.0`, replicated-state schema `1.0`만 지원한다.
- Format v1 archive(`NMQBKUP1`)는 이 release에서 읽지 않는다. 오류는 `unsupported archive format 1; capture a new backup with this release`이며, v1 archive는 v1을 지원하는 이전 release에서 새 v2 archive를 만들기 전에 복원해야 한다.
- Restore는 cross-cluster migration, voter 재구성, learner 복원을 지원하지 않는다. 현재 bootstrap voter ID와 member identity를 바꾸지 않는다.
- Retained command 추가는 schema `1.0`의 Draft wire format을 변경한다. 이전 schema `1.0` release도 unknown field를 거절하므로 같은 schema 번호만으로 호환된다고 가정해서는 안 된다. Retained proposal은 모든 non-retired member의 정확한 release/protocol/schema가 일치한 뒤에만 허용한다.
- 이전 release로의 rolling rollback 또는 v2 archive를 이전 release에서 restore하는 경로는 지원하지 않는다. downgrade 필요 시 이 archive와 retained state를 이전 binary에 직접 공급하지 않는다.
- Offline queue는 복제·archive 범위 밖이다. Retention, RPO/RTO, production EBS/power-loss와 release 간 archive compatibility는 별도 acceptance가 필요하다.

## 5. 초안을 확정하기 전에 해결할 사항

- Offline queue의 replicated state와 archive 포함 여부를 포함한 전체 CLU-14 object set 및 checksum acceptance.
- Production-like Linux filesystem/EBS power-loss 및 volume-loss acceptance.
- Release 간 archive producer/consumer compatibility matrix와 승인된 migration/rollback 정책.
