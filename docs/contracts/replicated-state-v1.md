# Replicated-state v1 인터페이스 계약(Contract)

이 계약은 cluster node가 복제하고 복구하는 command·snapshot의 bytes와 호환성 규칙을 정한다. Backup archive 안의 typed state도 같은 decode 규칙을 따른다. Archive framing은 [Cluster backup v3](cluster-backup-v3.md)가 정의한다.

| 항목 | 값 |
| --- | --- |
| 이름과 버전 | `narumq-replicated-state` 1.0.0 |
| 상태 | Active |
| 소유자 | NaruMQ 프로젝트 |
| 만드는 쪽(Producer) | Cluster leader와 snapshot·backup을 만드는 runtime |
| 사용하는 쪽(Consumer) | 모든 cluster node와 offline restore |
| 방향 | Cluster node ↔ Cluster node·offline restore |
| 스키마 기준 파일 | [replicated-state-v1.proto](replicated-state-v1.proto) |
| 기준 동작 명세 | [Cluster §8](../specs/cluster-session-and-delivery.md#8-mixed-version-compatibility) |
| 관련 요구사항 | CLU-11, CLU-14, SEC-07 |
| 호환성 범위 | Schema `1.0`; release window와 feature activation은 [Release compatibility v1](cluster-release-compatibility-v1.md)를 따른다. |
| 관련 결정 | [ADR-0031](../decisions/ADR-0031-replicated-state-schema-strict-evolution.md) Accepted |
| 관련 시험 | [Cluster §11.29](../specs/cluster-session-and-delivery.md#1129-replicated-state-v1-계약-수용-매핑) |

## 1. 형식과 의미

`CommandEnvelope`는 protobuf `proto3` schema major/minor와 payload 한 개를 담는다. `MembershipRecord`, `OwnershipChange`, `SubscriptionChange`, `DeliveryChange`, `RetainedChange`, `OfflineChange`, `NodeDrainChange`, `TenantQuotaChange`, `MemberProfileChange`, `FeatureActivation`, `SessionMetadataChange`, `ConnectionRebalanceChange`, `CredentialRevocationChange`를 지원한다. `MemberProfileChange`는 `member_id`, canonical `release_version`과 정렬된 `features`를 담는다. 변경 가능한 build profile field만 갱신한다. `FeatureActivation`은 feature token을 담고 모든 non-retired member가 광고할 때만 적용한다. `MemberIdentity`는 commit된 release/features profile을 담는다. `ReplicatedStateSnapshot.active_features`는 정렬된 feature 집합이다.

Session metadata의 동작과 불변 조건은 [ADR-0025](../decisions/ADR-0025-replicated-session-metadata-and-owner-loss-recovery.md)와 [Cluster §4.3](../specs/cluster-session-and-delivery.md#43-session-metadata-복제와-owner-손실-복구)를 따른다. 실행 증거는 [§11.24](../specs/cluster-session-and-delivery.md#1124-session-metadata-복제와-owner-손실-복구-수용-매핑)에 있다. 선택 field `OwnershipChange.session`은 CONNECT acquisition 또는 transfer의 metadata를 담는다. `ACQUIRE`·`TRANSFER`에서만 유효하다. Ownership과 새 `CONNECTED` metadata를 같은 command에서 원자적으로 적용하고 이전 record와 pending Will을 대체한다. `session-metadata` 비활성 상태에서는 이 field를 생략해야 한다. `SessionMetadataChange`는 owner/epoch fencing을 적용하는 `DETACH`·`CLAIM_WILL`·`DISCARD` 전이다. Metadata를 담은 proposal과 record는 feature activation 뒤에만 쓴다. Build는 `session-metadata`를 광고하지만 activation 전에는 metadata payload를 제안하거나 저장하지 않는다. 비활성 cluster의 기존 동작과 bytes를 유지한다.

`ConnectionRebalanceChange`는 `START`·`REPORT`·`FAIL`·`CANCEL`을 담는다. Command oneof 번호는 22, snapshot의 `connection_rebalance` 번호는 13이다. Active `connection-rebalance` feature 뒤에만 기록해야 한다. Snapshot의 connection rebalance record는 0개 또는 1개이며 `objectCount`에 1개로 센다. Participant는 NodeID 오름차순이다. 동작은 [ADR-0033](../decisions/ADR-0033-connection-rebalance-operation.md)과 [Cluster §9](../specs/cluster-session-and-delivery.md#9-rebalance)를 따른다.
`CredentialRevocationChange`는 command field 25이며 `REVOKE` 또는 `RESTORE`, UTF-8 `credential_id` 1~512 bytes, 양수 proposer Unix milliseconds를 담는다. `CredentialRevocationState`는 snapshot field 16에 저장한다. `credential-revocation` feature가 활성화된 뒤에만 폐기 상태를 propose하거나 snapshot에 저장한다. Snapshot은 최대 4096개 entry를 credential ID의 UTF-8 bytewise 엄격 오름차순으로 보존한다. 같은 ID의 재폐기는 최초 `revoked_at_unix_millis`를 유지하고, capacity 초과는 `credential_revocation_capacity_exceeded`로 거절한다.

Snapshot의 `membership_history`는 각 전이가 commit된 정확한 `committed_index`를 저장한다. 제안 시에는 Raft entry index를 모르므로 command payload의 이 field는 0이어야 한다. Application은 apply 때 실제 index를 기록한다. Snapshot은 `0 < committed_index <= last_included_index`를 검증한다. 복구한 operation status는 snapshot 경계가 아니라 마지막 전이의 index를 보고한다.

Snapshot의 `lastIncludedIndex`는 Raft가 적용한 마지막 index와 같아야 한다. 리더 선출의 빈 entry처럼 application event가 없는 commit도 typed state index를 전진시킨다. Member·operation 내용은 바꾸지 않는다. 이 규칙으로 learner 추가 직후 snapshot의 `ConfState`와 typed snapshot index를 맞추고 learner 복구를 지원한다.

`DeliverySessionState`는 `(tenantId, clientId)`별 복제 aggregate다. 증가하는 `next_sequence`, sequence 순서의 `DeliveryMessage`와 packet ID 순서의 `InboundMessage`를 담는다. Delivery message는 topic·binary payload·effective QoS·retain·expiry·packet ID·phase를 저장한다. Packet ID 0은 `QUEUED`다. Inbound state는 PUBREL 전 publisher QoS 2 payload를 보존한다. RELEASE 뒤에는 payload 없는 tombstone을 보존한다. ATTACH/DETACH/DISCARD와 SEND/ADVANCE/COMPLETE는 정확한 owner/epoch fencing으로 aggregate를 변경한다. Stale command는 commit 순서 위치를 소비하지만 aggregate를 바꾸지 않는다.

`SnapshotEnvelope`는 schema major/minor, `lastIncludedIndex`, `objectCount`, SHA-256과 typed `ReplicatedStateSnapshot`을 담는다. `objectCount`는 members·ownerships·subscriptions·delivery sessions와 내부 messages/inbound·membership history·retained·offline sessions와 내부 messages·node drains·active feature 값·tenant quota profile의 object·session metadata record·credential revocation entry를 센다. Digest 입력은 envelope를 제외한 `ReplicatedStateSnapshot`의 deterministic protobuf bytes다. Unknown field, 정의되지 않은 enum/action, 중복 singular field·oneof, 잘못된 UTF-8, 비정규 bytes, 미지원 version, count·digest 불일치는 apply·recovery·readiness를 fail-closed로 막는다. Release 호환성은 이 검증을 완화하지 않는다.

### 크기 제한

| 범위 | 한도·규칙 |
| --- | --- |
| `CommandEnvelope` | 직렬화 bytes 최대 1 MiB; Raft `maxDynamicCommandSize`와 같다. |
| `SnapshotEnvelope` | 직렬화 bytes 최대 64 MiB |
| Cluster ID | UTF-8 1..200 byte |
| Address | UTF-8 1..255 byte, 유효한 host:port |
| DNS SAN | ASCII DNS 1..253 byte |
| Release version | Canonical `MAJOR.MINOR.PATCH`; 각 값은 선행 0 없는 `uint32` decimal; ASCII 최대 64 byte |
| Idempotency key | 공백 없는 printable ASCII 1..128 byte |
| Tenant ID | UTF-8 1..128 byte |
| Client ID | UTF-8 1..256 byte |
| Topic/filter | 유효한 MQTT UTF-8 1..65,535 byte; filter는 MQTT 규칙으로 검증 |
| Certificate fingerprint | Protobuf에서는 정확히 32 byte; HTTP JSON에서는 소문자 64-hex |
| Durable delivery QoS·packet ID | QoS 1 또는 2; `QUEUED`에서만 packet ID 0, inflight에서는 양수; phase는 QoS와 일치 |
| Inbound QoS 2 | Packet ID 1..65535; 미release entry는 payload 보존; release tombstone은 빈 payload와 유효한 `released_index` |
| Subscription QoS·retain handling | 각각 0..2 |
| Durable delivery | Session당 message 최대 64개와 inbound 최대 64개; cluster 전체 최대 4,096 entry·16 MiB; publish target 최대 1,024개 |
| Message expiry | Unsigned Unix milliseconds; 0은 만료 없음 |
| MQTT publish payload | 최대 1,000,000 byte; 포함 command도 1 MiB 이하 |
| Snapshot entry | 전체 최대 1,000,000개; 내부 delivery message/inbound·offline message 포함; session metadata 최대 4,096개; 직렬화 64 MiB 한도 우선 |
| Session metadata state | Cluster 전체 Will·principal bytes 최대 8 MiB; topic + payload + subject + credential ID + role bytes 합산; tenant storage quota에는 미청구 |
| Session metadata Will | 유효한 MQTT topic 1..65,535 byte; payload 최대 65,535 byte; QoS 0..2; principal은 Will이 있을 때만 허용 |
| Session metadata principal | Subject·credential ID는 유효한 UTF-8 각각 1..1,024 byte; expiry 양수; 고유 role 최대 64개, 각각 UTF-8 1..256 byte |
| Session metadata snapshot key | UTF-8 bytewise `(tenant_id, client_id)`로 정렬·고유; ownership row 필수; `record.epoch <= ownership.epoch`; active `session-metadata` 필수 |
| Connection rebalance `maxDeviationConnections` | `1..1_000_000` |
| Connection rebalance `connections` | `0..4_294_967_295` |
| Connection rebalance `failure_code` | `1..64` byte ASCII token; FAILED에서만 허용 |
| Connection rebalance snapshot | Record 0개 또는 1개; active `connection-rebalance` 필수; participant 최소 2개, NodeID 순증가 |
| Credential ID | UTF-8 1..512 bytes; credential ID는 전체 revoked entry에서 유일 |
| Credential revocation state | 최대 4096 entry; revoked 시각 양수; 비어 있지 않으면 active `credential-revocation` 필수 |

한도를 바꾸려면 이 contract를 변경하고 vector를 추가해야 한다.

### 정렬과 canonical bytes

Deterministic serialization은 같은 값에서 같은 bytes를 만드는 규칙이다. 이를 반드시 사용한다. Proto3 기본값 scalar는 생략하고 field 번호 오름차순으로 encode한다. `members`는 `member_id`, member의 `features`와 `active_features`는 사전순으로 정렬한다. Feature 중복은 거절한다. `ownerships`·`delivery_sessions`는 UTF-8 bytewise `(tenant_id, client_id)`, `subscriptions`는 `(tenant_id, client_id, filter)`, `membership_history`는 `operation_id`, `credential_revocations`는 `credential_id`로 정렬한다. Session 안의 messages는 sequence 오름차순, inbound는 양수 packet ID 오름차순이다. Canonical key 중복은 거절한다. Consumer는 parse·duplicate/unknown 검증·deterministic 재직렬화 뒤 bytes가 같은지 확인한다. 삭제한 field 번호와 enum 값은 영구 reserved로 남긴다.

### Proposal recovery 형식

`proposal-recovery` 활성화 뒤 `CommandEnvelope.proposal`(field 24)을 기록할 수 있다. Field 23은 기존 unknown-field 거절 vector의 의미를 유지하도록 reserved로 남긴다. Proposal은 origin NodeID, 최초 fresh proof index, retirement floor, 별도 canonical UUID `request_id`와 proposal field를 제외한 canonical command bytes의 SHA-256을 담는다. 활성화 전에는 이 field를 생략한다. 여러 전이가 공유하는 논리 OperationID와 재시도의 RequestID는 [ADR-0037](../decisions/ADR-0037-proposal-request-identity.md)에 따라 분리한다.

`ReplicatedStateSnapshot.proposal_origins`(field 15)는 NodeID 순서의 origin 상태다. Field 14는 reserved로 남긴다. Origin은 단조 증가 retirement floor와 RequestID 순서의 receipt를 담는다. `rejected_through`·`rejected_index`는 count 한도로 거절한 proof 경계와 그 판정의 적용 위치다. 둘 다 0이거나, 양수 `rejected_through < rejected_index <= last_included_index`여야 한다. 이미 저장한 receipt는 이 fence보다 먼저 조회한다. Receipt는 RequestID, 최초 proof index, command digest와 최초 apply 결과를 보존한다. Result의 index는 최초 적용 위치이며 snapshot 경계 이하이어야 한다. 중복 적용 표시는 transient 결과이며 receipt에 저장하지 않는다. Membership 결과의 MemberID·Phase는 원래 operation view의 값이다. Action으로 추정하지 않는다.

Origin과 receipt는 각각 `objectCount`에 1개로 센다. Result 안의 값은 receipt의 일부이며 별도 object로 세지 않는다. Node별 receipt 최대 1024개, 전체 최대 4096개, 결과의 논리적 저장량 전체 최대 32 MiB다. `ProposalResultStorageBytes`가 고정 record 공간, 문자열, slice element와 payload 길이를 합산한다. 아직 쓰지 않은 slot마다 512 byte를 conflict 저장 공간으로 남긴다. Wire result bytes도 32 MiB를 넘을 수 없다. Payload를 포함한 결과는 snapshot의 64 MiB 한도에도 포함한다. Receipt의 삭제·stale frame 차단·재제안은 [ADR-0036](../decisions/ADR-0036-replicated-proposal-receipts-and-safe-reproposal.md)과 [ADR-0038](../decisions/ADR-0038-proposal-receipt-capacity-fences.md)을 따른다.


## 2. 오류

| 오류 코드 | 조건 | 재시도 가능 | 상태 변경 |
| --- | --- | --- | --- |
| `incompatible_state_schema` | Root·하위 message의 unknown field, 모르는 command payload·snapshot collection, `1.0`이 아닌 schema | 불가 | Committed apply에서 Raft driver 중단·process 종료; startup recovery는 시작 실패; READY 불가; 거절한 state를 다시 쓰지 않음 |
| `credential_revocation_capacity_exceeded` | 폐기 목록이 4096 entry이고 새로운 credential ID를 revoke | 아니오 | 기존 replicated state 유지 |

정의되지 않은 enum 값도 기존 validator가 거절한다. 값 범위·정렬·count·digest·enum validation 오류는 기존 오류를 유지하며 schema sentinel로 분류하지 않는다. Backup 경로는 오류 wrapping 뒤에도 메시지에 `incompatible_state_schema`를 남긴다.

## 3. 호환성 규칙

- Strict decode를 따른다. Command·snapshot·backup archive에서 root와 하위 message의 unknown field, 모르는 payload·collection, `1.0`이 아닌 version과 정의되지 않은 enum 값을 거절한다. Unknown field를 보존하거나 무시하지 않는다.
- Schema는 `1.0`으로 고정한다. 추가 변경에 minor를 올리지 않는다. Major 변경은 [Release contract §7](cluster-release-compatibility-v1.md#7-breaking-change)의 breaking change다.
- 새 field 번호는 해당 message의 기존 최대 번호보다 커야 한다. 번호·enum 값·reserved 이름은 재사용하지 않는다. 기존 field의 번호·type·cardinality·의미는 바꾸지 않는다. Proto3 기본값은 기존 동작을 뜻해야 한다.
- 새 field·payload·collection·enum 값은 해당 feature token의 `FeatureActivation`이 commit된 뒤에만 propose하거나 저장한다. Activation 전 bytes는 baseline schema로 unknown 없이 decode되어야 한다.
- Feature activation이 그 feature의 rollback window를 끝낸다. Activation 뒤 해당 feature를 지원하지 않는 N-1은 READY가 되지 않는다. 새 field를 만나면 fail-closed다. 운영자의 window 종료 판정·승인 절차는 [Release contract §4.1](cluster-release-compatibility-v1.md#41-운영-승인과-rollback-window-종료)을 따른다.

## 4. 버전과 지원 단계

| 상태 | 조건 | 지원 종료·대체 |
| --- | --- | --- |
| Active | 사용자 결정과 ADR-0031 Accepted; producer/consumer·baseline·release 회귀 결과는 §11.29에서 관리 | Major 변경에는 명시적인 migration·recovery 계획 필요 |

## 5. 예제와 계약 테스트(Contract test)

기준 bytes는 [`internal/adapters/raft/records/testdata/replicated-state-v1/`](../../internal/adapters/raft/records/testdata/replicated-state-v1/)의 `*.hex`다. 소문자 hex를 byte 사이 공백 한 칸으로 구분한다. 한 줄은 최대 16 byte이며 끝에 줄바꿈을 둔다. 읽을 때 공백을 제거한다.

| 파일 이름 (`.hex`) | 종류 | 의미 |
| --- | --- | --- |
| `stage_join` | Producer command | Operation `00000000-0000-4000-8000-000000000004`, key `k1`, member 4, cluster `c`, address `n:1`, DNS `n`, fingerprint 32 byte `0x11`, release `1.0.0`, protocol `2.0`, schema `1.0`, phase STAGED |
| `ownership_acquire` | Producer command | Operation `00000000-0000-4000-8000-000000000001`, tenant `t`, client `c`, expected epoch 0, owner 1, epoch 1, ACQUIRE |
| `subscription_add`·`subscription_remove` | Producer command | Ownership과 같은 operation·tenant·client·owner·epoch, filter `#`, QoS 1, action ADD/REMOVE |
| `delivery_publish` | Producer command | Ownership과 같은 operation·tenant·client, PUBLISH, proposed time 1, topic `a`, payload `x`, QoS 1, target client `c` |
| `empty_snapshot` | Producer snapshot | Schema `1.0`, index 1, count 0, 빈 state, 빈 bytes의 SHA-256 |
| `command_ownership_unknown_field` | Reject | OwnershipChange 끝의 field 15 varint 1 (`78 01`), 길이 prefix 보정 |
| `command_unknown_payload` | Reject | Schema major 1·ownership operation 뒤 field 23 빈 bytes (`ba 01 00`) |
| `command_schema_minor_1` | Reject | Ownership command의 schema minor 1 |
| `snapshot_record_unknown_field` | Reject | Index 3, ownership `{t,c,owner 1,epoch 1,committed 1}` 끝의 `78 01`; raw state로 digest·count 계산 |
| `snapshot_unknown_collection` | Reject | 빈 state의 field 14 빈 bytes (`72 00`); raw state digest `96229c0a…`, count 0 |
| `snapshot_schema_minor_1` | Reject | Empty snapshot의 schema minor 1 |

`baseline-schema-1.0.binpb`는 마지막으로 호환을 약속한 schema descriptor다. 이번에는 현재 schema로 만든다. 실제 release를 게시할 때만 그 release의 deterministic descriptor로 교체한다. Proto 파일에는 import가 없다. 게시 release별 process matrix는 tasks §1.2의 별도 gate다.

| 시험 | 위치·검증 |
| --- | --- |
| `TestReplicatedStateContractVectors` | [state_vectors_test.go](../../internal/adapters/raft/records/state_vectors_test.go); producer bytes 일치·semantic round-trip, consumer 거절 분류 |
| `TestReplicatedStateSchemaPreservesBaseline` | [baseline_schema_test.go](../../internal/adapters/raft/records/baseline_schema_test.go); message·field·enum·reserved 보존, 새 field 번호 증가 |
| `TestProducerVectorsDecodeWithBaselineSchema` | 같은 파일; producer vector 6개의 baseline decode와 재귀 unknown 검증 |
| `TestInactiveFeatureRecordsDecodeWithBaselineSchema` | [cluster_records_baseline_test.go](../../cmd/narumq/cluster_records_baseline_test.go); 실제 DTO bridge의 비활성-feature command·snapshot을 baseline으로 검증 |

Repository root에서 `go test ./internal/adapters/raft/records ./cmd/narumq -count=1`을 실행한다. 실행 결과·실패 주입·fixture 보정·release 회귀·전체 local gate 증거는 [Cluster §11.29](../specs/cluster-session-and-delivery.md#1129-replicated-state-v1-계약-수용-매핑)를 참조한다.

## 6. 범위 밖 gate

게시 release별 baseline 갱신과 process matrix, production Linux/EBS storage 수용은 [PRD 구현 잔여 작업](../TODO.md)의 별도 작업이다. 운영 승인 정책의 수용 범위는 [Cluster §11.30](../specs/cluster-session-and-delivery.md#1130-feature-activation-운영-승인-수용-매핑)에서 관리한다.
