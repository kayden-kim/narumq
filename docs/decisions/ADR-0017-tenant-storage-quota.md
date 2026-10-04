# ADR-0017: Dynamic cluster의 tenant별 저장 quota

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (승인된 작업 계획; TEN-08 storage-quota scope only) |
| 결정일 | 2026-09-28 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD TEN-08](../requirements/PRD.md#93-멀티테넌시), [PRD CLU-14](../requirements/PRD.md#92-클러스터와-세션) |
| 관련 스펙·데이터 계약 | [Tenant와 security §5.1](../specs/tenancy-and-security.md#51-저장-quota), [Cluster §4.1·§4.2·§11.16](../specs/cluster-session-and-delivery.md), [replicated-state-v1.proto](../contracts/replicated-state-v1.proto), [Cluster backup v3](../contracts/cluster-backup-v3.md) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md), [Project structure](../architectures/project-structure.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

1. **범위:** Dynamic cluster의 tenant별 retained bytes, offline queue, 저장 총량 quota를 제공한다. Traffic rate와 connection/session 수 quota, tenant quota 관리 API, standalone profile은 범위 밖이다.
2. **설정 원천:** Quota는 정적 `-cluster-config`의 선택 필드 `tenantQuotas`에서 읽는다. 생략하면 built-in profile version 0을 사용하며, 네 차원의 default는 global upper bound와 같다. 설정에 명시한 profile은 `version >= 1`이어야 한다. Profile의 갱신은 version을 올린 설정으로 rolling restart해 제안한다.
3. **차원과 usage:**

   | Config key | Dimension | tenant별 집계 대상 | 최대값 |
   | --- | --- | --- | ---: |
   | `retainedBytes` | `retained_bytes` | retained value의 계산 bytes 합 | 16 MiB |
   | `offlineQueueMessages` | `offline_queue_messages` | QoS 0 offline message와 durable delivery outbound message(QUEUED·inflight) 수 | 8,192 |
   | `offlineQueueBytes` | `offline_queue_bytes` | 위 message bytes 합 | 32 MiB |
   | `storageBytes` | `storage_bytes` | retained, QoS 0 offline, durable delivery outbound·inbound QoS 2 bytes 합 | 48 MiB |

   각 최대값은 각각 `MaxRetainedBytes`, `MaxOfflineMessages + MaxDeliveryEntries`, `MaxOfflineBytes + MaxDeliveryBytes`, `MaxRetainedBytes + MaxOfflineBytes + MaxDeliveryBytes`에서 온다. Byte 계산은 기존 `retainedStateBytes`, `offlineMessageBytes`, `deliveryMessageBytes`, `deliveryInboundBytes` 규칙을 사용한다.
4. **Profile 적용:** `TenantQuota` Raft command로 더 높은 version을 commit한다. State machine은 committed profile만 admission에 사용하므로 모든 replica의 판단이 같다. 해당 tenant의 명시 profile이 없으면 `Default`를 적용한다. Profile이 아직 committed되지 않은 log entry는 tenant quota 검사 없이 기존 global limit만 적용한다. 이로써 profile 설치 전 command replay 결과가 달라지지 않는다. Snapshot과 whole-state SHA-256은 profile과 tenant entries를 포함한다.
5. **초과와 축소:** 기존 global limit을 먼저 검사한 뒤 tenant quota를 검사한다. 변경 차원이 `delta > 0`이고 `usage + delta > limit`이면 초과다. 검사 순서는 `retained_bytes`, `offline_queue_messages`, `offline_queue_bytes`, `storage_bytes`이며 첫 초과 차원을 보고한다. 초과 시 새 durable state를 거절하고 기존 state를 보존한다. Eviction은 하지 않는다. 삭제·축소·CLAIM·COMPLETE·DISCARD·expiry purge·ownership release와 같이 usage를 늘리지 않는 변경은 허용한다. 따라서 quota를 낮춰 기존 state가 한도를 넘더라도 보존되며, state를 줄이는 작업은 계속 가능하다.
6. **기존 결과 재사용:** Retained/offline write의 초과는 기존 capacity conflict로 거절된다. [ADR-0030](ADR-0030-publish-backpressure-admission.md)에 따라 durable delivery의 ingress 사전 판정은 전역·tenant 한도 초과를 publish 거절로 처리한다. MQTT 5는 `0x97`, MQTT 3.1.1은 연결 종료를 따른다. Tenant 초과는 `narumq_tenant_quota_rejections_total`에 기록한다. Apply 시점 경합은 기존 `Saturated` 결과를 유지한다. Session당 한도와 RELEASE 시점 포화도 기존처럼 target을 건너뛴다.
7. **경보와 readiness:** Proposer node는 `narumq_tenant_quota_rejections_total{tenant,dimension}` counter와 tenant·dimension별 10초 rate-limited Warn log를 기록한다. Tenant label은 최대 1,024개까지 보존하고 나머지는 `_overflow`로 합친다. Profile commit 여부·version·local config mismatch를 각각 gauge로 노출한다. Cluster MQTT admission readiness는 committed tenant quota profile이 존재할 때만 허용된다. Committed version보다 local 설정이 낮거나 같은 version의 내용이 다르면 mismatch gauge와 한 번의 Warn log를 남긴다.

## 2. 배경

Retained, QoS 0 offline queue와 QoS 1/2 durable delivery에는 전체 cluster 합산 global capacity가 있다. 이 제한은 한 tenant가 전체 저장 공간을 소비해 다른 tenant의 새 저장을 막는 상황을 방지하지 못한다. PRD TEN-08은 각 tenant의 저장 한도와 초과 경보를 요구한다. Quota 결정을 state-machine apply에서 수행해야 각 replica와 replay가 같은 결과를 내며, 이미 승인한 durable state를 제거하지 않도록 초과 시 새 쓰기만 거절한다.

## 3. 결정 요인

1. 같은 committed command log와 snapshot을 적용하는 모든 replica가 같은 tenant usage 및 admission 결과를 계산한다.
2. Retained, QoS 0 offline, durable delivery outbound·inbound usage를 빠뜨리지 않고 기존 byte 계산과 일치시킨다.
3. Quota를 낮춰도 기존 durable state를 조용히 삭제하지 않으며, usage를 줄이는 상태 전이는 계속 허용한다.
4. 기존 MQTT 응답, global capacity precedence, target별 durable saturation 및 QoS 0 경로를 보존한다.
5. 설정과 경보를 bounded하고, committed profile이 복구되기 전 admission을 READY로 표시하지 않는다.

## 4. 선택 근거

Tenant usage는 replicated state machine의 apply 결과로 유지하고 snapshot에서 state와 함께 복구한다. Profile은 versioned Raft command이므로 정적 config가 바뀌어도 cluster가 commit하기 전까지 이전 결정을 사용한다. Usage가 quota 아래로 돌아오지 않은 기존 state를 보존하면서 negative 또는 zero delta를 허용하면, 낮아진 quota가 cleanup·ACK·expiry·ownership release를 막지 않는다. 각 차원의 검사 순서를 고정하면 모든 replica에서 같은 첫 초과 사유를 반환한다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Quota profile을 `MemberIdentity` digest에 묶는다. | 기존 member compatibility gate와 profile을 함께 검사할 수 있다. | Identity는 commit된 뒤 불변이므로 quota를 갱신할 수 없고, 변경마다 member identity 재설계가 필요하다. | Versioned quota profile command는 identity를 바꾸지 않고 설정 갱신을 commit할 수 있다. |
| Management API PUT으로 quota를 변경한다. | 운영 중 API로 즉시 갱신할 수 있다. | 인증·RBAC·감사·idempotency·API schema·reconciliation 등 mutation 경계가 커진다. | TEN-08의 저장 quota 구현에는 정적 config와 versioned commit만 필요하며, Management API는 별도 범위다. |
| Quota 초과 시 오래된 entry를 eviction한다. | 새 메시지를 더 받을 수 있다. | 기존 durable state와 ACK 보장을 조용히 바꾸며 각 state 종류의 eviction 순서를 추가로 정해야 한다. | 승인된 정책은 새 state 거절이며 eviction 없음이다. |

## 6. 결과

### 긍정적 결과

- 한 tenant의 신규 저장량이 다른 tenant의 tenant quota를 소비하지 않는다.
- Profile version, state usage와 admission 결정은 Raft apply 및 snapshot으로 모든 replica에 동일하게 복구된다.
- Quota를 낮춰도 기존 state가 유지되고, state를 줄이는 transition은 허용된다.
- 초과 dimension과 config 불일치가 bounded-label Prometheus metric 및 log에서 확인된다.

### 부정적 결과와 비용

- State machine은 모든 storage mutation에서 tenant usage를 유지해야 하며, snapshot restore도 같은 aggregate를 재구성해야 한다.
- Quota 변경은 version 증가와 node별 정적 config rollout이 필요하며 즉시 Management API로 바꿀 수 없다.
- Config version이 committed profile보다 낮거나 동일 version의 내용이 다르면 node는 mismatch를 보고한다. Committed profile이 없는 node는 readiness를 통과하지 않는다.
- Tenant ID label은 1,024개 뒤 `_overflow`로 합쳐져 개별 tenant별 metric 식별이 제한된다.
- Linux/EBS power-loss, WAL torn-tail, retention/RPO/RTO와 archive N/N-1 compatibility는 이 결정으로 완료되지 않는다.

### 후속 작업

- [ ] Linux/EBS production storage gate, retention/RPO/RTO와 volume-loss/reseed를 별도 검증한다.
- [ ] Archive producer/consumer compatibility와 N/N-1 rolling upgrade·rollback을 별도 검증한다.
- [ ] Traffic·connection quota, Management API mutation과 usage threshold gauge는 이 범위 밖이다.

## 7. 검증

| 결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Profile validation·version, usage, dimension별 거절·축소와 snapshot replay | `internal/application/membership/tenant_quota_test.go` | Version conflict, tenant isolation, 4 dimensions, retained shrink, offline CLAIM, delivery COMPLETE와 snapshot restore 뒤 같은 rejection을 확인한다. | PASS: `go test ./internal/application/membership -count=1` |
| Replicated command·snapshot wire contract | `internal/adapters/raft/records/tenant_quota_codec_test.go`, `cmd/narumq/cluster_records_test.go` | Command/snapshot round-trip, profile validation and quota-aware object count are checked. | PASS: `go test ./internal/adapters/raft/records ./cmd/narumq -count=1`; `go vet ./...`; `go build ./...` |
| Config, profile controller, readiness, metrics | `cmd/narumq/cluster_tenant_quota_test.go`, `cmd/narumq/cluster_config_test.go`, `internal/adapters/metrics/registry_test.go` | Strict config, profile proposal, mismatch semantics, rejection rate/log, metric label bound and committed-profile readiness are checked. | PASS: `go test ./cmd/narumq -count=1`; `go test ./internal/adapters/metrics -count=1`; `go build ./...`; detailed evidence in Cluster §11.16. |
| 실제 3-node MQTT retained·offline·rolling quota 동작 | `tests/integration.TestDynamicClusterTenantStorageQuotaProcessAcceptance` | Retained 초과 거절, offline target saturation과 재접속 전달, committed version 사용, restart 후 usage rebuild 및 mismatch 수렴을 확인한다. | PASS: `GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterTenantStorageQuotaProcessAcceptance$' -count=1 -v -timeout 20m` (11.10s); detailed evidence in Cluster §11.16. |
| 전체 local verification | `GOMAXPROCS=4 go run ./tools/verify local` | Repository의 unit/integration, race, fuzz, vet, traceability, architecture와 coverage gate가 PASS한다. | PASS: `GOMAXPROCS=4 go run ./tools/verify local`; 상세 실행 증거는 Cluster §11.16에 있다. |

## 8. 재검토 조건

- Quota 단위, 집계 대상, dimension 검사 순서, maximum 또는 초과 동작이 바뀌면 새 결정으로 재검토한다.
- Quota profile을 정적 config 외 API로 바꾸거나 member compatibility identity에 넣는 변경은 설정·관리 경계와 rollout을 함께 검토한다.
- Tenant cardinality가 1,024보다 커져 label 집계 방식이 운영상 불충분해지면 cardinality 제한을 재검토한다.
- Production storage와 N/N-1 evidence는 이 ADR의 기능 acceptance와 별도 release gate로 판정한다.
