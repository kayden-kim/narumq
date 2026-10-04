# ADR-0030: Publish backpressure admission

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-04 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD CLU-12·NFR-05·TEN-08](../requirements/PRD.md) |
| 관련 스펙·데이터 계약 | [Cluster §4.5·§11.28](../specs/cluster-session-and-delivery.md#45-publish-backpressure-admission), [Tenant 저장 quota](../specs/tenancy-and-security.md#51-저장-quota) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md), [Project structure](../architectures/project-structure.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | [ADR-0015](ADR-0015-cluster-durable-qos-delivery.md) (§1 7·13항의 전역·tenant capacity 처리), [ADR-0017](ADR-0017-tenant-storage-quota.md) (§1 6항의 ingress 처리) |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

Backpressure는 새 저장을 받기 전에 압력과 저장 한도를 검사하는 admission이다. Replicated apply 결과는 바꾸지 않는다.

1. QoS 1 client PUBLISH의 phased `BeginPublish`와 inline `Publish`, QoS 2 첫 수신 `BeginReceiveQoS2`/`ReceiveQoS2`, payload가 있는 retained QoS 0에 압력 검사를 적용한다. Retained QoS 1도 포함한다.
2. QoS 2 RELEASE(PUBREL), 서버 Will(`trustedPublisherKey != nil`), non-retained QoS 0의 live·offline·forward, 빈 payload의 retained 삭제, SEND/ADVANCE/COMPLETE/ATTACH/DETACH/DISCARD는 압력 검사에서 제외한다. 이미 PUBREC로 승인한 QoS 2 상태의 처리를 막지 않는다.
3. 검사 순서는 disk → commit lag → retained commit과 route match → delivery capacity다. QoS 1 retained는 기존처럼 retained commit 뒤 delivery capacity를 검사한다. QoS 2 첫 수신은 retained를 commit하지 않고 route match와 capacity만 검사한다.
4. Local committed view의 전체 delivery 4,096 entry·16 MiB 또는 tenant quota를 넘는 새 durable publish는 proposal 전에 거절한다. 모든 eligible target의 합계를 검사한다. Session당 64개 또는 sequence 소진 target은 기존처럼 `Saturated`로 건너뛰고 metric을 기록한다. 사전 판정은 record를 만들거나 expiry purge를 수행하지 않는다. 경합으로 apply 시점에 한도를 넘으면 기존 `Saturated` 결과가 최종 결과다.
5. Commit backlog는 이 node가 `BeginDelivery`로 제출한 아직 끝나지 않은 command다. 가장 오래된 command의 나이가 `maxCommitLagMillis` 이상이면 새 QoS 1/2 PUBLISH를 거절한다. 이 경로는 기존 `clusterDurableCommitTimeout` 5초를 따른다. Timeout 없는 context를 받을 수 있는 다른 `Commit` 경로는 추적하지 않는다. Leader가 관찰한 follower별 commit index와 match index 차이는 metric으로만 기록한다.
6. Local cluster data directory의 filesystem 여유 공간을 시작 때와 1초마다 측정한다. `free < minFreeBytes`이면 disk 압력에 진입한다. `free >= minFreeBytes + minFreeBytes/8`이면 해제한다. `minFreeBytes == 0`이면 disk admission만 끄고 여유 공간 gauge는 계속 기록한다.
7. 측정 오류는 이전 상태를 유지한다. 처음 상태는 압력 없음이다(fail-open). 오류가 이어지는 동안 Warn을 한 번만 남긴다. 측정 성공 뒤 새 오류가 생기면 다시 Warn을 남긴다. ENOSPC는 기존처럼 `FAILED`다.
8. Readiness와 lifecycle은 바꾸지 않는다. Disk 압력은 local management summary의 `storagePressure=true`로만 추가 보고한다. `replicationHealth`와 `lastErrorCode`는 바꾸지 않는다.
9. Wire·replicated schema·apply 결과가 같으므로 feature gate를 추가하지 않는다. 새 config key를 이전 binary는 unknown field로 거절한다. Config는 binary와 함께 배포해야 한다. Historical binary가 참여하는 fixture에는 새 key를 넣지 않는다.

### 설정

`publishBackpressure`는 cluster config의 선택 object다. Object를 넣으면 두 key를 모두 명시해야 한다. Unknown key는 거절한다.

| Key | 기본값 | 허용 범위 |
| --- | --- | --- |
| `minFreeBytes` | `1073741824` (1 GiB) | `0`–`1125899906842624` (`1<<50`) |
| `maxCommitLagMillis` | `1000` (1초) | `100`–`10000` |

### 거절 응답

| 요청 | MQTT 5 | MQTT 3.1.1 |
| --- | --- | --- |
| QoS 1 PUBLISH | PUBACK `0x97`, 연결 유지 | 연결 종료 |
| QoS 2 첫 PUBLISH | PUBREC `0x97` | 연결 종료 |
| payload가 있는 retained QoS 0 | DISCONNECT `0x97` 뒤 종료 | 연결 종료 |

PUBACK에 허용되지 않는 `0x89`는 쓰지 않는다. PUBCOMP에는 `0x97`이 없으므로 RELEASE는 새 gate에서 제외한다.

### Metric

| Metric | 의미 |
| --- | --- |
| `narumq_publish_backpressure_rejections_total{reason}` | `commit_lag`, `disk_free`, `delivery_capacity`, `tenant_quota`별 거절 counter. 네 reason을 항상 출력한다. |
| `narumq_publish_backpressure_active{reason}` | `commit_lag`, `disk_free`의 0/1 gauge. 두 reason을 항상 출력한다. |
| `narumq_storage_free_bytes` | Local filesystem 여유 공간. 첫 성공 측정 전에는 출력하지 않는다. |
| `narumq_cluster_commit_backlog_oldest_seconds` | 가장 오래된 미완료 command의 나이. 초 단위 소수점 여섯 자리다. |
| `narumq_durable_delivery_saturated_targets_total` | Apply 결과의 포화 target 수. |
| `narumq_raft_peer_replication_lag_entries{peer}` | Leader가 관찰한 peer별 `max(CommitIndex-match, 0)`. Follower에서는 series를 출력하지 않는다. |

Tenant 거절은 기존 `narumq_tenant_quota_rejections_total{tenant,dimension}`에도 기록한다. 거절 Warn은 reason별 10초에 한 번으로 제한한다. Disk·lag 진입은 Warn, 해제는 Info로 기록한다.

## 2. 배경

기존 apply는 포화 target만 건너뛰고 publisher에게 성공 ACK를 허용한다. 복제 지연을 판단하는 ingress admission과 filesystem 여유 공간 측정도 없다. CLU-12는 drop보다 publish 제한을 우선하도록 요구한다.

## 3. 결정 요인

1. ACK된 상태와 기존 protocol 진행을 보존한다.
2. Replicated apply를 바꾸지 않고 새 ingress 저장만 제한한다.
3. 한 session의 backlog가 독립 subscriber를 막지 않도록 한다.
4. 거절 원인과 회복을 metric·log·실제 MQTT 응답으로 확인한다.

## 4. 선택 근거

전역·tenant 저장 한도는 모든 eligible target을 합산하여 사전 거절한다. Session 포화는 기존 apply 의미를 유지한다. Local ingress backlog는 이 node의 새 저장 진행이 밀린 상태를 직접 나타낸다. Disk 여유 공간의 절대값은 작은 시험 volume과 운영 volume에서 같은 단위로 설정할 수 있다. 1/8 hysteresis는 경계 부근의 반복 진입과 해제를 줄인다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 모든 포화 target에서 publish 전체 거절 | 모든 대상의 동시 수용을 요구한다. | 한 session이 다른 subscriber도 막는다. | Session 한도와 sequence 소진은 기존 `Saturated` 의미를 유지한다. |
| 현재 의미 유지 | 추가 admission이 없다. | 전역·tenant 한도에서도 publisher 성공을 허용한다. | CLU-12의 publish 제한 우선 요구를 충족하지 못한다. |
| 가장 느린 follower를 admission에 포함 | 뒤처진 replica를 보호한다. | Quorum이 진행해도 독립 follower가 모든 ingress를 막는다. | Ingress commit backlog만 admission에 쓰고 follower 지연은 관찰한다. |
| Disk 10% 또는 비율·절대값 혼합 | Volume 크기에 비례한다. | 작은 volume에서도 기준이 달라지고 설정이 복잡하다. | 이번 정책은 설정 가능한 절대 여유 공간 기준이다. |

## 6. 결과

### 긍정적 결과

- 저장 한도와 local 압력을 명시적 MQTT 응답으로 보고한다.
- 회복 뒤 기존 연결의 QoS 1 재시도가 가능하다.
- Apply·wire·schema 변경 없이 mixed-version 운용을 유지한다.

### 부정적 결과와 비용

- QoS 2 RELEASE 시점 fan-out 포화는 여전히 `Saturated`다.
- 사전 판정은 근사값이다. 만료된 entry가 아직 공간을 차지하면 보수적으로 거절할 수 있다. 동시 proposal은 apply에서 포화될 수 있다.
- QoS 1 retained commit 뒤 capacity 거절은 retained mutation을 되돌리지 않는다.
- Disk 판정은 local volume만 본다. 다른 voter의 volume 압력은 전파하지 않는다.
- 새 config key는 이전 binary와 호환되지 않는다.

### 후속 작업

- Production Linux/EBS 장애 보장은 별도 storage gate로 검증한다. 이 admission 수용은 이를 대체하지 않는다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Read-only capacity와 apply 불변 | `TestDeliveryPublishCapacity`, 기존 delivery 시험 | 상태 무변경, 전역·tenant 거절, session 포화 유지 | PASS; [Cluster §11.28](../specs/cluster-session-and-delivery.md#1128-publish-backpressure-수용-매핑) |
| Gate와 MQTT 응답 | `TestPublishAdmissionGatesNewDurableState`, `TestTCPPublishBackpressureReasonCodes` | 대상·제외 경로와 `0x97`/종료가 일치 | PASS; Cluster §11.28 |
| Disk·lag·설정·metric | Controller/config/metrics/diskspace unit | Hysteresis, backlog 제거, strict config, metric 교체 | PASS; Cluster §11.28 |
| 실제 포화와 회복 | `TestDynamicClusterPublishBackpressureProcessAcceptance`, `follower-disk-pressure` chaos | 거절 topic 미전달, healthy node 진행, 회복 뒤 성공 | PASS; Cluster §11.28 |
| 기존 경계와 hot path | Process 회귀, local gate, FUSE/ext4, BP-03 smoke | Gate 통과와 valid workload, rejected ACK·gap·duplicate 0 | PASS; Cluster §11.28 |

## 8. 재검토 조건

- Session 포화도 publisher 실패로 바꾸거나 RELEASE를 막아야 하면 ACK 계약을 다시 검토한다.
- 다른 voter의 disk 압력을 cluster 전체 admission에 쓰려면 peer 계약을 먼저 정한다.
- 실제 부하에서 기본 lag·disk 기준으로 불필요한 거절이 확인되면 측정 근거로 기본값을 재검토한다.
