# NaruMQ Architecture Decision Records

이 디렉터리는 NaruMQ의 중요한 기술·구조 선택과 근거를 기록한다. 제품 요구는 [PRD](../requirements/PRD.md), 외부 동작은 [시스템 스펙](../specs/README.md), 경계 형식은 [Contract](../contracts/README.md)를 기준으로 한다. 결정이 만든 목표 구조는 [Architecture Views](../architectures/README.md)에서 확인한다.

## 결정 목록

| ADR | 상태 | 결정 |
| --- | --- | --- |
| [ADR-0001](ADR-0001-core-implementation-language.md) | Accepted | Broker core를 Go로 구현한다. |
| [ADR-0002](ADR-0002-cluster-consensus-library.md) | Proposed | Cluster consensus core로 `etcd/raft`를 사용한다. |
| [ADR-0003](ADR-0003-durable-ack-barrier.md) | Proposed | 저장 상태는 quorum WAL 영속화·commit 뒤, target 없는 QoS 1 publish는 quorum read barrier 뒤 ACK한다. |
| [ADR-0004](ADR-0004-session-ownership-and-fencing.md) | Proposed | Session ownership을 quorum-committed epoch와 저장 경계 fencing으로 보장한다. |
| [ADR-0005](ADR-0005-management-api-contract-format.md) | Accepted | Phase 1 query profile의 기준 계약을 OpenAPI 3.1.2 JSON으로 관리한다. |
| [ADR-0006](ADR-0006-external-rollout-controller.md) | Proposed | Pod 삭제 전 drain을 승인하는 전용 rollout controller를 둔다. |
| [ADR-0007](ADR-0007-aws-production-deployment-platform.md) | Proposed | 초기 AWS production profile로 Amazon EKS on EC2를 사용한다. |
| [ADR-0008](ADR-0008-raft-peer-transport.md) | Accepted | 고정 3-node Raft transport v1에 mTLS TCP와 bounded queues를 사용한다. |
| [ADR-0009](ADR-0009-raft-production-state-profile.md) | Accepted (design target only) | Raft version, file-backed storage, replicated-state encoding family와 단일 group target을 정하되 production readiness는 승인하지 않는다. |
| [ADR-0010](ADR-0010-dynamic-cluster-membership.md) | Accepted (membership 설계만) | Cluster membership은 staged learner 전환과 영구 node ID를 사용한다. |
| [ADR-0011](ADR-0011-cluster-backup-restore-v1.md) | Superseded (v1 archive only) | Commit-boundary archive와 offline voter restore를 사용했다. |
| [ADR-0012](ADR-0012-retained-replication-and-cluster-backup-v2.md) | Accepted (retained scope; archive v2는 ADR-0013이 대체) | Retained state를 Raft로 복제하고 backup v2에 포함한다. |
| [ADR-0013](ADR-0013-offline-queue-replication-and-cluster-backup-v3.md) | Accepted (offline QoS 0 + backup v3 scope only) | Detached persistent session의 QoS 0 offline message를 Raft로 복제하고 backup v3에 포함한다. Restore release 규칙은 ADR-0021이 대체한다. |
| [ADR-0014](ADR-0014-cross-node-live-qos0-forwarding.md) | Accepted (cluster live QoS 0 forwarding only; QoS 1/2·durable ACK 제외) | Dynamic cluster의 cross-node live QoS 0 delivery를 별도 `NMQFWRD2` peer stream으로 전달한다. |
| [ADR-0015](ADR-0015-cluster-durable-qos-delivery.md) | Accepted (cluster QoS 1/2 durable delivery scope; production storage·BP-03·N-1 제외; target 0개 PUBLISH command는 ADR-0019가 대체) | Cluster QoS 1/2 전달 queue와 inflight를 Raft에 복제하고 ACK보다 먼저 commit한다. |
| [ADR-0016](ADR-0016-node-drain-and-safe-to-stop.md) | Accepted (broker drain/`SAFE_TO_STOP` scope only; rollout·SDK/Will·production storage·BP-07·N-1 제외) | Replicated drain operation과 `SAFE_TO_STOP` barrier를 제공한다. |
| [ADR-0017](ADR-0017-tenant-storage-quota.md) | Accepted (TEN-08 storage quota scope only) | Dynamic cluster에 versioned tenant별 storage quota와 새 state 거절·초과 경보를 제공한다. |
| [ADR-0018](ADR-0018-release-compatibility-window.md) | Accepted (N/N-1 mixed-release window scope; rollout preflight·historical-source wire 제외) | 인접 minor release의 mixed operation, committed build profile과 cluster-wide feature activation을 정한다. Archive cross-release restore 제외는 ADR-0021이 대체한다. |
| [ADR-0019](ADR-0019-untargeted-durable-publish-quorum-barrier.md) | Accepted (zero-durable-target scope only) | Durable target이 없는 QoS 1/2 publish는 delivery command 대신 quorum read barrier 뒤 완료한다. |
| [ADR-0020](ADR-0020-asynchronous-raft-wal-group-commit.md) | Accepted (비동기 저장 구조만; production storage·BP-03 제외) | 공식 Raft 저장 완료 메시지와 단일 bounded WAL writer의 group commit을 사용한다. |
| [ADR-0021](ADR-0021-backup-archive-release-window.md) | Accepted (backup archive v3 restore release window only; historical-source wire·production storage 제외) | N/N-1 양방향 restore를 허용하고 미지원 active feature를 storage 변경 전에 거절한다. |
| [ADR-0022](ADR-0022-bootstrap-volume-loss-guard.md) | Accepted (empty-directory bootstrap guard와 local process acceptance; production EBS 제외) | 빈 bootstrap directory는 모든 peer 진행 확인 뒤에만 시작하며 함께 시작한 peer는 Raft frame 수신 전까지 허용한다. |
| [ADR-0023](ADR-0023-raft-wal-compaction-and-snapshot-retention.md) | Accepted (local storage lifecycle; production Linux/EBS 제외) | Local snapshot마다 WAL을 영속 tail로 교체하고 최신 snapshot 하나를 보존하며 torn-write 근거가 있을 때만 마지막 CRC 오류를 절단한다. |
| [ADR-0024](ADR-0024-joiner-voter-volume-loss-admission.md) | Accepted (local joiner volume-loss admission; production Linux/EBS 제외) | Seed runtime은 기존 VOTER를 승인하지 않고 fresh joiner는 VOTER 응답을 설치하지 않는다. 같은-volume 복구는 persisted identity로 재개하고 기록 손실은 새 ID로 교체한다. |
| [ADR-0025](ADR-0025-replicated-session-metadata-and-owner-loss-recovery.md) | Accepted | `session-metadata` 복제와 owner crash 뒤 session expiry·Will 복구를 정한다. |
| [ADR-0026](ADR-0026-detach-transition-local-qos0-gate.md) | Accepted (owner-node local QoS 0 publish 경로만; forward·connected owner crash 제외) | Detach 전환 중 owner-node local QoS 0 publish를 queue detaching 표시와 DETACH 완료 대기로 보존한다. |
| [ADR-0027](ADR-0027-detach-forward-and-owner-crash-qos0-boundary.md) | Accepted | Detach 전환 중 forward QoS 0과 DETACH commit 전 owner crash의 local queue 유실을 at-most-once 경계로 확정한다. |
| [ADR-0028](ADR-0028-planned-handoff-will-and-reason-metric.md) | Accepted | 계획 인계에 표준 Will 규칙을 적용하고 최종 결과를 고정한 6개 사유로 집계한다. |
| [ADR-0029](ADR-0029-replication-factor-write-quorum-profile.md) | Accepted | Replication factor·write quorum은 3/2만 지원하고 removal 전 최소 voter 수를 검사한다. |
| [ADR-0030](ADR-0030-publish-backpressure-admission.md) | Accepted (§1 5항 backlog 나이 정의는 ADR-0035가 대체) | 새 durable publish에 전역·tenant capacity, ingress commit backlog와 local disk admission을 적용한다. |
| [ADR-0031](ADR-0031-replicated-state-schema-strict-evolution.md) | Accepted | Replicated-state schema `1.0`의 strict decode, feature activation 뒤 새 형식 기록과 baseline guard를 확정한다. |
| [ADR-0032](ADR-0032-feature-activation-approval-policy.md) | Accepted | 운영 체크리스트·외부 승인 기록으로 feature activation을 승인하며 commit이 rollback window를 끝낸다. API는 승인 evidence를 강제하지 않는다. |
| [ADR-0033](ADR-0033-connection-rebalance-operation.md) | Accepted | 수동 일회성 연결 rebalance를 복제하고 participant 보고로 수렴을 판정한다. |
| [ADR-0034](ADR-0034-node-local-tenant-connection-and-traffic-limits.md) | Accepted (승인된 작업 계획; TEN-06·TEN-07 node-local scope) | 인증된 tenant의 node별 연결·session·traffic 한도와 route projection의 subscription 한도를 적용한다. |
| [ADR-0035](ADR-0035-failover-aware-commit-lag-and-unsent-proposal-reclaim.md) | Accepted | 도달 가능한 leader와 제출 generation으로 commit lag를 계산하고 write 전 forwarded proposal만 회수한다. |
| [ADR-0036](ADR-0036-replicated-proposal-receipts-and-safe-reproposal.md) | Accepted | 활성화된 `proposal-recovery`의 bounded 복제 receipt와 origin별 retirement floor로 이미 전송한 application command도 안전하게 재제안한다. |
| [ADR-0037](ADR-0037-proposal-request-identity.md) | Accepted | Proposal 재제안의 RequestID를 여러 전이가 공유하는 논리 OperationID와 분리한다. |
| [ADR-0038](ADR-0038-proposal-receipt-capacity-fences.md) | Accepted | Receipt count 거절을 origin별 fence로 보존하고 가변 길이 결과를 mutation 전에 예약한다. |
| [ADR-0039](ADR-0039-management-audit-log.md) | Accepted (management plane 범위; MQTT data-plane 거절 감사는 ADR-0046, 조회 API 금지는 ADR-0048이 대체) | Management plane 감사 이벤트를 node-local hash-chain JSONL에 기록하고 mutation은 감사 기록 준비 상태에 따라 fail-closed한다. |
| [ADR-0040](ADR-0040-branch-coverage-arm-injection.md) | Accepted (diff 기준은 ADR-0044가 대체) | 빠진 arm을 사본에 주입해 branch를 측정하고 전체·core baseline ratchet을 강제한다. 결정 당시 diff 95% 기준은 ADR-0044가 대체한다. |
| [ADR-0041](ADR-0041-branch-ratchet-noise-tolerance.md) | Accepted (diff 수치는 ADR-0044가 대체) | 전체·core branch ratchet에 명시된 0.20%p 노이즈 허용폭을 적용한다. Diff 수치 변경은 ADR-0044를 따른다. |
| [ADR-0042](ADR-0042-p0-requirement-traceability-gate.md) | Accepted | PRD 기반 P0 요구사항 연결과 정렬된 예외 목록 ratchet을 적용한다. |
| [ADR-0043](ADR-0043-management-api-rate-limit.md) | Accepted (principal subject 목록은 ADR-0048이 대체) | Management API에 인증 주체별·인증 실패 IP별 node-local token bucket rate limit을 적용한다. |
| [ADR-0044](ADR-0044-diff-coverage-floor-90.md) | Accepted | 변경 코드의 line·function·branch diff coverage 하한을 각각 90%로 정한다. 전체·core 기준과 ratchet은 유지한다. |
| [ADR-0045](ADR-0045-operations-dashboard-grafana.md) | Accepted | Grafana와 Prometheus로 운영 Dashboard를 제공하고 조회 전용 API key를 사용한다. |
| [ADR-0046](ADR-0046-mqtt-data-plane-denial-audit.md) | Accepted | Dynamic cluster에서 MQTT data-plane 거절을 기존 node-local hash chain에 fail-open·bounded async 방식으로 기록한다. |
| [ADR-0047](ADR-0047-mqtt-password-authentication.md) | Accepted (SEC-05 적용 범위) | 고정 Argon2id PHC hash와 stdin hash 명령으로 MQTT password 인증을 제공한다. |
| [ADR-0048](ADR-0048-management-api-oidc-api-key-rbac.md) | Accepted (SEC-10 적용 범위) | Management API에 이름 있는 API key·OIDC subject binding과 4역할 permission을 적용하고 node-local 감사 조회를 제공한다. |
| [ADR-0049](ADR-0049-mqtt-credential-revocation.md) | Accepted (SEC-07 적용 범위) | Security configuration을 rolling restart로 회전하고 Dynamic cluster에서 replicated `credentialId` 폐기와 연결 종료를 제공한다. |
| [ADR-0050](ADR-0050-mqtt-client-blocklist.md) | Accepted (SEC-12 적용 범위) | Dynamic cluster에서 replicated client·username·certificate·IP blocklist와 기존 연결 회수를 제공한다. |
| [ADR-0051](ADR-0051-cluster-update-strategy.md) | Accepted (업데이트 방식과 선택 기준만) | 계획 업데이트는 in-place와 교체형 rolling을 함께 지원하고, 데이터 디렉터리 재연결 가능 여부로 선택한다. 별도 클러스터 Blue/Green은 지원하지 않는다. |
| [ADR-0052](ADR-0052-configuration-tiers-and-cluster-policy.md) | Proposed | 설정을 다섯 범주로 나누고 Dynamic cluster 정책은 Raft에 commit된 versioned 문서를 기준으로 한다. 설정 파일은 versioned 초기값을 제공한다. |
| [ADR-0053](ADR-0053-github-actions-required-ci-gate.md) | Accepted | GitHub Actions `verify`를 실행하고 main의 필수 CI status로 사용한다. |
| [ADR-0054](ADR-0054-tenant-noisy-neighbor-latency-acceptance.md) | Accepted | TEN-09를 같은 실행 baseline 대비 victim p99 1.5배 이하로 판정하는 local 3-node 수용 기준을 정한다. |

`Proposed` ADR은 검토 중인 제안이다. `Accepted` ADR은 각 기록에 명시한 범위 안에서 채택된 결정이다. `design target only`는 production runtime이나 acceptance 검증을 뜻하지 않는다.

## 번호와 변경

ADR 번호는 생성 순서대로 증가한다. Accepted 결정의 결론을 바꿀 때는 기존 파일을 고치지 않고 새 ADR로 supersede한다.
