# PRD 구현 잔여 작업

이 문서는 [PRD](requirements/PRD.md)의 구현·수용 시험·선행 결정 잔여 작업을 요구사항 ID 기준으로 추적한다. 범위는 MQTT·클라이언트 호환성, cluster/session, tenancy/security, management/observability, deployment, performance와 verification 전반이다. 기존 cluster backlog는 상태와 근거를 보존해 포함한다.

PRD는 `Draft` 상태이므로 이 목록은 요구사항을 구현 기준으로 채택했다는 뜻이 아니다. 구현 범위와 완료 판정은 채택된 스펙·계약·ADR 및 연결된 수용 시험을 따른다. 시험 명령, 실행 시간, run ID 같은 증거는 각 기준 문서의 수용 매핑에 기록하고 이 목록에는 반복하지 않는다.

| 표기 | 뜻 |
| --- | --- |
| `[ ]` | 열린 작업이다. 착수 전 링크한 기준 문서에서 범위를 확인한다. |
| `[ ]` **보류** | 외부 결정이나 실행 환경이 없어 착수하지 않는 작업이다. 보류 사유와 해제 조건을 함께 적는다. |
| `[x]` | 적힌 범위의 수용 시험을 통과했다. 범위 밖 gate는 §1의 열린 작업으로 따로 추적한다. |

열린 항목의 **미구현**은 필요한 기능이 없다는 뜻이고, **부분 구현**은 현재 기능이 요구 범위의 일부만 충족한다는 뜻이다. **결정 필요**는 구현·시험 전에 정책이나 계약을 확정해야 하며, **미검증**은 구현 증거가 있어도 해당 수용 시험은 남아 있다는 뜻이다. 후속 ADR과 최신 수용 매핑에서 해결된 과거 항목은 다시 열지 않는다.

## 1. 열린 작업

### 1.1 Production storage와 disaster recovery

2026-10-03 사용자 결정으로 실제 인프라 시험을 보류한다. 아래 두 항목이 닫히기 전에는 CLU-14, [ADR-0009 §7](decisions/ADR-0009-raft-production-state-profile.md#7-검증)의 production 행과 [ADR-0003 §9](decisions/ADR-0003-durable-ack-barrier.md#9-검증) 전체를 완료로 표시하지 않는다.

- [ ] **보류** — Linux/EBS 전원·volume 손실 수용: 지원 Linux filesystem/EBS에서 전원 손실 뒤 마지막 durable commit 보존, production WAL torn-tail 판정, volume loss/reseed를 시험한다.
  - 보류 사유: 인프라 시험을 보류했다. 시험용 AWS 계정이 없고 [PRD §18](requirements/PRD.md#18-미결정-사항) 질문 16(EKS minor version, EC2 instance family, EBS volume type·IOPS, NLB target mode)이 정해지지 않았다.
  - 해제 조건: 인프라 시험 재개 결정, 시험 계정·region 확보와 질문 16의 storage profile 확정.
  - 이 gate를 대신하지 않는 local 증거: Docker/FUSE 장애·power-cut simulation [§11.6](specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping), WAL lifecycle·torn-tail [§11.22](specs/cluster-session-and-delivery.md#1122-wal-compactionsnapshot-retentiontorn-tail-수용-매핑), voter volume 손실 [§11.20](specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑), local ext4 [§11.23](specs/cluster-session-and-delivery.md#1123-local-ext4-filesystem-수용-매핑).
- [ ] **보류** — Backup 보존 기간과 disaster-recovery RPO/RTO(`SPEC-TBD-09`): [Cluster §10](specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore) 7항의 보존 기간과 RPO/RTO를 정하고 restore 시간·보존을 시험한다. 현재 backup은 수동 online API와 offline restore만 제공하며 정기 backup과 외부 저장소는 없다.
  - 보류 사유: [PRD §18](requirements/PRD.md#18-미결정-사항) 질문 12의 정책 값이 정해지지 않았다.
  - 해제 조건: 질문 12에 대한 답변. 시험은 위 Linux/EBS 항목과 같은 실행 환경을 사용한다.

### 1.2 Release와 rollout gate

- [ ] **미검증** — 게시 release별 historical-source N-1↔N 호환성: 실제 release를 게시할 때마다 직전 게시 release와의 rolling upgrade·rollback·archive restore를 [§11.21](specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑) 방식으로 추가한다. 이때 [Replicated-state v1 §5](contracts/replicated-state-v1.md#5-예제와-계약-테스트contract-test)의 baseline descriptor를 게시 release의 descriptor로 갱신한다. 지정 baseline `58a95f8`만 통과했으며 현재 게시 release tag는 없다.
- [ ] **보류 · 미구현** — External rollout controller: [ADR-0006](decisions/ADR-0006-external-rollout-controller.md)(Proposed)의 `SAFE_TO_STOP` 기반 Pod 삭제 승인을 구현·시험한다. Custom resource와 reconciliation 계약, version preflight, 실패·timeout·unknown barrier에서 현재와 다음 node의 교체 중단을 포함한다([Release contract §8](contracts/cluster-release-compatibility-v1.md#8-blocker와-제외-범위)). Broker 쪽 drain은 완료했다([Drain spec §10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)).
  - 보류 사유: Kubernetes 실행 환경이 필요한 인프라 시험이다.
  - 해제 조건: 인프라 시험 재개 결정.
- [ ] **미검증** — BP-07 rolling drain 비교: PRD Phase 3 출시 gate다. Local Docker bridge의 3-node 순차 drain·같은 volume 재시작 harness를 구현했다. Readiness LB는 test-only다. Production broker의 리더 이전 proposal·fresh-read 소실과 harness의 round 역행을 수정했다. 실제 compare `bp07-20261005T031404.679399800Z-37844`의 두 target과 5회 교차 suite `bp07-suite-20261005T032136.047989300Z-30100`의 10개 target 실행이 모두 valid였다. 모든 suite 반복의 QoS 1/2 gap·QoS 2 duplicate·계획 밖 종료는 0이었다. QoS 1 duplicate와 QoS 0 loss는 별도로 기록했다. 최종 local gate도 PASS였다([성능 §3.5 실행 증거](specs/performance-and-verification.md#35-bp-07-rolling-drain-smoke-관찰-경계)). Local BP-03 smoke 통과를 대신 쓰지 않으며, release의 수치형 합격 기준은 [성능 §3](specs/performance-and-verification.md#3-benchmark-공통-계약)의 `SPEC-TBD-08`을 따른다.
  - 실행 증거 연결: 확인 커밋 `fca37620e053a121987c37dd20483fa4f584469f`에 BP-07 소스·시험 변경을 기록했다. Compare·suite run ID와 local gate 결과는 [성능 §3.5 실행 증거](specs/performance-and-verification.md#35-bp-07-rolling-drain-smoke-관찰-경계)에 연결했다. 원래 manifest의 `dirty=true`는 유지하며, 커밋 뒤 재실행이나 CI 통과로 기록하지 않는다.
  - 남은 조건: `SPEC-TBD-08`의 release 수치 결정이 남아 있다. `clusterPendingCommit.Wait` caller timeout 뒤 FIFO 재생·한 번만 적용·성공 응답 없음은 [Cluster §11.11의 Caller timeout 뒤 late commit 행](specs/cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑)의 in-process 3-node runtime + 실제 dynamic Raft adapter 시험과 TCP 오류 시험으로 확인했다. MQTT process 수준 경합 재현은 이 증거의 범위가 아니다.
- [ ] **미검증** — 최대 traffic 연속 100회 rolling: [Drain spec §9](specs/drain-and-rolling-update.md#9-수용-시험)의 MQTT 3.1.1/5·QoS 0/1/2·retained·Will·persistent session 혼합, gap·duplicate·reorder와 controller 중단을 검증한다. ADR-0028의 3-round broker process 시험으로 이 gate를 대신하지 않는다.
- [ ] **미검증** — 지원 SDK의 Server redirect·Will 호환성: [Client contract §6.2](contracts/mqtt-client-compatibility-v1.md#62-drain-compatibility-suite)의 `drain-paho-go-python-will-acl-final`은 9/10이다. Paho Go MQTT 5 `v0.23.0`의 `go-mqtt5-malformed-duplicate-server-reference`가 `serverReferenceAccepted=true`, `trap_accept=1`로 실패했다. 두 SDK의 callback adapter redirect·session resume·후속 전달·Will 억제와 Python malformed 4종은 통과했다([Drain spec §10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)). Artifact는 `.artifacts/compatibility/mqtt-clients-v1/drain-paho-go-python-will-acl-final/`이다. 범위는 test-only readiness LB와 시간 값이며 `SPEC-TBD-04` 운영 수치와 rollout timing은 제외한다. SDK 결함은 우회하지 않았다. Will 사유별 broker 계측은 §2.4에서 완료 범위를 추적한다.
  - 원인: Paho `Properties.Unpack`이 중복 Server Reference를 검사하지 않고 마지막 값으로 덮어쓴다. Callback에는 중복 정보가 남지 않는다([Client contract §6.2](contracts/mqtt-client-compatibility-v1.md#62-drain-compatibility-suite)).
  - 결정: 공식 SDK만 유지한다. 직접 패치한 SDK와 raw socket 검사는 도입하지 않는다.
  - 해제 조건: 이 결함을 고친 공식 SDK release의 version 변경을 검토한 뒤 drain 10개와 Phase 1 26개 case를 모두 통과한다. 조사 시점에는 수정된 공식 release가 없어 SDK 수정과 새 version 회귀는 실행하지 않았다.
- [x] **완료** — 교체형 업데이트 조합: [ADR-0051](decisions/ADR-0051-cluster-update-strategy.md)의 새 node 추가·승격 → 기존 node drain·`SAFE_TO_STOP` → planned removal을 3-node process로 확인했다. `TestDynamicClusterReplacementRollingUpdateProcessAcceptance`는 3회 연속 교체에서 session 손실 0, QoS 1/2 gap 0, QoS 2 duplicate 0과 최종 voter `{4,5,6}`을 확인한다([Drain spec §10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)). 기존 `MaxDeliveryMessagesPerSession=64`에 맞춰 offline session당 총 54개 메시지를 보낸다. 이 local process 증거는 production profile과 N-1↔N 혼합 교체를 포함하지 않는다.

### 1.3 Cluster 기능과 알려진 구현 한계

이 절의 연결·session rebalance(CLU-13)는 §2.4의 수동 일회성 operation 범위에서 완료했다. 별도 production 성능 수용은 완료 범위에 포함하지 않는다.

### 1.4 Drain 정책·관측과 production 배포

- [ ] **결정 필요** — Drain·reconnect 운영값(`SPEC-TBD-04`): [ADR-0016 §6](decisions/ADR-0016-node-drain-and-safe-to-stop.md#6-결과)과 [MQTT client contract §4](contracts/mqtt-client-compatibility-v1.md#4-session과-expiry)의 connection rate, LB 전파 대기, timeout, reconnect/backoff·Will safety margin과 최악 termination grace를 확정한다. 현재 drain API는 rate/delay/timeout을 호출자가 지정하며 제품 기본값이 없다. Phase 1 test-only 값을 production 기본값으로 쓰지 않는다. 확정한 값은 SDK 시험과 배포 admission의 기준으로 연결한다.
- [ ] **보류 · 미구현** — Kubernetes 종료 연동과 `EKS_EC2_HA` 배포 artifact(DEP-13~DEP-23): [Drain spec §7·§7.1](specs/drain-and-rolling-update.md#7-external-rollout-gate와-kuberneteseks), [ADR-0007 §7](decisions/ADR-0007-aws-production-deployment-platform.md#7-검증)과 [EKS architecture](architectures/eks-ec2-deployment.md)의 목표 범위다. `preStop`의 idempotent drain 호출·결과 기록, grace period 검증, PDB·3-AZ placement·교체 동시성, stable node identity와 EBS 결합, NLB target lifecycle 및 profile별 지원 상태를 배포 artifact에 연결한다. Controller 구현은 §1.2, storage 전원·volume 시험은 §1.1에서 추적한다.
  - 보류 사유: 인프라 시험 보류 중이며 ADR-0006/0007은 Proposed, EKS architecture는 Target/Draft이고 구현 증거가 없다.
  - 해제 조건: 인프라 작업 재개와 배포 결정 채택, [PRD §18](requirements/PRD.md#18-미결정-사항) 질문 10·16의 profile 확정 및 실행 환경 확보.
  - 완료 확인: hook 중복·실패·grace 부족, NLB 전파 중 신규 연결, Pod·EC2·단일 AZ 손실, EBS 재연결과 연속 rolling upgrade·rollback을 실제 profile에서 검증한다. Broker process의 `SAFE_TO_STOP` 통과로 대신하지 않는다.

### 1.5 설계 미결정과 비지원 범위

[Cluster architecture의 미결정 사항](architectures/cluster-architecture.md#미결정-사항)은 과거 target과 현재 구현을 함께 언급한다. 아래 경계는 새 기능의 구현 승인이 아니며, 현재 지원 범위를 넓힐 때 먼저 결정해야 할 사항이다.

- Route-index 물리적 배치와 rebuild 방식: 현재 node-local projection rebuild는 §2.4의 수용 범위에서 완료했다. Architecture의 물리적 배치 질문을 기존 rebuild 미구현으로 다시 열지 않는다. 별도 분산 배치나 rebuild 방식 변경이 필요하면 [ADR-0009 §1.4](decisions/ADR-0009-raft-production-state-profile.md#14-consensus-group-partition-profile)의 단일 group·node-local projection과의 관계, ordering과 routing readiness를 먼저 확정한다.
- Active peer transport 밖 RPC encoding: 현재 [Raft membership v2](contracts/raft-membership-v2.md)와 live QoS 0 forwarding은 구현·수용 범위가 있다. 그 밖의 RPC를 도입할 때 producer/consumer, message 형식·한도·인증·version 호환 계약을 먼저 정한다. Architecture의 일반적인 encoding 질문만으로 현재 경로 전체를 미구현으로 표시하지 않는다. Persisted schema 계약은 §2.6에서 완료했으며 미래 major migration은 별도 계약을 먼저 정한다.
- Cross-cluster migration과 3-bootstrap-voter identity 제한 밖 restore: [Cluster §10](specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore)의 현재 비지원 범위다. V1/v2 archive, learner를 포함한 archive와 voter-set 변경도 현재 restore로 지원하지 않는다. 별도 migration·identity 계약의 채택 없이 자동으로 구현 백로그에 넣거나 CLU-14의 현 수용 범위로 포함하지 않는다. Retained·offline·durable delivery backup 자체는 §2.3·§2.4에서 완료 범위를 확인한다.

### 1.6 MQTT protocol과 client 호환성

- [ ] **미검증 — MQTT 3.1.0 production 전환(MQTT-13)**: [PRD §9.1](requirements/PRD.md#91-mqtt-프로토콜)의 90일 이상 client 사용량 조사, 오래된 firmware·외부 장비 소유자 확인, 3.1.1 이상 전환 또는 별도 gateway 격리와 canary 거절 Metric 감시를 완료한다. Wire-level 거절 구현·시험은 완료된 범위이며 이 항목은 production migration gate다.
- [ ] **결정 필요 — WebSocket/WSS 필요성(MQTT-10)**: [PRD §18 질문 6](requirements/PRD.md#18-미결정-사항)에서 사용 현황을 확인한다. 사용 필요가 승인되면 WebSocket/WSS listener와 TCP와 같은 MQTT packet·session 동작을 구현·검증한다. 필요가 확인되지 않으면 PRD의 P1 범위를 갱신하기 전까지 구현하지 않는다.

### 1.7 Tenant·security와 감사

- [ ] **결정 필요 — noisy-neighbor p99 한도(TEN-09; PRD §18 질문 8)**: [PRD §18](requirements/PRD.md#18-미결정-사항)에서 tenant 부하와 허용할 다른 tenant의 p99 지연을 확정한다. TEN-06·TEN-07의 기능 격리 시험은 p99 수용 기준을 대신하지 않는다. TEN-08 저장 quota는 ADR-0017의 채택 범위로 제공하므로 다시 미구현으로 열지 않는다.
- [x] **완료 — SEC-05 MQTT username/password 인증**: 고정 Argon2id PHC hash와 strict `passwordIdentities` 설정, stdin hash CLI, JWT·password 상호 배타성, mTLS provider와 설정의 병행 및 dual credential 거절, tenant·Client ID·Will 경계를 구현했다. [ADR-0047](decisions/ADR-0047-mqtt-password-authentication.md)과 [Tenant와 security §9.5](specs/tenancy-and-security.md#95-sec-05-password-authentication-실행-증거)의 시험 및 실행 증거를 따른다.
- [x] **완료 — SEC-07 MQTT credential 회전·폐기와 만료 종료**: Security config의 rolling restart, Dynamic cluster의 committed `credentialId` 폐기, 활성 연결 종료와 protocol별 expiry를 구현했다. [ADR-0049](decisions/ADR-0049-mqtt-credential-revocation.md), [운영 가이드](how-to/credential-rotation-and-revocation.md)와 [Tenant와 security §9.7](specs/tenancy-and-security.md#97-sec-07-credential-회전폐기-실행-증거)의 acceptance 증거를 따른다.
- [x] **완료 — SEC-10 Management API 인증·RBAC·감사 조회**: named API key, 정적 OIDC role binding, permission matrix와 node-local audit 조회를 구현했다. [Tenant와 security §9.6](specs/tenancy-and-security.md#96-sec-10-실행-증거)에 저장소 검증을 기록했다.
- [x] **완료 — SEC-12 MQTT client·identity·IP blocklist**: Dynamic cluster에 tenant-scoped Client ID, CONNECT username, verified leaf certificate fingerprint와 source IP 차단을 구현했다. Raft 적용, 5초 전파, 새 CONNECT 거절, 기존 연결 종료, feature gate, RBAC와 audit을 검증했다. [ADR-0050](decisions/ADR-0050-mqtt-client-blocklist.md)와 [Tenant와 security §9.8](specs/tenancy-and-security.md#98-sec-12-blocklist-실행-증거)의 acceptance·verification 증거를 따른다.
- [ ] **부분 구현 — 감사 운영 범위(SEC-11·NFR-17·API-08)**: Management plane은 [ADR-0039](decisions/ADR-0039-management-audit-log.md)에 따라 활성 mutation 10개, 401·403 거절, node-local hash chain과 `-audit-verify`를 구현했다. MQTT CONNECT·Will·PUBLISH·SUBSCRIBE·UNSUBSCRIBE 거절은 [ADR-0046](decisions/ADR-0046-mqtt-data-plane-denial-audit.md)에 따라 bounded fail-open recorder로 구현하고 [Tenant와 security 스펙 §9](specs/tenancy-and-security.md#9-수용-시험과-범위)에 시험과 실행 증거를 연결했다. 남은 범위는 rollout controller producer, SIEM consumer의 unknown action/reason 검증, 보존 기간과 SIEM 접근 권한 결정이다. |
- [ ] **미검증 — 보안 출시 gate(NFR-19)**: [PRD §11.4·§15.4](requirements/PRD.md)의 threat model과 tenant-isolation penetration test를 release 전에 수행한다. 신뢰 경계·공격 사례를 승인된 threat model에 기록하고, penetration test의 결과와 해결 또는 위험 승인 기록을 남긴다.
  - 자동 사전 시험과 [threat model 초안](architectures/threat-model.md)을 완료했다. 시험 위치와 실행 증거는 [Tenant와 security §9.3](specs/tenancy-and-security.md#93-nfr-19-자동-penetration-test-실행-증거)에 있다.
- 남은 조건은 threat model 승인, R-01~R-03·R-05~R-07 해결 또는 위험 승인, 독립 penetration test다. NFR-19는 `미검증`으로 유지한다.

### 1.8 Management API·observability와 Dashboard

- [ ] **제안·미구현 — API-09 GitOps와 runtime 설정의 source of truth**: [Configuration and runtime options spec](specs/configuration-and-runtime-options.md)은 현재 적용 경로를, [ADR-0052](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)는 target policy를 설명한다. ADR은 Proposed이며 API-09 구현과 수용 시험은 남아 있다. 설정 구조 차이는 §1.10 G1~G10에서 추적한다. 제안: [ADR-0052](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)(Proposed).
- API-05 rate limit은 [ADR-0043](decisions/ADR-0043-management-api-rate-limit.md)과 [Management API 스펙 §1.5](specs/management-api-and-observability.md#15-rate-limit)로 구현·시험했다.
- [x] **완료 — UI-01~UI-05·UI-10 운영 Dashboard**: [ADR-0045](decisions/ADR-0045-operations-dashboard-grafana.md), [Management API 스펙 §8·§9](specs/management-api-and-observability.md#8-dashboard-계약), [운영 절차](how-to/operations-dashboard.md), `tests/integration/dashboard_stack_test.go`로 구현·시험했다. 2026-10-09 static contract와 local Docker process acceptance가 통과했다. Production deployment는 미검증이다.
- [ ] **결정 필요 — P1 관리 화면·exact-topic Metric(UI-06~UI-09·API-07·TEN-10)**: client 검색·상세, disconnect/redirect, retained·exact-topic Metric과 delegated tenant role은 active query/API 계약 또는 후속 acceptance에 걸쳐 있다. 요구 우선순위, tenant 권한, API 계약과 UI 제공 순서를 확정한다. 채택한 범위만 구현하고 권한·마스킹·pagination·idempotency·감사 기준을 시험한다. 아직 deferred인 API를 호출하는 화면은 구현된 제품 동작으로 표시하지 않는다.

### 1.9 Performance·test와 공통 출시 gate

- [ ] **결정 필요 — production workload와 성능 합격선(NFR-01~NFR-06·PERF-01~PERF-12)**: [PRD §18 질문 1~5·7~10·13~15](requirements/PRD.md#18-미결정-사항)과 [Performance와 verification 스펙](specs/performance-and-verification.md)의 `B_conn`, `B_pub`, `B_sub`, payload·topic 분포, tenant 부하, failure-domain profile, 고정 EMQX release/edition과 수치형 합격 기준을 확정한다. `SPEC-TBD-08`은 §1.2의 BP-07 release gate와도 연결되며, BP-03 smoke 완료는 전체 workload/SLO 결정을 대신하지 않는다.
- [ ] **미검증 — 미수용 benchmark profile(PERF-01·PERF-05~PERF-07·PERF-10~PERF-12·NFR-06)**: [Performance와 verification 스펙 §§4~6](specs/performance-and-verification.md)의 동등 조건, 5회 독립 반복, N-1·failure recovery와 release regression 기준으로 production BP-02 Production Mix와 BP-04 Subscription Stress의 비교·raw evidence를 보관한다. BP-02·BP-04·BP-06 local smoke는 아래 5회 suite로 충족했다. `SPEC-TBD-08` 수치, production workload·storage와 release regression은 남는다. BP-03과 BP-07은 각각 §2.5와 §1.2의 기존 항목에서 추적해 중복하지 않는다. Smoke artifact의 `releaseGating=false`는 release acceptance로 계산하지 않는다.
  - BP-06의 backlog 완료 결함과 불확실한 proposal 복구를 보완했다. 사용자가 채택한 [ADR-0036](decisions/ADR-0036-replicated-proposal-receipts-and-safe-reproposal.md)·[ADR-0037](decisions/ADR-0037-proposal-request-identity.md)·[ADR-0038](decisions/ADR-0038-proposal-receipt-capacity-fences.md)의 복제 receipt와 fence를 활성화했다. 최종 suite `bp06-suite-20261006T163812.777212000Z-92296`는 두 target 모두 5/5 valid였다. NaruMQ gap·QoS 2 duplicate·계획 밖 disconnect·protocol error·overload는 모든 반복에서 0이었다. 원래 5초 deadline·durable ACK·validity는 유지했다. EMQX 5.8.8의 gap 21건과 QoS 2 duplicate 251건은 기존 예외 안이었으며 EMQX 자체 결함은 수정하지 않았다. 전체 local gate도 PASS했다. 이전 0/5 suite는 그대로 보존한다. 세부 증거와 남은 경계는 [성능 §3.6](specs/performance-and-verification.md#36-bp-06-failure-recovery-smoke-관찰-경계)과 [Cluster §11.33](specs/cluster-session-and-delivery.md#1133-안전한-proposal-복구-수용-매핑)에 기록한다.
  - BP-04 local smoke: compare `bp04-20261007T110131.450776700Z-190180`에서 두 target이 모두 valid이고 equivalent였다. `bp04-suite-20261007T110511.057084500Z-196860`는 5/5 valid였고 target 순서를 교차했다. 두 artifact 모두 `releaseGating=false`다. 이 증거는 local smoke에 한정되며 production storage, BP-02 Production Mix, `SPEC-TBD-08` 수치형 기준과 release regression 수용을 대신하지 않는다([성능 §3.7](specs/performance-and-verification.md#37-bp-04-subscription-stress-smoke-관찰-경계)).
  - BP-02 local smoke: compare `bp02-20261007T133602.315971400Z-174004`에서 두 target이 모두 valid이고 equivalent였다. `bp02-suite-20261007T133920.246951900Z-122940`는 교차 순서로 5/5 valid였고 44개 metric 각각의 표본 수는 5였다. 각 target은 setup 49/49(TLS 25·TCP 24), offline session 10/10과 retained 구독자 4명을 확인했다. Missing·duplicate·stale·QoS mismatch·ACK timeout·protocol/transport error는 없었다. 전체 local gate도 PASS했다. 모든 artifact는 `releaseGating=false`다. 이 증거는 production workload·storage, `B_*`, N/N-1 SLO, `SPEC-TBD-08`과 release regression 수용을 대신하지 않는다([성능 §3.8](specs/performance-and-verification.md#38-bp-02-production-mix-smoke-관찰-경계)).
    - 후속 offline oracle 경계 수정: warmup·measure 각각의 실제 발행 수를 합산한다. 세 경계 회귀 사례가 수정 전 FAIL·수정 후 PASS했다. 별도 진단 compare `bp02-20261007T140157.761617200Z-105696`는 두 target 모두 valid였고 offline session 10/10과 exact-topic 기대 수를 확인했다. 수정 뒤 전체 local gate도 다시 PASS했다. 고정 smoke 입력과 production 수용 상태는 유지한다.
- [ ] **미검증 — coverage와 CI merge gate(TEST-06~TEST-10)**: PRD의 전체 line/function/branch 기준은 85/90/80%, 핵심 모듈은 95/95/90%, 변경 코드는 line/function/branch 각각 90%다. [ADR-0040](decisions/ADR-0040-branch-coverage-arm-injection.md)은 branch arm 측정 방식을, [ADR-0041](decisions/ADR-0041-branch-ratchet-noise-tolerance.md)은 overall·core ratchet의 0.20%p 노이즈 허용폭을 정한다. 현재 diff 하한은 [ADR-0044](decisions/ADR-0044-diff-coverage-floor-90.md)를 따른다. 사용자의 요청에 따라 전체 branch 기준을 80%로 낮추고 PRD·policy를 갱신했다. Run C는 10,830/13,073(82.84%)였고 `requirementShortfalls`는 비었다. `coverage: PASS`, `local: PASS`다. 당시 overall branch ratchet 80.03%와 core/diff 기준은 유지했다. [ADR-0042](decisions/ADR-0042-p0-requirement-traceability-gate.md)의 P0 mapping gate와 예외 ratchet을 구현했다. `go run ./tools/verify traceability`는 PASS이며 P0 81개 중 69개 연결, 예외 12개를 보고한다. 예외가 남아 TEST-10은 미검증이다. 필수 CI merge status와 TEST-07 diff CI 증거도 확인하지 않았다. 이전 Run A·B의 85% shortfall과 Run C 증거는 [성능 §8.4](specs/performance-and-verification.md#84-전체-branch-floor-수용-증거), 현재 diff 90% 기준의 실행 결과는 [§8.5](specs/performance-and-verification.md#85-변경-코드-diff-floor-90-수용-증거)에 기록한다.
- [ ] **미검증 — mutation·fuzz·race·flaky gate(TEST-12~TEST-15)**: critical state machine mutation score, packet/topic/session property·fuzz, race detector와 flaky-test owner·해결 기한 기준을 각 필수 CI 실행 증거와 연결한다. 격리된 flaky test가 있으면 owner와 해소 기한이 있어야 한다.
  - **원인 확인·fixture 수정 완료**: `TestNodeCloseFlushesHeldAppendOrReportsFailure/consumed-prior-error`의 실패 주입을 held sync 뒤로 옮기고, 선행 오류 뒤 보관한 append와 미처리 queue가 남는 전제를 assertion으로 확인했다. `unconsumed-prior-error` 시나리오도 추가해 선행 storage 오류를 소비하지 않고 `Close`가 `ErrStorage`와 `os.ErrPermission`을 모두 반환하는지 확인했다. 두 시나리오는 race 시험 50회 반복에서 통과했다. Production `Node.Close`는 변경하지 않았다. 실행 결과와 artifact 위치는 [성능 §8.3](specs/performance-and-verification.md#83-branch-ratchet-노이즈-허용폭-수용-증거)에 있다. 상위 TEST-12~TEST-15의 CI·mutation 검증은 남는다.
  - **두 간헐 실패 원인 수정**: 2026-10-08 local gate의 `TestRaftPeerDriverTicksQueuesAndStops`는 bootstrap 설정 적용 뒤 campaign하도록 시험 준비 순서를 고쳤다. `TestDynamicClusterMembershipTransitionCrashRecovery`와 관련된 release-profile controller는 응답 없는 proposal을 5초 뒤 반환하도록 고쳤다. Driver race 총 300회와 실제 process 회귀 3회가 통과했다. 원인 B의 실제 proposal 유실은 직접 관찰하지 않은 추정이다. 실행 증거는 [성능 §8.6](specs/performance-and-verification.md#86-간헐-실패-원인-수정-수용-증거)에 있다. CI 증거가 남아 상위 항목은 `미검증`으로 유지한다.
  - **TEST-12 로컬 측정·범위 확장**: CI mutation gate에 `mqtt`·`security`를 추가해 [성능 §7](specs/performance-and-verification.md#7-verification-계층) 표의 범위와 맞췄다. 5개 package 모두 efficacy·mutant coverage 80% 이상을 로컬에서 확인했고, `variableByteIntegerSize` 경계 assertion을 보강했다. 기본 mutator 5종을 유지한다. 실행 증거는 [성능 §8.7](specs/performance-and-verification.md#87-mutation-gate-범위-확장-수용-증거)에 있다. CI 증거가 없어 상위 항목은 `미검증`으로 유지한다.

<a id="110-실행-옵션설정-구조"></a>
### 1.10 실행 옵션·설정 구조

설정 형식은 [Process configuration v1](contracts/process-configuration-v1.md), 동작은 [실행 옵션과 설정 스펙](specs/configuration-and-runtime-options.md), 목표 구조는 [ADR-0052](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)(Proposed)를 따른다.

- [ ] **결정 필요 — G1 Cluster MQTT 조정값 부재**: `cmd/narumq/cluster_runtime.go`의 `runClusterMode`는 Maximum Packet Size `1048576`, Receive Maximum `64`, packet read timeout `30s`, CONNECT timeout `10s`, write timeout `5s`, control queue `256`, delivery queue `256`을 고정한다. ADR-0052의 채택 여부에 따라 처리한다.
- [ ] **결정 필요 — G2 Cluster MQTT TLS**: `clusterMQTTListeners`는 `peerTLS` 인증서를 재사용한다. Client CA와 client-auth 설정이 없어 TLS handshake에서 client certificate를 요청하지 않는다. 따라서 cluster의 `mtlsIdentities`를 MQTT TLS handshake에 사용할 수 없다. 이 내용은 코드 판독 결과이며 실행으로 확인하지 않았다.
- [ ] **결정 필요 — G3 평문 TCP 정책 차이**: Standalone `-tcp-listen`은 loopback만 허용한다. Cluster `mqtt.tcpListenAddress`는 유효한 host:port면 loopback 제한 없이 허용한다. [MQTT spec §1 3항](specs/mqtt-protocol-and-session.md#1-listener-계약)의 정책과 맞는지 결정한다.
- [ ] **미구현 — G4 중복 key 검사 차이**: `security_config.go`와 `raft_peer_config.go`는 중복 JSON key를 검사하지 않는다. `cluster_config.go`만 검사한다.
- [ ] **결정 필요 — G5 duration 표기 혼재**: flag와 security config는 Go duration을 쓴다. Cluster config는 `Millis` 접미사가 붙은 정수 millisecond를 쓴다.
- [ ] **미구현 — G6 cluster runtime metrics**: `-runtime-metrics-output`과 `-runtime-metrics-interval`은 standalone에서만 지원한다.
- [ ] **미구현 — G7 설정 조회 API 부재**: 실제 적용 설정을 조회하는 API와 `tenantQuotas`를 바꾸는 Management API가 없다. 이는 전용 console의 선행 조건이다.
- [ ] **결정 필요 — G8 standalone 식별자 고정**: Standalone Management API는 cluster ID `narumq-cluster-1`과 node ID `narumq-node-1`을 사용한다. 출처는 `cmd/narumq/main.go`의 `appmgmt.ServiceConfig`다.
- [ ] **결정 필요 — G9 secret 명령행 노출**: `-http-auth-token`은 secret 값을 명령행으로 받는다. Cluster config는 `clusterAdminTokenFile` 경로를 받는다.
- [ ] **결정 필요 — G10 cluster TLS 설정 부재**: Cluster mode에는 `tls-min-version`과 `tls-client-auth`에 대응하는 설정이 없다. `clusterMQTTListeners`는 peer certificate/key를 쓰고 TLS 1.2를 고정한다.

PRD [§16 선택적 adapter](requirements/PRD.md#16-단계별-제공-범위)의 HTTP webhook·MQTT bridge·Kafka와 DEP-24 ECS는 사용량·필요성 및 별도 수용 기준이 승인되기 전까지 조건부 후속 범위로 둔다. 이를 미구현 결함이나 완료된 기능으로 기록하지 않는다. MQTT-10과 API-07은 각각 §1.6·§1.8에서 따로 추적한다.

항목을 마치면 해당 PRD 요구사항·수용 시험 매핑에 시험 위치와 실행 결과를 연결하고, 영향받은 스펙·계약·ADR·구조 문서의 범위와 상태를 갱신한 뒤 이 목록의 상태를 바꾼다. 실행하지 않은 시험이나 미승인 `Draft`·`Proposed` 내용을 완료로 표시하지 않는다.
