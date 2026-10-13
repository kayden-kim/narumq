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

열린 작업은 **K8s(`EKS_EC2_HA`)·AWS 인프라 시험**, **운영 환경·출시 부하 검증**, **K8s 인프라 불필요 항목**으로 구분한다.

| 구분 | 해당 항목 | 선행 조건 및 검증 경계 |
| --- | --- | --- |
| **K8s(`EKS_EC2_HA`)·AWS 인프라 테스트 필요 (보류·미검증)** | • Linux/EBS 전원·volume 손실 수용 (`CLU-14`, §1.1)<br>• Linux/EBS에서 RPO/RTO 복구 검증 (`SPEC-TBD-09`, §1.1)<br>• K8s External rollout controller (`DEP-11·DEP-18`, §1.2)<br>• K8s 종료 연동과 `EKS_EC2_HA` 배포 산출물 (`DEP-13~23`, §1.4)<br>• Rollout controller 감사 이벤트 producer (`SEC-11`, §1.7)<br>• TEST-10 merge 차단 최종 gate (10 deferred K8s/EKS DEP P0 exceptions, §1.9) | 각 항목에 적힌 로컬 증거는 K8s·클라우드 인프라 수용 시험을 대체하지 않는다. 특히 §1.2 rollout controller와 §1.4 EKS 배포 산출물은 아직 구현·검증 증거가 없다. TEST-10의 CI failure 및 merge rejection probes는 확인했으며, 남은 조건은 deferred K8s/EKS DEP P0 exceptions 해결이다. |
| **운영 환경·출시 부하 검증 필요 (미검증)** | • 운영 Dashboard (`UI-01~05·UI-10`, §1.8)<br>• BP-07 rolling drain 비교 (`PERF-07`, §1.2)<br>• 최대 traffic 연속 100회 rolling (`DEP-01~12`, §1.2)<br>• Production benchmark profile (`PERF-01·05~07·10~12·NFR-06`, §1.9)<br>• 운영 경계 독립 침투 시험 (`NFR-19`, §1.7)<br>• MQTT-13 90일 운영 전환 감시 (`MQTT-13`, §1.6)<br>• TEN-09 production workload·절대 SLO (`§1.9`) | 해당 항목은 로컬 프로세스/Docker smoke 시험을 통과했지만 실제 운영 환경 또는 대상 인프라에서의 검증이 남아 있다. |
| **K8s 인프라 불필요 — 선행 정책·계약 결정** | • 보존 기간·SIEM 접근 권한 (`SEC-11·NFR-17`, §1.7)<br>• Threat model 승인·위험 처분 (`NFR-19`, §1.7)<br>• P1 관리 화면·API 계약·tenant 위임 권한 (`UI-06~09·API-07·TEN-10`, §1.8)<br>• Production workload 수치·성능 합격선 (`SPEC-TBD-08`, §1.9) | 정책·계약 결정 후 해당 입력 기준에 따라 로컬·통합 시험 또는 운영 인프라 검증을 진행한다. |
| **K8s 인프라 불필요 — 로컬·CI·외부 의존성 검증** | • 게시 release별 N-1↔N 호환성 (`CLU-11·NFR-15`, §1.2)<br>• 지원 SDK Server redirect·Will 호환성 (`CLIENT-01~09`, §1.2) | 로컬 프로세스·Docker 호환성 suite, GitHub 원격 CI 설정, 공식 SDK 패치 릴리스 또는 client 인벤토리 조사로 검증한다. |

### 1.1 Production storage와 disaster recovery

2026-10-03 사용자 결정으로 실제 인프라 시험을 보류한다. PRD §18 질문 16의 `EKS_EC2_HA` ARM64 qualification profile은 [ADR-0007](decisions/ADR-0007-aws-production-deployment-platform.md)과 [EKS architecture](architectures/eks-ec2-deployment.md)에 정의했다: EKS `1.37`, `m7g.large` worker 3대(AZ별 1대), broker별 20 GiB `gp3` EBS(3,000 IOPS / 125 MiB/s), NLB `IP` target/TCP listener, broker별 requests=limits `1 CPU`·`4 GiB`. Profile 정의는 production workload·storage 적합성이나 인프라 검증 완료를 뜻하지 않는다. 아래 두 항목이 닫히기 전에는 CLU-14, [ADR-0009 §7](decisions/ADR-0009-raft-production-state-profile.md#7-검증)의 production 행과 [ADR-0003 §9](decisions/ADR-0003-durable-ack-barrier.md#9-검증) 전체를 완료로 표시하지 않는다.

- [ ] **보류 · [K8s/AWS 인프라 테스트 필요]** — Linux/EBS 전원·volume 손실 수용: 지원 Linux filesystem과 Amazon EBS에서 전원 손실 뒤 마지막 durable commit 보존, production WAL torn-tail 판정, volume loss/reseed를 시험한다.
  - 보류 사유: 인프라 시험을 보류했고 시험용 AWS 계정이 없다. Profile은 선택했지만 production workload와 storage 적합성을 검증하지 않았다.
  - 해제 조건: 인프라 시험 재개 결정, 시험 계정·region 확보와 선택한 profile 적용.
  - 로컬 완료 범위(이 gate를 대신하지 않음): Docker/FUSE 장애·power-cut simulation [§11.6](specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping), WAL lifecycle·torn-tail [§11.22](specs/cluster-session-and-delivery.md#1122-wal-compactionsnapshot-retentiontorn-tail-수용-매핑), voter volume 손실 [§11.20](specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑), local ext4 [§11.23](specs/cluster-session-and-delivery.md#1123-local-ext4-filesystem-수용-매핑).
- [ ] **보류 · [선행 결정 + K8s/AWS 인프라 테스트 필요]** — Backup 보존 기간과 disaster-recovery RPO/RTO(`SPEC-TBD-09`): [Cluster §10](specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore) 7항의 보존 기간과 RPO/RTO를 정하고 restore 시간·보존을 시험한다. 현재 backup은 수동 online API와 offline restore만 제공하며 정기 backup과 외부 저장소는 없다.
  - 보류 사유: [PRD §18](requirements/PRD.md#18-미결정-사항) 질문 12의 정책 값이 정해지지 않았다.
  - 해제 조건: 질문 12 정책 확정 및 위 Linux/EBS 실행 환경 확보.

### 1.2 Release와 rollout gate

- [ ] **미검증 · [비인프라 — 로컬/CI 프로세스 검증]** — 게시 release별 historical-source N-1↔N 호환성: 실제 release를 게시할 때마다 직전 게시 release와의 rolling upgrade·rollback·archive restore를 [§11.21](specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑) 방식으로 추가한다. 이때 [Replicated-state v1 §5](contracts/replicated-state-v1.md#5-예제와-계약-테스트contract-test)의 baseline descriptor를 게시 release의 descriptor로 갱신한다. 지정 baseline `58a95f8`만 통과했으며 현재 게시 release tag는 없다.
- [ ] **보류 · 미구현 · [K8s 인프라 테스트 필요]** — External rollout controller(DEP-11·DEP-18): [ADR-0006](decisions/ADR-0006-external-rollout-controller.md)(Proposed)의 `SAFE_TO_STOP` 기반 Pod 삭제 승인을 구현·시험한다. Custom resource와 reconciliation 계약, version preflight, 실패·timeout·unknown barrier에서 현재와 다음 node의 교체 중단을 포함한다([Release contract §8](contracts/cluster-release-compatibility-v1.md#8-blocker와-제외-범위)). Broker 쪽 drain은 로컬 프로세스 시험으로 완료했다([Drain spec §10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)).
  - 보류 사유: Kubernetes 실행 환경이 필요한 인프라 시험이다.
  - 해제 조건: 인프라 시험 재개 결정 및 ADR-0006 채택.
- [ ] **미검증 · [선행 결정 + K8s/인프라 출시 비교 필요]** — BP-07 rolling drain 비교(PERF-07): PRD Phase 3 출시 gate다. Local Docker bridge의 3-node 순차 drain·같은 volume 재시작 harness 구현과 smoke 검증은 완료했다. Readiness LB는 test-only다. 실제 compare `bp07-20261005T031404.679399800Z-37844`의 두 target과 5회 교차 suite `bp07-suite-20261005T032136.047989300Z-30100`의 10개 target 실행이 모두 valid였다(QoS 1/2 gap 0, QoS 2 duplicate 0, 계획 밖 종료 0; [성능 §3.5 실행 증거](specs/performance-and-verification.md#35-bp-07-rolling-drain-smoke-관찰-경계)). Local BP-03 smoke 통과를 대신 쓰지 않으며, release의 수치형 합격 기준은 [성능 §3](specs/performance-and-verification.md#3-benchmark-공통-계약)의 `SPEC-TBD-08`을 따른다.
  - 로컬 실행 증거: 확인 커밋 `fca37620e053a121987c37dd20483fa4f584469f`에 소스·시험 변경을 기록했고 compare·suite run ID와 local gate 결과는 [성능 §3.5 실행 증거](specs/performance-and-verification.md#35-bp-07-rolling-drain-smoke-관찰-경계)에 연결했다(원래 manifest의 `dirty=true` 유지). `clusterPendingCommit.Wait` caller timeout 뒤 FIFO 재생·한 번만 적용·성공 응답 없음은 [Cluster §11.11](specs/cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑)의 in-process 3-node runtime 및 TCP 오류 시험으로 확인했다.
  - 남은 조건: `SPEC-TBD-08`의 release 수치 결정과 production 배포 환경에서의 기준 검증이 남아 있다.
- [x] **완료(로컬 프로세스 100회) / [ ] 미검증(K8s·운영 최대 부하)** — 최대 traffic 연속 100회 rolling: 3-node 동적 클러스터 프로세스에서 MQTT 3.1.1/5.0·QoS 0/1/2·retained·Will·persistent session 혼합 100회 연속 rolling drain(`TestDynamicClusterContinuousRollingDrainProcessAcceptance`)을 검증했다. 시험은 `NARUMQ_ROLLING_100=1`로 실행한다([Drain spec §9·§10](specs/drain-and-rolling-update.md#9-수용-시험)). 실제 운영 최대 부하와 외부 Kubernetes rollout controller 연동 검증은 남는다.
- [ ] **미검증 · [비인프라 — 외부 SDK 패치 대기 + 로컬 호환성 스위트]** — 지원 SDK의 Server redirect·Will 호환성: [Client contract §6.2](contracts/mqtt-client-compatibility-v1.md#62-drain-compatibility-suite)의 `drain-paho-go-python-will-acl-final`은 9/10이다. Paho Go MQTT 5 `v0.23.0`의 `go-mqtt5-malformed-duplicate-server-reference`가 `serverReferenceAccepted=true`, `trap_accept=1`로 실패했다. 두 SDK의 callback adapter redirect·session resume·후속 전달·Will 억제와 Python malformed 4종은 통과했다([Drain spec §10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)). Artifact는 `.artifacts/compatibility/mqtt-clients-v1/drain-paho-go-python-will-acl-final/`이다. 범위는 test-only readiness LB와 시간 값이며 `SPEC-TBD-04` 운영 수치와 rollout timing은 제외한다. Will 사유별 broker 계측은 [Drain spec §5](specs/drain-and-rolling-update.md#5-will-판정과-관측)에서 완료했다.
  - 원인 및 결정: Paho `Properties.Unpack`이 중복 Server Reference를 검사하지 않고 마지막 값으로 덮어쓴다. 공식 SDK만 유지하며 직접 패치한 SDK와 raw socket 검사는 도입하지 않는다.
  - 해제 조건: 이 결함을 고친 공식 SDK release가 배포되면 version 변경을 검토한 뒤 drain 10개와 Phase 1 26개 case를 모두 로컬에서 통과한다.
- [x] **완료(로컬 프로세스 범위)** — 교체형 업데이트 조합: [ADR-0051](decisions/ADR-0051-cluster-update-strategy.md)의 새 node 추가·승격 → 기존 node drain·`SAFE_TO_STOP` → planned removal을 3-node process(`TestDynamicClusterReplacementRollingUpdateProcessAcceptance`)로 확인했다([Drain spec §10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)). Production 배포 profile과 N-1↔N 혼합 교체는 위 K8s·release 항목에서 별도로 검증한다.

### 1.3 Cluster 기능과 알려진 구현 한계
연결·session rebalance(CLU-13)는 [Cluster §9](specs/cluster-session-and-delivery.md#9-rebalance)와 [ADR-0033](decisions/ADR-0033-connection-rebalance-operation.md)의 수동 일회성 operation 범위에서 완료했다. 별도 production 성능 수용은 완료 범위에 포함하지 않는다.

### 1.4 Drain 정책·관측과 production 배포

- [x] **완료(제품 기본값) / [ ] 미검증(K8s·NLB 실측)** — Drain·reconnect 운영값(SPEC-TBD-04): [ADR-0055](decisions/ADR-0055-drain-and-reconnect-operating-defaults.md)와 [Drain spec §2·§7](specs/drain-and-rolling-update.md#2-drain-operation-resource)에 제품 기본값(drain 100/s, LB 대기 30s, timeout 600s, 최소 grace 660s, reconnect 1~30s·10회)을 반영하고 생략 시 기본값 적용을 구현했다. 실제 NLB deregistration·SIGTERM 종료 시간 실측과 Kubernetes 배포 admission 검증은 아래 K8s 배포 항목에서 함께 수행한다.
- [ ] **보류 · 미구현 · [K8s/AWS 인프라 테스트 필요]** — Kubernetes 종료 연동과 `EKS_EC2_HA` 배포 artifact(DEP-13~DEP-23): [Drain spec §7·§7.1](specs/drain-and-rolling-update.md#7-external-rollout-gate와-kuberneteseks), [ADR-0007 §7](decisions/ADR-0007-aws-production-deployment-platform.md#7-검증)과 [EKS architecture](architectures/eks-ec2-deployment.md)의 목표 범위다. `preStop`의 idempotent drain 호출·결과 기록, grace period 검증, PDB·3-AZ placement·교체 동시성, stable node identity와 EBS 결합, NLB target lifecycle 및 profile별 지원 상태를 배포 artifact에 연결한다. Controller 구현은 §1.2, storage 전원·volume 시험은 §1.1에서 추적한다.
  - 보류 사유: 인프라 시험 보류 중이며 배포 artifact와 구현 증거가 없다. Profile은 [ADR-0007](decisions/ADR-0007-aws-production-deployment-platform.md)과 [EKS architecture](architectures/eks-ec2-deployment.md)에 정의했지만 qualification은 미검증이다.
  - 해제 조건: 인프라 작업 재개와 배포 결정 채택, PRD §18 질문 10의 profile 확정 및 실행 환경 확보. 질문 16 ARM64 profile은 정의되어 있다.
  - K8s 인프라 검증 항목: hook 중복·실패·grace 부족, NLB 전파 중 신규 연결, Pod·EC2·단일 AZ 손실, EBS 재연결과 연속 rolling upgrade·rollback을 실제 `EKS_EC2_HA` 환경에서 시험한다. Broker process의 `SAFE_TO_STOP` 통과로 대신하지 않는다.

### 1.5 설계 미결정과 비지원 범위

[Cluster architecture의 미결정 사항](architectures/cluster-architecture.md#미결정-사항)은 과거 target과 현재 구현을 함께 언급한다. 아래 경계는 새 기능의 구현 승인이 아니며, 현재 지원 범위를 넓힐 때 먼저 결정해야 할 사항이다.
- Route-index 물리적 배치와 rebuild 방식: 현재 node-local projection rebuild는 [Cluster §11](specs/cluster-session-and-delivery.md#11-수용-시험)의 수용 범위에서 완료했다. Architecture의 물리적 배치 질문을 기존 rebuild 미구현으로 다시 열지 않는다. 별도 분산 배치나 rebuild 방식 변경이 필요하면 [ADR-0009 §1.4](decisions/ADR-0009-raft-production-state-profile.md#14-consensus-group-partition-profile)의 단일 group·node-local projection과의 관계, ordering과 routing readiness를 먼저 확정한다.
- Active peer transport 밖 RPC encoding: 현재 [Raft membership v2](contracts/raft-membership-v2.md)와 live QoS 0 forwarding은 구현·수용 범위가 있다. 그 밖의 RPC를 도입할 때 producer/consumer, message 형식·한도·인증·version 호환 계약을 먼저 정한다. Persisted schema 계약은 [Replicated-state v1](contracts/replicated-state-v1.md)에서 완료했으며 미래 major migration은 별도 계약을 먼저 정한다.
- Cross-cluster migration과 3-bootstrap-voter identity 제한 밖 restore: [Cluster §10](specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore)의 현재 비지원 범위다. V1/v2 archive, learner를 포함한 archive와 voter-set 변경도 현재 restore로 지원하지 않는다. 별도 migration·identity 계약의 채택 없이 자동으로 구현 백로그에 넣거나 CLU-14의 현 수용 범위로 포함하지 않는다.

### 1.6 MQTT protocol과 client 호환성

- [ ] **미검증 · [비인프라 — 운영 전환 조사·감시]** — MQTT 3.1.0 production 전환(MQTT-13): [PRD §9.1](requirements/PRD.md#91-mqtt-프로토콜)의 90일 이상 client 사용량 조사, 오래된 firmware·외부 장비 소유자 확인, 3.1.1 이상 전환 또는 별도 gateway 격리와 canary 거절 Metric 감시를 완료한다. 브로커의 wire-level 거절 구현·로컬 시험은 완료된 범위이며 이 항목은 운영 전환 gate다.
- [x] **완료** — WebSocket/WSS listener(MQTT-10): [ADR-0057](decisions/ADR-0057-mqtt-websocket-listener.md), [MQTT spec §8.3](specs/mqtt-protocol-and-session.md#83-websocketwss-수용-매핑), [Configuration spec §6](specs/configuration-and-runtime-options.md#6-수용-시험)에 구현과 수용 검증을 기록했다.

### 1.7 Tenant·security와 감사

- [x] **완료(로컬 수용)** — TEN-09 noisy-neighbor 완화: [ADR-0054](decisions/ADR-0054-tenant-noisy-neighbor-latency-acceptance.md) · [Tenant와 security §9.9](specs/tenancy-and-security.md#99-ten-09-noisy-neighbor-실행-증거). Production workload와 절대 SLO는 §1.9에서 추적한다.
- [x] **완료** — SEC-05 MQTT username/password 인증: [ADR-0047](decisions/ADR-0047-mqtt-password-authentication.md)과 [Tenant와 security §9.5](specs/tenancy-and-security.md#95-sec-05-password-authentication-실행-증거)에 구현·검증을 기록했다.
- [x] **완료** — SEC-07 MQTT credential 회전·폐기와 만료 종료: [ADR-0049](decisions/ADR-0049-mqtt-credential-revocation.md), [운영 가이드](how-to/credential-rotation-and-revocation.md)와 [Tenant와 security §9.7](specs/tenancy-and-security.md#97-sec-07-credential-회전폐기-실행-증거)에 구현·검증을 기록했다.
- [x] **완료** — SEC-10 Management API 인증·RBAC·감사 조회: [ADR-0048](decisions/ADR-0048-management-api-oidc-api-key-rbac.md)과 [Tenant와 security §9.6](specs/tenancy-and-security.md#96-sec-10-실행-증거)에 구현·검증을 기록했다.
- [x] **완료** — SEC-12 MQTT client·identity·IP blocklist: [ADR-0050](decisions/ADR-0050-mqtt-client-blocklist.md)와 [Tenant와 security §9.8](specs/tenancy-and-security.md#98-sec-12-blocklist-실행-증거)에 구현·검증을 기록했다.
- [ ] **부분 구현 · [K8s 연동 + 비인프라 결정 혼합]** — 감사 운영 범위(SEC-11·NFR-17·API-08): Management plane은 [ADR-0039](decisions/ADR-0039-management-audit-log.md)에 따라 활성 mutation 10개, 401·403 거절, node-local hash chain과 `-audit-verify`를 구현했다. MQTT CONNECT·Will·PUBLISH·SUBSCRIBE·UNSUBSCRIBE 거절은 [ADR-0046](decisions/ADR-0046-mqtt-data-plane-denial-audit.md)에 따라 bounded fail-open recorder로 구현하고 [Tenant와 security 스펙 §9](specs/tenancy-and-security.md#9-수용-시험과-범위)에 시험과 실행 증거를 연결했다. SIEM consumer의 unknown event/action/reason preservation도 [Audit event v1 계약](contracts/audit-event-v1.md)과 [Tenant와 security 스펙 §9](specs/tenancy-and-security.md#9-수용-시험과-범위)에서 검증했다. 남은 작업은 다음과 같이 구분한다.
  - **K8s 인프라 테스트 필요**: 외부 rollout controller의 감사 이벤트 producer 구현·검증(§1.2 controller 작업과 연동).
  - **비인프라 정책 결정**: 감사 로그 보존 기간과 SIEM 접근 권한 확정.
- [ ] **미검증 · [선행 승인 + K8s/운영 인프라 테스트 필요]** — 보안 출시 gate(NFR-19): [PRD §11.4·§15.4](requirements/PRD.md)의 threat model과 tenant-isolation penetration test를 release 전에 수행한다.
  - 로컬 완료 범위: 자동 사전 침투 시험과 [threat model 초안](architectures/threat-model.md)을 완료했다([Tenant와 security §9.3](specs/tenancy-and-security.md#93-nfr-19-자동-penetration-test-실행-증거)).
  - 비인프라 선행 결정: Threat model 승인, `R-01·R-02`·`R-05~R-07` 해결 또는 위험 승인.
  - K8s/운영 인프라 테스트 필요: 로컬 loopback 시험 범위 밖인 NLB, TLS termination, secret 저장소, Kubernetes 경계를 포함한 독립 penetration test 수행.

### 1.8 Management API·observability와 Dashboard

- [x] **완료** — API-09 GitOps/runtime source of truth (`mqttLimits` 범위): [Configuration and runtime options §6](specs/configuration-and-runtime-options.md#api-09-mqttlimits-execution)에 구현·검증을 기록했다. [ADR-0052](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)는 Proposed overall이며, 나머지 policy domain은 채택된 첫 구현 범위 밖이다.
- [x] **완료** — API-05 rate limit: [ADR-0043](decisions/ADR-0043-management-api-rate-limit.md)과 [Management API 스펙 §1.5](specs/management-api-and-observability.md#15-rate-limit)로 구현·시험했다.
- [x] **완료(로컬 수용) / [ ] 미검증(K8s·운영 배포)** — UI-01~UI-05·UI-10 운영 Dashboard: [ADR-0045](decisions/ADR-0045-operations-dashboard-grafana.md), [Management API 스펙 §8·§9](specs/management-api-and-observability.md#8-dashboard-계약), [운영 절차](how-to/operations-dashboard.md), `tests/integration/dashboard_stack_test.go`로 구현·시험했다. 2026-10-09 static contract와 local Docker process acceptance가 통과했다.
  - **K8s/운영 인프라 테스트 필요**: 운영 클러스터 환경에서 Prometheus scrape, Grafana datasource·패널, 경보가 정상 동작하는지 배포 검증이 남아 있다([PRD §15.2 10항](requirements/PRD.md)).
- [ ] **결정 필요 · [비인프라 — 선행 계약·권한 결정 후 로컬 검증]** — P1 관리 화면·exact-topic Metric(UI-06~UI-09·API-07·TEN-10): client 검색(PRD `UI-06`의 IP 검색 요구와 현행 API의 IP 필터 금지 충돌 해소 포함)·상세 표현, disconnect/redirect API, retained 삭제·exact-topic Metric 계약과 delegated tenant 권한(`TEN-10`)은 우선순위, API 계약, UI 제공 순서를 먼저 확정한다. 채택한 범위만 구현하고 권한·마스킹·pagination·idempotency·감사 기준을 시험한다. 아직 deferred인 API를 호출하는 화면은 구현된 제품 동작으로 표시하지 않는다.

### 1.9 Performance·test와 공통 출시 gate

- [ ] **결정 필요 · [비인프라 — 선행 수치·입력 결정]** — production workload와 성능 합격선(NFR-01~NFR-06·PERF-01~PERF-12): [PRD §18 질문 1~5·7~10·13](requirements/PRD.md#18-미결정-사항)의 production workload 값과 [성능 §3.4](specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)의 `B_conn`, `B_pub`, `B_sub`, payload·topic 분포, tenant 부하 등을 확정한다. EMQX Enterprise `6.3.1` comparator와 §1.1의 `EKS_EC2_HA` qualification profile은 선택했지만, 질문 14의 95%/105% comparator ratio 채택 및 `SPEC-TBD-08` cutoff는 미결정이다. `SPEC-TBD-08`은 §1.2의 BP-07 release gate와도 연결되며, BP-03 smoke 완료는 전체 workload/SLO 결정을 대신하지 않는다.
- [ ] **미검증 · [K8s/AWS 인프라 벤치마크 테스트 필요]** — 미수용 benchmark profile(PERF-01·PERF-05~PERF-07·PERF-10~PERF-12·NFR-06): [Performance와 verification 스펙 §§4~6](specs/performance-and-verification.md)의 동등 조건, 5회 독립 반복, N-1·failure recovery와 release regression 기준으로 production BP-02 Production Mix와 BP-04 Subscription Stress의 비교·raw evidence를 보관한다. BP-03과 BP-07은 각각 [성능 §3.4](specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)와 §1.2에서 추적한다.
  - 로컬 완료 범위(모든 smoke artifact는 `releaseGating=false`이며 release 수용을 대신하지 않음):
    - BP-06 local smoke: [ADR-0036](decisions/ADR-0036-replicated-proposal-receipts-and-safe-reproposal.md)·[ADR-0037](decisions/ADR-0037-proposal-request-identity.md)·[ADR-0038](decisions/ADR-0038-proposal-receipt-capacity-fences.md) 적용 뒤 최종 suite `bp06-suite-20261006T163812.777212000Z-92296`에서 두 target 모두 5/5 valid를 확인했다([성능 §3.6](specs/performance-and-verification.md#36-bp-06-failure-recovery-smoke-관찰-경계), [Cluster §11.33](specs/cluster-session-and-delivery.md#1133-안전한-proposal-복구-수용-매핑)).
    - BP-04 local smoke: compare `bp04-20261007T110131.450776700Z-190180` 및 5회 교차 suite `bp04-suite-20261007T110511.057084500Z-196860` 5/5 valid 통과([성능 §3.7](specs/performance-and-verification.md#37-bp-04-subscription-stress-smoke-관찰-경계)).
    - BP-02 local smoke: compare `bp02-20261007T133602.315971400Z-174004`, 5회 교차 suite `bp02-suite-20261007T133920.246951900Z-122940` 5/5 valid, offline oracle 경계 보정 compare `bp02-20261007T140157.761617200Z-105696` 통과([성능 §3.8](specs/performance-and-verification.md#38-bp-02-production-mix-smoke-관찰-경계)).
  - K8s/AWS 인프라 검증 항목: 확정된 `SPEC-TBD-08` 수치, 실제 production workload, AWS/EKS/EBS storage 환경에서 N/N-1 SLO 및 release regression을 검증한다.
- [x] **완료** — coverage와 원격 CI gate(TEST-06~TEST-09): main commit `03c84f40c345195f0a323c5af2e623e9ebbbc998`의 원격 `verify` run [38112386284](https://github.com/kayden-kim/narumq/actions/runs/38112386284) PASS로 확인했다([성능 §8.9](specs/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거), [§8.4](specs/performance-and-verification.md#84-전체-branch-floor-수용-증거), [§8.5](specs/performance-and-verification.md#85-변경-코드-diff-floor-90-수용-증거)).
- [ ] **미검증 · [K8s/EKS DEP P0 예외 해제 필요]** — merge 차단(TEST-10): [ADR-0056](decisions/ADR-0056-branch-protection-for-admins.md)에 따라 `enforce_admins=true`를 영구 적용했다. 4개 defect probe PR에서 CI failure와 관리자 merge API 거절(HTTP 405)을 확인했다([성능 §8.9](specs/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거)). 남은 조건은 §1.2·§1.4의 10개 deferred K8s infrastructure DEP P0 exceptions를 닫는 것이다.
- [x] **완료** — fuzz·race·flaky CI gate(TEST-13~TEST-15): 원격 `verify` run [38112386284](https://github.com/kayden-kim/narumq/actions/runs/38112386284)과 간헐 실패 원인 수정([성능 §8.3](specs/performance-and-verification.md#83-branch-ratchet-노이즈-허용폭-수용-증거), [§8.6](specs/performance-and-verification.md#86-간헐-실패-원인-수정-수용-증거), [§8.9](specs/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거))으로 확인했다.
- [x] **완료** — TEST-12 수동 mutation 검증: `go run ./tools/verify mutation` 5개 package 모두 80% 이상 통과([성능 §8.8](specs/performance-and-verification.md#88-수동-mutation-검증-수용-증거), [§8.7](specs/performance-and-verification.md#87-mutation-gate-범위-확장-수용-증거)).

<a id="110-실행-옵션설정-구조"></a>
### 1.10 실행 옵션·설정 구조

설정 형식은 [Process configuration v1](contracts/process-configuration-v1.md), 동작은 [실행 옵션과 설정 스펙](specs/configuration-and-runtime-options.md), 나머지 목표 구조는 [ADR-0052](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)(Proposed)를 따른다. ADR-0052 §1.1.1의 G1 `mqttLimits`, §1.5의 Cluster MQTT TLS(G2·G10), 평문 TCP 정책(G3), duration 표기 정책(G5), standalone 고정 식별자(G8)를 채택했다.

- [x] **완료 — G1 Cluster MQTT 조정값**: [ADR-0052 §1.1.1](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#111-1차-구현-계약-mqttlimits) · [API-09 실행 기록](specs/configuration-and-runtime-options.md#api-09-mqttlimits-execution).
- [x] **완료 — G2 Cluster MQTT TLS**: [ADR-0052 §1.5](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#15-형식과-cluster-mqtt-tls) · [Cluster MQTT TLS 실행 기록](specs/configuration-and-runtime-options.md#cluster-mqtt-tls-g2-g10-execution).
- [x] **완료 — G3 평문 TCP 정책**: [ADR-0052 §1.5](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#15-형식과-cluster-mqtt-tls) · [MQTT spec §1](specs/mqtt-protocol-and-session.md#1-listener-계약) · [설정 스펙 §6](specs/configuration-and-runtime-options.md#6-수용-시험) `CFG-SCN-29`.
- [x] **완료 — G4 중복 key 검사**: [ADR-0052 §1.5](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#15-형식과-cluster-mqtt-tls) · [설정 스펙 §6](specs/configuration-and-runtime-options.md#6-수용-시험).
- [x] **완료 — G5 duration 표기 통일**: [G5 실행 기록](specs/configuration-and-runtime-options.md#g5-duration-millis-execution).
- [x] **완료 — G6 cluster runtime metrics**: [Cluster runtime metrics 실행 기록](specs/configuration-and-runtime-options.md#cluster-runtime-metrics-g6-execution).
- [x] **완료 — G7 적용 설정 조회와 ClusterPolicy tenant quota**: [설정 스펙 §6](specs/configuration-and-runtime-options.md#6-수용-시험) · [G7 실행 기록](specs/configuration-and-runtime-options.md#g7-config-view-and-tenant-quotas-execution).
- [x] **완료 — G8 standalone 식별자 고정**: [ADR-0052 §1.6](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#16-standalone과-console) · [설정 스펙 §6](specs/configuration-and-runtime-options.md#6-수용-시험) `CFG-SCN-39`.
- [x] **완료 — G9 secret 파일 flag**: [ADR-0052 §1.5](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md#15-형식과-cluster-mqtt-tls) · [설정 스펙 §6](specs/configuration-and-runtime-options.md#6-수용-시험).
- [x] **완료 — G10 cluster TLS 설정**: [Cluster MQTT TLS 실행 기록](specs/configuration-and-runtime-options.md#cluster-mqtt-tls-g2-g10-execution). ADR-0052의 나머지 범위는 `Proposed`로 남는다.

PRD [§16 선택적 adapter](requirements/PRD.md#16-단계별-제공-범위)의 HTTP webhook·MQTT bridge·Kafka와 DEP-24 ECS는 사용량·필요성 및 별도 수용 기준이 승인되기 전까지 조건부 후속 범위로 둔다. 이를 미구현 결함이나 완료된 기능으로 기록하지 않는다. MQTT-10과 API-07은 각각 §1.6·§1.8에서 따로 추적한다.

항목을 마치면 해당 PRD 요구사항·수용 시험 매핑에 시험 위치와 실행 결과를 연결하고, 영향받은 스펙·계약·ADR·구조 문서의 범위와 상태를 갱신한 뒤 이 목록의 상태를 바꾼다. 실행하지 않은 시험이나 미승인 `Draft`·`Proposed` 내용을 완료로 표시하지 않는다.
