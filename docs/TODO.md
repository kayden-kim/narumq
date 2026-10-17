# PRD 구현 잔여 작업

이 문서는 [PRD](requirements/PRD.md)의 열린 구현·검증·선행 결정만 추적한다. 완료 기록과 실행 증거는 [검증 기록](verification/README.md)에서 확인한다. 완료 판정은 채택된 스펙·계약·ADR의 해당 범위를 따른다. PRD의 `Draft` 표시는 구현 승인을 뜻하지 않는다.

최신 대조일: 2026-10-17. 해결되지 않은 실패와 검증 한계는 아래 해제 조건에 반영한다.

| 표기 | 뜻 |
| --- | --- |
| `[ ]` **미구현·부분 구현** | 필요한 구현이 없거나 일부만 있다. |
| `[ ]` **미검증** | 구현 또는 제한된 통과 증거는 있으나 해당 수용 시험이 남았다. |
| `[ ]` **결정 필요·보류** | 정책·운영 데이터·외부 release 등 선행 조건을 먼저 해결해야 한다. |

완료한 작업은 이 목록에서 제거한다. 시나리오와 기대 결과는 기준 스펙에 남긴다. 테스트 연결과 결과는 [테스트 컨벤션](conventions/test-conventions.md)에 따라 생성한 시나리오 보고서에서 확인한다. 과거 실행 증거는 [검증 기록](verification/README.md)에 남긴다. 부분 검증은 남은 범위와 해제 조건을 함께 적는다. 과거 링크를 유지하려고 절 번호는 보존한다.

## 실행 환경과 우선순위

기존 EKS와 이후 합성 시험 환경은 각각의 [최종 폐기 기록](../.artifacts/verification/eks-final-20261014T221024Z/final-decommission-summary.json), [재개 실행·정리 기록](../.artifacts/verification/eks-resume-20261015T000337Z-21df3b/final-summary.json), [마지막 EC2 시험 정리 기록](../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/final-summary.json)에서 삭제를 확인했다. 보호 snapshot과 공유 image는 유지한다. 이 기록을 현재 실행 중인 환경이나 전체 AWS 비용 0의 증거로 쓰지 않는다.

새 인프라 시험은 승인된 별도 state·VPC의 격리 합성 fixture에서만 수행한다. 보호 자원은 바꾸지 않는다. 필요한 증거를 보존하고 시험 후 새 전용 자원을 정리한다. 운영 데이터나 시간 경과가 필요한 시험을 기다리려고 EKS를 계속 유지하지 않는다.

새 인프라 시험은 아래 열린 항목 중 EKS에서만 확인할 수 있는 범위부터 선택한다. 운영 부하·보존 기간이 필요한 항목은 해당 선행 조건을 갖춘 뒤 실행한다. 이미 수용한 결함 matrix를 새 미구현 항목으로 다시 열지 않는다.

## 1. 열린 작업

### 1.1 Production storage와 disaster recovery

- [ ] **부분 검증 · K8s/AWS·지속 운영 관찰** — 백업 보존·재해 복구(`SPEC-TBD-09`). 일 1회 백업, 30일 보존, RPO 24시간·전체 RTO 1시간이 기준이다. 실제 S3·IRSA·새 PVC 세 개의 합성 전체 복원은 통과했다. 남은 범위는 지속적인 일일 백업 RPO, 실제 30일 경과 보존, production 데이터 크기와 업로드 실패 matrix다. [ADR-0058](decisions/ADR-0058-cluster-backup-s3-retention-and-restore.md), [Cluster §11.9](specs/cluster-session-and-delivery.md#119-offline-queue-replication과-backup-v3-수용-매핑)를 따른다.

### 1.2 Release와 rollout gate

- [ ] **미검증 · 게시 release 대기** — 게시 N-1↔N release로 upgrade·rollback·archive restore를 검증하고 baseline descriptor를 갱신한다(`CLU-11·NFR-15`). [Rollout controller contract §3](contracts/rollout-controller-v1.md#3-acceptance-matrix)의 게시 release 행도 같은 barrier로 판정한다. 지정 historical baseline 통과는 게시 release 전체의 수용을 대신하지 않는다. [Cluster §11.21](specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑)을 따른다.
- [ ] **미검증 · 운영 부하 선행** — BP-07 rolling drain 비교(`PERF-07`). 로컬 비교와 반복 suite의 제한된 통과는 유지한다. `SPEC-TBD-08` 수치형 합격 기준과 production 배포 환경에서의 비교가 필요하다. [성능 §3.5](specs/performance-and-verification.md#35-bp-07-rolling-drain-smoke-관찰-경계)를 따른다.
- [ ] **미검증 · 운영 최대 부하 선행** — 실제 `B_conn/B_pub/B_sub`, QoS·payload·session·tenant·TLS profile과 절대 cutoff를 확정한 뒤 운영 배포 profile에서 연속 100회 rolling을 검증한다(`DEP-01~12`). 로컬 process 시험은 운영 수용을 대신하지 않는다. [Drain §9·§10](specs/drain-and-rolling-update.md#9-수용-시험)을 따른다.
- [ ] **부분 검증 · 운영 rollout 수용 증거 필요** — Server redirect·Will 호환성(`CLIENT-01~09`). Phase 1과 정상 SDK drain 연동은 검증했다. 남은 범위는 production rollout timing과 배포 profile의 수용 증거다. Fake server의 malformed 응답 8개는 선택적 SDK 진단이며 제품 승인을 차단하지 않는다. Go 중복 Server Reference 진단 실패는 보존하되 공식 SDK 수정 release를 정상 drain 승인의 선행 조건으로 두지 않는다. [Client contract §6.2·§6.3](contracts/mqtt-client-compatibility-v1.md#62-drain-compatibility-suite)를 따른다.

### 1.3 Cluster 기능과 알려진 구현 한계

현재 열린 부하 검증은 §1.9에서 추적한다. 지원 기능 범위는 [Cluster 스펙](specs/cluster-session-and-delivery.md)을 따른다.

### 1.4 Drain 정책·관측과 production 배포

- [ ] **부분 검증 · K8s/EKS** — Kubernetes 종료 연동과 `EKS_EC2_HA` 전체 수용(`DEP-13~23`). 남은 범위는 preStop/drain hook 실패·timeout·grace expiry·부족한 grace 거절·강제 종료 경보, 정상 MQTT traffic의 NLB routing·기존 연결 drain·idle timeout·client IP·connection termination, 모델 밖의 AZ 장애(network·EBS 서비스·control plane)와 반복 실행, 게시 release의 배포 profile 지원 상태다. 로컬 descriptor 자동 검증과 외부 수용 하네스 추가는 [후속 기록](verification/drain-and-rolling-update.md#6-gate-연결과-release-profile-경로-보완-2026-10-17)에 있다. [Drain §7·§10](specs/drain-and-rolling-update.md#7-external-rollout-gate와-kuberneteseks), [EKS architecture](architectures/eks-ec2-deployment.md)를 따른다. 게시 release와 운영 부하 수용은 §1.2에서 추적한다.

### 1.5 설계 미결정과 비지원 범위

[Cluster architecture의 미결정 사항](architectures/cluster-architecture.md#미결정-사항)은 새 구현 승인이 아니다. Route-index의 물리적 분산 배치, 기존 관측 RPC 밖의 새 RPC, cross-cluster migration·voter-set 변경 복원은 별도 계약 채택 전에는 착수하지 않는다. 현재 구현한 route rebuild·관측 RPC를 다시 미완료로 분류하지 않는다.

### 1.6 MQTT protocol과 client 호환성

MQTT 3.1.0은 지원 대상이 아니다. Client의 관찰·전환·격리는 제품 출시 gate가 아니라 운영자의 이전 절차다. [PRD §9.1](requirements/PRD.md#mqtt-310-비지원과-이전-정책)을 따른다.

### 1.7 Tenant·security와 감사

- [ ] **미검증 · 독립 보안 시험 필요** — 운영 경계 침투 시험(`NFR-19`, `R-07`). 채택한 threat model과 위험 승인, 로컬 자동 사전 시험은 유지한다. NLB·TLS termination·secret 저장소·Kubernetes를 포함한 독립 시험이 남았다. [Threat model](architectures/threat-model.md)을 따른다.

### 1.8 Management API·observability와 Dashboard

- [ ] **보류 · 출시 후 조건부 범위** — P1 관리 화면·제어 API·exact-topic Metric·tenant 위임(`UI-06~09·API-07·TEN-10`). 현 출시에서는 Grafana P0 조회 대시보드를 유지한다. 재개할 때 우선순위·API 계약·UI 순서와 권한·마스킹·pagination·idempotency·감사 기준을 먼저 확정한다. [ADR-0045](decisions/ADR-0045-operations-dashboard-grafana.md)를 따른다. 완료한 P0 대시보드의 EKS 통합은 다시 열지 않는다.

### 1.9 Performance·test와 공통 출시 gate

- [ ] **Pod 장애 보완 검증 · local smoke 차단·B_* 미완료** — Production workload와 절대 cutoff(`NFR-01~06·PERF-01~12`, `SPEC-TBD-08`). 2026-10-16 사용자 결정에 따라 상용 EMQX Enterprise 라이선스를 요구하는 상대 비교 조건은 필수 제품 출시 게이트에서 제외하고 선택적 외부 벤치마크로 운영한다. 필수 릴리스 게이트는 NaruMQ 자체의 5% 회귀 차단 규칙과 `EKS_EC2_HA` qualification profile에서 측정한 N-1 자립형 수용 용량(`B_*`)으로 판정한다. Quota 계열 거절의 원인·구간 분류와 기록은 구현했다([§3.9](specs/performance-and-verification.md#quota-계열-거절-판정)). 과거 EKS 거절 3건은 Reason String이 없어 `unclassified`이며 재분류하지 않는다. 현재 차단 사항은 주소 대체 수정 뒤 원래 local-smoke profile로 실행한 2 cell 확인 단계 `02-r2`의 clean-session loss 1건(최대 나이 1,425.6ms > 1,000ms)이다([최신 summary](../.artifacts/verification/capacity-local-20261017T130729Z/capacity-summary.json)). 앞선 `02-r1` setup의 CONNACK `0x88` 2건·handshake timeout 8건 실패도 보존한다([과거 summary](../.artifacts/verification/capacity-local-20261017T123309Z/capacity-summary.json), [연결 기록](../.artifacts/verification/capacity-local-20261017T123309Z/steps/02-r1/connections.jsonl)). 진단에서 확인한 CONNECT 송신 뒤 주소 대체 결함은 수정했지만 원래 setup 거절의 전체 원인으로 확정하지 않았다([진단 기록](verification/performance-and-verification.md#capacity-connect-fallback-diagnosis)). PRF-SCN-062의 모든 단계 통과와 원격 CI 통과를 확인하기 전에는 EKS를 진행하지 않는다. 이후 남은 범위는 새 binary를 올린 EKS 전용 generator Job의 `B_*` 측정·SLO 확정이다. [성능 §3.9](specs/performance-and-verification.md#39-bp-02-eks-용량-측정-계약)를 따른다.
- [ ] **미검증 · K8s/AWS 출시 비교** — Production benchmark profile(`PERF-01·05~07·10~12·NFR-06`). 상용 EMQX 라이선스 비교는 선택적 외부 벤치마크로 운영하며, 필수 릴리스는 NaruMQ 자체 N-1 ↔ N 회귀(5% 미만) 및 failure recovery(BP-06)·rolling drain(BP-07) 기준으로 검증한다. [성능 §§3~6](specs/performance-and-verification.md#3-benchmark-공통-계약)을 따른다.
- [ ] **미검증 · 원격 CI** — PR [#9](https://github.com/kayden-kim/narumq/pull/9) head `c8d9b06`의 [run 39036288101](https://github.com/kayden-kim/narumq/actions/runs/39036288101)은 `ci --base origin/main`의 coverage gate에서 실패했다. 당시 diff 하한은 90.00%였고 diff branch 70.11%, diff line 80.08%가 미달했다. 2026-10-17 사용자 결정으로 diff 하한이 80%로 바뀌었다([ADR-0063](decisions/ADR-0063-diff-coverage-floor-80.md)). 같은 산출물을 새 하한으로 재평가하면 diff line은 통과하고 diff branch는 계속 미달한다. 전체 line 86.19%·branch 81.93%였다. 같은 실행의 시나리오 실행 검사는 634개 중 통과 512·실패 0·범위 안 미실행 0(게이트 밖 122)이었고 Python 659개는 OK였다. 남은 범위는 diff branch를 80.00% 이상으로 올린 뒤 새 원격 실행으로 확인하는 것이다. 2026-10-17 local gate 기록은 [Gate 보완 기록](verification/performance-and-verification.md#12-gate-실행-경로판정-보완-2026-10-17)을 따르고, 원격 증거는 [§8.9](verification/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거)에 있다.
- [ ] **미검증 · DEP P0 전체 수용 필요** — 남은 [P0 예외 8개](../tests/conformance/p0-requirement-exceptions-v1.json)는 개별 수용을 마칠 때만 제거한다(`TEST-10`). 관리자 포함 merge 차단 설정·probe 통과는 인프라 수용을 대신하지 않는다. [성능 검증 기록 §8.9](verification/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거)를 따른다.

<a id="110-실행-옵션설정-구조"></a>
### 1.10 실행 옵션·설정 구조

채택된 설정 범위는 [Process configuration v1](contracts/process-configuration-v1.md)과 [설정 스펙](specs/configuration-and-runtime-options.md)을 따른다. [ADR-0052](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)의 나머지 `Proposed` 범위는 승인 없이 작업으로 추가하지 않는다.

[PRD §16 선택적 adapter](requirements/PRD.md#16-단계별-제공-범위)의 HTTP webhook·MQTT bridge·Kafka와 DEP-24 ECS도 필요성·별도 수용 기준을 승인한 뒤에만 착수한다.

항목을 마치면 기준 스펙의 시나리오, 테스트 메타 정보와 실제 실행 결과를 갱신한다. 미실행이나 미승인 범위를 완료로 표시하지 않는다.
