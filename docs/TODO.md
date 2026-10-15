# PRD 구현 잔여 작업

이 문서는 [PRD](requirements/PRD.md)의 미완료 구현·수용 시험·선행 결정만 추적한다. 완료 이력, 시험 명령, 수치와 run ID는 연결한 기준 문서에서 관리한다. PRD의 `Draft` 표시는 구현 승인을 뜻하지 않는다. 완료 판정은 채택된 스펙·계약·ADR의 해당 범위를 따른다.

최신 대조일: 2026-10-15. 과거 실패는 후속 일부 시험이 통과해도 지우지 않는다. 특히 `CLU-14 FAILED/UNRESOLVED`와 전체 local 검증 실패는 유지한다.

| 표기 | 뜻 |
| --- | --- |
| `[ ]` **미구현·부분 구현** | 필요한 구현이 없거나 일부만 있다. |
| `[ ]` **미검증** | 구현 또는 제한된 통과 증거는 있으나 해당 수용 시험이 남았다. |
| `[ ]` **결정 필요·보류** | 정책·운영 데이터·외부 release 등 선행 조건을 먼저 해결해야 한다. |

완료한 작업은 이 목록에서 제거하고 기준 문서의 수용 매핑에 남긴다. 부분 검증은 남은 범위와 해제 조건을 함께 적는다. 과거 링크를 유지하려고 절 번호는 보존한다.

## 실행 환경과 우선순위

기존 EKS와 이후 합성 시험 환경은 각각의 [최종 폐기 기록](../.artifacts/verification/eks-final-20261014T221024Z/final-decommission-summary.json), [재개 실행·정리 기록](../.artifacts/verification/eks-resume-20261015T000337Z-21df3b/final-summary.json), [마지막 EC2 시험 정리 기록](../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/final-summary.json)에서 삭제를 확인했다. 보호 snapshot과 공유 image는 유지한다. 이 기록을 현재 실행 중인 환경이나 전체 AWS 비용 0의 증거로 쓰지 않는다.

새 인프라 시험은 승인된 별도 state·VPC의 격리 합성 fixture에서만 수행한다. 보호 자원은 바꾸지 않는다. 필요한 증거를 보존하고 시험 후 새 전용 자원을 정리한다. 운영 데이터나 시간 경과가 필요한 시험을 기다리려고 EKS를 계속 유지하지 않는다.

다음 우선 작업은 EKS에서만 확인할 수 있는 남은 gate의 시험이다. 후보는 preStop/drain 실패·timeout·grace expiry, 정상 MQTT traffic의 NLB routing·연결 drain, controller failure/restart matrix, 전체 voter 전원 손실과 volume loss/reseed다. 각 시험은 새 격리 fixture의 별도 승인된 계획으로 시작한다. S3 미게시 staging 복구·정리와 단일 follower EC2 비정상 종료 관찰은 각 기준 문서의 검증 범위까지 완료했다. 실제 운영 부하·보존 기간·공식 SDK release가 필요한 항목은 해당 선행 조건을 기다린다.

## 1. 열린 작업

### 1.1 Production storage와 disaster recovery

- [ ] **미검증 · K8s/AWS** — Linux/EBS 전원·volume 손실 수용(`CLU-14`). 마지막 durable commit 보존, production WAL torn-tail 판정, 실제 volume loss/reseed를 지원 filesystem·EBS에서 검증한다. 기존 실패는 **FAILED/UNRESOLVED**다. 이후 단일 follower·동일 EBS 복구의 제한된 통과로 전체 수용을 닫지 않는다. [Cluster §11.20](specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑), [local ext4 경계](specs/cluster-session-and-delivery.md#1123-local-ext4-filesystem-수용-매핑), [ADR-0007](decisions/ADR-0007-aws-production-deployment-platform.md)을 따른다.
- [ ] **부분 검증 · K8s/AWS·지속 운영 관찰** — 백업 보존·재해 복구(`SPEC-TBD-09`). 일 1회 백업, 30일 보존, RPO 24시간·전체 RTO 1시간이 기준이다. 실제 S3·IRSA·새 PVC 세 개의 합성 전체 복원은 통과했다. 남은 범위는 지속적인 일일 백업 RPO, 실제 30일 경과 보존, production 데이터 크기와 업로드 실패 matrix다. [ADR-0058](decisions/ADR-0058-cluster-backup-s3-retention-and-restore.md), [Cluster §11.9](specs/cluster-session-and-delivery.md#119-offline-queue-replication과-backup-v3-수용-매핑)를 따른다.

### 1.2 Release와 rollout gate

- [ ] **미검증 · 게시 release 대기** — Historical-source N-1↔N 호환성(`CLU-11·NFR-15`). 실제 release 게시 때 직전 게시 release와 upgrade·rollback·archive restore를 검증하고 baseline descriptor를 갱신한다. 지정 historical baseline 통과는 게시 release 전체의 수용이 아니다. [Cluster §11.21](specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑), [Replicated-state §5](contracts/replicated-state-v1.md#5-예제와-계약-테스트contract-test)를 따른다.
- [ ] **부분 검증 · K8s/EKS** — External rollout controller(`DEP-11·DEP-18`). 구현과 실제 EKS의 정상 교체·active-drain restart·일부 삭제 차단·감사 기록, Argo CD health 소비자는 검증했다. 남은 범위는 failure/API·restart·Kubernetes mutation 결과 불명·CAS matrix, Argo CD가 적용한 실제 broker 교체의 통합 경로, 게시 N/N-1과 최대 부하다. [Rollout controller contract §3](contracts/rollout-controller-v1.md#3-acceptance-matrix), [Drain §10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)를 따른다.
- [ ] **미검증 · 운영 부하 선행** — BP-07 rolling drain 비교(`PERF-07`). 로컬 비교와 반복 suite의 제한된 통과는 유지한다. `SPEC-TBD-08` 수치형 합격 기준과 production 배포 환경에서의 비교가 필요하다. [성능 §3.5](specs/performance-and-verification.md#35-bp-07-rolling-drain-smoke-관찰-경계)를 따른다.
- [ ] **미검증 · 운영 최대 부하 선행** — Kubernetes 최대 traffic의 연속 100회 rolling(`DEP-01~12`). 로컬 process의 혼합 MQTT·QoS·session 100회 시험은 통과했다. 실제 `B_conn/B_pub/B_sub`, QoS·payload·session·tenant·TLS profile과 절대 cutoff를 확정한 뒤 운영 배포 profile에서 검증한다. [Drain §9·§10](specs/drain-and-rolling-update.md#9-수용-시험)을 따른다.
- [ ] **보류 · 공식 SDK 수정 release 대기** — Server redirect·Will 호환성(`CLIENT-01~09`). Paho Go MQTT 5 `v0.23.0`의 중복 Server Reference 수락 결함이 남았다. 직접 수정한 SDK나 raw socket 대체 시험은 도입하지 않는다. 공식 수정 release 채택 뒤 drain 10개와 Phase 1 26개를 모두 검증한다. [Client contract §6.2](contracts/mqtt-client-compatibility-v1.md#62-drain-compatibility-suite)를 따른다.

### 1.3 Cluster 기능과 알려진 구현 한계

현재 지원하는 수동 일회성 rebalance는 완료했다. [Cluster §9](specs/cluster-session-and-delivery.md#9-rebalance)의 범위를 새 미구현 작업으로 다시 열지 않는다. Production 부하 검증은 §1.9에서 추적한다.

### 1.4 Drain 정책·관측과 production 배포

- [ ] **부분 검증 · K8s/EKS** — Kubernetes 종료 연동과 `EKS_EC2_HA` 전체 수용(`DEP-13~23`). 남은 범위는 preStop/drain 실패·timeout·grace expiry·admission/hook failure, 정상 MQTT traffic의 NLB routing·기존 연결 drain·idle timeout·client IP·connection termination, EC2/AZ 장애 matrix, snapshot/restore·disk pressure·durable ACK RPO 0, supported profile 자동 검증과 release 지원 상태다. [Drain §7·§10](specs/drain-and-rolling-update.md#7-external-rollout-gate와-kuberneteseks), [EKS architecture](architectures/eks-ec2-deployment.md)를 따른다. Controller와 CLU-14의 중복 범위는 각각 §1.2·§1.1에서 추적한다.
  - [x] **완료(단일 follower EC2 비정상 종료 관찰 범위)** — 격리 EKS에서 생존 Pod 고정 observer로 fault 전부터 동일 EBS의 다른 worker 복구까지 관리 API·MQTT 표본을 이어서 기록했다. 장애 요청 뒤 API 200개·MQTT 235개가 모두 성공했다. 표본 시작 간격은 관리 API 최대 7.065초, MQTT 최대 2.056초다. 관리 API 표본 13개는 각각 약 5.03초 걸렸고 최대 소요는 5.061초다. 이전 표본 종료→다음 시작 idle gap(최대 2.016초)과 구분한다. 표본 사이의 가용성은 입증하지 않는다. Fencing은 실패 없이 적용됐다. 복구한 broker에서 persistent session·오프라인 QoS 1 payload 3개·retained를 재발행 없이 확인했다. Broker 3개 Ready 관찰 상한은 471.06초이며 정확한 RTO나 연속 가용성 증거가 아니다. 이 범위만 닫는다. 기존 최초 observer 실패, 전체 EC2·AZ matrix, 전체 voter 손실과 volume loss는 계속 열려 있다. [수용 기록](specs/drain-and-rolling-update.md#생존-pod-고정-observer의-eks-재시험-2026-10-15)을 따른다.
- [x] **완료(EKS 1회 실행 범위)** — 단일 EC2 terminate 뒤 대체 broker의 EBS attach가 약 6분 늦어지던 문제다. 이전 세 EKS 실행은 모두 `Waiting for detach` 뒤 6분 3~5초에 attach됐고 Broker 3개 Ready는 471~473초였다. Node 삭제 시각이 기록된 두 실행에서 cloud controller는 NotReady 뒤 1~2초 안에 Node와 taint를 함께 삭제했다. 사용자는 2026-10-15에 taint 확인 직후 기존 Pod를 UID 조건과 grace 0으로 한 번 삭제하는 방식을 선택했다. 운영 문서와 관찰 도구를 바꾼 뒤 EKS에서 1회 재시험했다. 기존 Pod 삭제는 기존 Node와 taint가 남아 있을 때 실행됐다. `FailedAttachVolume`은 0건이었고 삭제 요청 약 5.5초 뒤 attach됐다. Broker 3개 Ready 관찰은 193.995초였고 데이터 oracle은 PASS다. 한 번의 실행이며 controller-manager 로그는 읽지 않았다. 관찰 도구의 `podRelease.outcome`이 `deleted`가 아닌 실행은 이 개선의 증거로 쓰지 않는다. [분석](../.artifacts/analysis/ebs-detach-wait-analysis.md), [재시험 기록](specs/drain-and-rolling-update.md#기존-pod-해제의-eks-재시험-2026-10-15).
- [ ] **조사 필요 · 원인 미확인** — 위 재시험에서 attach 뒤 대체 broker의 startup probe가 약 130초 동안 503이었다. 대체 node 3은 peer 1·2를 `connected=false`, `outbound queue full`로 기록했다. Leader node 2는 16:51:14Z부터 peer 3을 `connected=true`, `outbound queue full`로 기록했고 16:51:18Z부터 `peer write failed`를 기록했다. 16:51:23.161Z에 `connected=false`가 됐고 node 3은 0.011초 뒤 동기화됐다. [INFERENCE] Leader가 종료된 이전 Pod 주소의 peer 연결을 쓰기 실패까지 유지해 대체 node가 leader 메시지를 받지 못했다. Socket 상태, node 3 송신이 연결되지 않은 이유, transport의 write deadline·keepalive 설정은 확인하지 않았다. 해제 조건은 Raft peer transport 코드와 연결 계약 대조, 로컬 재현(같은 이름의 peer를 새 IP로 교체), 원인 확인 뒤 사용자 결정과 회귀 시험이다. [재시험 기록](specs/drain-and-rolling-update.md#기존-pod-해제의-eks-재시험-2026-10-15).

### 1.5 설계 미결정과 비지원 범위

[Cluster architecture의 미결정 사항](architectures/cluster-architecture.md#미결정-사항)은 새 구현 승인이 아니다. Route-index의 물리적 분산 배치, 기존 관측 RPC 밖의 새 RPC, cross-cluster migration·voter-set 변경 복원은 별도 계약 채택 전에는 착수하지 않는다. 현재 구현한 route rebuild·관측 RPC를 다시 미완료로 분류하지 않는다.

### 1.6 MQTT protocol과 client 호환성

- [ ] **미검증 · 운영 조사·감시** — MQTT 3.1.0 전환(`MQTT-13`). 90일 이상 client 사용량, 오래된 firmware·장비 소유자, 3.1.1 이상 전환 또는 gateway 격리, canary 거절 Metric을 확인한다. Wire-level 거절 구현의 로컬 통과는 운영 전환 완료가 아니다. [PRD §9.1](requirements/PRD.md#91-mqtt-프로토콜)을 따른다.

### 1.7 Tenant·security와 감사

- [ ] **부분 검증 · K8s·외부 운영 저장소** — 감사 운영 범위(`SEC-11·NFR-17·API-08`). Rollout producer의 EKS 배포와 실제 감사 PVC 기록은 확인했다. 남은 범위는 실제 PVC append/fsync 장애 시 삭제 차단, 외부 collector 전달, SIEM 최소 365일 보존과 node-local 최소 7일 버퍼의 운영 검증이다. [Rollout contract §3](contracts/rollout-controller-v1.md#3-acceptance-matrix), [Tenant와 security §8·§9](specs/tenancy-and-security.md#8-감사와-비밀정보)를 따른다.
- [ ] **미검증 · 독립 보안 시험 필요** — 운영 경계 침투 시험(`NFR-19`, `R-07`). 채택한 threat model과 위험 승인, 로컬 자동 사전 시험은 유지한다. NLB·TLS termination·secret 저장소·Kubernetes를 포함한 독립 시험이 남았다. [Threat model](architectures/threat-model.md)을 따른다.

### 1.8 Management API·observability와 Dashboard

- [ ] **보류 · 출시 후 조건부 범위** — P1 관리 화면·제어 API·exact-topic Metric·tenant 위임(`UI-06~09·API-07·TEN-10`). 현 출시에서는 Grafana P0 조회 대시보드를 유지한다. 재개할 때 우선순위·API 계약·UI 순서와 권한·마스킹·pagination·idempotency·감사 기준을 먼저 확정한다. [ADR-0045](decisions/ADR-0045-operations-dashboard-grafana.md)를 따른다. 완료한 P0 대시보드의 EKS 통합은 다시 열지 않는다.
- [ ] **조사 필요 · 원인 미확인** — EKS cycle 1에서 quorum을 유지한 healthy leader node 2가 `unknown`으로 집계됐다. 2026-10-15 실행은 3-voter cluster였다. Observer는 follower node 1, leader는 node 2, 종료 대상은 node 3이었다. Broker image digest는 앞선 EKS 실행과 같은 `sha256:140a99b2…`였다. `monitor.diagnostics.nodes.baseline`은 fault 전에 사용 가능했고 세 노드는 약 0.03초에 `OK`였다. Fault 요청은 14:18:13.043Z, out-of-service taint는 14:18:34.671Z였다. `GET /api/v1/nodes`에서 node 2와 3은 14:18:17.677Z, 14:18:26.711Z, 14:18:35.747Z 표본에서 `UNREACHABLE/node_query_timeout`(5.02–5.03초)이었다. Node 2는 14:18:44.794Z에 `OK`였다. 비정상 관측 구간은 표본 경계로 23.1–36.2초다. `GET /api/v1/cluster`는 ready 1/unknown 2(14:18:15.7–14:18:36.9) 뒤 ready 2/unknown 1로 바뀌었다. Leader 2와 quorum은 203개 표본 모두에서 유지됐다. MQTT QoS 1 PUBACK은 성공했다(대부분 9–23ms, 한 건 0.245초). Node 3은 대체 Pod Ready까지 비정상이었다(+473.383초). Oracle은 PASS였다. Node 1·2 로그에는 비정상 구간 기록이 없었다. Node 2의 첫 peer backpressure는 14:21:15Z로 구간 뒤다. 종료 전 target 로그는 수집하지 않았다. 두 CoreDNS replica는 종료 worker에 있었다. 대체 container는 14:18:40Z에 시작했다. 사후 dump의 Deployment `Available` 전환 시각은 14:18:41Z이며 대체 Pod Ready나 DNS 복구 시각이 아니다. DNS probe Pod의 정상 상태 조회 876회는 실패 0회, median 0.002초, max 0.012초였다. Fault 중 observer worker의 DNS는 측정하지 않았다. EndpointSlice에서 dead endpoint가 빠진 시각과 대체 Pod Ready 시각은 복원할 수 없다. [INFERENCE] 두 CoreDNS replica가 함께 사라져 fault 요청부터 대체 container 시작까지 약 27초간 새 hostname 조회가 지연되고 healthy peer 관측 RPC가 timeout됐을 가능성이 있다. 이 해석은 시간대와 두 원격 node의 동시 timeout에 맞지만 원인으로 확인되지 않았다. 로컬 DNS 실험은 같은 broker image로 8회 했다. D1 pause와 UDP/TCP DROP에서 healthy node 2와 node 3은 약 5초 `node_query_timeout` 뒤 빠르게 `node_unreachable`이 됐다. `unknown=2`, leader 2, quorum은 유지됐다. D2 peer pause에서는 node 3만 비정상이었다. D3에서는 DNS 복구 뒤 node 2가 `OK`였고, 멈춘 node 3은 peer 복구까지 비정상이었다. QoS 1 publish는 475/475 성공했다. Broker 로그는 모두 0줄이었다. Nodes 경로에는 ReadIndex barrier가 없다. 죽은 peer의 송신 queue는 block하지 않는다. Backpressure 로그는 관측 구간 뒤에 나왔다. A(barrier), B(rejection), C(deadline race)는 자료가 지지하지 않는다. D(dead-peer queue)는 이 구간 설명으로 기각됐다. 코드 분석은 원인을 확정하지 않는다. 실패·timeout query는 계약상 `UNREACHABLE`/`unknown`이며 현재 시험 목록은 DNS outage 또는 dead-peer를 다루지 않는다. 사용자는 (1) 계약에 맞는 동작으로 보고 CoreDNS replica를 서로 다른 worker에 배치하는 배포 전제를 두거나, (2) 제품 동작과 계약을 바꾸고 black-hole resolver 회귀 시험을 추가할 수 있다. 결정은 열어 둔다. 2026-10-15 기존 Pod 해제 재시험에서는 두 CoreDNS replica가 대상이 아닌 worker에 있었다. Fault 뒤 성공한 nodes 표본 45개에서 healthy node 1·2는 모두 `OK`였고 `UNREACHABLE`은 대상 node 3뿐이었다. 이 실행도 fault 중 DNS를 측정하지 않았으므로 원인 확인으로 쓰지 않는다([기록](../.artifacts/verification/eks-detach-release-20261015T162550Z-23b31b/fault/eks-detach-release-20261015T162550Z-23b31b/nodes-samples.jsonl)). 다음 EKS 재시험은 replica 배치와 fault 중 observer worker의 DNS를 기록하고 CoreDNS replica를 서로 다른 worker에 둔다. 해제 조건은 DNS 측 증거 또는 사용자 결정, 계약 대조 결과, 제품 변경 시 계약 개정과 회귀 시험이다. 근거: [cycle 1 summary](../.artifacts/verification/eks-unknown-retest-20261015T135242Z/fault/cycle-1/eks-unknown-retest-20261015T135242Z-2b7e10/summary.json), [노드 표본](../.artifacts/verification/eks-unknown-retest-20261015T135242Z/fault/cycle-1/eks-unknown-retest-20261015T135242Z-2b7e10/nodes-samples.jsonl), [CoreDNS timeline](../.artifacts/verification/eks-unknown-retest-20261015T135242Z/fault/cycle-1/post-fault/cycle1-coredns-timeline.json), [로컬 DNS 결과](../.artifacts/analysis/dns-stall/experiment.json), [코드 분석](../.artifacts/analysis/unknown-leader-timeout-analysis.md), [peer observability §3](contracts/peer-observability-v1.md#3-한도와-시간), [Management API v1](contracts/management-api-v1.md).

### 1.9 Performance·test와 공통 출시 gate

- [ ] **결정 필요 · 운영 workload 실측 대기** — Production workload와 절대 cutoff(`NFR-01~06·PERF-01~12`, `SPEC-TBD-08`). Comparator, `EKS_EC2_HA` qualification profile과 상대 합격 기준은 확정했다. 실제 `B_conn/B_pub/B_sub`, payload·topic 분포, QoS·session·tenant·TLS 부하를 확보해 절대 cutoff를 계산한다. TEN-09 production tenant 부하·절대 SLO도 이 작업에 포함한다. [PRD §18](requirements/PRD.md#18-미결정-사항), [성능 §3·§6](specs/performance-and-verification.md#3-benchmark-공통-계약)을 따른다.
- [ ] **미검증 · K8s/AWS 출시 비교** — Production benchmark profile(`PERF-01·05~07·10~12·NFR-06`). 위 workload 결정 뒤 동등 조건·독립 반복·N/N-1·failure recovery·release regression 기준으로 BP-02·BP-04 및 BP-03·BP-06·BP-07의 production 증거를 확보한다. 기존 Docker smoke는 `releaseGating=false`이며 대체 증거가 아니다. [성능 §§3~6](specs/performance-and-verification.md#3-benchmark-공통-계약)을 따른다.
- [ ] **미해결 · 로컬/CI** — 최근 전체 local 검증 실패를 해결한다. 과거 원격 CI·coverage·mutation 통과는 보존하지만 이후 집중 시험 통과로 전체 실패를 지우지 않는다. [마지막 local 기록](../.artifacts/verification/eks-resume-20261015T000337Z-21df3b/local-verification.json), [observer 검증 범위](specs/drain-and-rolling-update.md#eks-worker-observer-보완-기준)를 따른다. 기존 실패 원인 분석과 수정 뒤 해당 재현·전체 gate의 실제 결과가 필요하다.
- [ ] **미검증 · DEP P0 전체 수용 필요** — Merge 차단 최종 gate(`TEST-10`). 관리자 포함 merge 차단 설정과 결함 probe는 검증했다. 남은 DEP P0 예외 10개는 개별 수용을 마칠 때만 제거한다. 오래된 예외 사유는 최신화하되 예외 수를 줄여 통과시키지 않는다. [성능 §8.9](specs/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거), [P0 예외](../tests/conformance/p0-requirement-exceptions-v1.json)를 따른다.

<a id="110-실행-옵션설정-구조"></a>
### 1.10 실행 옵션·설정 구조

채택된 G1~G10은 완료했다. 기준과 실행 증거는 [Process configuration v1](contracts/process-configuration-v1.md), [설정 스펙 §6](specs/configuration-and-runtime-options.md#6-수용-시험)에 둔다. [ADR-0052](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)의 나머지 `Proposed` 범위는 승인 없이 새 작업으로 추가하지 않는다.

[PRD §16 선택적 adapter](requirements/PRD.md#16-단계별-제공-범위)의 HTTP webhook·MQTT bridge·Kafka와 DEP-24 ECS도 필요성·별도 수용 기준을 승인한 뒤에만 착수한다.

항목을 마치면 기준 문서의 수용 매핑과 실제 실행 결과를 먼저 갱신한다. 미실행이나 미승인 범위를 완료로 표시하지 않는다.
