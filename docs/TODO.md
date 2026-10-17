# PRD 구현 잔여 작업

이 문서는 [PRD](requirements/PRD.md)의 미완료 구현·수용 시험·선행 결정만 추적한다. 완료 이력, 시험 명령, 수치와 run ID는 연결한 기준 문서에서 관리한다. PRD의 `Draft` 표시는 구현 승인을 뜻하지 않는다. 완료 판정은 채택된 스펙·계약·ADR의 해당 범위를 따른다.

최신 대조일: 2026-10-16. 과거 실패는 후속 일부 시험이 통과해도 지우지 않는다. 특히 `CLU-14 FAILED/UNRESOLVED`와 전체 local 검증 실패는 유지한다.

| 표기 | 뜻 |
| --- | --- |
| `[ ]` **미구현·부분 구현** | 필요한 구현이 없거나 일부만 있다. |
| `[ ]` **미검증** | 구현 또는 제한된 통과 증거는 있으나 해당 수용 시험이 남았다. |
| `[ ]` **결정 필요·보류** | 정책·운영 데이터·외부 release 등 선행 조건을 먼저 해결해야 한다. |

완료한 작업은 이 목록에서 제거하고 기준 문서의 수용 매핑에 남긴다. 부분 검증은 남은 범위와 해제 조건을 함께 적는다. 과거 링크를 유지하려고 절 번호는 보존한다.

## 실행 환경과 우선순위

기존 EKS와 이후 합성 시험 환경은 각각의 [최종 폐기 기록](../.artifacts/verification/eks-final-20261014T221024Z/final-decommission-summary.json), [재개 실행·정리 기록](../.artifacts/verification/eks-resume-20261015T000337Z-21df3b/final-summary.json), [마지막 EC2 시험 정리 기록](../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/final-summary.json)에서 삭제를 확인했다. 보호 snapshot과 공유 image는 유지한다. 이 기록을 현재 실행 중인 환경이나 전체 AWS 비용 0의 증거로 쓰지 않는다.

새 인프라 시험은 승인된 별도 state·VPC의 격리 합성 fixture에서만 수행한다. 보호 자원은 바꾸지 않는다. 필요한 증거를 보존하고 시험 후 새 전용 자원을 정리한다. 운영 데이터나 시간 경과가 필요한 시험을 기다리려고 EKS를 계속 유지하지 않는다.

다음 우선 작업은 EKS에서만 확인할 수 있는 남은 gate의 시험이다. 후보는 preStop/drain 실패·timeout·grace expiry, 정상 MQTT traffic의 NLB routing·연결 drain, controller failure/restart matrix, 전체 voter 전원 손실과 volume loss/reseed다. 각 시험은 새 격리 fixture의 별도 승인된 계획으로 시작한다. S3 미게시 staging 복구·정리와 단일 follower EC2 비정상 종료 관찰은 각 기준 문서의 검증 범위까지 완료했다. 실제 운영 부하·보존 기간이 필요한 항목은 해당 선행 조건을 기다린다.

## 1. 열린 작업

### 1.1 Production storage와 disaster recovery

- [x] **완료(EKS 수용 matrix 실행 범위)** — Linux/EBS 전원 손실 수용(`CLU-14`). 2026-10-16 격리 EKS fixture(`narumq-clu14-20261016-a1925a`, run `eks-clu14-20261016T103521Z-a1925a`)에서 채택된 수용 기준을 모두 통과했다: (1) P1 전체 voter 전원 손실 모델(3개 EC2 동시 stop/start, 70.8초 Ready 복구, Durable ACK RPO 0, tail 절단 없음 관찰), (2) T1·T2 실제 EBS ext4 WAL 주입(short-header, zero-tail 정상 절단 및 Ready 복구; final-crc-no-zero-evidence, middle-crc 시작 거절 및 백업 복원 후 Ready 복구), (3) R1 1 MiB 초과 snapshot 재시드(`snapshot-stream` 활성화, 2.1 MiB snapshot 스트리밍 전송 완료, 새 voter 승격, 대형 retained 8개 topic SHA-256 일치). 2026-10-15 02:10 실패의 원인은 startup probe 결함으로 규명됐으며 실제 process 재현·수정 시험을 통과했다. 2026-10-15 FAILED/UNRESOLVED 과거 기록은 스펙에 보존한다. 시험 후 EKS·VPC·볼륨 등 모든 AWS 자원은 41개 완전 삭제를 검증했다. [Cluster §11.20·§11.37](specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑), [실행 결과](specs/cluster-session-and-delivery.md#2026-10-16-clu-14-완결-matrix-실행-결과)를 따른다.
- [ ] **부분 검증 · K8s/AWS·지속 운영 관찰** — 백업 보존·재해 복구(`SPEC-TBD-09`). 일 1회 백업, 30일 보존, RPO 24시간·전체 RTO 1시간이 기준이다. 실제 S3·IRSA·새 PVC 세 개의 합성 전체 복원은 통과했다. 남은 범위는 지속적인 일일 백업 RPO, 실제 30일 경과 보존, production 데이터 크기와 업로드 실패 matrix다. [ADR-0058](decisions/ADR-0058-cluster-backup-s3-retention-and-restore.md), [Cluster §11.9](specs/cluster-session-and-delivery.md#119-offline-queue-replication과-backup-v3-수용-매핑)를 따른다.
- [x] **완료(제품 구현과 EKS 검증)** — 1 MiB를 넘는 Raft snapshot 전송(`CLU-14`). [ADR-0062](decisions/ADR-0062-raft-snapshot-stream.md)와 [Raft snapshot stream v1](contracts/raft-snapshot-stream-v1.md)에 따라 `snapshot-stream` feature token으로 활성화하는 1 MiB 이하 프레임 mTLS 조각 스트림 전송을 구현했다. 로컬 process 시험(`TestDynamicClusterSnapshotStreamProcessAcceptance`, 10.71초)과 실제 EKS R1 재시드(2,105,083 byte snapshot 전송 완료, digest 일치)를 통과했다. [Cluster §7·§11.37](specs/cluster-session-and-delivery.md#1137-raft-snapshot-stream-수용-매핑)을 따른다.

### 1.2 Release와 rollout gate

- [ ] **미검증 · 게시 release 대기** — Historical-source N-1↔N 호환성(`CLU-11·NFR-15`). 실제 release 게시 때 직전 게시 release와 upgrade·rollback·archive restore를 검증하고 baseline descriptor를 갱신한다. 지정 historical baseline 통과는 게시 release 전체의 수용이 아니다. 이때 [Rollout controller contract §3](contracts/rollout-controller-v1.md#3-acceptance-matrix)의 `게시 release upgrade/rollback` 행도 같은 barrier로 검증한다(2026-10-16 인계). [Cluster §11.21](specs/cluster-session-and-delivery.md#1121-historical-source-n-1n-호환성-수용-매핑), [Replicated-state §5](contracts/replicated-state-v1.md#5-예제와-계약-테스트contract-test)를 따른다.
- [x] **완료(격리 EKS 결함 matrix 범위)** — External rollout controller 외부 gate(`DEP-18`). 2026-10-16 격리 EKS(run `eks-rollout-20261016T122800Z-a3f1b2`)에서 남은 결함 matrix A~G가 통과했다. `SAFE_TO_STOP` 전 Pod 삭제 0건과 drain 실패 시 현재·다음 Pod 갱신 중단을 확인했다. 시험 중 찾은 status 반복 write는 고친 controller로 다시 시험했다. Argo CD가 적용한 실제 broker 교체(r2 controller)는 교체·Pod별 controller DELETE 1건·health가 통과했지만, finalizer가 Git spec의 `abort: false`를 지워 sync 판정은 실패했다. 2026-10-17 격리 EKS(run `eks-rollout-20261016T232359Z-68569c`)에서 finalizer를 고친 controller로 실제 broker 교체와 완료 뒤 60초 `Synced`·`Healthy`를 함께 확인했다. 시험 자원은 모두 삭제했다. 게시 release upgrade/rollback은 위 `CLU-11·NFR-15` 항목, 최대 부하와 연속 100회는 아래 `DEP-01~12` 항목에서 검증한다. [Rollout controller contract §3](contracts/rollout-controller-v1.md#3-acceptance-matrix), [Drain §10 기록](specs/drain-and-rolling-update.md#external-rollout-controller-eks-결함-matrix-수용-2026-10-16)을 따른다.
- [x] **완료(격리 EKS 범위)** — Drain 실패·timeout 뒤 rollout 처리(`DEP-11`). 2026-10-16 격리 EKS에서 drain `FAILED`와 drain GET 장애 중 deadline 만료가 delete 없이 `Failed`로 끝나는 것을 확인했다. 2026-10-17 사용자 결정으로 `SAFE_TO_STOP` 뒤 실패한 rollout의 새 `retryToken`은 같은 drain을 새 승인 시한(`approvalDeadlineAt`, 재승인 뒤 10분) 안에서 fresh 증거로 다시 승인한다. Pod UID가 같으면 현재 Pod resourceVersion을 채택한다. 같은 날 격리 EKS(run `eks-rollout-20261016T232359Z-68569c`)에서 `DrainSafe` deadline 실패, 재승인 안내, broker deadline 뒤 재승인, 새 승인 시한의 실패와 재승인 뒤 교체 완료를 확인했다. 그동안 delete와 drain POST·취소는 0건이었다. Pod resourceVersion 변경 채택, `pod_identity_changed`·`retry_cancel_unconfirmed` 차단, `DeleteApproved`의 deadline 실패는 단위 시험이 판정한다. [Rollout controller contract §2.9](contracts/rollout-controller-v1.md#2-reconciliation-contract), [Drain §10 기록](specs/drain-and-rolling-update.md#재승인-retry와-argo-cd-적용-교체-eks-재시험-2026-10-17)을 따른다.
- [ ] **미검증 · 운영 부하 선행** — BP-07 rolling drain 비교(`PERF-07`). 로컬 비교와 반복 suite의 제한된 통과는 유지한다. `SPEC-TBD-08` 수치형 합격 기준과 production 배포 환경에서의 비교가 필요하다. [성능 §3.5](specs/performance-and-verification.md#35-bp-07-rolling-drain-smoke-관찰-경계)를 따른다.
- [ ] **미검증 · 운영 최대 부하 선행** — Kubernetes 최대 traffic의 연속 100회 rolling(`DEP-01~12`). 로컬 process의 혼합 MQTT·QoS·session 100회 시험은 통과했다. 실제 `B_conn/B_pub/B_sub`, QoS·payload·session·tenant·TLS profile과 절대 cutoff를 확정한 뒤 운영 배포 profile에서 검증한다. External rollout controller의 최대 부하 수용도 이 항목에서 판정한다(2026-10-16 인계). [Drain §9·§10](specs/drain-and-rolling-update.md#9-수용-시험)을 따른다.
- [ ] **부분 검증 · 운영 rollout 수용 증거 필요** — Server redirect·Will 호환성(`CLIENT-01~09`). Phase 1과 정상 SDK drain 연동은 검증했다. 남은 범위는 production rollout timing과 배포 profile의 수용 증거다. Fake server의 malformed 응답 8개는 선택적 SDK 진단이며 제품 승인을 차단하지 않는다. Go 중복 Server Reference 진단 실패는 보존하되 공식 SDK 수정 release를 정상 drain 승인의 선행 조건으로 두지 않는다. [Client contract §6.2·§6.3](contracts/mqtt-client-compatibility-v1.md#62-drain-compatibility-suite)를 따른다.

### 1.3 Cluster 기능과 알려진 구현 한계

현재 지원하는 수동 일회성 rebalance는 완료했다. [Cluster §9](specs/cluster-session-and-delivery.md#9-rebalance)의 범위를 새 미구현 작업으로 다시 열지 않는다. Production 부하 검증은 §1.9에서 추적한다.

### 1.4 Drain 정책·관측과 production 배포

- [ ] **부분 검증 · K8s/EKS** — Kubernetes 종료 연동과 `EKS_EC2_HA` 전체 수용(`DEP-13~23`). 남은 범위는 preStop/drain 실패·timeout·grace expiry·admission/hook failure, 정상 MQTT traffic의 NLB routing·기존 연결 drain·idle timeout·client IP·connection termination, 모델 밖의 AZ 장애(network·EBS 서비스·control plane)와 반복 실행, supported profile 자동 검증과 release 지원 상태다. [Drain §7·§10](specs/drain-and-rolling-update.md#7-external-rollout-gate와-kuberneteseks), [EKS architecture](architectures/eks-ec2-deployment.md)를 따른다. Controller와 CLU-14의 중복 범위는 각각 §1.2·§1.1에서 추적한다.
  - [x] **완료(단일 follower EC2 비정상 종료 관찰 범위)** — 격리 EKS에서 생존 Pod 고정 observer로 fault 전부터 동일 EBS의 다른 worker 복구까지 관리 API·MQTT 표본을 이어서 기록했다. 장애 요청 뒤 API 200개·MQTT 235개가 모두 성공했다. 표본 시작 간격은 관리 API 최대 7.065초, MQTT 최대 2.056초다. 관리 API 표본 13개는 각각 약 5.03초 걸렸고 최대 소요는 5.061초다. 이전 표본 종료→다음 시작 idle gap(최대 2.016초)과 구분한다. 표본 사이의 가용성은 입증하지 않는다. Fencing은 실패 없이 적용됐다. 복구한 broker에서 persistent session·오프라인 QoS 1 payload 3개·retained를 재발행 없이 확인했다. Broker 3개 Ready 관찰 상한은 471.06초이며 정확한 RTO나 연속 가용성 증거가 아니다. 이 범위만 닫는다. 기존 최초 observer 실패, 전체 EC2·AZ matrix, 전체 voter 손실과 volume loss는 계속 열려 있다. [수용 기록](specs/drain-and-rolling-update.md#생존-pod-고정-observer의-eks-재시험-2026-10-15)을 따른다.
  - [x] **완료(EKS 장애 matrix 1회 실행 범위)** — 2026-10-16 격리 EKS에서 [EKS 장애 matrix 보완 기준](specs/drain-and-rolling-update.md#eks-장애-matrix-보완-기준-2026-10-16)의 세 시나리오가 통과했다. 단일 leader EC2 비정상 종료는 9.2초 뒤 새 leader가 관찰되고 표본 기준 불가용 구간 없이 같은 EBS로 복구했다. 단일 AZ 전원 중단 모델은 480초 정지 동안 표본이 모두 성공했고 재개 뒤 180초 안정 상태를 지켰다. EBS disk pressure D1–D3는 `0x97` admission, follower ENOSPC 감지·복구, 두 follower ENOSPC 동안 `acked` 없음과 durable ACK 누락 0을 확인했다. 각 시나리오는 한 번 실행했고 표본 관찰이다. P0 예외 DEP-20·DEP-21은 Go traceability 시험이 없고 남은 범위가 있어 유지한다. [Drain §10](specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) 301–303행을 따른다.
- [x] **완료(EKS 1회 실행 범위)** — 단일 EC2 terminate 뒤 대체 broker의 EBS attach가 약 6분 늦어지던 문제다. 이전 세 EKS 실행은 모두 `Waiting for detach` 뒤 6분 3~5초에 attach됐고 Broker 3개 Ready는 471~473초였다. Node 삭제 시각이 기록된 두 실행에서 cloud controller는 NotReady 뒤 1~2초 안에 Node와 taint를 함께 삭제했다. 사용자는 2026-10-15에 taint 확인 직후 기존 Pod를 UID 조건과 grace 0으로 한 번 삭제하는 방식을 선택했다. 운영 문서와 관찰 도구를 바꾼 뒤 EKS에서 1회 재시험했다. 기존 Pod 삭제는 기존 Node와 taint가 남아 있을 때 실행됐다. `FailedAttachVolume`은 0건이었고 삭제 요청 약 5.5초 뒤 attach됐다. Broker 3개 Ready 관찰은 193.995초였고 데이터 oracle은 PASS다. 한 번의 실행이며 controller-manager 로그는 읽지 않았다. 관찰 도구의 `podRelease.outcome`이 `deleted`가 아닌 실행은 이 개선의 증거로 쓰지 않는다. [분석](../.artifacts/analysis/ebs-detach-wait-analysis.md), [재시험 기록](specs/drain-and-rolling-update.md#기존-pod-해제의-eks-재시험-2026-10-15).
- [x] **완료(로컬 Docker 범위)** — 위 재시험에서 attach 뒤 대체 broker의 startup probe가 약 130초 동안 503이던 문제다. 연결 방향상 leader(낮은 ID)만 node 3을 dial한다. Dynamic peer stream은 Go 기본 keepalive만 쓰고 감지 시간을 제한하는 설정이 없었다. 그래서 leader는 사라진 이전 IP의 연결을 응용 송신 대기열이 차고 write deadline이 실패할 때까지 유지했고, 그동안 새 node 3은 leader 메시지를 받지 못했다. 로컬 Linux Docker에서 leader 2 topology로 같은 DNS 이름의 node를 새 IP로 바꾸고 이전 IP를 DROP하자 같은 log 순서가 재현됐다(연결 종료 fault 뒤 49.5초, 대체 node 준비 28.74초). 사용자 요청에 따라 [ADR-0059](decisions/ADR-0059-dynamic-peer-tcp-liveness.md)로 Raft·forwarding stream socket에 keepalive와 Linux `TCP_USER_TIMEOUT` 10초를 적용했다. 같은 시나리오는 PASS했다(연결 종료 fault 뒤 12.7초 이내, 대체 node는 시작 직후 준비). [수용 기록](specs/cluster-session-and-delivery.md#1134-dynamic-peer-tcp-생존-확인-수용-매핑).
- [x] **완료(EKS 2회 실행 범위)** — ADR-0059 TCP 생존 확인과 조기 Pod 해제 경로의 EKS 입증이다. 2026-10-16 격리 fixture에서 follower EC2를 OS 종료 없이(`skipOsShutdown=true`) 두 번 종료했다. preStop·drain 기록은 없었다. 두 번 모두 leader는 이전 IP 연결에 송신 대기가 생긴 첫 표본 뒤 10.31초·10.32초에 그 연결을 닫았다. 기존 Pod 해제는 `deleted`, `FailedAttachVolume`은 0건이었다. 대체 node는 container 시작 뒤 3.5~4.4초에 release profile을 동기화했고 leader는 새 IP에 연결했다. Broker 3개 Ready 관찰은 79.11초·55.98초였고 데이터 oracle은 PASS다. 이전 약 130초 정체는 나오지 않았다. 정확한 RTO·연속 가용성·다른 장애 유형은 입증하지 않았다. 이전 부분 관찰(preStop 실행, `node_absent`)은 그대로 둔다. [Cluster §11.34 EKS 재측정](specs/cluster-session-and-delivery.md#eks-무응답-종료-재측정-2026-10-16)을 따른다.

### 1.5 설계 미결정과 비지원 범위

[Cluster architecture의 미결정 사항](architectures/cluster-architecture.md#미결정-사항)은 새 구현 승인이 아니다. Route-index의 물리적 분산 배치, 기존 관측 RPC 밖의 새 RPC, cross-cluster migration·voter-set 변경 복원은 별도 계약 채택 전에는 착수하지 않는다. 현재 구현한 route rebuild·관측 RPC를 다시 미완료로 분류하지 않는다.

### 1.6 MQTT protocol과 client 호환성

MQTT 3.1.0 연결 거절(`MQTT-13`)은 구현했고 P0 시험에 연결했다. MQTT 3.1.0은 지원 대상이 아니므로 2026-10-17 사용자 결정에 따라 3.1.0 client의 관찰·전환·격리를 제품 출시 gate와 이 목록에서 제외한다. 이 절차는 EMQX에서 전환하는 운영자가 [PRD §9.1](requirements/PRD.md#mqtt-310-비지원과-이전-정책)에 따라 수행한다.

### 1.7 Tenant·security와 감사

- [x] **완료(제품 감사 기능·포맷 및 EKS 실측 범위)** — 감사 운영 범위(`SEC-11·NFR-17·API-08`). 표준 Audit Event v1 스키마, node-local SHA-256 해시 체인, 위변조 검출, Management API(19개 mutation, 401/403) 및 Rollout producer의 EBS PVC fsync, 감사 실패 시 503 차단, 로컬 최소 7일 버퍼를 구현하고 로컬·EKS 실측으로 완료했다(2026-10-15 `rollout-durable-audit-result.json`). 2026-10-16 사용자 결정에 따라 외부 SIEM 365일 실제 경과 관찰은 제품 릴리스 차단 조건에서 제외하고 운영 인프라(Day-2 Ops) 요건으로 관리한다. [Tenant와 security §8·§9](specs/tenancy-and-security.md#8-감사와-비밀정보)를 따른다.
- [ ] **미검증 · 독립 보안 시험 필요** — 운영 경계 침투 시험(`NFR-19`, `R-07`). 채택한 threat model과 위험 승인, 로컬 자동 사전 시험은 유지한다. NLB·TLS termination·secret 저장소·Kubernetes를 포함한 독립 시험이 남았다. [Threat model](architectures/threat-model.md)을 따른다.

### 1.8 Management API·observability와 Dashboard

- [ ] **보류 · 출시 후 조건부 범위** — P1 관리 화면·제어 API·exact-topic Metric·tenant 위임(`UI-06~09·API-07·TEN-10`). 현 출시에서는 Grafana P0 조회 대시보드를 유지한다. 재개할 때 우선순위·API 계약·UI 순서와 권한·마스킹·pagination·idempotency·감사 기준을 먼저 확정한다. [ADR-0045](decisions/ADR-0045-operations-dashboard-grafana.md)를 따른다. 완료한 P0 대시보드의 EKS 통합은 다시 열지 않는다.
- [x] **완료(원인 확인·계약 동작 수용)** — EKS cycle 1에서 quorum을 유지한 healthy leader node 2가 `unknown`으로 집계된 문제다. 2026-10-16 격리 EKS에서 observer worker의 DNS를 fault 중 측정했다. 두 CoreDNS replica를 종료 worker에 둔 재현 실행에서 UDP DNS 조회가 6.22~48.22초에 timeout됐다. 같은 구간에 node 2는 `node_query_timeout`이었고 `unknown=2`였다. DNS가 돌아온 뒤 48.5초에 node 2는 `OK`가 됐다. 관측 RPC는 query마다 peer DNS 이름으로 새로 연결한다(`peer_observability_transport.go`). 사용자 결정에 따라 CoreDNS를 서로 다른 worker·AZ에 두는 전제를 `deploy/terraform/eks.tf`에 넣었다. 전제 실행에서도 죽은 replica의 endpoint가 23.8초까지 routing에 남아 DNS timeout 54회가 나왔다. Node 2는 12개 표본 중 1개에서 timeout이었다. 따라서 전제는 구간을 줄이지만 없애지 못한다. 2026-10-16 사용자는 실패·timeout query를 `UNREACHABLE`/`unknown`으로 보고하는 현재 계약을 유지하고 이 항목을 닫기로 했다. Leader·quorum·MQTT 표본·oracle은 두 실행 모두 정상이었다. 실행은 배치마다 한 번이다. 이전 cycle 1 기록은 [Drain §10](specs/drain-and-rolling-update.md#생존-pod-고정-observer의-eks-재시험-2026-10-15)에 남긴다. [CoreDNS 재현](specs/drain-and-rolling-update.md#coredns-배치와-healthy-node-unknown의-eks-재현-2026-10-16), [EKS architecture 배치 제약](architectures/eks-ec2-deployment.md#placement와-availability-constraint)을 따른다.

### 1.9 Performance·test와 공통 출시 gate

- [ ] **Pod 장애 보완 검증 · 거절 분류·B_* 미완료** — Production workload와 절대 cutoff(`NFR-01~06·PERF-01~12`, `SPEC-TBD-08`). 2026-10-16 사용자 결정에 따라 상용 EMQX Enterprise 라이선스를 요구하는 상대 비교 조건은 필수 제품 출시 게이트에서 제외하고 선택적 외부 벤치마크로 운영한다. 필수 릴리스 게이트는 NaruMQ 자체의 5% 회귀 차단 규칙과 `EKS_EC2_HA` qualification profile에서 측정한 N-1 자립형 수용 용량(`B_*`)으로 판정한다. 남은 범위는 quota 계열 거절 3건(runner가 0x97을 commit_lag 대신 tenant 한도로 집계)의 원인·구간 분류 보완 및 EKS 전용 generator Job을 통한 `B_*` 측정·SLO 확정이다. [성능 §3.9](specs/performance-and-verification.md#39-bp-02-eks-용량-측정-계약)를 따른다.
- [ ] **미검증 · K8s/AWS 출시 비교** — Production benchmark profile(`PERF-01·05~07·10~12·NFR-06`). 상용 EMQX 라이선스 비교는 선택적 외부 벤치마크로 운영하며, 필수 릴리스는 NaruMQ 자체 N-1 ↔ N 회귀(5% 미만) 및 failure recovery(BP-06)·rolling drain(BP-07) 기준으로 검증한다. [성능 §§3~6](specs/performance-and-verification.md#3-benchmark-공통-계약)을 따른다.
- [ ] **미해결 · 로컬/CI** — 최근 전체 local 검증 실패를 해결한다. 과거 원격 CI·coverage·mutation 통과는 보존하지만 이후 집중 시험 통과로 전체 실패를 지우지 않는다. [마지막 local 기록](../.artifacts/verification/eks-resume-20261015T000337Z-21df3b/local-verification.json), [observer 검증 범위](specs/drain-and-rolling-update.md#eks-worker-observer-보완-기준)를 따른다. 2026-10-15 commit `2b23116`에서 `GOMAXPROCS=4 go run ./tools/verify local`은 모든 package 시험이 통과한 뒤 coverage 단계에서 멈췄다(line 83.84% < 85%, branch 79.19% < ratchet 80.03%). 첫 시도는 `internal/adapters/raft` `TestWALSyncObserverReportsSuccessfulReadyFsyncs` 실패로 멈췄다([Cluster §11.34](specs/cluster-session-and-delivery.md#1134-dynamic-peer-tcp-생존-확인-수용-매핑)). 2026-10-16 ADR-0059·ADR-0060 변경을 포함한 작업 tree에서도 모든 package 시험이 통과한 뒤 같은 단계에서 멈췄다(line 83.82%, branch 79.14%). 해제 조건은 coverage 미달 원인 분석과 보완, 간헐 실패 원인 수정, 그 뒤 전체 gate의 실제 통과 결과다.
- [ ] **미검증 · DEP P0 전체 수용 필요** — Merge 차단 최종 gate(`TEST-10`). 관리자 포함 merge 차단 설정과 결함 probe는 검증했다. 남은 DEP P0 예외 10개는 개별 수용을 마칠 때만 제거한다. 오래된 예외 사유는 최신화하되 예외 수를 줄여 통과시키지 않는다. [성능 §8.9](specs/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거), [P0 예외](../tests/conformance/p0-requirement-exceptions-v1.json)를 따른다.

<a id="110-실행-옵션설정-구조"></a>
### 1.10 실행 옵션·설정 구조

채택된 G1~G10은 완료했다. 기준과 실행 증거는 [Process configuration v1](contracts/process-configuration-v1.md), [설정 스펙 §6](specs/configuration-and-runtime-options.md#6-수용-시험)에 둔다. [ADR-0052](decisions/ADR-0052-configuration-tiers-and-cluster-policy.md)의 나머지 `Proposed` 범위는 승인 없이 새 작업으로 추가하지 않는다.

[PRD §16 선택적 adapter](requirements/PRD.md#16-단계별-제공-범위)의 HTTP webhook·MQTT bridge·Kafka와 DEP-24 ECS도 필요성·별도 수용 기준을 승인한 뒤에만 착수한다.

항목을 마치면 기준 문서의 수용 매핑과 실제 실행 결과를 먼저 갱신한다. 미실행이나 미승인 범위를 완료로 표시하지 않는다.
