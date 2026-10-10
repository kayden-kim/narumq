# ADR-0051: Cluster 업데이트는 in-place와 교체형 rolling을 함께 지원한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (2026-10-10 사용자 결정; 업데이트 방식과 선택 기준만) |
| 결정일 | 2026-10-10 |
| 결정자 | NaruMQ 프로젝트 사용자 |
| 관련 요구사항 | [PRD DEP-01~DEP-12·DEP-16~DEP-24, CLU-11](../requirements/PRD.md#97-안전한-종료와-rolling-update) |
| 관련 스펙·계약 | [Drain과 rolling update §7.2](../specs/drain-and-rolling-update.md#72-업데이트-방식과-환경별-선택), [Raft membership v2](../contracts/raft-membership-v2.md), [Cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md), [Cluster backup v3](../contracts/cluster-backup-v3.md) |
| 관련 아키텍처 문서 | [Cluster architecture](../architectures/cluster-architecture.md), [EKS on EC2 deployment](../architectures/eks-ec2-deployment.md) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

1. 계획된 버전 업데이트는 한 번에 한 node씩 진행하는 rolling만 지원한다. 공식 방식은 두 가지다.
   - **In-place 교체:**
     1. 대상 node를 drain한다.
     2. `SAFE_TO_STOP`을 확인한다.
     3. 프로세스를 종료한다.
     4. 같은 `nodeId`, 같은 cluster config, 같은 데이터 디렉터리로 새 release를 시작한다.
     5. READY가 된 뒤 다음 node로 진행한다.
   - **교체형 교체:**
     1. 새 release node를 새 `nodeId`와 빈 데이터 디렉터리로 stage·join·promotion해 voter를 4개로 만든다.
     2. 기존 node를 drain하고 `SAFE_TO_STOP`을 확인한다.
     3. 기존 node를 planned removal로 제거해 voter를 3개로 되돌린다.
     4. 다음 node로 진행한다.
2. 이전 node의 데이터 디렉터리를 새 프로세스에 다시 연결할 수 있으면 in-place를 기본으로 쓴다. 다음 경우에는 교체형을 쓴다.
   - 데이터 디렉터리를 다시 연결할 수 없는 플랫폼. 예: ECS 서비스가 관리하는 EBS.
   - Host, volume 또는 가용 영역을 바꿔야 하는 경우.
   - 빈 디렉터리에 기존 `nodeId`를 다시 쓰지 않는다([ADR-0024](ADR-0024-joiner-voter-volume-loss-admission.md)).
3. 교체형은 committed voter가 정확히 3개일 때만 시작한다. Voter가 4개인 상태에서 다음 node를 추가하지 않는다. Joiner 승인은 기존 voter 3개를 전제로 한다([ADR-0024](ADR-0024-joiner-voter-volume-loss-admission.md)의 결과와 비용, [ADR-0029](ADR-0029-replication-factor-write-quorum-profile.md) §1 6항).
4. 두 방식 모두 [ADR-0018](ADR-0018-release-compatibility-window.md)의 N/N-1 release window 안에서만 혼합 운영한다.
   - 새 feature activation([ADR-0032](ADR-0032-feature-activation-approval-policy.md))은 모든 node를 교체한 뒤에만 한다.
   - Activation 전까지는 같은 방식으로 N-1 release를 다시 배포해 rollback한다.
5. 새 클러스터를 따로 만들고 load balancer를 전환하는 Blue/Green 업데이트는 지원하지 않는다.
6. 이 ADR은 업데이트 방식과 선택 기준만 정한다. 다음 항목은 각 기준 문서가 정한다.
   - Kubernetes rollout controller와 Pod 삭제 승인은 [ADR-0006](ADR-0006-external-rollout-controller.md)을 따른다.
   - ECS lifecycle adapter는 PRD DEP-24를 따른다.
   - Drain 운영값은 `SPEC-TBD-04`를 따른다.
   - 자동화 도구는 이 ADR의 범위가 아니다.

## 2. 배경

MQTT 연결은 오래 유지된다. Persistent session, subscription, inflight QoS 1/2, offline queue와 retained message는 Raft log와 node별 WAL·snapshot에 있다. 업데이트는 이 상태를 같은 클러스터 안에서 보존해야 한다.

플랫폼마다 데이터 디렉터리 유지 방식이 다르다. VM, Docker Compose named volume, EKS StatefulSet PVC는 같은 데이터 디렉터리를 다시 연결할 수 있다. ECS 서비스 task에 붙는 EBS는 task마다 새 volume을 사용하며 task가 끝나면 삭제된다([AWS ECS EBS 문서](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ebs-volumes.html)).

EMQX Operator의 `updateStrategy.type: Recreate`는 문서에서 "blue-green upgrade"라고 부른다. 이 방식은 새 버전 node를 같은 EMQX 클러스터에 추가하고, Node Evacuation으로 기존 node의 연결과 session을 옮긴 뒤, 기존 node를 제거한다([EMQX Operator API Reference v2](https://docs.emqx.com/en/emqx/latest/deploy/kubernetes/operator/reference/v2-reference.html), [EMQX Node Evacuation](https://docs.emqx.com/en/emqx/latest/deploy/cluster/rebalancing.html)). 이 방식은 클러스터를 둘로 나누지 않는다. NaruMQ의 교체형과 같은 구조다.

## 3. 결정 요인

1. Durable ACK RPO 0과 persistent session 보존.
2. 교체 중 quorum 유지와 추가 장애 여유.
3. 지원·후보 플랫폼의 volume 재연결 제약.
4. 새 broker 기능 없이 기존 drain·membership·release window 재사용.

## 4. 선택 근거

In-place는 꺼진 동안의 log만 따라잡는다. 그래서 빠르고 identity가 바뀌지 않는다. 대신 node가 꺼진 동안 voter가 2개라 추가 장애를 견디지 못한다.

교체형은 교체 중 voter 3개 이상을 유지한다. Volume을 다시 연결할 수 없는 플랫폼에서도 동작한다. 대신 매번 전체 상태를 새 node로 복제하고 `nodeId`를 새로 발급한다. 퇴역한 ID는 재사용하지 않는다.

두 방식은 같은 drain·`SAFE_TO_STOP` barrier([ADR-0016](ADR-0016-node-drain-and-safe-to-stop.md))와 membership([ADR-0010](ADR-0010-dynamic-cluster-membership.md))을 쓴다. 따라서 broker 코드와 wire를 바꾸지 않는다.

새 클러스터를 따로 만드는 Blue/Green은 지원하지 않는다. 클러스터 사이에 상태를 옮기는 경로가 없다. Restore는 같은 cluster와 정확히 세 bootstrap voter만 지원한다([Cluster backup v3](../contracts/cluster-backup-v3.md)). 상태를 옮기지 않고 전환하면 persistent session과 미전달 QoS 1/2 메시지를 잃어 PRD의 RPO 0 기준을 어긴다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| In-place만 지원 | 절차가 하나이고 빠르다. | ECS 서비스 관리 EBS처럼 volume을 다시 연결할 수 없는 플랫폼을 지원하지 못한다. | 결정 요인 3을 충족하지 못한다. |
| 교체형만 지원 | 절차가 하나이고 교체 중 voter 3개를 유지한다. | 매 업데이트마다 전체 상태를 복제하고 `nodeId`가 계속 바뀐다. | Volume을 다시 연결할 수 있는 환경에서 비용이 불필요하게 크다. |
| 별도 클러스터 Blue/Green | 전환과 되돌리기가 단순해 보인다. | 클러스터 간 상태 이전 경로가 없다. | 결정 요인 1을 어긴다. |

## 6. 결과

### 긍정적 결과

- 모든 후보 환경에서 선택할 공식 업데이트 절차가 생긴다.
- 별도 클러스터 Blue/Green 요구를 거절할 근거가 생긴다.

### 부정적 결과와 비용

- 운영자가 플랫폼에 따라 절차를 선택해야 한다.
- 교체형은 전체 상태 복제 시간과 새 volume 비용이 든다.

### 후속 작업

- 교체형 조합(추가 → drain → planned removal)의 3-node process 수용 시험.
- 플랫폼별 실행 절차(how-to).
- ECS lifecycle adapter(DEP-24).

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| In-place 교체가 세션과 상태를 보존한다 | 기존 process 수용 시험 | Drain 뒤 session resume과 restart `CLEAR`, N-1↔N 혼합 운영과 rollback | [Drain §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)의 `TestDynamicClusterNodeDrainProcessAcceptance`, [Cluster §11.17](../specs/cluster-session-and-delivery.md#1117-n-1n-rolling-upgraderollback-수용-매핑)의 `TestDynamicClusterReleaseCompatibilityProcessAcceptance` |
| 교체형 구성 요소가 동작한다 | 기존 process 수용 시험 | 새 ID 추가·승격과 planned removal이 voter 3 미만을 만들지 않는다 | [Cluster §11.20](../specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑), [Cluster §11.27](../specs/cluster-session-and-delivery.md#1127-replication-factorwrite-quorum-profile-수용-매핑) |
| 교체형 조합이 세션과 durable 메시지를 보존한다 | 추가 → drain → planned removal 3-node process 시험 | Session 손실 0, QoS 1/2 gap 0 | 미실행. 시험이 없다. |

## 8. 재검토 조건

- 교체형 조합 수용 시험이 실패한다.
- Replication profile이 3/2 밖으로 확장된다.
- Joiner 승인 규칙이 voter 3개 전제를 바꾼다.
- 클러스터 간 상태 이전 계약이 채택된다.
