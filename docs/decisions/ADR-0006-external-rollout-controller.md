# ADR-0006: 전용 rollout controller가 Pod 삭제를 승인한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 제안일 | 2026-08-16 |
| 결정일 | 2026-10-13 |
| 결정자 | 사용자 |
| 관련 요구사항 | DEP-01~DEP-24, CLIENT-03~CLIENT-09, CLU-11 |
| 관련 스펙 | [Drain과 rolling update](../specs/drain-and-rolling-update.md) |
| 관련 Architecture | [EKS on EC2 deployment](../architectures/eks-ec2-deployment.md), [System overview](../architectures/system-overview.md) |
| 관련 결정 | [ADR-0007](ADR-0007-aws-production-deployment-platform.md) |
| 관련 Contract | [Management API v1](../contracts/management-api-v1.md), [MQTT client compatibility v1](../contracts/mqtt-client-compatibility-v1.md) |
| 관련 해설 | [안전한 노드 drain과 Will](../explainers/safe-node-drain-and-will.md) |

## 1. 결정

NaruMQ 전용 rollout controller를 계획된 broker node 교체의 유일한 Pod 삭제 승인 주체로 사용한다.

초기 production deployment profile인 Amazon EKS on EC2에서도 이 Kubernetes controller와 `SAFE_TO_STOP` gate를 사용한다. Amazon ECS는 이 결정을 그대로 충족하는 것으로 간주하지 않으며 별도 lifecycle adapter와 후속 ADR이 필요하다.

Controller는 Pod 삭제를 요청하기 전에 management API로 drain을 시작하고 `SAFE_TO_STOP`을 확인한다. Drain 실패, timeout 또는 `unknown` barrier가 있으면 현재와 다음 node의 교체를 중단한다.

Argo CD는 NaruMQ rollout resource의 desired version을 적용하고 health를 관찰한다. Broker Pod를 일반 workload rolling update로 직접 교체하지 않는다.

`preStop`은 중복 가능한 최후 방어선으로 유지한다. Pod finalizer는 이미 시작된 삭제를 지연할 뿐 삭제 요청 전 승인을 제공하지 않으므로 primary gate로 사용하지 않는다.

## 2. 배경

Kubernetes `preStop`은 termination grace period 안에서 실행된다. Hook이 실패하거나 오래 걸려도 Pod는 grace period 이후 종료된다. 따라서 hook result만으로 “안전하지 않으면 삭제하지 않는다”는 제품 요구를 충족할 수 없다.

Kubernetes finalizer는 deletion timestamp가 설정된 resource의 실제 제거를 지연할 수 있다. 그러나 삭제 요청 뒤에는 object를 되살릴 수 없고, Pod termination 흐름이 이미 시작될 수 있다. NaruMQ는 삭제 요청 자체를 `SAFE_TO_STOP` 뒤로 미뤄야 한다.

## 3. 결정 요인

1. `SAFE_TO_STOP` 전 Pod delete 요청이 없어야 한다.
2. Drain 실패가 다음 node update를 차단해야 한다.
3. Controller crash 뒤 operation 상태를 복구해야 한다.
4. N-1↔N upgrade와 rollback을 같은 workflow로 처리해야 한다.
5. Argo CD와 Kubernetes lifecycle 의미를 왜곡하면 안 된다.

## 4. Controller 책임

- 대상 cluster와 node의 현재·목표 version을 관리한다.
- 한 번에 하나의 quorum node만 DRAINING으로 만든다.
- Drain operation ID, 상태, deadline과 barrier evidence를 persistent resource status에 기록한다.
- `SAFE_TO_STOP` 뒤에만 Pod delete 또는 workload replacement를 요청한다.
- 새 Pod가 sync와 readiness를 통과한 뒤 다음 node로 이동한다.
- Failure reason, retry 여부와 operator action을 상태에 남긴다.
- Controller restart 뒤 같은 operation을 idempotent하게 재개한다.

정확한 custom resource schema와 reconciliation contract는 후속 contract로 작성한다.

## 5. 대안

| 대안 | 장점 | 제외 이유 |
| --- | --- | --- |
| `preStop`만 사용 | 배포가 단순하다. | Grace period가 시작된 뒤이며 실패가 Pod 삭제를 막지 못한다. |
| Argo CD hook만 사용 | 기존 GitOps 도구 활용 | Node별 durable state, retry와 barrier evidence를 장기 상태로 관리하기 어렵다. Controller health integration에는 사용한다. |
| Pod finalizer | Kubernetes API에 deletion barrier 표현 | Delete 요청 뒤에 작동한다. 삭제 전 drain 승인이라는 요구와 순서가 다르다. |
| 수동 operator 절차 | 초기 구현이 적다. | 반복 가능성, 감사와 연속 100회 test gate를 만족하기 어렵다. 긴급 복구 수단으로만 남긴다. |
| 일반 StatefulSet rolling update | Kubernetes 기본 기능 | MQTT session checkpoint와 safe-to-stop barrier를 알지 못한다. |

## 6. 결과

- 계획 배포가 별도 product component가 된다.
- Controller RBAC, availability, upgrade와 관측성 책임이 추가된다.
- Argo CD는 controller resource health를 통해 실패를 표시할 수 있다.
- Pod template의 `preStop`, grace period, PDB와 topology 규칙은 계속 필요하다.
- Force delete는 제품 보장 밖의 비상 조치이며 데이터·Will 위험을 명시해야 한다.

## 7. 검증

1. 모든 drain phase에서 controller를 kill하고 재시작해 operation이 수렴하는지 확인한다.
2. Drain timeout·failure·API timeout에서 Pod delete call이 0인지 확인한다.
3. `SAFE_TO_STOP` 뒤 delete와 새 node readiness 사이에 다음 node가 시작되지 않는지 확인한다.
4. N-1↔N upgrade와 rollback을 연속 100회 수행한다.
5. `preStop` 중복 실행과 controller operation이 같은 drain을 중복 생성하지 않는지 확인한다.
6. Argo CD health가 Failed와 blocked 상태를 정확히 표시하는지 확인한다.

## 8. 재검토 조건

- Kubernetes workload controller가 외부 readiness gate로 delete 전 hook을 공식 제공한다.
- Argo CD가 동일한 persistent state·idempotency·barrier를 검증 가능하게 제공한다.
- NaruMQ 배포 단위가 Kubernetes Pod가 아닌 형태로 바뀐다.

## 9. 참고 자료

- [Kubernetes Container Lifecycle Hooks](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/)
- [Kubernetes Finalizers](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/)
- [Kubernetes Custom Resources](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/)
