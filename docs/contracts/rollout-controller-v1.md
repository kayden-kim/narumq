# Rollout controller v1 Contract

이 문서는 NaruMQ rollout controller의 Custom Resource, reconcile, drain gate와 감사 이벤트 경계를 정한다. 2026-10-14 사용자가 위임한 구현 결정으로 이 계약을 채택했다. 이 계약은 [ADR-0006](../decisions/ADR-0006-external-rollout-controller.md)의 `SAFE_TO_STOP` 삭제 장벽을 구체화하며 production/EKS 수용 완료를 뜻하지 않는다.

| 항목 | 값 |
| --- | --- |
| 이름과 버전 | `narumq-rollout-controller` 1.0.0 |
| 상태 | Active — 2026-10-14 delegated decision |
| 소유자 | NaruMQ project |
| 만드는 쪽(Producer) | GitOps/운영자(rollout 의도), NaruMQ rollout controller(status·audit) |
| 사용하는 쪽(Consumer) | NaruMQ rollout controller, Kubernetes workload API, Argo CD health integration, broker Management API, audit consumer |
| 방향 | `NaruMQRollout` spec/status; controller → Management API, OCI registry, Kubernetes workload API, durable audit log |
| 기준 artifact | [NaruMQRollout CRD](../../deploy/kubernetes/rollout-controller/crd.yaml) |
| 기준 스펙 | [Drain과 rolling update](../specs/drain-and-rolling-update.md) §§6–9 |
| 관련 ADR | [ADR-0006](../decisions/ADR-0006-external-rollout-controller.md) (Accepted) |
| 관련 계약 | [Management API v1](management-api-v1.md), [Cluster release compatibility v1](cluster-release-compatibility-v1.md), [Audit event v1](audit-event-v1.md) |
| 관련 요구사항 | DEP-11, DEP-18; DEP-13·DEP-14은 preStop/grace 연동 경계 |
| 호환성 범위 | Kubernetes `EKS_EC2_HA` 3-voter `StatefulSet`; ECS adapter는 별도 결정과 계약 대상 |
| 관련 시험 | [Drain spec §9](../specs/drain-and-rolling-update.md#9-수용-시험), [controller local acceptance mapping §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) |

## 1. 적용 범위와 기준

이 계약은 Argo CD가 적용한 rollout 의도를 controller가 순차적인 broker Pod 교체로 실행할 때 쓴다. Broker drain 안전성을 다시 정의하지 않는다. Broker의 committed `SAFE_TO_STOP` 결과만 종료 안전성의 권위 있는 근거다.

| 기준 | 규범 동작 | 기준 문서 |
| --- | --- | --- |
| 삭제 승인 주체 | 전용 NaruMQ rollout controller만 계획된 broker Pod 삭제를 요청한다. Argo CD는 `NaruMQRollout`과 `OnDelete` workload template을 적용하고 controller health를 관찰한다. | ADR-0006 §§1,3 |
| 삭제 장벽 | Controller는 drain을 시작하고 `SAFE_TO_STOP`을 확인하기 전에는 Pod delete를 요청하지 않는다. `preStop`과 Pod finalizer는 이 gate를 대체하지 않는다. | ADR-0006 §1; Drain spec §7 |
| 실패 안전 | Drain failure, timeout, API 결과 불명 또는 barrier `unknown`이면 현재 Pod의 delete 요청은 0건이어야 하고 다음 node로 진행하지 않는다. | ADR-0006 §§1,7; Drain spec §§6–7; DEP-11·DEP-18 |
| 직렬화 | 한 번에 quorum node 하나만 DRAINING한다. Replacement가 sync와 readiness를 통과하기 전에는 다음 node를 시작하지 않는다. | ADR-0006 §4; Drain spec §7 |
| 재시작 | Controller 재시작 뒤 CR status에서 operation ID, idempotency key, deadline, Pod UID/resourceVersion과 barrier evidence를 복구한다. | ADR-0006 §§3–4, §7 검증 1 |
| Version window | 모든 non-retired voter와 목표 release는 canonical version이며 같은 major에서 minor 차이가 0 또는 1이어야 한다. | Cluster release compatibility §§1,3,8; DEP-18 |
| 미확인 barrier | 필수 barrier 값 하나라도 unknown이면 성공으로 취급하지 않는다. Timeout은 `FAILED`/`drain_timeout`이며 삭제 승인이 아니다. | Drain spec §6 |
| 비상 삭제 | Force delete는 제품 보장 범위 밖이며 데이터 및 Will 위험이 있다. | ADR-0006 §6 |
| 이미지 provenance | Target·설치된 각 Pod의 immutable OCI manifest digest를 조회해 digest 검증된 OCI config label의 canonical release identity를 확인한다. CR 값만으로 release identity를 확정하지 않는다. | 이 계약 §2 |

Custom Resource는 `apiVersion: narumq.io/v1alpha1`, `kind: NaruMQRollout`이며 namespaced다. 지원 workload는 같은 namespace의 replicas=3, `updateStrategy.type=OnDelete`인 `StatefulSet` 하나다. StatefulSet ordinal `n`은 논리 node ID `n+1`에 대응한다. Controller는 StatefulSet spec을 고치지 않는다.

## 2. Reconciliation contract

1. Controller는 rollout CR, target StatefulSet, Pod, OCI metadata와 committed broker member profiles를 읽는다. 대상이 모호하거나 required evidence가 없으면 차단 상태를 기록하고 workload를 바꾸지 않는다.
2. Target image는 `repository@sha256:<64 lowercase hex>` OCI **platform manifest** digest여야 한다. OCI index/list digest와 mutable tag는 거절한다. Release build는 `org.opencontainers.image.version`에 canonical version, `io.narumq.features`에 sorted unique canonical feature list를 OCI config label로 기록한다. 두 label은 broker executable의 build linker inputs와 동일한 값에서 생성되며 config·manifest digest를 통해 immutable image에 묶인다. Controller는 OCI manifest 및 config descriptor digest를 검증하고 labels만 읽는다. Image filesystem layer를 내려받거나 압축 해제하지 않는다. 성공적으로 검증한 digest metadata만 process-local cache에 보관하며 cache는 4개 digest로 제한하고 실패 결과는 저장하지 않는다. |
3. `spec.target.releaseVersion`과 `spec.target.features`는 실제 target OCI labels와 정확히 같아야 한다. `features`는 `release.ValidFeatureSet` 규칙을 따르고 canonical sort/order를 가져야 한다. `spec.target.image`는 StatefulSet의 `narumq` container template image와 정확히 같아야 한다.
4. `GET /api/v1/cluster/features`의 모든 non-retired voter가 StatefulSet의 3개 ordinal과 정확히 대응해야 한다. 각 Pod `spec.image`, runtime `imageID`, OCI metadata와 committed `releaseVersion/features`가 일치해야 한다. 모든 voter·target version은 같은-major 인접-minor window에 있어야 한다. Committed active feature는 모든 현재 voter와 target의 feature set에 있어야 한다. 불일치·unknown이면 delete 0건이다.
5. Controller는 rollout과 workload를 대상으로 durable Kubernetes Lease claim을 얻는다. Controller Deployment는 `replicas=1`, `strategy.type=Recreate`이며 Kubernetes Lease leader election을 사용한다. Target Lease는 완료까지 다른 CR이 소유권을 가져갈 수 없다. 모든 status write는 resourceVersion CAS로 한다. 소유권을 검증하지 못하면 drain/delete를 실행하지 않는다.
6. Controller는 현재 ordinal에 대한 drain idempotency key와 operation intent를 먼저 CR status에 저장한다. 이후 drain GET을 먼저 조회한다. Operation이 없을 때만 POST하며, API 결과가 불명확하면 같은 key로 GET/POST replay를 수행한다. Drain GET은 주기적으로 조회한다. 유효하고 완전한 `COMPLETED`/`SAFE_TO_STOP` observation만 승인 조건이다. Broker가 준 시각(`startedAt`, `deadlineAt`)은 status가 저장하는 정밀도(UTC 초 단위)로 줄여 저장하고 비교한다. 저장된 deadline은 broker 값보다 늦어지지 않는다. 바뀐 내용이 없는 조회는 status를 다시 쓰지 않는다. Status를 쓰면 resourceVersion이 바뀌고 watch event가 생겨 조회 주기보다 먼저 다시 조정하기 때문이다.
7. Controller는 승인 직전 drain 상태와 Pod UID/resourceVersion을 다시 읽는다. Delete approval audit event는 `rollout UID + ordinal + Pod UID + Pod resourceVersion + 결정 key + action`으로부터 결정적 event ID를 만든다. 결정 key는 drain idempotency key다. 단, §2.9의 `SAFE_TO_STOP` 재승인 뒤에는 drain idempotency key에 새 `retryToken` hash를 더한다. Event는 Audit Event v1 producer·consumer schema와 secret-redaction 규칙을 따른다. Controller는 event를 durable sink에 idempotent append·fsync하고 delete intent와 drain evidence를 CR status에 resourceVersion CAS로 저장한 뒤 Kubernetes DELETE를 UID·resourceVersion precondition과 함께 호출한다. DELETE 결과가 불명확하면 다음 node를 진행하지 않고 Pod UID·deletionTimestamp를 재조회한다. 현재 Pod와 UID가 같고 deletion이 시작되지 않았을 때만 같은 precondition으로 재시도한다.
8. Controller는 동일 StatefulSet ordinal의 새 Pod UID, target image digest, Pod Ready, Pod IP의 `/health/ready` HTTP 200, committed target release profile을 모두 확인한다. 이 확인 전에는 다음 ordinal을 drain하지 않는다. 완료 전 실패하거나 timeout/unknown이면 차단한다.
9. Drain `FAILED` 뒤 controller는 자동으로 다음 node로 진행하거나 같은 operation을 새로 만들지 않는다. 승인 직전 증거를 다시 확인하는 `DrainSafe`·`DeleteApproved`에서 승인 deadline이 지나면 차단 상태로 기다리지 않고 `Failed`/`drain_timeout`으로 끝낸다. Operator는 `spec.retryToken`을 새 불투명 값으로 바꾸어야 retry한다. Retry는 마지막으로 관찰한 drain에 따라 둘로 나뉜다.
   - 마지막 drain이 `SAFE_TO_STOP`이 아니면 controller는 먼저 기존 failed drain을 idempotently 취소하고 새 intent/key로 같은 ordinal을 재시작한다.
   - 마지막 drain이 `SAFE_TO_STOP`이면 취소하지 않고 같은 drain을 다시 승인한다. Broker는 `SAFE_TO_STOP` drain을 취소하지 않기 때문이다([Drain spec §2.3](../specs/drain-and-rolling-update.md#23-취소)). Controller는 같은 drain idempotency key로 drain을 조회한다. 응답이 저장된 operation ID·node의 `COMPLETED`·`safeToStop=true`이고, 같은 ordinal의 Pod가 저장된 UID이며 삭제 중이 아니어야 한다. 그러면 drain idempotency key와 operation ID를 유지하고, Pod resourceVersion과 `SAFE_TO_STOP` 관찰 시각을 지금 값으로 바꾼 뒤 `DrainSafe`로 돌아간다. 이때 승인 deadline `approvalDeadlineAt`을 지금부터 10분(기본 drain timeout과 같은 값) 뒤로 저장한다. 이후 승인과 DELETE는 일반 경로와 같은 fresh 조회·Pod UID/resourceVersion·preflight·audit 조건을 다시 확인한다. 승인 deadline은 broker의 drain `deadlineAt`으로 줄이지 않는다. Broker deadline은 `SAFE_TO_STOP`에 도달하는 시한이고, 이미 도달한 drain의 삭제 승인 시한이 아니기 때문이다. Pod가 없거나 삭제 중이거나 UID가 다르면, 실패 전에 delete intent를 저장한 경우에만 기존 DELETE의 결과로 보고 `AwaitingReplacement`로 간다. Delete intent가 없으면 감사되지 않은 교체이므로 `pod_identity_changed`로 차단한다. 조회가 실패하거나 다른 operation·상태이면 `retry_cancel_unconfirmed`로 차단한다.

   `approvalDeadlineAt`이 없으면 승인 deadline은 저장된 drain deadline(`drainDeadlineAt`)이다. `spec.abort=true`는 안전하게 취소 가능한 drain만 취소하며, 이미 `SAFE_TO_STOP`이면 workload delete 없이 `Aborted`로 멈춘다. Force delete는 지원하지 않는다. `Failed` status의 `failureReason`과 Ready condition message는 실패 원인 뒤에 operator action을 고정 문구로 붙인다. 마지막으로 관찰한 drain이 `SAFE_TO_STOP`이 아니면 새 `spec.retryToken`으로 취소 뒤 retry하거나 `spec.abort=true`로 멈추라고 안내한다. 마지막으로 관찰한 drain이 `SAFE_TO_STOP`이면 새 `spec.retryToken`으로 같은 drain의 삭제를 fresh 증거로 다시 승인하거나 `spec.abort=true`로 멈추라고 안내한다. `rollout.failed` 감사 event의 `operatorAction`도 같은 문구이며 실패 원인은 `failureReason`에 따로 둔다. `rollout.failed`·`rollout.pod.delete_denied`의 event ID도 §2.7의 결정 key를 쓴다.
10. 전체 ordinal이 target digest와 release profile에 도달하면 CR phase를 `Completed`로 바꾸고 target Lease를 해제한다. Secret·token·registry credential은 status, event, log에 기록하지 않는다.
11. Controller는 처음 조정할 때 deletion finalizer `narumq.io/rollout-protection`을 설치하고, 삭제가 요청된 CR이 `Aborted`·`Completed`가 되면 해제한다. 두 write는 `metadata.finalizers`만 바꾸는 merge patch이며 resourceVersion 잠금을 건다. Controller는 CR spec 전체를 다시 쓰지 않는다. 전체를 다시 쓰면 Git에 적은 `abort: false` 같은 명시적 zero 값이 사라져 Argo CD가 CR을 계속 `OutOfSync`로 판단한다.

CRD spec은 `workloadRef.name`, `target.image`, `target.releaseVersion`, `target.features`, `imagePullSecretRef.name`, `management.endpoint`, `management.credentialSecretRef.name`, `retryToken`, `abort`를 사용한다. Secret은 같은 namespace에 있어야 하며 registry secret은 `kubernetes.io/dockerconfigjson`, management secret은 key `token`을 제공한다. Management API path는 contract에 고정하고 endpoint는 scheme/host/base URL만 지정한다.

Kubernetes Lease leader election은 controller process 하나만 reconcile하도록 한다. Target Lease는 동일 StatefulSet을 가리키는 복수 CR을 직렬화한다. Target Lease가 다른 CR UID에 묶여 있거나 소유권을 확인할 수 없으면 자동 탈취하지 않고 차단한다. 이 보수적 잠금은 중단 controller의 자동 복구보다 중복 삭제 방지를 우선한다.

Argo CD custom health mapping은 phase `Completed`만 `Healthy`, `Failed`·`Aborted`를 `Degraded`, 알려진 진행·차단 phase를 `Progressing`으로 반환한다. Unknown phase나 status가 없으면 `Progressing`이다.

## 3. Acceptance matrix

아래 시험은 controller producer/consumer acceptance다. 시험 파일이 존재한다는 이유만으로 통과로 표시하지 않는다. 실행 결과와 환경은 [Drain spec §9](../specs/drain-and-rolling-update.md#9-수용-시험)의 증거 표에 연결한다.

| Scenario | Acceptance 조건 | 기준 | 상태 |
| --- | --- | --- | --- |
| 정상 단일 node 교체 | `SAFE_TO_STOP` 전 delete 0; 승인 뒤 해당 UID만 삭제; replacement readiness 전 다음 drain/delete 없음. | ADR-0006 §7 검증 3; Drain spec §§7,9 | Local Kind PASS와 2026-10-15 재개 EKS 정상 경로 PASS: 실제 세 broker UID 교체·target digest·Ready, 해당 이전 UID 세 개의 완전한 SAFE barrier approval 감사 기록을 확인했다. Synthetic fixture 범위이며 production 최대 부하 수용은 아니다. [Drain §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑). |
| Drain failure / timeout | 실패·deadline 만료 시 현재 delete 0, 다음 node 진행 0; status에 실패와 operator action 표시. `SAFE_TO_STOP` 뒤 실패의 retry는 같은 drain을 새 승인 deadline 안에서 fresh 증거로 다시 승인한다. | DEP-11·DEP-18; ADR-0006 §7 검증 2 | **2026-10-16 EKS PASS**. 실제 broker drain `FAILED`와, drain GET 장애 중 저장된 deadline 만료에서 `Failed`/`drain_timeout`, operator action, delete 0, 다른 node drain 요청 0을 확인했다. EKS 시험 뒤 `DrainSafe`·`DeleteApproved`에서 deadline이 지나면 `barrier_unknown`으로 계속 차단되던 결함을 고쳤다. 2026-10-17 사용자 결정으로 `SAFE_TO_STOP` 뒤 retry는 같은 drain을 재승인한다(§2.9). **2026-10-17 EKS PASS**: 실제 `SAFE_TO_STOP` 뒤 audit 장애로 승인을 막자 broker deadline에서 `Failed`/`drain_timeout`과 재승인 안내가 표시됐다. Broker deadline 뒤 새 `retryToken`은 같은 key·operation으로 `DrainSafe`에 돌아가 새 `approvalDeadlineAt`(재승인 뒤 10분)을 저장했고, 그 시한에 다시 `Failed`가 됐다. 두 실패 동안 delete와 node 1 drain POST·취소는 0건이었다. Audit 파일 복구와 controller 재시작 뒤 셋째 token으로 재승인했고 세 broker가 교체됐다. Pod resourceVersion 변경 채택, `pod_identity_changed`·`retry_cancel_unconfirmed` 차단, `DeleteApproved`의 deadline 실패는 EKS에서 만들지 않았고 단위 시험이 판정한다. [2026-10-16 기록](../verification/drain-and-rolling-update.md#external-rollout-controller-eks-결함-matrix-수용-2026-10-16), [2026-10-17 기록](../verification/drain-and-rolling-update.md#재승인-retry와-argo-cd-적용-교체-eks-재시험-2026-10-17). |
| Barrier unknown / API unavailable | unknown·GET timeout·malformed/incomplete response는 승인하지 않고 현재 delete 0, 다음 node 진행 0. | ADR-0006 §1; Drain spec §6 | **2026-10-16 EKS PASS**. Controller 전용 fault proxy로 drain GET에 incomplete, malformed, HTTP 503, 응답 지연을 차례로 주입했다. 각 구간에서 실제 broker drain은 `DRAINING`, rollout은 `Draining`이었고 delete 0, 다른 node drain 요청 0이었다. 이어 저장된 deadline에서 `drain_timeout`으로 실패했다. 2026-10-15의 drain HTTP 503 부분 PASS도 유지한다. |
| OCI/version preflight 거절 | 잘못된 digest, OCI metadata/CR/profile mismatch, 다른 major, minor delta ≥2, active-feature 누락, 미확인 voter는 workload mutation 0. | Release compatibility §§1,3; DEP-18 | Local Kind와 2026-10-15 EKS 부분 PASS: caller `0.0.2` 대 OCI `0.0.1` mismatch에서 세 broker UID 불변. 다른 preflight 거절 조합은 EKS에서 미검증이며 단위 시험이 판정한다. |
| Controller restart | drain 요청·대기·승인·DELETE 전후·replacement 대기에서 restart해도 동일 operation/key와 evidence를 재사용하고 한 번만 해당 UID를 교체. | ADR-0006 §4, §7 검증 1,5 | **2026-10-16 EKS PASS(주입한 경계)**. 2026-10-15 active drain restart에 더해, drain POST 결과가 불명인 동안의 restart(새 leader가 같은 key로 POST를 replay하고 broker operation을 채택)와 DELETE 결과가 불명인 동안의 restart(같은 attempt key·UID/RV로 재시도, deletionTimestamp 뒤 중복 DELETE 0)를 확인했다. 승인 직전과 replacement 대기 중 restart는 EKS에서 주입하지 않았고 단위 시험이 판정한다. |
| Kubernetes mutation 결과 불명 | timeout 후 재조회 전 next-node drain 0; UID/RV mismatch Pod 삭제 0; deletionTimestamp가 있으면 중복 DELETE 0. | ADR-0006 §§3–4; DEP-11·DEP-18 | **2026-10-16 EKS PASS**. Admission webhook으로 Pod DELETE 응답을 늦춰 timeout을 만들었다. Rollout은 `delete_unconfirmed`를 유지했고 같은 UID/RV precondition으로만 재시도했다. Pod resourceVersion이 바뀌면 fresh 증거로 다시 승인했고, 이전 precondition의 DELETE는 API server가 409로 거절했다. deletionTimestamp 뒤 중복 DELETE 0, replacement 대기 중 다음 node drain 0이었다. |
| Exclusive controller/target | leader 아닌 process와 다른 CR은 drain/delete를 호출하지 않음; Lease/status CAS 충돌은 status refresh 뒤 중단. | 이 계약 §2 | **2026-10-16 EKS PASS**. 2026-10-15 `target_owned` 결과에 더해, leader가 아닌 controller process 2개는 관측 구간 동안 Management API 호출·Pod DELETE가 0건이었고 leader Lease를 가져가지 않았다. Status write와 경쟁하도록 CR annotation을 바꾸자 controller는 CAS 충돌 4건을 기록하고 다시 조정했다. 충돌 동안 delete intent 저장과 Pod 삭제는 0건이었다. |
| Audit | §5에 정한 approval·denial·terminal failure 세 action 모두 Audit event v1 schema·producer/action/reason/target·secret redaction을 만족; audit sink unavailable이면 승인/delete와 다음 node 진행 0. | Audit event v1; SEC-11 | Local schema/redaction·hash-chain·publisher failure 검증은 [Drain §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)의 기존 시험을 따른다. 2026-10-15 EKS producer 배포와 실제 EBS PVC의 세 lifecycle approval을 검증했다(`rollout-durable-audit-result.json`). **2026-10-16 EKS PASS**: 실제 PVC의 audit 파일 append를 막자 `audit_unavailable`로 차단했고 승인 저장·파일 변경·DELETE가 0건이었다. 세 action, Go verifier hash chain, 비밀정보 미포함도 확인했다. 2026-10-17 EKS 재시험도 세 action과 hash chain이 PASS였다. 이때 audit sink가 unavailable인 동안 끝난 두 `Failed`에는 `rollout.failed`·`rollout.pod.delete_denied` event가 없었다. 현재 `Service.fail`은 sink가 준비되지 않으면 두 event를 생략하고 status만 저장한다. §5는 이 생략을 정하지 않았다. 외부 collector·365일 보존은 미검증이다. |
| Argo CD health | 완료만 Healthy; blocked/progressing은 Progressing; failure/abort는 Degraded; unknown은 Healthy 아님. | ADR-0006 §4, §7 검증 6; DEP-18 | **2026-10-15 EKS PASS, health 소비자 범위**. 실제 Argo CD의 Git sync와 CR status 주입으로 확인했다. 실행 결과와 broker 미배포 경계는 [Drain §10의 Argo CD 수용 기록](../verification/drain-and-rolling-update.md#argo-cd-health-소비자-수용-2026-10-15)을 따른다. |
| Argo CD-applied broker 교체 | Argo CD가 Git의 StatefulSet template과 `NaruMQRollout`을 동기화하면 controller만 각 broker Pod를 한 번씩 교체한다. Argo CD는 Pod를 삭제하지 않는다. CR이 resource tree에 있는 동안 완료 전 Healthy가 없고, 완료 뒤 `Synced`·`Healthy`다. | ADR-0006 §7 검증 6; DEP-18; 이 계약 §2.11 | **2026-10-17 EKS PASS**. Finalizer write를 고친 controller(1.3.1)로 1.3.0→1.4.0 실제 교체를 실행했다. Pod별 controller DELETE 1건, Argo CD DELETE 0건, CR이 resource tree에 있는 동안 완료 전 Healthy 0건이었다. 완료 뒤 60초 동안 Application과 CR이 `Synced`·`Healthy`였고, Git CR의 `abort: false`가 live spec에 남았다. 2026-10-16 r2 controller 교체 시험은 같은 판정 중 sync만 FAIL이었다. 원인은 finalizer 설치가 spec의 `abort: false`를 지운 결함이었다. [2026-10-16 기록](../verification/drain-and-rolling-update.md#external-rollout-controller-eks-결함-matrix-수용-2026-10-16), [2026-10-17 기록](../verification/drain-and-rolling-update.md#재승인-retry와-argo-cd-적용-교체-eks-재시험-2026-10-17). |
| 게시 release upgrade/rollback | 실제 게시 N-1↔N과 rollback에서 같은 barrier를 적용한다. 연속 100회와 EKS HA는 infrastructure acceptance다. | ADR-0006 §7 검증 4; Drain spec §9 | **인계(2026-10-16 결정)**. 실제 게시 release가 필요하므로 [TODO §1.2](../TODO.md#12-release와-rollout-gate)의 `CLU-11·NFR-15` 항목에서 검증한다. 최대 부하와 연속 100회는 같은 절의 `DEP-01~12` 항목에서 검증한다. |

## 4. Compatibility principles

- CRD field meaning과 required status contract를 깨는 변경은 migration 계획과 새 contract major 또는 CRD conversion 정책을 요구한다.
- Controller는 broker Management API v1 field와 error code 의미를 유지한다.
- CR status는 restart·중복 reconcile을 견뎌야 한다. Process memory만을 operation의 기준으로 삼지 않는다.
- Unknown field, condition, phase 또는 barrier는 삭제 승인을 의미하지 않는다.
- Controller image/version policy는 immutable digest로 배포한다. CRD와 controller의 upgrade order는 CRD를 먼저 배포하고 controller를 뒤에 올린다.

## 5. Controller audit event boundary

Controller는 [Audit event v1](audit-event-v1.md) JSON schema를 따르며 `producer.component=ROLLOUT_CONTROLLER`를 사용한다. Event ID는 rollout UID·ordinal·Pod UID·Pod resourceVersion·persisted attempt key·action으로 안정적으로 생성하고 `requestId=eventId`로 둔다. 서로 다른 attempt나 Pod를 한 이벤트로 합치지 않는다. Producer instance ID는 controller Pod UID, version은 controller build version이다. Actor subject는 `narumq-rollout-controller`다.

| Action | Result / reason | Target | Details |
| --- | --- | --- | --- |
| `rollout.pod.delete_approved` | `ALLOWED` / `safe_to_stop` | `broker_pod` / Pod name | rollout UID, node ID, Pod UID, OCI digest, broker release version, drain operation ID, full persisted barrier evidence |
| `rollout.pod.delete_denied` | `DENIED` / stable blocker code | `broker_pod` / Pod name | rollout UID, node ID, observed blocker |
| `rollout.failed` | `FAILED` / stable failure code | `rollout` / namespaced CR identity | rollout UID, current node, drain operation ID and safe operator action |

`delete_denied` is emitted once when a terminal blocker is recorded, not for ordinary requeue/poll observations; `failed` records the terminal rollout outcome.

Controller audit writes to its dedicated durable append-only JSONL hash-chain file. The controller PVC is the restart boundary; this storage does not claim the externally approved 365-day SIEM retention or immutable external archive. The approval event is appended and fsynced before CR delete intent or Kubernetes DELETE. A failed append blocks that delete and all later nodes. Event append is idempotent by event ID. No secret, credential, payload or raw auth header enters status/event/log.
## 6. Kubernetes test boundary

Controller unit/reconcile tests use a fake client only for isolated state transitions. Producer/consumer acceptance also uses a real local Kubernetes API and a real three-process broker cluster. The local smoke must observe actual broker drain HTTP responses, Kubernetes Pod UID replacement and replacement readiness; an echo-only fake is not acceptance.

EKS deployment acceptance runs only in an explicitly provisioned, disposable fixture cluster with a dedicated namespace, ServiceAccount/identity, isolated StatefulSet/PVCs and an explicit cleanup boundary. The production `narumq-eks` source cluster and its workloads/PVCs are never smoke targets. No EKS/AWS API call is part of local verification.

Image/build와 격리 fixture 절차는 [development guide](../how-to/development-guide.md#격리된-eks-롤아웃-컨트롤러-인수-테스트-번들-준비-전용)를 따른다. 2026-10-15 사용자 재개 승인 뒤 별도 state·VPC의 `narumq-ha-acceptance-20261015`에서 전용 run-ID 태그와 검증된 ARM64 platform digest를 사용했다. 기존 `observability-20261014`는 OCI version label이 없어 rollout 후보에서 제외하고 보존했다. 최신 수용 범위는 §3과 Drain §10을 따른다.

## 7. Delegated decisions (2026-10-14)

The user delegated implementation decisions for DEP-11/DEP-18 and the SEC-11 rollout producer. The choices below are adopted within that scope. They do not create production evidence or decide work outside this controller.

| Choice | Adopted contract | Impact |
| --- | --- | --- |
| CRD API and schema | Group `narumq.io`, kind `NaruMQRollout`, namespaced API `narumq.io/v1alpha1`; machine schema is the checked-in CRD artifact. Status stores phase, conditions, current ordinal/node/Pod identity, drain idempotency key/operation/deadline/last full barrier evidence, delete intent and last operator action. | CRD v1alpha1 changes need an explicit migration before incompatible field changes. |
| Workload profile | Only a three-replica StatefulSet with `OnDelete` and stable ordinal-to-node mapping is supported. Argo updates the target template but cannot trigger automatic replacement. Controller never patches the template. | No Deployment, arbitrary selector, ECS adapter or blue/green rollout is implied. |
| OCI identity and version preflight | Target and every installed Pod use platform-specific immutable manifest digests. Controller verifies manifest/config descriptor digests and reads only the canonical version and sorted feature-set OCI config labels with a same-namespace Docker config Secret; it does not download or unpack filesystem layers. Before every cache hit it validates that named Secret's type, JSON and direct credential entry for the target registry; registry metadata is fetched only on successful-cache misses. Kubelet `imageID` must equal that manifest digest or its independently verified config descriptor digest. Controller compares OCI labels with CR target values and the committed broker profile. Successfully verified metadata is process-cached for at most four digests; failures are not cached. Every non-retired voter and target must be in the same-major adjacent-minor window; all committed active features must be supported by every voter and target. Missing/unknown evidence blocks. | Artifact metadata is generated from the same build inputs as the executable and verified from the registry; CR-authored version/features alone are insufficient. |
| Retry, restart and abort | Persist an idempotency key before POST and recover by GET first. API ambiguity reuses that key. Failure is terminal until operator changes `retryToken`. If the last observed drain was not `SAFE_TO_STOP`, retry first cancels the previous failed drain and then starts a fresh operation. The broker does not cancel a `SAFE_TO_STOP` drain, so a retry after a failure at `SAFE_TO_STOP` keeps the same key and operation, confirms them with GET and the same Pod UID, adopts the current Pod resourceVersion, and returns to `DrainSafe` with `approvalDeadlineAt = now + 10m`; approval then repeats the fresh GET, preflight and audit checks. A changed Pod is accepted only after a persisted delete intent (`AwaitingReplacement`); otherwise retry blocks with `pod_identity_changed`. An elapsed approval deadline in `DrainSafe` or `DeleteApproved` ends in `Failed`/`drain_timeout`, not a waiting blocker. `abort=true` cancels only cancellable drains; after `SAFE_TO_STOP` it halts without deleting. | No automatic advancement or force delete. A stuck target lock is never automatically stolen. A re-approval uses a new decision key, so its audit events never replace the earlier decision's events. |
| CR deletion | A finalizer is persisted before any target Lease, broker drain, audit append or Pod mutation. `deletionTimestamp` enters the same abort flow as `spec.abort=true`: cancel only a cancellable drain; after `SAFE_TO_STOP`, do not delete and finish `Aborted`; if deletion has already begun, confirm readiness for only the same ordinal and then finish `Aborted` without advancing. Persist terminal status and release the target Lease before removing the finalizer. Unknown deletion/drain state retains the finalizer. Finalizer install and release are metadata-only merge patches with a resourceVersion lock; the controller never rewrites the CR spec (§2.11). | Deleting the CR cannot erase the controller's durable safety state or permit a competing rollout. GitOps-authored spec fields such as `abort: false` stay as applied. |
| Exclusive ownership | One `Recreate` controller Deployment replica plus Kubernetes Lease leader election; one persistent target Lease per StatefulSet; status writes use resourceVersion CAS. A conflicting CR or unverifiable owner blocks. | Fail closed on stale or conflicting ownership; operator may need to resolve a stranded Lease. |
| Argo CD health | `Completed` → `Healthy`; `Failed`/`Aborted` → `Degraded`; all known active/blocked phases → `Progressing`; absent/unknown status never maps to Healthy. | GitOps surfaces blocked work without treating uncertainty as success. |
| Audit producer/sink | Use Audit event v1 and the action table in §5. Append once by deterministic event ID to a dedicated controller JSONL hash-chain PVC and fsync before delete approval. Sink failure blocks current delete and next-node progress. | PVC supports controller restart, not the separate external SIEM retention/immutability commitment. |
| Local and EKS test boundary | Reconcile fakes cover unit transitions. Acceptance requires a real local broker cluster plus disposable Kubernetes API lifecycle; EKS smoke requires a dedicated fixture cluster and explicit cleanup plan. | Fake client is not runtime acceptance; production/source EKS is prohibited as a target. |

## 8. Contract artifacts and verification

- [NaruMQRollout CRD](../../deploy/kubernetes/rollout-controller/crd.yaml) is the machine-readable schema and status contract.
- Producer/consumer schema tests and controller lifecycle acceptance are linked from [Drain spec §9–§10](../specs/drain-and-rolling-update.md#9-수용-시험).
- A 2026-10-15 local Kind smoke exercised a real three-broker StatefulSet through digest-bound preflight, `SAFE_TO_STOP`, durable audit, exact-UID/RV Pod replacement, and a no-mutation version mismatch; it is synthetic local evidence only, not production EKS or N/N-1 qualification ([Drain spec §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)).
- Operational infrastructure acceptance, including production 3-AZ behavior and continuous 100-rollout evidence, remains outside local controller acceptance.

`deploy/kubernetes/rollout-controller/kustomization.yaml` intentionally installs the CRD and least-privilege RBAC only. The controller Deployment is an image-input template and is not included in this unpinned base. A production release overlay must include `deployment.yaml` and replace `narumq-rollout-controller` with the operator-published immutable platform-manifest digest using its Kustomize `images` transformer, for example `name: narumq-rollout-controller`, `newName: <registry>/<repository>`, `digest: sha256:<published-controller-manifest-digest>`. `deployment.yaml` must never be applied directly. `overlays/kind` pins only the `:local` image for disposable local acceptance; it is not a production overlay.

Build and publish the controller only from the root with an explicit version and platform. For an ARM64 fixture image, set a unique task tag in the approved existing registry and run:

```sh
docker buildx build --platform linux/arm64 --provenance=false \
  --build-arg TARGETOS=linux --build-arg TARGETARCH=arm64 \
  --build-arg "NARUMQ_CONTROLLER_VERSION=${CONTROLLER_VERSION}" \
  --file deploy/kubernetes/rollout-controller/Dockerfile \
  --tag "${CONTROLLER_IMAGE_TAG}" --push .
```

Record the registry's immutable platform-manifest digest, verify the manifest is `linux/arm64` and its OCI config labels match `NARUMQ_CONTROLLER_VERSION`, then use that digest in the Kustomize `images` transformer. The committed base contains no release digest or mutable deployable image. Temporary fixture tags and their exclusively owned manifests/attestations are removed only after the fixture is destroyed.

This contract does not fabricate published N/N-1 release evidence, production `B_*` workload baselines, MQTT-13 90-day observation or an independent assessor result.
