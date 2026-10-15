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
6. Controller는 현재 ordinal에 대한 drain idempotency key와 operation intent를 먼저 CR status에 저장한다. 이후 drain GET을 먼저 조회한다. Operation이 없을 때만 POST하며, API 결과가 불명확하면 같은 key로 GET/POST replay를 수행한다. Drain GET은 주기적으로 조회한다. 유효하고 완전한 `COMPLETED`/`SAFE_TO_STOP` observation만 승인 조건이다.
7. Controller는 승인 직전 drain 상태와 Pod UID/resourceVersion을 다시 읽는다. Delete approval audit event는 `rollout UID + ordinal + Pod UID + drain idempotency key + action`으로부터 결정적 event ID를 만들며, event는 Audit Event v1 producer·consumer schema와 secret-redaction 규칙을 따른다. Controller는 event를 durable sink에 idempotent append·fsync하고 delete intent와 drain evidence를 CR status에 resourceVersion CAS로 저장한 뒤 Kubernetes DELETE를 UID·resourceVersion precondition과 함께 호출한다. DELETE 결과가 불명확하면 다음 node를 진행하지 않고 Pod UID·deletionTimestamp를 재조회한다. 현재 Pod와 UID가 같고 deletion이 시작되지 않았을 때만 같은 precondition으로 재시도한다.
8. Controller는 동일 StatefulSet ordinal의 새 Pod UID, target image digest, Pod Ready, Pod IP의 `/health/ready` HTTP 200, committed target release profile을 모두 확인한다. 이 확인 전에는 다음 ordinal을 drain하지 않는다. 완료 전 실패하거나 timeout/unknown이면 차단한다.
9. Drain `FAILED` 뒤 controller는 자동으로 다음 node로 진행하거나 같은 operation을 새로 만들지 않는다. Operator는 `spec.retryToken`을 새 불투명 값으로 바꾸어야 retry한다. Controller는 먼저 기존 failed drain을 idempotently 취소하고 새 intent/key로 같은 ordinal을 재시작한다. `spec.abort=true`는 안전하게 취소 가능한 drain만 취소하며, 이미 `SAFE_TO_STOP`이면 workload delete 없이 `Aborted`로 멈춘다. Force delete는 지원하지 않는다.
10. 전체 ordinal이 target digest와 release profile에 도달하면 CR phase를 `Completed`로 바꾸고 target Lease를 해제한다. Secret·token·registry credential은 status, event, log에 기록하지 않는다.

CRD spec은 `workloadRef.name`, `target.image`, `target.releaseVersion`, `target.features`, `imagePullSecretRef.name`, `management.endpoint`, `management.credentialSecretRef.name`, `retryToken`, `abort`를 사용한다. Secret은 같은 namespace에 있어야 하며 registry secret은 `kubernetes.io/dockerconfigjson`, management secret은 key `token`을 제공한다. Management API path는 contract에 고정하고 endpoint는 scheme/host/base URL만 지정한다.

Kubernetes Lease leader election은 controller process 하나만 reconcile하도록 한다. Target Lease는 동일 StatefulSet을 가리키는 복수 CR을 직렬화한다. Target Lease가 다른 CR UID에 묶여 있거나 소유권을 확인할 수 없으면 자동 탈취하지 않고 차단한다. 이 보수적 잠금은 중단 controller의 자동 복구보다 중복 삭제 방지를 우선한다.

Argo CD custom health mapping은 phase `Completed`만 `Healthy`, `Failed`·`Aborted`를 `Degraded`, 알려진 진행·차단 phase를 `Progressing`으로 반환한다. Unknown phase나 status가 없으면 `Progressing`이다.

## 3. Acceptance matrix

아래 시험은 controller producer/consumer acceptance다. 시험 파일이 존재한다는 이유만으로 통과로 표시하지 않는다. 실행 결과와 환경은 [Drain spec §9](../specs/drain-and-rolling-update.md#9-수용-시험)의 증거 표에 연결한다.

| Scenario | Acceptance 조건 | 기준 | 상태 |
| --- | --- | --- | --- |
| 정상 단일 node 교체 | `SAFE_TO_STOP` 전 delete 0; 승인 뒤 해당 UID만 삭제; replacement readiness 전 다음 drain/delete 없음. | ADR-0006 §7 검증 3; Drain spec §§7,9 | Local Kind PASS와 2026-10-15 재개 EKS 정상 경로 PASS: 실제 세 broker UID 교체·target digest·Ready, 해당 이전 UID 세 개의 완전한 SAFE barrier approval 감사 기록을 확인했다. Synthetic fixture 범위이며 production 최대 부하 수용은 아니다. [Drain §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑). |
| Drain failure / timeout | 실패·deadline 만료 시 현재 delete 0, 다음 node 진행 0; status에 실패와 operator action 표시. | DEP-11·DEP-18; ADR-0006 §7 검증 2 | 미실행 |
| Barrier unknown / API unavailable | unknown·GET timeout·malformed/incomplete response는 승인하지 않고 현재 delete 0, 다음 node 진행 0. | ADR-0006 §1; Drain spec §6 | 2026-10-15 EKS 부분 PASS: 실제 cluster/features를 전달하는 controller 전용 fault proxy의 drain HTTP 503에서 세 broker UID 불변. GET timeout·malformed/incomplete·전체 deadline 경계는 미검증. |
| OCI/version preflight 거절 | 잘못된 digest, OCI metadata/CR/profile mismatch, 다른 major, minor delta ≥2, active-feature 누락, 미확인 voter는 workload mutation 0. | Release compatibility §§1,3; DEP-18 | Local Kind와 2026-10-15 EKS 부분 PASS: caller `0.0.2` 대 OCI `0.0.1` mismatch에서 세 broker UID 불변. 다른 preflight 거절 조합은 미검증. |
| Controller restart | drain 요청·대기·승인·DELETE 전후·replacement 대기에서 restart해도 동일 operation/key와 evidence를 재사용하고 한 번만 해당 UID를 교체. | ADR-0006 §4, §7 검증 1,5 | Local Kind와 2026-10-15 EKS 부분 PASS: active drain 중 controller Pod 교체 뒤 같은 operation ID·attempt key를 복구하고 정상 교체를 완료했다. 다른 restart 경계는 미검증. |
| Kubernetes mutation 결과 불명 | timeout 후 재조회 전 next-node drain 0; UID/RV mismatch Pod 삭제 0; deletionTimestamp가 있으면 중복 DELETE 0. | ADR-0006 §§3–4; DEP-11·DEP-18 | 미실행 |
| Exclusive controller/target | leader 아닌 process와 다른 CR은 drain/delete를 호출하지 않음; Lease/status CAS 충돌은 status refresh 뒤 중단. | 이 계약 §2 | 2026-10-15 EKS 부분 PASS: 두 CR이 같은 StatefulSet을 요청하면 비소유 CR은 `target_owned`, 세 broker UID 불변. Non-leader process와 CAS 충돌 주입은 미검증. |
| Audit | §5에 정한 approval·denial·terminal failure 세 action 모두 Audit event v1 schema·producer/action/reason/target·secret redaction을 만족; audit sink unavailable이면 승인/delete와 다음 node 진행 0. | Audit event v1; SEC-11 | Local schema/redaction·hash-chain·publisher failure 검증은 [Drain §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)의 기존 시험을 따른다. 2026-10-15 EKS producer 배포와 실제 EBS PVC의 세 lifecycle approval·이전 Pod UID·완전한 barrier·producer Pod identity·hash chain을 검증했다(`rollout-durable-audit-result.json`). 실제 PVC append/fsync 장애, 외부 collector·365일 보존은 미검증. |
| Argo CD health | 완료만 Healthy; blocked/progressing은 Progressing; failure/abort는 Degraded; unknown은 Healthy 아님. | ADR-0006 §4, §7 검증 6; DEP-18 | 미실행 |
| 게시 release upgrade/rollback | 실제 게시 N-1↔N과 rollback에서 같은 barrier를 적용한다. 연속 100회와 EKS HA는 infrastructure acceptance다. | ADR-0006 §7 검증 4; Drain spec §9 | 미실행 |

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
| Retry, restart and abort | Persist an idempotency key before POST and recover by GET first. API ambiguity reuses that key. Failure is terminal until operator changes `retryToken`; retry first cancels the previous failed drain and then starts a fresh operation. `abort=true` cancels only cancellable drains; after `SAFE_TO_STOP` it halts without deleting. | No automatic advancement or force delete. A stuck target lock is never automatically stolen. |
| CR deletion | A finalizer is persisted before any target Lease, broker drain, audit append or Pod mutation. `deletionTimestamp` enters the same abort flow as `spec.abort=true`: cancel only a cancellable drain; after `SAFE_TO_STOP`, do not delete and finish `Aborted`; if deletion has already begun, confirm readiness for only the same ordinal and then finish `Aborted` without advancing. Persist terminal status and release the target Lease before removing the finalizer. Unknown deletion/drain state retains the finalizer. | Deleting the CR cannot erase the controller's durable safety state or permit a competing rollout. |
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
