# 동적 Raft 클러스터를 시작하고 멤버를 추가하는 방법

이 절차는 초기 Raft cluster의 voter 세 개(voter는 합의 투표에 참여하는 node)를 시작하고 새 node를 quorum(현재 voter 과반수의 동의)으로 commit하는 `STAGED` identity로 등록한다. 새 node는 learner(로그를 복제하지만 투표하지 않는 member)로 기록을 따라잡은 뒤 voter로 승격된다. 이 초안은 production 실행을 승인하지 않는다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | cluster를 구성하고 변경하는 운영자 |
| 검증 상태 | Draft |
| 검증 범위 | 로컬 다중 프로세스 acceptance와 제한된 EKS slot 재시드. 구체적 실행 범위는 §4의 기록을 따른다. |
| 관련 기준 문서 | [동적 멤버십 계약](../contracts/raft-membership-v2.md), [Cluster backup v3](../contracts/cluster-backup-v3.md), [Cluster backup/restore 절차](cluster-backup-restore.md), [cluster session 명세](../specs/cluster-session-and-delivery.md), [ADR-0010](../decisions/ADR-0010-dynamic-cluster-membership.md), [ADR-0012](../decisions/ADR-0012-retained-replication-and-cluster-backup-v2.md), [ADR-0013](../decisions/ADR-0013-offline-queue-replication-and-cluster-backup-v3.md), [ADR-0015](../decisions/ADR-0015-cluster-durable-qos-delivery.md), [cluster architecture](../architectures/cluster-architecture.md) |

## 1. 대상과 영향

- 대상: 동일한 `clusterId`와 bootstrap voter set을 사용하는 세 node 및 새 joiner
- 예상 사용자 영향: bootstrap과 멤버십 변경 중 quorum, 인증서, 네트워크 설정이 맞지 않으면 신규 MQTT 연결이 거절될 수 있다.
- 운영 경계: N/N-1·운영 부하·전체 lifecycle 동등성은 별도 수용이 필요하다. 이 절차는 TEN-08 tenant quota, QoS 0 general live forwarding과 전체 drain의 수용 절차도 아니다. 제한된 process·EKS 시험을 production 전체 수용으로 보지 않는다.

## 2. 전제 조건

- [ ] 모든 node가 cluster protocol `2.0`, state schema `1.0`을 지원해야 한다. Retained command는 fresh read barrier와 모든 non-retired committed member의 profile gate를 따른다. Release는 [Release contract](../contracts/cluster-release-compatibility-v1.md#3-version-handshake)의 same-major 인접 minor window를 허용하며 protocol/schema는 exact 비교를 유지한다.
- [ ] 세 bootstrap node의 `clusterId`, sorted `bootstrapMembers`, member ID, endpoint, DNS SAN(인증서에 기록된 허용 host 이름), certificate SHA-256 fingerprint가 서로 일치한다.
- [ ] 각 node의 TLS 1.3 peer certificate가 공통 CA(인증서 발급 기관)에서 검증되고, local DNS SAN과 fingerprint가 구성값에 일치한다.
- [ ] Joiner는 운영자가 승인한 미사용 positive `nodeId`, 자체 인증서와 새 데이터 디렉터리를 갖는다. retired ID를 재사용하지 않는다.
- [ ] Peer listener와 DNS 주소가 node 간 연결 가능하다. Joiner의 `seedAddresses`는 lowercase DNS `host:port`여야 하며 IP literal은 허용되지 않는다.
- [ ] 데이터 디렉터리는 node별로 분리된 영속 volume이다. 기존 디렉터리를 새 node에 복사하거나 `cluster.meta`, WAL(write-ahead log, Raft 변경 기록), snapshot을 수동 삭제하지 않는다.
- [ ] Management HTTP listener는 신뢰된 관리 네트워크 안에서만 접근 가능하다. 인증 token은 명령 history, 로그, ticket에 붙여 넣지 않는다. 원격으로 접근해야 하면 별도의 승인된 TLS 보호 경로를 사용한다.
- [ ] 아직 Draft 상태이므로 production cluster에는 적용하지 않는다.

## 3. 절차

실행 위치는 저장소 root이며 예시는 Bash 호환 shell 기준이다. 설치된 binary를 쓰면 `go run ./cmd/narumq`를 해당 binary 경로로 바꾼다. 전체 strict config schema와 예시는 [Process configuration v1 contract §1.3](../contracts/process-configuration-v1.md)을 따른다.

1. **Bootstrap 구성 파일을 세 node에 각각 만든다.** `role`은 `bootstrap`, `bootstrapMembers`는 정확히 같은 세 identity를 포함하고 `seedAddresses`는 빈 배열이어야 한다. 각 파일의 `nodeId`, `directory`, `listenAddress`, `advertisedAddress`, 인증서 경로, management 주소, MQTT 보안 파일은 해당 node 값으로 설정한다. 초기 bootstrap 디렉터리는 비어 있어야 한다.

   검사 기준: `clusterId` 및 bootstrap identity set은 세 config에서 동일하고, 각 config의 local certificate SAN/fingerprint는 local member entry와 같다.

2. **세 node를 각각 별도 터미널에서 시작한다.**

   ```sh
   go run ./cmd/narumq -cluster-config config/node1.json
   go run ./cmd/narumq -cluster-config config/node2.json
   go run ./cmd/narumq -cluster-config config/node3.json
   ```

   `-cluster-config`는 다른 CLI flag나 positional argument와 함께 쓸 수 없다. 각 process는 자신이 소유한 영속 디렉터리만 사용한다. 서로 다른 bootstrap identity set으로 node를 시작하지 않는다.

   빈 directory의 세 bootstrap node는 모두 동시에 시작해야 한다. Peer 확인 중에는 management·MQTT listener가 열리지 않으므로 첫 node의 health 응답을 기다린 뒤 다음 node를 시작하지 않는다. Kubernetes StatefulSet은 `podManagementPolicy: Parallel`을 사용한다.

3. **모든 node의 readiness를 확인한다.** Management endpoint에서 `/health/ready`를 조회한다. Liveness 응답만으로 준비 완료를 판단하지 않는다. READY는 committed voter role, replay/catch-up, route projection 재구축, healthy persistence 및 fresh quorum `ReadIndex` barrier(현재 Raft quorum에서 읽기 일관성을 확인하는 경계)가 모두 성공한 상태여야 한다. 어느 node라도 READY가 아니거나 quorum을 확인할 수 없으면 join을 진행하지 않는다.

4. **관리 API로 joiner identity를 한 번 stage한다.** 멤버십 contract §1.3의 request body에 joiner의 member ID, peer address, DNS SAN, leaf certificate SHA-256, release/protocol/schema version을 넣는다. `POST /api/v1/cluster/members`에는 같은 request 재시도를 구분하는 `Idempotency-Key`와 `ClusterAdmin` bearer token을 보낸다. `202 Accepted`는 `STAGED` 기록이 commit되었으며 operation이 계속 진행 중이라는 뜻이다. Response의 `operationId`를 보존한다.

   응답을 받지 못했으면 **같은 key와 동일 body로** 재요청한다. 다른 key로 중복 stage하지 않는다. Quorum 오류가 발생하면 joiner를 시작하지 말고 기존 voter 수와 node readiness를 확인한다.

5. **Stage commit 이후 joiner를 시작한다.** Joiner config는 `role="joiner"`, `bootstrapMembers: []`, DNS 형식 `seedAddresses`, 이미 승인된 `nodeId`, 새 empty directory를 사용한다. Joiner는 독립 Raft cluster로 bootstrap하지 않는다.

   ```sh
   go run ./cmd/narumq -cluster-config config/node4.json
   ```
   Joiner는 committed `STAGED` 확인만으로 Raft stream을 열지 않는다. Seed에 같은 admission 요청을 반복해 조회하고, Raft configuration에 반영된 `LEARNER` identity를 확인한 뒤에만 기존 voter와 peer stream을 맺는다. 이 자동 재조회 중 bootstrap quorum이 사라지거나 process가 종료되면 같은 volume으로 재시작하고 operation 상태를 다시 확인한다.


6. **동일 operation을 관찰해 종료 phase를 확인한다.** 저장한 ID로 `GET /api/v1/cluster/membership-operations/{operationId}`를 반복 조회한다. `STAGED`, `LEARNER`, `CATCHING_UP`, `PROMOTING`은 중간 상태다. `SUCCEEDED` 전까지 join 완료로 취급하지 않는다. 응답의 `committedIndex`는 해당 phase가 commit된 index이며 제안이 queue에 들어간 index가 아니다.


7. **Restart는 같은 identity와 data directory로 수행한다.** Process를 종료하고 다시 시작할 때 기존 node의 같은 config, 인증서와 volume을 유지한다. Joiner 역할 node는 이미 voter로 commit된 뒤에도 startup 시 seed admission으로 현재 peer identity를 다시 얻으므로, `seedAddresses`에 든 seed 중 하나 이상이 접속 가능해야 한다. Recovery가 committed WAL replay로 typed snapshot보다 앞선 상태를 만들면 runtime은 peer transport를 열기 전에 replay 완료 상태의 새 typed snapshot을 저장한다. 바뀐 인증서나 손상된 WAL을 복구한다는 이유로 metadata를 삭제하거나 디렉터리를 비우지 않는다. 오류 시 진행을 멈추고 증거를 보존한다.

### 계획된 member 교체

계획된 제거에서 leader는 대상 member가 소유한 모든 session을 남은 voter 중 member ID가 가장 작은 node로 이전한다. 소유권 이전과 epoch 증가는 commit된 뒤 대상 voter를 제거한다. 적용 과정에서 persistent subscription의 소유 node와 epoch를 갱신하고 기존 연결을 takeover로 닫는다. 이전이 막히면 제거 operation은 `REMOVING` 상태에 머문다. Successor voter가 없거나 ownership epoch가 고갈됐거나 orphan subscription이 감지되면 제거를 차단한다. QoS 1/2 delivery state는 `(tenantId, clientId)` key로 quorum에 이미 복제되어 있으므로 별도 checkpoint handoff가 필요하지 않고, 새 owner는 다음 CONNECT에서 ATTACH로 이를 복구한다. `LEAVING` member는 새 session admission을 거절한다.

중단된 제거를 관찰하거나 재시도할 때는 같은 operation ID와 idempotency key를 사용한다. 막힌 소유권 이전을 우회하려고 voter를 강제로 제거하지 않는다.

### 손실된 voter volume 교체

이 절차는 [ADR-0022](../decisions/ADR-0022-bootstrap-volume-loss-guard.md)와 [Cluster §7](../specs/cluster-session-and-delivery.md#7-failure-recovery)를 따른다. Quorum이 남아 있어야 하며, 손실 volume을 빈 directory로 바꿔 같은 ID를 시작하는 방법은 사용하지 않는다.

1. 손실 node를 중지하고 손실된 data directory를 그대로 두어 재시작하지 않는다. 조사 증거를 보존하며 기존 volume과 새 joiner volume을 분리한다.
2. 새 ID로 `POST /api/v1/cluster/members`를 호출한 뒤 joiner를 시작하고 해당 operation의 `SUCCEEDED`를 기다린다.
3. `DELETE /api/v1/cluster/members/{lostId}`를 호출하고 `SUCCEEDED`를 기다린다. 이 planned removal은 손실 member의 session ownership을 남은 voter로 이전한다.
4. 손실 ID는 재사용하지 않는다. 새 voter의 READY와 retained·persistent session·offline message 복구를 확인한다.

초기 배포에서 `cluster bootstrap refused`가 나오고 **아직 사용자 데이터가 없다면**, 세 node를 모두 중지하고 세 directory를 비운 뒤 동시에 다시 시작한다. 사용자 데이터가 있는 cluster에서는 directory를 지우지 말고 원래 volume을 복구하거나 위 새 ID 교체 절차를 따른다.

### EKS slot의 volume 손실 교체

이 절차는 `deploy/kubernetes/eks-ec2-ha`에서 EBS volume을 잃은 broker 자리(slot) 하나를 새 member ID로 다시 채운다. 근거와 결정은 [ADR-0061](../decisions/ADR-0061-eks-ordinal-member-slot.md), 합격 기준은 [Cluster §11.20 EKS slot 재시드 기준](../specs/cluster-session-and-delivery.md#eks-slot-재시드-기준-2026-10-16)이다. 절차는 Draft다. Peer hello 전 인증서 검증 수정을 포함한 image를 사용한다. 이전 image는 이전 ID 제거를 지연시키거나 quorum을 흔들 수 있다. 수정 전 실패와 로컬 재시험·격리 EKS 1회 결과는 [실행 기록](../verification/cluster-session-and-delivery.md#2026-10-16-eks-slot-재시드-실행-결과)을 따른다.

실행 위치는 대상 cluster의 `kubectl` context를 가진 운영자 작업 환경이며 예시는 Bash 기준이다. 관리 API는 `kubectl port-forward`로 생존 Pod의 8080에 연결해 호출한다. 아래 자리표시자는 실제 값으로 바꾼다.

| 자리표시자 | 뜻 |
| --- | --- |
| `<SLOT>` | 손실된 Pod의 ordinal. 예: `2` |
| `<LOST_ID>` | 손실된 slot이 쓰던 member ID. 예: `3` |
| `<NEW_ID>` | 아직 쓴 적 없는 새 member ID. 예: `4` |
| `<SURVIVOR>` | 정상인 Pod의 ordinal |
| `<SLOT_DNS>` | `narumq-<SLOT>.narumq-headless.narumq.svc.cluster.local` |
| `<ADMIN_TOKEN_FILE>` | `ClusterAdmin` token을 담은 파일 경로. token 값은 명령줄·로그·문서에 남기지 않는다. |

시작 전 확인:

- [ ] 남은 두 voter가 모두 READY이고 quorum이 있다. 한 번에 slot 하나만 교체한다.
- [ ] `snapshot-stream`을 장애 전에 활성화했다(`POST /api/v1/cluster/features/snapshot-stream`, [Feature activation](cluster-feature-activation.md), [Raft snapshot stream v1](../contracts/raft-snapshot-stream-v1.md)). 이 feature가 비활성이면 1 MiB 초과 snapshot을 새 ID에 보낼 수 없다([Cluster §7](../specs/cluster-session-and-delivery.md#7-failure-recovery) 7항). Stage 뒤 leader 로그에 `raft snapshot exceeds peer frame limit; not sent`가 나오면 이 상태다. 이때는 이전 ID를 제거하지 않는다. 남은 두 voter의 quorum과 서비스는 유지된다. 따라잡지 못하는 새 ID가 member로 남아 있으면 활성화가 `409 feature_not_supported_by_all_members`로 거절될 수 있다. 이 상태의 자동 복구 절차는 검증하지 않았다. 증거를 보존해 담당자에게 전달한다. 활성화는 되돌릴 수 없다. 활성화한 뒤에는 이 feature를 모르는 build로 rollback하지 않는다.
- [ ] 손실한 volume이 되돌릴 수 없게 사라졌다고 확인했다. 확인하지 못했으면 4단계 이후로 진행하지 않는다.
- [ ] `<NEW_ID>`는 `GET /api/v1/nodes`에 없고 이전에 쓴 적이 없다.
- [ ] Peer CA로 인증서를 발급할 수 있다. CA 비밀키는 이 절차의 산출물에 넣지 않는다.

1. **남은 voter와 quorum을 확인한다.** 생존 Pod에 연결해 두 voter가 모두 READY이고 leader가 있는지 본다.

   ```bash
   kubectl -n narumq port-forward pod/narumq-<SURVIVOR> 8080:8080
   ```

   다른 터미널에서 호출한다. Token은 stdin 설정으로 넘겨 process 목록에 남기지 않는다.

   ```bash
   mgmt() { # 사용: mgmt <METHOD> <PATH> [<Idempotency-Key> [<본문 파일>]]
     printf 'header = "Authorization: Bearer %s"\n' "$(cat "<ADMIN_TOKEN_FILE>")" |
       curl --silent --show-error --max-time 10 --config - --request "$1" "http://127.0.0.1:8080$2" \
         ${3:+--header "Idempotency-Key: $3"} ${4:+--header "Content-Type: application/json" --data "@$4"} \
         --write-out '\nHTTP:%{http_code}\n'
   }
   mgmt GET /api/v1/nodes
   ```

   검사 기준: `VOTER`가 정확히 둘이다. 손실 node는 `UNREACHABLE` 같은 비정상 상태일 수 있다. Quorum을 확인하지 못하면 이후 단계를 진행하지 않는다.

2. **빈 volume의 같은 ID 시작이 거절되는지 본다.** StatefulSet이 새 빈 PVC로 만든 Pod는 이전 ID로 시작해 `cluster bootstrap refused` 로그와 함께 exit 1로 끝나야 한다([ADR-0022](../decisions/ADR-0022-bootstrap-volume-loss-guard.md)). Pod를 지우거나 data directory를 만들지 않는다.

   ```bash
   kubectl -n narumq logs pod/narumq-<SLOT> --previous
   ```

3. **새 ID용 key와 인증서를 만든다.** DNS SAN은 `<SLOT_DNS>`이고 기존 peer CA(`ca.crt`)가 서명해야 한다. 새 key를 쓴다. 이전 ID의 인증서와 key를 재사용하지 않는다. 인증서의 SHA-256은 DER 형식 leaf 인증서 전체에 대한 값이다.

   ```bash
   openssl x509 -in <new-cert.pem> -outform DER | openssl dgst -sha256
   ```

   검사 기준: SAN에 `<SLOT_DNS>`가 있고, SHA-256이 이전 ID의 값과 다르다.

4. **새 ID를 stage한다.** 본문 파일에는 `memberId`=`<NEW_ID>`, `address`=`<SLOT_DNS>:7000`, `dnsSan`=`<SLOT_DNS>`, 3단계의 `certificateSha256`, 실행 중인 cluster와 같은 `releaseVersion`·`clusterProtocolMajor`·`clusterProtocolMinor`·`stateSchemaMajor`·`stateSchemaMinor`를 넣는다. `releaseVersion`은 `GET /api/v1/cluster` 응답에서 읽는다.

   ```bash
   mgmt POST /api/v1/cluster/members "eks-slot-stage-<NEW_ID>" <stage-body.json>
   ```

   `202 Accepted`와 `STAGED` phase를 확인하고 `operationId`를 보존한다. 응답을 잃으면 **같은 key와 같은 본문**으로만 다시 호출한다. 주소가 이전 ID와 같아도 membership은 member ID 중복만 거절한다. 주소 중복은 거절하지 않는다.

5. **ConfigMap과 Secret을 새 ID로 바꾼다.** 한 번에 같은 변경으로 적용한다.

   - ConfigMap `narumq-cluster-configs`에 `node-<NEW_ID>.json`을 더한다. 내용은 bootstrap 설정과 같은 필드를 쓰되 `nodeId`는 `<NEW_ID>`, `role`은 `joiner`, `bootstrapMembers`는 `[]`, `seedAddresses`는 **다른 두 slot**의 `<dns>:7000`이다. `advertisedAddress`와 `localDnsName`은 이 slot의 값 그대로다.
   - 같은 ConfigMap의 `slot-<SLOT>` 값을 `<NEW_ID>`로 바꾼다.
   - Secret `narumq-cluster-secrets`의 `node-<SLOT>.crt`와 `node-<SLOT>.key`를 3단계의 새 인증서와 key로 바꾼다. Key 이름은 그대로다.

   적용은 배포에 쓰는 같은 방법(GitOps 또는 `kubectl apply`)으로 한다. 적용 뒤 값을 읽어 확인한다.

   ```bash
   kubectl -n narumq get configmap narumq-cluster-configs -o jsonpath='{.data.slot-<SLOT>}'
   ```

   검사 기준: 출력이 `<NEW_ID>`이다. Secret 본문은 출력하지 않는다. 이 변경 뒤 손실 volume이 아닌 이전 data가 남은 Pod를 다시 시작하지 않는다([ADR-0061 §5](../decisions/ADR-0061-eks-ordinal-member-slot.md#5-결과와-제약)).

6. **손실된 PVC와 Pod를 삭제한다.** 손실 volume을 확인한 뒤에만 한다. StatefulSet이 빈 PVC로 Pod를 다시 만들고, 새 Pod는 `slot-<SLOT>`의 `<NEW_ID>`로 joiner를 시작한다.

   ```bash
   kubectl -n narumq delete pvc data-narumq-<SLOT> --wait=false
   kubectl -n narumq delete pod narumq-<SLOT>
   ```

7. **새 ID가 voter가 되는지 본다.** `operationId`가 `SUCCEEDED`가 되고 `GET /api/v1/nodes`에서 `<NEW_ID>`가 `VOTER`이며 Pod가 READY인지 확인한다.

   ```bash
   mgmt GET /api/v1/cluster/membership-operations/<operationId>
   mgmt GET /api/v1/nodes
   ```

8. **이전 ID를 제거한다.** 7단계가 끝난 뒤에만 호출한다. 이 planned removal은 이전 ID가 소유한 session을 남은 voter로 옮긴다.

   ```bash
   mgmt DELETE /api/v1/cluster/members/<LOST_ID> "eks-slot-remove-<LOST_ID>"
   ```

   `SUCCEEDED`를 기다린 뒤 `GET /api/v1/nodes`에서 `<LOST_ID>`가 `RETIRED`인지 확인한다.

9. **이전 ID의 설정을 지운다.** ConfigMap에서 `node-<LOST_ID>.json`을 삭제한다. 이 파일은 8단계 전에 지우지 않는다.

10. **결과를 확인한다.** Broker Pod 3개가 READY이고 voter가 남은 두 ID와 `<NEW_ID>`이며 `<LOST_ID>`가 `RETIRED`인지 확인한다. 손실 전에 기록한 retained marker와 persistent session·offline message가 새 member에서 복구되는지 확인한다. `<LOST_ID>`로 다시 stage하면 `409 member_id_reserved`여야 한다.

중단 조건(위 §5에 더한다):

- 시작 전 확인 항목 중 하나라도 충족하지 못했다.
- 2단계에서 `cluster bootstrap refused`가 아닌 다른 이유로 Pod가 실패하거나, 이전 ID가 시작에 성공했다.
- 4단계 응답이 `202 STAGED`가 아니거나 `member_id_reserved`·identity conflict가 나왔다.
- 5단계 뒤 `slot-<SLOT>`이 `<NEW_ID>`가 아니다.
- 7단계에서 operation이 `SUCCEEDED`가 되지 않거나 `<NEW_ID>`가 `VOTER`가 되지 않는다. 8단계를 호출하지 않는다.

중단하면 새 stage key를 만들거나 다른 ID를 stage하지 않는다. 4단계 이후에는 같은 operation과 같은 key로만 상태를 조회·재시도하고, 증거를 보존해 §7 담당자에게 전달한다.

`deploy/kubernetes/eks-ec2-ha-s3`의 restore Job은 ID 1~3의 설정을 고정해 쓴다. Voter ID가 바뀐 cluster에는 이 절차를 적용해도 restore Job을 쓰지 않는다. 이 조합의 S3 복원은 검증하지 않았다.

## 4. 검증

실행 시 아래 조건을 확인하고 대상 버전·환경·명령·결과를 기록한다. 과거 실행은 현재 코드 통과가 아니며, 현재 연결·실행 결과는 [시나리오 보고서](../../.artifacts/verification/scenarios/report.md)를 따른다. 로컬 membership·retained·backup·재시드의 실행 이력은 [Cluster 검증 기록](../verification/cluster-session-and-delivery.md)에 둔다. 과거 v2 archive 시험은 현재 v3 restore 수용을 대신하지 않는다.

| 관찰 대상 | 성공 조건 | 확인 명령·방법 |
| --- | --- | --- |
| Bootstrap readiness | 세 node 모두 fresh quorum barrier 이후 READY이고 quorum과 공통 leader가 확인된다. | 각 endpoint의 `GET /health/ready`와 cluster summary |
| Join·idempotency | Stage 직후·promotion 뒤 같은 key가 같은 operation ID를 반환하고, joiner가 voter/READY에 도달한다. | Stage API와 `GET /api/v1/cluster/membership-operations/{operationId}` |
| Restart convergence | 같은 identity·volume 재시작 뒤 committed membership과 operation phase가 수렴한다. | `go test -race ./tests/integration -run '^TestDynamicClusterMembershipTransitionCrashRecovery$' -count=2` |
| 교체·retired ID | Planned removal 뒤 voter set이 기대값으로 수렴하고 retired ID 재사용은 거절된다. 단일 voter 손실에서도 quorum을 유지한다. | Management API와 process acceptance |
| Compatibility·identity | Foreign CA, SAN/fingerprint/cluster ID 불일치, malformed frame, unsupported profile/schema, reserved ID가 commit·Step에 도달하지 않는다. | Transport unit tests와 membership process acceptance |
| Quorum loss | Readiness·MQTT admission이 내려가고 removal이 commit되지 않는다. | Readiness와 removal API |
| MQTT admission | 준비 전 CONNECT와 commit 전 subscription success ACK가 거절되고, 회복 뒤 수락된다. | `TestClusterReadinessGate`와 failpoint process acceptance |
| Peer refresh | Identity를 유지한 refresh가 outbound frame을 보존하고, 이전 inbound frame은 Step 완료까지 reconciliation 경계를 따른다. | `go test -race ./internal/adapters/tcp` |
| Session·archive 복구 | Persistent session과 retained·offline state가 재접속·restore·restart·learner catch-up 뒤 일치한다. | [Backup/restore 절차](cluster-backup-restore.md) |

관련 process 회귀를 실행하려면 저장소 root에서 다음 명령을 사용한다. 명령 목록 자체는 통과 증거가 아니다.

```sh
go build ./...
go test ./internal/application/membership/... ./internal/adapters/raft/records/... ./internal/adapters/memory/retained/... ./internal/application/publish/... ./internal/application/subscribe/... ./internal/application/management/... ./internal/adapters/http/management/... ./cmd/narumq/... ./tests/contracts/... -count=1
go test ./tests/integration -run '^(TestClusterBackupRestoreProcessCrashAndResync|TestDynamicClusterMembershipProcessAcceptance|TestDynamicClusterMembershipTransitionCrashRecovery|TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum|TestSnapshotFinalizationCrashRecoveryAcrossProcessRestart)$' -count=1 -v
```

전체 repository 검증은 [개발 가이드](development-guide.md#4-검증)를 따른다. 이 절차의 제한된 수용은 production power-loss·모든 volume 손실 모델·운영 부하 수용이 아니다.

## 5. 중단 조건

- TLS SAN/fingerprint, cluster ID, member ID, release/protocol/schema 불일치
- bootstrap member set 불일치, 비어 있지 않은 신규 데이터 디렉터리, 중복·retired ID
- `cluster bootstrap refused` 로그: 같은 ID의 빈 volume으로 재시작하지 않는다.
- Management API에서 `not_leader`, `quorum_unavailable`, identity/idempotency conflict 또는 미지원 compatibility 응답
- 기존 node 하나라도 readiness 실패, committed operation phase가 예상 상태에서 멈춤, peer 연결 또는 persistence 오류
- Joiner가 stage commit 전에 Raft peer stream을 시작하거나 별도 bootstrap을 시도함

중단 조건이 하나라도 발생하면 새 membership operation을 만들거나 기존 voter를 수동 제거하지 않는다. Node log, operation ID, committed index, health 응답과 관련 시각을 보존하되 token, private key, payload secret은 제외한다.

## 6. 이전 상태로 되돌리는 롤백 또는 복구

1. `STAGED` 이후 operation이 진행되지 않으면 동일 key와 동일 request만 재시도하고 원인을 확인한다. 새로운 stage key, 수동 tombstone 삭제, ID 재사용, 데이터 디렉터리 초기화는 복구 방법이 아니다.
2. Joiner가 learner가 된 뒤 응답을 잃었으면 같은 persistent directory로 재시작하고 동일 operation을 조회한다. Heartbeat timeout이나 process 종료만으로 learner/voter를 자동 삭제하지 않는다.
3. Planned removal은 successor ownership transfer가 commit된 뒤에만 허용된다. Durable delivery aggregate는 별도 checkpoint command 없이 새 owner가 다음 CONNECT에서 ATTACH로 재개한다. Quorum이나 successor transfer가 확인되지 않으면 `DELETE /api/v1/cluster/members/{memberId}`를 호출하지 말고 기존 voter를 유지한다. Active session handoff와 quorum loss 복구 acceptance는 `TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum`로 검증했다.
3a. 전원 손실이나 `SIGKILL` 뒤 같은 volume으로 재시작한 voter가 오래 `Ready`가 되지 않으면 Pod의 `restartCount`와 직전 container 종료 사유를 먼저 확인한다. Kubelet은 startup probe 예산을 다 쓴 container를 종료하기 전에 `preStop`을 실행하므로 node에 drain 기록이 생길 수 있다. `GET /api/v1/nodes/{nodeId}/drain`의 phase가 `COMPLETED`이면 재시작 뒤 자동으로 `CLEAR`된다. `FAILED`일 때만 원인을 해결한 뒤 `DELETE /api/v1/nodes/{nodeId}/drain`으로 취소한다. `/health/ready`를 직접 호출해 startup을 통과시키지 않는다. 그 호출은 startup 결함을 가린다. 규칙과 근거는 [Drain §8.1](../specs/drain-and-rolling-update.md#81-startup-probe-종료와-drain-기록-재시작-순환)을 따른다.
4. Restore 절차인 [Cluster backup/restore](cluster-backup-restore.md)는 v3 archive에 retained, offline 및 durable delivery state를 포함한다. Release window·active feature·동일 identity 복원은 최신 [Backup v3 §2·§4](../contracts/cluster-backup-v3.md#4-호환성-규칙)를 따른다. [Feature activation 승인](cluster-feature-activation.md) 뒤에는 해당 feature를 지원하지 않는 build로 rollback하지 않는다. Activation 전 backup 복원은 이후 commit 손실이 가능한 별도 disaster recovery다. Production volume power-loss는 이 절차에서 검증하지 않으며, 이전 v1/v2 archive 또는 binary로의 직접 downgrade 경로는 지원하지 않는다.

## 7. 해결되지 않을 때 전달할 대상

- 전달 대상: cluster 운영 당직자와 NaruMQ 개발 담당자
- 보존할 증거: node ID, operation ID, committed index, phase, UTC 발생 시각, 비밀을 제거한 로그, readiness 응답, 배포 version
- 비밀 값 제외 규칙: bearer token, 개인키, 인증서 private data 및 MQTT payload를 로그·보고서에 포함하지 않는다.
