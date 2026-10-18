# Cluster backup을 만들고 복원하는 방법

이 절차는 실행 중인 cluster에서 committed boundary의 NMQBKUP3 archive를 내려받고, retained state와 detached session의 QoS 0 offline queue를 포함한 같은 cluster의 현재 3-voter identity를 유지한 채 각 node를 오프라인 seed한다. 이 절차의 제한된 복원 시험은 production power-loss 수용을 대신하지 않는다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | Cluster 운영자와 복구 담당자 |
| 검증 상태 | Draft |
| 검증 범위 | Local multi-process 및 격리 EKS 복원. Archive format `3.0`, cluster protocol `2.0`, state schema `1.0`. 실제 실행은 §4의 기록을 따른다. |
| 관련 기준 문서 | [Cluster §10·§11.7·§11.8·§11.9](../specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore), [Cluster backup v3](../contracts/cluster-backup-v3.md), [ADR-0013](../decisions/ADR-0013-offline-queue-replication-and-cluster-backup-v3.md), [ADR-0058](../decisions/ADR-0058-cluster-backup-s3-retention-and-restore.md), [Management API v1](../contracts/management-api-v1.md) |

## 1. 대상과 영향

- 대상: 동일 `clusterId`와 동일한 현재 bootstrap voter ID 세 개로 구성된 cluster.
- Backup API archive는 `members`, state에서 도출한 tenant ID, session ownership, subscriptions, durable delivery session의 queued/inflight 및 inbound QoS 2 state, membership history, retained state 및 offline queue를 포함한다.
- Retained state는 `(tenantId, exact Topic Name)`별 마지막 값이고, offline state는 detached persistent session의 QoS 0 queue다. Archive summary는 `retained`, `offline`, `inflight`를 별도 kind로 집계하며 `inflight`에는 durable delivery message와 inbound QoS 2 state가 포함된다.
- Restore는 현재 voter 세 개만 지원한다. Archive에 learner ID가 있으면 거절한다. 다른 cluster ID로 이동하거나 node ID/address/DNS SAN/certificate fingerprint를 바꾸는 작업이 아니다. v1(`NMQBKUP1`) 및 v2(`NMQBKUP2`) archive는 current restore에서 명시적으로 거절한다.
- Cutover와 업그레이드 전 drain: 새 build로 cluster를 업그레이드할 때 이전 build의 node-local queue는 복제되지 않으므로, 업그레이드 전에 persistent subscriber를 재접속시켜 대기 큐를 drain해야 한다. Drain하지 않은 기존 node-local 큐는 프로세스 종료와 함께 유실될 수 있다.
- Archive에는 고객 식별자, retained payload와 session 정보가 들어갈 수 있다. 인증된 암호화 저장소에 보관하고 access를 제한한다.
- 채택된 [SPEC-TBD-09] 정기 백업 정책: 일 1회 수행하고 30일간 보존한다. 전체 클러스터 재해 복구 목표는 RPO 24시간·RTO 1시간이다(단일 노드 장애는 3-node Raft quorum으로 RPO 0 유지; [Cluster §10](../specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore) 참조).
- S3 is authoritative for 30-day retention; the private same-region bucket blocks public access, denies non-TLS access, uses SSE-S3, expires objects after 30 days, and aborts incomplete multipart uploads after one day. The PVC is staging only.
- 새 archive와 요청 기록은 같은 PVC의 비공개 임시 directory에 준비하고, directory를 `<archive filename>.staged`로 원자적으로 rename해 함께 게시한다. 업로더는 게시된 directory만 처리한다. 게시 전 중단된 `.staging-*` directory는 자동 업로드하지 않는다. 실패한 게시 directory는 다음 Job을 위해 보존한다. 기존 요청 없는 archive는 자동 이관하지 않고 기존 local `mtime +30` 정리만 적용한다.
- Backup 요청은 쓰기를 중지하지 않는다. Fresh quorum barrier가 유효하지 않으면 retryable `503`을 반환한다. Retained, offline 및 durable delivery operation은 [Release contract](../contracts/cluster-release-compatibility-v1.md#5-readiness와-persisted-state)의 fresh read·committed profile gate를 따른다. Release는 same-major 인접 minor window를 허용하며 protocol/schema는 exact 비교를 유지한다.

## 2. 전제 조건

- [ ] Management API에 ClusterAdmin 권한으로 접근하며 token은 명령 history, 로그 또는 티켓에 남기지 않는다.
- [ ] Cluster API와 node 목록을 읽어 현재 voter ID 세 개, quorum 및 readiness를 확인했다. Membership learner가 있는 동안에는 restore가 지원되지 않는다.
- [ ] 모든 non-retired member의 release가 [Release contract](../contracts/cluster-release-compatibility-v1.md#3-version-handshake)의 same-major 인접 minor window 안이고 cluster protocol `2.0`과 state schema `1.0`이 일치한다. [Feature activation 승인](cluster-feature-activation.md) 뒤에는 해당 feature를 지원하지 않는 build로 rollback하지 않는다.
- [ ] Archive, response headers와 restore config를 민감한 운영 자료로 보호한다.
- [ ] 복원 대상은 모두 중지되어 있다. 각 voter에 새롭고 별도인 빈 data directory를 준비했다. 기존 운영 volume이나 다른 node의 data directory를 재사용하지 않는다.
- [ ] 각 restore config는 `role=bootstrap`이며 archive의 cluster ID, cluster protocol, state schema, voter IDs, address, DNS SAN과 certificate SHA-256 fingerprint에 정확히 일치한다. Config의 local `nodeId`는 그 세 voter 중 하나다. Restore build의 release window·active feature 지원과 historical consumer 제한은 [Backup v3 §2·§4](../contracts/cluster-backup-v3.md#4-호환성-규칙)를 따른다. Config에 build release를 별도로 넣지 않는다.
- [ ] 모든 voter와 management endpoint에 접근 가능한 Linux/Bash 호환 관리 환경을 사용한다. EBS power-loss 보장이 필요한 운영 복구라면 이 Draft 절차만으로 승인하지 않는다.
- [ ] 업그레이드 전 절차: 이전 release cluster를 중지하기 전에 persistent subscriber를 재접속시켜 남아 있는 node-local queue를 drain하도록 유도했다(ADR-0013 cutover 정책).

## 3. 절차

실행 위치는 신뢰된 관리 host다. 아래 명령은 Bash 호환 shell을 기준으로 한다. `MANAGEMENT_URL`, 경로와 token이 포함된 `CURL_CONFIG` 파일 경로를 환경에 설정한다. `CURL_CONFIG`는 secret manager가 생성한 owner-only 파일이어야 하며 저장소에 넣지 않는다. Token을 command argument, history, 로그 또는 티켓에 직접 기록하지 않는다.

1. **Archive 경로를 비공개로 만든다.**

   ```sh
   umask 077
   install -d -m 0700 "$BACKUP_DIR"
   ARCHIVE="$BACKUP_DIR/cluster-backup.nrbak"
   HEADERS="$BACKUP_DIR/cluster-backup.headers"
   ```

   기대 결과: backup archive와 header 파일을 둘 directory가 현재 사용자만 접근할 수 있다.

2. **ClusterAdmin API로 online backup을 받는다.**

   ```sh
   curl --fail --silent --show-error \
     --request POST \
    --config "$CURL_CONFIG" \
     --dump-header "$HEADERS" \
     --output "$ARCHIVE" \
     "${MANAGEMENT_URL}/api/v1/cluster/backups"
   ```

   기대 결과: HTTP 200과 binary `application/octet-stream` archive. Header의 `X-NaruMQ-Backup-Commit-Index`는 archive commit index다. `X-NaruMQ-Backup-SHA256`은 trailer 앞의 archive byte를 해시한 값이며 마지막 32-byte trailer를 포함한 파일 전체의 digest가 아니다. 503이면 quorum/read barrier가 freshness 기준을 충족하지 못한 것이므로 readiness가 복구될 때까지 중단한다.

3. **세 target node의 복구 config를 각각 확인한다.**

   Config는 같은 cluster ID, 같은 세 정렬된 bootstrap voter ID와 동일 peer identity를 사용해야 한다. `nodeId`와 `directory`만 해당 target voter의 값으로 다르며 각 directory는 비어 있어야 한다. Archive source도 현재 voter여야 한다. Voter set, endpoint, DNS SAN 또는 인증서 fingerprint를 변경한 복구는 이 절차의 범위가 아니다.

   Restore build는 archive producer·모든 voter와 지원 release window 안이어야 하며 모든 active feature를 지원해야 한다. ADR-0021 이전 historical consumer의 exact-release 제한은 [Backup v3 §4](../contracts/cluster-backup-v3.md#4-호환성-규칙)를 따른다.

4. **각 voter의 빈 directory에 같은 archive를 오프라인 restore한다.**

   각 voter config에 대해 별도 restore process invocation을 실행한다. CLI는 seed 후 종료되므로 같은 shell에서 순서대로 실행할 수 있다.

   ```sh
   ./narumq -cluster-config "$NODE_1_CONFIG" -cluster-restore "$ARCHIVE"
   ./narumq -cluster-config "$NODE_2_CONFIG" -cluster-restore "$ARCHIVE"
   ./narumq -cluster-config "$NODE_3_CONFIG" -cluster-restore "$ARCHIVE"
   ```

   Windows executable을 실행할 때는 `./narumq` 대신 `narumq.exe` 경로를 사용한다. 성공 시 각 process는 data seed를 완료하고 종료한다. 이 명령은 peer, MQTT 또는 management listener를 시작하지 않는다. Archive checksum, canonical encoding, commit tail, state summary와 identity 검증에 실패하면 다음 node를 진행하지 않는다.

5. **세 node를 모두 seed한 뒤 정상 cluster config로 시작한다.**
   각 정상 cluster process는 별도 terminal 또는 process supervisor에서 시작해 계속 실행한다. 아래 명령을 foreground shell 하나에서 순서대로 실행하지 않는다.

   ```sh
   ./narumq -cluster-config "$NODE_1_CONFIG"
   ./narumq -cluster-config "$NODE_2_CONFIG"
   ./narumq -cluster-config "$NODE_3_CONFIG"
   ```

   모든 process가 살아 있고 quorum barrier를 통과한 뒤 다음을 확인한다.

   ```sh
   curl --fail "${MANAGEMENT_URL}/health/ready"
   curl --fail \
     --config "$CURL_CONFIG" \
     "${MANAGEMENT_URL}/api/v1/cluster"
   ```

   성공 기준: 세 voter의 readiness가 200이고 cluster summary가 동일한 leader와 quorum을 보고한다. 이후 joiner는 일반 membership 절차로 catch up시킨다. Restore는 자동으로 process를 시작하거나 learner를 복원하지 않는다.

## 4. 검증

실제 운영 환경의 address, node ID, binary release와 관찰값을 incident/backup record에 기록한다. Token, private key, certificate private data와 archive payload는 증거에 넣지 않는다. 과거 실행은 현재 코드 통과가 아니다. 로컬·EKS 실행 범위는 [Cluster 검증 기록](../verification/cluster-session-and-delivery.md), 현재 연결·결과는 [시나리오 보고서](../../.artifacts/verification/scenarios/report.md)를 확인한다.

| 관찰 대상 | 성공 조건 | 확인 명령·방법 |
| --- | --- | --- |
| Online v3 archive | HTTP 200, octet-stream, commit header·manifest 일치, 유효한 checksum, format major 3과 retained·offline summary | 단계 2; restore CLI의 전체 archive 검증 |
| Retained·offline restore/restart/resync | 세 restore process가 exit 0으로 seed하고, cross-node·restore·restart·joiner catch-up 뒤 replay·delivery가 일치함 | 단계 4–5; MQTT replay·delivery와 management readiness/summary |
| Identity mismatch·learner·v1/v2 archive | Restore를 거절하고 target이 시작되지 않음 | Restore CLI; `go test ./cmd/narumq/... -run 'TestClusterRestore' -count=1` |
| S3 helper 회귀 | 요청 기반 업로드 checksum, 결과 불명 재시도, 성공 후 정리, 목록·exact-key 다운로드·checksum 실패 정리 | `PYTHONDONTWRITEBYTECODE=1 python tests/test_backup_s3.py` |

## 5. 중단 조건

- Backup endpoint가 `503 cluster_backup_unavailable`을 반환하거나 response가 HTTP 200/octet-stream이 아니다.
- Archive checksum, format, commit boundary 또는 state summary 검증이 실패한다.
- Restore config의 cluster ID, protocol/schema, 세 voter ID, address, SAN, certificate fingerprint가 archive와 다르거나 restore build가 release window·active feature 지원 조건을 충족하지 못한다.
- Archive에 learner ID가 있거나 voter 수가 정확히 세 개가 아니다.
- Restore command가 non-zero로 끝난다. 해당 voter process를 시작하지 않고 원인과 marker 상태를 확인한다.
- 세 voter가 quorum을 만들지 못하거나 `/health/ready`가 200이 아니다. MQTT traffic을 허용하지 않는다.

## 6. 이전 상태로 되돌리는 롤백 또는 복구

1. Restore 실패 뒤 `restore-incomplete` marker를 직접 삭제하지 않고 해당 node를 offline으로 유지한다. Archive/identity 오류는 먼저 바로잡는다.
2. Directory에 restore marker가 있으면, 그 directory가 해당 실패 restore 전용임을 확인한 뒤 같은 valid v3 archive/config로 offline restore를 다시 실행한다. Marker나 restore-owned 파일을 손으로 수정하지 않는다.
3. Foreign file 또는 이전 cluster data가 target에 있으면 그 directory를 restore에 사용하지 않는다. 운영 data를 임의로 삭제하거나 덮어쓰지 않고 새 빈 directory를 준비한다.
4. v1/v2 release 또는 v1/v2 archive로 직접 downgrade/restore하는 경로는 지원하지 않는다. Retained/offline command/state를 기록한 뒤 이전 binary를 같은 data directory에서 시작하지 말고, 승인된 migration/rollback 절차가 확정될 때까지 v3 archive와 원본 증거를 보존한다.
5. Restore 후 readiness가 실패하면 MQTT admission을 열지 않고 비밀을 제거한 node log, node ID, commit index와 error code를 보존해 담당자에게 이관한다.

### S3 백업 전송과 오프라인 복원

기본 `deploy/kubernetes/eks-ec2-ha` overlay는 로컬 백업만 실행한다. 이 overlay는 S3 전용 `ServiceAccount`나 Terraform에서만 생성하는 설정 `ConfigMap`을 참조하지 않는다.

운영 환경에서 S3를 사용하려면 공유 `deploy/terraform` root의 `backup_s3_enabled`를 `true`로 설정한다(기본값 `false`). 격리된 acceptance fixture는 별도 `deploy/terraform-acceptance-s3` root/state를 사용한다. 이 root에는 `backup_s3_enabled` 변수가 없고 `main.tf`가 S3 module을 직접 호출하므로 운영자가 해당 root에 Terraform `apply`를 명시적으로 실행해야만 fixture S3 리소스가 생성된다. 선택한 Terraform root가 bucket, IRSA `ServiceAccount`, 설정 `ConfigMap`을 설치한 뒤 저장소 루트에서 `kubectl apply -k deploy/kubernetes/eks-ec2-ha-s3`를 실행한다. 이 overlay가 uploader sidecar와 S3 설정을 연결한다. `narumq-eks` 또는 정리할 수 없는 환경에는 적용하지 않는다.

목록 조회 및 시드 Job 매니페스트는 `deploy/kubernetes/eks-ec2-ha-s3`에 있지만 해당 overlay의 Kustomize 적용 대상에는 포함하지 않는다. 아래 절차의 지정된 시점에만 `kubectl create -f`로 실행한다. 고정 AWS 자격 증명은 사용하지 않는다.

1. **실패한 업로드 파일을 보존한다.** 예약 백업 CronJob은 새 archive를 요청하기 전에 PVC의 게시된 `backup-*.nrbak.staged` directory에 있는 요청을 먼저 처리한다. `PutObject`가 실패하거나 SHA-256 응답이 다르면 archive와 JSON 요청 기록을 함께 남겨 둔다. 다음 Job은 같은 SHA에서 파생된 key로 재시도한다. 게시 directory의 파일을 삭제·이름 변경·수정하지 않는다. 게시 전 중단된 `.staging-*`는 자동 업로드하지 않는다. 아래 offline maintenance 절차로만 복구·정리한다. 요청 기록이 없는 기존 archive는 S3 업로드 대상으로 삼지 않으며 기존 로컬 `mtime +30` 정리만 적용한다.

2. **복원 전용 IAM 역할로 백업 목록을 조회한다.** 저장소 루트에서 임시 목록 조회 Job을 생성하고, 생성된 Job 이름을 기록한다.

   ```sh
   date -u +'%Y-%m-%dT%H:%M:%SZ' # RTO 측정을 위한 복원 시작 시각 기록
   kubectl -n narumq create -f deploy/kubernetes/eks-ec2-ha-s3/restore-from-s3-list-job.yaml
   kubectl -n narumq get jobs -l app.kubernetes.io/component=restore-list
   kubectl -n narumq logs job/<generated-list-job-name>
   ```

   이 Job은 `system:serviceaccount:narumq:narumq-backup-restore` ServiceAccount를 사용한다. 해당 역할의 `s3:ListBucket` 권한은 설정된 cluster prefix로 제한된다. 출력에서 정확한 content-addressed key 하나를 선택한다. key에는 UTC 날짜·시각, commit index, 전체 파일 SHA-256이 포함된다. wildcard를 사용하거나 기존 로컬 archive에서 key를 조합하지 않는다.

3. **시드 Job을 실행하기 전에 깨끗하고 격리된 복원 대상을 준비한다.** 대상은 archive와 같은 cluster/member 설정 및 Secret을 사용하는 새 복구 환경이어야 하며 원본과 네트워크로 격리되어야 한다. `narumq-eks`, 실행 중인 원본 cluster 또는 `data-narumq-0/1/2` claim에 이미 데이터가 있는 fixture에는 restore overlay를 적용하지 않는다. 저장소 루트에서 실행한다.

   ```sh
   kubectl apply -k deploy/kubernetes/eks-ec2-ha-s3-restore-mode
   ```

   S3 restore overlay는 `StatefulSet/narumq`의 `replicas`를 0으로 유지하고 S3 backup CronJob의 `suspend`를 `true`로 설정하며 StatefulSet의 volumeClaimTemplate과 같은 이름으로 빈 voter PVC 세 개를 만든다. 로컬 archive만 복원할 때는 `deploy/kubernetes/eks-ec2-ha-restore-mode`를 사용한다. 두 overlay 모두 기존 claim을 비우거나 변경하지 않는다. 적용 후 대상 claim이 새로 만들어져 비어 있는지 확인한다. Seed Job은 예상하지 못한 PVC 파일이 있거나 `data`·`audit` 하위 경로가 비어 있지 않으면 거절하며 기존 파일을 삭제하지 않는다.

   기존 fixture와 같은 namespace에서 시험 복원을 할 때도 `data-narumq-*`를 재사용하지 않는다. 원본 broker 프로세스를 모두 정지하고 같은 identity가 동시에 실행되지 않음을 확인한다. 별도의 새 claim 이름을 seed Job과 복구 StatefulSet에 일관되게 지정한다. `WaitForFirstConsumer` claim은 seed Job의 node 배치를 지정해 각 voter가 서로 다른 AZ의 EBS에 놓이도록 한다. 이 작업은 비어 있는 새 대상의 복원이며 기존 claim을 지우는 절차가 아니다.

4. **이 순차 복원에 사용할 object key 하나만 지정한다.** 아래 명령의 `S3_KEY`에 목록에서 복사한 key를 넣는다. 이 값은 데이터이며 AWS 자격 증명이 아니다.

   ```sh
   kubectl -n narumq create configmap narumq-backup-restore-request \
     --from-literal="BACKUP_S3_KEY=${S3_KEY}"
   ```

   복원 요청 `ConfigMap`은 세 voter seed Job이 사용한다. Job 실행 중에는 다른 key를 넣어 재사용하지 않는다.

5. **StatefulSet이 정지된 동안 세 voter를 모두 내려받아 seed한다.**

   ```sh
   kubectl -n narumq create -f deploy/kubernetes/eks-ec2-ha-s3/restore-from-s3-seed-jobs.yaml
   kubectl -n narumq get jobs -l app.kubernetes.io/component=restore-seed
   kubectl -n narumq wait --for=condition=complete job/<generated-node-1-job> --timeout=3600s
   kubectl -n narumq wait --for=condition=complete job/<generated-node-2-job> --timeout=3600s
   kubectl -n narumq wait --for=condition=complete job/<generated-node-3-job> --timeout=3600s
   ```

   각 전송용 init container는 지정 key의 S3 SHA-256과 content length를 검사하고, 비공개 ephemeral volume에 다운로드한 뒤 전체 파일 SHA-256을 검증하고 종료한다. 별도 container는 기존 `/narumq -cluster-config ... -cluster-restore ...` CLI를 실행해 voter PVC 하나를 seed한 뒤 종료하며 listener를 시작하지 않는다. AWS 자격 증명은 전송 container에만 전달된다. Seed에 실패하면 해당 복원 대상을 계속 offline 상태로 둔다. 일부만 seed된 PVC에는 재시도하지 않는다. 증거를 보존한 뒤 새 빈 복원 대상을 준비한다.

6. **복구 시간을 측정하고 복구 상태를 검증한다.** 세 seed Job이 모두 성공하면 복원한 voter를 시작한다.

   ```sh
   kubectl -n narumq scale statefulset/narumq --replicas=3
   kubectl -n narumq rollout status statefulset/narumq --timeout=10m
   ```

   각 voter의 `/health/ready` 응답, quorum, 필요한 retained/offline 서비스 동작을 확인한 뒤 `date -u +'%Y-%m-%dT%H:%M:%SZ'`로 종료 시각을 기록한다. 복구 검증을 통과한 뒤에만 예약 백업 CronJob을 재개한다.

   ```sh
   kubectl -n narumq patch cronjob narumq-backup --type=merge -p '{"spec":{"suspend":false}}'
   ```

   아카이브 다운로드나 시드 작업 완료만으로는 전체 클러스터 RTO를 입증할 수 없다.

7. **증거를 보존한 뒤 임시 Job 리소스를 정리한다.** 생성한 목록 조회·시드 Job과 `narumq-backup-restore-request` `ConfigMap`은 정확한 이름을 지정해 삭제한다. 성공적으로 복원한 PVC가 복구된 cluster의 운영 데이터라면 유지한다. Fixture를 정리할 때는 증거 수집 후 fixture가 소유한 PVC와 다른 임시 리소스만 제거한다. S3에 보존된 object는 삭제하지 않는다.

정확한 object key를 받는 helper는 먼저 `HeadObject`로 checksum과 객체 크기(40 bytes~1 GiB)를 확인한 다음 `GetObject`의 checksum, 다운로드한 길이, 전체 파일 SHA-256을 검증하고 접근 권한 mode `0600`인 archive를 원자적으로 게시한다. NMQBKUP3를 파싱하지 않는다. Archive 형식, identity, state 검증은 기존 offline restore CLI가 담당한다.

| S3 수용 경계 | 기대 결과 |
| --- | --- |
| 새 요청 기록 기반 archive 업로드 | 일치하는 S3 checksum 응답을 확인한 뒤에만 로컬 staging 파일을 정리한다. 실패하면 archive와 요청 기록을 모두 남겨 재시도한다. |
| 기존 로컬 archive | 자동 이관하거나 형식을 파싱하지 않는다. 기존 로컬 `mtime +30` 정리만 적용한다. |
| 30일 보존 및 RPO/RTO | Lifecycle 설정만으로 실제 30일 보존을 증명하지 않는다. 지속적인 일일 백업 RPO ≤ 24h와 production 데이터 크기의 RTO ≤ 1h는 별도 수용이 필요하다. 격리 fixture의 단일 복원 결과는 [Cluster 검증 기록](../verification/cluster-session-and-delivery.md)에 두며 이 시간 기반 수용을 대신하지 않는다. |

### 미게시 staging의 offline 복구·정리

이 절차는 게시 전 Pod 종료로 남은 `.staging-XXXXXXXX` 하나만 처리한다. AWS 요청을 보내지 않는다. 파일 내용은 고객 데이터를 포함할 수 있으므로 비공개로 보존한다. 자동 스캔·오래된 파일 일괄 삭제는 하지 않는다.

1. **모든 backup 쓰기를 중지한다.** CronJob을 suspend한 뒤 기존 예약 Job과 수동 Job의 backup producer·uploader가 모두 종료됐는지 확인한다. Maintenance 중에는 새 Job을 시작하지 않는다. CronJob의 `Forbid`나 `suspend`만으로 수동 Job까지 멈췄다고 판단하지 않는다.
2. **백업 PVC를 maintenance 작업에만 연결한다.** Python 3가 있는 신뢰한 Linux 관리 container에 해당 PVC를 `/backups`로 연결한다. 기존 S3 helper ConfigMap의 `/scripts/s3_uploader.py`를 사용한다. AWS token은 필요 없다. 다른 작업의 mount·쓰기 상태를 확인할 수 없으면 진행하지 않는다.
3. **정확한 directory 이름과 파일 목록을 확인한다.** `.staging-XXXXXXXX` 하나의 이름을 선택한다. Wildcard나 상위 경로를 명령에 넘기지 않는다. 필요한 증거를 보호된 저장소에 보존한다.
4. **완성된 요청과 archive 쌍을 복구한다.** 관리 container 안에서 실행한다. 아래 `.staging-Ab12Cd34`는 확인한 실제 이름으로 바꾼다.

   ```sh
   python3 /scripts/s3_uploader.py recover-stage .staging-Ab12Cd34 --offline
   ```

   명령은 기존 metadata·요청 파일 이름·archive 크기·전체 SHA-256을 검사한다. 성공하면 기존 이름의 `<archive filename>.staged`로 directory를 게시한다. 이미 게시된 같은 경로가 있거나 검증에 실패하면 원본을 남기고 종료한다. Metadata를 추정하거나 손으로 고치지 않는다. 복원 적합성은 기존 offline restore CLI가 따로 검사한다.

5. **복구할 수 없는 잔여만 명시적으로 정리한다.** 다른 유효한 백업이 있고 필요한 증거를 보존했는지 먼저 확인한다. 삭제는 되돌릴 수 없다. Directory 안의 부분 archive·부분 request만 제거하며, 다른 백업·게시 directory는 바꾸지 않는다.

   ```sh
   python3 /scripts/s3_uploader.py discard-stage .staging-Ab12Cd34 --offline
   ```

   예상 밖 파일, 하위 directory, symlink 또는 서로 다른 backup의 파일이 있으면 거절한다. 강제로 지우지 말고 증거를 보존해 담당자에게 전달한다.

6. **Maintenance 작업을 끝낸 뒤 예약 백업을 재개한다.** 복구한 게시 directory는 다음 정상 uploader가 기존 checksum 확인 절차로 전송한다. `.staged` 게시만으로 S3 업로드 성공을 선언하지 않는다. `--offline`은 운영자가 중지 상태를 확인했다는 명시적 조건이며 자동 잠금이나 Kubernetes 상태 검사는 아니다.

로컬 검증 명령·실제 shell 중단 smoke·한계는 [Cluster §11.9](../specs/cluster-session-and-delivery.md#119-offline-queue-replication과-backup-v3-수용-매핑)에 있다. 실제 EBS 전원 손실·S3 장애 matrix 수용은 별도다.

## 7. 해결되지 않을 때 전달할 대상

- 전달 대상: cluster 운영 당직자와 NaruMQ 개발 담당자
- 보존할 증거: cluster ID, voter ID, release/protocol/schema, archive commit index와 digest header, restore exit code, node별 readiness/summary 및 비밀을 제거한 로그
- 비밀 값 제외 규칙: bearer token, private key, certificate private data와 archive payload를 로그·보고서에 포함하지 않는다.
