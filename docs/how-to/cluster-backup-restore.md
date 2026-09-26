# Cluster backup을 만들고 복원하는 방법

이 절차는 실행 중인 cluster에서 하나의 committed boundary archive를 내려받고, 같은 cluster의 현재 3-voter identity를 유지한 채 각 node를 오프라인 seed한다. API archive 생성과 process crash/restart·joiner resync는 local multi-process acceptance에서 통과했지만, `offline`/`retained` state와 production power-loss는 지원·검증 범위 밖이다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | Cluster 운영자와 복구 담당자 |
| 검증 상태 | Draft |
| 검증 환경·버전 | Local multi-process Go process acceptance; cluster protocol `2.0`, state schema `1.0` |
| 최종 검증일 | 2026-09-26 |
| 관련 기준 문서 | [Cluster §10·§11.7](../specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore), [Cluster backup v1](../contracts/cluster-backup-v1.md), [ADR-0011](../decisions/ADR-0011-cluster-backup-restore-v1.md), [Management API v1](../contracts/management-api-v1.md) |

## 1. 대상과 영향

- 대상: 동일 `clusterId`와 동일한 현재 bootstrap voter ID 세 개로 구성된 cluster.
- Backup API는 `members`, session ownership, subscriptions, QoS inflight/checkpoints, membership history와 그 state에서 도출한 tenant ID를 포함한다. `offline` message와 `retained` message는 포함하지 않는다.
- Restore는 현재 voter 세 개만 지원한다. Archive에 learner ID가 있으면 거절한다. 다른 cluster ID로 이동하거나 node ID/address/DNS SAN/certificate fingerprint를 바꾸는 작업이 아니다.
- Archive에는 고객 식별자와 session 정보가 들어갈 수 있다. 인증된 암호화 저장소에 보관하고 access를 제한한다.
- backup 요청은 쓰기를 중지하지 않는다. Fresh quorum barrier가 유효하지 않으면 retryable `503`을 반환한다.

## 2. 전제 조건

- [ ] Management API에 ClusterAdmin 권한으로 접근하며 token은 명령 history, 로그 또는 티켓에 남기지 않는다.
- [ ] Cluster API와 node 목록을 읽어 현재 voter ID 세 개, quorum 및 readiness를 확인했다. Membership learner가 있는 동안에는 restore가 지원되지 않는다.
- [ ] Archive, response headers와 restore config를 민감한 운영 자료로 보호한다.
- [ ] 복원 대상은 모두 중지되어 있다. 각 voter에 새롭고 별도인 빈 data directory를 준비했다. 기존 운영 volume이나 다른 node의 data directory를 재사용하지 않는다.
- [ ] 각 restore config는 `role=bootstrap`이며 archive의 cluster ID, release, cluster protocol, state schema, voter IDs, address, DNS SAN과 certificate SHA-256 fingerprint에 정확히 일치한다. Config의 local `nodeId`는 그 세 voter 중 하나다.
- [ ] 모든 voter와 management endpoint에 접근 가능한 Linux/Bash 호환 관리 환경을 사용한다. EBS power-loss 보장이 필요한 운영 복구라면 이 Draft 절차만으로 승인하지 않는다.

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

실제 운영 환경의 address, node ID, binary release와 관찰값을 incident/backup record에 기록한다. Token, private key, certificate private data와 archive payload는 증거에 넣지 않는다.

| 관찰 대상 | 성공 조건 | 확인 명령·방법 | 실제 결과 | 실행 증거 위치 |
| --- | --- | --- | --- | --- |
| Online archive | HTTP 200, octet-stream, commit header가 manifest와 일치하고 archive checksum이 유효하다. | 단계 2; restore CLI가 전체 archive 검증 | PASS: process acceptance에서 staged committed operation을 포함한 archive를 decode하고 commit index와 SHA-256 trailer header를 대조했다. | `tests/integration/cluster_backup_restore_process_test.go::TestClusterBackupRestoreProcessCrashAndResync`; Cluster §11.7 |
| Restore/restart/resync | 세 restore process가 종료 코드 0으로 seed하고, voter가 quorum READY가 되며, crash/restart한 node와 복원된 stage operation이 유지된다. Joiner catch up 뒤 summary가 수렴한다. | 단계 4–5; management readiness, cluster summary, membership operation | PASS: local multi-process process acceptance. Production volume/power-loss는 미실행이다. | 같은 process test; Cluster §11.7 |
| Identity mismatch 및 learner archive | Restore가 거절되고 target이 시작되지 않는다. | `go test ./cmd/narumq/... -run 'TestClusterRestore' -count=1` | PASS: unit/process command tests에서 cluster/member identity mismatch와 learner/archive mismatch를 거절했다. | `cmd/narumq/cluster_backup_restore_test.go` |

## 5. 중단 조건

- Backup endpoint가 `503 cluster_backup_unavailable`을 반환하거나 response가 HTTP 200/octet-stream이 아니다.
- Archive checksum, format, commit boundary 또는 state summary 검증이 실패한다.
- Restore config의 cluster ID, version, schema, 세 voter ID, address, SAN, certificate fingerprint 중 하나라도 archive와 다르다.
- Archive에 learner ID가 있거나 voter 수가 정확히 세 개가 아니다.
- Restore command가 non-zero로 끝난다. 해당 voter process를 시작하지 않고 원인과 marker 상태를 확인한다.
- 세 voter가 quorum을 만들지 못하거나 `/health/ready`가 200이 아니다. MQTT traffic을 허용하지 않는다.

## 6. 이전 상태로 되돌리는 롤백 또는 복구

1. Restore 실패 뒤 `restore-incomplete` marker를 직접 삭제하지 않고 해당 node를 offline으로 유지한다. Archive/identity 오류는 먼저 바로잡는다.
2. Directory에 restore marker가 있으면, 그 directory가 해당 실패 restore 전용임을 확인한 뒤 같은 valid archive/config로 offline restore를 다시 실행한다. Marker나 restore-owned 파일을 손으로 수정하지 않는다.
3. Foreign file 또는 이전 cluster data가 target에 있으면 그 directory를 restore에 사용하지 않는다. 운영 data를 임의로 삭제하거나 덮어쓰지 않고 새 빈 directory를 준비한다.
4. Restore 후 readiness가 실패하면 MQTT admission을 열지 않고 비밀을 제거한 node log, node ID, commit index와 error code를 보존해 담당자에게 이관한다.

## 7. 해결되지 않을 때 전달할 대상

- 전달 대상: cluster 운영 당직자와 NaruMQ 개발 담당자
- 보존할 증거: cluster ID, voter ID, release/protocol/schema, archive commit index와 digest header, restore exit code, node별 readiness/summary 및 비밀을 제거한 로그
- 비밀 값 제외 규칙: bearer token, private key, certificate private data와 archive payload를 로그·보고서에 포함하지 않는다.
