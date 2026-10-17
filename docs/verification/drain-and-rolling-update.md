# Drain과 rolling update 검증 기록

이 문서는 [Drain과 rolling update 스펙](../specs/drain-and-rolling-update.md)의 과거 실행 증거다. 아래 통과는 당시 작업 트리의 결과이며 현재 코드의 통과를 뜻하지 않는다. 현재 기준은 스펙, 연결은 테스트 소스의 `Scenarios:` 메타 정보, 결과는 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서를 따른다. EKS 시험환경은 폐기했으므로 live 관찰은 현재 접속 상태가 아니다. [검증 기록 작성 규칙](AGENTS.md)을 따른다.

## 1. 기록과 시나리오 대응

### 1.1 §10 이전 표의 행

옛 표의 시험 위치 색인은 현재 연결 정보와 중복되어 생략한다. 아래 실행 표와 항목별 기록은 당시 명령·환경·결과만 보존한다. 저장소 전체 gate는 개별 시나리오 통과를 대신하지 않는다.

### 1.2 §10 아래 절

DRN-SCN-011~012는 [Will 관측](#adr-0028-연속-rolling-관측), DRN-SCN-023~027은 아래 controller 기록, DRN-SCN-034~035는 cross-worker·EC2 복구 기록, DRN-SCN-036~038은 EKS 장애 matrix에 해당한다. 현재 연결 여부는 이 옛 대응표로 판정하지 않는다.

## 2. §10 수용 시험 매핑 표와 보충 문단

아래 표는 실행 증거를 요약한다. 상세 기록이 있는 항목은 그 절을 참조한다.

아래 evidence는 dynamic cluster의 broker-side drain과 `SAFE_TO_STOP`, 시험 전용 SDK drain 호환성, 그리고 local Kind controller lifecycle acceptance를 검증한다. Release-wide rollout이나 production storage 승인으로 확대 해석하지 않는다.

| Scenario | Acceptance test | 실행 증거 |
| --- | --- | --- |
| Local real-controller rollout (DEP-11/DEP-18/SEC-11) | 실제 3-process `OnDelete` StatefulSet, OCI/version/features·UID/RV·complete drain barrier·durable audit | **2026-10-15 PASS**: `NARUMQ_ROLLOUT_KIND_SMOKE=1 NARUMQ_ROLLOUT_REGISTRY=172.20.0.3:5000 NARUMQ_ROLLOUT_HOST_REGISTRY=http://127.0.0.1:5001 NARUMQ_ROLLOUT_OLD_DIGEST=sha256:dd44acdabea4188939ef08e84a5537ed7b7c1812f62a7ccc33935d16c26e0c2b NARUMQ_ROLLOUT_TARGET_DIGEST=sha256:cc6e480086f74a0233ffc52f88c62796accc346a03843bec6f8c78e25f4a195c NARUMQ_ROLLOUT_CONTROLLER_IMAGE=narumq-rollout-controller:local GOMAXPROCS=4 go test ./tests/integration -run '^TestRolloutControllerKindLifecycle$' -count=1 -v -timeout 15m`。Active-drain restart 뒤 같은 operation 복구, broker UID 3개 교체, approval audit와 caller version mismatch의 UID 불변을 확인했다. **2026-10-16 정상 경로 회귀 2회 PASS**: Kind v1.34.0, amd64 `kind-0.0.0`(`sha256:29a4a708…`)→`kind-0.0.1`(`sha256:a3c5bede…`), controller `0980bd5`와 미커밋 retry 변경. 위 명령의 digest·controller image를 바꿨으며 retry는 주입하지 않았다. 증거 `.artifacts/verification/kind-rollout-20261016T225857Z/`. 게시 release·production 부하·EKS/NLB 수용은 아니다. |
| MQTT 5 persistent QoS 1과 MQTT 3.1.1 live/detached session이 있는 3-node drain | `TestDynamicClusterNodeDrainProcessAcceptance`: readiness·admission 차단, 기존 connection 유지, idempotent replay, `DISCONNECT 0x9C`/Server Reference, rate-limited transfer, `SAFE_TO_STOP`, session resume와 restart `CLEAR`를 확인한다. | `go test ./tests/integration -run '^TestDynamicClusterNodeDrain' -count=1 -v -timeout 20m` PASS; 이 case 9.75s. |
| Cancel, crash/restart, completed-record clear와 missing-replica timeout | `TestDynamicClusterNodeDrainCancelTimeoutAndResumeProcessAcceptance`: cancel 뒤 READY, 같은 operation 재개, missing successor에서 `drain_timeout/insufficient_replicas`, 취소 뒤 readiness를 확인한다. | 같은 process command PASS; 이 case 16.03s. Combined package 25.916s. |
| Domain·application·wire·HTTP·metrics와 OpenAPI contract | `internal/domain/nodedrain`, `internal/application/membership`, `internal/adapters/tcp`, `internal/adapters/http/management`, `internal/adapters/metrics`, `tests/contracts/management-api-v1` 등 focused unit/contract suite. | `go test ./internal/domain/nodedrain ./internal/domain/mqtt ./internal/application/membership ./internal/application/connect ./internal/adapters/raft/records ./internal/adapters/tcp ./internal/adapters/http/management ./internal/adapters/metrics ./tests/contracts/management-api-v1 ./cmd/narumq ./tools/verify -count=1` PASS. |
| Existing ownership, subscription replay와 removal regressions | `TestDynamicClusterMQTTConnectOwnershipTakeoverProcessAcceptance`, `TestDynamicClusterPersistentSubscriptionReplayProcessAcceptance`, `TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum`. | Targeted integration command PASS; package 38.257s. |
| Rollout adapter/API 변경 뒤 전체 local (2026-10-15) | `GOMAXPROCS=4 go run ./tools/verify local` | **FAIL 유지**. TEN-09 victim p99 `1.517 > 1.5`, Raft `TestNodeCloseFlushesHeldAppendOrReportsFailure/unconsumed-prior-error` setup의 Windows `compact WAL: replace WAL: Access is denied`. 후속 `GOMAXPROCS=4 go test ./internal/adapters/raft -run '^TestNodeCloseFlushesHeldAppendOrReportsFailure$/^unconsumed-prior-error$' -count=1 -v`와 같은 명령의 `-race`는 PASS였다. TEN-09 단독 진단은 [별도 기록](tenancy-and-security.md#9-ten-09-noisy-neighbor-실행-기록)에 둔다. 단독 통과는 원인 확정이나 aggregate race-child exit 1 해소 증거가 아니며 그 진단은 확보하지 못했다. |
| NodeDrain core coverage ratchet | `go run ./tools/verify coverage --profile .artifacts/verification/coverage.out --policy tests/verification/coverage-policy.json --report .artifacts/verification/coverage-summary.json` | PASS; 100/102 executable lines (98.04%) and 13/13 functions (100%). |
| NodeDrain mutation gate | `go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-nodedrain.json internal/domain/nodedrain` | PASS; killed 64, lived·not covered 0, efficacy·mutant coverage 100% |
| SDK callback adapter의 redirect·session resume·Will 억제 | 필수 `tests/compatibility/mqtt-clients-v1 --suite drain`: Paho Go MQTT 5 `v0.23.0`과 Python `2.1.0`이 test-only readiness LB의 node3에서 node2로 이동한다. 후속 QoS 1 전달, Will Delay 뒤 no-Will과 drain `SAFE_TO_STOP`을 확인한다. | `drain-paho-go-python-will-acl-final`의 cluster case 2/2 PASS. 재접속 Go 89ms, Python 약 99.071ms. 기존 혼합 suite의 9/10 FAIL 기록은 보존한다. Artifact는 `.artifacts/compatibility/mqtt-clients-v1/drain-paho-go-python-will-acl-final/`에 있다. |
| 선택적 SDK 진단: malformed server DISCONNECT 4종과 configured endpoint fallback | `--suite sdk-diagnostics`의 SDK별 truncated·disallowed·duplicate·invalid-reference case. NaruMQ 대신 fake server를 사용하므로 제품 필수 합격 조건에서 제외한다. 진단은 Main CONNECT 두 번, vector 한 번과 trap 접속 부재를 기대한다. | 기존 기록은 Python 4/4 PASS, Go 3/4 PASS. `go-mqtt5-malformed-duplicate-server-reference`는 `serverReferenceAccepted=true`, `trap_accept=1`로 FAIL이며 이 결과는 유지한다. 2026-10-16 사용자 결정으로 SDK 진단 실패와 정상 drain 승인을 분리했다. 판정 규칙은 [Client contract §6.3](../contracts/mqtt-client-compatibility-v1.md#63-선택적-sdk-진단)에 있다. |
| SDK drain 뒤 회귀 | `GOMAXPROCS=4 go run ./tools/verify local`, nested module `go vet ./... && go build ./...`, Phase 1 Docker suite | 당시 local·coverage와 nested vet/build PASS. `phase1-drain-adapters-final` 26/26 PASS. Local gate는 SDK Docker suite를 대신하지 않는다. |
| 필수 drain과 선택적 SDK 진단의 판정 분리 | `TestDrainSuiteVerdictsSeparateSDKDiagnostics`와 실제 `--suite drain`·`--suite sdk-diagnostics` 실행. 필수 Will 실패·누락 결과는 계속 차단하며 SDK 진단 실패는 별도 suite에 남긴다. | 2026-10-16 회귀·vet와 필수 Docker drain PASS. 선택적 SDK 진단은 기존 중복 수락을 FAIL로 유지했다. 최초 fixture 실패와 수정·실행 명령·artifact는 [Client contract §6.4](../contracts/mqtt-client-compatibility-v1.md#64-검증-범위-분리-실행-기록-2026-10-16)에 있다. |
| 계획 인계 Will·집계 | Domain·membership·connect·metrics·TCP·runtime | 2026-10-04 `go build ./...`와 `go test ./internal/domain/session ./internal/application/membership ./internal/application/connect ./internal/adapters/metrics ./internal/adapters/tcp ./cmd/narumq -count=1` PASS |
| Session metadata 활성 3-node 연속 rolling의 표준 Will·사유 | `TestDynamicClusterDrainWillReasonProcessAcceptance`: node 3→1→2 drain, reference 재접속, session resume, observer 수신과 node restart를 확인한다. | 2026-10-04 PASS: `GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterDrainWillReasonProcessAcceptance$' -count=1 -v -timeout 20m`; test 49.06s, package 49.253s. Observer는 v311·zero·expire payload만 받았다. Safe delay 8초 뒤와 normal Will 무수신을 확인했다. 아래 delta 표 참조. |
| ADR-0028 영향 process 회귀 | Drain·cancel/timeout/resume·metadata·removal quorum·QoS 2 Will·takeover | 2026-10-04 아래 회귀 명령의 6개 시험 PASS |
| ADR-0028 전체 local | `GOMAXPROCS=4 go run ./tools/verify local` | 2026-10-04 PASS; coverage `.artifacts/verification/coverage-summary.json` |
| Recovery 전 재접속의 이전 Will ACL | `go test ./internal/application/connect -run '^TestConnectSupersededWillAuthorization$' -count=1 -v` | 2026-10-04 3개 case PASS. 새 CONNECT에 Will 없이, 이전 topic 허용 시 1회 발행·집계, 거절·principal 부재 시 새 연결 성공과 이전 Will 무발행·무집계를 확인했다. |
| ADR-0051 교체형 rolling | Node 4·5·6 추가, 기존 node drain·SAFE_TO_STOP·planned removal, live/offline session·QoS 1/2 | **2026-10-10 PASS 3회**: `GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterReplacementRollingUpdateProcessAcceptance$' -count=3 -v -timeout 45m`. 최종 voter `{4,5,6}`, offline `SessionPresent=true`, gap·QoS 1/2 duplicate 0. 초기 workload는 offline 90개로 session 한도 64를 넘어 seq 33–45 gap이 났다. 승인된 topic별 batch 3개(offline 총 54개)로 실행했다. 초기 gap을 교체 중 session 손실로 해석하지 않는다. |
| 연속 100회 in-place rolling | Node 3→1→2 drain·SAFE_TO_STOP·재시작, round당 QoS 1/2 24건·retained 1건·safe Will | **2026-10-11 PASS**: `NARUMQ_ROLLING_100=1 GOMAXPROCS=4 go test ./tests/integration -run '^TestDynamicClusterContinuousRollingDrainProcessAcceptance$' -count=1 -v -timeout 3h`. Topic별 1,200건, gap 0, QoS 1 duplicate 100, QoS 2 duplicate·reorder 0, retained 일치. Will suppressed 100·expected MQTT311 100·unexpected/unplanned 0. Production 최대 traffic·QoS 0·controller 장애·preStop·stale write·게시 release·EKS는 범위 밖이다. |
| Replacement/rolling harness gate | Windows/amd64, Go 1.26.5; `322cfe3`와 미커밋 fixture·decoder·replacement 시험, 후속 100회 fixture | 2026-10-10~11 `GOMAXPROCS=4 go run ./tools/verify local` PASS. 당시 실행만 기록한다. |
| ADR-0055 drain 기본값 | `ee01d3a`와 미커밋 drain domain·HTTP 기본값·contract 시험; Windows/amd64, Go 1.26.5 | 2026-10-11 `go test ./internal/domain/nodedrain ./internal/adapters/http/management ./internal/application/membership ./tests/contracts/management-api-v1 -count=1`, `go build ./...`와 위 drain process 2개 PASS. 전체 local 첫 실행은 rebalance 시험의 Windows WAL replace `Access is denied`로 FAIL, 단독과 전체 재실행은 PASS. `pwsh -NoProfile -Command '$env:GOMAXPROCS = "4"; go run ./tools/verify local'` PASS. SDK Docker drain은 test-only 값만 확인하므로 미실행이었다. |
| EKS 라이브 NLB 라이프사이클 및 전파 실측 (DEP-22, DEP-03, SPEC-TBD-04) | 라이브 클러스터(`narumq-eks`)에서 Node 3 drain 시작 후 readiness, EndpointSlice 및 AWS NLB TargetGroup 상태 전파 실측 | **2026-10-14 관측**: T0 drain 시작 `2026-10-14T02:22:30.437331Z`; `/health/ready`는 +0.079s에 HTTP 503. EndpointSlice `ready: false` T1 `02:22:41.957972Z`(+11.520641s), AWS TargetGroup `draining` T2 `02:22:43.085056Z`(+12.647725s); T2는 30초 전파 예산 이내였다. `nlbRoutingIsolated=false`: NLB 경유 20개 MQTT probe는 모두 unauthenticated였고 하나도 accepted되지 않아 healthy target에만 성공적으로 라우팅됨을 증명하지 않는다. 별도 direct drain-target probe는 `CONNACK 0x03`(Server Unavailable) 거절을 관측했다. 이 결과는 accepted-traffic 또는 active-connection 안전성을 증명하지 않으며, authenticated routing 및 established-connection drain도 시험하지 않았다. 아티팩트: `.artifacts/verification/nlb-lifecycle-verification-report.json`. |
| EKS 라이브 preStop 훅 시맨틱 계약 및 재진입 실측 (DEP-13, DEP-14) | `tests/integration/eks_deployment_test.go` 시맨틱 계약 검증 및 라이브 파드(`narumq-2`) 내 preStop 훅 2회 연속 실행 | **2026-10-14 PASS**: preStop 스크립트 시맨틱 계약(GET 사전확인, 404 시 POST, Bearer 인증, Idempotency-Key, 600s 데드라인, COMPLETED/safeToStop 루프) 검증 `TestEKSDeployment` 통과. 라이브 파드 내 실행: Run 1은 최초 404를 감지해 POST 후 COMPLETED 대기하여 36.532s에 exit 0 완료. Run 2는 기존 COMPLETED 상태(HTTP 200)를 감지하여 중복 POST 없이 1.515s만에 exit 0 완료. 동일 operationId(`cb584088-d8f6-444c-b9f3-3332239b8b03`) 재사용 확인. 아티팩트: `.artifacts/verification/prestop-hook-verification-report.json`. |
| EKS 파드 재생성 후 동일 EBS 볼륨 재연결 관측 (DEP-21, DEP-17, NFR-07) | 파드(`narumq-1`) 재생성 후 동일 PVC/EBS 재연결, 쿼럼 및 적용 index 관측 | **2026-10-14 관측**: 정상 Pod 재생성에서 동일 PVC(`data-narumq-1`)와 EBS 볼륨(`vol-09c3a0e11277903d9`, gp3 20Gi 암호화)이 재연결되었고 쿼럼을 유지했다. 65.064s에 Ready로 복구되며 applied index 93→100이었다. 이는 durable ACK payload 검증이나 WAL/snapshot 무손실 증명은 아니다. EBS 손실, 다른 worker로의 재부착, crash fault는 시험하지 않았다. AWS TargetGroup 3/3 healthy 복구 92.851s는 보고서에 기록되어 있다. 아티팩트: `.artifacts/verification/ebs-storage-az-recovery-report.json`. |
| 격리 EKS controller 교체·재시작 (2026-10-15 재개) | 전용 `narumq-ha-acceptance-20261015`, broker `0.0.0`→합성 `0.0.1`, ARM64 platform digest, 실제 Management API·EBS 감사 PVC | **PASS, 제한 범위**: 세 broker UID 교체·Ready, active-drain controller 재시작 뒤 같은 operation/key 복구, 전체 교체 156.16초. 수정 target의 startup은 수동 readiness 호출 없이 통과했다. `.artifacts/verification/eks-resume-20261015T000337Z-21df3b/rollout-lifecycle-result.json`; `rollout-active-drain-restart.json`. 게시 release 호환성·최대 부하·나머지 restart 경계는 미검증. |
| 격리 EKS API 실패·target 독점 (2026-10-15 재개) | Controller 전용 hostAlias fault proxy는 실제 cluster/features를 전달하고 drain에 HTTP 503을 반환한다. 별도 CR은 같은 target Lease를 요구한다. | **PASS, 관찰 범위**: API 실패와 `target_owned`에서 세 broker UID 불변. `.artifacts/verification/eks-resume-20261015T000337Z-21df3b/rollout-api-unavailable.json`; `rollout-exclusive-target.json`. Proxy를 Service selector로 연결한 첫 설정은 preflight에서 거절됐고 복구 중 한 ordinal이 교체됐다. 이 실패한 설정은 no-mutation 성공 증거에서 제외했다(`fault-setup-recovery.json`). |
| 실패 drain 취소 뒤 재시작 | `TestStateMachineFailedDrainCancellationClearsFailure`, `TestDynamicClusterNodeDrainCancelTimeoutAndResumeProcessAcceptance`의 취소 후 재시작 | **수정 전 EKS·local FAIL → 수정 후 PASS**. 취소 전이의 failure fields를 지웠다. Membership drain 시험 PASS(0.795초), startup·drain 재시작 process 시험 PASS(28.180초). EKS에서 실패한 container의 image만 바꿔 같은 Pod UID·WAL·EBS를 복구했고, 후속 동일 EC2 비정상 재시작도 235.05초에 3-voter Ready와 기존 marker 읽기를 통과했다. 증거: `.artifacts/verification/eks-resume-20261015T000337Z-21df3b/drainfix-same-volume-recovery.json`, `powerloss-afterfix-result.json`. 이전 실패 기록은 보존한다. |
| Startup probe 종료 뒤 drain 기록 재시작 (2026-10-16 원인 분석) | `TestDynamicClusterRestartStartupOnlyAfterProbeKillAndCrashProcessAcceptance`: 3-node process에서 node 3에 `preStop` 같은 drain을 `COMPLETED`까지 실행하고 `SIGKILL`한다. 재시작한 node는 `/health/startup`만 호출한다. 이어 drain 없이 한 번 더 `SIGKILL`하고 같은 방식으로 확인한다. | **수정 전 FAIL → 수정 후 PASS**. 수정 전 tree(`9ddd24d`)에서는 시험 제한 시간 동안 `/health/startup`이 계속 `503`, `node is joining`이었다(시험 98.64초). 수정 후 tree에서는 16.63초에 통과했다. `.artifacts/analysis/clu14-powerloss-0210-rca.md`. |
| Argo CD health·controller 결함 matrix·retry | DRN-SCN-023~027 | 환경·명령·결과는 [health](#argo-cd-health-소비자-수용-2026-10-15), [2026-10-16 결함 matrix](#external-rollout-controller-eks-결함-matrix-수용-2026-10-16), [2026-10-17 재승인](#재승인-retry와-argo-cd-적용-교체-eks-재시험-2026-10-17)에만 둔다. |
| Cross-worker·단일 EC2 복구 | DRN-SCN-034~035 | [계획 이동](#계획된-cross-worker-ebs-복구-수용-2026-10-15), [초기 observer 실패와 후속 관찰](#비정상-단일-ec2-worker-복구-관찰-2026-10-15), [수정 observer 재시험](#생존-pod-고정-observer의-eks-재시험-2026-10-15)을 구분한다. |
| 단일 leader EC2 비정상 종료 (DEP-20·DEP-17, NFR-07) | 현재 leader의 EC2를 OS 종료 없이 종료한다. 생존 follower로 관찰하며 새 leader 선출, 30초 이하 단일 불가용 구간, 같은 PVC·PV·EBS 복구와 durable stream RPO 0을 확인한다. | **2026-10-16 PASS(1회)**. 수정 image `sha256:edcff675…`, leader node 1(`ap-northeast-2b`)의 EC2를 `terminate-instances --skip-os-shutdown`으로 종료했다. 장애 요청 9.2초 뒤 node 3이 leader로 관찰됐다. 장애 뒤 관리 API 26/26·MQTT 37/37 표본이 성공해 표본 기준 불가용 구간은 없었다. 장애 요청 61.3초 뒤 같은 AZ 여분 worker에서 같은 PVC·PV·EBS로 Ready가 됐다. Durable stream은 acked 25개(22개는 장애 요청 뒤 전송)를 `SessionPresent=1`로 모두 받았다. 표본 사이 연속 가용성과 cross-host 시계 오차는 확인하지 않았다. [기준](../specs/drain-and-rolling-update.md#dep-20-leader-ec2-손실) |
| 단일 AZ 전원 중단 모델 (DEP-20, NFR-07) | Follower 한 개가 있는 AZ의 모든 worker를 OS 종료 없이 정지하고 대체 용량 없이 유지한 뒤 다시 시작한다. 중단 중 남은 두 AZ의 quorum·MQTT·session 인계와 재시작 뒤 3-voter 복구, durable stream RPO 0, 통제 해제 뒤 180초 안정 상태를 확인한다. | **2026-10-16 PASS(모델 범위, 1회)**. 2차 실행(수정 image `sha256:edcff675…`, `ap-northeast-2a`, node 3): 480초 정지 동안 관리 API 74/74·MQTT 254/254 표본 성공, 중단 중 생존 node에서 `SessionPresent=1`과 acked 60/60 수신, 시작 요청 뒤 74.0초에 대상 Pod Ready(같은 PVC·PV·EBS), 재개 뒤 180초 동안 ASG activity·instance·Pod UID 변화 0. 1차 실행(이전 image `sha256:1d780c51…`)도 중단·회복 조건은 통과했으나 재개 직후 ASG 교체가 판정 밖에서 일어나 기준을 보완했다. 표본 관찰이며 표본 사이 연속 가용성, network·EBS 서비스·control plane 장애는 확인하지 않았다. [기준과 기록](../specs/drain-and-rolling-update.md#dep-20-단일-az-전원-중단-모델) |
| EBS disk pressure·ENOSPC (DEP-21, NFR-07) | 실제 broker EBS filesystem에 ballast를 써서 follower admission 압박(`0x97`), follower ENOSPC, 두 follower ENOSPC를 차례로 만든다. 공간 회복과 재시작 뒤 durable stream RPO 0을 확인한다. | **2026-10-16 PASS(1회, 3번째 실행)**. 수정 image `sha256:407129c5…`, leader node 3(관찰), 대상 node 4·2, 부하 128 KiB × 2 topic. **D1**: 여유 공간 512 MiB에서 첫 `0x97`이 0.15초에 왔고(15회 모두 `0x97`), 관찰 표본 25/25 성공, ballast 축소 뒤 0.77초에 대상 경로 publish가 성공했다. **D2**: 대상 filesystem을 여유 0 byte까지 채운 4초 뒤 부하를 시작했다. 부하 시작 뒤 관찰을 시작하자 첫 표본에서 대상 자신의 관리 API에 `storage_no_space`·`storagePressure=true`·`replicationHealth=FAILED`가 보였다. ENOSPC가 첫 부하 publish보다 먼저 일어났는지는 확인하지 않았다. Pod는 6.3초에 NotReady가 되었다. 생존 경로 관리 API 54/54·MQTT 55/55 성공. Ballast 제거 45.0초 뒤 같은 Pod UID로 3개 Ready(container 재시작 1회). Durable 29/29, 누락·중복 0. **D3**: 두 대상 모두 자신의 API에서 storage 실패가 보인 10:04:18부터 회복까지 보낸 3개는 모두 결과 미상(`connection_lost`)이고 `acked`는 없었다. 회복 54.5초. Durable PUBACK 3개 모두 수신, 누락 0, 결과 미상 3개 미전달. 두 시나리오 모두 retained 부하 정리가 완료되었다(topic마다 PUBACK reason 0). 앞선 실행: 1번째는 D1 PASS. 2번째(storage-2)는 D2·D3 도구 결함(원격 행으로 판정, 관찰 tunnel 끊김)으로 판정 불가였고, 남은 1 MiB retained 부하가 CLU-14 재시드의 snapshot 결함을 드러냈다([Cluster §7](../specs/cluster-session-and-delivery.md#7-failure-recovery) 7항). 증거: `.artifacts/verification/eks-matrix-20261016T065350Z-24adc2/storage-3/eks-matrix-20261016T065350Z-24adc2/summary.json`. 표본 관찰이며 반복 성능이 아니다. |


## 3. 항목별 수용 기록

각 절은 해당 실행의 증거와 범위를 보존한다.

### ADR-0028 연속 rolling 관측

아래 값은 각 round의 전후 모든 node counter 합 차이다. Node restart로 counter가 0으로 돌아가기 전에 차이를 기록했다.

| reason | Round 1 (node 3) | Round 2 (node 1) | Round 3 (node 2) | 합계 |
| --- | --- | --- | --- | --- |
| `suppressed_normal_disconnect` | 1 | 0 | 0 | 1 |
| `suppressed_reconnected_within_delay` | 1 | 1 | 1 | 3 |
| `expected_mqtt311_server_disconnect` | 1 | 0 | 0 | 1 |
| `expected_zero_or_expired_will_delay` | 2 | 0 | 0 | 2 |
| `unexpected_supported_client` | 0 | 0 | 0 | 0 |
| `unplanned_failure` | 0 | 0 | 0 | 0 |

회귀 명령은 저장소 root에서 실행했다.

```sh
GOMAXPROCS=4 go test ./tests/integration -run '^(TestDynamicClusterNodeDrainProcessAcceptance|TestDynamicClusterNodeDrainCancelTimeoutAndResumeProcessAcceptance|TestDynamicClusterSessionMetadataProcessAcceptance|TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum|TestDynamicClusterDurableQoS2WillProcessAcceptance|TestDynamicClusterMQTTConnectOwnershipTakeoverProcessAcceptance)$' -count=1 -v -timeout 30m
```

정상 SDK drain은 통과했으며 Go 중복 Server Reference 실패는 선택적 진단에 남는다. Synthetic local controller 수용은 게시 release·production 부하·MQTT-13 90일 관측·독립 평가를 대신하지 않는다.

### Argo CD health 소비자 수용 (2026-10-15)

[Rollout controller v1 §3](../contracts/rollout-controller-v1.md#3-acceptance-matrix)의 `Argo CD health` 행과 ADR-0006 §7 검증 6을 확인했다. 대상은 HEAD `8096f30a05735084bfafd6eb42b2763c2db31f79` 작업 트리의 기존 `argocd-health.lua`와 `crd.yaml`이다. 두 파일은 이번 시험에서 수정하지 않았다. 환경은 별도 local state·VPC로 만든 EKS 1.37, ARM64 `m7g.large` worker 3대, Argo CD core `v3.5.5`, kubectl `v1.37.1`이다. Argo CD·Redis·시험용 Git 서버는 ARM64 manifest digest로 고정했다. 버전·hash·실제 Pod imageID는 [설치 출처](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/argo-install-provenance.json)와 [실행 출처](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/argo-runtime-provenance.json), [최종 실행 설정](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/argo-final-runtime.json)에 있다.

실제 Git repository를 내부 ClusterIP로 제공했다. Argo CD가 고정 revision에서 시험용 `NaruMQRollout`을 동기화했다. 이후 Kubernetes status subresource에 상태를 주입했다. 각 case는 Git sync가 `Synced`이고 ComparisonError가 없으며 리소스와 Application의 health가 모두 기대값일 때만 통과했다. 명령은 전용 임시 observer의 `python -B observe-health.py`다. 정확한 입력·기대값과 관찰 시각·resourceVersion·message는 [입력 matrix](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/health-matrix-inputs.json)와 [15개 case 결과](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/health-matrix-result.json)에 있다.

실행한 observer 원문은 [검증 아티팩트](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/observe-health.py)에 보존했다. 보존본의 SHA-256은 실행 당시 기록한 `observerSha256`과 같다. 실행 당시에는 전용 private 디렉터리의 `kubectl.exe`·`kubeconfig`와 같은 위치에서 실행했다. 이 입력과 AWS fixture는 시험 후 제거했으므로 보존본 자체를 현재 환경에 실행하는 재시험 명령으로 사용하면 안 된다.

2026-10-17에 DRN-SCN-025 observer와 15개 입력을 추적 경로 [Argo CD health observer](../../tests/chaos/eks-argo-health/main.py)와 [입력 matrix](../../tests/chaos/eks-argo-health/health_matrix.json)로 옮겼다. 경로·namespace·Application은 인자로 받고 AWS 인증 설정은 환경 변수로 받는다. `--private`에는 kubectl과 kubeconfig가 필요하다. `--output` 디렉터리 이름은 kubeconfig context와 같아야 한다. 이번 변경에서 EKS 시험은 **미실행**이다. 기존 fixture를 삭제했으므로 새 연결을 과거 실행의 통과 결과로 보지 않는다.

완료 후 실패, 취소 후 알 수 없는 phase, 완료 후 status 삭제 전이도 통과했다. 이전 `Healthy`가 남지 않았다. `failureReason`의 표시와 빈 phase도 확인했다. 같은 CR UID와 spec을 유지했으며 broker Pod는 0개였다. 이는 health 소비자 수용이다. NaruMQ controller의 상태 생성, 실제 broker drain·교체, GitOps 전체 배포, 게시 release·최대 부하 수용은 검증하지 않았다.

Argo CD의 resource health를 함께 관찰하려고 공식 `ARGOCD_APPLICATION_CONTROLLER_PERSIST_RESOURCE_HEALTH=true` 옵션을 켰다. Health 코드는 바꾸지 않았다. [관찰 설정](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/observer-configuration.json)과 [연결 절차](../how-to/development-guide.md#argo-cd-health-mapping-연결과-확인)를 따른다.

시험환경은 삭제했고 state 관리 자원은 0개다. 보호 snapshot 3개·공유 image는 유지했다([정리 확인](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/direct-cleanup-verification.json), [최종 기록](../../.artifacts/verification/eks-argo-health-20261015T042405Z-886bea/final-summary.json)). 전체 local은 미실행이며 이전 실패는 유지한다.

### External rollout controller EKS 결함 matrix 수용 (2026-10-16)

결과는 **남은 결함 matrix A~G 통과와 Argo CD 적용 교체의 부분 검증**이다. A~G의 마지막 결과 파일은 모두 `PASS`다. Argo CD 적용 교체 결과 파일(`rollout-argo-upgrade.json`)은 sync 판정이 `FAIL`이다. 고친 controller에서 Pod 교체 없이 sync만 확인한 결과(`rollout-argo-sync-r3.json`)는 `PASS`다. 시험 중 controller 결함 3개를 찾았다. 2개는 고친 image로 다시 시험했다. 나머지 1개는 시험 환경을 지운 뒤 단위 시험으로만 확인했다. 과거 실패 기록과 처음 판정 결과는 그대로 보존했다.

환경은 run `eks-rollout-20261016T122800Z-a3f1b2`다. 별도 private Terraform state와 VPC(`10.39.0.0/16`)로 EKS cluster `narumq-rollout-20261016-a3f1b2`를 만들었다. 생성 계획은 새 자원 41개만 만들었다([계획 검토](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/create-plan-review.json)). 변경 명령은 모두 kubeconfig context, cluster endpoint·tag, namespace label이 같은 run을 가리킬 때만 실행했다([fixture identity](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/fixture-identity.json)). Broker는 합성 데이터만 쓰는 3-voter `OnDelete` StatefulSet이다. Broker image는 같은 소스에서 version label만 다른 arm64 1.2.0·1.3.0·1.4.0이다([image 출처](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/image-provenance.json)).

도구는 [main.py](../../tests/chaos/eks-rollout-faults/main.py)다. CLI 진입점은 없고, 운영자 Python 세션에서 시나리오 함수를 차례로 호출했다. [Fault proxy](../../tests/chaos/eks-rollout-faults/proxy.py)는 controller와 broker Management Service 사이에서 실제 broker 응답을 바꿔 drain GET 장애를 만든다. [Admission webhook](../../tests/chaos/eks-rollout-faults/delete_delay_webhook.py)은 Pod DELETE 응답을 늦춰 결과 불명을 만든다. Pod DELETE는 EKS Kubernetes API audit log(CloudWatch)로 교차 확인했다. 도구 자체는 `python -B tests/test_eks_rollout_faults.py`로 시험한다(15개 PASS, AWS·kubectl 호출은 fake).

Controller image는 세 가지다. 처음 image는 `sha256:bfd26fd0cac446668b22e80e4b576fcd628b1b809b5784002f195ae1d2a4e790`이다. r2(`sha256:89dca1673f5e9c23b96fc894cc931cd0738740c86f2f1e9777f90d54d55b79c8`)는 아래 결함 1을 고쳤다. r3(`sha256:ba7bee40edfb7af8997e501d83af0395ccd75f3ea3b06f0c964bc2757fdac9b7`)는 r2에 결함 2의 수정만 더했다. r2와 r3의 차이는 `internal/adapters/rollout/reconciler.go`의 finalizer write뿐이다.

| 시나리오 | Controller | 결과 | 증거 |
| --- | --- | --- | --- |
| A. 실제 broker drain `FAILED` | r2 | PASS(판정 수정) | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-drain-failed.json), [처음 판정](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-drain-failed.initial-check-fail.json) |
| B. Drain GET incomplete·malformed·503·응답 지연 뒤 deadline | r2 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-get-faults-deadline.json) |
| C. 실제 PVC audit append 장애 | r2 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-audit-sink-failure.json) |
| D. Pod DELETE 결과 불명, 그 중 controller restart, Pod resourceVersion 변경 | r2 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-delete-unknown.json), [중단된 두 번째 시도](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-delete-unknown.attempt2-incomplete.json) |
| E. Drain POST 결과 불명 중 restart, non-leader process | r2 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-post-unknown-non-leader.json) |
| F. Status CAS 충돌 | r2 | PASS(판정 수정) | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-cas-conflict.json), [처음 판정](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-cas-conflict.initial-check-fail.json) |
| G. 실제 audit 파일의 approval·denial·failure | r2 | PASS(판정 수정) | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-audit-events.json), [처음 판정](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-audit-events.initial-check-fail.json) |
| A~G 구간의 Kubernetes audit DELETE | 처음·r2 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-kubernetes-audit-deletes.json), [구간](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/scenario-windows.json) |
| Argo CD 적용 교체 1.3.0→1.4.0 | r2 | **FAIL**(sync 1건, 결함 2) | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-upgrade.json), [처음 판정](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-upgrade.initial-check-fail.json), [Git 내용](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-git.json) |
| Argo CD 단계의 Kubernetes audit DELETE | r2 | PASS(판정 수정) | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-kubernetes-audit.json), [처음 판정(같은 기록으로 재계산)](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-kubernetes-audit.initial-check-fail.json) |
| Argo CD finalizer sync 재확인 | r3 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-argo-sync-r3.json) |

처음 image의 A·B·C·D 결과는 `*.controller-bfd26fd0.json`으로 보존했다. 처음 image의 D는 blocker 표시 확인에서 실패했다.

주요 실측은 다음과 같다.

- B: 각 장애 구간에서 실제 broker drain은 `DRAINING`이었다. Rollout은 `Draining`에 머물렀고 delete와 다른 node drain 요청은 0건이었다. GET 간격 중앙값은 incomplete 5.23초, malformed 5.2초, 503 5.19초, 응답 지연 10.17초였다. 이어 저장된 deadline에서 `drain_timeout`으로 실패했다.
- C: Audit 파일을 변경 불가로 만들자 실제 `SAFE_TO_STOP` 뒤에도 `audit_unavailable`로 45초 동안 차단했다. 승인 저장, 파일 변경, API server에 도달한 DELETE는 0건이었다.
- D: Rollout은 `delete_unconfirmed`를 계속 표시했고 같은 UID/RV precondition으로만 재시도했다. Restart 뒤 새 leader도 같은 precondition을 썼다. Pod resourceVersion이 바뀌자 fresh 증거로 다시 승인했다. 이전 precondition의 DELETE는 API server가 409로 거절했고, 다음 DELETE가 성공했다. deletionTimestamp 뒤 중복 DELETE는 0건이었다.
- E: 새 leader는 저장된 idempotency key로 POST를 replay하고, broker가 첫 POST로 만든 operation을 채택했다. Non-leader process는 Management API를 호출하지 않았고 Lease를 가져가지 않았다.
- F: Controller log에 CAS 충돌 4건이 있었다. 2건은 `Draining`, 2건은 `DrainSafe` 승인 write에서 났다. 충돌 동안 delete intent 저장과 Pod 삭제는 0건이었다. 승인은 충돌 뒤 다시 조정해 성공했다.
- Argo CD: Argo CD core `v3.5.5`가 시험용 Git 서버의 revision `08dc1c3e5552a33df87011ca83d858ed2f4b2046`에서 StatefulSet template(1.4.0)과 `NaruMQRollout`을 동기화했다. 15:46:15~15:49:30 UTC(195초)에 r2 controller가 세 broker를 하나씩 교체했다. 각 Pod에서 controller DELETE는 1건이었다. Argo CD의 DELETE는 0건이었다. Kubelet의 마무리 DELETE는 같은 UID precondition으로 Pod object를 지우는 Kubernetes 동작이다. Application은 교체 중 `Progressing`, 완료 뒤 `Healthy`였다. 그러나 완료 뒤 Application은 `OutOfSync`였으므로 이 결과 파일의 sync 판정은 `FAIL`이다(결함 2).
- Argo CD finalizer sync 재확인: r3에서 이전 CR을 지우자 controller가 finalizer를 patch로 해제했다. 새 revision `aee757cb1d73671247c52a8a2bdd40bbccc2c2e0`의 CR은 모든 Pod가 이미 1.4.0이므로 Pod 교체 없이 완료됐다. 완료 뒤 60초 이상 Application과 CR이 `Synced`·`Healthy`였다. Live CR에는 `spec.abort: false`와 finalizer가 함께 있었다. 이 확인은 finalizer write만 검증한다. 고친 controller로 실제 교체와 sync를 함께 확인하지는 않았다.

찾은 결함과 처리는 다음과 같다.

1. **Status 반복 write와 blocker 깜박임(처음 image)**: Controller는 broker의 밀리초 시각을 초 단위로 저장된 시각과 비교했다. 그래서 매 조회가 "증거 변경"이 되어 status를 다시 썼다. 다시 쓸 때마다 `failureCode`가 지워지고 Ready=True가 됐다. 매 write는 resourceVersion을 바꿔 watch event를 일으켰고, poll 간격 없이 바로 다시 조정했다. 14:29:44~14:35:16에 controller DELETE가 946건(500 945건, 409 1건), GET이 초당 약 6건이었다. r2는 시각을 저장 정밀도로 줄여 비교하고, 바뀐 내용이 없으면 status를 쓰지 않는다([계약 §2.6](../contracts/rollout-controller-v1.md#2-reconciliation-contract)). `TestUnknownDeleteRetryKeepsBlockerWithoutRewritingUnchangedStatus`는 수정 전 실패(`writes=10 want=8`), 수정 뒤 통과했다.
2. **Finalizer 설치가 Git spec을 지움(r2)**: Controller가 CR 전체를 `Update`로 다시 써서 `spec.abort: false`가 사라졌다. CRD의 `abort`에는 기본값이 없다. 그래서 Argo CD가 CR을 계속 `OutOfSync`로 판단했다. Sync 작업 자체는 `Succeeded`였다. r3는 `metadata.finalizers`만 바꾸는 merge patch를 resourceVersion 잠금과 함께 쓴다([계약 §2.11](../contracts/rollout-controller-v1.md#2-reconciliation-contract)). `TestReconcilerMissingSecretsAndNormalReconcileCAS`는 수정 전 실패(patch 0건), 수정 뒤 통과했다.
3. **Deadline 뒤 승인 단계가 끝나지 않음(처음 image에서 관찰)**: `DrainSafe`·`DeleteApproved`에서 저장된 deadline이 지나면 controller가 `barrier_unknown`으로 계속 차단했다. Delete는 0건이었지만 `Failed`와 operator action이 표시되지 않았다. 원인은 `requireSafeDrain`이 deadline 경과를 일반 오류로 돌려준 것이다. 시험 뒤 deadline 경과를 `Failed`/`drain_timeout`으로 끝내도록 고쳤다(§2.9). 이때 broker drain은 이미 `SAFE_TO_STOP`이라 취소할 수 없다. 그래서 status와 `rollout.failed` 감사 event의 operator action은 `spec.abort=true`만 안내한다. `TestApprovalAfterPersistedDeadlineFailsWithoutDelete` 4개 case가 수정 전 모두 실패, 수정 뒤 통과했다. 이 수정은 이 run에서 EKS로 다시 실행하지 않았다. 2026-10-17 재시험에서 `DrainSafe`의 deadline 실패를 EKS로 확인했다. 그때의 operator action은 재승인도 안내한다([아래 기록](#재승인-retry와-argo-cd-적용-교체-eks-재시험-2026-10-17)).

당시 미결정 사항이 하나 있었다. Broker는 `SAFE_TO_STOP` drain을 취소하지 않는다(§2.3). 그래서 `SAFE_TO_STOP` 뒤 실패한 rollout에서 새 `retryToken`은 `retry_cancel_unconfirmed`로 차단됐다. `spec.abort=true`는 delete 없이 `Aborted`로 끝났다([abort 결과](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/rollout-abort-after-safe-to-stop.json), 운영자 세션에서 직접 판정). 2026-10-17 사용자 결정으로 이 retry는 같은 drain을 다시 승인한다([계약 §2.9](../contracts/rollout-controller-v1.md#2-reconciliation-contract)). EKS 결과는 [아래 기록](#재승인-retry와-argo-cd-적용-교체-eks-재시험-2026-10-17)을 따른다.

판정 수정은 관찰 데이터가 처음 기대와 달랐을 때만 했다. 각 결과 파일의 `checkRevision`에 이유가 있다.

- A: 저장된 deadline을 초 단위로 줄였으므로 controller deadline이 broker deadline보다 먼저 온다. 그래서 controller가 broker `FAILED`보다 먼저 `drain_timeout`으로 실패한다. 판정은 실제 broker GET이 같은 operation의 `FAILED`를 보고할 때까지 기다리도록 바꿨다.
- F: Status adapter가 UID·resourceVersion 변경을 스스로 감지하므로 API server 409는 생기지 않는다. 승인 write 충돌은 3건이 아니라 2건이었다.
- G: 이전 시도 CR의 approval까지 세었으므로 rollout UID로 범위를 좁혔다.
- Argo CD health: 첫 표본(15:46:32)은 CR이 Argo CD resource tree에 나타나기 전이었다. 이 `Healthy`는 Argo CD 자체 집계다. 판정은 CR이 tree에 있는 표본만 본다. Sync 판정 FAIL은 그대로 둔다.
- Argo CD audit DELETE: Kubelet의 마무리 DELETE를 controller DELETE의 완료로 인정했다. 처음 결과 파일은 두 번째 실행이 덮어썼으므로, 같은 audit 기록에 원래 판정을 다시 계산해 보존했다.

범위의 한계는 다음과 같다. 합성 fixture에서 한 번 실행했다. A~G는 r2, finalizer sync는 r3, 결함 3 수정은 단위 시험으로만 확인했다. 다른 OCI/version preflight 거절 조합, 승인 직전·replacement 대기 중 restart는 EKS에서 주입하지 않았다. 게시 release N-1↔N과 rollback은 `CLU-11`, 최대 부하와 연속 100회는 `DEP-01~12` TODO 항목으로 넘겼다.

시험환경과 run 전용 image는 삭제했고 보호 snapshot 3개·공유 ECR tag 2개는 유지했다([정리 확인](../../.artifacts/verification/eks-rollout-20261016T122800Z-a3f1b2/cleanup-verification.json)). Private 입력은 DPAPI로 암호화했다.

### 재승인 retry와 Argo CD 적용 교체 EKS 재시험 (2026-10-17)

결과는 **DEP-11의 남은 범위와 고친 controller의 Argo CD 적용 교체·sync 통과**다. 마지막 결과 파일은 모두 `PASS`다. `SAFE_TO_STOP` retry의 첫 실행은 도구 결함으로 `FAIL`이었고, 그 기록을 그대로 보존했다. 시험 대상은 HEAD `0980bd5`에 아직 commit하지 않은 `SAFE_TO_STOP` retry 변경(`internal/application/rollout`, `internal/adapters/rollout`, CRD)을 더한 작업 트리다.

환경은 run `eks-rollout-20261016T232359Z-68569c`다. 별도 private Terraform state와 VPC(`10.40.0.0/16`)로 EKS 1.37 cluster `narumq-rollout-20261016-68569c`와 `m7g.large` worker 3대(AZ a·b·c)를 만들었다. 생성 계획은 새 자원 41개만 만들었다([계획 검토](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/create-plan-review.json)). 변경 명령은 모두 같은 run의 identity를 확인한 뒤에만 실행했다([fixture identity](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/fixture-identity.json)). Broker 1.2.0·1.3.0·1.4.0 image는 같은 소스에서 다시 build했고, digest가 이전 run과 같았다. Controller는 version `1.3.1`(`sha256:b27b1f3286ea7a69a8229c44b11769022755cec5b5238c5755797bb0ea4d5086`)이다([image 출처](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/image-provenance.json)). Controller 전용 Operator API key는 broker를 처음 배포하기 전에 설정에 넣었다. 그래서 이전 run과 달리 broker를 다시 초기화하지 않았다.

도구는 이전과 같은 [main.py](../../tests/chaos/eks-rollout-faults/main.py)다. 새 함수 `scenario_safe_retry`를 사용했다. 이 함수는 controller audit 파일을 변경 불가(`chattr +i`)로 만든다. 그러면 실제 `SAFE_TO_STOP` 뒤에도 승인이 `audit_unavailable`로 막힌다. 이어서 `retryToken` 세 개로 다음 순서를 확인한다.

1. 첫 token: broker deadline이 150초인 drain이 `SAFE_TO_STOP`에 도달한다. Rollout은 그 deadline에서 `Failed`/`drain_timeout`이 된다.
2. 둘째 token: broker deadline이 지난 뒤 바꾼다. Controller는 같은 idempotency key와 operation으로 다시 승인하고 새 `approvalDeadlineAt`을 저장한다. 승인은 계속 막히고, rollout은 그 시한에서 다시 `Failed`가 된다.
3. 셋째 token: audit 파일을 되돌리고 controller를 재시작한 뒤 바꾼다. Controller는 다시 승인하고 세 broker를 교체한다.

| 시나리오 | 결과 | 증거 |
| --- | --- | --- |
| A. 실제 broker drain `FAILED` | PASS(2회) | [결과](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/rollout-drain-failed.json), [첫 CR의 결과](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/rollout-drain-failed.run1-narumq-upgrade-130.json) |
| H. `SAFE_TO_STOP` 뒤 retry 재승인 | PASS(도구 수정 뒤 재실행) | [결과](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/rollout-safe-retry.json), [첫 실행](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/rollout-safe-retry.initial-check-fail.json), [재실행 전 초기화](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/fixture-reset-before-rerun.json) |
| G. 실제 audit 파일의 approval·denial·failure | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/rollout-audit-events.json) |
| 위 구간의 Kubernetes audit DELETE | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/rollout-kubernetes-audit-deletes.json), [구간](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/scenario-windows.json) |
| Argo CD 적용 교체 1.3.0→1.4.0과 완료 뒤 60초 sync 유지 | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/rollout-argo-upgrade.json), [Git 내용](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/rollout-argo-git.json) |
| Argo CD 단계의 Kubernetes audit DELETE | PASS | [결과](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/rollout-argo-kubernetes-audit.json) |

주요 실측은 다음과 같다. 시각은 cluster 시계의 UTC다.

- H(CR `narumq-upgrade-130b`): Broker drain deadline은 00:31:47.078Z였다. Broker가 같은 operation을 `COMPLETED`·`safeToStop=true`로 보고하는 상태에서 rollout은 00:31:49Z에 `Failed`/`drain_timeout`이 됐다. Status는 재승인 또는 abort를 안내했다. 둘째 token 뒤 rollout은 같은 attempt key와 operation ID로 `DrainSafe`가 됐고 `approvalDeadlineAt`은 00:41:53Z였다. 이 값에서 10분을 빼도 broker deadline보다 늦다. 따라서 broker deadline이 지난 뒤 재승인했고, 승인 시한도 broker deadline으로 줄지 않았다. 승인은 audit 장애로 계속 막혔고 rollout은 00:41:57Z에 다시 `Failed`/`drain_timeout`이 됐다. 두 실패 동안 audit 파일의 byte와 broker Pod UID는 바뀌지 않았다. Controller 재시작과 셋째 token 뒤 00:42:51Z에 `narumq-0` 삭제를 승인했다. 00:44:54Z에 세 Pod가 1.3.0으로 교체되어 `Completed`가 됐다. 첫 실패 뒤 node 1로 간 drain POST와 취소는 0건이었다.
- H의 resourceVersion: 첫 실패와 재승인에서 Pod resourceVersion이 모두 `15641`이었다. 이번 실행은 바뀐 resourceVersion을 채택하는 경로를 지나지 않았다. 이 경로는 단위 시험이 판정한다.
- G: Hash chain 검증을 통과한 event는 approval 3건, denial 2건, failure 2건이다. 재승인 뒤 approval도 포함한다. Failure 2건은 두 CR의 A 실패다. H의 두 실패는 audit 장애를 일부러 만든 동안 일어났으므로 `rollout.failed`·`rollout.pod.delete_denied` event가 없다. 기존 `Service.fail`은 audit sink가 준비되지 않으면 이 두 event를 생략하고 `Failed` status만 저장한다. 계약 §5는 이 생략을 명시하지 않는다.
- Argo CD: Argo CD core `v3.5.5`가 시험용 Git 서버의 revision `22312ca8c3fdd4c14cfda3d3e5d34898543d5a9b`에서 StatefulSet template(1.4.0)과 `NaruMQRollout`을 동기화했다. Git의 CR에는 `abort: false`가 있었다. 00:48:12~00:52:25 UTC(253초)에 controller가 세 broker를 하나씩 교체했다. 각 Pod의 controller DELETE는 1건, Argo CD의 DELETE는 0건이었다. Application은 교체 중 `Progressing`, 완료 뒤 `Healthy`였다. 완료 뒤 60초 동안 Application과 CR이 `Synced`·`Healthy`였다. Live CR spec은 Git spec과 같았고 finalizer도 있었다. 이전 run에서 남은 "고친 controller의 실제 교체와 sync" 확인이 이것으로 끝났다.

H의 첫 실행은 도구 결함 두 가지로 `FAIL`이었다.

1. 셋째 단계에서 controller를 재시작하지 않았다. `FileAuditPublisher`는 쓰기에 한 번 실패하면 process가 다시 시작할 때까지 사용할 수 없는 상태로 남는다(기존 fail-closed 규칙). 그래서 audit 파일을 되돌려도 10분 동안 `audit_unavailable`이었고 rollout은 다시 `Failed`가 됐다. Delete는 0건이었다.
2. Retry 시각을 workstation 시계로 broker deadline과 비교했다. Workstation 시계가 cluster보다 4초 이상 늦어 판정이 틀렸다.

도구를 고쳐 셋째 단계 전에 controller를 재시작하고, cluster 시계(`approvalDeadlineAt`−10분과 broker `deadlineAt`)로 비교했다. 첫 실행에서 나머지 단계 1·2 판정은 통과했다. 재실행 전에 첫 CR을 abort했다. Delete는 0건이었고 drain은 `SAFE_TO_STOP`으로 남았다. 운영자가 `narumq-0`을 1.2.0으로 한 번 재시작해 drain 기록을 지우고 controller도 재시작했다.

범위의 한계는 다음과 같다. 합성 fixture에서 한 번 실행했다. Pod resourceVersion 변경 채택, delete intent 뒤 Pod 변경(`AwaitingReplacement`), `pod_identity_changed`·`retry_cancel_unconfirmed` 차단과 `DeleteApproved` 단계의 deadline 실패는 EKS에서 만들지 않았다. 이 경로들은 단위 시험이 판정한다. 최대 부하, 연속 100회와 게시 release 수용은 기존 TODO 항목이다.

시험환경과 run 전용 image는 삭제했고 보호 snapshot 3개·공유 ECR tag 2개는 유지했다([정리 확인](../../.artifacts/verification/eks-rollout-20261016T232359Z-68569c/cleanup-verification.json)). 증거의 secret 부재를 확인하고 private 입력은 DPAPI로 암호화했다.

### 계획된 cross-worker EBS 복구 수용 (2026-10-15)

대상은 commit `9c96670`과 기존 제품 코드다. 코드·배포 template은 바꾸지 않았다. 별도 local state·VPC의 EKS 1.37에 `m7g.large` 3대와 빈 gp3 PVC 3개로 합성 3-voter cluster를 만들었다. 초기 startup은 수동 readiness GET 없이 Kubernetes probe만으로 통과했다. 기존 합성 fixture의 인증 설정만 재사용했고 새 cluster ID와 PVC를 사용했다. 원본 archive나 보호 snapshot은 복원하지 않았다. 이동 대상 EBS와 같은 `ap-northeast-2c`에 임시 ARM64 worker 하나를 추가했다. [배포 기록](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/fixture-deployment.json)과 [검증한 이미지](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/task-image-verified.json), [worker·AMI·volume 소유 기록](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/cleanup-ownership.json)을 참조한다.

시험 파일은 [MQTT oracle](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/mqtt-oracle.py)과 [worker 이동 observer](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/relocate-worker.py)다. Python 3.14의 전용 임시 venv에 Paho MQTT `2.1.0`을 설치했다. MQTT 5 password 인증과 session expiry 3,600초를 사용했다. 파일에는 비밀 값을 넣지 않았다. `--private` 위치의 전용 kubeconfig·kubectl 1.37.1·인증 파일과 같은 run 디렉터리의 `relocation-plan.json`이 선행 입력이다. Survivor Pod의 management/MQTT는 localhost `23880`/`23883`, 대상 Pod는 `28880`/`28883`으로 전달했다. 아래는 실제 실행 순서다. 각 `<...>`는 당시 전용 경로다. 환경을 폐기했으므로 현재 그대로 실행하는 재시험 명령은 아니다.

```text
<venv-python> -B <run-directory>/mqtt-oracle.py prepare --private <private-directory>
<venv-python> -B <run-directory>/relocate-worker.py --private <private-directory>
<venv-python> -B <run-directory>/mqtt-oracle.py verify --private <private-directory>
```

2026-10-17에 DRN-SCN-034의 물리 이동 observer와 MQTT oracle을 [물리 이동 진입점](../../tests/chaos/eks-planned-relocation/relocate_worker.py)과 [MQTT 진입점](../../tests/chaos/eks-planned-relocation/mqtt_oracle.py)으로 옮겼다. 실행 순서는 `mqtt_oracle.py prepare`, `relocate_worker.py`, `mqtt_oracle.py verify`다. 세 명령에는 같은 `--private`와 `--output`을 전달한다. `--output`에는 원본 형식의 `relocation-plan.json`이 필요하며 디렉터리 이름은 cluster ID와 kubeconfig context다. AWS 인증 설정은 환경 변수로 받는다. Pod 교체 뒤에는 대상 Pod port-forward를 다시 연결하고 `verify`를 실행한다. 두 시나리오 함수가 모두 통과해야 물리 이동과 MQTT 상태를 함께 확인한 것이다. 이번 변경에서 EKS 시험은 **미실행**이다. 기존 fixture가 없으므로 과거 통과 기록을 새 실행 결과로 옮기지 않는다.

첫 명령은 persistent subscription을 만든 뒤 연결을 끊었다. 이어 retained marker 하나와 오프라인 QoS 1 메시지 세 개의 성공 PUBACK을 확인했다. 이동 observer는 follower node 2의 기존 worker를 cordon하고 실제 drain API의 `COMPLETED/SAFE_TO_STOP`을 확인했다. 연결 잔여 0, pending WAL 0, under-replicated 0과 실패 없음도 확인했다. 이후에만 UID·resourceVersion precondition을 사용해 Pod를 삭제했다. [삭제 전 barrier 증거](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/safe-before-delete.json)가 순서를 입증한다.

관찰 시작 후 64.008초에 다른 EC2 worker의 새 Pod를 포함한 broker 3개가 Ready인 것을 확인했다. 이 시간에는 cordon과 drain이 포함된다. AWS attachment 확인과 복구 후 표본 수집까지 포함한 observer 전체 실행은 73.746초였다. PVC UID·PV 이름·EBS volume ID가 같았고 AWS attachment와 CSI VolumeAttachment는 새 worker를 가리켰다. Survivor 경로의 36개 표본은 모두 quorum available과 성공 PUBACK을 기록했다. 이는 표본 관찰이며 표본 사이의 모든 순간이나 production SLO를 증명하지 않는다. 세 번째 명령은 이동한 broker에 재연결해 `sessionPresent=true`, 기대한 오프라인 QoS 1 payload hash 세 개와 retained hash를 확인했다. 재발행은 하지 않았고 이 실행의 QoS 1 중복은 0개였다. [물리 복구 결과](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/physical-relocation-result.json), [연속 관찰](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/continuity-observations.json), [복구 후 MQTT 결과](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/mqtt-after-relocation.json), [최종 broker·CSI 기록](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/recovered-broker-and-csi.json)을 따른다.

결과는 [계획된 cross-worker PASS](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/acceptance-result.json)다. 생존 voter의 복제도 사용하므로 EBS 단독 복구, EC2 전원 손실·강제 detach·AZ 손실·volume loss/reseed·전체 voter commit·자연 WAL torn-tail·production 부하는 확인하지 않았다. 기존 `CLU-14 FAILED/UNRESOLVED`와 다른 계획 이동 모델이다.

시험환경과 run 전용 image는 삭제하고 보호 snapshot 3개·공유 image는 유지했다([정리 확인](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/direct-cleanup-verification.json), [최종 기록](../../.artifacts/verification/eks-ebs-worker-20261015T052948Z-cd5fab/final-summary.json)). 관찰 뒤 합성 Pod의 강제 정리는 SAFE_TO_STOP 수용과 별개다. 전체 local은 미실행이다.

### 비정상 단일 EC2 worker 복구 관찰 (2026-10-15)

결과는 **초기 observer 실패와 같은 장애의 복구 후속 관찰을 구분**한다. 초기 실행을 통과로 바꾸지 않았고 장애도 다시 주입하지 않았다. [결과 기록](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/acceptance-result.json)의 상태는 `LIMITED_RECOVERY_OBSERVED_WITH_FAILED_INITIAL_OBSERVER`다. 기존 `CLU-14 FAILED/UNRESOLVED`, 전체 EC2·AZ 장애 matrix, 연속 가용성과 자동 observer 완료 gate는 열린 상태로 유지한다.

대상 HEAD는 `0c2e527`이다. 제품 코드가 `9c96670`과 같음을 확인한 뒤 이전 합성 ARM64 broker image를 작업 전용 tag로 다시 게시했다. 공유 image는 바꾸지 않았다. 별도 local state·새 VPC의 EKS 1.37, `m7g.large` 3-AZ worker, 새 gp3 PVC 3개를 사용했다. 기존 합성 fixture에서는 인증 설정만 재사용했다. 초기 startup은 수동 readiness GET 없이 Kubernetes probe로 통과했다. 같은 AZ의 별도 Ready worker 하나와 대상 follower EBS의 완료된 전용 snapshot을 먼저 확보했다. 실제 `/data` filesystem은 ext4였고 data volume의 `DeleteOnTermination=false`도 확인했다. [배포 입력 경계](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/fixture-deployment.json), [fault plan](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/fault-plan.json), [사전 점검](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/fault-preflight.json), [filesystem](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/target-filesystem.json)을 참조한다.

시험 소스는 [MQTT oracle](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/mqtt-oracle.py), [최초 abrupt worker observer](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/abrupt-worker.py), [읽기 전용 후속 observer](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/observe-recovery.py)다. 전용 임시 venv의 Python 3.14·Paho MQTT 2.1.0과 kubectl 1.37.1을 사용했다. `<private-directory>`에는 해당 fixture의 kubeconfig·kubectl·인증 파일이 필요하다. 같은 run 디렉터리의 plan도 선행 입력이다. 아래는 당시 실행 순서이며 폐기한 환경에 현재 그대로 실행하는 재시험 명령이 아니다.

```text
<venv-python> -B <run-directory>/mqtt-oracle.py prepare --private <private-directory>
<venv-python> -B <run-directory>/abrupt-worker.py --private <private-directory>
# 위 observer의 CAS 실패와 기존 Node의 자동 삭제를 확인한 뒤, 같은 장애만 계속 관찰했다.
<venv-python> -B <run-directory>/observe-recovery.py --private <private-directory>
<venv-python> -B <run-directory>/mqtt-oracle.py verify --private <private-directory>
```

첫 명령은 persistent subscription을 만든 뒤 연결을 끊고 retained marker와 오프라인 QoS 1 메시지 세 개의 성공 PUBACK을 확인했다. 두 번째 명령은 active drain 기록이 없는 follower node 3의 EC2에 `TerminateInstances`의 `SkipOsShutdown=true`를 요청했다. Fault 전에 drain·Pod delete를 호출하지 않았다. `terminated` 상태는 요청 후 19.405초에 확인했다. [종료 요청](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/fault-request.json)과 [종료 확인](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/terminated-fencing-proof.json)이 이 경계를 기록한다. Operator의 out-of-service 변경은 이 확인 뒤에만 허용했다([Kubernetes non-graceful shutdown 절차](https://kubernetes.io/docs/concepts/cluster-administration/node-shutdown/#non-graceful-node-shutdown)).

최초 observer의 out-of-service taint JSON Patch 요청은 HTTP 422로 거절돼 종료됐다. Patch에는 UID·resourceVersion 검사가 있었다. 기대한 resourceVersion은 `2217`이었고, Kubernetes audit에는 그 요청 0.300초 전에 다른 client의 성공 patch가 resourceVersion을 `3175`로 바꾼 기록이 있다. 오래된 resourceVersion 검사에서 거절됐을 가능성이 높지만, 응답에 실패한 연산이 없어 원인을 확정하지 않는다. 거절된 taint는 적용되지 않았다. 이후 AWS cloud controller의 기존 Node DELETE가 HTTP 200으로 완료됐다. 수동 Pod delete나 EC2 `Force`·EBS force-detach 요청은 사용하지 않았다. [실패 원문](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/abrupt-recovery-failure.json), [patch 거절 audit](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/node-patch-rejection-audit.json), [Node lifecycle audit](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/old-node-lifecycle-audit.json)을 따른다.

초기 관리 API probe는 첫 표본만 quorum·PUBACK 성공을 기록했고 이후 세 표본은 client timeout이었다. Client timeout 5초는 [Peer observability §3](../contracts/peer-observability-v1.md#3-한도와-시간)의 전체 query 예산과 같다. 이 표본 수집기는 관리 조회 뒤 MQTT publish를 수행하므로 timeout된 세 조회 뒤에는 MQTT probe를 보내지 못했다. 이를 MQTT 전송 실패나 서버 계약 위반의 증거로 쓰지 않는다. 후속 observer는 client timeout 10초로 읽기만 수행했고 69개 조회 모두 quorum available을 반환했다. 실패 뒤 관찰 공백이 있으므로 전체 구간의 연속 가용성은 입증하지 않는다. [초기 표본](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/continuity-progress.json)과 [observer 한계](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/observer-limitations.json)에 기록했다.

같은 장애의 후속 관찰에서는 종료한 EC2와 다른 같은-AZ EC2가 동일 PVC UID·PV·EBS를 연결했다. AWS attachment와 CSI VolumeAttachment도 새 worker를 가리켰고 나머지 두 broker Pod UID는 그대로였다. Broker 3개의 Ready와 quorum을 확인한 시점은 fault 요청 후 472.829초다. 이 값은 관찰 공백을 포함한 상한이며 정확한 RTO가 아니다. 마지막 명령은 복구한 broker에서 `sessionPresent=true`, 오프라인 QoS 1 payload hash 세 개와 retained hash를 재발행 없이 확인했다. 이 수신 구간의 QoS 1 중복 관찰값은 0개다. [물리 복구 후속 결과](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/physical-recovery-followup.json)와 [복구 후 MQTT 결과](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/mqtt-after-recovery.json)를 따른다. 살아 있는 quorum의 복제를 사용하므로 EBS만을 원본으로 한 독립 복구, 전체 voter 전원 손실, 자연 발생 WAL torn-tail, volume loss/reseed 또는 production SLO의 수용으로 확대하지 않는다.

시험환경·run 전용 snapshot·image는 삭제했고 보호 snapshot 3개·공유 image는 유지했다([정리 확인](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/direct-cleanup-verification.json), [최종 기록](../../.artifacts/verification/eks-ec2-abrupt-20261015T060452Z-44b24a/final-summary.json)). 관찰 뒤 강제 정리는 복구 증거가 아니다. 전체 local은 미실행이다.

## 4. EKS worker observer 보완 기준 실행 기록

스펙의 [EKS worker observer 보완 기준](../specs/drain-and-rolling-update.md#eks-worker-observer-보완-기준)에서 옮긴 이력과 하위 절이다. 해당 절의 판정 기준은 스펙에만 있다.

### 보완 기준 소개 문단과 Cycle 1 확인 이력

검증 도구의 범위는 합성 fixture 관찰과 operator fencing이다. 제품 복구 정책·API 예산·drain gate는 바꾸지 않았다. Cycle 1 fault 전 세 node는 약 0.03초에 `OK`였다([결과](../../.artifacts/verification/eks-unknown-retest-20261015T135242Z/fault/cycle-1/eks-unknown-retest-20261015T135242Z-2b7e10/summary.json)). 실제 장애 재시험과 한계는 아래 절을 따른다.

### 구현·시험·EKS 재시험 하위 절

#### 구현과 로컬 검증 (2026-10-15)

진입점은 [main.py](../../tests/chaos/eks-worker-recovery/main.py)의 `main`이다. `monitor`는 읽기 전용 관리 조회와 합성 MQTT QoS 1 publish를 실행한다. `IndependentMonitor`는 [observer.py](../../tests/chaos/eks-worker-recovery/observer.py)에서 두 작업의 JSONL 표본·오류·종료를 관리한다. 예를 들어 API 응답이 6초 걸려도 MQTT 작업은 기다리지 않고 PUBACK을 기록한다. 표본 사이의 가용성은 추정하지 않는다.

`fault`는 명시한 `--execute`와 별도 plan이 있어야 실행한다. `preflight`에서 전용 namespace·account·cluster·Pod/Node/EC2·PVC/PV/EBS·같은-AZ 여분 worker·완료 snapshot을 확인한다. 이후 `observe_fault`가 단일 follower EC2를 종료하고 `fence_node`의 fresh 관찰 뒤 `release_fenced_pod`로 기존 대상 Pod를 한 번 삭제한다. 결과는 `pod-release-observation.json`과 summary의 `recovery.podRelease`에 남는다. 이후 대체 Pod·volume attachment를 읽고, `recovery-observations.json`의 각 표본에 Pod 삭제 시각, 기존 Node의 존재·taint·Ready, VolumeAttachment와 EBS attachment 상태를 기록한다. Persistent session·오프라인 payload·retained oracle은 별도 시나리오다. 명령의 PASS를 그 시나리오나 전체 DEP gate의 PASS로 쓰지 않는다. 준비와 실행 방법은 [개발 가이드](../how-to/development-guide.md#eks-worker-observer-실행)를 따른다.

로컬 회귀 파일은 [test_eks_worker_recovery.py](../../tests/test_eks_worker_recovery.py)다. `python -B -W error::ResourceWarning tests/test_eks_worker_recovery.py`로 48개 시험을 통과했다(2026-10-15, 기존 Pod 해제 시험 9개 추가). CI에도 별도 단계로 연결했다. `go run ./tools/verify local`은 이 Python 시험을 실행하지 않는다.

실제 표면 검증은 다음 범위였다. 회귀별 연결은 위 테스트 소스에만 둔다.

- 실제 3-voter broker 앞의 6초 API 지연 중 MQTT는 독립 진행했다(API 2개·MQTT 6개 성공, 첫 API 대기 중 PUBACK 3개).
- 11초 API 지연은 client 10초 timeout 1개와 MQTT 성공 6개를 보존하고 exit 1·FAIL이었다. 기록·종료 실패도 PASS로 처리하지 않았다.
- 실제 Kind API에서 EC2 running 입력의 mutation 0, UID/provider 변경·Node 삭제·CAS·응답 유실의 fresh 조회, taint 보존과 NoExecute 경계를 확인했다. Patch는 1회뿐이었다.
- Kind API의 기존 Pod 해제는 종료·fencing·Node identity를 요구했다. UID 조건 1회 삭제, 교체 Pod 보존, 거절·미확인 삭제의 실패 판정과 대상 VolumeAttachment 선택을 확인했다. EC2·EBS 응답은 통제된 입력이었다.

기존 Pod 해제의 실제 API 확인은 [Kind 기록](../../.artifacts/verification/pod-release-local-20261015T1625Z/kind-release-proof.json)을 따른다. Kind에는 attach-detach controller가 EBS를 붙이는 경로가 없다. 따라서 이 기록은 삭제 조건만 확인하며 detach 단축의 증거가 아니다. Kind cluster는 시험 뒤 삭제했다.

기록은 [최종 결과](../../.artifacts/verification/worker-observer-local-20261015T071513Z/final-summary.json), [6초 지연](../../.artifacts/verification/worker-observer-local-20261015T071513Z/delayed-api-proof.json), [10초 deadline 실패 보존](../../.artifacts/verification/worker-observer-local-20261015T071513Z/deadline-api-proof.json), [Kubernetes 7개 경계](../../.artifacts/verification/worker-observer-local-20261015T071513Z/kubernetes-fencing-proof.json)를 따른다. Python 3.14·Paho MQTT 2.1.0, Docker의 실제 broker 3개와 Kind Kubernetes 1.31.2를 사용했다. Broker는 기존 `9c96670` 합성 image를 사용했다. 로컬 broker의 준비 상태는 시험용 GET으로 확인했다. 이를 EKS startup probe 증거로 쓰지 않는다.

EC2 상태는 통제된 입력이므로 AWS 종료·EBS 재연결 증거가 아니다. 임시 broker·Kind 자원은 삭제했고 AWS 자원은 만들지 않았다. 전체 Go local·원격 CI는 미실행이며 기존 실패는 유지한다.

#### 생존 Pod의 관찰 경로 고정 (2026-10-15)

기존 fault observer는 외부에서 연 두 port의 대상 identity를 확인하지 않았다. Cluster summary만으로는 요청을 받은 Pod를 특정할 수 없다. 대상 worker로 잘못 연결하면 fault와 함께 관찰도 끊길 위험이 있다.

`main`은 fault plan의 `observerPod`·`observerPodUID`·`observerWorkerName`·`observerInstanceId`로 `SurvivorTunnel`을 시작한다. 이 class는 실제 Pod·Node·provider와 EC2 running 상태를 확인한 뒤 kubectl을 직접 실행한다. `23880:8080`과 `23883:1883`을 지정 Pod 하나에 연결하고 두 port의 준비 뒤 identity를 다시 읽는다. Fault 직전에도 identity·process를 확인한다. `observer-endpoint.json`과 summary에 확인한 identity를 기록한다. 각 관찰 작업은 process 종료를 실패로 기록한다. 관찰과 MQTT 종료 뒤 소유한 tunnel·출력 thread를 닫는다. `monitor` mode의 외부 tunnel은 이 검증 범위가 아니다.

생존 Pod UID가 준비 중 바뀌면 tunnel을 닫고 fault를 주입하지 않는 안전 경계를 확인했다.

| 기준 | 시험·확인 방법 | 결과 |
| --- | --- | --- |
| 대상 Pod·worker·instance 관찰 거절 | `SurvivorTests.test_target_pod_worker_or_instance_cannot_be_used_as_survivor` | PASS. 세 identity의 충돌을 각각 거절 |
| 교체·NotReady·삭제 중·종료 instance 거절 | `SurvivorTests.test_changed_identity_notready_or_terminating_survivor_is_rejected` | PASS. Pod UID·worker·provider 불일치와 readiness·deletion·running 경계 확인 |
| Fault CLI의 거절 기록 | `SurvivorTests.test_fault_cli_persists_refusal_before_mqtt_or_aws_mutation` | PASS. exit 1·summary FAIL, MQTT 시작·AWS mutation 없음 |
| 실제 두 port의 정확한 Pod 연결 | Kind 1.31.2·kubectl 1.34.1, 실제 Python HTTP/TCP Pod와 `SurvivorTunnel.start` | PASS. 두 port에서 같은 합성 Pod 표식을 확인하고 UID 기록 |
| 기존 port를 재사용하지 않음 | Host의 23880을 점유한 뒤 실제 kubectl 실행 | PASS. 준비 실패 뒤 소유 process·thread 종료 |
| 연결 준비 중 UID 교체 | 두 번째 identity GET 전에 실제 Pod 삭제·재생성 | PASS. 다른 UID를 거절하고 tunnel 종료 |
| Process 종료 뒤 자동 재연결 없음 | 실제 tunnel process 종료 뒤 `check` | PASS. 오류 반환, 같은 종료 process 유지 |
| 유휴 tunnel의 Pod 삭제 감지 한계 | 실제 Pod 삭제 뒤 kubectl 종료를 25초 기다림 | **예상한 process 종료 대기 실패를 보존**. 유휴 process가 남았다. Process 생존은 Pod 생존의 증거가 아니며 즉시 Pod 손실 감지를 보장하지 않는다. Fault 직전 fresh identity 검사는 별도로 유지한다. |

`ef6e561` 이후 미커밋 observer 변경에서 `python -B tests/test_eks_worker_recovery.py -v` 20개 PASS. [실제 runtime](../../.artifacts/verification/survivor-tunnel-20261015T085807Z/runtime-proof.json)과 [최종 기록](../../.artifacts/verification/survivor-tunnel-20261015T085807Z/final-summary.json)을 따른다. Endpoint는 합성 HTTP/TCP, EC2 running은 통제된 입력이므로 AWS/EKS 장애 수용은 아니다. 임시 Kind·tunnel은 삭제했고 전체 Go local·원격 CI는 미실행이다.

#### 생존 Pod 고정 observer의 EKS 재시험 (2026-10-15)

결과는 **단일 follower EC2 비정상 종료에 한정한 통과**다. 상태는 `PASS_SINGLE_FOLLOWER_ABRUPT_EC2_OBSERVER_AND_DATA_RECOVERY_LIMITED`다. [결과 기록](../../.artifacts/verification/eks-observer-acceptance-20261015T091838Z-617088/acceptance-result.json)과 [최종 기록](../../.artifacts/verification/eks-observer-acceptance-20261015T091838Z-617088/final-summary.json)을 따른다. 기존 `CLU-14 FAILED/UNRESOLVED`, 과거 최초 observer 실패, 전체 EC2·AZ 장애 matrix와 production 수용은 바꾸지 않는다.

대상 HEAD는 `ef6e561`이고 observer·시험·문서의 미커밋 변경을 포함했다. 제품 코드는 `9c96670`과 같음을 확인했다. 이전 합성 ARM64 broker image를 작업 전용 tag로 다시 게시했고 공유 image는 바꾸지 않았다. 별도 local state·새 VPC의 EKS 1.37, 서로 다른 AZ의 `m7g.large` worker 3대와 새 gp3 PVC 3개를 사용했다. Cluster ID는 run ID다. Startup은 수동 readiness GET 없이 Kubernetes probe로 통과했다. 도구는 Python 3.14, Paho MQTT 2.1.0, kubectl 1.37.1이다. kubectl은 공식 SHA-256과 일치했다.

준비 단계에서 leader는 node 3이었다. 대상은 follower `narumq-1`(node 2, AZ b), 관찰 경로는 다른 worker·instance의 `narumq-0`(AZ a)이다. 대상과 같은 AZ의 Ready worker 한 대를 추가하고, run 전용 snapshot이 `completed`임을 확인했다. 대상 data volume은 암호화돼 있고 `DeleteOnTermination=false`였다. 서로 다른 두 생존 Pod에서 pre-fault drain 기록 조회가 같은 404를 반환했다([조회 기록](../../.artifacts/verification/eks-observer-acceptance-20261015T091838Z-617088/drain-record-read-path.json)). 장애 전 persistent session, retained marker 1개와 오프라인 QoS 1 메시지 3개의 PUBACK 4개를 확인했다. 모두 Reason Code 0이었다.

실행 순서는 다음과 같다. `<private-directory>`와 plan에 필요한 값은 [개발 가이드](../how-to/development-guide.md#eks-worker-observer-실행)를 따른다.

```text
<venv-python> -B tests/chaos/eks-worker-recovery/payload_oracle.py prepare --private <private-directory> --run-id <run-id> --output <result-directory> --target-port <대상 Pod tunnel port> --publisher-port <생존 Pod tunnel port>
<venv-python> -B tests/chaos/eks-worker-recovery/main.py fault --private <private-directory> --output <run-directory>/<run-id> --plan <fault-plan.json> --execute
<venv-python> -B tests/chaos/eks-worker-recovery/payload_oracle.py verify --private <private-directory> --run-id <run-id> --output <result-directory> --target-port <복구된 대상 Pod tunnel port>
```

`prepare`의 tunnel은 `fault` 시작 전에 닫아야 한다. `fault`가 `23880`·`23883`을 직접 소유하기 때문이다.

| 관찰 항목 | 실제 결과 |
| --- | --- |
| Fault 요청·종료 확인·fencing | `SkipOsShutdown=true` 요청 후 17.1초에 `terminated`, 약 19.0초에 out-of-service `NoExecute` taint 적용. Patch 실패와 CAS 거절은 없었다. 기존 Node는 `NodeNotReady` 뒤 cloud controller가 2초 안에 삭제했다. 수동 Pod 삭제와 EBS force detach는 사용하지 않았다. |
| API·MQTT 관찰 | 도구가 소유한 `narumq-0` tunnel에서 관리 API 203개·MQTT 238개 표본이 모두 성공했다. Fault 요청 뒤 표본은 각각 200개·235개다. 표본 시작 시각 기준 최대 간격은 관리 API 7.065초, MQTT 2.056초다. 관리 API 표본 13개(11:39:02Z~11:40:32Z)는 각각 약 5.03초 걸렸고 모두 `quorumAvailable=true`를 반환했다. 최대 소요는 5.061초다. 이전 표본 종료부터 다음 표본 시작까지의 idle gap은 관리 API 최대 2.016초다. 두 값은 다르다. 종료 때 worker와 journal을 닫았다. |
| 대체 Pod와 volume | 대체 Pod는 fault 후 95.6초 관측 표본에 나타났다. 같은 AZ의 다른 worker에서 같은 PVC·PV·EBS·VolumeAttachment를 사용했다. 나머지 두 broker Pod UID는 바뀌지 않았다. Broker 3개 Ready와 quorum은 fault 후 471.06초에 관찰했다. |
| 데이터 oracle | 복구한 broker에서 `sessionPresent=true`, 오프라인 QoS 1 payload hash 3개를 발행 없이 받았다. 중복은 0개, retained hash는 같았다. |

471.06초는 표본으로 관찰한 상한이며 정확한 RTO나 production SLO가 아니다. 관리 API 표본은 장애 구간에서 약 7초 간격까지 벌어졌다. 이전 후속 관찰의 상한 472.829초와 비슷하다. 대체 Pod가 생성된 뒤 `FailedAttachVolume`(`waiting for detach`)이 11:40:35Z부터 11:46:40Z까지 약 365초 이어졌다. 이 대기가 복구 시간의 대부분이다. 이 시험은 제품 코드나 Kubernetes 설정을 바꿔 그 시간을 줄이지 않았다.

세 EKS 실행의 event를 비교하면 detach 대기는 모두 6분 3~5초였다. Out-of-service taint 적용이 실패한 실행과 성공한 두 실행의 대기가 같았다. Node 삭제 시각이 기록된 두 실행에서 cloud controller는 Node가 NotReady가 된 뒤 1~2초 안에 Node를 삭제했고, 대체 Pod는 Node 삭제 45~48초 뒤 생성됐다. Kubernetes `v1.37.0` source에서 PodGC는 NotReady이면서 taint가 있는 Node의 종료 중 Pod만 강제 삭제한다(20초 주기). Attach-detach controller는 Pod object가 삭제되거나 종료 상태가 보고돼야 volume을 detach 대상으로 본다. 그 뒤 Node object에 taint가 있으면 바로 detach하고, 그렇지 않으면 `maxWaitForUnmountDuration`(기본 6분) 뒤 force detach한다. [INFERENCE] EC2 terminate에서는 Node 삭제와 함께 taint도 사라진다. 그래서 taint가 detach를 앞당기지 못하고 6분 기본 대기로 복구된다. EKS patch 차이, controller-manager 로그와 실제 설정값은 확인하지 않았다. 이 분석에 따라 관찰 도구는 taint 확인 직후 기존 Pod를 UID 조건으로 삭제하도록 바뀌었다([EKS worker observer 보완 기준](../specs/drain-and-rolling-update.md#eks-worker-observer-보완-기준)). 효과는 [기존 Pod 해제의 EKS 재시험](#기존-pod-해제의-eks-재시험-2026-10-15) 1회에서 확인했다. 근거는 [EBS detach 대기 분석](../../.artifacts/analysis/ebs-detach-wait-analysis.md)에 있다.

함께 보존할 결과와 한계는 다음과 같다.

- 이번 장애는 follower 하나이며 생존 voter 둘이 quorum을 유지했다. EBS 데이터도 quorum 복제와 함께 쓰이므로 EBS만으로 독립 복구했다는 증거가 아니다. 전체 voter 전원 손실, volume loss/reseed, 단일 AZ 손실, 자연 발생 WAL torn-tail, production 최대 부하와 SLO는 시험하지 않았다.
- 표본이 모두 성공해도 표본 사이의 연속 가용성은 입증하지 않는다. 관리 API의 7초 간격 동안과 모든 표본 사이의 상태는 알 수 없다. `kubectl port-forward` process 생존은 Pod 생존의 증거가 아니다. 이번 결과는 한 Pod의 tunnel에서 관찰한 표본이다.
- EKS cycle 1에서 healthy leader node 2가 `node_query_timeout`으로 `unknown`이 된 구간은 표본 경계로 23.1~36.2초였다. Leader·quorum·MQTT·oracle은 정상이었다. 당시 fault 중 DNS는 관측하지 않아 원인을 확정하지 않았다. 두 CoreDNS replica는 종료 worker에 있었다([node 표본](../../.artifacts/verification/eks-unknown-retest-20261015T135242Z/fault/cycle-1/eks-unknown-retest-20261015T135242Z-2b7e10/nodes-samples.jsonl), [CoreDNS timeline](../../.artifacts/verification/eks-unknown-retest-20261015T135242Z/fault/cycle-1/post-fault/cycle1-coredns-timeline.json)). Fixture는 삭제했고 두 번째 fault cycle은 미실행이었다.
- 후속 DNS 동시 측정으로 원인을 확인하고 사용자가 계약 동작으로 수용했다. 중간 로컬 가설·재현 시도 대신 [CoreDNS EKS 재현](#coredns-배치와-healthy-node-unknown의-eks-재현-2026-10-16)에 결론과 증거를 둔다.
- EKS 실행은 이전 artifact의 oracle 사본을 사용했다. 저장소의 [payload_oracle.py](../../tests/chaos/eks-worker-recovery/payload_oracle.py)는 같은 검증 논리에 `--run-id`·`--output` 인자를 붙여 옮긴 것이다. 실제 로컬 Docker 3-voter broker에서 `prepare`와 `verify`를 모두 통과했다([로컬 검증](../../.artifacts/verification/eks-observer-acceptance-20261015T091838Z-617088/tracked-oracle-local-proof.json)). 이 로컬 검증은 EC2 장애나 broker 재시작을 포함하지 않고 broker가 `joining` 요약 상태일 때 실행했다. 따라서 이 파일 자체를 EKS 장애 복구의 증거로 쓰지 않는다.

시험환경·run 전용 snapshot·image는 삭제했고 보호 snapshot 3개·공유 image는 유지했다([정리 확인](../../.artifacts/verification/eks-observer-acceptance-20261015T091838Z-617088/direct-cleanup-verification.json)). 관찰 뒤 강제 정리는 복구 증거가 아니다. 전체 Go local·원격 CI는 미실행이다.

#### 기존 Pod 해제의 EKS 재시험 (2026-10-15)

결과는 **1회 실행에서 6분 detach 대기가 사라졌다**는 것이다. 대체 Pod의 `FailedAttachVolume`은 0건이었고, 기존 Pod 삭제 요청 약 5.5초 뒤 같은 EBS가 여분 worker에 attach됐다. Broker 3개 Ready 관찰은 fault 요청 뒤 193.995초다. 이전 세 실행의 471~473초보다 짧다. Tool 결과와 데이터 oracle은 모두 PASS다. [최종 기록](../../.artifacts/verification/eks-detach-release-20261015T162550Z-23b31b/final-summary.json)과 [tool summary](../../.artifacts/verification/eks-detach-release-20261015T162550Z-23b31b/fault/eks-detach-release-20261015T162550Z-23b31b/summary.json)를 따른다.

Fixture는 이전과 같은 Terraform root(EKS `v1.37.0-eks-b9510e3`, 새 VPC, `m7g.large` worker 3대)와 같은 broker image digest `sha256:140a99b2…`를 사용했다. Image는 작업 전용 tag로 다시 게시했다. Leader는 `narumq-1`(node 2, AZ a)이었다. 대상은 follower `narumq-2`(node 3, AZ b), 관찰 경로는 `narumq-0`(AZ c)이다. 대상과 같은 AZ b에 여분 worker 한 대를 추가했다. 두 CoreDNS replica는 대상이 아닌 worker에 있었다. EBS CSI controller replica 하나가 대상 worker에 있었고 attacher·provisioner lease를 갖고 있었다. Attach 시간에 controller 장애 조치가 섞이지 않도록 대상 worker를 잠시 cordon한 뒤 그 replica를 삭제했다. 새 replica가 Ready가 되고 lease가 대상이 아닌 replica로 옮겨진 뒤 uncordon했다([배치 기록](../../.artifacts/verification/eks-detach-release-20261015T162550Z-23b31b/pre-fault-placement.json)). 이 조정은 fault 전 작업이다.

| 시각(UTC) | 관찰 |
| --- | --- |
| 16:48:23.762 | Fault 요청(`SkipOsShutdown`) |
| 16:48:43.548 | EC2 `terminated` 확인 |
| 16:48:47.014 | Out-of-service taint 적용(`out_of_service`) |
| 16:48:51 | `TaintManagerEviction` event |
| 16:48:54.458 | 기존 Pod UID 조건 grace 0 삭제 요청. 2.1초 뒤 fresh GET에서 새 UID Pod 확인(`deleted`) |
| 16:48:57 | 대체 Pod가 여분 worker에 scheduling |
| 16:48:58.899 | 첫 복구 표본. 기존 Node는 남아 있고 taint가 있었다. VolumeAttachment는 여분 worker의 것 1개뿐이고 `attached=true`였다. EBS attach 시각은 16:48:59 |
| 16:49:00 | `SuccessfulAttachVolume` event |
| 16:49:02~03 | Container 시작 |
| 16:49:21.372 | 기존 Node가 없는 첫 표본 |
| 16:49:13~16:51:23 | Startup probe 503 14회 |
| 16:51:31.344 | 관리 API 표본에서 `ready 3` 확인(+187.6초) |

Fault 요청 뒤 관리 API 85개·MQTT 99개 표본이 모두 성공했다. 표본 시작 간격은 최대 7.042초·2.054초다. 데이터 oracle은 복구한 broker에서 `sessionPresent=true`, 오프라인 QoS 1 payload 3개와 retained를 재발행 없이 확인했다. 첫 표본 전에 기존 VolumeAttachment가 사라졌으므로 정확한 제거 시각은 16:48:54.458과 16:48:58.899 사이로만 안다. 이 결과는 가설과 맞는다. 다만 한 번의 실행이며 controller-manager 로그는 읽지 않았다.

새로 드러난 대기는 attach 뒤 약 130초의 startup probe 실패다. 대체 node 3은 16:50:02Z부터 peer 1·2를 `connected=false`, `outbound queue full`로 기록했다. Leader `narumq-1`은 16:51:14Z부터 peer 3을 `connected=true`, `outbound queue full`로, 16:51:18Z부터 `peer write failed`로 기록했다. 16:51:23.161Z에 `connected=false`가 됐고, node 3은 16:51:23.172Z에 release profile 동기화를 기록했다. [INFERENCE] Leader가 종료된 이전 Pod 주소로 맺은 peer 연결을 쓰기 실패가 날 때까지 유지했고, 그동안 대체 node가 leader 메시지를 받지 못했다. Socket 상태, node 3의 송신이 연결되지 않은 이유와 transport의 write deadline은 확인하지 않았다. 이전 실행에서는 attach가 6분 늦어 이 대기가 가려졌을 수 있다 [INFERENCE]. 이 항목은 TODO §1.4에서 추적한다. [Broker log](../../.artifacts/verification/eks-detach-release-20261015T162550Z-23b31b/fault/post-fault/logs/narumq-1.log)를 따른다.

후속 로컬 재현에서 같은 순서를 확인했다. 연결 방향상 leader만 node 3을 dial한다. Dynamic peer stream은 Go 기본 keepalive만 쓰고 감지 시간을 제한하는 설정이 없었다. 이전 IP를 응답 없이 버리자 leader는 응용 송신 대기열이 차고 write deadline이 실패할 때까지 연결을 유지했다. [ADR-0059](../decisions/ADR-0059-dynamic-peer-tcp-liveness.md)의 TCP 생존 확인을 적용하자 leader의 연결 종료가 fault 뒤 49.5초에서 12.7초 이내로 당겨졌다. 대체 node 준비 시간은 28.74초에서 시작 직후로 줄었다([Cluster §11.34](../specs/cluster-session-and-delivery.md#1134-dynamic-peer-tcp-생존-확인-수용-매핑)). 2026-10-16 EKS 재측정 결과는 [Cluster §11.34 EKS 무응답 종료 재측정](cluster-session-and-delivery.md#eks-무응답-종료-재측정-2026-10-16)에 있다.

시험환경·run 전용 snapshot·image는 삭제했고 보호 snapshot 3개·공유 image는 유지했다([정리 확인](../../.artifacts/verification/eks-detach-release-20261015T162550Z-23b31b/direct-cleanup-verification.json)). 전체 Go local·원격 CI는 미실행이며 기존 실패는 유지한다.

#### CoreDNS 배치와 healthy node `unknown`의 EKS 재현 (2026-10-16)

결과는 **원인 확인**이다. Worker 하나를 잃으면 그 worker의 CoreDNS endpoint로 간 DNS 조회가 timeout된다. 관측 RPC는 query마다 peer의 DNS 이름으로 새로 연결한다(`internal/adapters/tcp/peer_observability_transport.go`). 그래서 DNS 조회가 5초 query 예산을 다 쓰면 healthy node도 `UNREACHABLE/node_query_timeout`이 된다. CoreDNS를 서로 다른 worker·AZ에 두는 배포 전제는 이 구간을 줄인다. 그러나 없애지는 못한다. 2026-10-16 사용자는 이 동작을 계약 동작으로 수용했다. 실패하거나 timeout된 query는 [peer observability §3](../contracts/peer-observability-v1.md#3-한도와-시간)과 [Management API v1](../contracts/management-api-v1.md)에 따라 `UNREACHABLE`·`unknown`으로 보고한다.

같은 fixture에서 두 번 실행했다. 실행 방법·image·socket 결과는 [Cluster §11.34 EKS 무응답 종료 재측정](cluster-session-and-delivery.md#eks-무응답-종료-재측정-2026-10-16)과 같다. 관찰 도구가 연결한 node 1의 worker에 DNS probe Pod를 두었다. 이 Pod는 node 2·3의 headless DNS 이름을 UDP로 0.5초마다(제한 3초), `getaddrinfo`로 1초마다 조회했다([probe](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/scripts/dns_probe.py)). CoreDNS Pod와 EndpointSlice 상태는 1초마다 기록했다.

시각은 fault 요청 뒤의 초다.

| 관찰 | 재현 실행 | 전제 실행 |
| --- | --- | --- |
| CoreDNS 배치 | 두 replica 모두 대상 worker | Terraform 전제 적용. 대상 worker(AZ c)와 leader worker(AZ b)에 하나씩 |
| Observer worker의 UDP DNS timeout | 600회 중 170회. 6.22~48.22초. 10~45초에는 모두 실패 | 508회 중 54회. 5.39~27.89초 |
| 죽은 CoreDNS endpoint가 routing에서 빠짐 | 새 replica 준비 42.7초·45.7초 | 23.8초에 not ready |
| Healthy leader node 2의 node 표본 | 3.2~39.4초에 시작한 5개 표본 모두 `node_query_timeout`. 48.5초에 `OK` | 12개 표본 중 22.3초에 시작한 1개만 `node_query_timeout` |
| Cluster 요약 `unknown=2` | 3.3~45.6초 | 2.3초·9.3초·16.4초 표본. 23.4초부터 `unknown=1` |
| Leader·quorum·MQTT·oracle | leader 2 유지, 표본 모두 성공, PASS | leader 2 유지, 표본 모두 성공, PASS |

재현 실행은 2026-10-15 cycle 1과 같은 모습이다. DNS가 모두 실패한 구간과 node 2의 timeout 구간이 겹친다. DNS가 돌아온 직후 node 2도 `OK`가 됐다. 전제 실행에서는 살아 있는 replica가 응답했다. 그러나 죽은 replica의 endpoint가 23.8초까지 routing에 남아 일부 조회가 timeout됐다. 그동안 cluster 요약과 node 표본이 간헐적으로 healthy node를 미확인으로 보였다.

운영자는 worker 하나를 잃은 뒤 수십 초 동안 healthy node가 `unknown`으로 보일 수 있다고 예상해야 한다. 이 구간의 `unknown` 하나만으로 그 node가 죽었다고 판단하지 않는다. Leader·quorum 상태를 함께 보고, 다음 표본에서 다시 확인한다. 배치 전제는 [EKS architecture의 배치 제약](../architectures/eks-ec2-deployment.md#placement와-availability-constraint)을 따른다.

한계는 다음과 같다. 실행은 배치마다 한 번이다. 죽은 endpoint가 빠지는 시각은 Node 상태와 fencing 시각에 따라 달라진다. 이번 두 실행은 관찰 도구의 fencing과 기존 Pod 해제를 포함한다. Fencing이 없으면 이 구간이 더 길 수 있다 [INFERENCE]. NodeLocal DNSCache 같은 다른 DNS 구성은 시험하지 않았다. [결과](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/result.json), [분석](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/analysis.json), [재현 실행 DNS 표본](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/cycle-b/post-fault/dns-probe.jsonl), [전제 실행 DNS 표본](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/cycle-a/post-fault/dns-probe.jsonl), [CoreDNS 배치 기록](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/cycle-a-coredns-placement.json)을 따른다.

시험환경·run 전용 snapshot·image는 삭제했고 보호 snapshot 3개·공유 image는 유지했다([정리 확인](../../.artifacts/verification/eks-adr59-dns-20261016T053249Z-f3b329/direct-cleanup-verification.json)). 증거의 secret 부재를 확인하고 private 입력은 DPAPI로 암호화했다. 기존 `CLU-14 FAILED/UNRESOLVED`와 전체 local 실패는 유지한다.

## 5. EKS 장애 matrix 실행 기록 (2026-10-16)

스펙의 [EKS 장애 matrix 보완 기준](../specs/drain-and-rolling-update.md#eks-장애-matrix-보완-기준-2026-10-16)에서 옮긴 실행 이력이다. 판정 기준은 스펙에만 있다.

### 공통 durable stream oracle의 첫 실행 이력 (DRN-SCN-036, DRN-SCN-037, DRN-SCN-038)

2026-10-16 첫 EKS 실행은 이 상한 없이 0.2초 간격으로 보냈다. D3 subscriber session은 PUBACK을 받은 1,515개 중 seq 65 이후를 받지 못했다. 이는 위 session당 64개 한도와 일치하며 oracle 설계 오류로 판정한다. 그 실행의 RPO 판정은 쓰지 않는다. 증거는 `.artifacts/verification/eks-matrix-20261016T065350Z-24adc2/storage/`에 보존한다.

### DEP-20 단일 AZ 전원 중단 모델의 첫 실행 이력 (DRN-SCN-037)

2026-10-16 첫 실행(`HealthCheck` 미중단, 사후 관찰 없음)은 위 중단·회복 조건을 모두 통과했다. 그러나 도구가 process를 다시 켠 6초 뒤 ASG가 다시 시작한 instance를 "EC2 health check가 stopped를 보고함" 이유로 종료하고 새 instance를 띄웠다. Data volume은 같은 PVC로 새 instance에 붙었다. 이 교체는 도구 판정 뒤에 일어나 판정에 들어가지 않았다. 그래서 `HealthCheck` 중단과 사후 관찰 조건을 추가했다.

### DEP-21 EBS disk pressure·ENOSPC의 두 번째 실행 이력 (DRN-SCN-038)

2026-10-16 두 번째 실행(storage-2)은 8개 topic(1 MiB)을 썼다. D2 관찰 tunnel이 끊겨 retained 정리를 하지 못했다. 이 상태가 다음 CLU-14 재시드에서 1 MiB를 넘는 snapshot(1,055,832 byte)의 대부분을 차지한 것으로 본다 [INFERENCE].


## 6. Gate 연결과 release profile 경로 보완 (2026-10-17)

대상은 `b13d8254754bdb5333ae28d47e25c9d51cd85590` 기반 작업 트리다. 제품 drain 정책이나 수용 수치는 바꾸지 않고, 미연결 시나리오의 실제 실행 경로와 로컬 회귀를 추가했다.

| 범위 | 실행 경로 | 이번 수용 상태 |
| --- | --- | --- |
| `DRN-SCN-019` | `tests/chaos/eks-rolling-capacity/main.py` | 미실행. 실제 측정한 `B_*`, 같은 profile hash의 절대 cutoff, 전용 EKS·generator image가 필요하다. 100회를 줄이거나 임의 cutoff를 만들지 않는다. |
| `DRN-SCN-020` | `tests/chaos/eks-release-window/main.py` | 미실행. 게시 N-1/N image digest와 release record가 필요하다. 합성 `0.0.x`는 거절한다. |
| `DRN-SCN-022` | `tests/chaos/eks-rollout-faults/main.py` | 미실행. 합성 `0.0.0` fixture, fault proxy와 controller가 필요하다. 진행 중 재시작 뒤 같은 operation·key·barrier audit를 판정한다. |
| `DRN-SCN-030`·`031`·`033` | `tests/chaos/eks-prestop-faults/main.py`, `eks-nlb-drain/main.py`, `eks-pod-recreate/main.py` | 미실행. 격리 EKS가 필요하다. 실제 hook·admission, readiness·NLB 전파, 같은 PVC/EBS의 재생성을 각각 실행한다. |
| `DRN-SCN-032` | `deploy/release/deployment-profiles-v1.json`, `tools/release-profile` | 로컬 통과. 실제 descriptor와 EKS manifest를 대조하고, 지원하지 않는 profile의 HA·RPO·rolling 근거를 제외했다. 게시 release 전체 수용은 아니다. |

`go run ./tools/release-profile validate -descriptor deploy/release/deployment-profiles-v1.json`은 `EKS_EC2_HA=Supported`, `ECS_EC2=Experimental`, `EKS_FARGATE`·`COMPOSE_LOCAL=Unsupported`를 출력하고 exit 0으로 끝났다. `go test ./tools/release-profile -count=1`도 통과했다. [실행 증거](../../.artifacts/verification/gate-completion-20261017/release-profile-validate.json)를 따른다.

`deploy/kubernetes/eks-ec2-ha-admission`은 별도 opt-in package다. 기본 StatefulSet에 자동 설치하지 않았다. `kubectl kustomize deploy/kubernetes/eks-ec2-ha-admission`은 정책과 binding을 정상 렌더링했다. 실제 EKS에서의 전체 hook·admission 수용은 미실행이다.

CI와 같은 Python 3.9 Linux 환경의 첫 로컬 실행은 660개 중 timestamp 판정 하나가 실패했다. Python 3.9의 ISO parser가 `.5`처럼 한 자리 소수 초를 받지 못한 것이 원인이다. RFC 3339를 명시적으로 해석하고 nanosecond를 보존하도록 고쳤다. SAFE_TO_STOP보다 1ns 이른 승인은 거절한다. 수정 후 같은 660개가 건너뜀 없이 통과했다. [실패 기록](../../.artifacts/verification/gate-completion-20261017/python-linux-ci-unit.json)과 [후속 통과](../../.artifacts/verification/gate-completion-20261017/python-linux-ci-unit-fixed.json)를 함께 보존한다. 이는 하네스 판정의 검증이며 EKS 실행은 아니다.

Admission 검토에서는 annotation으로 실제 preStop보다 작은 timeout을 선언할 수 있는 결함을 찾았다. 이 override가 실제 hook을 바꾸지는 않으므로 600,000ms 미만을 거절하도록 고쳤다. 전용 Kind v1.34.0 API server에서 수정 전 정책은 `timeout=1ms, grace=61s`를 받아들였고 수정 후 정책은 거절했다. 기본 659초 거절·660초 허용, 긴 timeout의 660초 거절·1860초 허용, 잘못된 annotation 거절도 확인했다. 생성한 policy·binding·namespace는 이 smoke 뒤 삭제했다([API 원문](../../.artifacts/verification/gate-completion-20261017/admission-api-smoke.json)). 이는 실제 Kubernetes admission 증거이지만 EKS 전체 수용은 아니다.

소스 문자열·명령 복사만 확인하는 시험을 제거한 최종 Python 3.9 Linux suite는 659개 모두 통과했고 건너뜀은 0건이다([최종 실행](../../.artifacts/verification/gate-completion-20261017/python-linux-ci-final.json)).
