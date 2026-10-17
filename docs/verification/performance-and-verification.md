# Performance와 verification 검증 기록

이 문서는 [Performance와 verification 스펙](../specs/performance-and-verification.md)의 과거 실행 증거와 미해결 범위를 정리한다. 아래 통과 기록은 해당 실행의 결과이며 현재 코드의 통과를 뜻하지 않는다. 현재 기준·threshold는 [스펙 §8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate), 테스트 연결·현재 결과는 [테스트 컨벤션의 보고서](../conventions/test-conventions.md#5-검증-명령과-통과-기준)로 확인한다. 최근 통합 확인은 [§11](#11-시나리오-보고서-통합-확인-2026-10-17)에 있다.

| 기록 | 구 스펙 절 | 대상 시나리오 |
| --- | --- | --- |
| [Benchmark 공통 계약 기록](#3-benchmark-공통-계약-기록) | §3 | - |
| [BP-03 기록](#34-bp-03-durable-delivery-기록) | §3.4 | PRF-SCN-001~PRF-SCN-017 |
| [BP-07 기록](#35-bp-07-rolling-drain-기록) | §3.5 | PRF-SCN-018~PRF-SCN-026 |
| [BP-06 기록](#36-bp-06-failure-recovery-기록) | §3.6 | PRF-SCN-027~PRF-SCN-034 |
| [BP-04 기록](#37-bp-04-subscription-stress-기록) | §3.7 | PRF-SCN-035~PRF-SCN-039 |
| [BP-02 smoke 기록](#38-bp-02-production-mix-smoke-기록) | §3.8 | PRF-SCN-040~PRF-SCN-048 |
| [BP-02 EKS 용량 측정 기록](#39-bp-02-eks-용량-측정-기록) | §3.9 | PRF-SCN-049~PRF-SCN-064 |
| [SDK 호환성 기록](#71-mqtt-클라이언트-호환성-기록) | §7.1 | - |
| [Fuzz·traceability·gate 기록](#72-fuzz-게이트와-수동-mutation-기록) | §7.2 | PRF-SCN-065~PRF-SCN-071 |
| [Branch 측정 gate 기록](#82-branch-측정-gate-수용-증거) | §8.2 | PRF-SCN-077~PRF-SCN-086 |
| [Branch ratchet 허용폭 기록](#83-branch-ratchet-노이즈-허용폭-수용-증거) | §8.3 | PRF-SCN-087, PRF-SCN-088 |
| [전체 branch floor 기록](#84-전체-branch-floor-수용-증거) | §8.4 | PRF-SCN-089 |
| [diff floor 기록](#85-변경-코드-diff-floor-수용-증거) | §8.5 | PRF-SCN-090~PRF-SCN-096 |
| [간헐 실패 원인 수정 기록](#86-간헐-실패-원인-수정-수용-증거) | §8.6 | PRF-SCN-097~PRF-SCN-099 |
| [Mutation gate 범위 확장 기록](#87-mutation-gate-범위-확장-수용-증거) | §8.7 | PRF-SCN-100, PRF-SCN-101, PRF-SCN-103 |
| [수동 mutation 검증 기록](#88-수동-mutation-검증-수용-증거) | §8.8 | PRF-SCN-101, PRF-SCN-102, PRF-SCN-103 |
| [GitHub Actions 원격 CI 기록](#89-github-actions-원격-ci와-required-check-증거) | §8.9 | PRF-SCN-104~PRF-SCN-109 |

## 3. Benchmark 공통 계약 기록

구 §3 공통 계약의 Profile SHA-256 설명에 있던 기록이다. 대상 시나리오: -

2026-10-08 이전 BP-06 실행 artifact의 `profileSha256` `e8e4aa2e5df1a3e55ad901acff5a4505cdb1acf4699ff061f1a2bfc51754054d`는 같은 내용을 CRLF로 checkout한 bytes의 digest다.


## 3.4 BP-03 Durable Delivery 기록

대상 시나리오: PRF-SCN-001~PRF-SCN-017

Async WAL 보완 뒤 compare `bp03-20261002T193345.142241000Z-38956`와 suite `bp03-suite-20261002T193711.351279300Z-4364`는 양쪽 valid·5/5였다. 매회 QoS별 Step 3 offered/sent/ACK/delivery 600, offline QoS별 64, SessionPresent 4/4, gap·duplicate 0, overload 없음이었다. Step 3 ACK p99 median/MAD는 QoS 1 54.895/1.657ms, QoS 2 56.611/3.149ms, mean entries/sync는 2.053607/0.003409였다. 고정 local smoke의 반복 증거일 뿐 production SLO·target 우열·ADR-0003 전체 수용은 아니다. `releaseGating=false`, `ackLatencyDirectlyComparable=false`다.


## 3.5 BP-07 Rolling Drain 기록

대상 시나리오: PRF-SCN-018~PRF-SCN-026

최종 compare `bp07-20261005T031404.679399800Z-37844`에서 양쪽 workload·rolling·resource가 valid였고 QoS1/2 gap·duplicate, 계획 밖 종료, protocol error, overload는 0이었다. Suite `bp07-suite-20261005T032136.047989300Z-30100`는 교차 순서 5/5 valid였다. NaruMQ QoS1 duplicate는 첫 4회 각 1, 마지막 회 0이며 QoS0 loss 1–2건은 기록 전용이었다. EMQX QoS0 loss·QoS1 duplicate는 0이었다. NaruMQ v5-safe Will은 0, MQTT 3.1.1 server-close와 고유 Will은 매 회 1이었다. 12개 metric key 모두 5 sample을 가졌다. Round metric은 `roundNN_drain_millis`, `roundNN_restart_ready_millis`, `roundNN_reconnect_latency_max_millis`; aggregate는 `reconnects_total`, `qos1_duplicates_total`, `qos0_lost_total`이다. Drain contract가 달라 target 우열로 해석하지 않는다.


## 3.6 BP-06 Failure Recovery 기록

대상 시나리오: PRF-SCN-027~PRF-SCN-034

Suite `bp06-suite-20261006T163812.777212000Z-92296`는 5/5 valid였다. NaruMQ의 gap·QoS2 duplicate·계획 밖 disconnect·protocol error·overload는 매회 0이었고 QoS1 duplicate `0,0,0,1,0`은 기록 전용이다. EMQX crashDurability의 gap 21개와 QoS2 duplicate 251개는 지정 crash-window의 예외로 집계했다. ACKed gap은 주입 후 270ms 안에 있었다. 이 예외는 EMQX의 crash durability 동등성이나 guaranteed delivery를 뜻하지 않는다. `releaseGating=false`이며 production storage/release matrix와 CI-only check는 미완료다.


## 3.7 BP-04 Subscription Stress 기록

대상 시나리오: PRF-SCN-035~PRF-SCN-039

최종 compare `bp04-20261007T110131.450776700Z-190180`에서 두 target 모두 valid, setup 115, probe 12/12 성공, protocol/transport/unexpected delivery 0, resource/network valid, `equivalent=true`였다. Suite `bp04-suite-20261007T110511.057084500Z-196860`는 5/5 valid이며 모든 metric에 5 sample이 있었다. Production capacity나 release 합격 증거는 아니다.



## 3.8 BP-02 Production Mix smoke 기록

대상 시나리오: PRF-SCN-040~PRF-SCN-048

BP02 fixture 시험은 통과했지만 compare와 suite는 인증서 분리 변경 뒤 다시 실행하지 않았다.

Compare `bp02-20261007T133602.315971400Z-174004`와 suite `bp02-suite-20261007T133920.246951900Z-122940`는 양쪽 valid·5/5였다. Setup 49/49(TLS 25/TCP 24), persistent offline session 10/10, retained replay 구독자 4명, validity error 0, `finalCheck=true`였다. Fractional boundary compare `bp02-20261007T140157.761617200Z-105696`도 양쪽 valid이며 phase별 FLOOR publish count를 확인했다. Production gate가 아닌 고정 local smoke다. 당시 baseline `6c200085368d701f34dedfb31e1a968a84c09028`·`dirty=true`, profile SHA `b929efabd5f99e68758bec86a2034b0b80d153e9ce0291e64a8236b3e4afd7d4`와 원문은 artifact에 연결된다. 이후 채택한 용량 측정 방법은 [스펙 §3.9](../specs/performance-and-verification.md#39-bp-02-eks-용량-측정-계약)를 따른다.


## 3.9 BP-02 EKS 용량 측정 기록

대상 시나리오: PRF-SCN-049~PRF-SCN-064

### 시험 연결과 실행 기록

| 대상 | 시험 위치·명령 | 확인하는 규칙 | 결과 |
| --- | --- | --- | --- |
| Profile·cell·client ID | `tests/benchmark/capacity/{profile,plan}_test.go` | Profile 범위 검증, cell당 client 53명, tenant별 정확한 client ID, cell 밖으로 나가지 않는 filter | PASS |
| Gap·`cleanSessionLoss` 판정 | `tests/benchmark/capacity/collector_test.go` | 보장 수신자 규칙, persistent 무예외, PUBCOMP 뒤 미수신의 gap, clean publisher의 PUBCOMP 전 끊김(`cleanPublisherInterrupted`), 끊긴 동안의 publish 제외, p99 한도 경계의 `cleanSessionLoss` | PASS |
| 단계 판정·탐색 | `tests/benchmark/capacity/{verdict,ladder}_test.go` | p99 경계, invalid 우선, RTO·SessionPresent, 이분 탐색과 5회 확인 | PASS |
| Feature 활성화 | `tests/benchmark/capacity/management_test.go` `TestEnsureFeaturesActivatesAndVerifiesEveryRequiredFeature` | 세 feature를 활성화하고 하나라도 비활성이면 오류 | PASS |
| Marker 교환·Pod 자원 기록 | `tests/benchmark/capacity/control_test.go`; `tests/benchmark/capacity/eks_control_test.go` `TestEKSKillMarkersDecodeInRunner`; `tests/test_eks_capacity_controller.py` | Runner와 controller의 marker 순서와 timeout, 실제 Python producer → Go consumer의 5개 상태 JSON 형식, broker·sidecar·init container 자원의 분리 기록 | PASS. EKS에서 발견한 숫자 node ID 회귀는 수정 전 5개 상태 모두 FAIL → 문자열로 수정 뒤 모두 PASS. `GOMAXPROCS=2 go test ./tests/benchmark/capacity -count=1` 및 Python 33개 PASS, 2026-10-16 |
| EKS overlay 경계 | `GOMAXPROCS=2 go test -p=2 ./tests/integration -run '^TestEKSCapacityOverlay$' -count=1` | 기본 node 설정에서 MQTT TLS·security 경로만 다름, sidecar·Job 자원·routing 경계 | PASS, 2026-10-16 |
| Pod 장애 대상 identity | `tests/test_eks_capacity_controller.py` `FaultIdentityTests.test_signals_require_the_original_live_broker_identity` | 다른 Pod UID·worker·container, 종료·삭제·상태 미확인 대상과 sidecar에 신호를 보내지 않음 | PASS. Controller 시험 총 34개 |
| 실제 broker의 process 정지·복구 | [실행 결과](../../.artifacts/verification/capacity-pod-fault-20261016T033428Z/result.json), [controller rollback](../../.artifacts/verification/capacity-pod-fault-20261016T033428Z/controller-rollback-events.jsonl) | Native amd64 Linux Docker에서 실제 standalone broker PID 8의 thread 7개 정지 → HTTP 불통 → SIGCONT 뒤 Ready. 잘못된 실행 UUID 거절. 삭제 오류를 주입한 controller가 실제 process를 다시 실행시킴. 정상 TERM 전달 뒤 exit 0 | PASS. Docker stop 0.408초. 삭제 오류는 adapter에서 주입했으며 Kubernetes·EKS 수용 증거는 아님 |
| 삭제 요청 수락 뒤 종료 정리 | [실행 결과](../../.artifacts/verification/capacity-pod-cleanup-20261016T043338Z/result.json), [수정 전](../../.artifacts/verification/capacity-pod-cleanup-20261016T043338Z/before.json), [수정 뒤](../../.artifacts/verification/capacity-pod-cleanup-20261016T043338Z/after.json), [복구 timeout](../../.artifacts/verification/capacity-pod-cleanup-20261016T043338Z/restore-timeout.json) | 삭제 요청은 성공했지만 원래 UID인 Pod가 남은 상태에서 controller가 끝나도 같은 process의 재개를 시도함 | 수정 전에는 Ctrl+C 뒤 실제 broker가 계속 stopped였다. 수정 뒤 Ctrl+C·controller 예외·restore timeout 모두 HTTP Ready와 worker uncordon 확인. Kubernetes 삭제 지연은 adapter에서 주입했으며 이 정리 수정의 EKS 실행은 하지 않음 |
| CPU throttling·OOM·재시작 관측 | `tests/test_eks_capacity_controller.py` `RuntimeDiagnosticsTests`의 4개 시험; `python -B -W error::ResourceWarning tests/test_eks_capacity_controller.py` | cgroup v1/v2 단위 변환, 누계 증가량, 재시작·Pod 교체·읽기 실패 때 baseline 무효화, 현재·직전 OOM 상태 보존 | PASS, 2026-10-16 controller 시험 총 33개 |
| 관측 수집 경로 smoke | [실행 결과](../../.artifacts/verification/capacity-runtime-health-20261016T014702Z/result.json), [수집 자료](../../.artifacts/verification/capacity-runtime-health-20261016T014702Z/runtime-health.jsonl) | CPU·메모리 제한 Docker container의 실제 `cpu.stat` 읽기 → `ResourceSampler`·`RuntimeDiagnostics` → JSONL 기록. Docker inspect의 실제 상태를 Pod DTO로 변환한 adapter 사용 | PASS, 0.25 CPU·32 MiB에서 약 3초 부하: 30개 period 중 30개 throttled, 증가 시간 2,405,808µs. 재시작 뒤 delta `null`, 실제 OOMKilled exit 137 기록. Container 삭제 완료. Kubernetes exec/status 전송은 미검증 |
| 실제 EKS 관측 사전 확인 | [runtime-health](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/preflight/runtime-health.jsonl) | 실제 `kubectl exec`·Pod status 조회, cgroup v2 누계와 증가량 수집 | PASS, broker 3개 모두 읽기 성공. 유휴 상태 throttling 0. Kubernetes 1.37·AL2023 ARM64. 부하 중 여유나 모든 OOM 이력의 증거는 아님 |
| EKS의 큰 credential Secret 준비 | [실패](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/fixture-prepare-failure.json), [수정 뒤 성공](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/fixture-prepare-success.json) | Credential 파일 585,729 bytes를 annotation에 넣지 않고 Secret으로 저장 | Client-side apply FAIL → server-side apply PASS. Secret 값은 출력·공개 artifact에서 제외. Controller 회귀 시험 33개 PASS |
| Local 단계 실행 | `go run ./tests/benchmark/capacity local --profile tests/benchmark/capacity/testdata/local-smoke.json` | 장애 주입·복구·판정·정리가 끝까지 실행됨 | 수정 전 FAIL([기록](../../.artifacts/verification/capacity-local-20261015T231636Z/capacity-summary.json)), 수정 뒤 PASS 3/3([기록](../../.artifacts/verification/capacity-local-20261016T011708Z/capacity-summary.json)) |
| EKS 측정 | `tests/benchmark/capacity/eks/controller.py`, 아래 2026-10-16 실행 기록과 Pod 장애 보완 재실행 | `B_*` 기록 | 초기 3회와 주입 보완 뒤 1회 모두 invalid. 보완 뒤에는 삭제·N-1 측정·Ready·resync 복구가 끝났지만 quota 계열 거절 3건으로 중단했다. `B_*`와 bounds는 채택하지 않음 |
| 보완한 Pod 장애의 EKS 실행 | [Controller events](../../.artifacts/verification/eks-podfix-20261016T033737Z/capacity/controller/events.jsonl), [최종 node 상태](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/management-nodes.json) | 원래 Pod UID·container·실행 UUID 확인 → broker thread 정지 → 강제 삭제 → N-1 → 새 Pod Ready·resync | 해당 경로 1회 확인. 전체 용량 판정은 invalid이며 5회 확인과 상위 단계는 실행하지 않음 |

2026-10-15 수정 전 local 실행(Docker 3 node, 1–2 cell)은 단계 3개 중 1개가 통과하고 2개가 실패했다. 아래 원인 분석에서 실패가 broker 동작에서 나왔음을 재현으로 확인했다.

- Leader kill 뒤 생존 node가 PUBACK한 shared group `a_job`(clean session member) QoS 1 message를 살아 있는 member가 받지 못했다(gap 1개, 3개).
- 생존 node에 연결된 persistent `alert` subscriber 3개가 `measureN1` 중 broker 쪽에서 끊겼다. 약 0.4초 뒤 `SessionPresent=true`로 다시 연결했다.
- 1 cell 확인 실행의 `measureN1` QoS 1 p99는 2.59초였다.

원인 분석은 [N-1 delivery 분석](../../.artifacts/analysis/n1-delivery/analysis.md)에 있다. 2026-10-15 사용자 결정에 따라 다음과 같이 처리했다.

- Shared group gap: member 선택이 owner node의 도달 여부를 보지 않았고, durable 대상은 primary만 썼다. 죽은 node의 clean member queue에 들어간 message는 그 member가 `CleanStart=true`로 돌아오면 사라졌다. [ADR-0060](../decisions/ADR-0060-shared-subscription-durable-owner-loss-reroute.md)으로 ADR-0015 §8을 고쳤다. Ingress node는 도달할 수 없는 owner의 member를 고르지 않는다. Member session이 reset·폐기·만료되면 남은 shared message를 같은 group의 다른 member로 옮긴다. 수용 기록은 [Cluster §11.35](../specs/cluster-session-and-delivery.md#1135-공유-구독-durable-전달의-owner-손실-수용-매핑)다.
- 생존 node의 persistent 연결 끊김: 죽은 leader에 전달된 PUBACK COMPLETE commit이 5초 commit timeout에 걸려 broker가 연결을 닫았다. `proposal-recovery`를 켜면 재현 8회 중 0회로 사라졌다(끄면 8회 중 4회). Broker는 바꾸지 않았고 위 환경 조건에서 이 feature를 켠다.
- QoS 2: PUBREC 뒤 PUBREL commit 전에 clean session publisher의 연결이 끊기면 message가 전달되지 않는다. PRD §8에서 이 경우를 보장 대상에서 뺐고, 판정은 `cleanPublisherInterrupted`로 따로 기록한다.

수정 뒤 같은 명령을 다시 실행했다([기록](../../.artifacts/verification/capacity-local-20261016T011708Z/capacity-summary.json)). 세 feature가 활성 상태였고 단계 3개가 모두 통과했다.

| 단계 실행 | Cell | RTO | p99 `measureN` QoS 0/1/2 | p99 `measureN1` QoS 0/1/2 | Gap | `cleanSessionLoss` | `measureN1` 계획 밖 disconnect |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 01-r1 | 1 | 1.85초 | 2.2 / 53.7 / 57.0ms | 1.4 / 54.5 / 39.4ms | 0 | 0 | 0 |
| 02-r1 | 2 | 3.55초 | 25.2 / 96.9 / 160.7ms | 3.3 / 115.3 / 88.3ms | 0 | 0 | 0 |
| 02-r2(확인) | 2 | 4.12초 | 81.1 / 606.2 / 652.7ms | 1.3 / 81.7 / 98.5ms | 0 | QoS 1 2개, 최대 36.7ms | 0 |

이 실행은 Docker 한 host의 local smoke이며 p99 한도는 1,000ms다. 판정 경로가 동작한다는 증거일 뿐 `B_*`나 EKS 100ms 기준의 증거가 아니다. 확인 실행의 `measureN` p99가 600ms를 넘은 것은 장애 전 구간이다. 원인은 확인하지 않았다 [INFERENCE: 같은 host의 Docker CPU 경합].

### EKS 용량 실행 기록 (2026-10-16)

사용자가 승인한 격리 fixture `eks-capacity-20261016T015110Z-db4c38`에서 실행했다. Broker는 3-AZ의 `m7g.large` 3대, generator는 taint로 분리한 `c7g.xlarge` 1대다. Broker container는 각각 1 CPU·4 GiB이고 sidecar 예산은 별도다. [Image 기록](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/image-records.json)에 ARM64 digest·OCI label을 고정했다. Peer 인증서·bootstrap pin을 새로 만들었고 합성 데이터만 썼다. `proposal-recovery`·`session-metadata`·`shared-subscription-reroute`를 모두 활성화했다.

| 실행 | 결과와 근거 |
| --- | --- |
| 기본 profile, 첫 8 cell·424 연결·320 publish/s | Setup 중 `CONNACK 0x88`로 invalid. Measure·장애 주입 전에 종료. 원인은 미확정이며 CPU 포화로 해석하지 않음. [Summary](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity/generator-output/capacity-summary.json) |
| 작은 부하 탐색, 첫 1 cell·53 연결·40 publish/s | 기본 단계 앞에 1·2·4 cell을 추가한 [profile](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/eks-capacity-low-range-v1.json). Rate·p99 100ms·RTO 30초·5회 확인·손실 기준은 그대로다. Python marker가 숫자 node ID를 써서 Go runner가 ACK하지 못함. Pod 삭제 없이 invalid. [Summary](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity-low/generator-output/capacity-summary.json) |
| Marker 수정 뒤 같은 작은 부하 | Leader node 2 Pod 삭제, N-1 60초 측정, uncordon까지 실행. 대체 Pod는 running이지만 10분 안에 Ready가 되지 않아 invalid. [Summary](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity-fixed/generator-output/capacity-summary.json), [controller events](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity-fixed/controller/events.jsonl) |

세 번째 실행의 마지막 직접 조회에서 node 2는 replication `HEALTHY`·`SAFE_TO_STOP`·`ready=false`였다([관리 API](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/management-nodes.json)). `/health/ready`는 503과 `node drain record is active`를 반환했다([응답](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/restored-pod-health.json)). 삭제 때 `FailedPreStopHook` event가 있었고, 대체 broker는 같은 drain operation을 재개해 `COMPLETED`를 commit했다([event](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/events.json), [log](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/narumq-1.log)). 현재 `force delete`가 preStop을 실행하지 않는다는 가정은 성립하지 않았다. 정상 drain을 포함한 이 실행을 갑작스러운 broker 손실의 수용 증거로 쓰지 않는다. 장애 전용 lifecycle 분리 또는 정상 drain 시험으로의 구분은 다음 실행 전에 결정해야 한다. 기준을 낮추거나 active drain을 임의로 지우지 않았다.

복구 실패와 별도로 runner는 QoS 1·2 ACKed gap을 각각 1개 기록했다. QoS 1은 `a_job` sequence 517, QoS 2는 `a_command` sequence 717이다([message 표본](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity-fixed/generator-output/steps/01-r1/messages-sample.jsonl)). 원인은 아직 확인하지 않았다. 다음은 이 invalid 실행의 관찰값이며 통과 판정이 아니다([step summary](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/capacity-fixed/generator-output/steps/01-r1/step-summary.json)).

| QoS | `measureN` p99 | `measureN1` p99 |
| --- | --- | --- |
| 0 | 4.629ms | 4.937ms |
| 1 | 63.083ms | 66.681ms |
| 2 | 64.700ms | 65.851ms |

Runner가 관찰한 영향 client 13개의 reconnect RTO는 7.597초, SessionPresent failure는 0이었다. 이는 대체 broker의 Ready 복구 시간이 아니다. Generator의 측정 구간 CPU는 3 CPU 한도 기준 약 0.294%였다. [Pod resource 표본의 최댓값](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/resource-peaks.json)은 broker Pod 263m CPU·36 MiB, generator Pod 16m CPU·19 MiB다. Pod 값은 sidecar를 포함하며 연속 관측의 절대 최댓값이 아니다.

[Runtime 관측 분석](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/telemetry-analysis.json)에는 세 실행의 원본 JSONL과 같은 container에서 얻은 증가량만 합산했다. 세 번째 실행의 broker별 관측 period 합은 7,225·6,162·7,408이고 `nr_throttled`·`throttled_usec` 증가량은 모두 0이었다. 삭제·교체 중 node 2에서 CPU 값을 얻지 못한 표본 5개는 정상값으로 대체하지 않았다. `run-start`·`step-ready`·`step-complete`·`run-end` 경계와 periodic 표본이 실제 EKS에서 수집됐다. 관측된 현재·직전 종료 상태에 `OOMKilled`는 없었다. 삭제 사이의 모든 종료나 고부하에서의 안전성을 보장하지 않는다.

모든 실행의 `adopted`·`bounds`는 `null`이다. 5회 확인과 상위 단계 탐색은 완료하지 못했다. `B_*`, production 지원과 release gate는 미검증으로 유지한다.

시험 뒤 Namespace·NLB·target group·시험 PVC/EBS를 먼저 정리했다. 별도 state에서 만든 Terraform 자원은 계획한 42개만 생성하고 42개 모두 삭제했다. Fixture CloudWatch log group과 이번에 만든 ECR image tag 두 개도 삭제했다. AWS API로 cluster·VPC·활성 instance·시험 EBS·EIP·NLB·target group·IAM role·log group이 없고 NAT gateway가 deleted임을 확인했다([정리 계획](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/destroy-plan-review.json), [적용 기록](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/destroy-apply.txt), [최종 확인](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/cleanup-verification.json)). 보호 snapshot 3개와 `observability-20261014` image digest는 유지했다.

현재 Secret·TLS key·kubeconfig·Terraform state와 계획·비공개 log 등 58개 파일은 DPAPI CurrentUser로 암호화했다. 복호화한 SHA-256이 원본과 같은지 확인한 뒤 평문·임시 script·binary·provider cache를 지웠다([보관 결과](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/final/private-archive.json)). [집중 검증 기록](../../.artifacts/verification/eks-capacity-20261016T015110Z-db4c38/focused-verification.json)은 capacity package·Python 33개·vet·traceability PASS를 연결한다. 앞선 전체 local gate의 coverage 실패는 해결하거나 재실행하지 않았다.

### Pod 장애 주입 보완의 EKS 재실행 (2026-10-16)

사용자가 Pod 장애 보완을 선택한 뒤 `eks-podfix-20261016T033737Z` fixture에서 다시 실행했다. 앞선 fixture는 이미 정리했으므로 별도 state로 같은 규모의 새 환경을 만들었다. Broker·runner ARM64 image digest와 작은 부하 탐색 profile은 앞선 실행과 같다([image](../../.artifacts/verification/eks-podfix-20261016T033737Z/image-records.json), [profile](../../.artifacts/verification/eks-podfix-20261016T033737Z/eks-capacity-low-range-v1.json)). Broker binary나 운영용 preStop은 바꾸지 않았다.

준비 때 broker 3개가 서로 다른 worker·AZ에 있고 generator는 전용 node에 있는지 확인했다([배치](../../.artifacts/verification/eks-podfix-20261016T033737Z/preflight/placement.json)). 모든 broker의 process helper가 실제 Pod UID를 반환했다. Kustomize가 만든 process ConfigMap은 namespace를 명시해 StatefulSet volume과 같은 이름으로 연결했다([실제 render](../../.artifacts/verification/eks-podfix-20261016T033737Z/rendered-broker.json)). MQTT 두 target group의 대상 3개가 모두 healthy인 상태에서 시작했다([NLB](../../.artifacts/verification/eks-podfix-20261016T033737Z/nlb-preflight.json)).

AWS Load Balancer Controller는 Helm chart 1.17.1·image v2.17.1을 썼다. 실제 container status의 digest `sha256:3f4a2222c128a7e2baccae3f1d2cc566c70aa7a6cddda6caa22488fd3aa0337c`는 teardown 중 남아 있던 EKS audit log에서 확보했다. 새로 pending인 Pod의 빈 status를 runtime 증거로 쓰지 않았다([도구 정보](../../.artifacts/verification/eks-podfix-20261016T033737Z/tooling.json), [선별한 audit 증거](../../.artifacts/verification/eks-podfix-20261016T033737Z/controller-image-audit-evidence.json)). Metrics server managed addon은 `v0.9.0-eksbuild.13`이다.


첫 1 cell(53 연결, 40 publish/s)에서 leader node 3을 골랐다. 원래 UID·container ID·실행 UUID를 확인하고 PID 8의 thread 9개를 멈췄다. 이어서 worker를 cordon한 상태로 Pod를 강제 삭제했다. `killing`·`down`·`restoring`·`restored`를 runner가 모두 관찰했다. Restore 처리는 uncordon과 새 Pod Ready·resync 확인을 포함해 28.93초였다([controller events](../../.artifacts/verification/eks-podfix-20261016T033737Z/capacity/controller/events.jsonl), [step summary](../../.artifacts/verification/eks-podfix-20261016T033737Z/capacity/generator-output/steps/01-r1/step-summary.json)). 삭제 때 preStop 실패 event는 있었지만 대체 broker는 active drain에 묶이지 않았다. 마지막 직접 조회에서 세 broker 모두 `READY`·`ready=true`였고 `/health/ready`도 모두 200이었다([node 상태](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/management-nodes.json)).

| QoS | `measureN` p99 | `measureN1` p99 |
| --- | --- | --- |
| 0 | 6.042ms | 3.273ms |
| 1 | 56.665ms | 61.815ms |
| 2 | 73.060ms | 68.873ms |

두 측정 구간의 ACK timeout·protocol error·계획 밖 disconnect는 모두 0이었다. 영향 client 13개가 복구됐고 SessionPresent 실패는 0이었다. Runner가 첫 장애 연결 끊김을 기준으로 잰 RTO는 2.227초였다. 주입 전 `killing` 관찰(ACK 직전)부터 회복까지를 같은 generator clock으로 넓게 잡으면 상한은 11.534초다. 이 상한에는 장애 전 controller 처리도 포함된다. 서로 다른 host 시각을 빼서 정확한 process 정지 RTO로 보고하지 않는다([clock 계산](../../.artifacts/verification/eks-podfix-20261016T033737Z/recovery-clock-analysis.json)).

이 한 번의 실행에서 ACKed gap·QoS 2 중복·`cleanSessionLoss`는 모두 0이었다. QoS 1 unacked 48개와 QoS 0 미수신 132개는 별도 기록이다. 모든 offered message가 전달됐다는 뜻은 아니다. 앞선 drain 혼합 실행에서 나온 ACKed gap의 원인과 재현 조건을 닫는 증거로도 쓰지 않는다.

전체 판정은 **invalid**다([summary](../../.artifacts/verification/eks-podfix-20261016T033737Z/capacity/generator-output/capacity-summary.json)). Runner는 `tenant limit rejection observed 3 times`를 기록했다. 그러나 이 counter는 MQTT `0x97`·`0x9F`를 모두 tenant 한도로 분류한다(`tests/benchmark/capacity/verdict.go` `isLimitRejection`, `client.go` `finishAck`). Broker는 서버 commit lag·delivery capacity 거절에도 `0x97`을 쓴다(`internal/adapters/tcp/server_submission.go`). Node 2의 실제 metric은 `narumq_publish_backpressure_rejections_total{reason="commit_lag"} 3`, `reason="tenant_quota"`는 0이었다([metric](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/narumq-1-metrics.txt), [log](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/narumq-1.log)). 따라서 runner 문구만으로 실제 tenant 한도 초과라고 단정하지 않는다.

Metric 합계는 서버 backpressure 설명을 뒷받침하지만 패킷별 원인이나 발생 구간과 직접 연결한 증거는 아니다. 재생성한 node의 metric에는 죽은 container의 이력이 없다. Quota 계열 거절의 원인·구간 기록과 tenant/server 분류 보완은 남았다. 이번 3건을 임의로 failure 구간의 허용 거절로 바꾸거나 제외하지 않았다([분석·한계](../../.artifacts/verification/eks-podfix-20261016T033737Z/rejection-analysis.json)). `adopted`·`bounds`는 null이고 5회 확인·상위 단계·`B_*` 채택은 미완료다.

[Resource 표본](../../.artifacts/verification/eks-podfix-20261016T033737Z/resource-peaks.json)의 broker Pod 최댓값은 193m CPU·35 MiB, generator Pod는 15m CPU·18 MiB였다. [Runtime 관측](../../.artifacts/verification/eks-podfix-20261016T033737Z/telemetry-analysis.json) 14개에서 읽을 수 있었던 같은 container의 throttling 증가량은 모두 0이었다. 삭제·교체 중 node 3의 CPU를 읽지 못한 표본 5개는 제외했다. 수집한 현재·직전 종료 상태에 OOMKilled는 없었다. 이 낮은 부하의 표본은 CPU 100% 포화나 더 높은 부하의 안전성을 검증하지 않는다.

시험 뒤 Namespace·NLB·target group·시험 PVC/EBS를 정리하고 Terraform 자원 42개를 모두 삭제했다. AWS API로 fixture cluster·VPC·활성 instance·시험 EBS·EIP·NLB·target group·IAM role·log group이 없고 NAT gateway가 deleted임을 확인했다. 이번에 다시 올린 image tag 두 개도 삭제했다. 보호 snapshot 3개와 `observability-20261014` digest는 유지했다([정리 계획](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/destroy-plan-review.json), [적용 기록](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/destroy-apply.txt), [최종 확인](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/cleanup-verification.json)).

TLS key·JWT credential·현재 Secret·kubeconfig·state·계획·비공개 log·임시 Docker 인증 등 62개 파일은 DPAPI CurrentUser로 암호화했다. 복호화한 SHA-256을 원본과 대조한 뒤 평문·임시 script·binary·provider cache를 지웠다([보관 결과](../../.artifacts/verification/eks-podfix-20261016T033737Z/final/private-archive.json)). Capacity package·Python 34개·EKS overlay 집중 시험·vet는 PASS다([실행 기록](../../.artifacts/verification/eks-podfix-20261016T033737Z/focused-verification.json)). Traceability도 PASS였지만 전체 local gate는 재실행하지 않았으며 기존 coverage 미달은 남아 있다.

### 삭제 지연 시 종료 정리 보완 (2026-10-16)

EKS 재실행과 자원 정리 뒤 `handle_kill`의 종료 정리 결함을 확인했다. 삭제 요청이 수락되자 `frozen` 추적을 끝내고 있어, 실제 삭제가 지연된 상태에서 controller가 중단되면 `finally`가 broker를 다시 실행시키지 않았다. 추적 종료를 `handle_restore`에서 다른 UID인 Ready Pod를 확인한 뒤로 옮겼다. 기존 대상이 남아 있으면 원래 identity에 대한 재개 경로를 유지한다.

Native amd64 Linux의 실제 standalone broker로 수정 전·후를 확인했다. 수정 전에는 삭제 수락 뒤 Ctrl+C를 주입하면 HTTP가 계속 응답하지 않았다. 수정 뒤에는 Ctrl+C·controller 예외·restore timeout 모두 `RunController.run`의 `finally`가 broker를 재개했고 HTTP Ready가 됐다. Worker uncordon도 확인했다. 시험 container는 0.5 CPU·256 MiB로 제한했다. Python controller 시험 34개도 PASS다([결과와 범위](../../.artifacts/verification/capacity-pod-cleanup-20261016T043338Z/result.json)).

실제 broker와 HTTP는 실행했지만 Kubernetes의 삭제 지연과 worker 상태는 adapter에서 주입했다. 관측 sampler는 이 정리 시험에서 제외했다. 이 수정으로 EKS를 다시 만들지 않았고, 앞선 EKS invalid 판정이나 `B_*` 미채택 상태도 바꾸지 않았다. Container는 정상 종료 코드 0을 확인한 뒤 지웠고 임시 binary도 지웠다.

사용자 결정에 따라 정리 가능한 중단만 지원한다. Controller 강제 종료나 workstation 장애를 복구하는 lease·watchdog은 추가하지 않았다. 이 경우에는 fixture의 수동 복구 또는 삭제가 필요하다. 따라서 앞선 “controller 중단 시 복구” 설명은 모든 process 종료를 보장하는 뜻이 아니다.

### Quota 계열 거절 원인·구간 분류 보완 (2026-10-17)

대상 시나리오는 MQTT-SCN-054~057과 PRF-SCN-117~119다. 기준은 [MQTT §7.2](../specs/mqtt-protocol-and-session.md#72-quota-계열-거절의-원인-표시)와 [성능 §3.9](../specs/performance-and-verification.md#quota-계열-거절-판정)다. Windows amd64·Go 1.26.5에서 broker와 runner를 함께 바꿨다. MQTT reason code는 유지했다. PUBACK·PUBREC의 Reason String으로 class·cause를 전달하고 runner는 거절마다 구간과 원인을 기록한다. Tenant 한도는 invalid, 서버 backpressure와 분류 불가는 fail이다. 실패 artifact 기록 오류도 실행 오류로 보존한다.

과거 EKS 실행의 거절 3건은 packet별 Reason String이 없으므로 `unclassified`다. Metric의 `commit_lag=3`을 이 packet들의 원인으로 대입하지 않았다. 과거 artifact·invalid 판정·`B_*` 미채택 상태는 바꾸지 않았다. 새 binary의 EKS 측정은 미실행이다. AWS/EKS 자원은 만들거나 바꾸지 않았다. SUBACK `0x97`과 `0x9F`는 분류 불가로 남긴다.

| 명령·확인 방법 | 결과 | 범위 |
| --- | --- | --- |
| 변경한 Go 파일에 `gofmt -w` 적용 | 통과 | 변경한 broker·runner·단위 시험 파일 |
| `go vet ./...` | 통과 | 전체 Go package |
| `go test ./internal/domain/mqtt ./internal/application/publish ./internal/adapters/tcp ./cmd/narumq ./tests/benchmark/capacity` | 통과 | Codec, 거절 원인 보존, TCP 응답, 분류·판정·구간별 JSONL·summary·기록 실패 보존 |
| `go run ./tools/verify scenarios` | 통과 | 연결 검사. Spec 7개·시나리오 632개·미연결 0·오류 0. 실행 통과 건수는 이 명령으로 증명하지 않는다. |
| `go run ./tools/verify markdown-links` | 통과 | 후속 문서 갱신까지 문서 190개·로컬 링크 3,181개 |
| `go test ./internal/adapters/tcp -run 'TestWritePacketDropsReasonStringAboveClientMaximumPacketSize\|TestTCPPublishBackpressureReasonCodes\|TestTCPPublishTenantLimitReasonCodes' -count=1 -v` | 통과 | 실제 socket의 PUBACK·PUBREC `0x97`, Reason String·Maximum Packet Size와 queue 원본 보존을 확인했다. 이 결과는 거절이 없는 Docker smoke와 별개다. |
| `go run ./tests/benchmark/capacity local --profile tests/benchmark/capacity/testdata/local-smoke.json` | 실패 | 명령은 exit 0·283.70초였지만 PRF-SCN-062의 모든 단계 통과 조건은 충족하지 못했다. 01-r1·01-r2 통과, 02-r1은 setup CONNACK `0x88`로 실패했다. [Summary](../../.artifacts/verification/capacity-local-20261017T123309Z/capacity-summary.json). |
| `go run ./tools/verify local` | 통과 | `NARUMQ_*` 환경 변수 없이 `local: PASS`. 시나리오 실행 통과 513·실패 0·범위 안 미실행 0, coverage PASS. 앞선 socket packet 시험과 이어 실행한 명령 전체는 626.42초다. Local 명령 단독 시간은 따로 계측하지 않았다. |

02-r1은 setup에서 끝나 측정·장애 주입 단계에 들어가지 않았다. [연결 기록](../../.artifacts/verification/capacity-local-20261017T123309Z/steps/02-r1/connections.jsonl)에는 CONNACK `0x88` 2건과 handshake timeout 8건이 있다. 일반 ladder는 실패한 상위 단계 뒤 하위 단계를 확인할 수 있으므로 명령이 0으로 끝났다. 그러나 local-smoke의 PRF-SCN-062는 모든 단계 통과를 요구한다. 이 실행을 smoke 통과나 EKS 용량의 증거로 쓰지 않는다. Broker의 같은 구간에 `stale_epoch` cleanup 경고가 있지만 첫 CONNECT 실패 원인과 직접 연결한 증거는 없다. 원인을 확인하지 않은 채 재시도·timeout·cutoff를 바꾸거나 같은 실행을 통과할 때까지 반복하지 않았다.

후속 검토에서 특정 미지원 CONNECT property를 고정하는 추가 회귀 시험과 그 시나리오 문구는 제거했다. 지원 CONNECT subset 자체는 바꾸지 않았다. 이 후속 변경에서는 전체 gate를 다시 실행하지 않고 Markdown 링크만 검사했다.

<a id="capacity-connect-fallback-diagnosis"></a>
### CONNECT 송신 뒤 주소 대체 진단과 차단 (2026-10-17)

대상은 PRF-SCN-120이다. 앞선 PRF-SCN-062 실패 원인을 찾으려고 기존 `0x88` 결정 경계에 임시 오류·시각 기록을 붙였다. Credential·CONNECT 본문은 기록하지 않았다. 원래 local-smoke profile과 resource·deadline·재시도·판정 값을 그대로 사용했다. 아래 세 실행은 진단용 binary다. 수용 통과의 증거로 쓰지 않는다. 임시 기록 코드는 진단 뒤 모두 제거했다.

| 진단 출력 root (`go run ./tests/benchmark/capacity local --profile tests/benchmark/capacity/testdata/local-smoke.json --output <root>`) | 관측 결과 |
| --- | --- |
| `.artifacts/verification/connect-diagnostic` | 180.55초. 02-r1 setup 실패를 재현했다. Node 1의 ownership commit이 `outcome=2`, `conflict=stale_epoch`, `err=nil`을 반환했고 CONNECT가 이를 `0x88`로 바꿨다. [Summary](../../.artifacts/verification/connect-diagnostic/capacity-local-20261017T125229Z/capacity-summary.json), [원인 경계 기록](../../.artifacts/verification/connect-diagnostic/capacity-local-20261017T125229Z/node1/broker.log). |
| `.artifacts/verification/connect-overlap-diagnostic` | 222.65초. 추가한 송신·dial·ownership 시각으로 같은 client의 겹친 CONNECT를 관측했다. Setup은 통과했으므로 앞선 setup 거절의 구체적 경로를 소급해 증명하지는 않는다. [Summary](../../.artifacts/verification/connect-overlap-diagnostic/capacity-local-20261017T125639Z/capacity-summary.json). |
| `.artifacts/verification/connect-lock-diagnostic` | 144.21초. CONNECT mutex 대기 시각까지 추가했다. 모든 단계는 통과했고 `0x88`은 재현되지 않았다. 이 결과로 앞선 실패를 해제하지 않았다. [Summary](../../.artifacts/verification/connect-lock-diagnostic/capacity-local-20261017T130144Z/capacity-summary.json). |

겹친 요청은 `bp02-a-c001-sub-telemetry-05`에서 확인했다. [Runner 기록](../../.artifacts/verification/connect-overlap-diagnostic/capacity-local-20261017T125639Z/runner.log)에는 node 2 주소로 CONNECT 송신 완료(12:58:50.101719859Z), CONNACK 대기 timeout(12:58:55.102462825Z), node 3 주소로 다음 CONNECT 송신 완료(12:58:55.102847095Z)가 순서대로 남았다. [Node 3](../../.artifacts/verification/connect-overlap-diagnostic/capacity-local-20261017T125639Z/node3/broker.log)은 epoch 2를 획득한 뒤 CLAIM이 stale epoch로 거절됐다. 아직 처리 중이던 첫 요청은 [node 2](../../.artifacts/verification/connect-overlap-diagnostic/capacity-local-20261017T125639Z/node2/broker.log)에서 12:58:55.479361325Z~12:58:55.540377533Z에 epoch 3을 획득했다. 즉 첫 CONNECT를 보낸 뒤의 timeout을 단순한 dial 실패처럼 대체하면 ownership 처리가 겹칠 수 있다.

이 관측으로 확인한 runner 주소 대체 결함만 수정했다. 공통 MQTT client의 `DialError`는 CONNECT 전 TCP dial·TLS handshake 실패만 표시한다. Capacity endpoint는 이 오류만 다음 주소로 대체한다. 쓰기 오류(일부 CONNECT byte 송신 포함), CONNACK timeout·거절·protocol 오류는 그대로 반환한다. 원래 오류는 `Unwrap`으로 보존한다. 기존 문자열 기반 거절 검사 helper는 제거했다. Broker ownership 처리·epoch 충돌·deadline·reconnect 횟수는 바꾸지 않았다. 이 수정은 과거 setup `0x88`의 전체 원인이 확정됐다는 뜻이 아니다.

`go test ./tests/benchmark/capacity ./tests/benchmark/internal/mqttclient -count=1`과 `go vet ./tests/benchmark/capacity ./tests/benchmark/internal/mqttclient`는 통과했다. 실제 scripted TCP listener로 CONNECT 송신 뒤 timeout·거절·일부 byte 뒤 reset이 두 번째 CONNECT를 만들지 않는지 확인했다. 외부 DNS는 쓰지 않았으며, CONNECT 전 dial 실패의 대체와 TLS 오류·취소의 typed wrapping도 확인했다.

수정 뒤 임시 계측이 없는 source로 원래 명령 `go run ./tests/benchmark/capacity local --profile tests/benchmark/capacity/testdata/local-smoke.json`을 실행했다. 245.97초 뒤 명령은 exit 0이었지만 PRF-SCN-062는 다시 **실패**했다([summary](../../.artifacts/verification/capacity-local-20261017T130729Z/capacity-summary.json)). 01-r1·02-r1은 통과했다. 02-r2 확인 실행은 clean-session loss 1건의 최대 나이 1,425.6ms가 1,000ms를 넘어 실패했다. 이어 실행한 하위 01-r2는 통과했다. 원래 기대한 세 실행이 모두 통과하지 않았으므로 smoke 통과로 표시하지 않는다. 이 실행에서는 setup `0x88`이 없었지만 과거 실패를 해제하거나 원인을 확정하는 증거는 아니다. 추가 동작 변경·기준 완화·반복 실행은 하지 않았다.

`go run ./tools/verify local`은 487.94초 뒤 **실패**했다. 실행 시작 시 도구는 `75d4688`의 source와 이 주소 대체 수정을 기준으로 빌드됐다. 실행 중 별도 도구 수정 `c6c74c7`이 `TestScenarioAlternatePlatformDeclarationsKeepActiveKeyInScope`를 추가했다. Go package test는 모두 통과했지만 뒤의 실행 검사가 새 test metadata를 읽으면서 기존 `go-test.json`에 없는 PRF-SCN-121 시험 1건을 누락으로 판정했다(통과 514, 실패 0, 실행 범위 안 미실행 1·밖 119). 이 혼합 snapshot을 local 통과의 증거로 쓰지 않는다. 해당 새 시험을 포함하는 `go test ./tools/verify -run "TestScenario" -count=1`은 별도로 통과했다(3.478초). [단독 JSON 증거](../../.artifacts/verification/scenario-platform-scope/alternate-declaration-results.json)도 실제 실행으로 확보했다(exit 0, 3.46초). 문서 링크 검사는 190개 파일·3,194개 local link에서 통과했다.

위 혼합 snapshot 실패를 보존한 뒤 `c8d9b06`으로 source·test metadata를 동결하고 기본 `go run ./tools/verify local`을 한 번 실행했다. 521.18초에 **local: PASS**였다. 실행 검사는 634개 중 통과 515·실패 0·실행 범위 안 미실행 0·게이트 밖 119였고 coverage도 통과했다. 별도 opt-in을 켜지 않았다. 이 결과는 분류·주소 대체 구현과 기본 local gate의 완료 증거이며 PRF-SCN-062 smoke 실패를 해제하지 않는다. 같은 동결 commit의 문서 링크 검사는 190개 파일·3,195개 local link에서 통과했다. Source·test metadata는 이 기록을 추가할 때도 변경하지 않았다.

<a id="capacity-isolated-rerun-1"></a>
#### 격리 재실행 1회차 (2026-10-17)

사용자가 승인한 계획은 최대 3회 실행이며, 실패 1건이면 FAIL로 판정하는 것이다. 부하는 주지 않았다. 명령은 바꾸지 않았다.

`go run ./tests/benchmark/capacity local --profile tests/benchmark/capacity/testdata/local-smoke.json`

[실행 summary](../../.artifacts/verification/capacity-local-20261017T140817Z/capacity-summary.json)

01-r1, 02-r1, 01-r2는 통과했다. 02-r2는 **실패**했다. setup 단계에서 CONNACK 읽기가 `i/o timeout`으로 18번 실패했다. 실패 연결 17건은 `172.21.0.2`로, 1건은 `172.21.0.4`로 향했다. 노드와 주소의 대응은 확인하지 못했다. 응답 대기 제한은 5초 `handshakeTimeout`이다(`tests/benchmark/capacity/client.go:21`).

Broker 로그에는 실패한 client ID가 없었다. 실패 시각 전후에 TLS 오류, readiness 변경, leader 변경도 없었다.

한 실행에서 cluster 하나를 재사용했다. 02-r2는 02-r1에서 node2 leader를 재시작한 뒤 9.7초에 시작했다. 02-r2에는 자체 장애 주입이 없었다.

2회차와 3회차는 규칙에 따라 실행하지 않았다.

실행 환경은 격리되지 않았다. 실행 전과 실행 중에 사용자 앱이 호스트 CPU를 사용했다. 실행 전 평균은 약 42%였다. 실행 중 평균은 68.97%, 최대는 99.81%였다. setup 구간의 표본은 99.81%였다. Kind와 registry의 잔여 container도 실행 중이었다. 증거는 [격리 실행 증거](../../.artifacts/verification/capacity-isolated-20261017/)에 있다.

따라서 격리 가설은 검증하지 못했다. PRF-SCN-062는 계속 FAIL이다.

[추론] 호스트 CPU 경합이나 대상 노드의 응답 지연이 setup 실패의 원인일 수 있다. 확인하지 못했다.











## 7.1 MQTT 클라이언트 호환성 기록

대상 시나리오: -

기존 혼합 suite의 9/10 FAIL 기록은 변경하지 않는다. 그중 필수 cluster drain은 2/2 PASS, SDK 진단은 7/8 PASS였으며 Go의 중복 Server Reference trap case가 실패했다. Phase 1 adapter suite는 26/26 통과했다. 이는 전체 운영 SDK 승인 결과가 아니다.

## 7.2 Fuzz 게이트와 수동 mutation 기록

대상 시나리오: PRF-SCN-065~PRF-SCN-071

| 검증 계약 | 영구 시험 또는 실행 결과 |
| --- | --- |
| P0 연결·미연결·stale 예외·PRD ID·정렬·tracking 경계 | `tools/verify/requirements_test.go::TestVerifyP0RequirementMapping` — 8개 사례 PASS |
| 예외 JSON strict decode·schema·중복·빈 값·경로 경계 | `TestLoadP0ExceptionsStrictlyValidatesJSON` — 9개 사례 PASS |
| PRD prefix 파싱과 MQTT statement ID 규칙 | `TestParseTraceabilityAcceptsPRDPrefixesAndKeepsStatementRule` PASS |
| 미연결·stale P0의 CLI 거절 | 임시 예외 파일에서 `UI-01` 제거와 `API-01` stale 등록을 실행; 두 명령 모두 exit 1 및 지정 오류 출력 |


## 8.2 Branch 측정 gate 수용 증거

대상 시나리오: PRF-SCN-077~PRF-SCN-086

2026-10-08, Windows/amd64·Go 1.26.5, 변경 전 revision `3b1541f2d4fd916db314f56cd3e091132592a6f7`에서 확인했다. Production source는 변경하지 않았다.

| 검증 | 명령·시험 | 관찰 결과 |
| --- | --- | --- |
| 실제 Go coverage block·분모 대응 | `tools/verify/branch_test.go::TestBranchCoverageMatchesGoCoverBlocks`; `go test ./tools/verify -count=1` | 17개 outcome 중 실행 6개·미실행 11개 일치. 원본·계측 사본의 line/function total 일치 |
| 계측·manifest 경계 | `TestInstrumentBranchesPreservesLinesAndAddsOnlyMissingArms`, `TestLoadBranchManifestRejectsInvalidSchemaAndJSON`, `TestEvaluateBranchCoverageRejectsStaleOrMismatchedManifest` | 줄 수·select 의미 보존, 중복 arm 없음, schema·SHA·파일·block 불일치 거절 |
| 출력 경로 안전성 | `TestBranchInstrumentRejectsUnsafeOutputBeforeDeletingFiles`, `TestBranchOutputRejectsSymlinkToRepositoryRoot`, `TestBranchOutputRejectsRepositoryLinkToExternalDirectory` | Root·상위·증거 directory와 Windows junction alias를 삭제 전에 거절. 외부 sentinel 보존 |
| Gate 거절 | `TestBranchRatchetDiffFloorAndRequirementShortfall`; 임시 policy로 `coverage` 실행 | Routing ratchet 100% 거절. Historical diff `--base 02ef51d`의 branch 69.14%·line 84.69%도 당시 하한 미달로 거절 |
| 최초 baseline 측정 | `go run ./tools/verify local` | 원본·사본의 line 33,701개·function 1,673개 분모 일치. 반복 측정의 branch 79.81~79.87%로 baseline 산정 |

출력 안전성 CLI 증거는 `.artifacts/verification/branch-output-safety-evidence.json`, 같은 profile 재평가는 `coverage-summary-after-safety-review.json`에 있다. 현재 gate 수치는 [스펙 §8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate)을 따른다.

Baseline 측정 중 `TestNodeCloseFlushesHeldAppendOrReportsFailure/consumed-prior-error`가 `close WAL: permission denied`로 실패했다. 원인은 fixture가 held sync 전에 실패 flag를 검사한 순서였다. 검사를 held sync 뒤로 옮기고 미처리 queue·held append assertion을 추가했다. Production `Node.Close`는 바꾸지 않았다.

Windows/amd64·Go 1.26.5·`CGO_ENABLED=1`·`CC=gcc`에서 `go test -race ./internal/adapters/raft -run "^TestNodeCloseFlushesHeldAppendOrReportsFailure$/^consumed-prior-error$" -count=1000 -cpu=1,2,4 -failfast` 총 3,000회, 상위 시험 총 900회와 Raft package 1회가 통과했다.

후속 local 시도에는 branch ratchet 미달, `TestRaftPeerDriverTicksQueuesAndStops` 선출 실패, `TestDynamicClusterMembershipTransitionCrashRecovery`의 node 4 READY 실패와 Windows 파일 잠금이 있었다. 실패를 제외하거나 PASS로 계산하지 않았다. 로그는 `.artifacts/verification/raft-close-fixture-local-{first,second,third,fourth,fifth,sixth,seventh}.log`에 있다. 두 cluster 대기 결함의 수정 근거는 [§8.6](#86-간헐-실패-원인-수정-수용-증거)에 있다.

## 8.3 Branch ratchet 노이즈 허용폭 수용 증거

대상 시나리오: PRF-SCN-087, PRF-SCN-088

2026-10-08, Windows/amd64·Go 1.26.5에서 측정했다. 반복 branch 측정은 80.03~80.08%, spread 0.05%p였고 계획한 35개 outcome은 각 실행에서 covered였다. 이 결과로 [ADR-0041](../decisions/ADR-0041-branch-ratchet-noise-tolerance.md)의 허용폭을 확인했다. 현재 ratchet·허용폭은 [스펙 §8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate)을 따른다.

### 결정적 시험과 branch outcome

Leader 제거·proposal 회수·readiness·membership 변경·peer identity·delivery queue·TCP writer 종료·bootstrap handshake 경계의 시험으로 35개 outcome을 확인했다. 당시 outcome ID와 미실행 목록은 `.artifacts/verification/branch/manifest.json`과 반복 coverage report에 있다. 소스의 과거 줄 번호는 현재 시험 위치로 사용하지 않는다.

`cmd/narumq`의 해당 focused race 시험 20회·`TestRunClusterModeInProcess` 3회, Raft의 membership replay·close 시험 50회, deliveryqueue와 TCP focused 시험 각 20회가 통과했다. 이는 전체 suite나 원격 CI 실행이 아니다.

### 재기준과 허용폭 경계

`.artifacts/verification/coverage-tolerance-c.out`과 저장소 밖 policy 사본으로 CLI 경계를 확인했다. 측정 80.08%에서 ratchet 80.29%(+0.21%p)는 exit 1, 80.27%(+0.19%p)는 exit 0이었다. 당시 전체 branch PRD floor에는 미달했으며 이후 floor 결정은 [§8.4](#84-전체-branch-floor-수용-증거)에 있다.

## 8.4 전체 branch floor 수용 증거

대상 시나리오: PRF-SCN-089

2026-10-09, Windows/amd64·Go 1.26.5에서 시험을 보강한 뒤 branch 82.82~82.89%를 측정했다. 당시 85% PRD floor에는 미달했다. 사용자 결정으로 전체 branch floor를 80%로 낮춘 뒤 실행한 `GOMAXPROCS=4 go run ./tools/verify local`의 최종 profile은 branch 10,830/13,073(82.84%), line 87.59%, function 98.68%였고 `requirementShortfalls`가 비었다. 이는 당시 policy 평가 결과다. 현재 기준은 [스펙 §8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate)에 둔다.

### 반복 race 검증

Cluster·TCP·auditlog·deliveryqueue focused 시험을 race detector 아래 각 20회 실행해 통과했다. Production source와 core ratchet은 변경하지 않았다.

### 미실행 outcome과 미달 원인

당시 미실행 outcome 2,246개 중 comparator nil guard 22개, 생성 protobuf marshal 오류 15개, 1 GiB archive 제한 2개, Windows stable-store sync 5개를 직접 분류했다. 이 44개가 전체 제외 대상은 아니며 나머지 2,202개도 모두 제외하거나 테스트 가능하다고 단정하지 않았다.

결과는 `.artifacts/verification/coverage-summary-floor-{a,b,c}.json`과 `floor-local-{a,b,c}.log`에 있다. Ratchet 경계 +0.21%p 거절·+0.19%p 통과는 `coverage-boundary-plus-{021,019}.json`에 있다. 이 작업에서는 원격 필수 CI status·merge 차단을 확인하지 않았다.

## 8.5 변경 코드 diff floor 수용 증거

대상 시나리오: PRF-SCN-090~PRF-SCN-096

2026-10-09 사용자 결정 [ADR-0044](../decisions/ADR-0044-diff-coverage-floor-90.md)에 따라 기존 profile을 새 diff policy로 평가했다. 대상 commit은 `de19302d9887bfe2650dd6dcc5ad975281d4a192`, 비교 부모는 `74c3cdbb4abc9b01e07e8fa7b936653e25c446f7`이다. 현재 threshold는 [스펙 §8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate)을 따른다.

```text
go run ./tools/verify coverage --profile .artifacts/verification/coverage.out --policy tests/verification/coverage-policy.json --branch-manifest .artifacts/verification/branch/manifest.json --base 74c3cdbb4abc9b01e07e8fa7b936653e25c446f7 --report .artifacts/verification/coverage-summary-diff-floor90.json
```

결과는 exit 0이며 diff line 82/84(97.62%), function 16/16(100%), branch 32/34(94.12%)였다. `passed=true`, branch `measured`, shortfall 없음이었다. `--base 02ef51d` 재평가는 branch 82.25%로 exit 1이었다(`coverage-summary-diff-floor90-historical.json`).

`TestBranchRatchetDiffFloorAndRequirementShortfall`과 `TestDiffCoverageLineAndFunctionFloorBoundaries`는 하한과 같은 값 허용·각 지표의 독립적인 미달 거절·diff에 ratchet 허용폭 미적용을 확인했다. `go test ./tools/verify -count=1`도 통과했다. 기존 profile 재평가이며 전체 local·원격 CI를 새로 실행한 결과는 아니다.

### 2026-10-17 diff 하한 80% 적용

사용자 결정 [ADR-0063](../decisions/ADR-0063-diff-coverage-floor-80.md)에 따라 `diffFloor`를 세 지표 모두 80%로 바꿨다. 위 기록의 90%는 당시 기준이다. `TestDiffCoverageLineAndFunctionFloorBoundaries`, `TestRunCoverageRejectsHistoricalDiffBelowFloor`, `TestRunCoverageAcceptsDiffAtOrAboveFloorWithMeasuredBranches`는 이제 저장소 policy의 `diffFloor` 값을 읽어 경계를 만든다.

원격 CI [run 39036288101](https://github.com/kayden-kim/narumq/actions/runs/39036288101)의 `coverage.out`과 branch manifest를 새 policy로 다시 평가했다. 산출물은 Linux runner가 만들었고 manifest는 LF 원본의 SHA-256을 쓴다. Windows 작업 트리는 CRLF라 그대로는 `branch manifest is stale`로 중단한다. 그래서 `core.autocrlf=false`로 만든 임시 clone에서 `GOOS=linux`로 실행했다. 평가 대상은 `c8d9b06d9d103596c3f322522dd516083a3a13a3`이고, 임시 clone의 `origin/main`은 `8a59c22fcb401f8ae67bed2608c8a005bfda8a29`로 맞췄다. 이 두 commit 사이에서 `internal/`·`cmd/`는 `eca98da`까지 바뀌지 않았다. `go build -o <임시 경로>/verify.exe ./tools/verify`로 만든 같은 소스의 실행 파일을 썼다.

```text
GOOS=linux verify.exe coverage --profile <artifact>/coverage.out --policy tests/verification/coverage-policy.json --branch-manifest <artifact>/branch/manifest.json --base origin/main --report .artifacts/verification/diff-floor80-smoke/coverage-summary.json
coverage: coverage gate failed:
diff branch coverage 70.11% is below 80.00%
```

결과는 exit 1이다. Diff line 3968/4955(80.08%)와 function 368/406(90.64%)은 새 하한을 넘었고, diff branch 1520/2168(70.11%)만 미달했다. 80%에 닿으려면 변경 결정점의 outcome 1735개가 covered여야 하므로 215개를 더 실행해야 한다. 미실행 648개 중 상위 파일은 `internal/adapters/rollout/kubernetes_runtime.go` 79/80, `internal/adapters/tcp/peer_observability_adapter.go` 49/56, `internal/adapters/tcp/peer_snapshot_stream.go` 48/156, `internal/adapters/rollout/oci_metadata.go` 42/68, `internal/adapters/tcp/peer_observability_transport.go` 42/105이다(미실행/전체 diff outcome). 상위 네 파일의 미실행 outcome 합이 218개다.

## 8.6 간헐 실패 원인 수정 수용 증거

대상 시나리오: PRF-SCN-097~PRF-SCN-099

2026-10-09, Windows/amd64·Go 1.26.5·`CGO_ENABLED=1`·`CC=gcc`에서 §8.2의 두 cluster 대기 실패를 보완했다. 시험 제외나 대기 시간 증가는 하지 않았다.

| 실패·원인 | 수정·관찰 |
| --- | --- |
| `TestRaftPeerDriverTicksQueuesAndStops`: bootstrap ConfChange 적용 전 campaign이 무시돼 randomized election에 의존 | 두 driver 시작·bootstrap 적용 뒤 campaign. 기존 5초 제한 유지. `go test -race ./cmd/narumq -run '^TestRaftPeerDriverTicksQueuesAndStops$' -count=100 -cpu=1,2,4 -v` 총 300회 통과 |
| Node 4 READY 실패·`release_profile_unsynced`: profile proposal이 process 수명 context로 결과를 기다려 reconcile을 막을 수 있음 | `clusterReleaseProfileController`의 Commit에 기존 cluster 경로와 같은 5초 상한 적용. Gate는 닫힌 채 다음 reconcile에서 재확인. 무응답 proposal 회귀 시험은 수정 전 FAIL·수정 뒤 PASS |

기존 로그는 leader 손실 뒤 proposal 유실 자체를 입증하지 않는다. 무기한 대기 경로를 회귀 시험으로 확인했고 실제 process는 `go test ./tests/integration -run '^TestDynamicClusterMembershipTransitionCrashRecovery$' -count=3 -v -timeout 30m`으로 3회 통과했다. 이 성공 출력만으로 일시적인 `release_profile_unsynced` 지속 시간을 측정하지는 못했다.

Profile controller race 3회와 `cmd/narumq` package race 1회도 통과했다. 원본 출력은 `.artifacts/verification/flaky-cluster-{profile-before,profile-after,profile-race,driver-race,package-race,membership-process}.log`, 명령별 결과는 `flaky-cluster-results.json`에 있다. Local gate의 합격선·시험 선택은 바꾸지 않았다.

## 8.7 Mutation gate 범위 확장 수용 증거

대상 시나리오: PRF-SCN-100, PRF-SCN-101, PRF-SCN-103

2026-10-09, Windows/amd64·Go 1.26.5, 변경 전 revision `471bf60`의 기록이다. 당시 CI mutation에 MQTT·security를 추가했고 `variableByteIntegerSize` 경계 assertion을 보강했다. 현재 manual-only 정책은 [스펙 §7.2](../specs/performance-and-verification.md#72-fuzz-게이트와-수동-mutation-검증), 실행 결과는 [§8.8](#88-수동-mutation-검증-수용-증거)을 따른다.

다섯 package의 순차 mutation 결과는 killed 697, lived 0, not covered 6, timed out 0이었다. MQTT만 mutant coverage 98.85%, 나머지는 100%였고 efficacy는 모두 100%였다. MQTT의 not covered 6개는 `framing.go`의 expression switch 경계 mutant이며, 원인 설명은 coverage block 표현에 근거한 추정이다.

임시 사본의 `<= 128` mutant에 `go test ./internal/domain/mqtt -run '^TestRemainingLengthFramingHelperEdges' -count=1`을 실행했다. 기존 assertion은 통과했지만 보강 assertion은 `variableByteIntegerSize(128) = 1, want 2`로 실패해 검출력 차이를 확인했다. Production source는 바꾸지 않았다. 증거는 `.artifacts/verification/mutation-scope-manual-{strengthened,original}.log`에 있다.

순차 JSON은 `.artifacts/verification/mutation-scope-{delivery,mqtt,nodedrain,security,session}.json`, 당시 command 결과는 `mutation-scope-results.json`, 로컬 CI runner 출력은 `mutation-scope-ci.log`에 있다. `mutation-<package>.json`은 현재 수동 명령이 덮어쓰므로 과거 CI 보존본으로 취급하지 않는다. 이 실행은 원격 필수 status 증거가 아니다.

## 8.8 수동 mutation 검증 수용 증거

대상 시나리오: PRF-SCN-101, PRF-SCN-102, PRF-SCN-103

2026-10-11, Windows/amd64·Go 1.26.5에서 `go run ./tools/verify mutation`은 exit 0, `mutation: PASS`였다. Package별 Gremlins `workers=1`, `test-cpu=1`로 실행했다.

| Package | Killed | Lived | Not covered | Timed out | Efficacy | Mutant coverage |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `internal/domain/delivery` | 26 | 0 | 0 | 1 | 100% | 100% |
| `internal/domain/mqtt` | 515 | 0 | 6 | 1 | 100% | 98.85% |
| `internal/domain/nodedrain` | 63 | 0 | 0 | 1 | 100% | 100% |
| `internal/domain/security` | 90 | 0 | 0 | 2 | 100% | 100% |
| `internal/domain/session` | 56 | 0 | 0 | 1 | 100% | 100% |

전체 750 killed·0 lived·6 not covered·6 timed out이며 timeout은 kill로 세지 않는다. Not viable·skipped는 0개였다. 결과 `.artifacts/verification/mutation-<package>.json`은 다음 수동 실행 때 덮어쓴다.

`TestRunMutationExecutesAllPackagesWithOneGremlinsWorker`, `TestRunLocalOmitsMutationChecksAndCoverageWithoutBase`, `TestRunCIRequiresBaseAndPropagatesBaseToCoverage`와 로컬 `go run ./tools/verify ci --base HEAD`로 수동 package 선택·자동 gate의 mutation 제외를 확인했다. 이 명령은 당시 `main` merge base인 `HEAD` 기준이며 원격 CI 실행이 아니다. 과거 base `23005e1a0d819a357507ce173b8d0f89cf40cf19` 실행의 diff line 88.74%·branch 82.41% 미달은 비교 범위가 달라 별도 실패로 남는다.


## 8.9 GitHub Actions 원격 CI와 required check 증거

대상 시나리오: PRF-SCN-104~PRF-SCN-109

| 실행 | Commit·환경 | 확인 결과 |
| --- | --- | --- |
| Remote main | `03c84f40c345195f0a323c5af2e623e9ebbbc998`; GitHub Actions | [Run 38112386284](https://github.com/kayden-kim/narumq/actions/runs/38112386284)의 `verify=success`. Artifact `verification-38112386284-1`의 보관 기한은 2026-11-10이다. |
| Full Linux CI 사전 실행 | 같은 commit; Go 1.26.5 Docker container | `go run ./tools/verify ci --base f847a491e7b4a68d7fc48a868ad801895894d32a`가 `coverage: PASS`, `ci: PASS`. Diff line 96%·function 100%·branch 91.67%. 로그: `.artifacts/verification/ci-linux-one-shot-admission-close.log` |
| [PR #9 run 39036288101](https://github.com/kayden-kim/narumq/actions/runs/39036288101) | `c8d9b06d9d103596c3f322522dd516083a3a13a3`; GitHub Actions `verify`, `ci --base origin/main` | **실패.** `Run CI gate`의 coverage에서 `diff branch coverage 70.11% is below 90.00%`, `diff line coverage 80.08% is below 90.00%`로 중단했다. 전체 line 86.19%·function 96.79%·branch 81.93%, diff function 90.64%. 시나리오 634·통과 512·실패 0·범위 안 미실행 0·게이트 밖 122. Python 659 OK. Combine·Upload 성공. Artifact `verification-39036288101-1`. 로그: `.artifacts/verification/remote-ci-20261017-39036288101/` |

마지막 workflow 검증은 `ubuntu-24.04`·Node 24 기반 `actions/*@v7` 설정이다. 문서 PR [#2](https://github.com/kayden-kim/narumq/pull/2)의 [run 38113714789](https://github.com/kayden-kim/narumq/actions/runs/38113714789)에서 PASS했고 Node 20·Ubuntu 26 전환 경고가 없었다. Artifact `verification-38113714789-1`은 30일 보관한다.

초기 원격 실행에서 서로 다른 세 회귀가 발견됐다. 테스트를 끄거나 대기를 늘리지 않고 연결 종료와 inbound slot 반환 경계를 수정했다.

| Run | 실패 관찰 | 수정 |
| --- | --- | --- |
| [38107031157](https://github.com/kayden-kim/narumq/actions/runs/38107031157) | BP-05 reconnect 시험에서 두 시도 중 하나만 성공했다. | `f847a49`에서 client가 peer close를 확인한 뒤 reconnect하도록 했다. `go test ./tests/benchmark/bp05 -count=25`가 통과했다. |
| [38108358932](https://github.com/kayden-kim/narumq/actions/runs/38108358932) | Bootstrap probe의 후속 TLS handshake가 `EOF`로 실패했다. [추론] 직전 응답 뒤 client가 바로 반환하면서 server의 inbound slot 반환과 다음 연결이 경합했다. | `f7b24da`에서 client가 peer close를 기다리고, server는 slot을 반환한 뒤 connection을 닫도록 했다. 관련 시험을 race detector 아래 100회 반복해 통과했다. |
| [38111263856](https://github.com/kayden-kim/narumq/actions/runs/38111263856) | `TestDynamicSeedAdmissionRequiresTLSBoundCommittedStage`의 순차 seed admission 요청이 연결되지 못했다. | `03c84f4`에서 `ProbeBootstrapPeer`와 `RequestSeedAdmission`이 공통 close helper로 peer 종료를 기다리게 했다. Server는 close 전에 inbound slot을 반환한다. 해당 시험은 Linux race detector 아래 50회, Windows 아래 10회 통과했다. |

### Main branch protection

`main` 보호 설정은 GitHub Branch Protection API로 적용한 뒤 다시 조회했다.

| 설정 | 조회 결과 |
| --- | --- |
| 필수 status check | `verify` |
| Strict status check | `true`; 최신 `main` 반영 필요 |
| Required review | 없음 |
| Admin enforcement | `true` (2026-10-11 [ADR-0056](../decisions/ADR-0056-branch-protection-for-admins.md) 적용); 관리자에게도 동일하게 보호 적용 |
| Force push·branch deletion | 둘 다 금지 |
| Push restriction | 없음 |

### 실패 PR probe (초기 관찰)

초기 PR [#1](https://github.com/kayden-kim/narumq/pull/1)·[run 38112802413](https://github.com/kayden-kim/narumq/actions/runs/38112802413)은 의도적 test 실패로 `verify=failure`·`mergeable_state=blocked`를 확인했다. 관리자 merge 차단의 최종 증거는 아래 네 probe다.

### TEST-10 merge 거절 probe

2026-10-11에 [ADR-0056](../decisions/ADR-0056-branch-protection-for-admins.md)에 따라 `enforce_admins=true`를 영구 설정하고, PRD TEST-10의 네 조건(test 실패, coverage 저하, 미매핑 P0, 미매핑 normative statement)마다 별도 probe PR을 열어 CI 실패와 관리자 merge API 거절(HTTP 405)을 검증했다.

| Probe | PR·실행 증거 | 거절 원인 |
| --- | --- | --- |
| `test-failure` | [#3](https://github.com/kayden-kim/narumq/pull/3)·[run 38125505236](https://github.com/kayden-kim/narumq/actions/runs/38125505236) | `TestBranchProtectionProbe` 의도적 실패 |
| `coverage-drop` | [#4](https://github.com/kayden-kim/narumq/pull/4)·[run 38125509535](https://github.com/kayden-kim/narumq/actions/runs/38125509535) | 테스트 없는 함수 주입으로 function ratchet·diff line/function 미달 |
| `unmapped-p0` | [#5](https://github.com/kayden-kim/narumq/pull/5)·[run 38125513065](https://github.com/kayden-kim/narumq/actions/runs/38125513065) | `API-99`의 시험·예외 없음 |
| `unmapped-mqtt-statement` | [#6](https://github.com/kayden-kim/narumq/pull/6)·[run 38125516594](https://github.com/kayden-kim/narumq/actions/runs/38125516594) | `MQTT-4.9.0-1`의 빈 tests 배열 |

모두 `mergeable_state=blocked`, 관리자 merge API HTTP 405(`Required status check "verify" is failing.`)였다. 호출 뒤 `merged=false`, `origin/main` SHA `4c1ab4cc143426411cb5ea9ad1c0d2b501b2b5d4` 불변을 확인했다. 네 PR·remote branch는 정리했고 probe 변경은 `main`에 남지 않았다.

이 결과는 merge 차단과 TEST-06~TEST-09·TEST-13~TEST-15의 원격 CI 실행 증거다. 허용된 P0 예외가 남아 있어 [ADR-0042](../decisions/ADR-0042-p0-requirement-traceability-gate.md) §1.6에 따라 TEST-10은 **미검증**이다. Traceability PASS는 예외 목록·시험 연결의 일치이지 P0 전체 수용이 아니며 production workload·release gate도 별도다.

## 10. 시나리오 연결 보완 (2026-10-17)

이번 변경은 readiness 진단과 검증 CLI의 회귀 시험을 추가했다. Production 동작과 수용 기준은 바꾸지 않았다.

| 시나리오 | 연결한 시험·하네스 | 이번 실행 |
| --- | --- | --- |
| PRF-SCN-017 | `cmd/narumq/cluster_readiness_diagnostics_test.go`의 `TestLogReadinessFailureSamplesRepeatedRejectionsAndOmitsSensitiveFields` | PASS. 최초 출력, 1초 표본 간격, 누적 건수와 민감한 필드 제외를 확인했다. |
| PRF-SCN-068 | `tools/verify/traceability_cli_test.go`의 `TestRunTraceabilityRejectsUnmappedAndStaleP0Exceptions` | PASS. 임시 저장소의 `UI-01` 예외 제거와 `API-01` stale 예외를 CLI가 exit 1로 거절했다. |
| PRF-SCN-069, PRF-SCN-096 | 같은 파일의 `TestRunTraceabilityPassesAtRepositoryRoot` | PASS. 실제 저장소 root에서 CLI exit 0과 P0 연결 수·예외 수의 합을 확인했다. |
| PRF-SCN-083 | `tools/verify/branch_test.go`의 `TestBranchCoverageMatchesGoCoverBlocks` | PASS. 원본과 계측 사본에서 얻은 line·function total이 같았다. |
| PRF-SCN-084 | `tools/verify/coverage_cli_test.go`의 `TestRunCoverageRejectsRoutingBranchRatchetAboveMeasured` | PASS. 저장소 밖 policy의 routing branch ratchet만 높였고 CLI가 exit 1로 거절했다. 현재 오류 문구에는 ratchet 허용폭도 포함된다. |
| PRF-SCN-085, PRF-SCN-094 | 같은 파일의 `TestRunCoverageRejectsHistoricalDiffBelowFloor` | PASS(당시 하한 90%). 임시 Git 저장소의 이전 commit을 `--base`로 지정했다. CLI가 diff branch·line 0%를 각각 policy의 diff 하한 미달로 보고했다. 현재 시험은 policy 값을 읽는다. |
| PRF-SCN-090 | 같은 파일의 `TestRunCoverageAcceptsDiffAtOrAboveFloorWithMeasuredBranches` | PASS. 실제 policy의 diff floor를 사용하는 임시 Git 저장소에서 CLI exit 0, 세 diff 지표, `passed=true`, branch `measured`와 빈 shortfall을 확인했다. |
| PRF-SCN-062 | `tests/benchmark/capacity/local.go`의 `localCommand` | 메타 정보만 연결했다. 이번 작업에서 Docker 하네스를 실행하지 않았다. |
| PRF-SCN-063, PRF-SCN-064 | `tests/benchmark/capacity/eks/controller.py`의 `run_command` | 메타 정보만 연결했다. 이번 작업에서 EKS 하네스를 실행하지 않았다. 앞의 invalid 실행과 미채택 `B_*` 기록을 유지한다. |

Windows/amd64, Go 1.26.5에서 다음 명령을 실행했다.

```text
GOMAXPROCS=2 go test -p=2 -count=1 ./cmd/narumq -run TestLogReadinessFailureSamples
GOMAXPROCS=2 go test -p=2 -count=1 ./tools/verify ./tests/benchmark/capacity
```

첫 명령은 PASS였다. 두 번째 명령도 두 package 모두 PASS였다. 전체 local gate, mutation과 원격 CI는 실행하지 않았다. PRF-SCN-095에 적힌 package 전체 실행은 이번에 통과했지만, 그 실행 절차 자체를 별도 시험에 연결하지는 않았다.

PRF-SCN-056·057·059의 Docker 실행과 PRF-SCN-060·061의 EKS 사전 확인은 결과 artifact만 남아 있다. 해당 조건과 모든 기대 결과를 함께 판정한 재실행 하네스는 찾지 못했다. 기존 controller의 개별 수집·준비 함수나 단위 시험을 전체 수용 시험으로 대신 연결하지 않았다.

PRF-SCN-070·082·089는 전체 gate 실행·기준선 확정 절차다. PRF-SCN-104~109는 실제 GitHub Actions·보호 설정·merge API 결과가 필요하다. 이 절차를 실제로 판정하는 추적 중인 시험이 없어 미연결로 유지한다. PRF-SCN-101의 80% 기준은 `tests/verification/gremlins.yaml`에 있지만 판정은 외부 Gremlins에 위임한다. `tools/verify`에는 efficacy·mutant coverage·`TIMED OUT` 분류를 직접 판정하는 코드나 시험이 없어 명령 구성 시험만 연결하지 않았다.

## 11. 시나리오 보고서 통합 확인 (2026-10-17)

대상은 `ba5a9f5` 이후 작업 트리다. 범위는 스펙의 시나리오 ID·테스트 주석 전환, `tools/verify scenarios`, Go JSON 저장, Python 결과 기록과 관련 문서다. 환경은 Windows x64, Go 1.26.5다.

연결 검사는 `GOMAXPROCS=2 go run ./tools/verify scenarios`로 실행했다. 결과는 **실패**다. 시나리오 623개 중 586개가 연결됐고 37개는 미연결이다. 오류는 0건이다. 결과 입력 없는 검사이므로 586개는 모두 미실행이며, 통과 수로 쓰지 않는다.

| 미연결 스펙 | 시나리오 ID |
| --- | --- |
| Cluster | CLU-SCN-046, 047, 050, 136, 137, 159, 210~215, 217 |
| Drain | DRN-SCN-019, 020, 022, 030~033 |
| Management | MGT-SCN-043 |
| Performance | PRF-SCN-056, 057, 059~061, 070, 082, 089, 095, 101, 104~109 |

이 목록은 예외 허용 목록이 아니다. 미연결 0건·오류 0건·대상 시험 통과 기준을 유지한다. 라이브 환경이 없는 항목, 추적 하네스가 없는 과거 실행, 미구현 release gate와 운영자 판단 절차를 각 검증 기록에 구분해 뒀다. PRD의 관련 시나리오 색인도 연결 검토가 끝나지 않은 항목이 남아 있다.

### 실제 결과 입력 smoke

`GOMAXPROCS=2 go test -p=2 -count=1 -json ./tools/verify -run '^TestScenariosCommandExitCodeAndReportFiles$'` 하나만 실행해 통과했고 결과를 `.artifacts/verification/scenarios/tool-smoke-go-test.json`에 저장했다. 별도 `observed-failure-results.jsonl`은 앞서 실제 실패한 `narumq/tests/integration.TestN1PersistentClose`를 입력한 것이다. `CLU-SCN-241`의 Windows WAL 교체 `Access is denied.`·quorum 손실 실패는 [Cluster 검증 기록](cluster-session-and-delivery.md)에 보존하며 재실행하거나 완화하지 않았다.

### Python 결과 통합 smoke

```text
PYTHONDONTWRITEBYTECODE=1 NARUMQ_SCENARIO_RESULTS=.artifacts/verification/scenarios/python-ci-smoke-results.jsonl python tests/run_scenario_unittest.py tests/test_backup_s3.py tests/test_eks_worker_recovery.py tests/test_eks_capacity_controller.py tests/test_scenario_results.py tests/test_run_scenario_unittest.py
```

241개 중 **240개 통과·1개 건너뜀**, exit 0이었다. Windows symlink 권한에 의존하는 `test_stage_symlinks_never_follow_or_delete_external_data`를 건너뛰었다(`CLU-SCN-051`). 실제 AWS·EKS를 호출하지 않는 unittest다.

두 smoke 결과와 기존 실패 입력을 병합했다.

```text
GOMAXPROCS=2 go run ./tools/verify scenarios --go-test-json .artifacts/verification/scenarios/tool-smoke-go-test.json --results .artifacts/verification/scenarios/observed-failure-results.jsonl --results .artifacts/verification/scenarios/python-ci-smoke-results.jsonl --require-executed --output .artifacts/verification/scenarios/execution-smoke
```

보고서는 **실패**였다: 통과 4개·실패 1개·미실행 581개(범위 안 33·밖 548)·미연결 37개·오류 0건. 건너뜀과 `CLU-SCN-241` 실패를 그대로 반영한 결과 수집·병합 smoke이며 전체 시나리오 통과 증거가 아니다.

### Go preflight 실패 뒤 Python-only 보고 분기

Go preflight 실패 뒤에도 Python JSONL만으로 새 보고서를 만드는 경로를 확인했다. Workflow의 실제 Bash 본문에서 입력·출력 경로만 바꿔 `GOMAXPROCS=2 "C:/Program Files/Git/bin/bash.exe" <분기별 smoke script>`로 실행했다. Go-only·Python-only·두 입력은 모두 미연결을 유지해 exit 1, 입력 없음은 `No Go or Python execution results were produced.`와 exit 1로 보고서를 생성하지 않았다. 결과는 `.artifacts/verification/scenarios/ci-branch-{python-only,go-only,combined}/`에 있다. Import 전 파일 초기화·import/class setup 오류의 이전 PASS 제거는 `PYTHONDONTWRITEBYTECODE=1 python tests/test_run_scenario_unittest.py` 4개 통과로 확인했다.

실제 결과 입력 보고서의 4통과·1실패와 분기 보고서의 3통과·0실패 차이는 후자가 `observed-failure-results.jsonl`을 넣지 않았기 때문이며, `CLU-SCN-241` 실패 해제를 뜻하지 않는다.

통합 확인에서 `go vet ./...`, `go run ./tools/verify traceability`(P0 73/81 연결·예외 8), `go run ./tools/verify architecture`, `GOMAXPROCS=2 go test -p=2 -count=1 ./tools/verify/`는 통과했다. **전체 local·원격 CI·라이브 EKS는 미실행**이며 기존 전체 local coverage 실패도 남아 있다.


## 12. Gate 실행 경로·판정 보완 (2026-10-17)

대상은 `b13d8254754bdb5333ae28d47e25c9d51cd85590` 기반 미커밋 작업 트리다. 환경은 Windows/amd64·Go 1.26.5·`GOMAXPROCS=4`, Linux container를 실행하는 Docker다. 외부 하네스의 fake fixture 단위 시험을 실제 EKS 수용에 연결하지 않았다. Coverage floor·ratchet, fuzz 횟수, rolling 100회와 제품의 timeout·부하 판정은 그대로다.

### 12.1 연결과 집중 실행 증거

| 명령·범위 | 실제 결과 |
| --- | --- |
| `go run ./tools/verify scenarios --output .artifacts/verification/gate-completion-20261017/linkage` | PASS. 623개 연결·미연결 0·오류 0. 결과 입력 없는 연결 검사이므로 실행 통과는 0이다. |
| `go run ./tools/verify package-tests --results .artifacts/verification/gate-completion-20261017/go-acceptance-results.jsonl` | PASS. 실제 `go test ./tools/verify -count=1` 실행과 수동 결과 기록을 확인했다. |
| `NARUMQ_ACCEPT_DOCKER=1 python tests/run_scenario_unittest.py tests/acceptance_docker_capacity.py` | PASS. 3개 시험, 실제 broker STOP·CONT·TERM, 원본 process 복구, cgroup CPU throttling과 OOM exit 137. Kubernetes API만 대역이며 실제 EKS 삭제 지연의 증거는 아니다. |
| Workflow와 같은 전체 Python 목록, Python 3.9 Linux container | 최종 659개 모두 PASS·건너뜀 0. 첫 실행의 timestamp 실패·수정과 실제 Kind admission은 [Drain §6](drain-and-rolling-update.md#6-gate-연결과-release-profile-경로-보완-2026-10-17)에 기록했다. |
| 모든 process opt-in을 켠 `go test -p=2 ./tests/integration -count=1 -json -timeout=45m` | FAIL. 45분의 전체 명령 제한에 도달했다. 최상위 41개 통과 뒤 다음 시험을 시작하던 때였고 개별 assertion 실패는 없었다. 100회 rolling은 1793.94초에 통과했다. 전체 integration PASS로 표시하지 않는다. |
| `go run ./tools/verify mutation` | PASS. `GOMAXPROCS=4`에서 5개 패키지 순차 배치 실행, 4303.942초 소요. delivery 100%/100%, mqtt 100%/98.81%, nodedrain 100%/100%, security 100%/100%, session 100%/100%로 모든 패키지가 80% 하한을 통과했다. `TIMED OUT` 변이체는 killed로 산정하지 않았다. [실행 증거](../../.artifacts/verification/gate-completion-20261017/mutation-final-summary.json)를 따른다. 첫 시도의 3000초 timeout 중단 기록은 [보존 기록](../../.artifacts/verification/gate-completion-20261017/mutation-initial-timeout.json)에 둔다. |
| `go run ./tools/verify baseline`, 첫 실행 | FAIL. 첫 local 실행의 integration package가 3시간 제한에 걸렸다. 실패한 assertion은 없었다. `TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance`가 `git archive` 종료를 174분 기다렸다. Coverage 명령은 branch 계측 사본(`.artifacts/verification/branch/module`)에서 실행된다. 하위 directory에서 실행한 `git archive`는 그 경로 prefix 아래만 archive한다. Historical revision에는 이 경로가 없어 빈 tar가 나왔다. Helper가 빈 tar를 읽고 남은 stdout을 비우지 않은 채 `Wait`한 점도 관찰했다. 다만 이것이 174분 대기의 직접 원인이라는 재현은 하지 않았다. 시험 helper가 `git rev-parse --show-toplevel`로 실제 저장소 root에서 archive하도록 고쳤다. 대상 directory를 먼저 만들고, 남은 stdout을 비운 뒤 pipe를 닫고 `Wait`하도록 했다. 수정 뒤 두 historical release 시험을 함께 실행해 통과했다. |
| 라이브 opt-in을 켠 `go run ./tools/verify local` 단일 실행 | exit 0, 1118.51초. Line 86.15%, branch 81.89%였다. 시나리오 실행 검사는 통과 536·미실행 0(게이트 밖 87)이었다. 단, `NARUMQ_ROLLING_ROUNDS=2` dry run을 섞었으므로 `DRN-SCN-018` 통과와 전체 gate 수용 증거로 쓰지 않는다. |
| `go run ./tools/verify baseline`, 수정 후 재실행 | PASS, 9285.672초. `NARUMQ_ROLLING_ROUNDS`를 subprocess 환경에서 제거하고 모든 local opt-in(Kind rollout, release compat, durable ACK fault, WAL lifecycle, 100회 rolling, dashboard)을 켰다. 세 local 실행 모두 `local: PASS`이고 overall branch는 13603·13603·13597/16612(81.88%·81.88%·81.85%)다. Baseline은 overall 81.88%이며 core 9개는 connrebalance 96.15, delivery 100, mqtt 99.27, nodedrain 97.72, release 100, routing 94.23, security 96.94, session 98.73, tenancy 99.33%다. Policy는 바꾸지 않았다. nodedrain 측정값 97.72%는 ratchet 97.73%보다 0.01%p 낮다. 이 차이는 [ADR-0041](../decisions/ADR-0041-branch-ratchet-noise-tolerance.md)의 0.20%p 허용폭 안이라 통과한다. 세 실행의 log는 모두 시나리오 실행 검사 통과 536·실패 0·범위 안 미실행 0·게이트 밖 87을 보인다. `go-test.json`은 실행마다 덮어쓰므로 시험별 원문은 마지막 실행 것만 남았다. 마지막 실행에서 2164개 최상위 시험이 모두 통과했다. `DRN-SCN-018`은 100 round(QoS 1·2 각 1200건, reorder 0, `unexpected_supported_client`·`unplanned_failure` 0)로 1707.05초에 통과했다. 1·2회차의 round 수는 같은 환경 설정으로만 추정하며 원문은 없다. [결과](../../.artifacts/verification/gate-completion-20261017/baseline-result.json), [baseline](../../.artifacts/verification/gate-completion-20261017/branch-baseline.json)과 `baseline-local-1~3.log`를 따른다. |
| Opt-in 수용 분리 뒤 `go run ./tools/verify local`, `NARUMQ_*` 환경 변수 없음 | PASS, 546초(이전 opt-in 포함 1회 약 3095초). Line 86.17%, branch 13605/16612(81.90%)로 이전 baseline 81.88%보다 낮지 않다. Process 시험은 별도 binary를 실행하므로 coverage에 기여하지 않는다. 시나리오 실행 검사는 625개 중 통과 506·실패 0·범위 안 미실행 0·게이트 밖 119다. `Acceptance:` 표시 시험 11개에만 연결된 시나리오가 게이트 밖으로 옮겨졌다. 같은 날 `go run ./tools/verify acceptance`는 환경 변수 6개가 없을 때 시험을 시작하지 않고 exit 1이었다. `NARUMQ_WAL_LIFECYCLE=1 go run ./tools/verify acceptance --run WALCompaction`은 1개 시험 PASS였다. `NARUMQ_ROLLING_100=1 NARUMQ_ROLLING_ROUNDS=2 go run ./tools/verify acceptance --run ContinuousRolling`은 2 round 검사를 마친 뒤 시험이 `SKIP`(44.28초)으로 보고되어 `(건너뜀)`과 exit 1이었다. 나머지 opt-in 시험은 이번 분리 뒤 `acceptance`로 재실행하지 않았다. 이전 통과 증거는 위 baseline 행이다. |

집중 실행의 exit code·명령·원문은 [증거 디렉터리](../../.artifacts/verification/gate-completion-20261017/)에 있다. `integration-result.json`과 `integration.jsonl`은 45분 제한 실패를 보존한다. 전체 gate의 coverage 명령은 기존 3시간 fixture를 수용하도록 `-timeout=3h`를 사용한다. 짧은 진단 실행의 제한을 제품이나 개별 시나리오의 deadline으로 해석하지 않는다.

### 12.2 실행 경로와 미실행 경계

`tools/verify baseline`은 실제 `local`을 세 번 실행하고 새 schema 2 summary를 요구한다. 첫 두 branch 측정값 중 작은 값을 소수 둘째 자리까지 내림한 baseline 후보만 기록한다. Policy는 자동 갱신하지 않는다. `tools/verify package-tests`도 실제 명령의 결과만 기존 JSONL 형식으로 기록한다.

`runMutation`은 Gremlins 결과 다섯 개를 `verifyMutationReports`로 직접 판정한다. `TIMED OUT`은 killed나 lived로 세지 않는다. 두 지표의 80% 하한은 기존 `gremlins.yaml`에서 읽는다. 병렬 runner는 OS가 종료 코드만 반환한 경우에도 context 취소 원인을 보존한다. 실제 하위 process의 deadline 취소와 exit 23을 분리한 회귀가 검증 도구 package 시험에 포함된다.

PRF-SCN-060·061은 `tests/acceptance_eks_capacity.py`, PRF-SCN-104~109는 `tests/acceptance_github_ci.py`에 실제 opt-in 경로를 추가했으나 이번 작업에서 외부 수용은 미실행이다. GitHub probe는 명시적 환경 변수·origin 일치·관리자 권한·보호 설정 확인을 요구한다. 실패 probe의 merge가 실제 허용되면 `main`을 바꿀 수 있으므로 자동 실행하지 않는다. 기존 원격 CI 기록을 이번 미커밋 코드의 통과로 재사용하지 않는다.

## 13. Go 플랫폼별 시나리오 실행 범위 (2026-10-17)

대상은 `ecd57cef3a630e24a928071e2b0f01052935b5c1` 기반 작업 트리의 PRF-SCN-121 변경이다. 환경은 Windows/amd64·Go 1.26.5와 Docker의 `golang:1.26.5` Linux/amd64다. 이 변경은 AWS 자원을 만들지 않았다.

[PR #9 실행 39034250585](https://github.com/kayden-kim/narumq/actions/runs/39034250585)의 Go 시험은 통과했지만 시나리오 실행 검사는 CLU-SCN-153·MGT-SCN-014·SEC-SCN-050을 결과 누락으로 실패시켰다. 원문은 `.artifacts/verification/remote-ci-20261017-39034250585/failed-job.log` 229–234행이다. `gh run download 39034250585 --name verification-39034250585-1 --dir .artifacts/verification/scenario-platform-scope/remote-artifact`로 별도 디렉터리에 원본 결과를 받았다.

소스 연결은 모든 플랫폼을 계속 읽는다. 실행 범위만 module별 `go list -e -find -json`의 명시적 `IgnoredGoFiles`로 좁힌다. 누락된 package 정보나 package 오류를 전체 테스트 면제로 바꾸지 않는다. `-find`로 의존 package 탐색도 피한다. 새 메타 정보로 플랫폼을 중복 정의하지 않았다.

첫 Linux replay는 일반 파일의 `TestClusterPathsOverlapAllowsDifferentWindowsVolumes`가 `runtime.GOOS`로 건너뛰는 문제도 그대로 실패시켰다. 이 시험은 `cluster_config_windows_test.go`로 옮기고 런타임 건너뜀을 제거했다. 결과 누락을 숨기는 예외는 추가하지 않았다. 첫 replay의 나머지 미실행은 이전 CI 결과에 없는 새 PRF-SCN-120 시험이었다. 해당 두 시험의 새 실제 실행 결과를 별도 입력했다.

| 명령·범위 | 실제 결과 |
| --- | --- |
| `go test ./tools/verify -count=1` | PASS, 최종 실행 44.241초. Windows·Linux, arm64, GOFLAGS tag, 활성 파일의 누락·건너뜀, 제외 파일의 명시적 pass·fail, 제외된 잘못된 메타 정보, 의존성을 받을 수 없는 nested module의 연결 검사와 package 오류의 fail-closed 동작을 확인했다. |
| `go vet ./tools/verify` | PASS. |
| `go run ./tools/verify markdown-links` | PASS, 파일 190개·로컬 링크 3190개. |
| `go run ./tools/verify scenarios --output .artifacts/verification/scenario-platform-scope/links` | PASS, 시나리오 634개·미연결 0·오류 0. 결과 입력 없는 연결 검사다. |
| `go test -json -count=1 ./tools/verify ./cmd/narumq ./internal/adapters/raft ./internal/adapters/auditlog` | Windows PASS. `windows-go-test.json`에 보존했다. 원래 지적된 Windows 시험 4개 모두 실제 pass 이벤트가 있다. |
| `go test ./cmd/narumq -run TestClusterPathsOverlapAllowsDifferentWindowsVolumes -count=1` | Windows 파일 이동 뒤 PASS, 1.999초. |
| Docker Linux에서 `go test -json -count=1 ./tools/verify` | PASS, 9.393초. `linux-go-test.json`에 보존했다. |
| Docker Linux에서 `go test -json -count=1 -run 'TestEndpointsOnlyFallThroughBeforeConnect\|TestConnectDialErrorPreservesCancellation' ./tests/benchmark/capacity ./tests/benchmark/internal/mqttclient` | PASS. `linux-new-quota-results.json`에 보존했다. 과거 CI에 없던 새 연결의 결과만 보충한다. |
| 아래 Windows·Linux 실제 `scenarios` 명령 | 모두 PASS. Linux 통과 523·실패 0·범위 안 미실행 0·게이트 밖 111. Windows 통과 526·실패 0·범위 안 미실행 0·게이트 밖 108. 두 보고서 모두 시나리오 634·미연결 0·오류 0이다. |

실행 명령은 저장소 루트에서 다음과 같다. Linux 명령은 `docker run --rm -v D:/Workspace/narumq:/workspace -w /workspace golang:1.26.5` 안에서 실행했다. 아래 `ROOT`는 `.artifacts/verification/scenario-platform-scope`다.

```sh
go run ./tools/verify scenarios --go-test-json ROOT/remote-artifact/go-test.json --go-test-json ROOT/linux-go-test.json --go-test-json ROOT/linux-new-quota-results.json --results ROOT/remote-artifact/scenarios/python-ci-results.jsonl --require-executed --output ROOT/linux-final-report
go run ./tools/verify scenarios --go-test-json ROOT/remote-artifact/go-test.json --go-test-json ROOT/windows-go-test.json --go-test-json ROOT/linux-new-quota-results.json --results ROOT/remote-artifact/scenarios/python-ci-results.jsonl --require-executed --output ROOT/windows-final-report
```

원본과 새 결과, 첫 실패와 최종 보고서는 [증거 디렉터리](../../.artifacts/verification/scenario-platform-scope/)에 있다. 이는 원격 CI 결과에 새 집중 시험을 합친 기능 smoke다. 전체 원격 gate를 다시 통과했다는 주장은 아니다. 보고 도구는 현재 Go 환경으로 누락 결과의 실행 범위를 판정한다. 외부 JSON에는 원래 빌드 환경이 없으므로 원래 GOOS·GOARCH·CGO와 GOFLAGS를 재현해야 한다. 여러 플랫폼 결과를 합치면 명시적 pass·fail은 유지하지만 누락 결과를 플랫폼별로 추론하지 않는다. 일반 활성 파일의 건너뜀은 계속 실패다.

후속 검토에서 같은 package·함수 이름을 Windows·Linux 파일에 각각 선언하면 마지막 파일이 활성 선언을 덮는 경계를 확인했다. 결과 키를 합칠 때 활성 선언을 우선하도록 고쳤다. `TestScenarioAlternatePlatformDeclarationsKeepActiveKeyInScope`는 두 플랫폼의 결과 누락과 일반 건너뜀을 각각 입력해 실행 검사 실패, 범위 안 미실행 1개와 활성 파일 경로를 확인한다. `go test ./tools/verify -run "TestScenario" -count=1`은 3.478초에 PASS했고 `go vet ./tools/verify`도 PASS했다. 이 후속 변경 뒤 전체 원격 CI는 아래 실행 39036288101에서 수행했다.

[PR #9 실행 39036288101](https://github.com/kayden-kim/narumq/actions/runs/39036288101)은 `c8d9b06d9d103596c3f322522dd516083a3a13a3`에서 `go run ./tools/verify ci --base origin/main`의 coverage gate가 실패했다. Diff branch는 70.11%(1520/2168), diff line은 80.08%(3968/4955)로 90.00% 하한에 미달했다. Diff function은 90.64%(368/406)였다. 전체 coverage는 line 86.19%(37405/43399), function 96.79%(2138/2209), branch 81.93%(13639/16647)였다. 같은 실행의 시나리오 실행 검사는 634개 중 통과 512·실패 0·범위 안 미실행 0·게이트 밖 122로 통과했고, Python 659개는 OK였다. 앞선 실행 39034250585의 시나리오 누락 실패는 재발하지 않았다. 원본 로그와 `coverage-summary.json`은 [증거 디렉터리](../../.artifacts/verification/remote-ci-20261017-39036288101/)에 있다. 이 실행은 diff coverage 미달로 전체 원격 gate 통과 증거가 아니다.

