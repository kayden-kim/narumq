# ADR-0016: Broker node drain and SAFE_TO_STOP

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (broker drain/`SAFE_TO_STOP` scope only; external rollout·supported-SDK/Will·production storage·BP-07·N-1 제외) |
| 결정일 | 2026-09-28 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD DEP-01~DEP-06·DEP-07 (broker close scope only; Will/client compatibility acceptance excluded)·DEP-08~DEP-10·DEP-12](../requirements/PRD.md) |
| 관련 스펙·데이터 계약 | [Drain과 rolling update §1~§6·§10](../specs/drain-and-rolling-update.md), [Management API v1](../contracts/management-api-v1.md), [replicated-state-v1.proto](../contracts/replicated-state-v1.proto), [Cluster backup v3](../contracts/cluster-backup-v3.md), [MQTT client compatibility v1](../contracts/mqtt-client-compatibility-v1.md) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md), [Project structure](../architectures/project-structure.md), [Broker core boundaries](../explainers/broker-core-boundaries.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | [ADR-0028](ADR-0028-planned-handoff-will-and-reason-metric.md) (부분) |

## 1. 결정

1. `-cluster-config` dynamic cluster의 committed voter에 broker-side drain API를 제공한다. 한 cluster에서 active drain operation은 한 번에 하나다. Operation과 phase 전이는 Raft state에 commit하고, node restart 뒤 저장된 phase에서 driver를 재개한다. Standalone mode에는 이 API를 제공하지 않는다.
2. Drain start 뒤 해당 node는 신규 MQTT CONNECT admission을 닫고 readiness를 실패시킨다. 설정된 load-balancer 전파 대기 시간 동안 기존 연결을 유지한 뒤 `PlanRemovalHandoff`의 ownership transfer를 재사용해 연결 종료·session 인계를 설정된 속도로 수행한다. MQTT 5 연결에는 `DISCONNECT 0x9C`와 선택적 Server Reference를 보낸다. MQTT 3.1.1 연결은 session state를 복제한 뒤 닫는다. Ownership takeover close는 이전 연결의 Will을 억제한다.
3. `SAFE_TO_STOP`은 barrier를 모두 통과한 뒤 replicated `COMPLETE` transition이 commit된 경우에만 반환한다. Admission/readiness, 남은 connection, local ownership/subscription, unapplied WAL, quorum/connected successor, local leadership, local commit, pending Will 상태 중 하나라도 통과하지 못하면 계속 대기하거나 deadline에서 `FAILED`가 된다. `FAILED`는 node 종료를 승인하지 않는다.
4. API는 `Operator`의 POST/DELETE와 `ReadOnly`의 GET을 사용한다. POST/DELETE는 `Idempotency-Key`를 요구한다. `connectionRatePerSecond`, `loadBalancerPropagationDelayMs`, `timeoutMs`는 필수이며 각각 `1..10000`, `0..3600000`, `1000..86400000` 범위다. Timeout은 LB 대기보다 커야 한다. `serverReference`는 선택이며 UTF-8 256 bytes 이하, 제어 문자가 없어야 한다. 배포 기본값은 이 결정에서 만들지 않는다.
5. `SAFE_TO_STOP` 완료는 취소할 수 없다. 취소 가능한 operation은 replicated `CANCEL`을 거친 뒤 route와 replica readiness를 다시 확인하고서만 READY가 된다. Restart 후 남은 완료 record는 node가 READY 확인을 통과한 뒤 지운다.
6. 이 결정은 broker가 protocol redirect와 안전한 정지 상태를 제공하는 범위에 한정한다. External rollout controller와 Kubernetes/EKS 배포, 지원 SDK의 자동 Server Reference 추종, MQTT 3.1.1/Will release policy, DEP-12 reason metric, `SPEC-TBD-04` 제품 기본값, BP-07, N-1↔N 및 production EBS/power-loss 검증은 승인하지 않는다.
2026-10-10 구현 메모: G5는 duration JSON field를 `Millis` 정수로 통일했다. API의 현재 이름은 `loadBalancerPropagationDelayMillis`, `timeoutMillis`다. Accepted 결정의 기존 이름은 당시 계약을 보존한다.

## 2. 배경

Session ownership과 QoS delivery state가 Raft에 복제되어도, 운영자가 node를 계획적으로 빼기 위한 admission 차단, 기존 session 인계, 종료 안전 판정이 없으면 broker는 안전한 종료 허가를 제공할 수 없다. Drain 진행 중 node가 재시작해도 phase와 deadline을 이어가야 하며, timeout·quorum 상실을 성공으로 취급해서는 안 된다.

`docs/specs/drain-and-rolling-update.md`는 broker 동작과 external rollout gate를 함께 설명하지만 외부 controller·SDK·production deployment acceptance는 별도다. 이 ADR은 process test로 검증한 broker slice만 수용하고, 그 증거를 전체 rolling-update 지원이나 production readiness로 확대하지 않는다. `ADR-0004`는 session ownership 전체 결정이 아직 Proposed이므로 상태를 바꾸지 않는다.

## 3. 결정 요인

1. Pod/process 종료 전에 session ownership과 연결 상태를 복구 가능한 cluster state로 확정한다.
2. Quorum, successor, pending mutation 등 확인할 수 없는 상태를 성공으로 오판하지 않는다.
3. Crash/restart, cancel, timeout 동작을 결정적이고 관측 가능하게 한다.
4. 기존 ownership handoff와 session epoch fencing을 재사용하고 별도 handoff 규칙을 만들지 않는다.
5. Broker 동작의 수용과 외부 배포·SDK·storage release gates를 분리한다.

## 4. 선택 근거

Drain record를 replicated state로 두면 operation, phase, progress와 deadline이 process memory에만 머물지 않고 재시작 후 driver가 재개할 수 있다. Barrier는 connection·ownership·subscription·WAL·replica·leader·local commit·Will 상태를 함께 확인하며, blocker code를 operation failure에 연결한다. 기존 `PlanRemovalHandoff`와 committed ownership transfer를 사용하면 drain 전용 ownership mutation 경로를 추가하지 않는다.

실제 3-node process acceptance는 신규 CONNECT 거절, readiness, rate-limited takeover, MQTT 5 `0x9C`/Server Reference, `SAFE_TO_STOP`, session resume, crash/restart resume, cancel, timeout과 completed-record clear를 확인했다. 이는 외부 scheduler가 삭제를 차단하는지, 각 지원 SDK가 redirect를 따르는지, production storage 장애에서도 상태가 보존되는지를 증명하지 않는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Drain phase와 progress를 process-local memory에만 둔다. | 초기 구현이 단순하고 replicated schema 변경이 없다. | Process crash 뒤 operation과 deadline을 복구하지 못해 readiness/admission 상태가 실제 ownership과 달라질 수 있다. | Restart 뒤 재개와 durable `SAFE_TO_STOP`이 acceptance 조건이므로 부적합하다. |
| External rollout controller가 직접 연결을 끊고 node 삭제를 결정한다. | Deployment API와 node lifecycle을 한 곳에 둔다. | Controller가 session ownership, committed subscription, quorum/WAL 상태를 직접 판정해야 하며 broker API 장애가 곧 안전 판정 불능이 된다. | Broker는 안전 조건을 authoritative replicated state에서 판정하고 controller는 후속 별도 gate로 남긴다. |
| 기존 membership removal handoff만 실행하고 별도 admission/barrier 없이 node를 제거한다. | 기존 Transfer 경로를 재사용한다. | 신규 CONNECT가 유입될 수 있고 남은 connection, local mutation, WAL 및 leadership을 확인하지 않는다. | Ownership transfer 하나만으로 안전한 stop 조건이 충족되지 않는다. |
| 새로운 drain 전용 ownership planner를 추가한다. | Drain에 맞춘 정책을 독립적으로 표현할 수 있다. | Membership removal과 ownership handoff 규칙이 중복되고 두 경로의 fencing 동작이 달라질 위험이 있다. | 기존 `PlanRemovalHandoff`와 committed transfer를 재사용한다. |

## 6. 결과

### 긍정적 결과

- Drain operation과 phase transition은 replicated state에 남아 process restart 뒤 재개할 수 있다.
- 신규 session admission은 닫히고, 기존 ownership은 rate limit과 epoch fencing 아래 successor로 이동한다.
- Barrier 통과 전에는 `SAFE_TO_STOP`을 반환하지 않으며, timeout은 실패 상태로 남는다.
- Management API에서 operation phase, progress, failure blocker와 SAFE_TO_STOP 여부를 조회할 수 있다.

### 부정적 결과와 비용

- Drain phase/progress 전이마다 replicated command가 발생한다. Session transfer 속도와 LB 대기·timeout은 호출자가 지정해야 한다.
- MQTT 5 Server Reference를 내보내도 client가 자동으로 따라간다는 보장은 없다. Ownership takeover는 Will을 억제하지만 MQTT 3.1.1/Will 정책과 DEP-12 분류 metric은 아직 승인된 release contract가 아니다.
- Broker `SAFE_TO_STOP`만으로 Pod 삭제가 방지되지는 않는다. Rollout controller·`preStop`·termination grace·PDB·AZ placement가 필요하다.
- Local crash/restart 증거는 EBS/physical power-loss durability, BP-07의 반복 rolling, N-1 compatibility 또는 production profile을 검증하지 않는다.

### 후속 작업

- [ ] External rollout controller와 Kubernetes/EKS deletion gate를 별도 ADR/spec 및 process acceptance로 검증한다.
- [ ] 지원 SDK의 Server Reference 자동 추종, MQTT 3.1.1/Will policy와 DEP-12 metric을 compatibility acceptance로 검증한다.
- [ ] `SPEC-TBD-04` rate/delay/timeout 제품 값과 worst-case termination grace를 확정한다.
- [ ] BP-07, N-1↔N, Linux/EBS power-loss·restore와 production profile gate를 별도 실행한다.

## 7. 검증

| 결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Admission·ownership transfer·protocol redirect·SAFE_TO_STOP | `TestDynamicClusterNodeDrainProcessAcceptance` process test | New CONNECT 거절, 0x9C+Server Reference, rate-limited transfer, barrier 완료, session resume 및 restart 뒤 CLEAR | PASS 9.75s; [Drain spec §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) |
| Cancel·timeout·crash recovery | `TestDynamicClusterNodeDrainCancelTimeoutAndResumeProcessAcceptance` process test | Cancel 뒤 READY, crash/restart 뒤 같은 operation 재개, missing replica timeout은 `FAILED/insufficient_replicas`, cancel 후 readiness 복구 | PASS 16.03s; [Drain spec §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) |
| Domain·wire·management API·replicated record contracts | Focused unit/contract suite: `internal/domain/nodedrain`, MQTT, membership, TCP, management, raft records, metrics, OpenAPI contract, runtime | Behavior, serialization, permission, request validation과 API schema 일치 | PASS; 실행 명령과 package 목록은 [Drain spec §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) 참조 |
| Barrier rule regression sensitivity | Gremlins mutation run on `internal/domain/nodedrain` | LIVED=0, NOT_COVERED=0 | PASS: KILLED=64, LIVED=0, NOT_COVERED=0; efficacy 100%, mutant coverage 100%; 76s |
| Full local verification and NodeDrain coverage | `GOMAXPROCS=4 go run ./tools/verify local` and the generated core coverage report | Full local gate passes; NodeDrain core ratchet remains at or above its recorded threshold. | PASS 204.42s; 100/102 executable lines (98.04%), 13/13 functions (100%); [Drain spec §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) |

## 8. 재검토 조건

- External controller가 broker operation을 읽고 `SAFE_TO_STOP` 이전 삭제를 차단하는 acceptance가 추가되면 승인 범위를 넓힐지 별도 검토한다.
- Supported SDK에서 `0x9C` 추종 실패, 지원 client의 예상치 못한 Will 발행 또는 drain reconnect 중 session 손실이 관찰되면 client/Will 계약과 close 동작을 재검토한다.
- Barrier 조건, quorum profile, ownership handoff 또는 drain request bounds가 바뀌면 replicated command/API contract와 acceptance를 함께 재검토한다.
- Production storage·BP-07·N-1 결과는 이 ADR의 제한된 broker scope를 production readiness로 확대하지 않고 별도 release gate로 판단한다.
