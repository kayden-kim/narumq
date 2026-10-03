# ADR-0027: Detach 전환 forward와 연결 중 owner crash의 QoS 0 유실 경계

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-04 |
| 결정자 | NaruMQ 프로젝트 (사용자 승인) |
| 관련 요구사항 | [PRD CLU-14](../requirements/PRD.md#92-클러스터와-세션), [PRD §8](../requirements/PRD.md#8-메시지-보장-모델)의 QoS 0 best effort 원칙 |
| 관련 스펙·데이터 계약 | [Cluster §4.1·§11.26](../specs/cluster-session-and-delivery.md), [Raft peer transport v1](../contracts/raft-peer-transport-v1.md) |
| 관련 아키텍처 | [Project structure](../architectures/project-structure.md), [Cluster architecture](../architectures/cluster-architecture.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | [ADR-0026](ADR-0026-detach-transition-local-qos0-gate.md) §4 마지막 항목의 “Forward 경로와 연결 중 owner crash는 열린 수용 범위로 남긴다” 문장만 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

1. 다른 node에서 forward된 QoS 0 delivery가 detach 전환 중이거나 local queue 제거 뒤 도착하면 수신 node는 기다리지 않고 버린다. 수신 node는 그 delivery를 offline ENQUEUE로 다시 보내지 않는다. 같은 forward lane의 다른 session 처리는 막지 않는다. [ADR-0014](ADR-0014-cross-node-live-qos0-forwarding.md)의 at-most-once(최대 한 번 전달) 의미를 유지한다.
2. 연결 중 owner process가 offline DETACH commit 전에 끝나면 그 process의 local queue에 있던 QoS 0 message는 복구하지 않는다. 보호는 DETACH commit부터 시작한다. Commit 뒤 crash하면 DETACH record의 message와 그 뒤 ENQUEUE는 보존되어 재접속 때 FIFO(들어온 순서)로 전달된다.
3. `session-metadata` feature가 활성이면 owner 재시작 recovery가 pending이 빈 DETACH를 commit하여 이후 QoS 0 capture를 재개한다. Crash 전에 이미 전달한 message를 다시 전달하지 않는다. 재접속 CONNACK은 `SessionPresent=1`이다.
4. Feature 비활성 상태의 한계(Cluster §4.1 9항 첫 문장)는 바꾸지 않는다.

ADR-0026 §1 6·7항의 동작은 그대로 유지한다. 이 ADR은 그 동작을 수용 경계로 확정한다. Production storage 보장과 CLU-14 전체 완료는 선언하지 않는다.

## 2. 배경

ADR-0026은 owner-node local publish의 detach 전환만 보호했다. Forward와 연결 중 owner crash는 수용 시험이 연결되지 않은 범위로 남았다. 사용자는 QoS 0 보장 수준을 넓히지 않고 현재 경계를 유지하기로 승인했다.

Disconnect는 durable DETACH, offline DETACH, metadata DETACH 순서로 commit한다. Offline DETACH 전후에 process가 끝나면 metadata는 아직 `Connected=true`다. 재시작 recovery는 queue를 등록한 뒤 빈 offline DETACH와 metadata DETACH를 commit한다. 같은 epoch의 offline DETACH는 이미 보존된 message를 덮어쓰지 않는 no-op `Applied`다.

## 3. 결정 요인

1. MQTT QoS 0 at-most-once와 PRD best effort(최선 노력) 원칙을 유지한다.
2. Connected live publish 경로에 message당 Raft write를 추가하지 않는다([ADR-0013](ADR-0013-offline-queue-replication-and-cluster-backup-v3.md) 결정 요인 2).
3. Forward lane에서 한 session의 대기가 다른 session을 막는 head-of-line blocking을 금지한다(ADR-0026 §3).
4. 이미 전달한 message를 중복 전달하지 않는다.

## 4. 선택 근거

Forward 수신부는 committed owner가 자기 node일 때만 local sink를 호출한다. `AcceptForwarded`는 `ErrDetaching`을 `ErrUnavailable`로 바꾸며 기다리지 않는다. 수신부는 이 결과를 조용히 버린다. DETACH 뒤 ownership row가 남아 있어도 제거된 local queue에는 전달하지 않는다.

Connected message는 process-local queue에만 있으므로 commit 전 crash에서 복구할 기록이 없다. Offline DETACH commit 뒤에는 replicated record가 capture 근거가 된다. 이 경계는 기존 ADR-0013·ADR-0014와 맞으며 추가 buffer나 live publish의 Raft 기록을 요구하지 않는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Forward lane에서 DETACH 완료 대기 | 전환 중 forward 유실을 줄일 수 있다. | 같은 lane의 다른 session도 기다린다. | 결정 요인 3 위반 |
| 수신 node의 session별 지연 buffer 뒤 ENQUEUE 재전송 | Forward 유실을 줄일 수 있다. | ADR-0014 의미가 바뀐다. Ingress 쌍 사이 순서와 새 memory 상한이 필요하다. | 결정 요인 1의 현재 의미를 바꾸며 QoS 0 요구사항에 없는 보호를 추가한다. 결정 요인 4를 위한 추가 설계도 필요하다. |
| Connected message 전체 Raft 기록 | Commit된 live message도 crash 뒤 복구할 수 있다. | Message마다 Raft write가 필요하다. | 결정 요인 2 위반 |

## 6. 결과

### 긍정적 결과

- 기존 유실 경계와 recovery 동작을 package·process 수용 시험으로 고정한다.
- Forward lane의 다른 session은 detach 대기에 영향을 받지 않는다.

### 부정적 결과와 비용

- 전환 중 또는 local queue 제거 뒤 도착한 forward QoS 0은 유실될 수 있다.
- DETACH commit 전 crash의 local queue QoS 0은 복구하지 않는다.

### 후속 작업

- 없음. Production storage와 CLU-14 전체 수용은 기존 별도 범위로 남는다.

## 7. 검증

실행 증거의 기준은 [Cluster §11.26](../specs/cluster-session-and-delivery.md#1126-detach-전환-forward연결-중-owner-crash-유실-경계-수용-매핑)이다.

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| 정확한 crash 경계 | `TestClusterFailpointParse`, `TestClusterFailpointMatchesOnlyItsBoundary` | 두 값이 parse되고 자기 경계에서만 일치한다. | PASS — §11.26 failpoint parse·경계 |
| Forward 비대기와 재전송 금지 | `TestClusterForwardDuringDetachIsDroppedWithoutBlockingLane` | DETACH 대기 중 다른 session 전달이 끝난다. 전환 중·뒤 forward는 DETACH와 ENQUEUE에 없다. | PASS — §11.26 forward 경계·race |
| Commit 전 owner crash | `TestDynamicClusterDetachOwnerCrashProcessAcceptance/before-commit` | Exit 86·marker, outage count 0, recovery 뒤 capture 재개, SessionPresent=1, live 재전달·중복 없음, sentinel 수신 | PASS — §11.26 commit 전 process 수용 |
| Commit 뒤 owner crash | `TestDynamicClusterDetachOwnerCrashProcessAcceptance/after-commit` | Exit 86·marker, outage count 1 보존, outage → after-recovery FIFO, SessionPresent=1, 중복 없음, sentinel 수신 | PASS — §11.26 commit 뒤 process 수용 |
| 기존 경로 유지 | Focused·race·build, 기존 process 회귀, 전체 local gate | 모든 명령 PASS | PASS — §11.26 실행 명령과 결과 |

## 8. 재검토 조건

- QoS 0 offline 보존을 요구하는 요구사항이 생길 때
- 지원 client 계약이 전환 중 QoS 0 무손실을 요구할 때
