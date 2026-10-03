# ADR-0026: Detach 전환 중 local QoS 0 publish 보존

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (owner-node local QoS 0 publish 경로만; forward·connected owner crash 제외) |
| 결정일 | 2026-10-04 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD CLU-14](../requirements/PRD.md#92-클러스터와-세션) |
| 관련 스펙·데이터 계약 | [Cluster §4.1·§11.25](../specs/cluster-session-and-delivery.md) |
| 관련 아키텍처 | [Project structure](../architectures/project-structure.md), [Cluster architecture](../architectures/cluster-architecture.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | [ADR-0013](ADR-0013-offline-queue-replication-and-cluster-backup-v3.md) §1 10항의 detach 전환 race 문장 중 owner-node local publish 경로만 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

1. Persistent session disconnect는 `BeginDetach`로 local queue를 detaching 상태로 표시하고 만료되지 않은 FIFO message의 복사본을 가져온다. 이 표시는 queue 조회와 같은 lock 안에서 설정한다.
2. Detaching queue는 `DurableSequence == 0`인 process-local item의 enqueue를 `ErrDetaching`으로 거절한다. Batch에 이 item이 하나라도 있으면 만료 purge를 포함한 상태 변경을 하지 않는다. Durable item은 기존 규칙대로 받는다.
3. Disconnect는 QoS 0 pending message로 `DetachSession`을 실행한다. 호출이 끝나면 `FinishDetach`를 정확히 한 번 실행한다. 성공하면 session queue를 제거하고, 실패하면 기존 queue를 유지하며 정상 enqueue를 재개한다. `FinishDetach`와 `Remove`는 대기자를 깨운다.
4. Owner node에서 publish된 QoS 0 후보는 offline commit, remote forwarding, local queue 순서로 판정한다. Local enqueue가 `ErrDetaching`이면 lock 밖에서 DETACH 결과를 한 번 기다린 뒤 같은 순서로 다시 판정한다. 성공한 DETACH 뒤에는 offline `ENQUEUE`로 보내고 실패 뒤에는 local queue로 보낸다. 캡처한 message와 같은 publisher의 이후 message는 FIFO를 유지한다.
5. 대기 상한은 10초다. 대기 실패, 두 번째 `ErrDetaching` 또는 시도 소진은 `ErrUnavailable`로 반환한다. Offline committer가 있을 때 local queue 제거로 받은 `ErrUnavailable`은 한 번만 재판정한다. 대기와 제거 재판정을 합쳐 최대 세 번 판정한다.
6. 다른 node에서 forward된 QoS 0 delivery의 `AcceptForwarded`는 기다리지 않는다. `ErrDetaching`을 `ErrUnavailable`로 바꾸며 [ADR-0014](ADR-0014-cross-node-live-qos0-forwarding.md)의 at-most-once(최대 한 번 전달) 유실 경계를 유지한다.
7. 연결 중 owner crash로 잃은 local queue는 보호하지 않는다. Connected live message마다 Raft write를 추가하지 않는 ADR-0013 결정 요인 2를 유지한다.
8. 각 candidate 판정은 새 현재 시각으로 absolute Message Expiry deadline을 검사한다. 대기 중 만료된 message는 offline commit·forward·local enqueue를 실행하지 않고 전달 결과에도 넣지 않는다. 유효한 message는 그 판정 시각을 offline proposal과 local enqueue에 전달한다. Ingress에서 계산한 deadline은 바꾸지 않는다.

## 2. 배경

기존 disconnect는 `ListQueued` 뒤 DETACH를 commit하고 `Remove`로 queue를 지웠다. Publish는 connect의 mutex를 잡지 않으므로 다음 두 순서에서 message를 잃었다.

1. Queue 조회 뒤 local publish가 message를 추가한다. DETACH에는 그 message가 없고 이후 `Remove`가 삭제한다.
2. Publish가 DETACH 이전 상태를 읽는다. DETACH가 commit되고 queue가 제거된다. 이어지는 local enqueue는 `ErrUnavailable`을 반환하며 message를 버린다.

## 3. 대안

| 대안 | 제외 이유 |
| --- | --- |
| Queue에 tail을 쌓은 뒤 별도 ENQUEUE한다. | 대기 중인 publisher의 이후 message가 먼저 기록되어 순서가 역전될 수 있다. |
| Forward lane도 DETACH 완료까지 기다린다. | 같은 lane을 쓰는 다른 session의 forward 전달을 막는다. |
| Connected message를 모두 Raft에 기록한다. | ADR-0013 결정 요인 2와 충돌하며 연결 중 crash 보호까지 범위를 넓힌다. |

## 4. 결과

- Owner-node local publish는 detach 전환 때문에 local queue에서 지워지거나 제거 직후 버려지는 두 유실 순서를 피한다.
- Detaching session이 대상이면 해당 publisher 처리가 DETACH commit 시간만큼 지연된다. 최대 대기는 10초다.
- 10초를 넘기면 해당 대상을 `Unavailable`로 보고한다.
- DETACH commit timeout 뒤 늦게 commit된 경우는 기존 Cluster §4.1 3항의 경계를 그대로 따른다.
- Forward 경로와 연결 중 owner crash는 열린 수용 범위로 남긴다. Production storage 보장과 CLU-14 전체 완료를 선언하지 않는다.

## 5. 검증

실행 증거의 기준은 [Cluster §11.25](../specs/cluster-session-and-delivery.md#1125-detach-전환-local-qos-0-보존-수용-매핑)다. 아래 결과는 실행 뒤에만 갱신한다.

| 시험 | 확인할 동작 | 증거 |
| --- | --- | --- |
| `TestBeginDetachRejectsLocalEnqueueUntilFinish` | Local 거절·durable 허용·실패 뒤 FIFO 재개 | PASS — §11.25 adapter·focused·race |
| `TestFinishDetachCommittedRemovesAndWakesWaiters` | Commit 성공 뒤 제거·대기자 해제 | PASS — §11.25 focused·race 및 대기 진입 보강 시험 |
| `TestRemoveWakesDetachWaiters` | Remove 뒤 대기자 해제 | PASS — §11.25 focused·race 및 대기 진입 보강 시험 |
| `TestPublishLocalDetachTransition` | Commit·실패·제거 재판정·committer 부재·timeout·대기 중 expiry | PASS — §11.25 publish focused·race; 두 번째 대기 금지·재판정 상한과 expiry 직전·경계·이후 포함 |
| `TestAcceptForwardedDetachingIsUnavailable` | Forward는 기다리지 않고 Unavailable 반환 | PASS — §11.25 publish focused·race |
| `TestClusterDisconnectDetachesPendingQueueAndRemovesLocalCopy` | Detach 성공 뒤 전환 완료 | PASS — §11.25 connect 단계·focused |
| `TestClusterDetachFailureKeepsLocalQueue` | Detach 실패 뒤 local queue 유지 | PASS — §11.25 connect 단계·focused |
| `TestClusterDisconnectSkipsDetachWhenBeginDetachFails` | BeginDetach 실패 시 DETACH 생략 | PASS — §11.25 connect 단계·focused |
| `TestClusterDetachCapturesConcurrentLocalQoS0Publish` | 실제 queue의 동시 publish 보존과 FIFO | 수정 전 FAIL, 수정 후 PASS — §11.25 targeted·focused·race |
| Focused·race 시험 및 일반·failpoint 빌드 | 영향 package·data race·빌드 회귀 | PASS — §11.25 실행 명령과 결과 |
| 기존 process 회귀 세 가지 | Offline FIFO·SessionPresent·metadata 유지 | PASS — §11.25 실제 broker process 회귀 |
| `go run ./tools/verify local` | 전체 local gate | PASS — §11.25 local gate 실행 증거 |
