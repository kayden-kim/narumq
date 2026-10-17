# ADR-0062: 1 MiB를 넘는 Raft snapshot은 `snapshot-stream` feature가 켜진 cluster에서 별도 조각 stream으로 보낸다

| 항목 | 내용 |
| --- | --- |
| 상태 | `Accepted` |
| 결정일 | 2026-10-16 |
| 결정자 | 사용자 선택("1 MiB 초과 snapshot은 조각 stream + feature token으로 구현", 2026-10-16) |
| 관련 요구사항 | [PRD](../requirements/PRD.md#92-클러스터와-세션) CLU-14, CLU-04~CLU-06 |
| 관련 스펙·계약 | [Cluster §7](../specs/cluster-session-and-delivery.md#7-failure-recovery) 7항, [Cluster §11.37](../specs/cluster-session-and-delivery.md#1137-raft-snapshot-stream-수용-매핑), Active [Raft snapshot stream v1](../contracts/raft-snapshot-stream-v1.md), Draft [Raft membership v2](../contracts/raft-membership-v2.md#raft-메시지-프레임과-크기-한도), [Release compatibility v1](../contracts/cluster-release-compatibility-v1.md) |
| 관련 결정 | [ADR-0010](ADR-0010-dynamic-cluster-membership.md), [ADR-0032](ADR-0032-feature-activation-approval-policy.md), [ADR-0059](ADR-0059-dynamic-peer-tcp-liveness.md), [ADR-0061](ADR-0061-eks-ordinal-member-slot.md) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

직렬화한 Raft `MsgSnap`이 peer frame 한도(1,048,576 byte)를 넘으면, `snapshot-stream` feature가 활성화된 cluster의 leader는 그 메시지를 Raft 연결로 보내지 않고 별도 TCP 연결의 조각 stream으로 보낸다.

- Feature token은 `snapshot-stream`이다. 모든 새 build가 이 token을 광고한다. 활성화는 기존 `FeatureActivation` 절차([ADR-0032](ADR-0032-feature-activation-approval-policy.md))를 쓴다. 관리 API는 `POST /api/v1/cluster/features/snapshot-stream`이다.
- 활성화 전 동작은 지금과 같다. 1 MiB를 넘는 `MsgSnap`은 보내지 않고, 해당 peer의 snapshot 전송을 실패로 Raft에 알리고(`ReportSnapshot(SnapshotFailure)`), peer별 30초 제한의 `WARN`을 남긴다.
- 활성화 뒤에는 1 MiB를 넘는 `MsgSnap`만 stream으로 보낸다. 1 MiB 이하 `MsgSnap`과 다른 모든 Raft 메시지는 기존 Raft 연결을 그대로 쓴다.
- Stream은 mTLS와 Raft 연결과 같은 동적 admission·identity 검사를 쓰는 새 연결이다(매직 `NMQSNAP1`). 전체 snapshot의 SHA-256, 조각별 CRC32C와 offset 순서를 검사하고, 한 번에 한 transfer만 받는다. 형식은 [Raft snapshot stream v1](../contracts/raft-snapshot-stream-v1.md)이 정한다.
- 수신 측은 검증을 마친 뒤 `MsgSnap` 하나를 크기 한도를 올린 전용 Step 경로(`Node.StepSnapshotStream`)에 넣는다. 송신 측은 완료하면 `ReportSnapshot(SnapshotFinish)`, 실패하면 `SnapshotFailure`를 Raft에 알린다. Raft Ready loop는 stream 송신을 기다리지 않는다.
- 활성화 뒤에는 N-1 build로 rollback할 수 없다. 이 feature를 모르는 build는 `NMQSNAP1` 연결을 거절하므로 큰 snapshot을 받을 수 없다. 이는 다른 feature와 같은 활성화 의미다.

## 2. 배경

Raft `MsgSnap`은 snapshot 전체를 한 메시지에 담는다. Peer frame 한도는 1 MiB다. 복제 상태 snapshot은 64 MiB까지 허용하므로, 상태가 1 MiB를 넘으면 새 learner와 뒤처진 follower는 snapshot을 받을 수 없다. 2026-10-16 EKS 시험에서 이 한계가 voter volume 손실 교체를 막았다. 새 leader마다 1 MiB 초과 `MsgSnap`을 만들었고, 그 오류로 leader가 반복해서 죽었다. 죽는 문제는 "보내지 않고 실패로 알린다"로 이미 막았다([Raft membership v2 §1.5](../contracts/raft-membership-v2.md#raft-메시지-프레임과-크기-한도)). 그러나 큰 snapshot을 보낼 방법은 여전히 없다. CLU-14의 voter volume 손실 복구(DEP-13~23)는 상태 크기와 관계없이 끝나야 한다.

## 3. 결정 요인

1. 새 voter와 learner가 상태 크기(최대 64 MiB)와 관계없이 따라잡아야 한다.
2. Raft 연결의 frame 한도, FIFO 크기와 backpressure 의미를 바꾸지 않는다. 큰 transfer가 heartbeat와 AppendEntries를 막으면 안 된다.
3. 수신 메모리는 한 transfer의 snapshot 크기(최대 64 MiB + 1 KiB)로 묶는다.
4. 변조·누락·순서 오류가 있으면 snapshot을 Raft에 넣지 않는다.
5. 혼합 버전 규칙([Release compatibility v1](../contracts/cluster-release-compatibility-v1.md))과 활성화 승인 규칙([ADR-0032](ADR-0032-feature-activation-approval-policy.md))을 지킨다. 새 wire 동작은 모든 member가 지원한 뒤에만 쓴다.

## 4. 대안

| 대안 | 장점 | 제외 이유 |
| --- | --- | --- |
| 조각 stream + feature token (채택) | Raft 연결을 건드리지 않는다. 활성화 전 동작과 wire가 같다. 크기 상한은 snapshot 한도 하나다. | 새 연결·프로토콜 코드가 필요하다. 활성화 뒤 rollback이 안 된다. |
| Raft peer frame 한도를 64 MiB로 올림 | 코드가 적다. | 모든 peer 대기열(32 frame·4 MiB)과 수신 memory 예약 모델을 다시 써야 한다. Heartbeat와 큰 snapshot이 한 FIFO에서 서로 막는다. N-1이 한도를 넘는 frame을 닫는다. |
| 운영 절차만 사용 (S3 backup restore로 새 member 시드) | 제품 코드를 바꾸지 않는다. | 새 member마다 offline 복원이 필요하다. 장애 중에 quorum을 잃은 cluster는 활성화나 복원 없이 복구할 수 없다. 자동 따라잡기가 없다. |
| 같은 Raft 연결에서 `MsgSnap`을 조각 메시지로 쪼갬 | 연결이 하나다. | Raft 연결의 FIFO·backpressure·reclaim 의미와 섞인다. 수신 재조립 상태가 Raft lane 상태기계에 들어간다. |

## 5. 결과와 제약

- **Quorum 손실 사고는 활성화가 먼저여야 풀린다.** Feature는 `FeatureActivation`이 commit되어야 켜진다. Commit에는 quorum이 필요하다. Quorum을 잃은 뒤에는 활성화할 수 없다. 큰 상태를 가질 cluster는 사고 전에 `snapshot-stream`을 활성화해야 한다. 활성화하지 않은 cluster에서 1 MiB를 넘는 상태로 voter를 잃고 quorum도 잃으면 이 stream은 쓸 수 없다.
- 모든 non-retired member가 이 feature를 광고해야 활성화된다. 광고하지 않는 member(N-1)가 남은 cluster에서는 활성화되지 않는다.
- 활성화 뒤 N-1 build로 rollback하면 큰 snapshot을 받을 수 없다. 이는 cluster policy 등 다른 feature와 같은 의미다.
- Stream 연결은 일반 peer 연결과 같은 인증을 쓰므로 같은 CA와 commit된 인증서만 접속한다. 수신 한 번에 최대 64 MiB + 1 KiB를 메모리에 둔다. Raft adapter가 `MsgSnap`을 복사하고 해독하는 동안 일시적으로 그 몇 배가 된다 [INFERENCE: 정확한 최대치는 측정하지 않았다].
- 송신은 leader의 Ready loop 밖 goroutine이다. 한 leader는 peer당 한 transfer, 동시에 2개까지만 보낸다.
- 이 stream은 snapshot 전달만 다룬다. Backup archive 형식과 Raft 1 MiB frame 한도는 바꾸지 않는다.
- 이 결정은 production 운영 승인을 뜻하지 않는다. Raft membership v2와 새 계약은 Draft다.

## 6. 검증

수용 기준과 시험 매핑은 [Cluster §11.37](../specs/cluster-session-and-delivery.md#1137-raft-snapshot-stream-수용-매핑)에 기록한다. 이 ADR은 시험 결과를 주장하지 않는다.
