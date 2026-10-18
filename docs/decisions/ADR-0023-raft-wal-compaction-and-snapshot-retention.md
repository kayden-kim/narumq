# ADR-0023: Raft WAL 교체와 snapshot 보존

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (local storage lifecycle; production Linux/EBS 제외) |
| 결정자 | NaruMQ 프로젝트; 승인된 WAL lifecycle 구현 계획 |
| 관련 요구사항 | [PRD](../requirements/PRD.md) CLU-14 |
| 관련 스펙 | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md) §11.22 |
| 관련 결정 | [ADR-0009](ADR-0009-raft-production-state-profile.md), [ADR-0020](ADR-0020-asynchronous-raft-wal-group-commit.md), [ADR-0022](ADR-0022-bootstrap-volume-loss-guard.md) |

## 배경

WAL은 재시작할 때 재생하는 영속 기록이다. 기존 `wal.bin`은 snapshot을 만든 뒤에도 이전 entry를 계속 보관했다. Snapshot은 특정 index까지의 상태를 담은 파일이며, 이전 snapshot 파일도 삭제하지 않아 저장 공간이 계속 증가했다. 마지막 frame의 checksum 불일치를 무조건 잘라 내던 복구는 durable commit을 잃을 위험이 있었다.

## 결정

Local snapshot마다 **snapshot 파일 확정 → WAL 교체 → 이전 snapshot 삭제** 순서로 저장 공간을 회수한다. Snapshot 파일은 기존 file sync·close·atomic rename·directory sync 순서로 먼저 확정한다. Node mutex를 소유하고 `appendPending == 0 && heldAppend == nil`인 기존 경계에서만 WAL을 교체한다. 수신 snapshot을 저장하는 WAL worker 경로는 변경하지 않으며 다음 local snapshot이 정리한다.

WAL은 memory storage에 fsync 완료 뒤 반영된 snapshot 이후의 모든 entry와 최신 HardState로 재작성한다. Commit 이전이라도 이미 영속된 tail은 보존한다. Payload 크기 한도 안에서 entry를 여러 frame으로 나누며 snapshot record는 넣지 않는다. `wal.bin.tmp`에 magic·frame을 쓰고 sync·close한 뒤 기존 WAL handle을 sync·close한다. 이어 atomic replace·directory sync를 완료하고 새 WAL handle을 append 위치로 다시 연다. Ready append 전용 hook과 sync observer는 호출하지 않는다.

Windows의 atomic rename은 다른 reader가 삭제 공유(`FILE_SHARE_DELETE`) 없이 파일을 열면 실패할 수 있다. Raft adapter의 기존 `renameFile` 경계에서 `ERROR_ACCESS_DENIED`·`ERROR_SHARING_VIOLATION`만 10ms 간격으로 최대 250ms 동안 다시 시도한다. 대기 예산은 첫 해당 오류부터 센다. 정상 경로에는 대기를 추가하지 않는다. 같은 `MoveFileExW`의 replace·write-through flag를 유지하며 파일을 먼저 삭제하지 않는다. 잠금이 계속되거나 다른 오류가 나면 기존 terminal storage 오류 경로를 따른다. 이 처리는 WAL·snapshot·cluster metadata의 공통 파일 확정에만 적용하며 application proposal이나 MQTT 요청을 재시도하지 않는다.

최신 finalized snapshot 하나만 보존한다. 최신 index보다 작은 finalized snapshot 및 snapshot temp를 삭제하고 directory를 sync한다. 더 큰 index의 temp는 유지한다. Compaction·prune 오류는 node를 terminal 상태로 만드는 기존 storage 오류 경로를 따른다. 재시작 때 남은 `wal.bin.tmp`는 삭제하고 directory를 sync한다.

Frame byte format은 변경하지 않는다. 이전 build가 교체된 WAL을 읽는 호환성은 `58a95f8` N-1 binary rollback 시험으로 검증한다. Config key, metric, size threshold는 추가하지 않는다. Dynamic runtime의 기존 1000 applied entries snapshot 주기를 따른다.

Torn write는 쓰기가 끝나지 않은 마지막 기록이다. Header보다 짧은 tail과 부족한 payload는 기존처럼 절단한다. Length 0 header는 남은 영역 전체가 0일 때만 절단한다. CRC가 틀린 frame은 파일 끝 또는 전부 0인 padding 앞에 있고, frame 자체의 파일 offset 기준 512-byte 구간(양 끝 부분 구간 포함)에 전부 0인 구간이 있을 때만 절단한다. Zero padding만 있고 frame 내부에 근거가 없으면 복구를 실패시킨다. Oversized frame, decode 오류, 중간 checksum 오류도 fail-closed를 유지한다. Zero 검사는 오류 경로에서만 실행한다.

## 대안

| 대안 | 제외 이유 |
| --- | --- |
| etcd식 segment 파일 | 다수 시험과 chaos harness가 `wal.bin`에 의존한다. 기존 파일 계약을 유지한다. |
| Raw byte 복사 compaction | Worker idle 경계의 memory tail이 이미 canonical 영속 상태다. 별도 raw replay를 추가하지 않는다. |
| Size threshold | 운영 설정과 결정적 시험 조건이 늘어난다. 기존 local snapshot 경계를 재사용한다. |

## 결과와 비용

Snapshot 이전 WAL과 오래된 snapshot을 회수하면서 최신 snapshot과 영속 tail로 복구할 수 있다. 교체 중 오류는 서비스 지속보다 복구 가능한 상태 보존을 우선한다. Zero sector가 없는 마지막 frame CRC 불일치는 restart를 실패시킨다. 운영자는 [ADR-0022](ADR-0022-bootstrap-volume-loss-guard.md)의 새 ID 재시드로 복구한다.

실제 Linux/EBS 전원 손실, volume loss, `SPEC-TBD-09` retention·RPO/RTO는 이 결정의 수용 범위가 아니다. Production gate는 열린 상태로 유지한다.

## 검증과 재검토

실제 명령·결과·wall time의 기준은 Cluster spec §11.22다. 구현 전 실패 재현과 구현 후 focused, race, process restart, release rollback, durable ACK, Docker/FUSE, local gate를 연결한다. 실제 storage 장애가 마지막 durable commit 손실을 보이면 순서와 torn-write 근거를 재검토하되 자동 절단 규칙을 임의로 완화하지 않는다.
