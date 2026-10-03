# ADR-0020: Raft 저장 완료 메시지와 bounded WAL group commit

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (비동기 저장 구조만; production storage·BP-03 수용 제외) |
| 결정일 | 2026-10-02 |
| 결정자 | NaruMQ 프로젝트 — 사용자 지시 `제안한 방식대로 개선해보자` |
| 관련 요구사항 | [PRD](../requirements/PRD.md) CLU-01, CLU-04~CLU-11, NFR-07~NFR-10 |
| 관련 스펙·데이터 계약 | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md), [성능 및 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계) |
| 관련 아키텍처 | [Broker core의 저장 경계](../architectures/broker-core.md#raft-영속성-경계) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | [ADR-0009 §1.2](ADR-0009-raft-production-state-profile.md#12-walsnapshot-storage와-동기화)의 synchronous Ready scheduling만 대체 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

Pinned `go.etcd.io/raft/v3 v3.7.0`의 `AsyncStorageWrites=true`를 사용한다. 저장 요청과 완료 응답을 나누는 공식 메시지 경로로 전환하며 `RawNode.Advance`는 호출하지 않는다. MQTT ACK를 비동기로 앞당기는 결정은 아니다.

Node adapter는 RawNode·memory index·apply history를 한 mutex 아래에서 소유하고, 단일 WAL worker는 disk write·sync만 소유한다. Worker는 RawNode나 application을 호출하지 않는다. `ProcessReady`는 disk를 기다리지 않고 준비된 완료와 새 Ready를 처리한다. `StorageReady`는 합쳐진 완료 알림이며, 알림 한 번과 완료 한 건을 같은 것으로 보지 않는다.

### 1.1 저장·apply·응답 경계

- `MsgStorageAppend`는 local FIFO에 정확히 한 번 전달한다. Local thread ID와 local storage message는 peer wire에 보내거나 peer 입력으로 받지 않는다.
- Worker는 기존 CRC WAL frame을 요청 순서대로 쓴다. 이미 대기 중인 요청을 bounded batch로 모아 한 번 sync한다. 의도적인 추가 수집 지연은 넣지 않는다. 같은 WAL에 여러 writer나 병렬 fsync를 붙이지 않는다.
- HardState의 term·vote·commit-only 변경도 반드시 sync한다. 영속 값이 없는 요청은 앞선 FIFO 작업의 완료를 기다리되 새 frame이나 sync는 만들지 않는다.
- Memory index와 durable HardState는 worker 완료를 driver가 인수한 뒤 갱신한다. 메시지에 첨부된 정확한 Responses를 그 순서로 처리하며, term/index가 바뀐 완료를 최대 index 하나로 대체하지 않는다.
- Entry-bearing `MsgApp`도 해당 local append 완료 뒤에만 peer로 해제한다. 공식 async 경로의 leader 복제·로컬 저장 병행을 그대로 쓰면 leader가 WAL을 보내기 전에 두 follower가 commit·ACK할 수 있으므로 기존 crash-point 계약보다 느슨해진다. 이 추가 저장 의존성은 bounded 요청의 byte 계산에 포함한다. 저장에 의존하지 않는 heartbeat·tick은 막지 않는다.
- Apply 요청에는 발행 시점의 append sequence를 붙인다. 그 앞의 저장 요청이 모두 성공하기 전에는 committed command를 application에 노출하지 않는다. `CompleteApply`는 반환된 batch의 sequence를 확인하고 application·projection 처리 뒤에만 apply 완료 응답을 전달한다. 동일하거나 잘못된 sequence는 성공으로 취급하지 않는다.
- Quorum 영속화·commit·local application/projection 뒤 성공 ACK, 변경별 독립 fresh ReadIndex, Packet ID·epoch fencing은 유지한다.

### 1.2 용량과 lifecycle

저장 queue는 진행 중·완료 인수 대기를 포함해 최대 64개 요청과 일반 요청 합계 4 MiB를 사용한다. 큰 incoming snapshot은 기존 snapshot 상한 안에서 단독 저장한다. Ready를 인수했지만 공간이 없는 경우 adapter가 최대 한 Ready를 보관하며 다음 Ready는 인수하지 않는다. 저장을 더 만드는 proposal·append/snapshot peer 입력은 포화에서 backpressure를 반환하지만 tick과 필요한 응답 처리는 계속 가능해야 한다. Append/snapshot은 Raft 복제 재시도를 따르지만 forwarded `MsgProp`는 자동 재전송을 보장하지 않는다. 포화에서 거절된 미commit proposal의 원래 caller는 성공 ACK 없이 timeout할 수 있다. Node가 이미 인수한 local 저장 요청과 용량 부족으로 보관한 append는 버리지 않는다. Driver가 queue에 blocking send하면서 저장 완료를 인수하지 못하는 순환 대기를 만들지 않는다.

Incoming snapshot은 앞뒤 WAL batch 사이의 barrier다. Snapshot temp write·file sync·rename·directory sync를 완료하기 전 성공 응답을 해제하지 않는다. Local snapshot·compaction은 대기 중 WAL 작업과 application 완료가 없는 경계에서 수행하고, runtime은 강제 snapshot 요청도 완료 가능한 경계까지 보존한다. Backup과 drain은 durable/applied/projection index를 사용하며 Ready 호출 종료만으로 영속화를 추정하지 않는다.

Write·sync·snapshot 오류는 terminal failed 상태로 전이한다. 실패한 batch의 저장 의존 응답과 ACK는 해제하지 않는다. Pending caller를 실패로 끝낸다. Close는 신규 작업을 막고 WAL worker가 이미 queue에 있는 작업 뒤 보관된 append까지 저장하도록 하며, join 뒤 파일을 닫는다. Shutdown의 보관 append는 응답을 해제하지 않고 disk만 기록하므로 완료 channel 상한을 넘기지 않는다. 선행 또는 마지막 저장이 실패하면 Close는 이 보관 작업의 저장 실패를 명시적으로 반환한다. 다른 Node 인스턴스에는 이전 worker 완료가 전달될 수 없다. Hook은 WAL worker에서도 실행되므로 node lock 보유를 가정하지 않으며 Node 재진입과 borrowed payload 보관을 금지한다.

### 1.3 Proposal 수집

이미 독립 fresh proof를 완료한 FIFO command만 최대 64개·64 KiB로 제출한다. Control 처리를 막는 8ms timed driver drain 대신, 지금 대기 중인 작업을 공용 64개 예산으로 처리해 제출한다. WAL worker가 fsync 중에도 driver가 계속 Ready를 발행하므로 후속 WAL 요청은 worker에서 모인다. 모든 control 도착을 기다리거나 매번 collection timer를 새로 시작하지 않는다. Low-load command에 추가 대기를 강제하지 않는다. 더 긴 collection delay·proof 한도 확대·외부 설정 knob는 채택하지 않는다.

## 2. 배경

최신 고정 BP-03 `bp03-20261002T133437.979734800Z-36656`는 count·offline 복구를 유지했지만 Step 3 generator overload로 invalid였다. Node별 Ready 시간의 약 96.7%가 WAL sync였고, 세 node 합계 8,100 sync에 평균 1.75 entry/sync였다. 이 비율은 CPU 사용률이나 workload 전체 시간 비중이 아니며 schedule lag의 모든 원인을 증명하지 않는다.

기존 구조는 fsync 중 Node mutex와 driver 진행을 막는다. Proposal aggregation만으로는 이 결합을 없애지 못한다. 비동기 저장만으로는 sync 횟수를 줄이지 못하므로 두 책임을 함께 변경한다.

## 3. 결정 요인

1. ACK와 복구 보장을 약화하지 않는다.
2. WAL 대기와 RawNode 진행을 분리한다.
3. Queue·memory·goroutine 수와 오류·종료 경계를 명시한다.
4. 기존 WAL·snapshot 형식과 단일 Raft group을 재사용한다.

## 4. 선택 근거

공식 [v3.7.0 async storage 계약](https://github.com/etcd-io/raft/blob/v3.7.0/doc.go)은 local append/apply FIFO와 저장 후 Responses 전달을 제공한다. Native term/index 완료 검사를 재사용하면 독자적인 Ready pipelining보다 오래된 완료·log overwrite 위험을 줄일 수 있다. 라이브러리가 허용하는 느슨한 durability는 프로젝트의 commit-only HardState 계약에 적용하지 않는다.

## 5. 대안

| 대안 | 장점 | 단점·제외 이유 |
| --- | --- | --- |
| 기존 batching만 보강 | 작은 변경 | fsync 동안 driver 정체가 남는다. |
| 기존 File.Sync만 goroutine으로 이동 | 변경이 작아 보임 | Ready 수명·Advance·응답 순서를 해결하지 못한다. |
| Sync 생략·ACK 선반환 | 저장 대기 감소 | ACK된 상태 복구 계약을 위반한다. |
| Queue·deadline 확대 | 일시적 burst 흡수 | 처리 능력을 늘리지 않고 장애 관찰만 늦춘다. |

## 6. 결과

Disk 대기 중 driver 진행과 여러 WAL frame의 공동 sync를 목표로 한다. 개선량·BP-03 통과는 실행으로 판정한다. File format, quorum 정책, fresh proof와 단일 group은 바꾸지 않는다. 비동기 resource 소유와 crash-ordering 검증 비용이 추가된다.

## 7. 검증

실제 실행 증거는 [성능 및 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)와 [Cluster §11.18](../specs/cluster-session-and-delivery.md#1118-비동기-walapplication-완료-경계-수용-매핑)에 연결한다. 초기 독립 suite의 4/5 invalid를 보존한다. Release-profile wake 보완 뒤 고정 compare `bp03-20261002T193345.142241000Z-38956`와 suite `bp03-suite-20261002T193711.351279300Z-4364`는 양쪽 valid·5/5 성공이다. 단위·race·실제 MQTT·ACK fault·installed-snapshot release process·최종 storage chaos·full local gate도 PASS했다. 이 결과는 local smoke 범위이며 production storage·ADR-0003 전체 수용이나 모든 EOF 제거를 증명하지 않는다.

| 경계 | 합격 기준 |
| --- | --- |
| 보류된 fsync | Tick·peer 응답 인수 가능, 저장 의존 응답·apply·성공 ACK 없음 |
| Group commit·오류 | FIFO frame 복구, 여러 요청에 한 sync, 실패한 batch의 성공 응답 없음 |
| Commit-only HardState | 해당 sync 전 application apply 없음, reopen에서 commit 복구 |
| Overwrite·재시작 | 오래된 완료가 다른 term의 entry를 안정화하지 않음 |
| 용량·종료 | Local 작업 무손실, queue 상한, 포화·cancel·close에서 순환 대기 없음 |
| Snapshot·backup·drain | 저장·application 완료 경계와 crash/recovery 시나리오 유지 |
| 실제 MQTT·fault | 기존 ACK fault, storage chaos와 full local gate 통과 |
| 성능 | 동일 BP-03 compare, 유효하면 독립 5회 suite; invalid를 통과로 재해석하지 않음 |

## 8. 재검토 조건

- Crash evidence에서 ACK된 상태 손실이나 잘못된 term/index 완료가 관찰된다.
- 고정 workload에서 queue 포화 또는 tail latency가 악화된다.
- 지원 filesystem의 저장 순서가 기존 WAL 계약을 충족하지 못한다.
