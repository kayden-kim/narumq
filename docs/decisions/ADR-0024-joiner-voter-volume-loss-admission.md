# ADR-0024: Joiner voter 기록 손실은 seed admission으로 복구하지 않는다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (local joiner volume-loss admission; production Linux/EBS 제외) |
| 결정일 | 2026-10-03 |
| 결정자 | NaruMQ 프로젝트 — joiner voter volume-loss 구현 계획 승인 |
| 관련 요구사항 | [PRD](../requirements/PRD.md) CLU-02, CLU-07, CLU-14 |
| 관련 스펙·계약 | [Cluster §1·§7·§11.20](../specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑), [Membership v2](../contracts/raft-membership-v2.md#seed-admission-exchange) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md) |
| 관련 결정 | [ADR-0010](ADR-0010-dynamic-cluster-membership.md), [ADR-0022](ADR-0022-bootstrap-volume-loss-guard.md) |
| 이 결정이 대체하는 이전 결정 기록 | ADR-0022 §6의 joiner voter 보호 설명만 구체화하며 bootstrap probe 규칙은 유지 |

## 배경

Voter는 합의에서 투표하는 node다. 투표·ACK 기록을 잃은 voter를 빈 node처럼 다시 참여시키면 기존 합의의 안전성을 지킬 수 없다. Seed admission은 새 joiner가 이미 승인된 membership을 확인하는 절차이며, 잃은 voter 기록을 대신하는 복구 증거가 아니다.

기존 seed는 후보를 제외한 voter가 정확히 3개이면 committed `VOTER` 후보도 허용했다. 이 조건은 3-voter cluster에서는 손실 후보를 막지만, 승격 직후 `{1,2,3,4}`에서는 node 4를 막지 못한다. 로컬 process 실험에서 node 4의 volume을 삭제하고 같은 config로 시작하자 heartbeat 처리에서 `tocommit(9) is out of range [lastIndex(0)]` panic과 exit 2가 관찰됐다. READY 복구나 split-brain을 관찰한 것은 아니다.

## 결정

1. Seed runtime은 committed phase가 `VOTER`인 후보의 seed admission을 거절한다. Voter 수를 검사하기 전에 phase로 판단한다. 기존 `STAGED`·`LEARNER`·`CATCHING_UP`의 identity, operation ID, ConfState와 세 incumbent voter 검증은 유지한다.
2. Joiner transport는 local phase가 `STAGED`인 경우 seed의 accepted `VOTER` 응답을 설치하지 않는다. 이전 binary seed가 이를 허용해도 fresh node가 voter identity와 peer registry를 설치하지 못한다.
3. 같은 volume의 정상 voter 재시작은 persisted snapshot/WAL에서 복구한 committed local identity로 재개한다. 기존 `runSeedAdmission`은 이 identity가 있으면 seed 요청 없이 끝난다. 기록을 잃은 voter는 새 ID stage·join·promotion과 기존 ID planned removal로 교체한다.
4. 기존 wire format·protocol/schema·metadata·config는 변경하지 않는다. `VOTER` response decoder와 이미 learner/catching-up 상태인 transport의 phase 진행 표현은 유지한다. Fresh `STAGED→VOTER` 진입만 금지하고, 현재 seed runtime은 voter 요청을 승인하지 않는다.
5. 거절은 기존 generic seed rejection과 NOT_READY 경로를 따른다. Joiner는 초기 metadata/WAL과 listener를 만들 수 있으므로 bootstrap의 exit 1·directory 무변경·listener 미개방 규칙과 같지 않다. 거절된 node는 성공 MQTT admission이나 Raft 참여를 할 수 없다. Panic recover, heartbeat 특례, 자동 WAL 재생성, startup flag와 새 retry 정책은 추가하지 않는다.

## 대안

| 대안 | 제외 이유 |
| --- | --- |
| 후보 제외 voter 수로만 보호 | 승격 후 4-voter 상태에서 손실 voter를 허용한다. |
| Heartbeat panic을 잡거나 commit을 낮춤 | 참여 전에 막아야 할 기록 손실을 숨기며 합의 정책을 바꾼다. |
| Seed snapshot으로 기존 voter 자동 재시드 | 투표 기록을 잃은 같은 ID를 새 node로 승인하는 별도 안전성 설계가 필요하다. |
| 새 durable-history proof wire field | 형식 변경과 cross-release 복구 계약이 필요하다. 기존 persisted recovery·새 ID 교체 경계를 사용한다. |

## 결과와 비용

서버는 voter 요청을 허용하지 않고 새 client도 fresh voter 응답을 거절하므로 서로 다른 binary가 참여하는 두 방향에서 보호한다. 이전 binary끼리만 동작하는 cluster를 이 변경이 보호한다는 뜻은 아니다. 정상 same-volume recovery, learner/catching-up crash recovery와 historical release 회귀를 함께 검증한다.

4-voter 상태에서 기록을 잃은 node를 교체할 때는 손실 voter를 먼저 제거해 3 incumbent로 만든 뒤 새 ID를 추가한다. 3-voter 상태에서는 down voter를 포함한 기존 set에서 새 ID를 먼저 승격하고 손실 voter를 제거한다. 이는 기존 seed wire의 세 incumbent 조건을 유지하기 위한 순서다.

실제 Linux/EBS volume·전원 손실과 `SPEC-TBD-09` backup retention·RPO/RTO는 이 local 수용 범위가 아니며 production gate는 유지한다.

## 검증과 재검토

시험 위치·실제 명령·before FAIL/after PASS·wall time의 기준은 [Cluster §11.20](../specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑)이다. Fresh VOTER 응답 거절은 실제 TLS socket으로, 3-voter·4-voter 상태와 새 ID의 retained/session/offline 복구는 broker process로 확인한다. 정상 복구가 seed VOTER 응답에 의존하는 증거가 나오면 persisted identity 복구 경계를 분석하며, 기록 없는 voter를 허용하는 방식으로 우회하지 않는다.

2026-10-03 local 수용 완료: TLS FAIL→PASS, 승격 직후 4-voter의 seed wire 승인·heartbeat panic FAIL→PASS, 기존 3-voter 보호와 양쪽 새 ID 복구 PASS. Membership crash/restart, TCP·runtime race, synthetic·지정 historical release와 전체 `local: PASS`(377.424s)를 확인했다. Production gate는 변경하지 않았다.
