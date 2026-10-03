# ADR-0025: Replicated session metadata와 owner 손실 복구

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-03 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | CLU-08, MQTT-03, MQTT-05, CLU-05 |
| 관련 스펙·데이터 계약 | [Cluster §4.3·§11.24](../specs/cluster-session-and-delivery.md), [replicated-state-v1.proto](../contracts/replicated-state-v1.proto) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

1. 모든 build는 `session-metadata` feature token을 기본 광고한다. `FeatureActivation` commit 전에는 session metadata를 제안하거나 저장하지 않으며, wire bytes와 동작은 기존과 같다. 활성화는 영구적이다.
2. `SessionMetadataState`는 `(tenant, client)` key, 기록 epoch, protocol version(4/5), retention(`IMMEDIATE`/`FINITE`/`INFINITE`), expiry interval, connected 여부, detached 시각, session 만료 시각, Will(topic·payload·QoS·retain·delay)과 Will 예정 시각, Will ACL 재검사용 principal(subject·credential ID·roles·만료)을 보관한다. Principal은 Will이 있을 때만 저장한다.
3. CONNECT는 metadata를 담은 ownership `Acquire`/`Transfer` command로 같은 commit에서 `CONNECTED` record를 설치한다(추가 commit 없음). 새 record는 기존 record와 pending Will을 대체한다. 따라서 재접속은 Will을 취소하며 기존 takeover의 Will 억제 규칙과 같다.
4. DETACH·CLAIM_WILL·DISCARD는 owner/epoch fencing을 거친다. Will은 claim command가 commit된 뒤에만, claim한 node만 발행한다(at-most-once). Claim commit 실패(예: quorum 없음) 시 발행하지 않는다.
5. READY가 된 node는 자기 소유이며 local 연결이 없는 record를 재시작 후 처리한다. 현재 ownership epoch에서 `CONNECTED`인 record는 recovery 시각을 disconnect 시각으로 하는 비정상 종료로 처리하여 Will 규칙을 적용하고 expiry를 시작한다.
6. 새 client 연결 없이 transfer되는 경우(drain·planned removal handoff), successor가 record를 받으면 이전 epoch의 `CONNECTED` record를 정상 종료처럼 처리해 Will을 폐기하고 successor recovery 시각부터 expiry를 시작한다. `DETACHED` record는 저장된 절대 시각을 기준으로 Will·expiry timer를 다시 건다.
7. Owner-local expiry timer는 metadata `DISCARD`와 기존 durable `DISCARD`를 commit하여 만료 state를 회수한다. Ownership row는 epoch 재사용(ABA)을 막기 위해 남기며 `Release`하지 않는다. Offline record는 기존 lazy purge가 제거한다.
8. Cluster 전체 Will+principal byte 합은 8 MiB(`MaxSessionWillBytes`)로 제한한다. 이 값은 tenant storage quota에 포함하지 않는다.
9. 활성화 뒤 `/api/v1/clients`와 `/api/v1/clients/{tenantId}/{clientId}`는 어느 node에서나 committed metadata를 조회한다. Backup은 metadata를 typed snapshot과 whole-state digest에 포함하고 kind summary는 추가하지 않는다. Archive format `3.0`은 유지한다.

### 명령·상태 유효성 및 적용

`SessionConnect` version은 4 또는 5다. MQTT 3.1.1은 retention이 `IMMEDIATE` 또는 `INFINITE`, expiry interval이 0, Will delay가 0이어야 한다. MQTT 5는 `IMMEDIATE`와 interval 0, `INFINITE`와 interval `0xFFFFFFFF`, `FINITE`와 그 사이 양수 interval을 대응시킨다. Will topic은 유효해야 하고 payload는 최대 65,535 bytes, QoS는 0..2다. Principal은 Will이 있을 때만 허용하며 subject와 credential ID는 각각 1..1,024 UTF-8 bytes, expiry는 양수, roles는 최대 64개이고 각 role은 1..256 UTF-8 bytes이며 중복될 수 없다. `Session`은 ownership `Acquire`/`Transfer`에서만 허용한다.

Metadata change에는 유효한 identity, 양수 owner·epoch·proposed timestamp가 필요하다. `DETACH`의 disposition은 `DISCARD`, `CLAIM`, `SCHEDULE` 중 하나다. `SCHEDULE`은 0이 아닌 `WillDueAt >= ProposedAt`을 요구하며 다른 disposition은 `WillDueAt=0`이어야 한다. `SessionExpiresAt`은 0 또는 `ProposedAt`보다 커야 한다. `CLAIM_WILL`은 disposition과 두 시각이 모두 0/unspecified여야 한다. `DISCARD`는 disposition이 `DISCARD` 또는 `CLAIM`이고 두 시각은 0이어야 한다.

State는 connect 필드 규칙을 만족해야 한다. Connected state에서는 detached·expiry·Will due 시각이 모두 0이다. Disconnected state는 detached 시각이 양수이고 retention이 `IMMEDIATE`가 아니어야 한다. Finite expiry는 detached 시각보다 크고 Infinite expiry는 0이다. Will이 있을 때와 Will due가 양수일 때는 동치다. Snapshot은 `(tenant, client)` 정렬·유일성, ownership row 존재 및 `record.epoch <= ownership.epoch`, 전체 Will/principal bytes 8 MiB 이하를 만족해야 하며 record가 하나라도 있으면 `session-metadata`가 active feature여야 한다. Session metadata record 수는 snapshot object count에 포함한다.

Apply는 feature가 비활성일 때 `feature_inactive`, bytes 한도 초과 시 `session_metadata_capacity_exceeded`, metadata 불변식 위반 시 `session_metadata_invalid`로 거절하며 실패한 mutation은 ownership 및 기존 record를 바꾸지 않는다. Metadata command의 owner/epoch가 current ownership과 일치하지 않으면 `stale_epoch`다. Record가 없는 유효한 command는 `Applied, Found=false`다. `DETACH`는 기록 epoch와 connected/detached 시각을 갱신하고 만료 시각을 기록한다. `DISCARD` disposition은 Will·principal을 버리고, `CLAIM`은 Will을 결과로 반환한 뒤 버리며, `SCHEDULE`은 Will이 있으면 예정 시각을 저장한다(Will이 없으면 principal도 버린다). `CLAIM_WILL`은 Will을 결과로 반환한 뒤 Will·principal·due를 제거한다. `DISCARD` action은 record를 지우며 disposition `CLAIM`일 때만 pending Will을 결과로 반환한다. 모든 갱신은 global bytes accounting에 반영된다.

## 2. 배경

Session expiry, Will, protocol 및 owner epoch는 기존에 owner process의 local lifecycle에 의존했다. Connected owner crash 시 DETACH가 없으므로 expiry가 시작되지 않을 수 있고 durable delivery/offline state와 capacity가 남는다. Ownership과 durable delivery는 복제되지만 복구에 필요한 lifecycle metadata는 복제되지 않았으므로, owner 재시작과 management 조회에서 session 상태를 재구성할 committed source가 필요하다.

## 3. 결정 요인

1. Owner 손실 뒤 committed session lifecycle·Will·expiry를 결정적으로 복구한다.
2. Quorum 없는 Will 발행과 stale owner mutation을 방지한다.
3. 비활성 cluster의 기존 동작과 wire bytes를 보존한다.
4. 전체 cluster metadata memory/backup 비용을 bounded하게 유지한다.
5. 기존 archive format과 kind summary contract를 유지한다.

## 4. 선택 근거

CONNECT의 ownership commit에 metadata를 함께 실으면 connection ownership과 session description 사이에 별도 commit의 불일치 창이 없다. Fenced metadata action은 apply 순서와 committed state를 권위로 삼으며, claim commit 후 단 한 node만 Will을 발행하게 한다. 재시작 recovery는 실패 detector나 replica-local timeout 추정 대신 owner 자신이 READY가 된 뒤 복구한다. Feature gate는 활성화 전 payload 제안·저장을 막아 기존 cluster wire와 동작을 보존한다. Snapshot·digest 포함은 backup/restore state를 완전하게 만들고, 별도 summary kind를 추가하지 않아 archive `3.0`을 유지한다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| Leader failure detector로 owner crash를 판정한다. | 남은 node가 즉시 복구를 시작할 수 있다. | Timeout 추정은 false positive와 split-brain 동작을 만든다. | 사용자 결정으로 거부했으며 spec §2.1 및 ADR-0004의 timeout ownership 금지와 충돌한다. |
| 별도 OPEN command로 metadata를 설치한다. | Ownership schema와 분리할 수 있다. | 추가 commit이 필요하고 ownership과 metadata가 불일치하는 창이 생긴다. | CONNECT ownership `Acquire`/`Transfer`와 동일 commit에 설치한다. |
| Metadata를 조회 시 lazy purge한다. | Background expiry timer가 필요 없다. | Pending Will과 durable state 회수가 늦어지거나 유실될 수 있다. | Owner-local expiry timer의 committed DISCARD를 사용한다. |
| 만료 때 ownership `Release`를 한다. | Ownership row 수를 줄인다. | Epoch 재사용으로 ABA fencing 위험이 있다. | Ownership row는 보존하고 metadata만 폐기한다. |
| Schema minor를 올린다. | Schema revision으로 field 추가를 드러낸다. | 별도 migration/profile 분기가 필요하다. | Codec exact `1.0`을 유지하고 feature gate로 계약을 제어한다. |

## 6. 결과

### 긍정적 결과

- Owner 재시작 뒤 committed metadata에서 session expiry와 Will을 복구한다.
- 활성화된 cluster의 어느 node에서나 session metadata 조회가 가능하다.
- Will 발행은 quorum-committed claim 뒤 한 번으로 제한되고 stale owner는 fencing된다.
- 비활성 cluster는 기존 동작과 wire bytes를 유지하며 archive `3.0` kind summary는 바뀌지 않는다.

### 부정적 결과와 비용

- Will은 owner가 재시작해 READY가 된 뒤에야 발행된다.
- Owner host를 영구히 잃으면 planned removal handoff 전까지 회수되지 않으며 handoff에서는 Will을 억제한다.
- Disconnect와 지연 Will 처리마다 commit 1회가 추가된다.
- Quorum 손실 중에는 Will을 발행하지 않는다.
- Feature 활성화 전 연결은 재접속 전까지 metadata record가 없다.
- Principal subject·roles 등 ACL 재검사 정보가 Raft state와 backup에 저장된다.
- Principal이 한도를 넘으면 저장되지 않으며 복구 Will은 ACL 재검사를 통과할 수 없다.

### 후속 작업

- 승인된 local 수용 범위는 [Cluster §11.24](../specs/cluster-session-and-delivery.md#1124-session-metadata-복제와-owner-손실-복구-수용-매핑)에서 모두 PASS다. 비활성 feature의 synthetic·지정 historical `58a95f8` release 호환성도 통과했다.
- Production storage, 이후 게시 release별 호환성과 tenant별 metadata quota는 이 결정의 완료 범위 밖이다. 기존 storage·release gate와 metadata의 cluster 전체 8 MiB 한도를 유지한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Metadata feature가 비활성일 때 기존 동작·wire bytes가 유지된다. | §11.24 비활성 codec reference vector와 기존 cluster regressions | Empty snapshot과 Session 없는 ownership encoding이 동일하고 비활성 시 metadata가 저장되지 않는다. | PASS — §11.24 codec reference vectors·runtime gate·10개 기존 process regressions |
| Owner crash 뒤 metadata 조회·재시작 recovery 및 만료 회수가 동작한다. | §11.24 dynamic 3-node process acceptance | Crash 이후 committed session 조회, 재시작 뒤 Will/expiry 복구, 만료 후 404와 capacity 회수를 확인한다. | PASS — §11.24 process acceptance와 focused tests |
| Planned handoff가 Will을 억제하고 detached timer를 절대 시각으로 재개한다. | §11.24 process/unit recovery scenarios | Handoff의 connected record는 Will 없이 expiry하며 detached record timer는 persisted due/expiry에 맞는다. | PASS — §11.24 connect/controller tests와 process acceptance |
| Will claim은 commit 이후 at-most-once이며 quorum failure에서 발행하지 않는다. | §11.24 connect transition 및 process acceptance | Claim commit 성공 후 단 한 번 발행하고 commit 실패 시 발행하지 않는다. | PASS — §11.24 committed-claim failure tests와 process acceptance |
| Local repository gate가 통과한다. | 승인 계획의 focused tests 및 `go run ./tools/verify local` | 계획에 정의된 테스트와 local gate 모두 통과한다. | PASS — §11.24 최종 local gate, traceability·architecture·fuzz·package/integration·race·coverage |

## 8. 재검토 조건

- Owner 재시작 외에 빠른 crash recovery가 요구되고 안전한 fencing 근거가 승인되면 failure detection 방식을 재검토한다.
- At-most-once Will 유실 가능성 또는 principal을 replicated backup에 저장하는 보안 경계가 요구사항과 맞지 않는 근거가 나오면 재검토한다.
- Feature 비활성 cluster의 wire reference vector가 달라지거나 metadata capacity/quota 요구가 변경되면 gate·한도를 재검토한다.
