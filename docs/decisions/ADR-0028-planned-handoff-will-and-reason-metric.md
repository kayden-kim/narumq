# ADR-0028: 계획 인계의 표준 Will 처리와 사유별 집계

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (2026-10-04 사용자 결정) |
| 결정일 | 2026-10-04 |
| 결정자 | NaruMQ 프로젝트 사용자 |
| 관련 요구사항 | [PRD DEP-07·DEP-12](../requirements/PRD.md), MQTT-3.1.2-8 |
| 관련 스펙·데이터 계약 | [Drain §5·§10](../specs/drain-and-rolling-update.md), [Cluster §4.3](../specs/cluster-session-and-delivery.md#43-session-metadata-복제와-owner-손실-복구), [Metric 계약](../specs/management-api-and-observability.md#7-metric-계약) |
| 관련 아키텍처 | [Project structure](../architectures/project-structure.md), [Cluster architecture](../architectures/cluster-architecture.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | [ADR-0016](ADR-0016-node-drain-and-safe-to-stop.md) §1 항목 2의 Will 억제 범위 (부분), [ADR-0025](ADR-0025-replicated-session-metadata-and-owner-loss-recovery.md) §1 항목 3·6 (부분) |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

1. Drain과 planned member removal의 계획 인계 close는 서버가 시작한 비정상 종료다. 후계 node는 MQTT 3.1.1, Will Delay 0 또는 retention `IMMEDIATE`이면 Will을 즉시 claim·발행한다. 나머지 MQTT 5 연결은 Will Delay로 예약한다. Delay는 session expiry로 제한하고 후계 recovery 시각부터 잰다.
2. 이전 owner는 Will을 결정하지 않는다. TCP takeover·drain cleanup은 `DisconnectTakeover`로 local Will을 버리고 집계하지 않는다. 같은 epoch의 live client takeover 억제는 유지한다. ADR-0016의 “Ownership takeover close는 이전 연결의 Will을 억제한다”는 이전 owner의 local 처리만 뜻한다.
3. Recovery 전에 재접속하면 ownership Acquire/Transfer apply가 이전 record를 대체한다. 이전 epoch의 `CONNECTED` record에 남은 Will을 apply 결과로 반환한다. 재접속 node는 항목 1의 즉시 발행 조건이면 발행한다. 그 밖에는 `suppressed_reconnected_within_delay`로 집계한다. `DETACHED` record의 예약 Will을 대체해도 같은 사유로 집계한다. ADR-0025의 “재접속은 Will을 취소”에는 이 즉시 발행 예외를 적용한다.
4. `session-metadata` feature가 비활성이면 기존 drain Will 억제를 유지하며 집계하지 않는다. 표준 drain Will 처리는 feature 활성을 전제로 한다.
5. Replicated schema와 wire는 바꾸지 않는다. 예약 Will의 계획/비계획 출처는 예약 node의 memory에만 둔다. Process 재시작이나 detached session 이전 뒤 metadata에서 복원한 Will은 `unplanned_failure`로 집계한다. Mixed N-1/N에서 N-1 후계는 기존처럼 Will을 버린다.
6. Will 하나는 결과를 확정한 node에서 정확히 한 번 집계한다. Client `DISCONNECT 0x04`, takeover close, ACL 거절·metadata commit 실패로 발행하지 않은 Will, feature 비활성 drain은 집계하지 않는다. `unexpected_supported_client`는 계획 인계 MQTT 5·Will Delay>0·retention≠`IMMEDIATE`인데 즉시 발행한 broker 규칙 위반만 탐지한다. Client 증거는 기존 SDK drain suite가 맡는다. 사유별 표는 [Drain §5](../specs/drain-and-rolling-update.md#5-will-판정과-관측)를 따른다.

## 2. 배경

이전 owner의 takeover cleanup과 후계 recovery가 모두 정상 종료처럼 Will을 버렸다. 서버가 시작한 종료에서도 Will 규칙을 적용해야 하는 MQTT-3.1.2-8 및 DEP-12와 맞지 않았다. 후계 recovery와 재접속이 경쟁하므로 두 경로 모두 committed 대체·claim 결과를 기준으로 처리해야 한다.

## 3. 결정 요인

1. 표준 Will 규칙과 owner/epoch fencing을 함께 지킨다.
2. 같은 Will을 두 node에서 발행하거나 집계하지 않는다.
3. N-1 apply 및 기존 replicated schema를 보존한다.
4. Metric label을 고정한 6개 사유로 제한한다.

## 4. 선택 근거

이전 owner의 local 폐기와 후계의 표준 판단을 분리한다. Ownership apply 결과에 대체된 Will을 실으면 추가 replicated field 없이 recovery 전 재접속을 처리한다. 별도 `WillOutcomeRecorder` port를 사용하여 cluster connect의 auth metric과 O(n) session gauge 계산을 새로 켜지 않는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 기존 억제 유지 | 구현 변경 없음 | 서버 종료의 Will을 항상 버림 | MQTT-3.1.2-8·DEP-12 위반 |
| Will 출처를 replicated field로 저장 | 재시작 뒤 정확한 사유 보존 | Unknown field를 거절하는 N-1 schema에서 apply가 갈라질 수 있음 | Release 호환성 변경을 요구함 |

## 6. 결과

### 긍정적 결과

- 계획 인계에서도 MQTT 3.1.1과 MQTT 5 Will Delay 규칙을 적용한다.
- 예상 발행과 재접속 억제를 broker metric으로 구분한다.

### 부정적 결과와 비용

- Feature 비활성 cluster와 N-1 후계에서는 기존 억제가 남는다.
- 복원한 예약 Will의 출처는 알 수 없어 `unplanned_failure`로 분류한다.

### 후속 작업

- 외부 rollout과 최대 traffic 연속 100회 rolling은 기존 열린 gate로 유지한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| 표준 규칙·집계·대체 결과 | Focused domain/membership/connect/metrics/TCP/runtime 시험 | 모든 새 시나리오 PASS | 2026-10-04 PASS; 명령·시간은 [Drain §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) 참조 |
| 연속 rolling | `TestDynamicClusterDrainWillReasonProcessAcceptance` | 3 round PASS, unexpected 0, reconnected 합계 3 | 2026-10-04 PASS; 49.06s, round별 delta와 observer 결과는 [Drain §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) 참조 |
| 영향 process 회귀 | Drain·owner crash·removal·QoS 2 Will·takeover 6개 시험 | 모두 PASS | 2026-10-04 PASS; package 39.569s, [Drain §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) 참조 |
| 전체 local gate | `GOMAXPROCS=4 go run ./tools/verify local` | PASS | 2026-10-04 PASS; 최초 통과 wall 298.08s. ACL 경계 3개 시험 추가 뒤 focused·local 재검증도 PASS(두 명령 합계 286.02s). 첫 실행의 requirement metadata 누락과 mapping 순서는 수정했다. 상세 증거는 [Drain §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) 참조 |
| 이전 Will의 ACL 경계 | 새 CONNECT에 Will을 넣지 않고 허용·topic 거절·principal 없음의 3개 case 확인 | 허용이면 한 번 발행·집계, 거절이면 새 CONNECT 성공·이전 Will 발행/집계 없음 | 2026-10-04 focused PASS(package 0.406s), `go run ./tools/verify traceability` PASS. `SEC-06`과 MQTT compliance mapping도 함께 확인했다. 제품 코드 변경 없이 시험 입력·연결만 정리했으며 상세 증거는 [Drain §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑) 참조 |

## 8. 재검토 조건

- 비활성 cluster에서도 표준 drain 처리가 필요할 때 재검토한다.
- 복원한 예약 Will의 정확한 출처 보존이 필요하면 schema와 release 호환성을 함께 결정한다.
