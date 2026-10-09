# ADR-0042: P0 요구사항 추적성 gate

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-09 |
| 결정자 | 사용자 |
| 관련 요구사항 | [TEST-10](../requirements/PRD.md#115-테스트와-coverage) |
| 관련 스펙·계약 | [Performance와 verification §7.2](../specs/performance-and-verification.md#72-phase-0-mutationfuzz-게이트) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md#verification-and-integration-test-entrypoints) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

1. Go test와 fuzz 함수의 doc comment에서 `Traceability:`를 유일한 추적성 metadata marker로 사용한다.
2. `traceability` gate는 marker의 요구사항 ID가 PRD에 있는지 검사하고, 모든 P0 요구사항을 하나 이상의 test 함수에 연결한다.
3. test 연결이 없는 P0 요구사항은 `tests/conformance/p0-requirement-exceptions-v1.json`에 이유와 저장소 내 추적 경로를 등록할 수 있다. 예외는 P0 ID 오름차순으로 관리한다.
4. 시험 연결이 생긴 예외는 gate 실패로 처리한다. 연결과 예외가 모두 없는 P0 요구사항도 gate 실패다.
5. Unit 시험과 수용 시험은 구분하지 않는다.
6. 예외가 남아 있거나 필수 CI merge gate 실행 증거가 없으면 TEST-10은 미검증으로 유지한다. 로컬 gate 통과만으로 TEST-10 완료를 선언하지 않는다.

## 2. 배경

기존 `go run ./tools/verify traceability`는 MQTT normative statement와 compliance matrix의 연결만 확인했다. PRD의 P0 요구사항이 시험에 연결되었는지는 검사하지 않았다. PRD에는 P0 요구사항이 81개 있으며, 구현·시험이 없는 요구사항을 예외로 명시하고 이후 누락을 늘리지 않는 규칙이 필요하다.

## 3. 결정 요인

1. P0 요구사항의 누락과 오래된 예외를 자동으로 거절한다.
2. PRD를 요구사항 ID와 우선순위의 기준으로 유지한다.
3. 기존 test comment 추적 방식과 MQTT compliance matrix의 책임을 보존한다.
4. 실제 CI merge 차단 증거가 없을 때 완료로 오인하지 않는다.

## 4. 선택 근거

PRD 표에서 모든 요구사항 ID와 P0 ID를 읽으면 수동 요구사항 목록의 중복을 피할 수 있다. 단일 `Traceability:` marker를 사용하면 같은 test comment에서 MQTT statement와 일반 PRD requirement를 함께 추적할 수 있다. 예외 목록은 이유와 추적 경로를 요구하고, 정렬·중복·stale 상태를 gate가 확인하므로 현재 shortfall을 기록하면서 새 누락이 쌓이는 것을 막는다.

현재 local traceability command는 P0 81개 중 58개에 test 연결을 확인하고 23개 예외를 허용한다. 이 결과는 gate 동작의 증거이며 TEST-10 완료 증거는 아니다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 기존 MQTT compliance matrix에 모든 PRD 요구사항을 추가 | 기존 verifier 경로를 재사용한다. | Normative MQTT statement와 일반 test-to-requirement 연결의 책임을 한 파일에 섞는다. | Matrix는 MQTT conformance 계약을 유지한다. |
| 예외 없이 미연결 P0를 모두 거절 | 규칙이 단순하다. | 현재 shortfall을 추적 가능한 형태로 표시하지 못하고 초기 gate를 사용할 수 없다. | 사용자가 예외 목록 ratchet 방식을 채택했다. |
| test comment 밖의 별도 요구사항 목록을 수동 관리 | PRD parser가 필요 없다. | PRD ID·우선순위와 중복되어 drift할 수 있다. | PRD를 단일 기준으로 읽는다. |

## 6. 결과

### 긍정적 결과

- P0 연결이 없거나 오래된 예외가 남으면 `traceability`가 실패한다.
- PRD에 없는 ID도 위치와 함수 이름을 포함해 보고한다.
- 기존 MQTT compliance matrix는 변경하지 않는다.

### 부정적 결과와 비용

- P0 예외마다 이유와 추적 경로를 유지해야 한다.
- 현재 23개 예외가 남아 있어 TEST-10은 미검증이다.
- CI merge 차단은 실제 필수 status 실행 증거가 생길 때까지 입증되지 않는다.

### 후속 작업

- 각 예외의 근거가 해결되면 test marker를 추가하거나 요구사항·범위를 승인된 문서 변경으로 조정하고 예외를 제거한다.
- 필수 CI merge gate를 구성하고 실제 실행으로 차단 동작을 확인한다.

## 7. 검증

| 결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| PRD ID·P0 연결과 예외 ratchet | `tools/verify/requirements_test.go::TestVerifyP0RequirementMapping` | 정상, 미연결, stale, non-P0, unknown, unsorted, tracking과 PRD 중복 사례 판정 | `go test ./tools/verify -count=1 -v -run 'P0|Traceability'` PASS |
| 예외 JSON·경로 경계와 marker 파싱 | `TestLoadP0ExceptionsStrictlyValidatesJSON`, `TestParseTraceabilityAcceptsPRDPrefixesAndKeepsStatementRule` | strict decode, 경로, PRD prefix와 MQTT statement 규칙 판정 | 같은 focused suite PASS |
| 저장소 traceability gate | `go run ./tools/verify traceability` | MQTT matrix PASS, P0 요약 출력 | `traceability: PASS (P0 mapped 58/81, exceptions 23)` |

## 8. 재검토 조건

- PRD 요구사항 표의 형식이 바뀌어 현재 parser가 ID와 우선순위를 정확히 읽지 못하는 경우
- 예외 목록을 대체할 더 엄격한 운영 계약을 채택하는 경우
- 모든 P0 연결이 해결되고 TEST-10의 CI merge 차단 증거가 확보된 경우
