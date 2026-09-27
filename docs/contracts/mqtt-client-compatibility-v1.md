# MQTT client compatibility v1 Contract

이 문서는 NaruMQ broker와 공식 지원 MQTT client가 연결, session resume와 계획 drain에서 지켜야 할 호환 profile을 정의한다. MQTT wire 의미는 OASIS 표준과 [MQTT spec](../specs/mqtt-protocol-and-session.md)을 기준으로 한다.

| 항목 | 값 |
| --- | --- |
| 이름과 version | `narumq-mqtt-client-compatibility` 1.0.0 |
| 상태 | Draft |
| Producer | NaruMQ broker와 MQTT client |
| Consumer | MQTT client와 NaruMQ broker |
| 방향 | MQTT bidirectional connection·packet flow |
| 기준 artifact | 이 문서의 profile과 compatibility matrix |
| 기준 spec | [MQTT protocol과 session](../specs/mqtt-protocol-and-session.md), [Drain과 rolling update](../specs/drain-and-rolling-update.md) |
| 관련 Architecture | [System overview](../architectures/system-overview.md), [Security boundaries](../architectures/security-boundaries.md) |
| 관련 requirement | MQTT-01~MQTT-13, CLIENT-01~CLIENT-09, DEP-06~DEP-12 |
| 호환성 범위 | MQTT 3.1.1 protocol level 4, MQTT 5.0 protocol level 5 |
| 검증 | `tests/compatibility/mqtt-clients-v1`의 strict Docker/E2E harness와 checked-in `profile.json` |

## 1. 공통 client 의무

모든 지원 profile의 client는 다음을 지켜야 한다.

- 재접속할 때 같은 인증 tenant와 Client ID를 사용한다.
- 자동 재접속에 exponential backoff와 full jitter를 적용한다.
- PUBACK 또는 PUBCOMP 전 outbound publish를 local outbox에 보관한다.
- QoS 1 consumer는 같은 application message ID의 중복 처리를 견딘다.
- Disconnect reason, reconnect attempt와 `Session Present` 결과를 관측 가능하게 기록한다.
- MQTT 3.1.0 `MQIsdp`와 protocol level 3을 사용하지 않는다.

Full jitter delay는 `random(0, min(cap, base × 2^attempt))` millisecond다. `base`, `cap`과 최대 attempt는 deployment profile이 정한다. 값이 없으면 SDK profile을 Active로 승인할 수 없다.

이 문서의 Phase 1 자동 검증에서만 사용하는 profile bound는 다음과 같다. 이 값은 production rollout default가 아니며, rollout과 reconnect의 운영 수치는 `SPEC-TBD-04`가 계속 소유한다.

| 항목 | Phase 1 test-only 값 |
| --- | --- |
| connect timeout | `10s` |
| operation timeout | `10s` |
| Session Expiry | `60s` |
| Will Delay | `5s` |
| reconnect within | `2s` |
| reconnect base / cap | `100ms` / `2s` |
| maximum attempts | `5` |
| deterministic jitter seed | `20260921` |

Profile status는 다음 의미만 사용한다.

| 상태 | 의미 |
| --- | --- |
| `phase1_verified` | 이 문서에 연결된 Phase 1 single-node 자동 case와 artifact가 모두 통과했다. |
| `blocked` | 현재 phase에 없는 broker 동작 또는 미결정 기준 때문에 실행할 수 없다. |
| `unverified` | 지원 후보이지만 수용된 자동 artifact가 아직 없다. |

전체 contract는 drain, Server Reference, rollout timing과 MQTT 3.1.0 migration evidence가 남아 있으므로 `Draft`를 유지한다. SDK matrix의 `phase1_verified`는 전체 contract를 `Active`로 승격하지 않는다.

## 2. Compatibility profile

| Profile | Protocol | Session 설정 | 계획 drain | Will 보장 |
| --- | --- | --- | --- | --- |
| `MQTT311_BASIC` | MQTT 3.1.1 | `Clean Session=1` 허용 | Connection 종료 뒤 공통 LB로 재접속 | Server 주도 종료에서 Will이 발행될 수 있다. |
| `MQTT311_PERSISTENT` | MQTT 3.1.1 | `Clean Session=0`, 같은 Client ID | Broker가 committed session state를 복구한 뒤 공통 LB로 재접속 | Will을 설정한 client는 no-Will 보장 대상이 아니다. |
| `MQTT5_STANDARD` | MQTT 5.0 | Workload 필요에 맞는 Clean Start·Session Expiry | `DISCONNECT 0x9C`를 처리하거나 공통 LB로 재접속 | Will Delay 설정에 따름 |
| `MQTT5_DRAIN_SAFE` | MQTT 5.0 | `Clean Start=0`, Session Expiry가 최악 배포 시간보다 큼 | `0x9C`와 Server Reference 처리, 같은 session key로 재접속 | Will Delay 안에 재접속하면 계획 drain Will을 억제한다. |

Persistent message 보장이 필요한 client는 `MQTT311_BASIC` 또는 Session Expiry 0인 `MQTT5_STANDARD`를 사용해서는 안 된다.

## 3. Server Reference 형식

NaruMQ가 MQTT 5 `DISCONNECT 0x9C`에 넣는 Server Reference는 다음 형식을 사용한다.

```text
dns-hostname:port
```

- 하나의 DNS hostname과 decimal port만 보낸다.
- URI scheme, path, query, user info와 공백 목록을 사용하지 않는다.
- Hostname은 해당 listener의 TLS certificate와 검증 가능한 이름이어야 한다.
- Client가 이 형식을 지원하지 않으면 configured common load balancer endpoint로 재접속해야 한다.
- `0x9C`는 임시 이동이다. Client는 permanent endpoint 설정을 바꾸지 않는다.

IPv6 literal과 여러 reference 목록은 v1 profile에서 지원하지 않는다. 필요하면 compatibility test를 추가하고 contract를 개정한다.

## 4. Session과 expiry

- Persistent profile은 재접속 뒤 `Session Present`를 확인해야 한다.
- `Session Present=0`이면 client는 필요한 subscription을 다시 만들고 local outbox를 기준으로 미완료 publish를 복구해야 한다.
- MQTT 5 Session Expiry는 `maximumRolloutDuration + reconnectSafetyMargin`보다 커야 한다.
- `MQTT5_DRAIN_SAFE` Will Delay는 `maximumReconnectDuration + willSafetyMargin`보다 커야 한다.
- 위 네 deployment 값의 숫자는 `SPEC-TBD-04`가 확정한다. 숫자가 없는 profile은 Draft다.

## 5. Drain과 Will matrix

| 조건 | Broker 동작 | Client 동작 | 분류 |
| --- | --- | --- | --- |
| MQTT 5, delay 안에 session resume | `0x9C`와 Server Reference 뒤 connection 종료 | 같은 session key로 재접속 | `suppressed_reconnected_within_delay` |
| MQTT 5, 정상 DISCONNECT 협력 | Broker의 session-state replication 완료 신호 제공 | Client가 정상 DISCONNECT 후 재접속 | `suppressed_normal_disconnect` |
| MQTT 5, Will Delay 0·만료 | 표준 시점에 Will 처리 | 재접속하더라도 이미 발행될 수 있음 | `expected_zero_or_expired_will_delay` |
| MQTT 3.1.1, Will 설정 | Broker가 committed session state를 보존한 뒤 connection 종료 | 공통 LB로 재접속 | `expected_mqtt311_server_disconnect` |
| `MQTT5_DRAIN_SAFE` 조건 충족인데 Will 발행 | 원인과 packet trace 기록 | Disconnect·resume trace 제공 | `unexpected_supported_client` |

## 6. SDK compatibility matrix

Phase 1 inventory는 다음 exact candidate minimum version으로 고정한다. 이 표의 상태는 SDK별 Phase 1 evidence 상태이며 “officially supported”를 의미하지 않는다.

| SDK | 최소 version | MQTT 3.1.1 | MQTT 5 | `0x9C`·Server Reference | Session resume | Phase 1 상태 |
| --- | --- | --- | --- | --- | --- | --- |
| Eclipse Paho Go MQTT 3.1.1 (`github.com/eclipse/paho.mqtt.golang`) | `v1.5.1` | `phase1_verified` | 해당 없음 | `blocked` | `phase1_verified` | `phase1_verified` |
| Eclipse Paho Go MQTT 5 (`github.com/eclipse/paho.golang`) | `v0.23.0` | 해당 없음 | `phase1_verified` | `blocked` | `phase1_verified` | `phase1_verified` |
| Eclipse Paho Python (`paho-mqtt`, callback API `VERSION2`) | `2.1.0` | `phase1_verified` | `phase1_verified` | `blocked` | `phase1_verified` | `phase1_verified` |
현재 `phase1_verified` 상태는 최종 source tree에서 실행한 `phase1-paho-go-python-readiness-atomic-final` artifact의 `summary.json`에서 26개 expected case가 모두 통과하고 missing/unexpected case가 0임을 확인한 뒤 기록했다. artifact 위치는 `.artifacts/compatibility/mqtt-clients-v1/phase1-paho-go-python-readiness-atomic-final/`이다.

각 matrix 행의 Phase 1 evidence는 다음 case ID에 연결된다.

```text
go-mqtt311-basic-roundtrip
go-mqtt311-persistent-resume
go-mqtt311-qos1-retransmit
go-mqtt311-qos2-retransmit
go-mqtt311-abrupt-will
go-mqtt5-basic-roundtrip
go-mqtt5-persistent-resume
go-mqtt5-qos1-retransmit
go-mqtt5-qos2-retransmit
go-mqtt5-will-delay-resume
python-mqtt311-basic-roundtrip
python-mqtt311-persistent-resume
python-mqtt311-qos1-retransmit
python-mqtt311-qos2-retransmit
python-mqtt311-abrupt-will
python-mqtt5-basic-roundtrip
python-mqtt5-persistent-resume
python-mqtt5-qos1-retransmit
python-mqtt5-qos2-retransmit
python-mqtt5-will-delay-resume
go-to-python-mqtt311-qos1
python-to-go-mqtt311-qos1
go-to-python-mqtt5-qos1
python-to-go-mqtt5-qos1
go-mqtt31-rejected
python-mqtt31-rejected
```

Cross-SDK QoS 1 case는 subscriber가 `ready.json`에 `schemaVersion`, `caseId`, `sdk`, `role=subscriber`, `protocol`, `ready=true`를 기록한 뒤에만 publisher를 시작한다. Orchestrator는 이 object를 strict decode하고 case·SDK·protocol을 검증한 뒤 publisher의 `readiness.json`으로 복사하여 `/input/readiness.json`에 read-only mount한다. Publisher는 파일이 없거나 형식·값이 다르면 publish하지 않고 failed result를 기록한다.

`MQTT5_DRAIN_SAFE`는 `DEP-06`~`DEP-12`와 `SPEC-TBD-04` 때문에 계속 `blocked`다. 현재 phase에는 broker drain controller, `DISCONNECT 0x9C`, Server Reference와 rollout timing이 없다. Harness는 redirect나 malformed server DISCONNECT를 흉내 내지 않는다. MQTT 3.1.0 90일 inventory와 migration isolation도 아직 `blocked`다.

SDK matrix row는 연결된 Phase 1 case가 모두 통과하고 immutable image/result artifact가 보존된 뒤에만 `phase1_verified`가 될 수 있다. required drain, Will 또는 migration evidence가 하나라도 `blocked`인 동안에는 이 Draft contract를 `Active`로 승격하지 않는다.

### 6.1 Client-result schema

각 SDK driver는 `schemaVersion`, `caseId`, `sdk`, `sdkVersion`, `protocol`, `profile`, `role`, `status`, `startedAt`, `finishedAt`, `observations`와 선택적 `error`를 포함하는 strict result object 하나를 기록한다. failed result에는 `error`가 반드시 있어야 한다. Orchestrator는 알 수 없는 schema version, 불일치하는 identity field, 누락된 required observation, 중복 role result와 예상 밖 case result를 거부한다.

## 7. 호환성 변경

- 새 optional profile 추가는 기존 profile 의미를 바꾸지 않으면 호환된다.
- 기존 profile의 Session, retry, Will 또는 Server Reference 의미 변경은 breaking change다.
- 지원 SDK 최소 version 상향은 consumer migration과 deprecation 기간이 필요하다.
- Broker release는 Active SDK matrix를 N-1↔N 양쪽에서 실행해야 한다.

## 8. Draft blocker

- `SPEC-TBD-04`: rollout·reconnect 시간과 production backoff 수치
- `DEP-06`~`DEP-12`: drain controller, redirect, rollout barrier와 관련 운영 동작
- `MQTT5_DRAIN_SAFE`: `DISCONNECT 0x9C`, Server Reference, malformed disconnect와 drain Will evidence
- SDK별 automated compatibility harness의 Phase 1 artifact가 없는 후보는 `unverified`
- MQTT 3.1.0 90일 관찰과 legacy 격리 완료 전까지 migration evidence가 blocked

## 9. 참고 자료

- [OASIS MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)
- [OASIS MQTT 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
