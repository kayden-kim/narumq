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
| 검증 | `tests/compatibility/mqtt-clients-v1`의 strict Docker/E2E harness. `profile.json`은 필수 Phase 1, `drain-profile.json`은 필수 drain과 선택적 SDK 진단의 case 목록을 정한다. |

## 1. 공통 client 의무

모든 지원 profile의 client는 다음을 지켜야 한다.

- 재접속할 때 같은 인증 tenant와 Client ID를 사용한다.
- 자동 재접속에 exponential backoff와 full jitter를 적용한다.
- PUBACK 또는 PUBCOMP 전 outbound publish를 local outbox에 보관한다.
- QoS 1 consumer는 같은 application message ID의 중복 처리를 견딘다.
- Disconnect reason, reconnect attempt와 `Session Present` 결과를 관측 가능하게 기록한다.
- MQTT 3.1.0 `MQIsdp`와 protocol level 3을 사용하지 않는다.

Full jitter delay는 `random(0, min(cap, base × 2^attempt))` millisecond다. Deployment profile이 따로 정하지 않으면 [ADR-0055](../decisions/ADR-0055-drain-and-reconnect-operating-defaults.md)에 따라 `base=1000ms`, `cap=30000ms`, 최대 attempt `10`을 쓴다.

아래 값은 Phase 1과 drain suite의 자동 검증에만 사용한다. Production rollout 기본값이 아니다. 운영 기본값은 §4와 [ADR-0055](../decisions/ADR-0055-drain-and-reconnect-operating-defaults.md)를 따른다.

| 항목 | Phase 1 test-only 값 | Drain test-only 값 |
| --- | --- | --- |
| connect timeout | `10s` | `10s` |
| operation timeout | `10s` | `45s` |
| Session Expiry | `60s` | `60s` |
| Will Delay | `5s` | `5s` |
| reconnect within | `2s` | `2s` |
| reconnect base / cap | `100ms` / `2s` | `100ms` / `2s` |
| maximum attempts | `5` | `5` |
| deterministic jitter seed | `20260921` | `20260921` |
| LB 전파 대기 | 해당 없음 | `1000ms` |
| drain connection rate | 해당 없음 | `10/s` |
| drain timeout | 해당 없음 | `60s` |

Profile status는 다음 의미만 사용한다.

| 상태 | 의미 |
| --- | --- |
| `phase1_verified` | 이 문서에 연결된 Phase 1 single-node 자동 case와 artifact가 모두 통과했다. |
| `drain_verified` | §6.2의 해당 SDK 정상 drain case와 필수 artifact가 모두 통과했다. 운영 rollout 승인이나 SDK의 자동 redirect 지원을 뜻하지 않는다. |
| `blocked` | 필수 broker 동작, 결정되지 않은 기준 또는 아직 충족하지 못한 compatibility evidence 때문에 시험을 통과시킬 수 없다. |
| `unverified` | 지원 후보이지만 수용된 자동 artifact가 아직 없다. |

전체 contract는 rollout timing 수용 증거가 남아 있으므로 `Draft`를 유지한다. SDK matrix의 부분 검증 상태는 전체 contract를 `Active`로 승격하지 않는다. §6.3의 SDK 진단 실패는 제품 승인 차단 조건이 아니다. MQTT 3.1.0 client의 관찰·전환·격리는 2026-10-17 사용자 결정으로 이 contract의 승격 조건에서 제외했다([PRD §9.1](../requirements/PRD.md#mqtt-310-비지원과-이전-정책)).

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

이 Draft 중 §6.2의 test-only drain suite와 아래 callback adapter 규칙은 승인된 구현 기준이다. Production 시간 값과 rollout 승인은 이 채택 범위에 포함하지 않는다.

Callback adapter는 Server Reference가 다음 조건을 모두 만족할 때만 수락해야 한다.

- 공백과 `,`, `/`, `?`, `#`, `@`, `[`, `]`이 없어야 한다.
- 마지막 `:`를 기준으로 host와 port를 나눈다.
- Port는 10진 숫자만 허용한다. 범위는 1–65535다.
- Host는 253자 이하여야 한다. 각 label은 1–63자이며 `[A-Za-z0-9-]`만 허용한다.
- Label은 `-`로 시작하거나 끝나면 안 된다. IP literal은 거절한다.

두 MQTT 5 SDK는 Server Reference를 자동으로 따라가지 않는다. Application callback adapter가 server DISCONNECT `0x9C`와 유효한 reference를 받으면 다음 접속 한 번만 참조 endpoint로 시도한다. 다른 reason, 형식 위반, SDK parse 오류 또는 연결 손실이면 configured 공통 endpoint로 재접속한다. 참조 endpoint의 TCP 오류, CONNACK 거절 또는 CONNACK 전 종료도 공통 endpoint로 돌아가는 조건이다. Permanent endpoint 설정은 바꾸지 않는다.

SDK별 실행 가능한 기준 설정 예시는 다음과 같다.

- [Go `drain.go`](../../tests/compatibility/mqtt-clients-v1/clients/go/drain.go)의 `newDrainSafeClient`: `OnServerDisconnect`가 reason·reference를 검증하고 크기 1 channel에 결정을 기록한다. `OnClientError`는 공통 endpoint 결정을 기록한다. `AttemptConnection`은 callback 경합을 피하려고 첫 재접속에서 최대 1초 기다린 뒤 결정을 한 번 소비한다. 이후 시도는 공통 endpoint를 쓴다. `RequestProblemInfo=true`는 MQTT 기본값이며 Paho가 property `0x17`을 생략하게 한다. 기존 broker의 CONNECT property 범위를 따른 설정이다.
- [Python `client.py`](../../tests/compatibility/mqtt-clients-v1/clients/python/client.py)의 `DrainSafeClient`: VERSION2 `on_disconnect`가 server packet 여부·reason·reference를 기록한다. Adapter thread는 `reconnect_on_failure=False`와 수동 `loop(timeout=0.1)`를 사용한다. SDK parse 예외를 잡으면 socket을 닫고 공통 endpoint로 재접속한다. 첫 disconnect 분류를 유지한다.

두 예시는 같은 Client ID·인증 정보·Session Expiry를 유지한다. 첫 연결만 Clean Start를 사용한다. 재접속은 §1의 full jitter와 최대 5회 시도를 적용한다. 이 예시는 malformed property를 SDK 밖에서 검사하지 않는다. Go의 중복 Server Reference 수락은 §6.3의 알려진 SDK 진단 실패로 보존한다. 직접 패치한 SDK나 raw socket 검사를 도입하지 않는다. 이 profile은 NaruMQ의 정상 응답에 대한 연동을 보장하며, 잘못된 broker 응답을 모두 탐지하는 SDK의 방어 수준까지 보장하지 않는다.


## 4. Session과 expiry

- Persistent profile은 재접속 뒤 `Session Present`를 확인해야 한다.
- `Session Present=0`이면 client는 필요한 subscription을 다시 만들고 local outbox를 기준으로 미완료 publish를 복구해야 한다.
- MQTT 5 Session Expiry는 `maximumRolloutDuration + reconnectSafetyMargin`보다 커야 한다.
- `MQTT5_DRAIN_SAFE` Will Delay는 `maximumReconnectDuration + willSafetyMargin`보다 커야 한다.
- `maximumRolloutDuration=3600s`, `reconnectSafetyMargin=600s`이므로 Session Expiry는 4200초보다 커야 한다. `maximumReconnectDuration=60s`, `willSafetyMargin=30s`이므로 `MQTT5_DRAIN_SAFE` Will Delay는 90초보다 커야 한다([ADR-0055](../decisions/ADR-0055-drain-and-reconnect-operating-defaults.md)).

## 5. Drain과 Will matrix

| 조건 | Broker 동작 | Client 동작 | 분류 |
| --- | --- | --- | --- |
| MQTT 5, delay 안에 session resume | `0x9C`와 Server Reference 뒤 connection 종료 | 같은 session key로 재접속 | `suppressed_reconnected_within_delay` |
| MQTT 5, 정상 DISCONNECT 협력 | Broker의 session-state replication 완료 신호 제공 | Client가 정상 DISCONNECT 후 재접속 | `suppressed_normal_disconnect` |
| MQTT 5, Will Delay 0·만료 | 표준 시점에 Will 처리 | 재접속하더라도 이미 발행될 수 있음 | `expected_zero_or_expired_will_delay` |
| MQTT 3.1.1, Will 설정 | Broker가 committed session state를 보존한 뒤 connection 종료 | 공통 LB로 재접속 | `expected_mqtt311_server_disconnect` |
| `MQTT5_DRAIN_SAFE` 조건 충족인데 Will 발행 | 원인과 packet trace 기록 | Disconnect·resume trace 제공 | `unexpected_supported_client` |

## 6. SDK compatibility matrix

SDK 후보의 최소 version은 아래 값으로 고정한다. 각 열은 연결된 시험 범위의 evidence 상태다. “공식 지원 승인”을 뜻하지 않는다.

| SDK | 최소 version | MQTT 3.1.1 | MQTT 5 | Drain redirect | Session resume | Phase 1 상태 |
| --- | --- | --- | --- | --- | --- | --- |
| Eclipse Paho Go MQTT 3.1.1 (`github.com/eclipse/paho.mqtt.golang`) | `v1.5.1` | `phase1_verified` | 해당 없음 | 해당 없음 | `phase1_verified` | `phase1_verified` |
| Eclipse Paho Go MQTT 5 (`github.com/eclipse/paho.golang`) | `v0.23.0` | 해당 없음 | `phase1_verified` | `drain_verified` | `phase1_verified` | `phase1_verified` |
| Eclipse Paho Python (`paho-mqtt`, callback API `VERSION2`) | `2.1.0` | `phase1_verified` | `phase1_verified` | `drain_verified` | `phase1_verified` | `phase1_verified` |
현재 `phase1_verified` 상태는 최종 SDK adapter source에서 실행한 `phase1-drain-adapters-final` artifact의 `summary.json`을 기준으로 한다. Expected case 26개가 모두 통과했고 missing·unexpected case는 0이다. Artifact는 `.artifacts/compatibility/mqtt-clients-v1/phase1-drain-adapters-final/`이다. Suite 선택 직후의 `phase1-suite-flag`도 26/26으로 통과했다.

기존 혼합 suite의 `.artifacts/compatibility/mqtt-clients-v1/drain-paho-go-python-will-acl-final/`에는 정상 drain 2개와 SDK 진단 8개의 결과가 있다. 당시 전체 판정 9/10 FAIL은 변경하지 않는다. 정상 drain 2개는 모두 통과했고 cluster redirect·session resume·후속 전달·Will 억제를 확인했다. SDK 진단은 7/8 통과했으며 Go의 중복 Server Reference 1개가 실패했다. 현재 제품 승인에는 정상 drain 결과만 사용한다. MQTT 3.1.1에는 server DISCONNECT가 없으므로 drain redirect 열은 해당하지 않는다.

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

`MQTT5_DRAIN_SAFE`의 test-only redirect·Will evidence는 §6.2에서 확인했다. 전체 profile은 production rollout timing의 수용 증거가 남아 있어 계속 `blocked`다. SDK 진단 실패는 이 판정에 포함하지 않는다. Broker-side drain controller와 `DISCONNECT 0x9C`/Server Reference 송신은 [Drain spec §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)에 연결한다.

SDK matrix row는 연결된 Phase 1 case가 모두 통과하고 immutable image/result artifact가 보존된 뒤에만 `phase1_verified`가 될 수 있다. required drain 또는 Will evidence가 하나라도 `blocked`인 동안에는 이 Draft contract를 `Active`로 승격하지 않는다.

### 6.1 Client-result schema

각 SDK driver는 `schemaVersion`, `caseId`, `sdk`, `sdkVersion`, `protocol`, `profile`, `role`, `status`, `startedAt`, `finishedAt`, `observations`와 선택적 `error`를 포함하는 strict result object 하나를 기록한다. failed result에는 `error`가 반드시 있어야 한다. Orchestrator는 알 수 없는 schema version, 불일치하는 identity field, 누락된 required observation, 중복 role result와 예상 밖 case result를 거부한다.

### 6.2 Drain compatibility suite

2026-10-16 사용자 결정에 따라 제품 필수 호환성 검증과 선택적 SDK 진단을 분리한다. 필수 검증은 NaruMQ의 올바른 MQTT 출력과 지원 SDK·callback adapter의 정상 연동이다. Fake server가 생성한 비정상 응답에 대한 방어 수준은 §6.3에서 별도로 관찰한다. 이 변경은 SDK의 중복 수락을 수정하거나 통과로 바꾸는 결정이 아니다.

NaruMQ 자체 decoder의 잘못된 client 입력 처리와 broker MQTT conformance 기준은 완화하지 않는다. 이번 분리는 fake server 응답을 받는 외부 SDK·adapter의 진단 범위에만 적용한다.

[`drain-profile.json`](../../tests/compatibility/mqtt-clients-v1/drain-profile.json)은 정상 drain 2개와 SDK 진단 8개를 포함한 고정 목록이다. `--suite drain`은 `kind=cluster_drain`인 다음 2개만 선택한다. 실행 artifact의 `profile.json`·manifest·summary에는 선택한 case만 기록한다. 각 case는 해당 SDK의 `client` 결과 하나를 요구한다.

```text
go-mqtt5-drain-redirect
python-mqtt5-drain-redirect
```

Case마다 새 3-node broker와 시험 전용 readiness LB를 만든다. LB는 `/health/ready`가 200인 첫 backend에만 새 연결을 전달한다. 우선순위는 node3, node1, node2다. Backend가 unready가 되어도 기존 연결은 끊지 않는다. 실제 NLB·Kubernetes 시험을 대신하지 않는다. JWT fixture는 세 Client ID에 `compat/#`의 subscribe·publish·will 권한을 부여한다. Will은 publish와 별도의 ACL action이므로 이 권한이 없으면 CONNECT가 거절된다.

Broker별 시험 데이터 tmpfs 상한은 2GiB다. 기본 디스크 여유 기준 1GiB를 충족하도록 하며 backpressure 정책은 낮추지 않는다. Container memory limit은 512MiB로 유지한다. 이 저장소는 기능 smoke 전용이며 실제 disk 영속성의 수용 증거가 아니다.

Driver readiness 시점까지 LB `connect`는 정확히 하나여야 한다. Backend는 `node3.compat.test:1883`이어야 한다. Leader에 node3 drain을 요청한 뒤 client는 `disconnectReasonCode=156`과 `serverReference=node2.compat.test:1883`를 기록해야 한다. 같은 endpoint로 2초 안에 재접속하고 `sessionPresent=true`, `postRedirectDelivery=true`를 확인해야 한다. Disconnect 분류 시각에서 Will Delay 5초와 추가 1초가 지난 뒤에도 `willReceived=false`여야 한다. 분류는 `suppressed_reconnected_within_delay`이며 permanent endpoint는 `lb.compat.test:1883`로 유지해야 한다. Drain 조회는 `phase=COMPLETED`, `state=SAFE_TO_STOP`이어야 한다.

저장소 root에서 Go 1.26.5와 Docker의 Linux/amd64 환경을 준비한 뒤 실행한다. Run ID와 output은 기존 artifact와 겹치지 않아야 한다.

```text
go run ./tests/compatibility/mqtt-clients-v1 --suite drain \
  --run-id drain-required-storage-fixed-20261016 \
  --output .artifacts/compatibility/mqtt-clients-v1/drain-required-storage-fixed-20261016
```

두 필수 case가 모두 통과하고 missing·unexpected case가 0이어야 한다. SDK 진단은 이 실행에 포함하지 않는다. Phase 1 26개와 정상 drain 2개는 계속 필수다. 운영 rollout과 migration의 미검증 조건은 이 변경으로 해제하지 않는다.

각 run은 manifest·summary·case JSONL·build log·image inspect를 보존한다. `cases/<caseId>/`에는 `client/result.json`, client log, `lb/lb-trace.jsonl`, `drain-operation.json`과 node별 log·inspect가 있다. SDK result가 `passed`여도 observation 또는 trace가 계약을 위반하면 orchestrator의 case·summary는 실패다.

기존 `drain-paho-go-python-will-acl-final`의 정상 case는 모두 통과했다. Go 재접속은 89ms, Python은 약 99.071ms였다. 두 case 모두 session resume·후속 payload·Will 억제와 `SAFE_TO_STOP`을 확인했다. 초기 run `drain-paho-go-python`과 `drain-paho-go-python-connect-fixed`는 harness 설정 오류 때문에 cluster case를 통과하지 못했다. Go CONNECT 기본 property 설정과 누락된 `will` ACL을 고친 뒤 최종 run을 실행했다. 초기 artifact도 보존하며 호환성 판정에는 최종 run만 사용한다.

### 6.3 선택적 SDK 진단

`--suite sdk-diagnostics`는 같은 고정 목록에서 `kind=malformed_disconnect`인 다음 8개만 실행한다. NaruMQ 대신 fake server가 잘못된 응답을 생성한다. `invalid_server_reference_format`은 MQTT 전체의 금지 형식이 아니라 NaruMQ adapter의 제한된 주소 형식을 시험한다. 이 진단은 제품 필수 release gate와 `drain_verified` 판정에 포함하지 않는다.

```text
go-mqtt5-malformed-truncated-property
go-mqtt5-malformed-disallowed-property
go-mqtt5-malformed-duplicate-server-reference
go-mqtt5-malformed-invalid-server-reference
python-mqtt5-malformed-truncated-property
python-mqtt5-malformed-disallowed-property
python-mqtt5-malformed-duplicate-server-reference
python-mqtt5-malformed-invalid-server-reference
```

```text
go run ./tests/compatibility/mqtt-clients-v1 --suite sdk-diagnostics \
  --run-id sdk-diagnostics-20261016 \
  --output .artifacts/compatibility/mqtt-clients-v1/sdk-diagnostics-20261016
```

선택적으로 실행했더라도 진단 실패를 숨기지 않는다. Observation·trace가 기대와 다르거나 결과가 누락되면 해당 case와 SDK 진단 summary는 실패이며 명령은 nonzero exit다. 이를 필수 drain 실행이나 제품 승인 실패로 합치지 않는다. Case·SDK·version 식별자와 원본 실패 artifact는 유지한다.

각 case는 scripted fake server와 별도 trap listener를 사용한다. `<trap>`은 `localhost:1886`의 UTF-8 hex `6C 6F 63 61 6C 68 6F 73 74 3A 31 38 38 36`이다.

| Vector | DISCONNECT wire hex |
| --- | --- |
| `truncated_property` | `E0 07 9C 05 1C 00 FF 74 72` |
| `disallowed_property` | `E0 16 9C 14 23 00 01 1C 00 0E <trap>` |
| `duplicate_server_reference` | `E0 24 9C 22 1C 00 0E <trap> 1C 00 0E <trap>` |
| `invalid_server_reference_format` | `E0 1A 9C 18 1C 00 15 6D 71 74 74 3A 2F 2F <trap>` |

첫 main CONNECT 뒤 vector를 한 번 보내고 재접속을 기다린다. Main `connect`는 정확히 두 개여야 한다. 두 Client ID는 `compat-drain-client`여야 한다. Main·trap listener는 각각 한 번 열려야 하며 `trap_accept`는 없어야 한다. 진단의 기대 결과는 `serverReferenceAccepted=false`와 reconnect·permanent endpoint `localhost:1883`다. Disconnect surface는 `server_disconnect`, `client_error`, `sdk_exception`, `connection_lost` 중 하나다. Case artifact에는 `fakeserver/fakeserver-trace.jsonl`이 있다.

기존 혼합 run의 진단은 Python 4/4, Go 3/4 통과다. Go 중복 vector는 `server_disconnect`, `serverReferenceAccepted=true`, `trap_accept=1`을 기록했다. SDK가 중복 Server Reference를 덮어쓰므로 adapter가 유효한 참조로 판단했다. 이후 공통 endpoint로 돌아왔지만 진단 기대를 충족하지 못했다. Python은 중복 property를 `sdk_exception`으로 거절했다. 나머지 진단은 trap 접속 없이 통과했다.

원인은 SDK decoder다. [`v0.23.0`의 `Properties.Unpack`](https://github.com/eclipse-paho/paho.golang/blob/v0.23.0/packets/properties.go#L581-L606)은 property가 해당 packet에 허용되는지만 검사한다. [`PropServerReference` 분기](https://github.com/eclipse-paho/paho.golang/blob/v0.23.0/packets/properties.go#L697-L702)는 같은 필드에 값을 다시 대입하며 중복 여부를 검사하지 않는다. [MQTT 5 §3.14.2.2.5](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901214)는 Server Reference 중복을 Protocol Error로 규정한다. [§4.13.1](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901259)은 수신 검사의 엄격성에 구현상 절충이 있음을 설명하며, 오류를 감지한 Client의 연결 종료는 SHOULD다. 이 계약은 SDK가 모든 비정상 응답을 탐지한다고 보장하지 않는다.

원인 분석용 probe는 `first.example:1883` 뒤에 `second.example:1883`을 넣었다. 결과는 `parseError=<nil>`, `decodedReference="second.example:1883"`이었다. 첫 참조가 빈 문자열인 경우도 같았다. Callback은 두 번째 값만 받으므로 adapter의 문자열 검사로는 중복을 판별할 수 없다. 이 probe는 원인 확인이며 호환성 통과 증거가 아니다.

공식 SDK 유지 방침과 Go `v0.23.0`·Python `2.1.0` 기준은 유지한다. SDK 수정 release는 정상 drain 승인의 선행 조건이 아니다. 나중에 SDK version을 변경하면 필수 Phase 1 26개와 정상 drain 2개를 다시 검증한다. SDK 진단을 다시 실행하면 새 결과를 별도로 기록하고 기존 실패를 지우거나 통과로 바꾸지 않는다.


### 6.4 검증 범위 분리 실행 기록 (2026-10-16)

검증 대상은 이 절의 suite 분리와 시험용 tmpfs 수정이 포함된 작업 트리다. 환경은 Windows/amd64, Go 1.26.5, Linux/amd64 Docker다. Broker 제품 코드와 SDK version은 이 변경에서 수정하지 않았다. 각 Docker run의 manifest·image inspect·build log는 아래 summary와 같은 디렉터리에 보존한다.

| 확인 대상·명령 | 결과·증거 |
| --- | --- |
| 첫 `--suite drain --run-id drain-required-20261016` | FAIL, 0/2. [원본 summary](../../.artifacts/compatibility/mqtt-clients-v1/drain-required-20261016/summary.json)를 보존한다. 각 broker log는 256MiB tmpfs의 여유 `268423168` byte가 기본 기준 `1073741824` byte보다 작아 `disk_free` backpressure를 켰음을 기록했다. 두 SDK 모두 redirect·session resume는 확인했지만 후속 publish가 거절돼 필수 case는 실패했다. |
| tmpfs 수정 뒤 위 §6.2의 필수 `--suite drain` 명령 | PASS, 2/2. [최종 summary](../../.artifacts/compatibility/mqtt-clients-v1/drain-required-storage-fixed-20261016/summary.json). Missing·unexpected case는 0이다. 정상 redirect·session resume·후속 전달·Will 억제와 `SAFE_TO_STOP`을 확인했다. |
| 위 §6.3의 선택적 `--suite sdk-diagnostics` 명령 | FAIL, 7/8 통과·1개 실패, exit 1. [별도 진단 summary](../../.artifacts/compatibility/mqtt-clients-v1/sdk-diagnostics-20261016/summary.json). Go 중복 Server Reference의 수락·trap 접속 실패를 별도 결과로 유지했다. 정상 drain case는 이 summary에 포함되지 않는다. |
| `go test ./tests/compatibility/mqtt-clients-v1 -count=1` | PASS, package 1.695s. `TestDrainSuiteVerdictsSeparateSDKDiagnostics`가 필수·진단 판정 분리, 필수 Will 실패와 누락된 SDK 결과의 차단을 확인한다. |
| `go vet ./tests/compatibility/mqtt-clients-v1` | PASS. |

전체 local gate와 Phase 1 Docker 26개는 이번에 다시 실행하지 않았다. 변경 대상은 drain suite 선택과 fixture이며, Phase 1 driver·broker 동작은 변경하지 않았다. 기존 Phase 1 증거는 §6을 따른다. Production rollout·migration은 여전히 별도 미검증 범위다.

## 7. 호환성 변경

- 새 optional profile 추가는 기존 profile 의미를 바꾸지 않으면 호환된다.
- 기존 profile의 Session, retry, Will 또는 Server Reference 의미 변경은 breaking change다.
- 지원 SDK 최소 version 상향은 consumer migration과 deprecation 기간이 필요하다.
- Broker release는 Active SDK matrix를 N-1↔N 양쪽에서 실행해야 한다.

## 8. Draft blocker

- `DEP-06`~`DEP-12`: external rollout gate의 end-to-end evidence. Broker의 표준 Will 처리와 reason 계측은 [ADR-0028](../decisions/ADR-0028-planned-handoff-will-and-reason-metric.md)을 따르며 수용 증거는 [Drain §10](../specs/drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)에 둔다.
- `MQTT5_DRAIN_SAFE`: production rollout timing 수용 증거. §6.3의 SDK 진단 실패는 blocker가 아니다.
- SDK별 automated compatibility harness의 Phase 1 artifact가 없는 후보는 `unverified`

## 9. 참고 자료

- [OASIS MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)
- [OASIS MQTT 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
