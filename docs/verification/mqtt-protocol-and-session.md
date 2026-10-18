# MQTT protocol과 session 검증 기록

이 문서는 [MQTT protocol과 session 스펙](../specs/mqtt-protocol-and-session.md)의 과거 실행 증거다. 아래 통과는 당시 작업 트리의 결과이며 현재 코드의 통과를 뜻하지 않는다. 현재 연결은 테스트 소스의 `Scenarios:` 메타 정보, 현재 결과는 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서를 따른다. 원기록에 대상 커밋·날짜·환경·명령이 없는 경우 이를 새로 추정하지 않았다.

## 1. 수용 테스트 일반 기록

대상 시나리오: `MQTT-SCN-013`, `MQTT-SCN-014`

| 대상 | 환경 | 명령 | 결과 | 증거 |
| --- | --- | --- | --- | --- |
| SUBACK 뒤 retained replay 순서 회귀 | 원문에 환경 기록 없음. 명령에 `GOMAXPROCS=4` 지정 | `GOMAXPROCS=4 go test ./tests/integration -run '^TestMQTTIngressRetainedMessageAndSubscriptionOptions$' -count=20` | 통과 | 20회 연속 PASS (10.230s) |

대상 시나리오: `MQTT-SCN-024`

고정 workload의 실제 TCP·Docker 실행은 [BP-03 수용 증거](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)를 따른다.

## 2. Resource limit·비정상 입력 기록

대상 시나리오: `MQTT-SCN-026`~`MQTT-SCN-042`

2026-10-05 Windows local 작업 트리에서 TCP·integration focused 시험, TCP 전체·race와 stream fuzz 10,000회가 통과했다. 원기록에는 focused 명령이 없다. CONNACK 한도 광고, 양 protocol의 오류 종료, Receive Maximum 회수, 부분 packet timeout, frame buffer 회수, queue 포화, client 한도와 복구 상태를 확인했다.

주요 관찰은 다음과 같다.

- Resource 회수 시험에서 goroutine은 `7→7`, heap 증가 484,216 byte였다.
- 실제 binary에서 최대 packet 설정의 CONNECT·PING, Receive Maximum 거절 `0x93`, 부분 packet 종료와 잘못된 flag의 시작 거절을 확인했다. 이 smoke는 [§5의 추적 회귀 시험](#5-실제-binary-한도-회귀-시험-연결-2026-10-17)으로 연결했다.
- `go run ./tests/compatibility/mqtt-clients-v1 --run-id phase1-resource-limits --output .artifacts/compatibility/mqtt-clients-v1/phase1-resource-limits`는 `PASS 26/26`이었다. 해당 디렉터리에 결과를 보존했다.
- `GOMAXPROCS=4 go run ./tools/verify local`은 coverage를 포함해 통과했다.

변경 전 임시 socket 실험은 CONNECT 전 부분 입력 64개에서 heap 증가
66884496 byte로 16 MiB 기준을 넘었다. 변경 후 같은 실험은 515944 byte로
통과했다. 임시 시험과 binary smoke client는 확인 뒤 제거했다. Listener 최대
연결 수·connection rate와 tenant message rate의 완료 증거는 아니다.


## 3. Listener 연결 수·connection rate 기록

대상 시나리오: `MQTT-SCN-043`~`MQTT-SCN-053`

2026-10-05 Windows/amd64 작업 트리에서 TCP admission race, TLS·Metric·command package와 listener admission·TLS integration focused 시험이 통과했다. 원기록에는 focused 명령이 없다. 최대 연결 수 회수, clock 기반 rate 보충·상한·역행, 최대 연결 수 우선순위, 원본 Accept 오류 보존과 TLS 안쪽 admission을 확인했다. 실제 binary의 양 한도 거절은 0-byte 종료와 각 Metric `1`, 회수 뒤 성공 CONNACK을 기록했다. 추적 가능한 binary 회귀는 [§5](#5-실제-binary-한도-회귀-시험-연결-2026-10-17)에 둔다.

| 검증 | 명령·증거 | 결과 |
| --- | --- | --- |
| SDK 첫 실행 | run ID `phase1-listener-admission`(명령 원문 없음), `.artifacts/compatibility/mqtt-clients-v1/phase1-listener-admission/` | **FAIL 25/26**; `go-mqtt311-persistent-resume`이 `identifier rejected`로 거절됐다. 원인은 확정하지 않았다. |
| 같은 코드의 격리 SDK 실행 | `go run ./tests/compatibility/mqtt-clients-v1 --run-id phase1-listener-admission-isolated --output .artifacts/compatibility/mqtt-clients-v1/phase1-listener-admission-isolated` | PASS 26/26. 첫 실패를 대체하지 않는다. |
| 전체 local gate | `GOMAXPROCS=4 go run ./tools/verify local` | PASS; `.artifacts/verification/coverage-summary.json`·`coverage.out`에 coverage 보존 |
| 별도 mutation gate | `tools/verify/runner.go`의 CI mutation 명령 3개(`delivery`·`nodedrain`·`session`) | 모두 PASS; killed `27/64/56`, lived·not covered·timed out `0`. `.artifacts/verification/mutation-delivery.json`·`mutation-nodedrain.json`·`mutation-session.json` |

Local gate는 mutation이나 Docker SDK suite를 대신하지 않는다. 첫 SDK 실행은 local·mutation과 동시에, 두 번째는 종료 뒤 같은 26개 case로 실행했다. SDK·제품 session 처리에 재시도·대기·오류 무시를 추가하지 않았다. Listener 한도 기본값 `0`에서는 admission wrapper를 만들지 않는다.

## 4. WebSocket/WSS 기록

대상 시나리오: `MQTT-10-SCN-01`~`MQTT-10-SCN-10`

MQTT-10-SCN-01~10의 환경·명령·결과는 [WebSocket 실행 기록](configuration-and-runtime-options.md#websocket-execution)에만 둔다.

## 5. 실제 binary 한도 회귀 시험 연결 (2026-10-17)

대상 시나리오: MQTT-SCN-035, MQTT-SCN-041, MQTT-SCN-053

대상 범위: 현재 작업 트리에서 `cmd/narumq`를 빌드하고 시작하는 `tests/integration/mqtt_resource_limits_process_test.go`다. 제품 코드는 바꾸지 않았다. 환경은 Windows/amd64다.

실행 명령: `GOMAXPROCS=2 go test -p=2 -count=1 ./tests/integration -run "^TestMQTTBinary" -v`.

결과: 같은 세 테스트를 포함한 focused 실행 통과.

- `TestMQTTBinaryAcceptsMaximumRemainingLengthAndAdvertisesLimits`: 최대 Remaining Length 설정으로 MQTT 5 CONNECT와 PING을 확인했다. CONNACK의 두 한도 값을 확인했다.
- `TestMQTTBinaryEnforcesReceiveMaximumPartialPacketTimeoutAndFlagBounds`: CONNACK 광고, 미완료 QoS 2 세 번째 PUBLISH의 DISCONNECT `0x93`, 부분 packet의 설정 기한 뒤 종료, `-receive-maximum 0`의 시작 거절을 확인했다.
- `TestMQTTBinaryListenerAdmissionClosesWithoutBytesCountsAndRecovers`: 각 listener 한도의 0-byte 종료, 거절 Metric `1`, 연결 회수와 rate 보충 뒤 성공 CONNACK, 값 `1000001`의 시작 거절을 확인했다.

MQTT-SCN-042는 기존 `tests/compatibility/mqtt-clients-v1/runner.go`의 `Run`에 연결했다. 이 진입점은 Go·Python SDK 전체 profile을 실행하며 listener 한도 flag를 추가하지 않아 기본값 `0`을 사용한다. 이번 변경에서는 Docker SDK suite를 다시 실행하지 않았다. 이유: 테스트 동작은 바꾸지 않고 메타 정보만 추가했다. 영향: 현재 SDK 결과는 `미실행`이며, §2·§3의 과거 통과·실패 기록을 새 실행 결과로 취급하지 않는다.
