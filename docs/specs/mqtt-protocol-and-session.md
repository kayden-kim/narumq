# MQTT protocol과 session 스펙

이 문서는 MQTT client가 NaruMQ listener에 연결해 인증하고 publish·subscribe하며 session을 재개할 때 관찰하는 동작을 정의한다. Cluster 내부 복제 방식은 [cluster spec](cluster-session-and-delivery.md), tenant 권한 판정은 [security spec](tenancy-and-security.md)을 기준으로 한다.

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 관련 요구사항 | MQTT-01~MQTT-13, CLIENT-01~CLIENT-08 |
| 근거 | [PRD 8장과 9.1절](../requirements/PRD.md), [OASIS MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html), [OASIS MQTT 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html) |
| 관련 Architecture | [System overview](../architectures/system-overview.md), [Broker core](../architectures/broker-core.md) |
| 관련 Contract | [MQTT client compatibility v1](../contracts/mqtt-client-compatibility-v1.md) |
| 해설 | [MQTT 전달 보장과 QoS·Session·Will protocol 흐름](../explainers/mqtt-delivery-guarantees-and-protocol-flows.md) |

2026-10-05 승인된 resource limit 구현 범위는 §1의 packet 완성 시간과 §7.1·§8.1이다. 나머지 Draft 범위의 채택 상태는 바꾸지 않는다.

## 현재 구현 상태: standalone QoS 1/2와 dynamic cluster의 제한된 durable QoS 1/2 delivery

Standalone runtime은 MQTT 3.1.1/5.0 client의 `CONNECT → SUBSCRIBE/PUBLISH → PING/DISCONNECT`와 process-local session delivery를 제공한다. 기본 standalone startup은 `-security-config` secure mode로 JWT/JWKS 또는 verified client certificate mTLS authentication과 tenant-aware ACL을 적용한다. mTLS는 handshake가 검증한 client certificate의 `commonName`, `dnsSan`, `emailSan` selector를 strict identity binding으로 바꾸며 configured Client ID가 허용 목록과 일치해야 한다. 인증·tenant 경계 없는 plaintext ingress는 `-allow-insecure-anonymous-mqtt`를 지정한 literal loopback `-tcp-listen` development·benchmark 예외에서만 허용하며 TLS listener에는 적용하지 않는다.

Dynamic cluster는 별도 `-cluster-config` composition root다. QoS 0 retained PUBLISH와 detached persistent session의 QoS 0 offline queue는 Raft에 commit해 replay하고 NMQBKUP3 archive에 포함한다([Cluster §4.1](cluster-session-and-delivery.md#41-offline-message-capture와-전달), §11.8–§11.9). ADR-0015와 ADR-0019는 QoS 1/2 delivery와 ACK 경계를 정한다. Target 있는 delivery는 관련 command commit 뒤 ACK하고, target 없는 QoS 1 client PUBLISH는 delivery command 대신 proposal 없는 fresh quorum ReadIndex barrier 뒤 PUBACK한다([Cluster §4.2](cluster-session-and-delivery.md#42-qos-12-durable-delivery)). QoS 2 publisher inbound handshake는 target과 무관하게 commit한다. Detached QoS 1/2 delivery, takeover, backup restore, process kill과 quorum-loss 동작은 제한된 process acceptance로 확인했다(§11.11). Production storage/EBS power-loss, BP-03, N/N-1과 TEN-08은 포함하지 않는다.

- `internal/domain/mqtt`는 CONNECT/CONNACK, QoS 0·1·2 PUBLISH, PUBACK/PUBREC/PUBREL/PUBCOMP,
  SUBSCRIBE 계열과 PINGREQ/PINGRESP/DISCONNECT를 decode·encode한다. PING packet은
  Remaining Length 0만 허용하고 MQTT 5 DISCONNECT는 property-free allowlist만 지원한다.
- MQTT 5 CONNECT는 Session Expiry Interval(0x11), Receive Maximum(0x21),
  Maximum Packet Size(0x27)를 받는다. 성공 CONNACK은 server Receive Maximum과
  Maximum Packet Size를 알린다.
- `internal/domain/routing`은 non-shared 및 `$share/{ShareName}/{filter}` 공유 구독,
  `+`·`#` filter, `$` system topic 경계, session별 중복 제거와 overlap 최대 granted QoS를
  결정한다. 공유 구독은 그룹 내 가입자를 Round-Robin으로 primary에 선택하고, 선택된
  member의 delivery queue가 full 또는 unavailable이면 같은 group의 다음 eligible member로
  순서대로 handoff한다.
  `No Local`은 비공유 구독에서 동일 publisher session을 제외하며(공유 구독에 NoLocal 설정 시
  0x82 Protocol Error 거절), `Retain As Published`는 live delivery의 RETAIN을 결정하며,
  `Retain Handling`은 SUBSCRIBE replay 시점에만 적용한다.
- `internal/application/subscribe`는 요청 순서를 유지한 SUBACK를 만들고, retained
  replay batch를 route mutation 전에 queue에 원자적으로 예약한다. MQTT 3.1.1과
  MQTT 5의 non-shared retained replay 및 MQTT 5 subscription options를 지원하며,
  공유 구독 가입 시 retained message replay를 억제([MQTT-4.8.2-1])하고 NoLocal을 프로토콜 에러로 거절([MQTT-3.8.3-4])한다.
- `internal/application/publish`는 tenant와 exact topic별 retained PUBLISH를 replace/delete한다. Standalone profile은 QoS 0·1·2 ingress를 처리한다. Dynamic cluster는 QoS 0 retained/offline 경로와 ADR-0015의 QoS 1/2 durable publish 경로를 분리하며, target 없는 QoS 1 client PUBLISH/Will은 ADR-0019의 proposal 없는 ReadIndex barrier 뒤 처리한다. Commit·barrier 순서와 ACK 조건은 [Cluster §4.2](cluster-session-and-delivery.md#42-qos-12-durable-delivery)에 정의한다.
- `internal/domain/mqtt`의 MQTT 5 PUBLISH는 Message Expiry Interval property를 검증하고,
  application은 ingress 수신 시각과 interval을 한 번만 계산해 absolute deadline으로
  변환한다. Standalone delivery queue와 memory retained repository는 `now >= deadline`에서 lazy purge한다. Dynamic cluster retained state는 expiry deadline과 proposal 시각을 command에 포함해 Raft apply에서 만료를 결정적으로 제거한다. MQTT 5 onward PUBLISH에는 올림한 남은 초를 다시 넣으며 MQTT 3.1.1 wire에는 property를 쓰지 않아도 같은 deadline을 적용한다.
- `internal/application/ports/retained`와 `internal/adapters/memory/retained`는 bounded tenant/topic repository를 제공한다. Standalone mode에서는 이것이 process-local authority다. Dynamic cluster mode에서는 같은 adapter가 replay lookup projection이며, 권위 있는 retained state는 replicated state machine이 소유한다. Startup·snapshot restore 뒤 projection을 rebuild하며 실패하면 readiness를 허용하지 않는다. Cluster retained process evidence는 [§11.8](cluster-session-and-delivery.md#118-retained-replication과-backup-v2-수용-매핑)에 있다.
- Standalone ingress는 QoS 0·1·2를 지원한다. QoS 1은 메시지 라우팅 후 PUBACK을 반환하며, QoS 2는 수신 시 PUBREC을 반환하고 PUBREL 수신 시 라우팅 후 PUBCOMP를 반환한다. 중복 Packet Identifier 재전송 시 중복 라우팅 없이 동일 ACK를 반환한다.
- Standalone outbound delivery는 구독자의 granted QoS와 메시지 QoS 중 낮은 QoS로 전송하며 QoS 1/2 subscriber handshake를 완료한다. Dynamic cluster QoS 1/2 durable delivery도 MQTT handshake를 복구할 수 있지만 process acceptance는 [Cluster §11.11](cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑)의 범위에 한정된다. Cluster retained replay의 evidence는 QoS 0이 §11.8, QoS 1/2가 [§11.32](cluster-session-and-delivery.md#1132-qos-12-retained-수용-매핑)에 있다.
- 만료되지 않은 persistent session은 network connection이 종료되어도 process-local
  delivery queue의 FIFO queued entry와 완료되지 않은 송신 QoS 1/2 상태를 보존한다.
  새 connection은 `CONNACK`을 먼저 전송한 뒤 복구 packet을 전송해야 한다.
- 복구된 QoS 1/2 `PUBLISH`는 저장된 Packet Identifier를 재사용하고 `DUP=1`로
  전송한다. QoS 2가 PUBLISH 단계 뒤 `PUBREC`까지 완료된 상태는 같은 Identifier의
  `PUBREL`로 재개한다. reconnect protocol version에 맞춰 packet을 다시 인코딩한다.
- Message Expiry가 지난 queued entry와 PUBLISH-stage 송신 상태는 제거하고 전송하지
  않는다. 이미 PUBLISH를 수신한 뒤 `PUBCOMP`를 기다리는 QoS 2 `PUBREL` 상태는
  원래 publication deadline과 무관하게 복구한다.
`internal/adapters/memory/sessionstore`와 delivery queue는 session state와 local delivery projection을 process-local memory에 둔다. Standalone mode는 이를 process restart, replication 또는 durable storage recovery에 보존하지 않는다. Cluster mode에서는 detached QoS 0 offline queue를 [Cluster §4.1](cluster-session-and-delivery.md#41-offline-message-capture와-전달), QoS 1/2 durable target queue 및 inbound/outbound protocol state를 [§4.2](cluster-session-and-delivery.md#42-qos-12-durable-delivery)에 따라 Raft에서 복구한다. QoS 1/2 evidence와 production durability 한계는 [§11.11](cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑)을 참조한다. TLS transport와 mTLS client authentication은 listener 계약에 정의한 범위에서 지원한다.

영속 session의 queued·in-flight delivery state는 clean reset 또는 Session Expiry가
끝날 때만 제거한다. 정상적인 persistent disconnect에서는 session, route와 함께 delivery
state를 유지한다.

## 1. Listener 계약

1. TCP와 TLS listener는 MQTT 3.1.1 protocol level 4와 MQTT 5.0 protocol level 5를 수용해야 한다.
2. listener마다 bind address, TLS 정책, 인증 방식, 최대 연결 수, connection rate와 packet 제한을 독립 적용해야 한다.
3. 평문 TCP listener는 기본 설정에서 비활성화해야 한다. MQTT listener를 활성화할 때는 [security spec](tenancy-and-security.md)의 `-security-config` secure mode를 선택해야 하며, 유일한 예외는 `-tcp-listen`의 literal loopback bind와 `-allow-insecure-anonymous-mqtt`를 함께 지정하는 development·benchmark mode다. 두 mode를 함께 지정하거나 TLS listener에 예외 mode를 적용해서는 안 된다.
4. WebSocket과 WSS는 P1 선택 기능이다. 활성화하면 MQTT packet과 session 의미가 TCP listener와 같아야 한다.
5. connection은 설정된 CONNECT timeout 안에 유효한 CONNECT를 보내야 한다. timeout 또는 malformed packet이면 session을 만들지 않고 연결을 닫아야 한다.
   CONNECT 뒤 첫 byte가 도착한 packet은 `-packet-read-timeout`(기본 30s) 안에
   완성되어야 한다. 시간을 넘기면 응답 없이 연결을 닫는다. KeepAlive=0이고
   미완성 byte가 없는 유휴 연결은 유지한다.
6. development profile의 평문 `-tcp-listen`은 `127.0.0.0/8` 또는 `::1` literal
   loopback만 허용한다. 이 listener는 `-allow-insecure-anonymous-mqtt`를 명시한 경우에만
   anonymous MQTT를 허용한다. TLS listener는 `-tls-listen`으로 별도 활성화하며,
   TLS 1.2 이상, 허용된 AEAD cipher suite와 설정된 서버 인증서를 적용한다.
   `request` 또는 `require` client authentication을 선택하면 strict security config의
   mTLS identity binding이 필요하다. TLS adapter는 verified chain의 leaf certificate에서
   application `ClientCertificateDTO`를 만들고, authentication adapter가 exact
   Common Name, DNS SAN 또는 email SAN match를 수행한다.
7. `-tcp-listen`과 `-tls-listen`은 하나의 `tcp.Server`에 함께 등록할 수 있고,
   listener별 transport 보안 정책을 유지하면서 동일한 session·route registry를 공유한다.
8. CONNECT 뒤에는 negotiated version으로 packet을 decode하고, 모든 socket write는 한
   writer goroutine의 bounded control queue 또는 delivery wake를 통과한다.
9. binary smoke와 실제 loopback wire 검증은 `tests/integration/mqtt_ingress_test.go`와
   TLS/mTLS transport 검증은 `tests/integration/mqtt_tls_test.go` 및 `cmd/narumq`를
   기준으로 한다.

## 2. CONNECT 처리 순서

서버는 CONNECT를 다음 순서로 처리해야 한다.

```mermaid
flowchart TD
    A["Network connection"] --> B["Fixed header와 packet 경계 검증"]
    B --> C["Protocol name과 level 판정"]
    C --> D["CONNECT flags·property·UTF-8 검증"]
    D --> E["Listener 보안 정책과 인증"]
    E --> F["tenant와 clientId binding·차단 확인"]
    F --> G["quota와 session ownership 획득"]
    G --> H["session 생성 또는 복구"]
    H --> I["CONNACK 반환"]
```

각 단계가 실패하면 이후 단계의 durable state를 만들어서는 안 된다. ownership을 얻은 뒤 응답 전 실패하면 재시도 시 같은 session 상태로 수렴해야 한다.

인증 단계는 configured verified client certificate가 있으면 mTLS provider만 사용한다.
그 CONNECT에 Username/Password가 함께 있으면 dual credential 오류로 거절하며 JWT로
fallback하지 않는다. certificate가 없으면 JWT provider가 configured된 경우에만
Username/Password 양쪽 flag와 token을 요구한다. 인증·Client ID binding·Will ACL이
성공하기 전에는 session, route, queue를 변경하지 않는다.

### 2.1 Protocol version

| 입력 | 외부 동작 |
| --- | --- |
| Protocol Name `MQTT`, level 4 | MQTT 3.1.1로 처리한다. |
| Protocol Name `MQTT`, level 5 | MQTT 5.0으로 처리한다. |
| Protocol Name `MQIsdp`, level 3 | MQTT 3.1.0 wire variant로 인식하지만 비지원으로 거절한다. 응답 없이 연결을 닫고 session을 만들지 않는다. |
| Protocol Name `MQTT`, level 3 | MQTT 3.1.0 wire variant로 인식하지만 MQTT 3.1.1 형식의 CONNACK `0x01`을 반환한 뒤 연결을 닫고 session을 만들지 않는다. |
| 지원하지 않는 level | MQTT 3.1.1 형식으로 판정 가능한 경우 CONNACK `0x01`을 반환하고 연결을 닫는다. MQTT 5 형식으로 판정 가능한 경우 CONNACK `0x84`를 반환하고 연결을 닫는다. |
| packet 구조상 version을 안전하게 판정할 수 없음 | 응답 없이 연결을 닫을 수 있다. malformed packet으로 기록한다. |

두 MQTT 3.1.0 wire variant의 거절은 `reason="unsupported_protocol_version"`, `protocol="3.1.0"`으로 같은 Metric에 집계해야 한다. 원문 credential은 로그에 남기지 않는다. `MQIsdp`와 `MQTT`의 Protocol Name 차이는 응답 정책만 바꾼다.

### 2.2 Session 선택

session key는 `(tenantId, clientId)`다. tenant와 clientId가 하나라도 달라지면 기존 session을 재개해서는 안 된다.

| Protocol 입력 | Session 동작 | CONNACK |
| --- | --- | --- |
| MQTT 3.1.1 `Clean Session=1` | 기존 session을 폐기하고 새 session으로 시작한다. 연결 종료 뒤 session을 보존하지 않는다. | `Session Present=0` |
| MQTT 3.1.1 `Clean Session=0` | 만료되지 않은 기존 session을 복구한다. 없으면 새 persistent session을 만든다. | 복구했을 때만 `Session Present=1` |
| MQTT 5 `Clean Start=1` | 기존 session을 폐기하고 새 session으로 시작한다. | `Session Present=0` |
| MQTT 5 `Clean Start=0` | 만료되지 않은 기존 session을 복구한다. | 복구했을 때만 `Session Present=1` |
| MQTT 5 `Session Expiry Interval=0` | network connection 종료 뒤 session을 제거한다. | 위 규칙에 따름 |

동일 session key로 새 connection이 인증과 ownership 획득에 성공하면 새 connection이 owner가 된다.
production target에서는 MQTT 5이면 이전 connection에 `DISCONNECT 0x8E`(`Session taken over`)를
보내고, MQTT 3.1.1은 연결을 닫아야 한다. 현재 development slice는 takeover를 구현하지 않고
active duplicate를 negative CONNACK으로 거절한다.

## 3. Publish와 내구성 ACK

### 3.1 수신 QoS 상태

| QoS | 성공 경계 | 재전송·중복 처리 |
| --- | --- | --- |
| 0 | packet을 수용해 routing queue로 넘긴 시점 | ACK가 없다. 장애 시 유실될 수 있다. |
| 1 | 메시지와 필요한 전달 상태가 configured durability barrier를 통과한 뒤 성공 PUBACK | 같은 Packet Identifier와 DUP 재전송을 최소 한 번 의미로 처리한다. 이미 성공 처리한 packet은 중복 durable message를 만들지 않고 일관된 PUBACK을 반환해야 한다. |
| 2 | 수신 PUBLISH와 handshake 상태가 durability barrier를 통과한 뒤 성공 PUBREC | PUBREL과 PUBCOMP까지 상태를 복구한다. 중복 PUBLISH·PUBREL은 같은 handshake 결과로 수렴해야 한다. |
수신 QoS 1/2 핸드셰이크가 PUBACK 또는 PUBCOMP로 완료되면 client는 같은 연결에서 Packet Identifier를 다시 사용할 수 있다. 이때 `DUP=0`인 새 PUBLISH는 새 교환이며, broker는 기존 완료 상태만 바꾸고 메시지를 새로 처리해야 한다. 진행 중인 교환 상태는 재사용으로 덮어쓰지 않는다. QoS 2 완료 상태의 tombstone은 반복 PUBREL을 멱등 처리하며, 같은 Packet Identifier의 새 RECEIVE가 commit될 때 교체한다.

Dynamic cluster의 durable TCP 연결은 handshake 단계 둘을 별도 FIFO 작업 lane으로 처리한다. 첫 lane은 `PUBLISH`·`PUBACK`·`PUBREC`, 둘째는 `PUBREL`·`PUBCOMP`를 처리한다. 각 lane은 16개 대기 항목으로 제한하고, 처리 중 항목을 포함한 두 lane의 입력 byte 합계는 `MaxPacketSize+5` 이하로 제한해 TCP 읽기에 backpressure를 건다. PUBLISH 수신 순서, PUBREL 수신 순서, 같은 방향·Packet Identifier의 전체 수신 순서는 유지한다. 나머지 control packet은 앞선 두 lane의 작업이 끝난 뒤 처리한다. Decoder가 소유한 packet 값만 lane에 전달한다. `deliveryMu`는 map·PacketID 예약/해제·상태 변경을 보호하며, durable I/O를 기다리는 동안은 잠그지 않는다. 예약한 ID는 promotion이 끝날 때까지 유지하고, durable commit 성공 뒤 상태를 갱신한 다음 ACK/PUBREL을 전송한다. 비정상 종료 시 socket을 닫고 모든 phase worker와 writer가 끝난 뒤 session을 cleanup한다. 정상 DISCONNECT는 앞선 phase 작업과 제어 패킷을 먼저 끝내고 cleanup한 뒤 socket을 닫는다. Cleanup 표시는 `deliveryMu` 아래 설정해 늦게 완료된 promotion이 닫힌 connection에 상태를 다시 넣지 못하게 한다. Fresh read barrier, quorum commit, epoch fencing과 재시도 시 예약 유지 계약은 바꾸지 않는다.

첫 lane의 durable PUBLISH는 준비·제출과 완료를 분리한다. 준비 단계는 수신 순서대로 검증하고, 준비된 변경을 driver의 대기 목록에 넣는다. Driver는 각 변경의 고유한 ReadIndex context로 새 quorum proof를 요청한다. Proof의 index까지 local apply/projection이 끝난 뒤에만, 대기 목록의 입력 순서대로 실제 Raft proposal을 제출한다. 앞선 proof가 늦으면 뒤의 변경도 추월하지 않으며, 취소된 변경은 제출하지 않는다. 이때 반환하는 pending 값은 proof나 성공 ACK의 근거가 아니다. 별도 완료 worker가 제출 순서대로 WAL 내구성·quorum commit·local apply/projection 완료를 기다린 뒤 inbound 상태를 갱신하고 PUBACK/PUBREC을 전송한다. 서로 다른 Packet Identifier의 다음 PUBLISH는 앞선 proof·commit 완료를 기다리지 않고 준비할 수 있다. 같은 ID의 재전송·재사용은 앞선 완료까지 기다리며, control barrier와 종료 처리는 준비된 모든 pending 값의 완료 처리까지 포함한다. Retained 및 QoS 0 PUBLISH는 기존 동기 경로를 사용하고 앞선 제출의 완료 뒤 처리한다. Lane의 16개 제한과 공용 byte 제한은 제출 후 완료 대기 항목까지 포함한다. 동기 application 호출은 같은 pending 값을 즉시 기다리는 API를 사용한다. 기존 quorum proof를 재사용하거나 read-before-write를 생략하지 않으며, ReadIndex/control은 proposal 수집 timer 때문에 기다리지 않는다.

둘째 lane의 비-retained QoS 2 `PUBREL`도 같은 준비·완료 방식을 사용한다. Inbound state가 가리키는 원래 PUBLISH를 기준으로 RELEASE를 준비하고 입력 순서대로 fresh proof 뒤 제출한다. 완료 worker가 release commit을 확인한 뒤에만 tombstone과 PUBCOMP를 만든다. Retained RELEASE와 이미 완료된 duplicate PUBREL은 앞선 pending 완료 뒤 기존 동기 경로로 처리한다. 같은 방향·ID의 predecessor 규칙과 lane 전체 제한은 두 lane에 동일하게 적용한다.

송신 ACK의 `ADVANCE`·`COMPLETE`도 lane에서 준비하고 완료 worker가 commit 결과 뒤 outbound map·PacketID를 변경한다. `deliveryqueue.Queue.BeginReplaceOutbound`와 `BeginCompleteOutbound`가 application 소유 pending transition을 반환한다. Cluster queue는 새 proof 뒤 durable 변경을 제출하고, pending 완료 시에만 local queue를 변경한다. Memory queue는 I/O가 없으므로 같은 변경을 즉시 수행한 완료 값을 반환한다. PUBREL 전송과 PacketID 해제는 완료 뒤에만 가능하다. 같은 방향·ID의 predecessor 규칙은 duplicate PUBREC/PUBCOMP 및 sender ID 재사용에도 적용한다.

송신 writer는 queued QoS 1/2 메시지를 최대 8개씩 준비해 `deliveryqueue.Queue.PromoteBatch`에 넘긴다. `PeekBatch`는 caller가 제공한 고정 크기 배열까지만 FIFO 항목과 소유한 payload를 반환한다. Batch는 durable SEND마다 별도 fresh proof를 요구하며 여러 SEND를 하나의 의미 없는 성공으로 합치지 않는다. 모든 pending 결과를 회수하고, 성공한 연속 prefix만 local queue에서 outbound로 옮긴다. Writer는 그 prefix만 순서대로 wire에 보낸다. 앞선 SEND의 응답이 실패해도 뒤의 SEND가 이미 commit되었을 수 있으므로, 실패 뒤의 모든 suffix는 PacketID 예약을 재시도 또는 연결 정리까지 보존한다. 일부 prefix가 성공했더라도 나머지 오류를 성공으로 바꾸지 않는다. 재시도는 같은 ID를 사용하고, local queue처럼 durable sequence가 없는 항목도 예약 하나를 여러 메시지에 중복 사용하지 않는다. Local mutation이나 wire PUBLISH가 proof·commit보다 앞서서는 안 된다. 기존 단일 항목 promotion과 memory queue도 같은 FIFO·expiry·capacity·PacketID 규칙을 적용한다.

quorum, 저장 quota 또는 replication 조건을 충족하지 못하면 성공 PUBACK/PUBREC을 반환해서는 안 된다. MQTT 5에서는 상황에 맞는 `0x89`(`Server busy`), `0x97`(`Quota exceeded`) 또는 `0x83`(`Implementation specific error`)를 사용할 수 있다. MQTT 3.1.1에서 packet 단위 실패를 표현할 수 없으면 연결을 닫을 수 있다. 어느 경우든 성공 ACK로 위장해서는 안 된다.

이 slice는 단일 노드 인메모리 프로세스 로컬 환경에서 수신 QoS 1 및 QoS 2 핸드셰이크를 지원한다.
수신 QoS 1은 fan-out 성공 후 성공 PUBACK(MQTT 5는 ReasonCode 0)을 반환하며, 중복 패킷
재전송 시 중복 전달 없이 동일한 PUBACK을 반환한다.
수신 QoS 2는 1단계에서 PUBREC을 반환하고, 클라이언트로부터 PUBREL이 수신되면 실제 fan-out을
수행한 뒤 PUBCOMP를 반환하여 핸드셰이크를 완료한다. 중복 PUBLISH 또는 PUBREL 재전송 시
동일한 PUBREC 또는 PUBCOMP로 수렴한다.
용량 초과(`retained.ErrFull`) 등의 실패 시 MQTT 5는 `0x97` 등의 negative ACK/DISCONNECT를 반환하고,
MQTT 3.1.1은 연결을 닫는다.
### 3.2 송신 QoS와 offline queue

1. 전달 QoS는 publish QoS와 subscription granted QoS 중 낮은 값이어야 한다.
2. process-local delivery queue entry는 Message Expiry deadline과 현재 bounded
   capacity를 적용해야 한다. 만료되지 않은 persistent session은 연결이 끊긴 동안에도
   FIFO queued entry와 완료되지 않은 outbound QoS 1/2 state를 보존해야 한다.
3. QoS 1 송신 상태는 PUBACK까지, QoS 2 송신 상태는 PUBCOMP까지 복구할 수 있어야 한다.
   QoS 2에서 PUBLISH 뒤 PUBREC까지 성공한 상태는 PUBREL 전송 단계로 복구한다.
4. 재접속 뒤 미완료 PUBLISH를 재전송할 때 저장된 Packet Identifier와 `DUP=1`을
   사용한다. 복구된 PUBREL도 저장된 Packet Identifier를 사용하며, 새 connection의
   MQTT protocol version으로 인코딩한다.
5. queued entry 또는 PUBLISH-stage retransmission의 expiry가 지나면 상태에서 제거하고
   전송하지 않는다. 만료로 제거된 상태는 `expired` 사유로 집계해야 하며, 구체적인
   Metric 이름과 histogram bucket은 `SPEC-TBD-07`에서 확정한다. PUBREL 대기 상태는
   PUBLISH가 이미 수신되었으므로 원래 publication deadline이 지나도 복구한다.
6. `CONNACK`은 같은 reconnect에서 복구된 packet보다 먼저 wire에 기록되어야 한다.
   복구된 outbound state는 Packet Identifier 오름차순으로 한 번만 재개한 뒤 새 queued
   entry를 FIFO 순서로 처리한다.

## 4. Subscribe와 routing 입력

1. Topic Filter의 `+`, `#` 위치와 UTF-8을 표준대로 검증해야 한다.
2. 겹치는 subscription이 같은 client session에 여러 번 match하면 MQTT version의 표준 전달 규칙과 granted QoS를 적용해야 한다.
3. `$share/{group}/{filter}`는 tenant 안의 같은 group에서 한 활성 subscriber를 선택해야 한다. 선택된 subscriber가 전달을 완료하지 못하면 session·QoS 규칙에 따라 다른 member가 이어받을 수 있어야 한다.
4. MQTT 5 subscription option은 exact filter별로 보존하고 다음처럼 적용해야 한다.
   - `No Local=true`면 publication publisher와 같은 `clientId`의 live delivery를 제외한다.
   - `Retain As Published=false`면 established-subscription delivery의 RETAIN을 지우고, `true`면 incoming RETAIN을 보존한다.
   - SUBSCRIBE가 발생시킨 retained replay는 `Retain As Published` 값과 무관하게 항상 RETAIN을 설정한다.
   - `Retain Handling=0`은 항상 replay하고, `1`은 새 exact filter일 때만 replay하며, `2`는 replay하지 않는다.
5. 권한이 없는 filter는 subscribe 결과에서 거절해야 하며 route index에 추가해서는 안 된다.
6. unsubscribe는 존재하지 않는 subscription에도 안전하게 재시도할 수 있어야 한다.
7. Broker는 해당 SUBSCRIBE가 발생시킨 retained replay PUBLISH를 보내기 전에 그 SUBSCRIBE의 SUBACK을 wire에 기록해야 한다(MUST). SUBSCRIBE 적용과 SUBACK 전송 중에는 TCP writer가 queued delivery를 보류하고, SUBACK 기록 뒤 delivery를 재개한다.

## 5. Retained Message

Standalone development profile은 bounded process-local retained repository를 사용한다. Dynamic cluster profile은 Raft committed retained state를 권위 있는 값으로 쓰고 node-local repository를 replay용 projection으로 재구성한다. Retained replication과 cross-node replay acceptance는 QoS 0 PUBLISH를 [Cluster §11.8](cluster-session-and-delivery.md#118-retained-replication과-backup-v2-수용-매핑)에서, QoS 1/2 `Retain=true` PUBLISH를 [Cluster §11.32](cluster-session-and-delivery.md#1132-qos-12-retained-수용-매핑)에서 확인한다. QoS 1/2 retained mutation 순서는 [Cluster §4.2](cluster-session-and-delivery.md#42-qos-12-durable-delivery)를 따른다. QoS 2는 PUBREL을 받을 때 retained state를 commit한다. Retained mutation은 fresh read barrier와 정확한 member profile 확인 뒤 route matching/live fan-out보다 먼저 commit되어야 한다. Commit 실패 시 해당 PUBLISH의 live fan-out을 시작하지 않는다.

- Retained key는 tenant와 exact Topic Name의 쌍 `(tenantId, topic)`이다. 같은 topic이어도 tenant가 다르면 서로 다른 value를 보관한다.
- 입력 `PUBLISH`가 `Retain=true`이고 payload가 비어 있지 않으면 해당 tenant/topic의 마지막 value를 publisher session identity 및 QoS와 함께 replace한다.
- retained PUBLISH의 zero-byte payload는 해당 tenant/topic value를 삭제한다. 삭제 packet 자체는 established subscription에 live publication으로 전달된다.
- `Retain=false`인 PUBLISH는 retained repository를 읽거나 변경하지 않는다.
- MQTT 5 Message Expiry Interval이 있으면 ingress 수신 시각에 interval 초를 더한 absolute deadline을 retained value에 저장한다. `interval=0`인 retained PUBLISH는 delete로 처리한다. Standalone repository와 cluster replay projection은 `List(now)`에서 `now >= deadline`인 값을 제거하므로 만료 값은 replay되지 않는다. Raft 권위 state는 다음 retained command를 apply할 때 그 command의 proposal 시각으로 만료 entry를 제거한다. 다음 command 전까지 만료 entry가 snapshot/archive와 용량 한도에 남을 수 있다. MQTT 3.1.1에는 Message Expiry property가 없지만 다른 protocol version에서 저장한 deadline은 replay 시 계속 적용한다.
- 새 non-shared subscription은 matching retained value를 topic 정렬 순서로 replay한다. 공유 구독(`$share/...`)은 Retain Handling 옵션과 무관하게 retained value를 replay하지 않는다([MQTT-4.8.2-1]). MQTT 3.1.1과 MQTT 5 `Retain Handling=0`은 항상 replay하고, `=1`은 exact filter가 새로 생길 때만 replay하며, `=2`는 replay하지 않는다.
- Replay delivery의 RETAIN은 항상 `true`다. Established-subscription delivery는 matching filter 중 RAP가 하나라도 켜져 있고 incoming RETAIN이 true일 때만 RETAIN을 유지한다. `No Local`은 stored publisher session과 subscriber session이 같으면 replay와 live delivery를 모두 제외한다.
- Retained replay의 QoS는 stored QoS와 granted QoS 중 작은 값이며 MQTT writer는 QoS 0·1·2 outbound handshake를 적용한다. Cluster ingress도 입력 PUBLISH의 QoS를 retained value에 저장한다.
- Retained state의 적용 한도는 기존 global 기준(value 1,024개, 계산된 총 16 MiB, payload당 1,000,000 bytes)에 tenant별 `retainedBytes` 저장 quota를 더해 [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md)에 따라 검사한다. Retained value의 계산 bytes는 tenant ID, topic, payload와 publisher routing ID의 byte 길이를 합산한다. Tenant quota 초과는 기존 non-expired value를 보존하고 live fan-out을 시작하지 않지만, 같은 apply에서 expired entry는 proposal 시각에 따라 제거될 수 있다.
- Message Expiry는 bounded queue와 retained repository에서 지원한다. MQTT 5 outbound PUBLISH에는 현재 시각 기준 올림한 남은 초를 넣고, MQTT 3.1.1 outbound에는 expiry property를 생략한다. Delivery queue의 queued·in-flight state는 process-local persistent session 범위에서 reconnect까지 보존하지만 process restart와 replication은 이 slice의 범위가 아니다. Cluster retained state는 committed snapshot에서 restart/restore 뒤 재구성한다.

## 6. Will Message

Will은 CONNECT의 인증·ACL·quota 검증을 통과한 뒤 session state에 등록해야 한다.

| 종료 원인 | Will 동작 |
| --- | --- |
| client의 정상 DISCONNECT | 발행하지 않는다. MQTT 5 `0x04`(`Disconnect with Will Message`)는 표준대로 발행한다. |
| network 단절, keepalive timeout, protocol 오류 | Will Delay가 없으면 표준 시점에 발행한다. |
| MQTT 5 Will Delay 안에 같은 session key로 재접속 | 발행하지 않는다. |
| Session Expiry가 Will Delay보다 먼저 끝남 | MQTT 5 표준이 정한 더 이른 시점에 처리한다. |
| MQTT 3.1.1 server 주도 연결 종료 | Will이 설정돼 있으면 발행될 수 있다. no-Will 보장 대상이 아니다. |

Will publish에도 tenant 격리, ACL, payload·rate quota, retained와 delivery 규칙을 적용해야 한다. Drain 중 규칙은 [drain spec](drain-and-rolling-update.md)을 따른다.

## 7. Resource limit과 protocol 오류

| 조건 | MQTT 5 동작 | MQTT 3.1.1 동작 |
| --- | --- | --- |
| packet size 초과 | 가능한 경우 `0x95` 후 종료 | packet을 처리하지 않고 종료 |
| Receive Maximum 초과 | `DISCONNECT 0x93` | 연결별 inflight 한도를 넘기지 않도록 read backpressure 또는 종료 |
| message rate 초과 | `0x96` 또는 `0x97`이 허용되는 packet에서 반환 | backpressure 후 필요하면 종료 |
| connection rate 초과 | CONNACK `0x9F` 또는 handshake 전 종료 | CONNACK `0x03` 또는 handshake 전 종료 |
| malformed packet·invalid UTF-8 | 표준 reason code가 허용되면 반환한 뒤 종료 | 즉시 종료 |
| slow client·write queue 포화 | bounded queue와 timeout 적용 후 종료 | 동일 |

모든 connection queue와 inflight collection에는 설정된 상한이 있어야 한다. 제한에 도달했을 때 무제한 할당으로 진행해서는 안 된다.

### 7.1 현재 상한과 기본값

Receive Maximum은 ACK가 끝나지 않은 QoS 1/2 PUBLISH 수의 상한이다. Maximum
Packet Size는 fixed header를 포함한 전체 packet byte 수의 상한이다.

| 상한 | 설정(standalone) | 기본값 standalone / cluster | 초과 시 MQTT 5 | 초과 시 MQTT 3.1.1 |
| --- | --- | --- | --- | --- |
| Remaining Length | `-max-packet-size` | 1048576 / 1048576 | CONNECT 뒤 DISCONNECT 0x95 후 종료. CONNECT 전 응답 없이 종료 | 종료 |
| Inbound Receive Maximum(PUBACK·PUBCOMP·PUBREC≥0x80을 아직 쓰지 않은 QoS 1/2 PUBLISH 수) | `-receive-maximum` | 64 / 64 | DISCONNECT 0x93 후 종료 | 종료 |
| 시작된 packet 완성 시간(CONNECT 뒤) | `-packet-read-timeout` | 30s / 30s | 응답 없이 종료 | 응답 없이 종료 |
| CONNECT 완료 시간 | `-connect-timeout` | 10s / 10s | 응답 없이 종료 | 응답 없이 종료 |
| Socket write 시간 | `-write-timeout` | 5s / 5s | 종료 | 종료 |
| 연결별 control queue | `-control-queue-capacity` | 16 / 256 | 종료 | 종료 |
| Session별 delivery queue | `-delivery-queue-capacity` | 64 / 256 | 기존 §3 backpressure 규칙 | 기존 규칙 |
| 연결별 수신 buffer | 없음 | 4 KiB에서 시작해 받은 byte가 차면 2배씩 늘림(최대 Remaining Length+5), 비면 4 KiB로 반환 | — | — |
| Malformed packet·invalid UTF-8(CONNECT 뒤) | 없음 | — | DISCONNECT 0x81 후 종료 | 종료 |
| Protocol error(CONNECT 뒤 두 번째 CONNECT 등, reader가 발견한 경우) | 없음 | — | DISCONNECT 0x82 후 종료 | 종료 |
| Client Receive Maximum(outbound) | client CONNECT 0x21 | 없으면 65535 | 미확인 QoS 1/2 PUBLISH가 값에 도달하면 새 PUBLISH를 보류한다. PUBREL·PUBACK 등 다른 packet은 지연하지 않는다 | 해당 없음 |
| Client Maximum Packet Size(outbound) | client CONNECT 0x27 | 없으면 제한 없음 | 넘는 PUBLISH는 보내지 않고 전송 완료로 처리한다. QoS 1/2는 promote 후 즉시 complete한다. 다른 packet이 넘으면 보내지 않고 종료한다 | 해당 없음 |

`MaxPacketSize` 설정 범위는 MQTT Remaining Length 규칙에 따라 1~268435455다.
TCP `NewServer`도 codec의 같은 규칙을 적용한다. 범위를 벗어나면
`ErrInvalidConfig`로 거절한다. 이 검사는 수신 buffer 크기와 CONNACK 한도를
계산하기 전에 끝나야 한다.

CONNECT 단계의 malformed·oversize는 protocol version을 신뢰할 수 없으므로
CONNACK 없이 닫는다. Cluster durable phase lane이 늦게 발견한 protocol 오류도
기존처럼 DISCONNECT 없이 닫는다.

완료된 inbound exchange 기록은 DUP 판정을 위해 같은 Packet Identifier를 다시
사용하기 전까지 남는다. 이 기록은 Packet Identifier 공간(65535)으로 제한한다.
미완료 exchange는 Receive Maximum으로 제한한다. MQTT 3.1.1에서도 초과 시
종료한다. Read backpressure는 미완료 QoS 2의 PUBREL 수신을 막을 수 있어 쓰지 않는다.

Message rate는 TEN-07에서 추적한다. Listener 최대 연결 수와 connection rate는
이번 완료 범위에서 제외한다. 잔여 작업은 [TODO](../TODO.md)에서 추적한다.

Memory delivery queue는 만료되지 않은 persistent session의 queued·in-flight
delivery를 reconnect까지 보존한다. Clean start/session expiry와 message expiry
deadline에 도달한 state만 제거한다. Cluster recovery 범위는 §4와 cluster spec을 따른다.

## 8. 수용 시험

- MQTT 3.1.1/5.0 compliance matrix가 지원한다고 선언한 모든 normative statement를 자동 test와 연결한다.
- 현재 Phase 1 matrix는 [`tests/conformance/mqtt-compliance-v1.json`](../../tests/conformance/mqtt-compliance-v1.json)이며, 61개 statement와 MQTT/TEST requirement 및 test function을 함께 기록한다.
- `go run ./tools/verify traceability`가 matrix → source와 source → matrix를 모두 검사한다. matrix에만 있거나 source에만 있는 statement, 또는 requirement union이 다른 연결은 합격으로 처리하지 않는다.
- QoS별 정상, DUP, reconnect, owner crash와 Message Expiry 시나리오를 검증한다.
- `MQIsdp` level 3은 응답 없이 닫고, `MQTT` level 3은 MQTT 3.1.1 CONNACK `0x01` 뒤 닫으며, 두 경우 모두 session을 만들지 않아야 한다.
  이 동작과 Metric 집계는 `tests/integration/management_api_test.go::TestMQTT310RejectionIsObservable`에서 두 wire variant를 함께 검증한다.
- wildcard 경계, shared subscription failover와 MQTT 5 subscription option 조합을 검증한다.
- parser fuzzing, slowloris, packet·queue 상한 시험에서 memory와 goroutine leak이 없어야 한다.
- Cluster mode에서 설정된 TCP/TLS listener를 열고, TLS 설정 실패 시 이미 연 TCP listener를 닫는다. `cmd/narumq/cluster_runtime_test.go::TestClusterMQTTListenersBindConfiguredEndpointsAndClosePartialOpen`이 binding과 cleanup을 확인한다.
- 두 protocol version client가 같은 외부 topic에서 ACL 범위 안의 메시지를 상호 전달해야 한다.
- 이 slice의 acceptance mapping은 `tests/integration/mqtt_ingress_test.go`의 MQTT 3.1.1/5.0
  CONNECT, QoS 0 fan-out, retained replace/delete, retained Message Expiry,
  SUBACK-before-replay, Retain Handling, No Local, Retain As Published, PINGRESP,
  normal DISCONNECT, persistent Session Present, persistent offline delivery,
  outbound QoS 1/2 recovery, unsupported/malformed CONNECT와 QoS negative ACK/close
  시나리오다. `TestMQTTIngressRetainedMessageAndSubscriptionOptions`는 retained replay의 SUBACK-before-PUBLISH 순서를 socket으로 검증한다.
  순서 회귀는 `GOMAXPROCS=4 go test ./tests/integration -run '^TestMQTTIngressRetainedMessageAndSubscriptionOptions$' -count=20`에서 20회 연속 PASS (10.230s).
  `TestMQTTIngressPersistentOfflineDelivery`는 CONNACK-before-recovery,
  FIFO, clean reset과 queued Message Expiry를 검증하고,
  `TestMQTTIngressPersistentOutboundQoSRecovery`는 PUBLISH/PUBREL 단계 recovery와
  Packet Identifier·DUP 규칙을 검증한다.
- `internal/adapters/tcp/server_durable_test.go::TestDispatchPublishReusesCompletedPacketIdentifier` directly verifies durable QoS 1/2 reuse after PUBACK/PUBCOMP commits a new exchange, while pre-PUBCOMP QoS 2 PUBLISH preserves its in-flight RECEIVE. `tests/integration/mqtt_ingress_test.go::TestMQTTIngressQoS1Handshake` and `TestMQTTIngressQoS2Handshake` verify the corresponding second ACK and routed delivery over real sockets after legal `DUP=0` Packet Identifier reuse.
- Durable phase 완료 경계는 `internal/adapters/tcp/server_phases_test.go`의 `TestTCPDurablePublishSubmitsBeforePriorCommitCompletes`, `TestTCPDurablePublishFailureDrainsPendingWithoutSuccessAck`, `TestTCPDurablePhasesCompleteIndependentExchangeDuringReceiveCommit`, `TestTCPDurablePhasesPreserveDuplicateReleaseBeforeIdentifierReuse`, `TestTCPDurableOutboundAckKeepsIdentifierUntilOrderedCommit`, `TestWriteDeliveriesReservesPacketIDWhilePromotionWaits`에 연결한다. 준비된 다음 변경이 앞선 commit 대기 중에도 진행하되 성공 ACK/PUBREL·ID 재사용은 commit 뒤에만 가능한지, 실패 시 pending 작업을 회수하며 성공 ACK가 없는지 확인한다. Driver의 proof·projection gap·취소와 FIFO proposal은 `cmd/narumq/cluster_runtime_test.go::TestClusterCommitReadProofPreservesSubmissionOrderAndCancellation`이 실제 3-node Raft로 확인한다. 고정 workload의 실제 TCP·Docker 실행은 [BP-03 수용 증거](performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)를 따른다.
- Batch SEND 실패와 재시도는 `cmd/narumq/cluster_durable_test.go::TestClusterDurableBatchRetriesCommittedSuffixWithSameIdentifiers`와 `internal/adapters/tcp/server_phases_test.go::TestTCPPartialPromotionPreservesEverySuffixIdentifier`로 검증한다. 앞선 SEND가 commit되었지만 호출자에게 quorum 오류가 돌아오고 뒤의 SEND도 commit된 경우, 동일 ID로 재시도해 수렴하고 ADVANCE·COMPLETE 뒤 replicated message가 제거되는지 확인한다. TCP 시험은 일부 prefix만 wire에 보낸 뒤 durable/local queue 모두에서 모든 suffix ID를 보존하고, 재시도 wire topic·PacketID 대응이 유지되는지 확인한다.
- `TestMQTTIngressSharedSubscriptionHandoff`는 delivery queue가 가득 찬 persistent offline
  member에서 같은 group의 다른 member로 handoff하고 reconnect queued delivery를 보존하는
  실제 socket-level MQTT-07 failover 증거다.
- `tests/integration/mqtt_tls_test.go`는 TLS 1.2/1.3, trusted/untrusted/expired client
  certificate, mTLS identity의 exact Client ID binding과 MQTT 5 wire CONNACK 결과를
  실제 socket에서 검증한다. `cmd/narumq` TLS flag test는 client-auth mode와 mTLS
  identity binding의 strict configuration 조합을 검증한다.
- `MQTT-3.3.1-5`~`MQTT-3.3.1-13`, `MQTT-3.8.3-3`,
  `MQTT-3.8.4-3/4`는 tests/conformance/mqtt-compliance-v1.json에서 양 protocol
  version test와 source traceability comment를 함께 가져야 한다. `MQTT-3.3.2-5/6`는
  MQTT 5.0 Message Expiry property와 만료 흐름을 검증하는 test 및 source traceability
  comment를 가져야 하며 MQTT 3.1.1에는 이 property를 적용하지 않는다.
- 이 matrix와 gate는 전체 MQTT 표준 적합성 선언이 아니다. ACL·tenant와 durable
  cluster replication은 후속 범위로 남아 있다.

### 8.1 Resource limit·비정상 입력 수용 매핑

2026-10-05 Windows local 실행 결과다. TCP focused 시험은 2.318s, integration
focused 시험은 2.139s에 통과했다. TCP 전체 시험과 race 시험도 통과했다.

| Scenario | 관찰할 결과 | 시험 위치 | 실행 증거 |
| --- | --- | --- | --- |
| Server 한도 광고 | MQTT 5 CONNACK에 두 상한. MQTT 3.1.1 body는 2 byte | `internal/adapters/tcp/server_limits_test.go::TestTCPConnAckAdvertisesServerLimits` | PASS, TCP focused 2.318s |
| Reader protocol 오류 | MQTT 5는 0x95·0x81·0x82·0x93 후 종료. MQTT 3.1.1은 응답 없이 종료 | `internal/adapters/tcp/server_limits_test.go::TestTCPProtocolErrorsAfterConnectDisconnectWithReason` | PASS, 양 version·네 오류 |
| Inbound quota 반환과 DUP | ACK 후 새 PUBLISH 수용. 미완료 ID 재전송은 quota를 추가하지 않음 | `internal/adapters/tcp/server_limits_test.go::TestTCPReceiveMaximumQuotaReturnsAfterAcknowledgement` | PASS, TCP race 포함 |
| Packet 완성 시간 | 부분 packet은 timeout. KeepAlive=0 유휴 연결은 유지 | `internal/adapters/tcp/server_limits_test.go::TestTCPPartialPacketAfterConnectTimesOut` | PASS, 양 version·200ms timeout·600ms 유휴 |
| 수신 buffer 상한 | 작은 초기 buffer, 단계별 증가, 소비 후 반환 | `internal/adapters/tcp/frame_reader_test.go::TestFrameReaderStartsSmallGrowsByDoublingAndReleases` | PASS |
| Control queue 상한 | 용량 1에서 PINGREQ가 쌓이면 연결 종료 | `internal/adapters/tcp/server_test.go::TestTCPControlQueueFullClosesConnection` | PASS, TCP queue focused 0.742s |
| Delivery queue 상한 | 읽지 않는 subscriber의 queue가 차면 해당 session 종료. Publisher는 PINGRESP 수신 | `internal/adapters/tcp/server_test.go::TestTCPDeliveryQueueSaturationClosesSubscriber` | PASS, TCP queue focused 0.742s |
| Queued·in-flight 용량과 FIFO | Queue가 차면 enqueue·promote는 `ErrFull`. 실패한 promote는 head를 보존. 앞선 delivery는 FIFO로 유지 | `internal/adapters/memory/deliveryqueue/queue_test.go::TestQueueFIFOAndPayloadOwnership`, `TestQueueOutboundCapacityLeavesHeadOnFailure` | PASS, memory queue focused 0.447s |
| Packet 설정 상한 | MQTT 최대값+1과 `int` 최대값은 `NewServer`에서 `ErrInvalidConfig` | `internal/adapters/tcp/server_connection_test.go::TestNewServerValidatesEveryDependencyAndBound` | 두 case 모두 수정 전 FAIL, 수정 후 PASS |
| 유효한 최대 packet 설정 | Remaining Length 268435455에서 실제 MQTT 5 연결과 PING 성공 | 실제 binary boundary smoke | PASS. CONNACK Receive Maximum=2, Maximum Packet Size=268435460, PINGRESP 수신 |
| Client Receive Maximum | ACK 후 다음 PUBLISH 전달. 복구 PUBREL은 지연하지 않음 | `internal/adapters/tcp/server_limits_test.go::TestWriteDeliveriesHonorsClientReceiveMaximum`, `TestWriteRecoveredDeliveriesHonorsClientReceiveMaximum`; `tests/integration/mqtt_resource_limits_test.go::TestMQTTOutboundHonorsClientReceiveMaximum` | PASS, 실제 socket과 복구 상태 |
| Client Maximum Packet Size | 큰 PUBLISH는 전송 완료로 제거. 작은 PUBLISH는 순서대로 전달 | `internal/adapters/tcp/server_limits_test.go::TestWriteDeliveriesDiscardsPublishAboveClientMaximumPacketSize`; `tests/integration/mqtt_resource_limits_test.go::TestMQTTOutboundDiscardsPacketsAboveClientMaximumPacketSize` | PASS, promote→complete와 persistent reconnect |
| 비 PUBLISH packet 한도 | 성공·거절 CONNACK도 client 한도를 넘으면 전송하지 않고 종료 | `internal/adapters/tcp/server_limits_test.go::TestTCPConnAckAboveClientMaximumPacketSizeClosesWithoutSending` | PASS, client Maximum Packet Size=1 |
| Stream fuzz | 비정상 입력 후 연결 goroutine 종료 | `internal/adapters/tcp/server_fuzz_test.go::FuzzConnectionInput` | PASS, 10000x·single worker, 12.434s |
| Slowloris·느린 reader·반복 오류 | CONNECT 전 64개 소켓에서 heap 증가 16 MiB 미만. 종료 후 connection·goroutine·heap 회수 | `tests/integration/mqtt_resource_limits_test.go::TestMQTTResourceLimitsReleaseConnectionsGoroutinesAndMemory` | PASS 0.91s. Heap 증가 484216 byte. Goroutine 7→7. Heap 628072→787320 byte |
| Traceability·binary | 실제 flag·wire 동작과 추적 매핑 확인 | `go run ./tools/verify traceability`; binary smoke | PASS. CONNACK 2/1048580, DISCONNECT 0x93, 부분 packet 2.0083697s 후 종료. `-receive-maximum 0`은 지정 오류로 종료 |
| 실제 SDK 회귀 | Paho Go/Python의 CONNACK·기존 handshake 호환 | `go run ./tests/compatibility/mqtt-clients-v1 --run-id phase1-resource-limits --output .artifacts/compatibility/mqtt-clients-v1/phase1-resource-limits` | PASS 26/26, 180.65s. `.artifacts/compatibility/mqtt-clients-v1/phase1-resource-limits`에 결과 보존 |
| 전체 local gate | 전체 package·race·다섯 fuzz·coverage 통과 | `GOMAXPROCS=4 go run ./tools/verify local` | 설정 상한 수정 후 PASS, 147.22s. Traceability·architecture·coverage 포함. TCP stream fuzz 10000x, 20.068s |

변경 전 임시 socket 실험은 CONNECT 전 부분 입력 64개에서 heap 증가
66884496 byte로 16 MiB 기준을 넘었다. 변경 후 같은 실험은 515944 byte로
통과했다. 임시 시험과 binary smoke client는 확인 뒤 제거했다. Listener 최대
연결 수·connection rate와 tenant message rate의 완료 증거는 아니다.

전체 회귀에서 발견한 기존 MQTT 5 cluster CONNACK helper의 property-free
가정은 `tests/integration/cluster_drain_process_test.go`에서 domain decoder로
바꿨다. Property가 있는 성공 CONNACK도 검증한 뒤 session 결과를 읽는다.

완료 검증 중 `TestRunFlushesRuntimeMetricsOnCancellation`이 초기 기록 하나만
관찰한 실행이 있었다. Sampler는 같은 timestamp의 기록을 생략하므로 시험은
완성된 초기 JSONL과 다음 clock tick을 확인한 뒤 취소하도록 고쳤다. 제품의
sampler 동작은 바꾸지 않았다. 이 시험은 별도 30회 반복에서 PASS했다.
