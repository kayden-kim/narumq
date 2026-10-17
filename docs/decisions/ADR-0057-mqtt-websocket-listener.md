# ADR-0057: MQTT WebSocket/WSS listener

| 항목 | 내용 |
| --- | --- |
| 상태 | `Accepted` |
| 결정일 | 2026-10-11 |
| 결정자 | 사용자 |
| 관련 요구사항 | [PRD MQTT-09·MQTT-10](../requirements/PRD.md) |
| 관련 스펙·계약 | [MQTT protocol과 session §1](../specs/mqtt-protocol-and-session.md#1-listener-계약), [Process configuration v1 §1](../contracts/process-configuration-v1.md#1-형식과-의미) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md), [Broker core](../architectures/broker-core.md) |
| 관련 ADR | [ADR-0047](ADR-0047-mqtt-password-authentication.md), [ADR-0052](ADR-0052-configuration-tiers-and-cluster-policy.md) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

MQTT-10을 구현한다. WebSocket과 WSS listener는 기존 TCP server에 `net.Listener`로 연결하며, TCP와 같은 MQTT packet·session 처리와 listener별 admission 제한을 사용한다.

WebSocket adapter는 `github.com/coder/websocket` v1.8.15를 사용한다. Dependency는 ISC license이며 transitive dependency가 없다. 새 `internal/adapters/websocket` package는 inner `net.Listener`를 감싸고 binary WebSocket message를 byte stream `net.Conn`으로 노출한다. `tcp.Server`의 MQTT state machine은 바꾸지 않는다. 각 listener는 TCP server의 같은 session·route registry를 공유한다.

WebSocket 연결은 `/mqtt` 경로와 `mqtt` subprotocol만 허용한다. Text frame은 거절한다. Compression은 끄고 HTTP/2는 사용하지 않는다. HTTP header는 8 KiB로 제한한다. `Origin`은 검사하지 않는다. MQTT authentication은 MQTT credential을 사용하고 cookie authentication을 사용하지 않기 때문이다.

WSS는 기존 MQTT TLS profile과 certificate·client-auth 설정을 공유한다. WebSocket connection은 verified TLS `ConnectionState`를 TCP TLS adapter가 읽을 수 있게 노출한다. mTLS identity는 verified client certificate에서 만든다.

Listener 계층 순서는 다음과 같다.

```text
raw TCP → admission limit(설정한 경우) → TLS(WSS만) → WebSocket HTTP upgrade
```

Admission은 TLS handshake와 HTTP upgrade 전에 적용한다. WS/WSS의 동시 연결 수와 connection rate는 각 listener에 따로 설정한다.

Standalone `-ws-listen`은 literal loopback 주소만 허용하고 `-security-config`를 요구한다. `-allow-insecure-anonymous-mqtt`는 TCP listener에만 적용하며 WS/WSS와 함께 사용할 수 없다. Standalone `-wss-listen`은 기존 `-tls-*` 설정을 사용한다. Dynamic cluster의 `mqtt.webSocket.listenAddress`는 유효한 host:port를 허용하고 security config를 항상 요구한다. `mqtt.webSocket.tlsListenAddress`는 `mqtt.tls` profile을 공유한다.

HTTP upgrade와 WSS TLS handshake에는 고정 10초 timeout을 적용한다. MQTT CONNECT timeout은 upgrade 성공 뒤 기존 설정대로 시작한다. Source IP는 hijacked TCP socket의 peer address다. `X-Forwarded-For`는 읽지 않는다.

Listener 종료는 WebSocket `CloseNow` semantics를 사용한다. Close handshake를 기다리지 않아 unresponsive peer가 shutdown을 지연시키지 않는다. `websocket`과 `wss`는 connection metric과 listener admission metric의 고정 transport label이다.

Paho 또는 다른 MQTT SDK와의 WebSocket compatibility는 이 변경의 acceptance 범위가 아니다.

## 2. 배경

PRD MQTT-10은 WebSocket과 WSS listener를 요구한다. 사용 필요성은 사용자가 확인했다. 기존 broker는 MQTT TCP/TLS listener와 공통 `tcp.Server`가 packet, authentication, session, route 및 connection lifecycle을 처리한다. WebSocket을 추가하려면 이 처리 경로를 복제하지 않고 기존 byte-stream 경계에 연결해야 한다.

HTTP upgrade, TLS handshake와 MQTT admission 순서는 자원 사용과 source IP 판정에 영향을 준다. 별도 transport label은 기존 connection 및 admission metric에서 WebSocket을 구분하는 데 필요하다.

## 3. 결정 요인

1. TCP와 같은 MQTT packet·session 동작을 재사용한다.
2. HTTP upgrade와 TLS handshake 전에 설정된 admission 제한을 적용한다.
3. mTLS certificate identity와 TCP peer source IP를 기존 보안 경계에 전달한다.
4. 종료 시 무응답 peer를 기다리지 않는다.
5. 외부 dependency와 API 표면을 작고 검증 가능하게 유지한다.

## 4. 선택 근거

`net.Conn` byte stream은 `tcp.Server`의 `frameReader`, deadline, writer, drain, blocklist와 tenant logic이 이미 소비하는 경계다. WebSocket message 경계에서 payload를 읽고 이어 붙이는 adapter는 MQTT state machine을 복제하지 않고 기존 behavior를 유지한다.

`coder/websocket` v1.8.15는 RFC 6455 구현을 제공하고 transitive dependency가 없다. Hand-written protocol 구현보다 유지보수 부담이 작고 legacy `golang.org/x/net/websocket`보다 read/close control을 제공한다.

Admission을 raw TCP와 TLS/WebSocket wrapper 사이에 두면 limit을 넘긴 connection이 TLS handshake나 HTTP upgrade 비용을 쓰기 전에 거절된다. WSS의 `ConnectionState()` 노출로 기존 certificate extractor를 재사용한다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| MQTT packet state machine을 WebSocket adapter에 복제 | WebSocket message를 직접 다룬다. | 인증·session·route·deadline·shutdown logic이 중복된다. | 기존 `tcp.Server`의 `net.Conn` 경계를 재사용한다. |
| RFC 6455 직접 구현 | 외부 dependency가 없다. | handshake, framing, control frame, masking, close와 timeout 처리를 직접 유지해야 한다. | `coder/websocket` v1.8.15를 사용한다. |
| `golang.org/x/net/websocket` | 기존 Go module을 사용할 수 있다. | 필요한 read/close control이 부족하고 legacy API다. | 선택한 library의 protocol control을 사용한다. |
| Graceful close handshake 대기 | Peer가 정상 close를 관찰할 수 있다. | 무응답 socket마다 shutdown이 지연될 수 있다. | `CloseNow`로 빠른 bounded shutdown을 보장한다. |
| `Origin` 검사 | Browser cookie 기반 접속의 일부 CSRF 위험을 줄일 수 있다. | MQTT auth가 cookie를 사용하지 않으며 allowlist 구성도 새 설정이 된다. | 현재 credential 기반 인증 경계에서 검사하지 않는다. |

## 6. 결과

### 긍정적 결과

- WS/WSS는 기존 MQTT packet, session, authentication, route와 shutdown 경로를 공유한다.
- Listener admission은 TLS handshake와 HTTP upgrade 전에 동작한다.
- WSS는 TLS 1.2 이상 정책과 mTLS binding을 기존 TLS 설정에서 재사용한다.
- Metric label은 `tcp`, `tls`, `ws`, `wss`로 고정된다.

### 부정적 결과와 비용

- `coder/websocket` dependency를 유지해야 한다.
- Standalone WS는 loopback 전용이다. 외부 plain WebSocket 사용은 지원하지 않는다.
- WS/WSS listener마다 별도 동시 연결 수와 connection rate 설정이 필요하다.
- WebSocket SDK interoperability는 검증되지 않는다.

### 후속 작업

- Listener 계약, process configuration, Management API schema와 적용 설정 조회를 같은 변경에서 갱신한다.
- Adapter, command, 실제 socket, cluster process acceptance를 MQTT-10 시험에 연결한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Binary WebSocket byte stream이 MQTT packet framing을 보존한다. | Adapter와 실제 socket integration 시험 | Split message와 multi-packet message가 같은 byte stream으로 decode된다. | 구현 뒤 [MQTT spec §8.3](../specs/mqtt-protocol-and-session.md#83-websocketwss-수용-매핑)에 기록한다. |
| Timeout과 종료가 TCP server에 호환된다. | Adapter deadline·close 시험과 KeepAlive Will integration 시험 | Deadline은 timeout error로 분류되고 Close는 unresponsive peer를 기다리지 않는다. | 구현 뒤 [MQTT spec §8.3](../specs/mqtt-protocol-and-session.md#83-websocketwss-수용-매핑)에 기록한다. |
| WSS mTLS와 source IP가 기존 보안 경계로 전달된다. | Certificate와 blocklist integration 시험 | Verified client CN 및 TCP peer IP가 올바른 identity·blocklist 판정에 사용된다. | 구현 뒤 [MQTT spec §8.3](../specs/mqtt-protocol-and-session.md#83-websocketwss-수용-매핑)에 기록한다. |
| Settings와 applied-config response가 contract를 따른다. | CFG-SCN-40..46 parser/runtime/API 시험 | 주소·TLS·anonymous·admission·API response 경계를 검사한다. | 구현 뒤 [Configuration spec §6](../specs/configuration-and-runtime-options.md#6-수용-시험)에 기록한다. |

## 8. 재검토 조건

- 운영에서 외부 plain WebSocket bind가 필요하다는 요구가 승인되면 standalone bind 정책을 재검토한다.
- Browser cookie authentication을 도입하면 `Origin` 정책을 별도 보안 결정으로 검토한다.
- 지원 SDK 호환성이 요구되면 해당 SDK와 version을 명시한 별도 compatibility acceptance를 추가한다.
