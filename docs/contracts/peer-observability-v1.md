# Peer observability v1 계약

이 계약은 Dynamic cluster에서 한 node가 다른 node의 실제 운영 snapshot을 조회하는 내부 TCP 교환 형식을 정의한다. Producer와 consumer는 `internal/adapters/tcp`의 peer transport와 관측 query service다. HTTP credential은 피어에 전달하지 않는다.

| 항목 | 값 |
| --- | --- |
| 이름과 version | `narumq-peer-observability` 1.0.0 |
| 상태 | Draft |
| Producer | 요청을 받은 Dynamic peer transport의 local snapshot handler |
| Consumer | Dynamic cluster 관측 query service |
| 방향 | 요청·응답, 연결당 한 번 |
| 관련 계약 | [Raft 멤버십 v2 §1.5](raft-membership-v2.md#15-피어-전송-v2-핸드셰이크와-프레이밍), [Management API v1](management-api-v1.md) |
| 관련 스펙 | [Management API와 observability §4·§7](../specs/management-api-and-observability.md) |

## 1. 보안과 지원 범위

- 같은 peer listener에서 TLS 1.3 mTLS를 사용한다. 기존 CA·EKU·등록 인증서 지문·DNS SAN·cluster ID·release 호환성 검사를 다시 적용한다.
- Caller와 target는 commit된 peer여야 한다. `STAGED`와 `RETIRED`는 caller 또는 target로 허용하지 않는다.
- 관측 capability는 정확히 `cluster-observability-v1`이다. Source member의 기능 목록에 이 값이 없으면 RPC를 보내지 않고 `UNSUPPORTED`로 처리한다.
- RPC는 node-local handler만 호출한다. Handler는 peer query를 재귀 호출하지 않는다. 관측 트래픽은 Raft와 forwarding 연결·queue를 사용하거나 교체하지 않는다.
- Frame이 올바르지 않거나 TLS·identity 검증에 실패하면 응답을 보내지 않고 연결을 닫는다. 자격 증명, 주소와 원시 I/O 오류를 응답에 싣지 않는다.

## 2. Frame 형식

모든 frame은 big-endian `uint32` payload 길이와 payload로 구성된다. 길이 prefix는 payload 한도에 포함하지 않는다. 한 연결은 한 request/response 교환만 처리하고 server가 닫는다. Client는 response를 받은 뒤 server의 close를 확인한다.

첫 payload는 기존 Dynamic peer hello와 같은 identity field layout을 사용한다. 매직 값은 ASCII `NMQOBS01` 8 bytes이고 최대 payload는 1,024 bytes다. Identity는 Raft hello와 같은 commit된 member identity, phase, operation ID, address·DNS SAN, certificate fingerprint, release·cluster protocol·state schema 정보를 포함하며 동일한 인증 규칙을 따른다.

두 번째 payload는 정확히 11 bytes다.

| 순서 | 필드 | 인코딩 | 규칙 |
| --- | --- | --- | --- |
| 1 | wire version | `uint16` | `1` |
| 2 | kind | `uint8` | `1=node`, `2=metrics` |
| 3 | target member ID | `uint64` | 양수이며 수신 node 자신의 ID와 같아야 한다 |

알 수 없는 version·kind, 다른 target ID, 누락 또는 뒤따르는 byte가 있으면 요청을 거절한다.

Response payload는 최대 8 MiB다.

| 순서 | 필드 | 인코딩 | 규칙 |
| --- | --- | --- | --- |
| 1 | magic | ASCII 8 bytes | `NMQOBS01` |
| 2 | wire version | `uint16` | `1` |
| 3 | status | `uint8` | `1=OK`, `2=UNSUPPORTED`, `3=BUSY`, `4=SNAPSHOT_TOO_LARGE`, `5=INTERNAL_ERROR` |
| 4 | responder member ID | `uint64` | 인증된 수신 node ID |
| 5 | kind | `uint8` | request kind와 같아야 한다 |
| 6 | body | 남은 payload | `OK`에서만 하나의 NodeData 또는 NodeMetrics JSON |

`OK` response는 정확히 하나의 JSON object여야 하며 중복 key·알 수 없는 field·추가 JSON 값·kind 불일치·responder ID 불일치를 거절한다. Decoder는 object·array를 포함한 container nesting 깊이 최대 16을 허용하고 입력 길이를 확인한 뒤에만 body를 읽는다.

Node JSON의 nullable field·timestamp·query metadata 형식은 Management API OpenAPI `NodeData`와 같다. Metrics JSON의 typed family schema는 Management API `MetricFamily`·`NodeMetricsData`를 따른다. Integer 값은 정확한 base-10 문자열이다. Float는 finite 값의 canonical decimal 문자열이다. NaN·Inf, 중복 label, 알 수 없는 family·descriptor와 schema 위반은 거절한다.

## 3. 한도와 시간

- 전체 HTTP query 또는 collection은 5초를 넘지 않는다. 각 RPC는 남은 request deadline과 5초 중 짧은 시간을 사용한다.
- Node readiness 판단은 local 2초 상한을 가진다. 이 제한 안에 readiness proof가 실패하면 정상 관측 결과 `ready=false`로 반환한다. Request 취소는 query failure다.
- Process별 outbound RPC slot은 8개다. Collection은 최대 8 worker를 사용한다. Server authenticated handler와 local snapshot 읽기도 각각 8개 동시 작업으로 제한한다.
- Server는 transport 전체 inbound limit에 관측 slot 8개를 더한다. Seed와 기존 Raft·forwarding queue 한도는 바꾸지 않는다.
- Slot 대기와 local handler는 요청 context로 제한한다. 취소 시 socket을 닫고, 종료 시 transport가 모든 관측 handler를 기다린다.
- 자동 재시도와 주기 cache는 없다. Snapshot이 8 MiB를 넘으면 `SNAPSHOT_TOO_LARGE`로 응답한다.

## 4. 시험 연결

실제 mTLS socket, 양방향 member ID 조합, phase·identity 거절, 잘못된 frame, duplicate JSON, 크기 한도, 8개 처리 slot과 BUSY, 취소·종료 뒤 slot 반환을 `internal/adapters/tcp/peer_observability_test.go`에서 검증한다. Application 계약과 HTTP 상태는 `internal/application/observability` 및 `tests/contracts/management-api-v1`에서 검증한다.
