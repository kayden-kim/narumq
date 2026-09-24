# ADR-0008: Raft peer transport v1은 mTLS TCP와 bounded queues를 사용한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 관련 요구사항 | CLU-01~CLU-02, CLU-09~CLU-10, SEC-09, NFR-07~NFR-10 |
| 관련 스펙 | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md) |
| 관련 Architecture | [Cluster architecture](../architectures/cluster-architecture.md), [Project structure](../architectures/project-structure.md) |
| 관련 Contract | [Raft peer transport v1](../contracts/raft-peer-transport-v1.md) |
| 상위 결정 | [ADR-0002](ADR-0002-cluster-consensus-library.md) (계속 Proposed) |

## 1. 결정

현재 고정 voter `{1,2,3}`을 위한 transport v1은 TCP와 TLS 1.3 mutual TLS(mTLS)를 사용한다. 이 결정은 peer 연결, 인증, frame 형식, 메모리 한도와 재연결만 다룬다. `etcd/raft` production version, WAL·snapshot engine과 `fsync`, membership 변경, replicated state encoding, MQTT readiness·durable ACK 또는 release N/N-1 호환성을 승인하지 않는다. 따라서 [ADR-0002](ADR-0002-cluster-consensus-library.md)는 계속 `Proposed`이고 [cluster release compatibility v1](../contracts/cluster-release-compatibility-v1.md)은 `Draft`로 남는다.

- 서버와 client 모두 신뢰된 CA의 인증서를 검증하고, 서버는 client 인증서를 필수로 요구한다. TLS 최소 버전은 1.3이다.
- 각 configured peer는 고정 node ID, TCP address 및 인증서 DNS SAN을 가진다. TLS 체인/EKU 검사에 더해 hello의 node ID가 allowlist의 DNS SAN에 해당하는지 확인한다. Unknown ID, 다른 인증서 identity, cluster ID 또는 protocol version 불일치는 연결을 닫는다.
- TLS handshake 뒤 양쪽은 [contract](../contracts/raft-peer-transport-v1.md)의 `NMQRAFT1` hello를 교환한다. 각 Raft protobuf message는 uint32 network-order 길이 prefix를 가진다. 길이와 I/O deadline을 검사한 뒤에만 payload를 할당한다.
- 각 peer의 outbound 및 inbound staging queue는 각각 최대 32개 message, 4 MiB framed bytes다. Outbound writer가 쓰고 있는 message도 limit에 포함한다. 포화 시 즉시 backpressure를 반환하고 해당 peer connection만 재설정한다. 다른 peer나 quorum 처리는 차단하지 않는다.
- peer I/O·handshake timeout은 5초, 재연결 주기는 100 ms다. 한 peer의 재접속은 그 peer의 이전 inbound connection만 교체하며 다른 peer의 writer/queue는 유지한다.
- 동일 peer 간 중복 연결을 피하기 위해 node ID가 작은 쪽만 dial하고 큰 쪽은 accept한다.

이 숫자와 protocol byte layout은 고정 3-node transport v1의 선택이며 release 간 호환성 약속이 아니다. Transport queue의 bounded capacity, Raft의 entry-bearing `MsgApp` inflight 한도(16), OS socket buffer와 retained Raft log는 별도 한도다.

## 2. 배경

`internal/adapters/raft.Node`는 durable `ProcessReady()` 후 직렬화한 outbound `MessageDTO`를 반환하고 `Step()`으로 inbound 메시지를 받는다. 기존 통합 시험은 FIFO in-process delivery만 사용해 실제 network authentication, framing, queue memory 또는 transport backpressure를 확인하지 않는다. ADR-0002는 이 책임들을 NaruMQ에 남긴다.

## 3. 결정 요인

1. Peer identity를 TLS trust chain, certificate SAN과 고정 voter allowlist 모두에 묶는다.
2. Length 검증 전에 큰 payload를 allocate하지 않고 malformed input을 적용하지 않는다.
3. Slow peer 하나가 다른 peer의 Raft progress를 막지 않으며, message 수와 bytes 양쪽 모두 bounded다.
4. Application/domain과 Raft adapter가 network adapter에 의존하지 않도록 조립 책임을 `cmd/narumq`에 둔다.
5. MQTT ACK, node membership, readiness, release compatibility가 아직 구현됐다고 오인하지 않는다.

## 4. 선택 근거와 범위

TLS 1.3 mTLS는 별도 RPC dependency 없이 표준 Go TLS stack에서 chain, SAN, EKU를 검증한다. Fixed voter allowlist는 현재 Node의 `{1,2,3}` membership과 일치하고 discovery나 membership protocol을 미리 도입하지 않는다. Length-prefixed opaque protobuf는 기존 Raft adapter의 serialized message boundary를 유지하며 frame parser가 payload 크기를 먼저 제한할 수 있다.

Per-peer bounded FIFO는 slow consumer의 메모리 사용을 count와 byte로 제한한다. 포화된 연결을 닫으면 Raft의 heartbeat·재전송 경로가 복구를 담당한다. 성공한 `Send`는 local queue admission일 뿐 remote delivery, log durability 또는 commit receipt를 의미하지 않는다.

Transport는 `internal/adapters/tcp`의 adapter-owned DTO를 사용하고 `internal/adapters/raft`를 import하지 않는다. `cmd/narumq`만 concrete transport와 Raft node를 함께 만들고 DTO를 변환한다. Replication use case/port가 아직 없으므로 이번 결정에서 `application/ports/transport`를 추가하지 않는다.

## 5. 대안

| 대안 | 장점 | 제외 이유 |
| --- | --- | --- |
| HTTP/2 or gRPC transport | Framing, stream lifecycle와 tooling이 제공된다. | 고정 Raft message path에 추가 dependency/protocol lifecycle을 두고 현재 version compatibility를 미리 결정해야 한다. |
| TLS 없는 private-network TCP | 구현이 단순하다. | Network placement만으로 peer identity와 unauthorized member 거절을 보장할 수 없다. |
| 모든 peer가 상호 dial | 단순한 재시도 모델 | 각 pair에 중복 연결이 생기고 메시지 순서/대체 규칙이 추가된다. ID 방향 규칙으로 하나만 유지한다. |

## 6. 결과와 제한

- Certificate SAN에 설정된 peer DNS name이 있어야 하고, 양방향 TLS를 위한 client/server EKU를 갖춰야 한다.
- Peer failure는 transport status에서 degraded로 드러난다. Bounded queue rejection은 해당 peer의 `ReportUnreachable`와 snapshot failure를 RawNode에 전달해 protocol 재전송을 유도하고, 같은 Ready의 다른 peer message는 계속 처리한다.
- WAL/Node의 terminal failure는 driver의 fatal error다. 현재 executable peer-only mode는 MQTT endpoint, proposal API, readiness 또는 durable ACK를 제공하지 않는다.
- Membership change, dynamic discovery, certificate rotation, mixed-version rollout과 production replicated state는 후속 결정이다.

## 7. 검증

구현과 검증 결과를 [cluster acceptance mapping](../specs/cluster-session-and-delivery.md#113-phase-0-gate-4-production-peer-transport-mapping)에 연결한다.

1. Golden hello/frame과 length boundaries는 contract와 일치한다.
2. Untrusted CA, 다른 allowlisted identity, cluster/version mismatch, malformed/truncated/oversized frames는 node state에 적용되지 않는다. Invalid protobuf/envelope는 adapter가 거절하고 peer connection을 reset한다.
3. Per-peer count/byte cap, backpressure 중 healthy peer progress, rejected message의 Raft 재생성을 확인한다.
4. 실제 loopback 3-node TLS cluster에서 quorum progress, isolated peer의 catch-up 및 cancellation cleanup을 확인한다.
5. Transport, integration, CLI tests와 project traceability/architecture/local verification 결과는 §11.3에 기록한다.

## 8. 재검토 조건

- Dynamic membership가 static allowlist나 direction-based dial 규칙을 대체한다.
- Frame/message 크기, queue memory 또는 reconnect 지연의 workload evidence가 현재 한도와 맞지 않는다.
- N-1↔N interoperability, transport encryption policy 또는 certificate rotation 요구가 확정된다.
- Durable ACK/readiness가 실제 transport status와 통합될 때 health semantics를 다시 설계한다.
