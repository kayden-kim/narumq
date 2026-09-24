# Raft peer transport v1 Contract

이 문서는 고정 3-voter `{1,2,3}` 사이에서 serialized Raft protobuf message를 교환하는 transport byte format과 자원 경계를 정의한다. 이 contract는 release N/N-1 compatibility, membership protocol 또는 application command encoding을 정의하지 않는다. Network read/write의 구체 경계는 [ADR-0008](../decisions/ADR-0008-raft-peer-transport.md), consensus 제한은 [ADR-0002](../decisions/ADR-0002-cluster-consensus-library.md)를 따른다.

| 항목 | 값 |
| --- | --- |
| 이름과 version | `narumq-raft-peer-transport` 1.0.0 |
| 상태 | Active |
| Producer → Consumer | NaruMQ Raft node ↔ NaruMQ Raft node |
| 방향 | TLS 연결 위의 양방향 stream |
| 기준 형식 | 이 문서의 hello와 frame encoding |
| 관련 spec | [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md) |
| 관련 ADR | [ADR-0008](../decisions/ADR-0008-raft-peer-transport.md) |
| 관련 requirements | CLU-01~CLU-02, CLU-09~CLU-10, SEC-09, NFR-07~NFR-10 |
| 검증 | `internal/adapters/tcp/peer_transport_test.go`, `tests/integration/raft_peer_transport_test.go` |

## 1. Connection security and identity

1. TCP connection은 TLS 1.3으로만 협상한다. Client와 server 양쪽은 신뢰된 CA에서 발급한 인증서 chain과 적절한 TLS EKU를 검증한다. Server는 client certificate를 필수로 요구한다.
2. 각 node config는 다른 두 voter ID마다 TCP address와 expected DNS SAN을 지정한다. Certificate에 해당 DNS SAN이 없거나 chain/EKU가 유효하지 않으면 연결을 거절한다.
3. TLS 이후 양방향 hello를 교환한다. Hello의 cluster ID와 version은 local config와 정확히 일치해야 하며 peer ID는 `{1,2,3}` 중 local ID와 다른 allowlisted ID여야 한다.
4. Certificate SAN이 해당 hello peer ID에 설정된 DNS name과 일치해야 한다. 알 수 없거나 등록된 다른 node의 identity로 주장하는 hello는 닫고 message를 전달하지 않는다.
5. 동일 peer pair에는 더 낮은 node ID가 dial하고 더 높은 ID가 accept한다. 각 handshake는 최대 5초다.

## 2. Hello encoding

모든 integer는 unsigned, big-endian(network byte order)이다. TLS handshake 성공 직후 각 endpoint는 아래 형식을 한 번 보내고 상대의 형식을 한 번 읽는다.

| Offset | Width | Field | 제한 |
| --- | ---: | --- | --- |
| 0 | 8 bytes | Magic | ASCII `NMQRAFT1` |
| 8 | 2 bytes | Major | `1` |
| 10 | 2 bytes | Minor | `0` |
| 12 | 2 bytes | Cluster ID length | UTF-8 byte length `1..200` |
| 14 | 1..200 bytes | Cluster ID | UTF-8; 선언 길이만큼 |
| 14 + length | 8 bytes | Node ID | configured voter ID `1..3` |

Hello 전체 길이는 23..222 bytes다. Parser는 fixed header를 읽고 길이가 1..200인지 확인한 뒤에만 Cluster ID buffer를 allocate한다. Invalid UTF-8, magic, version, cluster, node ID 또는 certificate identity는 connection rejection이다.

Golden vector (`clusterID="x"`, `nodeID=1`):

```text
4e 4d 51 52 41 46 54 31  00 01 00 00 00 01 78  00 00 00 00 00 00 00 01
```

## 3. Raft message frame

각 Raft message는 다음 frame 하나로 인코딩한다.

```text
uint32 big-endian payload length | serialized Raft protobuf bytes
```

- Payload length는 `1..1,048,576` bytes다. Zero/oversized length는 allocation 전에 거절되고 malformed/truncated frame은 connection을 닫는다.
- Payload는 Raft adapter가 serialize한 protobuf message다. Transport는 그 의미를 decode하거나 변환하지 않는다. Consumer의 Raft adapter는 protobuf `From`/`To`를 envelope identity와 대조한다.
- Frame order는 한 peer connection에서 FIFO다. Reconnect 뒤 queued-but-undelivered frame의 remote delivery 여부는 보장하지 않는다. Driver는 backpressure rejection을 `ReportPeerMessageFailure`로 Raft에 알려 protocol retry가 복구하게 한다.

Golden vector (payload bytes `08 01`):

```text
00 00 00 02 08 01
```

## 4. Queue and backpressure

각 peer마다 outbound FIFO와 inbound staging은 각각 아래 한도를 넘지 않는다.

| Limit | 값 | 계수 규칙 |
| --- | ---: | --- |
| Message count | 32 | Outbound writer가 현재 쓰는 frame도 포함 |
| Framed bytes | 4 MiB | 4-byte length prefix와 payload 합계 |
| Frame payload | 1 MiB | Prefix 제외 |
| I/O timeout | 5 s | Handshake, full-frame read/write |
| Reconnect interval | 100 ms | Peer 단위 retry |

Outbound `Send`는 payload를 소유한 copy로 queue에 넣는다. Queue count 또는 bytes limit에 걸리면 즉시 `ErrPeerBackpressure`를 반환하며 기존 queue를 넘기거나 다른 peer를 차단하지 않는다. Driver는 반환된 Ready message를 다른 peer로 계속 전달하고, 거절된 destination에 Raft의 unreachable/snapshot-failure 보고를 보내 재전송을 유도한다. Queue rejection은 개별 message delivery를 보장하지 않으며, 성공한 queue admission도 network delivery, peer processing, Raft commit, durable ACK 또는 MQTT readiness를 의미하지 않는다. Inbound staging이 가득 차면 해당 connection을 닫고 buffer bound를 지킨다.

Raft `MaxInflightMsgs=16`(entry-bearing `MsgApp`), queue count/bytes, OS socket buffer와 retained log는 각각 독립적인 bound다. `ProcessReady`는 한 batch만 처리하므로 rejected peer 때문에 다음 peer의 message 처리가 막히지 않는다.

## 5. Compatibility and failure handling

| Advertised protocol | Peer protocol | 결과 | Evidence |
| --- | --- | --- | --- |
| `1.0` | `1.0`, same cluster와 certificate identity | Accept | `TestPeerTransportHelloGoldenVector`, `TestRaftPeerTransportThreeNodeReplicationAndCatchup` |
| major/minor mismatch 또는 지원하지 않는 version | any | Reject before message delivery | `TestPeerTransportHelloRejectsInvalidFields`, `TestRaftPeerTransportRejectsUnauthenticatedAndMalformedPeers` |

이 표는 transport wire protocol만 다루며 software release N/N-1 compatibility를 약속하지 않는다.

- Version/cluster mismatch, untrusted certificate, unknown/mismatched node identity와 malformed/truncated/oversized frame은 connection을 닫으며 invalid frame은 Raft Node에 전달되지 않는다.
- Length-valid malformed protobuf와 authenticated envelope와 protobuf `From` 불일치는 Raft adapter가 거절한다. Peer-only driver는 해당 peer connection을 reset하며 node state/command에 적용하지 않는다.
- TLS certificate/private key와 CA secret은 이 contract나 log에 기록하지 않는다. Peer status의 오류 문구는 인증서나 파일의 secret material을 노출하지 않아야 한다.
- Peer reconnect는 해당 peer의 transport state만 복구한다. Membership, readiness, durable publish ACK 또는 delivery guarantee를 만들지 않는다.
- Byte layout이나 required identity semantics를 바꾸는 incompatible change는 새 contract major가 필요하다.
