# Raft snapshot stream v1 계약

이 계약은 직렬화한 Raft `MsgSnap`이 peer frame 한도(1 MiB)를 넘을 때 leader가 대상 peer에 snapshot을 전달하는 별도 TCP 교환 형식을 정의한다. Producer와 consumer는 `internal/adapters/tcp`의 Dynamic peer transport다. 결정은 [ADR-0062](../decisions/ADR-0062-raft-snapshot-stream.md)다.

| 항목 | 값 |
| --- | --- |
| 이름과 version | `narumq-raft-snapshot-stream` 1.0.0 |
| 상태 | Active (2026-10-16 채택; EKS 실행 결과는 별도 수용 기록) |
| Producer | Leader node의 Dynamic peer transport (`SendSnapshot`) |
| Consumer | 대상 peer의 Dynamic peer transport (stream handler → Raft `Step`) |
| 방향 | 송신자가 연다. 요청·응답은 lock-step이다. 한 연결에 transfer 하나 |
| Feature gate | `snapshot-stream` (활성화 뒤에만 송신) |
| 관련 계약 | [Raft 멤버십 v2 §1.5](raft-membership-v2.md#15-피어-전송-v2-핸드셰이크와-프레이밍), [Release compatibility v1](cluster-release-compatibility-v1.md), [Replicated-state v1](replicated-state-v1.md) |
| 관련 스펙 | [Cluster §7](../specs/cluster-session-and-delivery.md#7-failure-recovery) 7항, [§11.37](../specs/cluster-session-and-delivery.md#1137-raft-snapshot-stream-수용-매핑) |

## 1. 언제 쓰는가

- Leader의 Raft adapter가 `MsgSnap` 하나를 직렬화했을 때 크기가 1,048,576 byte를 넘으면 그 메시지를 Raft 연결로 보내지 않는다.
- `snapshot-stream`이 **활성화되지 않았으면** 지금 동작을 유지한다. 메시지를 보내지 않고 `RawNode.ReportSnapshot(SnapshotFailure)`를 호출하고 `WARN raft snapshot exceeds peer frame limit; not sent`를 peer별 30초에 한 번 남긴다([Raft 멤버십 v2 §1.5](raft-membership-v2.md#raft-메시지-프레임과-크기-한도)).
- **활성화되었으면** 직렬화한 `MsgSnap`을 이 계약의 stream으로 보낸다. 1 MiB 이하 `MsgSnap`과 다른 Raft 메시지는 영향이 없다.
- 활성화 여부는 송신자의 committed active feature 집합으로 판단한다. 수신자는 활성화 여부와 관계없이 이 build가 stream을 지원하면 받는다. 수신자가 새 learner일 때 아직 복제 상태가 없어 활성화 여부를 알 수 없기 때문이다.

## 2. 연결과 인증

송신자가 대상 peer의 commit된 주소로 새 TLS 1.3 mTLS 연결을 연다. 일반 Raft 연결과 별개다. 기존 Raft·forwarding 연결, 대기열, 용량 계산을 사용하거나 바꾸지 않는다.

1. TLS handshake 직후, hello를 보내기 전에 송신자가 대상의 리프 인증서 SHA-256과 DNS SAN을 commit된 identity와 대조한다. 다르면 연결을 닫는다([Raft 멤버십 v2 §1.5](raft-membership-v2.md#15-피어-전송-v2-핸드셰이크와-프레이밍)).
2. 송신자가 hello frame을 보낸다. 형식은 Raft hello와 같다. 매직만 ASCII `NMQSNAP1` 8 byte다. 최대 payload는 1,024 byte다.
3. 수신자는 Raft 연결과 같은 검사를 한다. 송신자 ID가 commit된 peer여야 하고, TLS 리프 인증서 지문·DNS SAN·cluster ID·phase·operation ID·release 호환성이 commit된 identity와 일치해야 한다. 하나라도 틀리면 응답 없이 연결을 닫는다. Raft 연결의 연결 방향 규칙(작은 ID가 연결)은 적용하지 않는다. 이 연결은 일회성이고 송신자는 leader이기 때문이다.
4. 수신자가 자신의 hello frame으로 답한다. 송신자는 identity와 release 호환성을 다시 확인한다.

두 hello가 모두 통과하기 전에는 snapshot byte를 보내거나 받지 않는다. Staged 로컬 identity는 hello를 만들 수 없으므로 Raft 연결과 같이 stream에도 참여할 수 없다.

## 3. Frame 형식

모든 frame은 big-endian `uint32` payload 길이와 payload다. Payload는 1..1,048,576 byte다. 길이가 0이거나 한도를 넘으면 payload 메모리를 할당하기 전에 연결을 닫는다. Payload의 첫 byte는 frame 종류다. 정수는 모두 big-endian이다.

송신자 → 수신자:

| 종류 | 값 | 본문 | 의미 |
| --- | --- | --- | --- |
| `MANIFEST` | `0x01` | `snapshot_index u64`, `snapshot_term u64`, `total_bytes u64`, `sha256 [32]` | Transfer를 선언한다. 정확히 57 byte. |
| `CHUNK` | `0x02` | `offset u64`, `crc32c u32`, `data` | `data`는 1 byte 이상이다. `crc32c`는 Castagnoli 다항식이고 `data`만 덮는다. Payload 최대 1,048,576 byte라서 `data`는 최대 1,048,563 byte다. 송신자는 524,288 byte씩 보낸다. |
| `DONE` | `0x03` | 없음 | 모든 byte를 보냈다. 정확히 1 byte. |

수신자 → 송신자:

| 종류 | 값 | 본문 | 의미 |
| --- | --- | --- | --- |
| `ACCEPT` | `0x81` | `resume_offset u64` | `MANIFEST`를 받아들였다. 정확히 9 byte. 처음은 0이다. |
| `ACK` | `0x82` | `next_offset u64` | `CHUNK`를 받아 저장했다. 다음에 받을 offset이다. 정확히 9 byte. |
| `COMPLETE` | `0x83` | 없음 | 전체 digest를 검증하고 snapshot을 로컬 Raft에 넘겼다. 정확히 1 byte. |
| `ERROR` | `0x8F` | `code u8` | 실패한다. 정확히 2 byte. 보낸 뒤 연결을 닫는다. |

### 3.1 Transfer 내용

전송하는 byte는 `raftpb.Message{Type: MsgSnap}` 하나를 protobuf로 직렬화한 것이다. `total_bytes`는 그 길이다. 값은 1..67,109,888(`maxSnapshotPayload` 64 MiB + 1,024 byte)이다. `snapshot_index`와 `snapshot_term`은 그 메시지의 `Snapshot.Metadata.Index`와 `Term`이다. `sha256`은 전체 byte의 SHA-256이다.

### 3.2 순서

```text
송신자              수신자
 hello  →
        ←  hello
 MANIFEST →
        ←  ACCEPT(resume_offset)
 CHUNK(offset=resume_offset) →
        ←  ACK(next_offset)
 ... (lock-step: CHUNK 하나에 ACK 하나)
 DONE →
        ←  COMPLETE | ERROR
```

- 송신자는 `ACCEPT`를 받기 전에 `CHUNK`를 보내지 않는다. 다음 `CHUNK`는 이전 `ACK`를 받은 뒤에 보낸다.
- 수신자는 `offset`이 기대 offset과 정확히 같을 때만 받는다. `offset + len(data)`가 `total_bytes`를 넘거나 `CRC32C`가 다르면 거절한다.
- `DONE`은 모든 byte를 받은 뒤에만 유효하다. 수신자는 `DONE` 뒤에 SHA-256을 계산해 `MANIFEST`의 값과 비교한다. 같으면 §5의 hand-off를 하고 `COMPLETE`를 보낸다.
- `COMPLETE` 뒤에 송신자가 보낼 데이터는 없다. 수신자가 닫는다. 송신자는 close를 확인하고, 뒤에 데이터가 붙었으면 실패로 처리한다.

## 4. 오류 코드

| 코드 | 이름 | 조건 |
| --- | --- | --- |
| 1 | `BUSY` | 수신자가 이미 다른 transfer를 받고 있다. |
| 2 | `INVALID_MANIFEST` | `total_bytes`가 0이거나 한도를 넘는다. 길이나 필드가 틀렸다. |
| 3 | `OUT_OF_ORDER` | `CHUNK`의 `offset`이 기대 offset과 다르다. 허용하지 않는 순서의 frame이 왔다. |
| 4 | `CHECKSUM` | `CHUNK`의 CRC32C가 다르다. |
| 5 | `DIGEST` | 전체 SHA-256이 `MANIFEST`와 다르다. |
| 6 | `OVERFLOW` | `CHUNK`가 `total_bytes`를 넘는다. 또는 `DONE`이 모든 byte를 받기 전에 왔다. |
| 7 | `REJECTED` | 로컬 Raft가 snapshot을 거절했다(ErrBackpressure, 잘못된 메시지, index·term 불일치 등). |
| 8 | `UNSUPPORTED` | 수신 측에 stream handler가 없다. |

수신자는 `ERROR`를 보낸 뒤 연결을 닫는다. 알 수 없는 frame 종류, 잘린 frame, 뒤에 데이터가 붙은 frame은 응답 없이 연결을 닫는다. 코드 3·4·5·6은 수신 중이던 부분 상태를 버린다(§6).

## 5. Raft로의 hand-off

수신자는 digest를 확인한 뒤 `MsgSnap` byte를 로컬 Raft adapter의 stream 전용 Step 경로(`Node.StepSnapshotStream`)에 넘긴다. 이 경로는 일반 `Step`과 같은 검사(`From`·`To`·메시지 종류·storage 포화 확인)를 하고, 크기 한도만 `maxSnapshotPayload + 1,024`로 올린다. 이 한도는 `MsgSnap`에만 쓴다. Adapter는 해독한 `Snapshot.Metadata.Index`와 `Term`이 `MANIFEST`와 같은지 확인한다. 이 경로는 일반 Raft 연결의 `MsgSnap` budget 예외와 같은 예외를 쓴다. 로컬 Raft가 거절하면 `REJECTED`를 보낸다.

## 6. 자원 한도와 시간 제한

| 항목 | 값 |
| --- | --- |
| Frame payload | 1,048,576 byte |
| `total_bytes` | 1..67,109,888 byte |
| 수신 동시 transfer | 1개 (수신자 전체) |
| 송신 동시 transfer | 2개 (송신자 전체). 초과하면 `ErrPeerBackpressure`로 실패 |
| Peer당 송신 transfer | 1개 (송신 coordinator가 보장) |
| 수신 메모리 | 한 transfer의 `total_bytes`와 현재 frame까지. 첫 검증 통과 `CHUNK`에서 전체 저장 공간을 한 번 예약한다. `MANIFEST` 선언만으로 `total_bytes`를 미리 할당하지 않는다. |
| Frame당 I/O 제한 시간 | 5초 (`peerIOTimeout`) |
| `DONE` 뒤 `COMPLETE` 대기 | 30초 |
| Transfer 전체 제한 시간 | 2분 |
| 송신 시도 | 최대 3회 (연결 오류만 재시도) |
| 부분 상태 유지 | 연결 오류 뒤 30초. 같은 송신자의 같은 manifest만 이어 받는다. |

**재개.** 연결 오류로 끊어지면 수신자는 받은 byte를 최대 30초 보관한다. 송신자가 같은 송신자 ID, `snapshot_index`, `snapshot_term`, `total_bytes`, `sha256`의 `MANIFEST`를 다시 보내면 `ACCEPT.resume_offset`이 받은 byte 수다. 송신자는 그 offset부터 보낸다. 다르면 `resume_offset`은 0이고 이전 상태를 버린다. 코드 3·4·5·6 오류와 시간 초과 뒤에는 부분 상태를 버린다. 수신자는 부분 상태를 하나만 둔다. 같은 송신자의 새 manifest(다른 index 또는 term)가 오면 이전 상태를 버리고 새로 받는다. 이것이 "더 새로운 snapshot이 나타나면 이전 transfer를 취소한다"의 의미다.

**송신 측 취소.** Raft는 `SnapshotFailure`나 `SnapshotFinish`를 보고받기 전에는 같은 follower에게 새 `MsgSnap`을 만들지 않는다. 따라서 coordinator가 같은 peer에게 이미 transfer를 진행 중인데 새 `MsgSnap`을 받으면 새 것을 무시한다. 진행 중 transfer는 transport 종료, peer 제거, 전체 제한 시간 초과 때 취소하고 `SnapshotFailure`로 알린다.

## 7. Raft 보고

- Transfer가 `COMPLETE`로 끝나면 송신자는 `RawNode.ReportSnapshot(peer, SnapshotFinish)`를 호출한다.
- 어떤 이유로든 `COMPLETE`를 받지 못하고 끝나면 `ReportSnapshot(peer, SnapshotFailure)`를 호출한다. Raft는 그 peer를 probe 상태로 돌리고 다음 heartbeat 응답 뒤 다시 시도한다.
- 이 보고는 Ready loop 밖 goroutine에서 `Node`의 mutex를 잡고 한다. Ready loop는 stream 송신을 기다리지 않는다.

## 8. 호환성

| 상태 | 동작 |
| --- | --- |
| N, 활성화 전 | 1 MiB 초과 `MsgSnap`은 보내지 않고 실패로 알린다. Wire는 이전과 같다. |
| N, 활성화 뒤 | 1 MiB 초과 `MsgSnap`을 이 stream으로 보낸다. |
| N-1 수신자 | `NMQSNAP1` 매직을 모르는 build는 연결을 닫는다. 활성화는 모든 non-retired member가 `snapshot-stream`을 광고해야 commit되므로 활성화된 cluster에 N-1 member는 없다. |
| 활성화 뒤 rollback | 지원하지 않는다. 큰 snapshot을 받을 수 없다. 다른 feature와 같다. |
| 활성화 전에 stage한 새 member | 큰 snapshot을 받지 못한 learner는 build profile을 commit하지 못한다. 시험 중 활성화가 `409 feature_not_supported_by_all_members`로 거절됐다. 이 상태의 자동 복구 절차는 검증하지 않았다. 큰 snapshot이 예상되는 새 member는 활성화를 끝낸 뒤 stage해야 한다. 장애 중에는 ADR-0032의 모든 member READY 승인 조건을 충족하지 못할 수 있으므로 장애 전에 활성화해야 한다. |

Hello layout, Raft 연결 프로토콜 `2.0`, Raft frame 한도와 replicated-state 형식은 바뀌지 않는다.

## 9. 관측 신호

송신자와 수신자는 구조화 로그를 남긴다. 필드 이름과 메시지는 고정이다.

| 위치 | 수준 | 메시지 | 필드 |
| --- | --- | --- | --- |
| 송신 시작 | `INFO` | `raft snapshot stream started` | `peer_id`, `snapshot_index`, `total_bytes` |
| 송신 완료 | `INFO` | `raft snapshot stream completed` | `peer_id`, `snapshot_index`, `total_bytes`, `chunks` |
| 송신 실패 | `WARN` | `raft snapshot stream failed` | `peer_id`, `snapshot_index`, `reason` |
| 수신·Raft 인계 | `INFO` | `raft snapshot stream received` | `from_peer_id`, `snapshot_index`, `total_bytes` |

`reason`은 원시 I/O 오류 문자열이 아니라 `busy`, `invalid_manifest`, `out_of_order`, `checksum`, `digest`, `overflow`, `rejected`, `unsupported`, `timeout`, `canceled`, `connection`, `backpressure` 중 하나다. 한 transfer가 재시도로 여러 연결을 써도 `started`와 `completed`는 한 번씩 남긴다.

## 10. 시험

수용 매핑은 [Cluster §11.37](../specs/cluster-session-and-delivery.md#1137-raft-snapshot-stream-수용-매핑)이다.
