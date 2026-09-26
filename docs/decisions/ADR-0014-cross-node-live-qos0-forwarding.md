# ADR-0014: Dynamic cluster의 cross-node live QoS 0 forwarding

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (cluster live QoS 0 forwarding only; QoS 1/2·durable ACK 제외) |
| 결정일 | 2026-09-27 |
| 결정자 | NaruMQ 프로젝트 (승인된 작업 계획) |
| 관련 요구사항 | [PRD CLU-03](../requirements/PRD.md#92-클러스터와-세션) |
| 관련 스펙·경계 계약 | [Cluster §3.1·§11.10](../specs/cluster-session-and-delivery.md), [Raft membership v2 §1.5](../contracts/raft-membership-v2.md#15-peer-transport-v2-handshake-and-framing) |
| 관련 결정 | [ADR-0008](ADR-0008-raft-peer-transport.md), [ADR-0013](ADR-0013-offline-queue-replication-and-cluster-backup-v3.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

1. **전달 경로:** `-cluster-config` mode에서 ingress node는 committed route projection으로 각 delivery candidate의 유효 QoS, Retain과 shared-subscription 선택을 계산한다. Detached session은 기존 Raft offline ENQUEUE 경로를 먼저 사용한다. 그 외 candidate의 committed owner가 원격 node이면 ingress는 한 건의 QoS 0 delivery를 기존 v2 peer listener의 전용 `NMQFWRD2` stream으로 owner에게 보낸다. Local owner 또는 ownership row가 없는 candidate는 node-local delivery queue를 사용한다.
2. **Ownership 판정:** Ingress는 local committed state의 ownership을 읽으며 message마다 Raft read barrier를 수행하지 않는다. Receiver는 자기 local committed state가 해당 session의 owner임을 확인하고 local delivery queue에 등록된 session으로만 enqueue한다. Ownership이 local이 아니거나 session이 등록되지 않았으면 조용히 버린다.
3. **전달 보장:** At-most-once이며 ACK와 자동 retry가 없다. Ingress에서 peer outbound queue에 frame을 넣은 시점을 성공으로 본다. 연결 손실, queue full, ownership race 또는 receiver queue full에서 message가 유실될 수 있다. FIFO 순서는 같은 `(ingress node, owner node)` 쌍 안에서만 보장한다. Will publish도 같은 경로를 사용한다.
4. **격리된 한도:** 각 방향·peer별 forwarding queue는 최대 1,024 frame 및 framed bytes 8 MiB다. Raft queue는 기존 peer별 32 frame 및 4 MiB 한도와 full 시 reset 동작을 유지한다. Forward queue full은 Raft stream을 reset하지 않는다. Receiver delivery queue가 full이면 local delivery saturation과 같이 subscriber connection을 닫는다.
5. **Protocol compatibility:** Cluster protocol은 `2.0`으로 유지한다. `NMQRAFT2` hello와 Raft frame bytes는 바뀌지 않으며, `NMQFWRD2`는 같은 TLS 1.3 mTLS listener의 additive stream이다. 알 수 없는 magic을 지원하지 않는 node는 연결을 닫는다. 현재 exact-release gate는 mixed-release runtime을 지원하지 않는다.
6. **Scope:** Cluster ingress에서 QoS 1/2는 기존처럼 거절한다. Forwarded payload는 Raft log, snapshot, backup 또는 offline queue에 기록되지 않는다.

## 2. 배경

Dynamic cluster의 committed persistent subscription projection은 cluster-wide route matching을 제공하지만, live delivery queue는 node-local이다. Publisher가 node A에 연결되고 matching persistent subscriber가 node B에 연결된 경우 A의 queue는 B 소유 session을 등록하지 않아 delivery를 enqueue할 수 없다. PRD CLU-03은 서로 다른 node의 publisher와 subscriber 사이 delivery를 요구한다. Detached session의 QoS 0 message는 이미 [ADR-0013](ADR-0013-offline-queue-replication-and-cluster-backup-v3.md)에서 Raft ENQUEUE로 처리하므로, 이 결정은 connected live session 경로만 추가한다.

## 3. 결정 요인

1. CLU-03의 cluster-wide subscription matching과 cross-node delivery를 구현한다.
2. Connected QoS 0 message마다 quorum write를 추가하지 않는다.
3. Raft의 현재 wire bytes, peer queue 한도와 reset 정책을 보존한다.
4. Owner가 아닌 node에서 delivery queue 접근을 금지하고 receiver에서 committed ownership을 재확인한다.
5. Bounded queue와 at-most-once 의미를 명시하고 QoS 1/2 durable ACK 범위와 분리한다.

## 4. 선택 근거

전용 stream은 같은 인증 listener와 committed peer identity를 재사용하면서 Raft byte framing을 변경하지 않는다. 별도 lane의 queue와 backpressure 정책은 publish burst가 Raft message queue를 소진하거나 Raft connection을 초기화하지 않게 한다. Forwarding은 session별 추가 consensus commit을 만들지 않으므로 QoS 0 live path의 quorum 비용을 피한다.

Detached check를 remote forwarding보다 먼저 하는 이유는 detached session이 기존 owner의 ownership row를 계속 가질 수 있기 때문이다. 그 순서를 뒤집으면 offline ENQUEUE 대신 이미 연결이 끊긴 owner로 live frame을 보내 message를 잃는다. Receiver ownership check는 route projection 지연이나 takeover race가 잘못된 node의 local queue에 message를 넣는 것을 막는다. Check는 local committed view를 사용하므로 committed ownership 변경과 다른 node의 observation 사이에는 at-most-once loss window가 남는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| QoS 0 publish마다 Raft commit | Remote owner와 durable ordering 경계를 공유한다. | Message마다 quorum write와 storage 비용이 생기며 QoS 0 live path가 바뀐다. | QoS 0 live delivery에 요구되지 않고 승인된 scope보다 넓다. |
| Raft connection에 frame-kind byte를 multiplex한다. | Peer당 connection 하나로 두 message type을 전달한다. | `NMQRAFT2` frame bytes가 바뀌고 cluster protocol `2.1`이 필요하다. Protocol `2.0`은 `internal/application/membership/validation.go`, `service.go`, `cmd/narumq/cluster_config.go`와 committed member record에서 고정한다. | Existing Raft byte compatibility와 exact `2.0` gate를 보존한다. |
| 별도 listener port를 추가한다. | Raft와 delivery 연결의 lifecycle을 독립시킨다. | Configuration, listener lifecycle과 certificate/port 운영 표면이 추가된다. | 기존 authenticated v2 peer listener의 additive stream이 더 작은 변경이다. |

## 6. 결과

### 긍정적 결과

- READY node 사이 live QoS 0 route delivery가 owner node의 local session queue로 도착한다.
- Publisher와 subscriber가 다른 node에 있어도 committed subscription projection으로 전달한다.
- Raft queue와 bytes, Raft frame format, protocol `2.0`, snapshot·backup·replicated state를 변경하지 않는다.

### 부정적 결과와 비용

- Forward queue admission 이후 peer disconnect, receiver crash, ownership race 또는 queue saturation이 발생하면 delivery가 유실될 수 있다.
- Retry와 receiver acknowledgement가 없으므로 at-most-once 외의 성공 보장은 제공하지 않는다.
- FIFO는 ingress-owner node pair에 한정되며 여러 ingress node의 global ordering을 보장하지 않는다.
- Shared-subscription fallback은 forwarding queue saturation/unavailability가 ingress에서 관찰될 때만 다음 candidate로 진행한다. 이미 admitted frame의 수신 결과는 관찰하지 못한다.
- QoS 1/2, durable ACK, stale-epoch fencing barrier와 cross-node transaction은 이 결정에 포함되지 않는다.

### 후속 작업

- [ ] QoS 1/2 route delivery와 durable ACK barrier를 ADR-0003 범위에서 결정·검증한다.
- [ ] Ownership race 및 stale-epoch end-to-end acceptance를 수행한다.
- [ ] Quorum loss에서 durable publish ACK가 성공하지 않는지 검증한다.
- [ ] Release N/N-1 transport compatibility는 별도 승인된 compatibility matrix로 확인한다.

## 7. 검증

| 결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Isolated forwarding lane | Dynamic peer transport unit tests | Forward queue는 독립된 frame/byte quota를 적용하고 Raft queue/full 동작에 영향을 주지 않는다. | 구현·시험 후 Cluster §11.10에 기록 |
| Cross-node live QoS 0 routing | Three-process MQTT acceptance | 원격 subscriber가 FIFO 순서로 한 번씩 수신하고 local+remote fan-out 및 owner reconnect 뒤 이전 owner node로의 전달을 확인한다. | 미실행: 구현 후 Cluster §11.10에 기록 |
| QoS 1/2, stale ownership race, quorum-loss durable ACK | Cluster acceptance | 각 별도 delivery/ownership/ACK 기준을 통과한다. | 미실행·이 결정 범위 밖; 후속 작업으로 유지 |
| Production storage와 release N/N-1 | Linux/EBS 및 release compatibility acceptance | Production failure 경계 및 N/N-1 양방향 조합을 검증한다. | 미실행·별도 gate |

## 8. 재검토 조건

- At-most-once QoS 0 loss window가 CLU-03 또는 client contract와 맞지 않는다는 근거가 생기면 검토한다.
- QoS 1/2, receiver acknowledgement/retry 또는 durable cross-node delivery를 요구하면 별도 state/ACK 결정과 함께 재검토한다.
- Forwarding을 Raft connection에 multiplex하거나 release 간 혼합 운영을 허용하려면 protocol version과 compatibility contract를 새로 승인한다.
- Shared subscription, stale ownership 또는 ordering 요구가 현재 후보 순서·at-most-once semantics를 바꾸면 spec과 이 ADR을 함께 갱신한다.
