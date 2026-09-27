# ADR-0003: Durable ACK는 quorum WAL 영속화와 commit 뒤 반환한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Proposed |
| 제안일 | 2026-08-16 |
| 결정일 | 미정 |
| 결정자 | 미정 |
| 관련 요구사항 | MQTT-02, CLU-06~CLU-09, CLU-12, NFR-05, NFR-07~NFR-10 |
| 관련 스펙 | [MQTT protocol과 session](../specs/mqtt-protocol-and-session.md), [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md) |
| 관련 Architecture | [Cluster architecture](../architectures/cluster-architecture.md), [EKS on EC2 deployment](../architectures/eks-ec2-deployment.md) |
| 관련 결정 | [ADR-0002](ADR-0002-cluster-consensus-library.md) |
| 관련 Contract | Replicated state encoding v1의 Draft schema [`replicated-state-v1.proto`](../contracts/replicated-state-v1.proto)와 generated codec은 구현됐지만, 검토된 contract의 크기 제한·마이그레이션 규칙·릴리스 호환성은 미완료다. 이 ADR의 durable ACK 수용 기준은 별도로 검증한다. |
| 관련 해설 | [MQTT 전달 보장과 QoS·Session·Will protocol 흐름](../explainers/mqtt-delivery-guarantees-and-protocol-flows.md), [EKS 고가용성과 장애 도메인](../explainers/eks-ha-failure-domains.md) |

## 1. 결정

기준 production profile은 quorum member 3개, replication factor 3, write quorum 2를 사용한다.

NaruMQ는 QoS 1의 성공 PUBACK과 QoS 2의 성공 PUBREC을 다음 조건이 모두 충족된 뒤 반환한다.

1. Message와 복구에 필요한 handshake·delivery command가 replicated log에 기록된다.
2. 최소 write quorum 2개가 해당 log entry와 필요한 consensus state를 stable storage에 영속화한다.
3. Entry가 quorum commit되어 새 leader가 같은 command를 복구할 수 있다.
4. Apply 전 검증이 끝났고 command가 현재 epoch와 quota 정책에 유효하다.

Group commit과 batching은 허용한다. ACK를 먼저 반환하고 나중에 `fsync`하는 방식은 허용하지 않는다.

모든 성공 QoS 1·2 inbound publish에 이 경계를 적용한다. Matching durable subscriber가 있을 때만 durability를 적용하는 최적화는 사용하지 않는다. QoS 0은 이 결정의 보장 범위가 아니다.

## 2. 배경

PRD는 성공 PUBACK/PUBREC을 broker가 message 소유권을 인수한 경계로 정의한다. 단일 node 장애에서 RPO 0을 보장하려면 한 node의 page cache나 local WAL만으로는 부족하다.

Subscriber 존재 여부에 따라 ACK 의미가 달라지면 publisher가 같은 QoS에서 서로 다른 durability를 구분할 수 없다. 따라서 성공 ACK의 의미를 하나로 고정한다.

## 3. 결정 요인

1. 성공 ACK 뒤 단일 node를 잃어도 message를 복구해야 한다.
2. Publisher가 subscriber topology를 알지 않아도 ACK 의미가 같아야 한다.
3. Quorum 상실과 disk 오류를 성공으로 숨겨서는 안 된다.
4. Batching으로 throughput을 높이되 durability 의미를 바꾸면 안 된다.
5. 설정 가능한 replica·quorum 요구를 제공해야 한다.

## 4. 선택 근거

3 replica와 quorum 2는 기준 3-node cluster에서 한 node 장애를 허용하면서 두 독립 stable copy를 남긴다. Commit된 entry는 owner node가 사라져도 새 quorum에서 복구할 수 있다.

ACK 경계를 subscriber 유무와 분리하면 publish admission, monitoring과 client contract가 단순해진다. Offline delivery 여부는 subscription, session expiry, message expiry와 quota가 별도로 결정한다.

## 5. Profile과 설정

- `replicationFactor`와 `writeQuorum`은 배포 profile에서 명시한다.
- RPO 0을 선언하는 3-node production profile은 3/2보다 낮은 값을 허용하지 않는다.
- 더 높은 replica나 quorum은 허용하지만 N-1 capacity와 latency gate를 다시 통과해야 한다.
- 비기준 profile이 더 낮은 값을 사용하면 API·Dashboard·release artifact에 보장 축소를 표시해야 한다.
- 설정 변경은 quorum commit과 compatibility 검증을 거쳐야 한다.

WAL·snapshot의 storage target과 `fsync` 순서는 scoped [ADR-0009](ADR-0009-raft-production-state-profile.md)에 기록한다. 이 선택은 production storage 구현·crash 복구·quorum ACK 검증을 완료하지 않으며, 이 ADR은 `Proposed` 상태로 남는다.

## 6. 실패 동작

| 실패 | 외부 결과 |
| --- | --- |
| Quorum 상실 | 성공 PUBACK/PUBREC을 반환하지 않는다. Backpressure 뒤 protocol 오류 또는 연결 종료를 적용한다. |
| Replica disk full·I/O 오류 | Healthy write quorum이 없으면 durable ACK을 중단한다. |
| Commit timeout | 결과를 성공으로 추정하지 않는다. Client 재전송이 같은 command로 수렴해야 한다. |
| Owner epoch 변경 | 이전 epoch command를 거절한다. |
| Apply validation 실패 | Commit 전에 검증 가능한 오류는 proposal 전에 거절한다. Commit 뒤 deterministic apply 오류는 node를 unhealthy로 만들고 ACK하지 않는다. |

## 7. 대안

| 대안 | 장점 | 제외 이유 |
| --- | --- | --- |
| Leader local `fsync` 뒤 ACK | 낮은 latency | Leader와 volume 장애에서 ACK state를 잃을 수 있다. |
| Quorum memory replication 뒤 ACK | Disk 비용 감소 | 동시 재시작·전원 장애에서 RPO 0을 보장하지 못한다. |
| Durable subscriber가 있을 때만 quorum | 불필요한 write 감소 | Publisher가 ACK durability를 구분할 수 없고 route race가 생긴다. |
| 모든 3 replica `fsync` 뒤 ACK | 더 많은 copy | 한 느린 replica가 전체 availability와 tail latency를 결정한다. |

## 8. 결과

- 성공 ACK의 의미가 모든 tenant와 route에서 일관된다.
- Disk와 inter-node network가 QoS 1·2 latency의 일부가 된다.
- Quorum loss에서 availability보다 durability를 우선한다.
- Group commit, bounded batch와 admission control이 필수다.
- BP-03이 이 결정의 성능·durability release gate가 된다.

## 9. 검증

1. ACK 직후 각 replica를 차례로 kill해 message gap이 0인지 확인한다.
2. WAL write, `fsync`, commit 전후 모든 지점에 crash를 주입한다.
3. Follower disk full·slow I/O와 partition에서 잘못된 ACK이 없는지 검증한다.
4. 같은 Packet Identifier 재전송이 중복 durable command를 만들지 않는지 검증한다.
5. BP-03에서 group commit 크기별 p99·p99.9와 disk 비용을 측정한다.

이 ADR-0003 §9 기준에 대한 후속 evidence는 [ADR-0015 §7](ADR-0015-cluster-durable-qos-delivery.md#7-검증)과 [Cluster §11.11](../specs/cluster-session-and-delivery.md#1111-qos-12-durable-delivery-수용-매핑)에 있다. Process acceptance는 QoS 1 PUBACK 뒤 publisher node process를 kill하고 persistent subscriber reconnect 뒤 committed message를 받는지, quorum loss 중 QoS 1 PUBACK/QoS 2 PUBREC 성공을 보내지 않는지 확인했다. 이는 일부 ACK/message-gap 시나리오를 검증하지만, 각 replica kill, WAL write/`fsync`/commit 전후 crash injection, follower disk-full/slow-I/O ACK boundary 또는 BP-03을 확인하지 않는다. ADR-0003 §9 gate는 이 나머지 항목이 끝날 때까지 미완료다.

## 10. 재검토 조건

- Phase 0/2에서 production SLO를 충족하지 못한다.
- RPO 목표가 바뀌거나 storage topology가 독립 failure domain을 제공하지 못한다.
- Client가 durability level을 명시적으로 선택하는 새 제품 요구가 승인된다.
