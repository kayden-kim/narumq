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
| 관련 결정 | [ADR-0002](ADR-0002-cluster-consensus-library.md), [ADR-0015](ADR-0015-cluster-durable-qos-delivery.md), [ADR-0019](ADR-0019-untargeted-durable-publish-quorum-barrier.md) |
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

성공 QoS 1·2 ACK가 의존하는 publish 상태(retained mutation, publisher QoS 2 inbound 상태, durable target이 있는 delivery)에 이 경계를 적용한다. Durable target 후보가 없는 QoS 1 publish와 QoS≥1 Will은 저장할 delivery 상태가 없으므로 ADR-0019에 따라 proposal 없는 fresh quorum read barrier 뒤 ACK한다. Quorum 확인은 모든 성공 ACK에 남는다. QoS 0은 범위 밖이다.

## 2. 배경

PRD에서 message 또는 protocol state를 저장하는 경로의 성공 PUBACK/PUBREC은 broker가 해당 durable state의 소유권을 인수한 경계다. Target 없는 QoS 1 client PUBLISH의 성공 PUBACK은 ADR-0019에 따라 payload 소유권을 약속하지 않고 quorum barrier만 확인한다. QoS 2 client PUBLISH의 inbound RECEIVE는 target 유무와 무관하게 PUBREC 전에 저장한다.

성공 ACK에는 subscriber 유무와 무관하게 quorum 확인이 필요하다. 다만 저장할 delivery state가 없으면 message payload의 durable ownership을 약속하지 않으므로 PRD는 target 유무에 따른 제품 보장을 구분한다.

## 3. 결정 요인

1. 성공 ACK 뒤 단일 node를 잃어도 message를 복구해야 한다.
2. Publisher가 subscriber topology를 알지 않아도 ACK 의미가 같아야 한다.
3. Quorum 상실과 disk 오류를 성공으로 숨겨서는 안 된다.
4. Batching으로 throughput을 높이되 durability 의미를 바꾸면 안 된다.
5. 설정 가능한 replica·quorum 요구를 제공해야 한다.

## 4. 선택 근거

3 replica와 quorum 2는 기준 3-node cluster에서 한 node 장애를 허용하면서 두 독립 stable copy를 남긴다. Commit된 entry는 owner node가 사라져도 새 quorum에서 복구할 수 있다.

ACK의 quorum 확인을 subscriber 유무와 분리하고 저장 대상만 target 유무로 정한다. Offline delivery 여부는 subscription, session expiry, message expiry와 quota가 별도로 결정한다.

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
| Durable subscriber가 있을 때만 delivery write | 불필요한 write 감소 | Quorum 확인 자체를 생략하면 minority node가 ACK할 수 있다. 채택안은 확인을 유지하고 target 없는 write만 생략한다 ([ADR-0019](ADR-0019-untargeted-durable-publish-quorum-barrier.md)). |
| 모든 3 replica `fsync` 뒤 ACK | 더 많은 copy | 한 느린 replica가 전체 availability와 tail latency를 결정한다. |

## 8. 결과

- 모든 성공 QoS 1·2 ACK는 quorum 확인 뒤 반환한다. Durable target 유무에 따라 payload 저장 보장은 달라진다.
- Disk와 inter-node network가 QoS 1·2 latency의 일부가 된다.
- Quorum loss에서 availability보다 durability를 우선한다.
- Group commit, bounded batch와 admission control이 필수다.
- 실제 저장 실행은 [ADR-0020](ADR-0020-asynchronous-raft-wal-group-commit.md)의 단일 async WAL worker를 따른다. Entry 복제·native storage Responses는 필요한 local 저장 완료 뒤, application 완료는 projection 적용 뒤 확인한다. Fresh proof와 MQTT ACK 경계는 유지한다. 실행 증거는 [Cluster §11.18](../specs/cluster-session-and-delivery.md#1118-비동기-walapplication-완료-경계-수용-매핑)에 있다.
- BP-03이 이 결정의 성능·durability release gate가 된다.

## 9. 검증

1. ACK 직후 각 replica를 차례로 kill해 message gap이 0인지 확인한다.
2. WAL write, `fsync`, commit 전후 모든 지점에 crash를 주입한다.
3. Follower disk full·slow I/O·failed fsync와 partition에서 잘못된 ACK이 없는지 검증한다.
4. 같은 Packet Identifier 재전송이 중복 durable command를 만들지 않는지 검증하고, `ErrRetryableUnavailable` 시 Packet ID 보존과 timer 재시도를 검증한다.
5. BP-03에서 group commit 크기별 p99·p99.9와 disk 비용을 측정한다.
6. Held WAL fsync 중 follower torn cut과 simultaneous three-node power cut 뒤 ACKed delivery·session recovery를 확인한다. Cluster-cut acceptance는 `client.publish`가 PUBLISH packet을 성공적으로 쓴 뒤 in-flight barrier를 signal하고 `awaitAck` 전에 publisher를 보류한다. Cut 때문에 PUBACK 관찰이 중단된 그 QoS 1 결과만 `unknown`이다. Known timeout/closed outcomes는 no-ACK으로 기록하되 unknown으로 오분류하지 않는다. Docker/FUSE simulation은 physical power-loss evidence가 아니다.

추가 process evidence는 [Cluster §11.13](../specs/cluster-session-and-delivery.md#1113-adr-0003-durable-ack-fault-수용-매핑)에 기록했다. `NARUMQ_DURABLE_ACK_FAULTS=1 go test ./tests/integration -run '^TestDynamicClusterDurableAck(ReplicaKill|CrashPoint|Partition)ProcessAcceptance$' -count=1 -v -timeout 30m`가 PASS했다: 세 replica 각각에서 QoS 1/2 ACK 직후 kill한 여섯 조합은 recovery 뒤 count=1, WAL/commit crash matrix의 20개 조합과 leader/follower partition 두 시나리오도 통과했다. WAL-before-write/sync/after-sync leader failpoint에서 QoS 1 성공 ACK는 없었고 QoS 2 packet은 recovery 뒤 한 번 전달됐다. Commit-before/after-apply는 leader/follower, QoS 1/2 모두 ACK 뒤 한 번 전달됐다. Partition에서는 isolated node가 성공 ACK를 보내지 않았고 majority는 계속 ACK했다.

`storage-fault-durable-20260927d`의 세-scenario Docker durable ACK gate는 PASS했다. Final six-scenario gate `storage-power-loss-20260930g`도 PASS했다: sync-error reopen은 exit code 1로 fail-closed됐고 ACKed QoS 1/2 일곱 건이 recovery 뒤 전달됐다. 실제 `node/wal.bin`에서 held WAL fsync/torn cut은 16 unsynced bytes 중 8 bytes를 남겼다. Simultaneous three-node cut 뒤 index 39→42 수렴, `SessionPresent=true`, ACKed QoS 1/2 여섯 건의 missing/duplicate는 0이었다. During-cut publisher는 네 QoS 1 ACK와 PUBLISH write 뒤 await 전 barrier에서 hold된 `qos1-05`의 unknown PUBACK outcome을 기록했다. Unknown evidence에는 `qos1-05`만 포함하며, known timeout/closed 결과와 구분한다. 상세 evidence는 [Cluster §11.6·§11.13](../specs/cluster-session-and-delivery.md#116-storage-fault-chaos-mapping)에 연결한다. 이는 Docker/FUSE simulation이며 physical power-loss 보장은 아니다.

Process exit는 실제 전원 손실을 모사하지 않는다. 특히 WAL-after-sync는 `File.Sync` 성공 뒤의 process crash일 뿐 device/page-cache power-loss proof가 아니다. Docker/FUSE cut도 rollback journal을 적용한 local simulation이며 Linux/EBS 또는 실제 physical power-loss를 대체하지 않는다.

고정 profile BP-03 local compare `bp03-20260930T061549.390008900Z-28772`, `bp03-20260930T063854.898438300Z-34520`는 모두 NaruMQ workload invalid로 끝났다. Node 3에서 각각 `dispatch MQTT PUBLISH: quorum unavailable`, `dispatch MQTT PUBREL: quorum unavailable`가 관측되어 유효한 ACK/WAL 수치를 얻지 못했다. 추가 고정 smoke 5회 suite `bp03-suite-20260930T064519.153992500Z-35844`도 완료 반복 5회 중 유효 반복 0회로 invalid였다. NaruMQ가 5회 모두 overload 및 workload invariant 실패로 invalid했고, EMQX도 1회차 workload invariant 실패로 suite aggregate metric이 생성되지 않았다. 이 실행들은 §9-5 group-commit 크기별 p99/p99.9와 disk 비용의 증거가 아니다. 유효한 BP-03 측정과 Linux/EBS·physical power-loss·production volume gate가 남아 있으므로 ADR-0003은 `Proposed` 상태이며 §9 전체를 완료로 표시하지 않는다.
추가 BP-03 compare `bp03-20260930T123200.653059500Z-32492`도 NaruMQ workload `valid=false`로 종료됐고 EMQX만 valid였다. NaruMQ step 2는 overload와 QoS 1/2 delivery gaps를 기록했으며 q2-02 TCP reset으로 중단됐다. 완주한 step 1의 per-node fsync occupancy는 40.56%/41.46%/42.01%, `meanEntriesPerSync`는 0.505802/0.505003/0.505535였고 step 2 WAL end scrape는 없다. 시작 시각의 세 `read_barrier` deadline warning과 reset 사이 인과는 확인되지 않았다. Conditional suite는 실행하지 않았고 이 diagnostic run은 §9-5의 유효한 성능·durability evidence가 아니므로 ADR-0003은 `Proposed`로 유지한다([성능 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)).
추가 compare `bp03-20260930T145505.056290600Z-31628`과 이후 진단 run `bp03-20260930T150424.465458500Z-37680`은 `ControlQueueCapacity: 256`, `deliveryqueue: 256`, `deliveryRetryInterval: 100ms`, `writeDeliveries` 내 control packet 플러시 적용 뒤 실행했다. `q2-02` 소켓 리셋이 해소되어 워크로드 전체를 완주했고, Step 1(gap 0), Step 2(480/480 전송·전달 완료, gap 0)와 Offline 구간(64/64 전달, sessionPresent 4/4)은 통과했다. 그러나 Follower node 3에 연결된 `q2-01`의 ACK 지연(avg 447.1ms)으로 Step 2 max schedule lag 3.336s(`generatorOverload=true`)가 발생했고, Step 3는 `bp03-sub-q2-02` 포화 28건(QoS 2 gap 28)과 max schedule lag 22.164s로 인해 NaruMQ workload는 invalid였다. 복구 불변식을 위반하는 commit-only Ready fsync 생략 시도 및 PacketID 경합이 있는 락 분리 실험은 모두 안전하게 되돌렸으며, proposal 전 fresh read barrier는 유지한다. Conditional suite는 실행하지 않았으며 ADR-0003은 계속 `Proposed` 상태다([성능 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)).

2026-10-01 최종 compare `bp03-20261001T133103.344202600Z-10712`도 NaruMQ invalid였다. Step 1·2의 QoS 1/2 전송·ACK·전달은 일치했지만 Step 3 warmup의 peer inbound queue 포화, durable proposal/read-barrier deadline과 subscriber EOF로 중단됐다. Conditional 5회 suite는 승인된 invalid 분기에 따라 실행하지 않았다. 완료된 두 WAL window의 occupancy·entries-per-sync는 진단 값이며 §9-5 수용 근거가 아니다. 반면 변경 후 replica kill·20 crash point·partition process acceptance, Docker/FUSE `storage-batch-20261001` 여섯 scenario, 최종 `GOMAXPROCS=4 go run ./tools/verify local`은 PASS했다. 상세 실행과 한계는 [성능 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)에 기록했다. Commit-only HardState sync와 command별 fresh proof를 유지하며, ADR-0003은 `Proposed`, BP-03과 production storage gate는 미완료다.

2026-10-02 계측·proposal batching 뒤 두 BP-03 compare도 NaruMQ invalid였다. Step 2의 Ready 시간에서 WAL fsync가 96.32–96.73%를 차지했지만 batching run의 Step 3은 QoS 1 gap 91, QoS 2 gap 93으로 중단됐다. 이는 병목 진단의 부분 관찰이며 §9-5 성능 수용 근거가 아니다. Command별 fresh proof·commit-only HardState sync·quorum-before-ACK을 유지했고, replica kill·20개 WAL/commit crash·leader/follower partition process acceptance는 PASS(159.235s)했다. Inbound 포화의 연결 종료 정책 변경은 미승인 후보로 분리했다. 상세 artifact와 검증은 [성능 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)에 연결하며 ADR-0003은 `Proposed`, BP-03 수용은 미완료로 유지한다.

앞선 두 compare 이후 사용자가 `제한된 수신 대기 채택`을 명시적으로 선택해 [v2 계약](../contracts/raft-membership-v2.md)의 dynamic Raft inbound 범위만 채택했다. 기존 ACK·WAL 경계와 outbound 거절 정책은 유지한다. 첫 후속 compare `bp03-20261002T021302.960231500Z-30036`도 Step 3 gap 15/17과 deadline·EOF로 invalid였으므로 BP-03 성능 수용은 계속 미완료다. 후속 실행과 안전성 증거는 성능 스펙에 구분해 기록한다.

Fresh ReadIndex 발행 제어·origin별 예산 분할과 runner의 persistent QoS 2 수신 상태 보존 뒤 compare `bp03-20261002T133437.979734800Z-36656`는 Step 1·2·3와 offline의 전송·ACK·전달 일치, gap·duplicate·protocol error 0, 세션 4/4로 완주했다. 그러나 Step 3 최대 schedule lag 3.809644755s로 generator overload가 기록되어 NaruMQ workload는 invalid였고 조건부 5회 suite는 미실행이다. 최신 matched-target ACK fault process suite와 Docker/FUSE `storage-readcredit-20261002` 여섯 scenario는 PASS했다. 새 scheduling은 command별 fresh proof·commit-only HardState sync·quorum-before-ACK을 바꾸지 않는다. 실제 수치·회귀 및 완료 gate는 [성능 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)에 연결한다. 이 부분 개선은 §9-5 또는 ADR-0003 전체 수용 완료가 아니며 상태는 `Proposed`로 유지한다.

2026-10-03 async WAL·release-profile wake 보완 뒤 고정 compare `bp03-20261002T193345.142241000Z-38956`와 독립 suite `bp03-suite-20261002T193711.351279300Z-4364`는 양쪽 valid·5/5 성공이었다. Local smoke의 ACK/WAL 수치와 중앙값·MAD는 [성능 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)에 기록했다. 이전 4/5 invalid suite에서 snapshot restore → release gate 무효화 → PUBLISH 거절 → EOF를 진단했고, 재확인 wake만 앞당겨 기존 fail-closed·fresh proof를 유지한다. 최종 5회에는 restore 표본이 없으므로 모든 EOF 해소를 입증하지 않는다. Group 크기별 production p99/p99.9·disk 비용과 Linux/EBS·physical power-loss 검증을 대체하지 않으며 ADR 상태는 `Proposed`다.

## 10. 재검토 조건

- Phase 0/2에서 production SLO를 충족하지 못한다.
- RPO 목표가 바뀌거나 storage topology가 독립 failure domain을 제공하지 못한다.
- Client가 durability level을 명시적으로 선택하는 새 제품 요구가 승인된다.
