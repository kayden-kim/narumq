# EMQX와 NaruMQ 주요 기능 대조표

EMQX Self-Hosted Enterprise는 MQTT broker 외에도 rule engine, data integration, multi-protocol gateway와 운영 도구를 제공하는 넓은 플랫폼이다. 현재 NaruMQ는 MQTT core, tenant-scoped Raft cluster와 제한된 QoS 1/2 durable delivery scope를 구현했으며 해당 범위의 acceptance도 확인했다. 하지만 외부 연동, management dashboard, production storage durability와 넓은 lifecycle·compatibility 보장은 동등한 범위로 완료되지 않았다. 이 문서는 확인된 구현, 제한된 동작, 계획과 근거 부족을 분리한다.

| 항목 | 값 |
| --- | --- |
| 기준 독자 | EMQX 운영자와 NaruMQ 기능·이전 범위를 비교하는 개발자·운영자 |
| 먼저 읽을 문서 | [PRD의 실제 EMQX 비교 기준](../requirements/PRD.md), [MQTT session과 delivery spec](../specs/mqtt-protocol-and-session.md) |
| 관련 기준 문서 | [PRD](../requirements/PRD.md), [Cluster spec](../specs/cluster-session-and-delivery.md), [Tenant·security spec](../specs/tenancy-and-security.md), [Management API spec](../specs/management-api-and-observability.md) |
| 대상 버전·확인일 | 일반 기능 대조: EMQX 공식 `latest` rolling docs·NaruMQ 조사 당시 working tree(2026-09-27). 내구성·ACK 세부 비교: EMQX 5.8.8 Enterprise·NaruMQ 기준 문서와 실행 evidence(2026-10-01) |

## 비교 범위와 판정 기준

- **EMQX 기준:** 공식 기능 비교표의 **Self-Hosted (Enterprise)** 열을 주 기준으로 사용한다. 그 표에는 EMQX Cloud Serverless와 Dedicated Flex도 별도 열로 있으므로 self-hosted 기능을 Cloud 전체에 일반화하지 않는다. EMQX 5.9부터 BSL 1.1이 적용되고, 문서상 단일 node Community License와 상용 cluster license 조건이 있으므로 사용 version·license를 별도로 고정해야 한다. [E1][E16]
- **NaruMQ 기준:** 현재 상태는 code와 대응 수용 시험의 evidence로 판정한다. MQTT·cluster spec은 Draft이고 cluster architecture는 Target/Draft다. Draft/Target의 normative statement만으로 구현 완료 처리하지 않으며, cluster 기능은 수용 시험이 확인한 범위(in-process, process-level, production)를 구분한다. [N1][N2][N5][N6]
- **EMQX 비교 대상의 적용 범위:** 2026-10-01 사용자 요청으로 BP-03 local smoke는 EMQX 5.8.8 Enterprise의 replicated Durable Sessions와 JWT 인증으로 전환했다. 설정·실행 증거와 남은 ACK/fsync·보안 차이는 [성능 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)를 따른다. BP-01/BP-05의 역사적 OSS comparator와 장기 production benchmark version·edition의 미결정 상태는 바꾸지 않는다. 이 문서는 제품 검증이나 전환 결정을 대신하지 않는다. [N5]

**EMQX version 차이:** `latest`는 rolling docs다. Durable Sessions는 v5.7부터, Namespace는 v5.9부터 제공되는 등 기능별 최소 version이 다르다. 이 표는 모든 기능이 하나의 EMQX version에 동시에 있다는 뜻이 아니므로 실제 기준 release에서 확인한다. [E3][E11]

| 표시 | 의미 |
| --- | --- |
| **확인** | 현재 code와 대응 시험 evidence가 연결되어 있다. 주장은 해당 시험의 범위·profile까지만 적용한다. |
| **부분** | 시험된 subset은 있으나 더 넓은 delivery 보장, cluster contract 또는 production gate는 확인되지 않았다. |
| **계획** | PRD/설계에 있지만 active implementation·acceptance evidence가 확인되지 않았다. |
| **미확인** | 조사한 기준 문서로는 지원 여부를 판단할 근거가 없다. 비지원과 같은 뜻이 아니다. |
| **비목표** | NaruMQ PRD에서 명시적으로 초기 범위 밖으로 정했다. |

## 한눈에 보는 차이

| 비교 축 | EMQX Self-Hosted Enterprise | NaruMQ 현재 evidence | 의미 |
| --- | --- | --- | --- |
| 제품 범위 | MQTT broker, 통합 처리, schema, 다중 프로토콜 gateway와 관리 기능을 포함한다. [E1][E13] | MQTT broker 중심. Kafka·DB connector 묶음, 범용 rule engine과 비-MQTT gateway는 초기 제품 비목표다. [N5] | EMQX는 application integration platform까지 제공한다. NaruMQ는 현재 그 기능을 직접 대체하지 않는다. |
| MQTT 및 client compatibility | MQTT 3.x·5.0, TCP/TLS, WS/WSS와 QUIC 등 문서화된 연결 경로가 있다. [E1][E6][E7] | MQTT 3.1.1/5.0, TCP/TLS 구현. MQTT 3.1.0은 거절하며 WebSocket/WSS는 PRD의 선택 기능이다. [N1][N5] | 오래된 client protocol과 browser/QUIC transport 사용 여부를 먼저 inventory해야 한다. |
| Session 내구성 | 일반 session은 node memory, Durable Sessions는 설정 시 disk-backed storage와 cluster replication을 사용한다. [E3] | Standalone session은 process-local. Dynamic cluster의 retained/offline QoS 0 replay 및 QoS 1/2 durable detached queue, reconnect, inflight takeover와 backup v3 restore는 제한된 process acceptance에서 확인됐다. Production power-loss와 release N/N-1은 별도 미검증 gate다. [N1][N2] | QoS 1/2 process acceptance를 production storage 또는 모든 lifecycle 조합의 동등성으로 확대 해석하지 않는다. |
| 성공 ACK의 내구성 의미 | BP-03의 5.8.8은 메시지 저장을 비동기로 요청하므로 PUBACK가 저장 buffer 처리보다 먼저 나올 수 있다. [E17][E18][E20] | ADR-0003이 명세한 경계는 과반수 WAL 영속화·commit 후 PUBACK/PUBREC이다. ADR은 `Proposed`이며 production 보장 수용은 미완료다. [N9] | 같은 3 replicas나 QoS label만으로 ACK 보장이 동등해지지 않는다. 아래 내구성 세부 비교를 참고한다. |
| 운영 기능 | Dashboard, management API, rule/connector 관리, cluster rebalancing·node evacuation 등 기능이 있다. [E1][E13][E14] | 13개 GET query profile, health probe와 Prometheus/OpenMetrics가 구현되어 있다. Dashboard와 mutation/drain API는 deferred다. [N4][N5] | UI 및 operator workflow를 API·외부 도구로 대체할 수 있는지 확인해야 한다. |
| 생산 준비 evidence | 기능과 배포 지원은 version/edition/license에 따라 달라진다. [E1][E16] | EKS EC2 3-AZ가 목표 profile이다. Linux/EBS power-loss, volume loss와 RPO/RTO gate는 미실행으로 남았다. [N5][N6] | 문서상 기능 유무만으로 동일한 production guarantee를 추론할 수 없다. |

## 상세 기능 대조표

### MQTT protocol, 연결과 delivery

| 기능 | EMQX Self-Hosted Enterprise | NaruMQ 현재 상태 | 비교 시 주의점 |
| --- | --- | --- | --- |
| MQTT protocol version | MQTT 3.x와 MQTT 5.0을 지원한다고 공식 기능표에 기재한다. Enterprise WebSocket 문서는 MQTT 3.1, 3.1.1, 5.0 호환을 명시한다. [E1][E6] | **부분:** MQTT 3.1.1/5.0을 지원한다. MQTT 3.1.0의 `MQIsdp`·protocol level 3은 거절하며 48개 normative statement의 compatibility matrix만 수용 evidence로 연결되어 있다. [N1][N5] | EMQX를 쓰는 중인 client의 3.1.0 여부를 확인한다. 현재 연결 snapshot만으로 충분하지 않으며 PRD는 장기 관찰을 요구한다. |
| MQTT 5 properties와 protocol options | MQTT 5 complete implementation으로 설명하며 Reason Code, User Property, Topic Alias, expiry와 Enhanced Authentication 등을 문서화한다. [E1][E2] | **부분:** Session Expiry, Message Expiry, subscription options 등 시험에 연결된 동작을 구현·검증한다. 48개 conformance statement는 전체 MQTT 5.0 적합성 선언이 아니다. Will Delay는 standalone socket test에서 reconnect suppression만 확인했다. [N1][N8] | 양쪽 모두 MQTT 5를 지원한다고 해서 모든 property·edge case가 같다고 볼 수 없다. 필요한 client property별 interoperability test가 필요하다. |
| Transport | TCP/TLS, WS/WSS와 QUIC를 Self-Hosted Enterprise에서 지원한다. [E1][E6][E7] | **확인:** TCP/TLS. **계획:** WebSocket/WSS는 P1 선택 기능. QUIC 구현 evidence는 확인되지 않았다. [N1][N5] | Web browser, proxy/firewall 제한, 이동망 또는 QUIC 사용 client는 전환 전 별도 확인한다. |
| QoS 0/1/2 | MQTT QoS 0/1/2 표준 동작을 제공하며, Durable Sessions는 persistent session을 대상으로 offline message를 저장할 수 있다. [E2][E3] | **부분:** Standalone ingress/egress는 process-local session state를 사용한다. Dynamic cluster는 QoS 1/2 durable publish ACK와 detached QoS 1/2 queue/reconnect를 제한된 process acceptance로 확인했다. [N1][N2] | Process test는 Linux/EBS power-loss, N/N-1 또는 모든 QoS lifecycle 조합을 증명하지 않는다. |
| Persistent session과 reconnect | 일반 session은 memory 기반이다. 설정한 Durable Sessions는 session과 전달 message를 disk에 저장하고 node restart 뒤 복구한다. [E3] | **부분:** Standalone persistent session은 process-local이며 process 재시작 뒤 복구되지 않는다. Dynamic cluster process acceptance는 persistent subscription, detached QoS 1/2 delivery와 SessionPresent, subscriber QoS inflight takeover를 확인했다. [N1][N2] | Session expiry가 같아도 persistence mode와 crash boundary가 같다는 뜻은 아니다. |
| Offline message queue | Durable Session은 durable subscriber를 대상으로 queued message를 저장하고 cluster Durable Storage를 사용한다. 일반 memory session은 node restart 시 유실될 수 있다. [E3] | **부분:** `TestClusterBackupRestoreProcessCrashAndResync`가 detached QoS 0 queue를, `TestDynamicClusterDurableQoSTakeoverProcessAcceptance`가 detached QoS 1/2 queue와 reconnect를 확인했다. Production power-loss와 TEN-08 tenant quota는 미검증이다. [N2] | `QoS 0 offline replication`과 `durable QoS 1/2 queue`는 별도 acceptance evidence로 비교한다. |
| Retained message | MQTT Retainer와 expiry를 지원하며 Dashboard/API로 조회·관리할 수 있다. 저장 mode와 한도를 설정할 수 있다. [E1][E2] | **부분:** Standalone은 bounded process-local repository다. `TestClusterBackupRestoreProcessCrashAndResync`는 dynamic cluster의 tenant/topic-scoped QoS 0 retained cross-node replay와 backup/restore를 확인했다. 제한은 총 1,024개·16 MiB, payload 최대 1,000,000 bytes이며 tenant별 저장 quota는 없다. [N1][N2] | process acceptance는 production EBS durability나 TEN-08 tenant별 retained byte quota를 충족하지 않는다. |
| Shared subscription | `$share/{group}/{filter}`와 여러 dispatch strategy를 지원한다. 문서화된 전략에는 round-robin, sticky, local과 hash 계열이 있다. [E8] | **부분:** `$share` group, tenant isolation, Round-Robin 및 선택 대상 queue가 full/unavailable일 때 다음 eligible member로 handoff하는 동작을 지원한다. 다른 dispatch strategy의 동등 기능은 근거가 없다. [N1][N2] | 사용 중인 EMQX dispatch strategy, ordering과 consumer failure behavior를 설정에서 조사한다. |
| Topic routing과 MQTT 5 subscription option | Wildcard, shared subscription, No Local, Retain As Published, Retain Handling 등 MQTT 기능을 지원한다. [E2][E8] | **확인:** `+`/`#`, `$` system topic boundary, overlapping subscription deduplication과 MQTT 5 No Local·RAP·Retain Handling을 문서화한다. [N1] | Topic matching 결과뿐 아니라 retained replay, shared subscription과 tenant boundary를 함께 비교한다. |
| Will과 Message Expiry | Will, Will Delay, Session Expiry와 Message Expiry를 지원한다. [E2][E3] | **부분:** Standalone `TestMQTTIngressWillMessagesAndSessionExpiry`가 socket drop, 정상/0x04 disconnect, reconnect 시 Will Delay suppression과 Session Expiry를 확인한다. Message Expiry는 별도 MQTT acceptance mapping에 있다. Cluster failover 중 Will timer 동등성은 미검증이다. [N1][N2][N8] | Server shutdown, disconnect와 node crash가 Will을 발생시키는 조건을 client별로 검증한다. |
| Multi-protocol gateway | Self-Hosted Enterprise 기능표에 MQTT-SN, CoAP, STOMP, LwM2M 등 gateway protocol이 포함된다. [E1] | **비목표:** MQTT-SN, CoAP, STOMP, LwM2M 등 비-MQTT gateway는 초기 제품 범위 밖이다. [N5] | 해당 device가 MQTT로 직접 전환할 수 없으면 gateway/adapter migration work가 필요하다. |

### Cluster, persistence와 lifecycle

| 기능 | EMQX Self-Hosted Enterprise | NaruMQ 현재 상태 | 비교 시 주의점 |
| --- | --- | --- | --- |
| Cluster 구성과 scaling | 공식 기능표는 multi-node cluster, discovery, autoscaling과 network split recovery를 제시한다. Self-hosted, Serverless와 Dedicated Flex의 cluster 특성은 다르다. [E1] | **부분:** Process acceptance는 dynamic membership add/remove, transition crash/restart, readiness와 active-session handoff를 확인한다. `TestDynamicClusterMembershipProcessAcceptance` 등은 전체 cluster architecture나 production readiness를 승인하지 않는다. [N2][N6] | 분산 방식의 이름보다 quorum loss, membership change, stale owner 차단과 복구 경계가 실사용에 중요하다. |
| Durable storage와 failover | Durable Storage는 RocksDB 기반 disk persistence와 Raft replication을 사용한다. Durable Sessions는 `durable_sessions.enable`로 활성화하지만 이것이 MQTT ACK의 동기 영속화 경계를 뜻하지는 않는다. [E3][E17][E18] | **부분:** QoS 1/2 detached delivery/takeover, process crash recovery와 backup v3 restore를 제한된 process acceptance로 확인했다. WAL/commit crash와 Docker/FUSE 장애 시험의 부분 증거는 있지만 production Linux/EBS power-loss, volume loss, 유효한 BP-03과 N/N-1 gate는 남아 있다. [N2][N7][N9] | 저장 기능, ACK 시점의 약속, 실제 장애 시험의 검증 범위를 구분한다. |
| Backup·restore | 공식 feature list는 data persistence와 cluster tooling을 제공하지만, 이 표만으로 NMQBKUP3와 같은 archive format/restore policy의 동등성을 판단할 수 없다. [E1] | **부분:** `TestClusterBackupRestoreProcessCrashAndResync`는 retained/offline QoS 0을, `TestDynamicClusterDurableQoSBackupRestoreProcessAcceptance`는 QoS 1/2 delivery state와 publisher tombstone의 backup v3 restore를 확인했다. EBS power-loss, volume loss/reseed와 retention/RPO/RTO는 미실행이다. [N2][N6] | Backup format 지원 여부와 실제 disaster recovery RPO/RTO를 따로 검증한다. |
| Node evacuation, drain, rebalance | Self-Hosted Enterprise 기능표는 node evacuation/rebalance와 hot upgrade를 포함한다. EMQX 문서는 maintenance 전 traffic/session evacuation workflow를 설명한다. [E1] | **계획/미완료:** Safe drain은 PRD lifecycle 목표지만 active management contract에서 drain mutation은 deferred다. | 연결 이동, replicated session state takeover와 Pod 삭제 gate를 별도로 비교한다. |
| Dynamic membership compatibility | Discovery 방식과 cluster 운영은 feature/version/edition에 종속된다. [E1] | **부분:** Membership process acceptance와 profile gate가 있지만, N/N-1 mixed-version state 호환성은 acceptance 완료로 간주하지 않는다. Retained/offline current profile은 exact member profile 조건을 쓴다. [N2][N6] | 동일 cluster에 서로 다른 release를 섞을 수 있다고 추정하지 말고 지원 matrix를 확인한다. |
| Production deployment profile | Self-hosted Enterprise와 Kubernetes Operator 배포를 문서화한다. [E1] | **계획·미검증:** EKS EC2 3-AZ가 목표 production profile이다. EBS/WAL power-loss gate 전에는 production storage guarantee로 표현하지 않는다. [N5][N6] | Docker/local process test는 Linux/EBS volume failure나 AZ durability proof가 아니다. |

### Durable Storage와 durable ACK의 보장 경계

이 절은 BP-03에 고정한 **EMQX 5.8.8 Enterprise**와 NaruMQ의 dynamic cluster 계약을 비교한다. 다른 EMQX release나 NaruMQ standalone memory profile에 그대로 적용하지 않는다. **복제 저장 구성은 유사하지만 성공 ACK의 내구성 의미는 동등하지 않다.** NaruMQ의 명세가 더 엄격하다는 사실과 production에서 그 보장을 완전히 검증했다는 주장은 별개다. [N7][N9]

**Durable Storage**는 메시지를 디스크에 저장하고 여러 node에 복제하는 저장소다. **Durable ACK**는 client에게 성공을 응답하기 전에 어떤 저장·복제 작업을 완료하는지 정하는 약속이다. **WAL**은 복구를 위해 command를 먼저 기록하는 로그이며, `fsync`는 운영체제에 디스크 동기화를 요청하는 작업이다. 같은 Raft나 replica 수를 사용해도 MQTT ACK가 그 완료를 기다리는지는 별도로 확인할 항목이다.

| 비교 항목 | EMQX 5.8.8 소스·실행에서 확인한 동작 | NaruMQ가 명세한 계약 |
| --- | --- | --- |
| 일반 메시지 저장 대상 | `emqx_persistent_session_ds_router:has_any_route`로 매칭 durable-session route가 있는지 확인한다. 단순히 live MQTT subscription이 있다는 조건이 아니다. [E17] | ADR-0019: committed QoS≥1 matching subscription(durable target)이 있을 때만 delivery 기록을 저장한다. Retained 값과 QoS 2 수신 상태는 별도로 저장한다. 모든 성공 ACK는 quorum 확인 뒤 반환한다. [N9][N10] |
| QoS 1 PUBACK | `sync => false`로 저장을 요청한다. Buffer에 비동기 요청을 보내고 저장 완료 응답을 기다리지 않는 경로다. [E17][E18][E20] | Target이 있으면 delivery command commit 뒤, 없으면 write 없이 quorum ReadIndex barrier 뒤 PUBACK한다. EMQX 비동기 ACK는 채택하지 않는다. [N10] |
| QoS 2 PUBREC | **소스 경로 확인:** broker publish 뒤 `awaiting_rel`을 갱신하고 state commit timer를 설정한다. Channel은 그 결과로 PUBREC을 만든다. 아래 격리 실험은 PUBREC을 시험하지 않았다. [E20][E21] | 원본 메시지와 복구에 필요한 handshake·delivery command를 영속화·commit한 뒤 성공 PUBREC을 반환하는 계약이다. [N1][N9] |
| 장애 검증 | 아래 실험은 저장 처리 전 PUBACK를 확인했다. 3-node ACK 직후 장애·전원 손실 또는 실제 메시지 손실을 검증한 것은 아니다. | Process crash·WAL/commit crash·partition 및 Docker/FUSE simulation의 부분 증거가 있다. ADR-0003은 `Proposed`이고 BP-03·production storage 수용은 미완료다. [N7][N9] |

EMQX의 메시지 저장 호출은 다음과 같다. `sync`는 이 저장 API의 완료 응답을 기다리는지 나타내며, `fsync` 설정과 같은 이름의 옵션으로 해석하지 않는다. [E17][E18]

```erlang
emqx_ds:store_batch(?PERSISTENT_MESSAGE_DB, [Msg], #{sync => false}).
```

현재 BP-03의 message database는 `atomic_batches=false`이며 이 호출은 `emqx_ds_buffer` 경로를 사용한다. `sync=false`에서는 `gen_server:cast`로 비동기 요청을 보낸다. Buffer가 이후 batch를 처리할 때 Raft 저장을 호출하므로 MQTT 성공 ACK가 그 처리·복제 완료를 기다리는 경계는 아니다. [N7][E18][E19]

**격리 실행 증거(2026-10-01):** BP-03과 같은 pinned Enterprise image로 별도 container `bp03-ack-boundary-probe-20261001`을 실행했다. 이 실험의 설정은 `builtin_raft`, `n_sites=1`, `replication_factor=1`, `n_shards=1`이며 **3-node quorum 내구성 시험이 아니다**.

1. Session Expiry 60초의 실제 MQTT durable subscriber가 `durability-probe/#`를 QoS 1로 구독했고 SUBACK을 받았다. 메시지 topic에 durable route가 있는 것도 확인했다.
2. 메시지 저장 buffer만 `sys:suspend`로 일시 정지했다.
3. Publisher가 QoS 1 메시지를 보냈고 성공 PUBACK body `00070000`을 받았다.
4. Buffer가 여전히 정지한 상태에서 해당 메시지가 처리되지 않은 `enqueue_req` 비동기 요청으로 저장 담당 process의 **메모리 대기열**에 남아 있었다.
5. Buffer를 재개한 뒤 subscriber가 정확한 topic·payload를 받았다. 연결과 격리 container는 정리했다.

이 실행은 **PUBACK가 저장 buffer 처리보다 먼저 나올 수 있음**을 입증한다. 그 대기 중 crash/kill을 주입하지 않았으므로 실제 손실을 관측한 결과로 표현하지 않는다. QoS 2 판단은 위 소스 경로에 근거하며 이 실험의 실행 결과로 확대하지 않는다. 별도 BP-03 3-node run의 replica set과 정상 workload·offline/reconnect 결과는 [성능 검증 §3.4](../specs/performance-and-verification.md#34-bp-03-durable-delivery-smoke-관찰-경계)에 있다. 이는 3 replicas의 구성·정상 동작 증거이지 ACK 직후 clustered fault proof가 아니다. [N7]

#### 채택: durable target이 없을 때 delivery 기록 생략

2026-10-02 **ADR-0019로 target 없는 delivery command 생략을 채택했다.** Durable target 후보는 PUBLISH 준비 시점의 ingress route projection에 있는 primary 후보 중 effective QoS가 1 이상인 subscription이다. Session의 persistent 여부는 후보 포함 여부를 바꾸지 않으며, apply 시 committed ownership과 subscription을 다시 검증한다. [N2][N10]

Target 후보가 없는 QoS 1 client PUBLISH와 QoS 1/2 Will은 delivery PUBLISH command를 만들지 않는다. 대신 fresh quorum ReadIndex barrier를 통과하고 그 index까지 local apply와 route projection이 끝난 뒤 QoS 0 lane을 처리한다. QoS 1 client는 그 다음에 PUBACK을 받으며 Will에는 publisher ACK가 없다. Quorum barrier가 실패하거나 5초 안에 끝나지 않으면 QoS 0 lane과 성공 PUBACK을 막고 연결을 닫는다.

| 상황 | 채택한 저장·ACK 범위 |
| --- | --- |
| Committed QoS 1 이상 subscription이 매칭됨 | Session의 persistent 여부와 무관하게 durable target으로 delivery state를 저장하고 command를 commit한다. |
| Target 후보가 없음 | Delivery payload와 record를 저장하지 않는다. Quorum 확인은 유지하지만 미래 subscriber 전달이나 장애 뒤 전달을 보장하지 않는다. QoS 2 client PUBLISH inbound RECEIVE와 retained 값은 별도로 저장한다. |
| Retained PUBLISH | 구독자 유무와 별개로 retained mutation을 commit한다. |
| QoS 2 client PUBLISH와 RELEASE | RECEIVE는 target과 무관하게 PUBREC 전에 commit하고, RELEASE도 target 수와 무관하게 PUBCOMP 전에 commit한다. |

EMQX 5.8.8은 matching durable-session route가 있을 때 메시지 저장 경로를 사용하며 `sync => false`로 PUBACK을 저장 완료 전에 반환할 수 있다. NaruMQ는 저장 대상만 나누고 성공 ACK의 quorum 확인은 유지한다. Target이 없을 때는 저장 command 대신 ReadIndex barrier를 사용하며 EMQX의 asynchronous ACK를 채택하지 않는다. 이 구분은 [ADR-0019](../decisions/ADR-0019-untargeted-durable-publish-quorum-barrier.md)의 범위다. [E17][E18][N10]

이 변경은 target 없는 QoS 1 client PUBLISH와 QoS 1/2 Will의 delivery write만 생략한다. Retained mutation, publisher QoS 2 RECEIVE, QoS 2 RELEASE와 target 있는 QoS 1/2 delivery는 기존 저장 경계를 유지한다. 기존 BP-03 부하는 durable target이 매칭되는 workload이므로 이번 변경이 해당 benchmark의 비용이나 미달성 결과를 바꾸지 않는다. [N7][N10]

### Security와 multi-tenancy

| 기능 | EMQX Self-Hosted Enterprise | NaruMQ 현재 상태 | 비교 시 주의점 |
| --- | --- | --- | --- |
| Tenant namespace | Namespace로 MQTT client를 논리 그룹화하고 topic/client ID isolation, management namespace와 tenant observability를 구성한다. Client isolation은 별도로 설정해야 한다. [E11] | **부분:** Authenticated tenant를 session key, routing, retained, ACL과 management query에 묶는 typed tenant model을 구현한다. 모든 tenant quota가 구현됐다는 뜻은 아니다. [N3][N4] | EMQX namespace도 자동으로 모든 격리 정책을 켜는 것은 아니다. 실제 topic mountpoint/client ID policy를 비교한다. |
| Tenant quota와 noisy-neighbor control | Namespace별 최대 session 수와 tenant/client message·byte rate limit을 제공한다. [E11][E12] | **부분:** Retained/offline 저장 상한은 전체 tenant 합산 한도다. TEN-08 tenant별 offline queue·retained bytes·storage quota 및 alarm은 미충족 상태다. 기타 tenant quota requirement도 current acceptance와 구분한다. [N2][N3][N5] | 전환 전 tenant별 connection, session, subscription, rate와 stored bytes quota가 필수인지 확정한다. |
| Client authentication | Username/password, JWT, X.509, MQTT 5 Enhanced Authentication/SCRAM, PSK와 built-in/external database·HTTP backend를 문서화한다. [E9] | **부분:** Current secure mode는 JWT/JWKS 또는 verified client certificate mTLS와 static identity binding을 사용한다. EMQX와 같은 backend 종류·authenticator chain은 확인되지 않았다. [N3] | 실제 credential source, key rotation/cache/fail-closed behavior를 인증 방식별로 검증한다. |
| Topic authorization | File, built-in DB, MySQL/PostgreSQL/MongoDB/Redis/LDAP/HTTP authorizer와 chain/caching을 문서화한다. [E10] | **부분:** Tenant-local default-deny ACL과 publish/subscribe/unsubscribe/Will 검사를 지원한다. 외부 dynamic ACL backend 및 mutation 관리 API의 동등 기능은 확인되지 않았다. [N3][N4] | Default deny만으로 external policy lifecycle·caching·audit 동등성을 의미하지 않는다. |
| Transport와 peer security | TLS, X.509 및 인증 방식의 폭은 listener/version 설정에 따라 다르다. [E1][E9] | **확인된 범위:** MQTT listener TLS 1.2 이상, JWT/JWKS 또는 verified client certificate mTLS. Cluster peer 연결은 별도 mTLS/member allowlist 경로를 사용한다. [N1][N2][N3][N6] | Client listener 보안, peer transport와 production storage/RPO 보장은 서로 다른 검증 경계다. |
| Management RBAC와 audit | Dashboard/REST API RBAC, API key, audit 기능을 제품 matrix와 Dashboard 문서에 기재한다. Namespace administrator는 trusted deployments 용도로 한정된다. [E1][E11][E13][E14] | **부분:** Bearer/API-key 보호와 active query endpoint RBAC가 있다. Management spec은 mutation, OIDC, audit 기반 운영 흐름을 deferred로 표시한다. [N3][N4] | UI role 이름만 비교하지 말고 tenant scope, mutation authorization, audit retention을 검사한다. |

### Integration, management와 observability

| 기능 | EMQX Self-Hosted Enterprise | NaruMQ 현재 상태 | 비교 시 주의점 |
| --- | --- | --- | --- |
| SQL Rule Engine / transformation | Topic·event source를 SQL로 filter, transform, enrich하고 action으로 republish 또는 sink에 전달한다. [E4] | **비목표:** EMQX SQL Rule Engine 호환, 범용 stream processing/ETL은 초기 제품 비목표다. [N5] | Migration에는 별도 subscriber/adapter 또는 downstream service 구현이 필요할 수 있다. |
| Data integration / bridge | 공식 matrix는 Kafka와 40+ enterprise integrations를 열거한다. Kafka connector는 rule engine sink/source로 양방향 연동한다. [E1][E5] | **비목표/계획:** 대규모 connector 묶음은 비목표. Webhook, MQTT bridge와 Kafka adapter는 Phase 5 선택 기능으로 계획되어 있고 current feature로 세지 않는다. [N5] | 실제 사용 중인 connector, delivery retry, queueing, credentials와 failure mode를 inventory한다. |
| Schema registry, codec, validation | Schema Registry, JSON/Avro/Protobuf codec 및 schema validation 기능을 edition별로 문서화한다. [E1] | **미확인:** NaruMQ current spec/code evidence에서 대응하는 broker-side schema registry/codec platform은 확인되지 않았다. [N5] | Application payload validation이 broker 외부에 있으면 이동 영향 범위를 따로 기록한다. |
| Plugin / extension model | Plugins, hooks, gateway와 hot loading/configuration을 self-hosted feature matrix에 기재한다. [E1] | **미확인:** Go port/adapter 구조는 source-code extension 경계이지, EMQX runtime plugin/hot-load feature와 같은 public extension contract는 아니다. [N6] | NaruMQ module 추가가 기존 EMQX plugin을 대체한다고 가정하지 않는다. |
| Web console | Built-in Dashboard에서 client, subscription, cluster, integrations와 configuration을 관리한다. [E13] | **계획/미완료:** NaruMQ Dashboard는 PRD Phase 4 목표이며 현재 management spec은 query API를 기준으로 한다. [N4][N5] | UI를 대체할 operator automation, access policy와 day-2 workflow를 함께 비교한다. |
| Management API | Versioned REST API와 broad management/feature endpoints를 제공한다. [E14] | **부분:** Active contract는 13개 GET query operations 및 health/metrics endpoints다. Tenant/ACL/quota mutation, client disconnect, retained mutation과 drain endpoints는 deferred다. [N4] | API를 통한 client 운영·설정 변경이 필요한지, read-only 조회만 필요한지 확인한다. |
| Metrics, tracing, diagnostics | Prometheus pull/push, cluster metrics, Dashboard, log trace와 slow-subscriber diagnostics를 문서화한다. [E1][E13][E15] | **부분:** Prometheus/OpenMetrics, health probes와 cluster/node/session 조회가 있다. 모든 per-topic/per-client diagnostics, slow-subscriber tracing과 Dashboard 기능의 동등성은 확인되지 않았다. [N4] | Metric name, cardinality, endpoint auth와 retention/export 경로를 실제 dashboard 기준으로 비교한다. |
| Deployment and support | Self-hosted Enterprise와 Kubernetes Operator를 제공하며 license/support terms가 별도 적용된다. [E1][E16] | **미확정/범위 밖:** 이 repository 자료만으로 NaruMQ license 및 상업적 사용 권한을 확정하지 않는다. Support SLA와 release packaging은 이 표의 평가 범위 밖이다. [N5] | 기능 대조는 구매·상용지원·라이선스 검토를 대신하지 않는다. |

## 비교 결과를 실제 선택에 연결하는 방법

1. **현재 EMQX를 고정한다.** 실제 production version, deployment type, license/edition, enabled feature와 configuration export를 확보한다. NaruMQ PRD도 benchmark version·edition 결정을 요구한다. [N5][N7]
2. **사용 중인 기능을 수집한다.** MQTT protocol version/property, WS/QUIC, QoS, persistent sessions, shared-subscription strategy, retained, gateway, rule, connector, auth backend, quota, management API와 drain workflow를 포함한다.
3. **보장 범위를 맞춘다.** QoS 0, volatile session, durable QoS 1/2, offline queue, node loss와 backup restore를 같은 durability/replication 조건에서 분리 시험한다. QoS label만 같게 설정하는 것은 동등 비교가 아니다. [N2][N7]
4. **기능 표와 성능 결과를 분리한다.** EMQX vendor-published capacity 수치와 NaruMQ 목표 수치를 직접 비교하지 않는다. PRD는 BP-01~BP-07에서 같은 workload, node/storage/network profile과 raw evidence를 요구한다. [N7]
5. **production gate를 별도로 닫는다.** NaruMQ의 local/process acceptance는 Linux/EBS power-loss, volume loss, retention/RPO/RTO와 tenant quota acceptance를 대체하지 않는다. [N2][N5][N6]

따라서 현재 evidence만으로 **기능 완전 동등**, **성능 우위** 또는 **바로 교체 가능**을 결론 내릴 수 없다. 가장 큰 기능 경계는 EMQX의 integration/gateway/operator platform과 durable session 범위이며, NaruMQ는 MQTT core·tenant isolation·선택된 Raft replicated state에 범위를 둔다.

## 프로젝트에서 찾을 위치

- MQTT protocol, QoS, session, retained, shared subscription: [MQTT protocol과 session spec](../specs/mqtt-protocol-and-session.md)
- ownership, offline queue, retained replication, backup/restore와 acceptance: [Cluster session과 durable delivery spec](../specs/cluster-session-and-delivery.md)
- tenant identity, authentication, ACL과 quota: [Tenant와 security spec](../specs/tenancy-and-security.md)
- query API, dashboard/deferred endpoints와 observability: [Management API와 observability spec](../specs/management-api-and-observability.md)
- 제품 범위·비목표·phase 및 benchmark: [PRD](../requirements/PRD.md), [Performance와 verification spec](../specs/performance-and-verification.md)
- cluster structure와 implementation status: [Cluster architecture](../architectures/cluster-architecture.md)

## 확인 질문

1. 운영 중인 EMQX client 중 MQTT 3.1.0, WebSocket/QUIC 또는 non-MQTT gateway에 의존하는 client가 있는가?
2. 연결이 끊긴 session에 쌓인 QoS 1/2 message를 EMQX node 재시작 또는 장애 뒤에도 전달해야 하는가?
3. NaruMQ의 현재 QoS 0 offline queue와 global aggregate storage cap이 tenant별 보장·quota 요구를 충족하는가?
4. Dashboard, SQL rule, connector, mutation API, drain과 audit를 대체할 운영 경로가 준비되어 있는가?
5. 기능 호환성 외에 동일 환경의 benchmark와 production storage gate를 통과했는가?

## 참고 자료

### NaruMQ 기준 문서

- [N1] [MQTT protocol과 session spec](../specs/mqtt-protocol-and-session.md)
- [N2] [Cluster session과 durable delivery spec](../specs/cluster-session-and-delivery.md)
- [N3] [Tenant와 security spec](../specs/tenancy-and-security.md)
- [N4] [Management API와 observability spec](../specs/management-api-and-observability.md)
- [N5] [Product requirements (PRD)](../requirements/PRD.md)
- [N6] [Cluster architecture](../architectures/cluster-architecture.md)
- [N7] [Performance와 verification spec](../specs/performance-and-verification.md)
- [N8] [Standalone Will and Session Expiry socket test](../../tests/integration/mqtt_ingress_test.go)
- [N9] [ADR-0003: Durable ACK 경계와 대안](../decisions/ADR-0003-durable-ack-barrier.md)
- [N10] [ADR-0019: Target 없는 durable publish의 quorum barrier](../decisions/ADR-0019-untargeted-durable-publish-quorum-barrier.md)

### EMQX 공식 문서

- [E1] [Feature Comparison](https://docs.emqx.com/en/emqx/latest/get-started/feature-comparison.html)
- [E2] [MQTT Advanced](https://docs.emqx.com/en/emqx/latest/get-started/mqtt-advanced.html)
- [E3] [MQTT Durable Sessions](https://docs.emqx.com/en/emqx/latest/guides/durability/durability_introduction.html)
- [E4] [Rule Engine](https://docs.emqx.com/en/emqx/latest/develop/data-integration/rules.html)
- [E5] [Stream MQTT Data into Apache Kafka](https://docs.emqx.com/en/emqx/latest/develop/data-integration/data-bridge-kafka.html)
- [E6] [MQTT over WebSocket](https://docs.emqx.com/en/emqx/latest/develop/connect-emqx/mqtt-over-websocket.html)
- [E7] [Use MQTT over QUIC](https://docs.emqx.com/en/emqx/latest/develop/mqtt-over-quic/getting-started.html)
- [E8] [MQTT Shared Subscription](https://docs.emqx.com/en/emqx/latest/get-started/messaging/mqtt-shared-subscription.html)
- [E9] [Authentication](https://docs.emqx.com/en/emqx/latest/guides/access-control/authn/authn.html)
- [E10] [Authorization](https://docs.emqx.com/en/emqx/latest/guides/access-control/authz/authz.html)
- [E11] [Namespace](https://docs.emqx.com/en/emqx/latest/guides/multi-tenancy/namespace-overview.html)
- [E12] [Configure and Manage Namespaces](https://docs.emqx.com/en/emqx/latest/guides/multi-tenancy/configure-manage-namespace.html)
- [E13] [EMQX Dashboard](https://docs.emqx.com/en/emqx/latest/guides/dashboard/introduction.html)
- [E14] [REST API](https://docs.emqx.com/en/emqx/latest/guides/api.html)
- [E15] [Monitor EMQX with Prometheus](https://docs.emqx.com/en/emqx/latest/guides/observability/prometheus.html)
- [E16] [Work with EMQX Enterprise License](https://docs.emqx.com/en/emqx/latest/get-started/deploy/license.html)
- [E17] [EMQX e5.8.8 메시지 저장 대상과 비동기 호출](https://github.com/emqx/emqx/blob/e5.8.8/apps/emqx/src/emqx_persistent_message.erl)
- [E18] [EMQX e5.8.8 저장 buffer의 동기·비동기 요청](https://github.com/emqx/emqx/blob/e5.8.8/apps/emqx_durable_storage/src/emqx_ds_buffer.erl)
- [E19] [EMQX e5.8.8 Raft backend의 buffered 저장 경로](https://github.com/emqx/emqx/blob/e5.8.8/apps/emqx_ds_builtin_raft/src/emqx_ds_replication_layer.erl)
- [E20] [EMQX e5.8.8 MQTT channel의 PUBACK/PUBREC 생성](https://github.com/emqx/emqx/blob/e5.8.8/apps/emqx/src/emqx_channel.erl)
- [E21] [EMQX e5.8.8 durable session의 QoS 2 publish·state commit](https://github.com/emqx/emqx/blob/e5.8.8/apps/emqx/src/emqx_persistent_session_ds.erl)
