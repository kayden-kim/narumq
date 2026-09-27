# EMQX와 NaruMQ 주요 기능 대조표

EMQX Self-Hosted Enterprise는 MQTT broker 외에도 rule engine, data integration, multi-protocol gateway와 운영 도구를 제공하는 넓은 플랫폼이다. 현재 NaruMQ는 MQTT core, tenant-scoped Raft cluster와 제한된 QoS 1/2 durable delivery scope를 구현했으며 해당 범위의 acceptance도 확인했다. 하지만 외부 연동, management dashboard, production storage durability와 넓은 lifecycle·compatibility 보장은 동등한 범위로 완료되지 않았다. 이 문서는 확인된 구현, 제한된 동작, 계획과 근거 부족을 분리한다.

| 항목 | 값 |
| --- | --- |
| 기준 독자 | EMQX 운영자와 NaruMQ 기능·이전 범위를 비교하는 개발자·운영자 |
| 먼저 읽을 문서 | [PRD의 실제 EMQX 비교 기준](../requirements/PRD.md), [MQTT session과 delivery spec](../specs/mqtt-protocol-and-session.md) |
| 관련 기준 문서 | [PRD](../requirements/PRD.md), [Cluster spec](../specs/cluster-session-and-delivery.md), [Tenant·security spec](../specs/tenancy-and-security.md), [Management API spec](../specs/management-api-and-observability.md) |
| 대상 버전·확인일 | EMQX 공식 `latest` rolling docs(기능별 최소 version은 아래 참고); NaruMQ current working tree; 2026-09-27 |

## 비교 범위와 판정 기준

- **EMQX 기준:** 공식 기능 비교표의 **Self-Hosted (Enterprise)** 열을 주 기준으로 사용한다. 그 표에는 EMQX Cloud Serverless와 Dedicated Flex도 별도 열로 있으므로 self-hosted 기능을 Cloud 전체에 일반화하지 않는다. EMQX 5.9부터 BSL 1.1이 적용되고, 문서상 단일 node Community License와 상용 cluster license 조건이 있으므로 사용 version·license를 별도로 고정해야 한다. [E1][E16]
- **NaruMQ 기준:** 현재 상태는 code와 대응 수용 시험의 evidence로 판정한다. MQTT·cluster spec은 Draft이고 cluster architecture는 Target/Draft다. Draft/Target의 normative statement만으로 구현 완료 처리하지 않으며, cluster 기능은 수용 시험이 확인한 범위(in-process, process-level, production)를 구분한다. [N1][N2][N5][N6]
- **EMQX edition 선택은 미결정:** NaruMQ PRD는 benchmark에 쓸 EMQX version과 edition을 미결정 질문으로 남긴다. 이 문서는 제품 검증이나 전환 결정을 대신하지 않는다. 실제 기준 환경이 다른 edition/version이면 그 기준으로 표를 다시 확인한다. [N5]

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
| Durable storage와 failover | Durable Storage는 RocksDB 기반 disk persistence와 Raft replication을 사용한다. 기능은 `durable_sessions.enable`로 활성화하며 성능·latency 비용이 있다. [E3] | **부분:** QoS 1/2 quorum ACK, detached delivery/takeover, process crash recovery와 backup v3 restore를 제한된 process acceptance로 확인했다. Linux/EBS power-loss, volume loss, WAL/fsync crash, BP-03과 N/N-1은 미검증이다. [N2][N6] | Local process crash acceptance는 production durability와 동일하지 않다. |
| Backup·restore | 공식 feature list는 data persistence와 cluster tooling을 제공하지만, 이 표만으로 NMQBKUP3와 같은 archive format/restore policy의 동등성을 판단할 수 없다. [E1] | **부분:** `TestClusterBackupRestoreProcessCrashAndResync`는 retained/offline QoS 0을, `TestDynamicClusterDurableQoSBackupRestoreProcessAcceptance`는 QoS 1/2 delivery state와 publisher tombstone의 backup v3 restore를 확인했다. EBS power-loss, volume loss/reseed와 retention/RPO/RTO는 미실행이다. [N2][N6] | Backup format 지원 여부와 실제 disaster recovery RPO/RTO를 따로 검증한다. |
| Node evacuation, drain, rebalance | Self-Hosted Enterprise 기능표는 node evacuation/rebalance와 hot upgrade를 포함한다. EMQX 문서는 maintenance 전 traffic/session evacuation workflow를 설명한다. [E1] | **계획/미완료:** Safe drain은 PRD lifecycle 목표지만 active management contract에서 drain mutation은 deferred다. | 연결 이동, replicated session state takeover와 Pod 삭제 gate를 별도로 비교한다. |
| Dynamic membership compatibility | Discovery 방식과 cluster 운영은 feature/version/edition에 종속된다. [E1] | **부분:** Membership process acceptance와 profile gate가 있지만, N/N-1 mixed-version state 호환성은 acceptance 완료로 간주하지 않는다. Retained/offline current profile은 exact member profile 조건을 쓴다. [N2][N6] | 동일 cluster에 서로 다른 release를 섞을 수 있다고 추정하지 말고 지원 matrix를 확인한다. |
| Production deployment profile | Self-hosted Enterprise와 Kubernetes Operator 배포를 문서화한다. [E1] | **계획·미검증:** EKS EC2 3-AZ가 목표 production profile이다. EBS/WAL power-loss gate 전에는 production storage guarantee로 표현하지 않는다. [N5][N6] | Docker/local process test는 Linux/EBS volume failure나 AZ durability proof가 아니다. |

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
