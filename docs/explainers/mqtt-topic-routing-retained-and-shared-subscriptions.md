# MQTT topic routing, retained message와 shared subscription

Topic Name은 publisher가 메시지를 보내는 정확한 주소이고 Topic Filter는 subscriber가 받을 주소 집합을 표현한다. Retained message는 exact topic마다 broker가 보관하는 최신 값 하나다. Shared subscription은 같은 group의 여러 session 중 한 곳에만 각 matching publication을 보내 load를 나눈다.

| 항목 | 값 |
| --- | --- |
| 기준 독자 | MQTT 토픽 발행/구독 기초는 알지만 와일드카드 매칭 트리, 보존 메시지(Retained) 및 공유 구독($share) 라우팅 원리가 생소한 주니어 개발자 |
| 선수 문서 | [MQTT 전달 보장과 QoS·Session·Will protocol 흐름](mqtt-delivery-guarantees-and-protocol-flows.md) |
| 관련 기준 문서 | [PRD](../requirements/PRD.md) (MQTT-06~10, CLU-14), [MQTT protocol과 session 스펙](../specs/mqtt-protocol-and-session.md), [Cluster backup v3](../contracts/cluster-backup-v3.md), [ADR-0012](../decisions/ADR-0012-retained-replication-and-cluster-backup-v2.md), [ADR-0013](../decisions/ADR-0013-offline-queue-replication-and-cluster-backup-v3.md), [MQTT client compatibility v1](../contracts/mqtt-client-compatibility-v1.md) |
| 대상 version·확인일 | MQTT 3.1.1 / MQTT 5.0 |

이 문서는 MQTT 3.1.1과 MQTT 5.0의 공통 topic 원리와 MQTT 5.0 shared subscription을 기준으로 설명한다. NaruMQ는 standalone development profile에서 bounded local retained store를, dynamic cluster profile에서 tenant-scoped Raft retained state와 replay projection을 사용한다. Cluster ingress의 retained PUBLISH는 현재 QoS 0으로 제한된다. Version별 client 지원 범위는 [MQTT client compatibility v1](../contracts/mqtt-client-compatibility-v1.md)이 최종 기준이다.

## 읽고 나면 답할 수 있는 질문

- Topic Name과 Topic Filter, `+`와 `#` wildcard를 구분할 수 있는가?
- Retained message가 offline queue와 다른 이유를 설명할 수 있는가?
- Shared subscription group에서 publication 하나가 어느 session으로 가는지 설명할 수 있는가?

## 문제 상황

세 client가 다음 subscription을 가진다고 가정한다.

```text
dashboard: sensors/+/temperature
archive:   sensors/#
worker-a:  $share/alerts/alerts/+
worker-b:  $share/alerts/alerts/+
```

`sensors/room-1/temperature` publication은 dashboard와 archive에 각각 한 copy씩 전달된다. `alerts/high` publication은 `alerts` shared group의 worker-a와 worker-b 중 한 session에만 전달된다.

## 필수 용어

| 용어 | 뜻 |
| --- | --- |
| Topic Name | PUBLISH가 사용하는 정확한 topic 문자열. Wildcard를 포함하지 않는다. |
| Topic Filter | SUBSCRIBE가 사용하는 matching 표현 |
| Topic level | `/`로 구분되는 한 구간 |
| `+` | 정확히 한 topic level을 대신하는 single-level wildcard |
| `#` | 남은 모든 level을 대신하는 multi-level wildcard. Filter의 마지막에만 온다. |
| Subscription | 한 session과 Topic Filter·QoS·option의 연결 |
| Retained message | Broker가 tenant와 exact Topic Name별로 보관하는 최신 publication |
| Shared subscription | `$share/{group}/{filter}`를 공유하는 session 중 하나에 전달하는 subscription |

Topic 문자열을 일반 filesystem path처럼 정규화하면 안 된다. 연속 `/`, 대소문자와 Unicode를 임의로 바꾸면 MQTT matching 의미가 달라질 수 있다.

## Topic matching

| Filter | Match | Match하지 않음 |
| --- | --- | --- |
| `sensors/+/temperature` | `sensors/a/temperature` | `sensors/a/b/temperature` |
| `sensors/#` | `sensors`, `sensors/a`, `sensors/a/temp` | `sensor/a` |
| `+/status` | `device/status` | `a/device/status` |
| `alerts/high` | `alerts/high` | `alerts/low` |

`#`와 `+`는 Topic Filter에서만 특별한 의미를 가진다. Topic Name에 wildcard 문자가 들어간 PUBLISH는 protocol validation에서 거절한다.

`$`로 시작하는 system topic은 root level의 `#` 또는 `+`와 별도 matching 규칙을 가진다. NaruMQ는 tenant namespace를 적용한 뒤에도 MQTT system topic 규칙을 보존해야 한다.

## 정상 routing 흐름

아래 화살표는 PUBLISH의 runtime data flow다.

```mermaid
flowchart LR
    P["Validated PUBLISH"] --> T["Tenant canonical topic"]
    T --> A["Publish ACL + quota"]
    A --> R["Routing index lookup"]
    R --> N["Non-shared matches"]
    R --> G["Shared group matches"]
    N --> O["Each matching session owner"]
    G --> S["One eligible owner per group"]
```

1. MQTT adapter가 Topic Name과 packet을 검증한다.
2. Security 경계가 인증된 tenant와 외부 Topic Name을 canonical topic으로 바꾼다.
3. Publish ACL과 quota를 적용한다.
4. Routing index가 canonical filter와 matching한다.
5. Non-shared subscription에는 session별 copy를 만든다.
6. Shared group에는 eligible session 하나를 선택한다.
7. 각 owner가 granted QoS, inflight와 offline queue 규칙으로 전달한다.

Route metadata에는 session key, owner node, owner epoch와 subscription option이 연결돼야 한다. Stale owner가 만든 route는 제거하거나 선택에서 제외한다.

## Retained message 흐름

Standalone development profile은 `Retain=true` QoS 0 publication을 bounded process-local repository에 보관한다. Dynamic cluster profile은 tenant와 exact Topic Name을 retained key로 쓰고, Raft committed state를 권위 있는 값으로 보관한다. Cluster MQTT ingress는 현재 QoS 0 PUBLISH만 허용한다.

```text
PUBLISH Retain=true, payload="new"
    ├── standalone: retained.Replace(tenant, topic, publisher, payload, qos)
    └── cluster: fresh read barrier + exact member-profile check
                     → Raft retained replace/delete commit
                     → node-local retained projection update
                     → live route matching and delivery

PUBLISH Retain=true, payload=""
    ├── standalone: retained.Delete(tenant, topic)
    └── cluster: commit tenant/topic delete → projection update
```

- Cluster에서는 retained mutation이 route matching/live fan-out보다 먼저 commit된다. Commit이 실패하면 그 PUBLISH의 live fan-out도 시작하지 않는다.
- 새 retained publication은 같은 tenant와 exact topic의 이전 값을 대체한다. 다른 tenant의 같은 topic 값은 분리해 보관한다.
- Zero-byte retained PUBLISH는 해당 tenant/topic의 value를 삭제하지만 established subscription에는 일반 live publication으로 전달된다.
- 새 non-shared subscription은 MQTT 3.1.1과 MQTT 5 `Retain Handling` 규칙에 따라 matching retained value를 topic 정렬 순서로 replay한다.
- Replay delivery는 항상 RETAIN=1이고 stored QoS와 granted QoS 중 작은 QoS를 쓴다. Cluster ingress가 저장하는 retained value는 QoS 0이다.
- Live delivery는 `No Local`이면 같은 publisher session을 제외한다. `Retain As Published=false`면 RETAIN을 지우고, `true`면 incoming RETAIN을 보존한다.
- Cluster retained limits는 최대 1,024 entries, 총 계산 크기 16 MiB, payload당 1,000,000 bytes다. Byte 계산은 tenant ID, topic, payload와 publisher routing ID를 포함한다. Capacity conflict는 기존 non-expired 값을 교체하지 않고 live fan-out을 시작하지 않는다. Apply 시각 기준으로 expired entry는 결정적으로 제거될 수 있다.
- Message Expiry deadline은 cluster command에 포함되며 모든 replica가 같은 proposal 시각을 사용한다. Startup/restore 뒤 local replay projection은 committed retained state에서 다시 만든다.
- MQTT 5 quota 초과는 DISCONNECT reason `0x97`로 연결을 닫는다. MQTT 3.1.1은 protocol에 없는 reason code를 보내지 않고 연결을 닫는다.

Retained message는 “마지막으로 알려진 값”이다. Offline queue처럼 특정 session이 접속하지 않은 동안 모든 publication을 쌓지 않는다. Cluster delivery queue와 QoS 1/2 durable acknowledgement는 별도 미완료 범위다.


## Shared subscription 흐름

`$share/alerts/alerts/+`는 다음 세 부분으로 읽는다.

```text
$share / alerts / alerts/+
marker   group    underlying filter
```

같은 full shared filter를 subscribe한 session들은 한 group을 이룬다. Matching publication 하나는 그 group의 session 하나에만 간다. 다른 group이나 non-shared subscription은 각각 별도 copy를 받을 수 있다.

Member 선택 algorithm은 MQTT가 고정하지 않는다. NaruMQ는 tenant, availability, owner epoch, inflight와 backpressure를 고려하되 unavailable owner를 계속 선택해서는 안 된다.

## 실패 흐름

| 실패 | 남는 상태와 처리 |
| --- | --- |
| Invalid wildcard 위치 | SUBACK에서 해당 filter를 거절하고 route를 만들지 않는다. |
| Subscribe ACL 거절 | 다른 허용 filter와 분리해 거절하고 금지 route를 만들지 않는다. |
| Selected shared owner 장애 | Shared subscription runtime은 이 slice에서 지원하지 않는다. |
| Stale owner epoch route | Routing과 delivery에서 거절하고 최신 route를 다시 구성한다. |
| Retained replay queue full | batch 전체를 queue에 넣지 않고 이전 route를 유지한다. MQTT 5는 `0x97`, MQTT 3.1.1은 `0x80`을 반환한다. |
| Retained repository capacity failure | Incoming value가 저장되지 않고 live fan-out이 시작되지 않는다. Proposal 시각에 이미 만료된 값은 deterministic apply에서 제거될 수 있다. |

## 자주 생기는 오해와 trade-off

- QoS는 routing fan-out 수를 정하지 않는다. 각 matching subscription의 전달 품질을 제한한다.
- Retained message는 session에 속하지 않으며 `(tenantId, exact topic)`을 key로 쓴다. Publisher session identity는 MQTT 5 `No Local` 판정에 사용한다.
- Replay와 live delivery는 같은 queue ownership boundary를 쓰지만, replay route
  commit은 batch enqueue 성공 뒤에만 일어난다.
- Shared subscription은 broadcast가 아니지만 현재 runtime은 shared filter를 거절한다.
- Wildcard filter를 매 publish마다 전체 scan하면 구현은 쉽지만 subscription 수가 늘 때 비용이 커진다. Index 최적화는 canonical matching 의미를 바꾸면 안 된다.
- MQTT 3.1.1 표준에는 MQTT 5.0의 shared subscription 정의가 없다. 후속 runtime이 추가되면 compatibility contract에 명시해야 한다.

## NaruMQ에서 찾을 위치

- 외부 protocol 동작: [MQTT protocol과 session](../specs/mqtt-protocol-and-session.md#4-subscribe와-routing-입력)
- Cluster route와 owner epoch: [Cluster session과 durable delivery](../specs/cluster-session-and-delivery.md#3-routing)
- Tenant canonicalization과 ACL: [Tenant와 security](../specs/tenancy-and-security.md)
- Target domain package: `internal/domain/routing`, `internal/domain/delivery` — [Project structure](../architectures/project-structure.md)
- Cluster committed retained state: [`internal/application/membership`](../../internal/application/membership), local replay projection과 proposal barrier: [`cmd/narumq/cluster_retained.go`](../../cmd/narumq/cluster_retained.go)

## 확인 질문

1. `sensors/+/temperature`가 `sensors/a/b/temperature`와 match하지 않는 이유는 무엇인가?
2. Retained message와 persistent session의 offline queue는 각각 어떤 key와 목적을 가지는가?
3. 같은 shared group에 session 세 개가 있을 때 publication 하나는 몇 session에 전달되는가?

## 참고 자료

- [OASIS MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)
- [OASIS MQTT 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [MQTT protocol과 session](../specs/mqtt-protocol-and-session.md)
