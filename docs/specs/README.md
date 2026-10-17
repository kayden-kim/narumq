# NaruMQ 시스템 스펙

이 디렉터리는 NaruMQ가 외부에서 보여야 할 동작을 정의한다. 제품의 필요성과 우선순위는 [PRD](../requirements/PRD.md), 구조 view는 [Architecture](../architectures/README.md), 기술 선택의 근거는 [ADR](../decisions/), 작동 원리의 해설은 [기술 해설](../explainers/README.md)을 기준으로 한다.

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 기준 요구사항 | PRD 0.5 |
| 관련 Architecture | [Architecture catalog](../architectures/README.md) |
| 관련 결정 | [ADR catalog](../decisions/README.md) |
| 관련 Contract | [Contract catalog](../contracts/README.md) |
| 대상 독자 | 구현자, 검토자, 시험 작성자, 운영 자동화 작성자 |

## 문서 목록

| 문서 | 외부 동작 계약 | 관련 요구사항 |
| --- | --- | --- |
| [MQTT protocol과 session](mqtt-protocol-and-session.md) | listener, CONNECT, QoS, session, retained, Will, subscription, 제한과 protocol 오류 | MQTT-01~MQTT-13, CLIENT-01~CLIENT-08 |
| [Cluster session과 durable delivery](cluster-session-and-delivery.md) | membership, session ownership, fencing, routing, 복제, partition, 복구 | CLU-01~CLU-14, NFR-07~NFR-10, NFR-15 |
| [Tenant와 security](tenancy-and-security.md) | tenant 결정, topic 격리, ACL, quota, 인증, RBAC, 감사 | TEN-01~TEN-10, SEC-01~SEC-12, NFR-16~NFR-19 |
| [Management API와 observability](management-api-and-observability.md) | REST resource, 공통 JSON 계약, probe, Metric, Dashboard 조회 | API-01~API-09, UI-01~UI-10 |
| [실행 옵션과 설정](configuration-and-runtime-options.md) | 실행 모드 선택, 설정 적재·검증, 시작 실패, 설정 변경 반영 경로 | API-05, API-09, SEC-02, SEC-05, MQTT-09, MQTT-11, CLU-09, CLU-12, TEN-06~TEN-08 |
| [Drain과 rolling update](drain-and-rolling-update.md) | node lifecycle, drain API, safe-to-stop barrier, client redirect, rollout gate, AWS deployment profile | DEP-01~DEP-24, CLIENT-01~CLIENT-09 |
| [Performance와 verification](performance-and-verification.md) | 성능 측정, 시험 계층, coverage, release gate와 증거물 | NFR-01~NFR-06, NFR-11~NFR-15, PERF-01~PERF-12, TEST-01~TEST-17 |

하나의 요구사항이 여러 문서에 나타나면 각 문서는 자기 경계의 관찰 가능한 동작만 정의한다. 예를 들어 session takeover의 소유권 규칙은 cluster spec이, 배포 중 실행 순서는 drain spec이 기준이다.

## 규범 문장

- **해야 한다**: 준수하지 않으면 해당 spec을 충족하지 못한다.
- **해서는 안 된다**: 금지된 동작이다.
- **권장한다**: 명시한 사유가 있을 때만 다르게 구현할 수 있다.
- **할 수 있다**: 선택 동작이다.

MQTT packet 이름, property와 reason code는 OASIS 명칭을 사용한다. 충돌이 있으면 MQTT wire 동작은 OASIS MQTT 표준을 우선한다.

## 공통 식별자와 시간

| 이름 | 계약 |
| --- | --- |
| `tenantId` | 인증 결과에서 얻는 불투명 식별자다. client가 제출한 값을 그대로 신뢰해서는 안 된다. |
| `clientId` | tenant 안에서 session을 식별한다. session key는 `(tenantId, clientId)`다. |
| `nodeId` | cluster membership 세대에서 node를 식별하는 불투명 문자열이다. |
| `requestId` | 관리 요청과 감사 이벤트를 연결한다. 응답과 구조화 로그에 같은 값을 기록한다. |
| timestamp | JSON에서는 UTC RFC 3339 문자열을 사용한다. Metric 시간은 수집 시스템의 timestamp 규칙을 따른다. |
| duration | JSON에서는 음수가 아닌 정수 millisecond를 사용하며 duration field 이름은 `Millis`로 끝난다. Command-line flag는 Go duration 문자열을 사용한다. 설정 file의 표기는 [Process configuration v1 §1.9](../contracts/process-configuration-v1.md#19-표기-규칙)을 따른다. |

## 오류 원칙

1. 내구성 또는 권한 조건을 충족하지 못하면 성공으로 응답해서는 안 된다.
2. 재시도 가능한 오류와 영구 오류를 protocol 또는 API가 허용하는 범위에서 구분한다.
3. 오류 응답에 secret, token, password, private key, 원문 credential을 포함해서는 안 된다.
4. 거절과 상태 전이 실패는 `requestId` 또는 connection correlation ID로 Metric·로그·감사 이벤트를 연결할 수 있어야 한다.
5. 알 수 없는 구현 오류는 내부 세부 정보를 숨기고 안정된 외부 오류 코드로 변환한다.

## 미결정 계약

아래 값은 PRD의 Phase 0 또는 후속 ADR에서 확정해야 한다. 확정 전에는 배포 설정이 값을 명시해야 하며, 구현의 암묵적 기본값을 제품 보장으로 간주하지 않는다.

| ID | 미결정 항목 | 확정 위치 | 제약 |
| --- | --- | --- | --- |
| SPEC-TBD-01 | WAL 영속화 기준, replication factor, write quorum | [ADR-0002](../decisions/ADR-0002-cluster-consensus-library.md), [ADR-0003](../decisions/ADR-0003-durable-ack-barrier.md) | 두 ADR은 Proposed다. 승인 전에는 제품 기본값으로 확정하지 않는다. |
| SPEC-TBD-02 | listener, packet, inflight, queue 및 tenant quota 수치 | 일반 설정은 [실행 옵션과 설정 스펙](configuration-and-runtime-options.md); Dynamic cluster tenant 저장 quota와 최대값은 [ADR-0017](../decisions/ADR-0017-tenant-storage-quota.md); tenant 연결·traffic 한도는 [Tenant §5.2](tenancy-and-security.md#52-연결-상태와-traffic-한도) | Tenant storage quota는 네 dimension과 cluster-wide 최대값이 확정됐다. 저장 quota에는 그 밖의 무제한 값을 허용하지 않는다. Tenant 연결·traffic 한도의 차원과 허용 범위는 확정됐다. 제품 기본 수치는 아직 정하지 않았으므로 배포 security config가 값을 명시해야 한다. 생략하면 0(제한 없음)이다. 이는 listener 한도와 같은 구현 기본값이며 제품 보장이 아니다. |
| SPEC-TBD-03 | session·message expiry 상한과 offline queue 초과 정책 | [실행 옵션과 설정 스펙](configuration-and-runtime-options.md) | 적용 결과가 protocol 응답과 Metric에 드러나야 한다. |
| SPEC-TBD-05 | topic 내부 prefix encoding과 canonicalization 세부 규칙 | tenancy ADR/spec 개정 | 외부 topic과 tenant 격리를 보존해야 한다. |
| SPEC-TBD-07 | Metric 이름 전체와 histogram bucket | metrics catalog | label cardinality 제한을 지켜야 한다. |
| SPEC-TBD-08 | 성능 SLO와 release-gating EMQX version·edition | [Performance와 verification §6](performance-and-verification.md#6-regression-gate) | EMQX Enterprise 6.3.1 comparator는 선택했다. 95%/105% 비율은 별도 미채택 제안이다. 실제 `B_*`, comparator 측정값과 profile별 숫자 기준이 없어 gate는 열린 상태다. |
| SPEC-TBD-09 | backup RPO/RTO와 보존 기간 | disaster recovery spec | snapshot 원자성과 checksum 검증을 포함해야 한다. |

### Phase 1 확정 SDK 목록

Phase 1 SDK inventory는 `github.com/eclipse/paho.mqtt.golang v1.5.1`, `github.com/eclipse/paho.golang v0.23.0`, `paho-mqtt 2.1.0`으로 확정했다. 이 inventory는 Phase 1 compatibility evidence에만 적용하며, 자동 artifact가 없는 후보와 전체 contract 승격 조건은 compatibility contract에서 별도로 관리한다.

Broker-side drain, `SAFE_TO_STOP`, replicated recovery와 protocol redirect emission이 구현됐으며 process evidence는 [Drain spec §10](drain-and-rolling-update.md#10-broker-drain-수용-시험-매핑)에 있다. 이는 release-wide drain-safe support 완료를 뜻하지 않는다. Supported-SDK Server Reference 추종(DEP-06), MQTT 3.1.1/Will 호환성(DEP-07/12), external rollout gate(DEP-11/13~18), production profile과 storage 검증(DEP-19~23)은 blocker로 남는다. MQTT 3.1.0 client의 90일 관찰과 전환은 2026-10-17 사용자 결정으로 제품 blocker에서 제외했다([PRD §9.1](../requirements/PRD.md#mqtt-310-비지원과-이전-정책)).

Management API v1 query profile은 미결정 항목이 아니다. 현재 active contract와 OpenAPI는 [`docs/contracts/management-api-v1.md`](../contracts/management-api-v1.md), [`docs/contracts/management-api-v1.openapi.json`](../contracts/management-api-v1.openapi.json)과 [`tests/contracts/management-api-v1`](../../tests/contracts/management-api-v1)에서 관리한다. Broker drain은 active mutation이며 API rate limit은 active management-plane 동작이다. 다른 mutation·OIDC는 deferred scope다.

미결정 항목 때문에 규범 동작을 시험할 수 없으면 해당 release gate는 열린 상태로 남는다.

## 추적 규칙

- Go test·fuzz 함수의 doc comment는 `Traceability:` marker로 관련 PRD 요구사항 ID를 선언해야 한다.
- MQTT compliance test는 PRD ID와 OASIS normative statement ID를 함께 가져야 한다.
- 각 spec의 수용 시험은 자동화된 test case 이름 또는 compliance matrix 행으로 연결해야 한다.
- spec의 동작을 바꾸는 결정은 관련 ADR과 PRD를 같은 변경에서 갱신해야 한다.
