# MQTT의 역사와 버전 진화: MQIsdp부터 MQTT 5.0까지

MQTT는 1999년 위성 기반 원격 송유관 감시(SCADA)를 위해 탄생한 경량 메시징 프로토콜로, 사유 규격인 MQIsdp(MQTT 3.1.0)를 거쳐 OASIS 표준인 MQTT 3.1.1과 클라우드 네이티브 기능을 보강한 MQTT 5.0으로 진화했습니다. NaruMQ는 안정적인 클러스터 세션 관리와 프로토콜 복잡도 제어를 위해 공식 표준인 MQTT 3.1.1과 5.0만 지원하며, 레거시 시그니처인 `MQIsdp`(Level 3) 연결은 명시적으로 거절합니다.

| 항목 | 값 |
| --- | --- |
| 기준 독자 | Go 기초와 네트워크 통신 기초를 알지만 MQTT 프로토콜의 역사적 배경과 패킷 헤더 구조에 익숙하지 않은 주니어 개발자 |
| 선수 문서 | [MQTT 전달 보장과 QoS·Session·Will protocol 흐름](mqtt-delivery-guarantees-and-protocol-flows.md) |
| 관련 기준 문서 | [PRD](../requirements/PRD.md) (MQTT-01, MQTT-13), [MQTT protocol과 session spec](../specs/mqtt-protocol-and-session.md) |
| 대상 version·확인일 | MQTT 3.1 (2010), MQTT 3.1.1 (2014), MQTT 5.0 (2019), 2026-09-18 |

## 읽고 나면 답할 수 있는 질문

- `MQIsdp`는 무엇이며 왜 최신 브로커에서 `MQTT` 문자열과 구분되는가?
- MQTT 3.1.0, 3.1.1, 5.0 버전 간의 핵심 변경 사항(헤더, Clean Session/Start, 오류 코드 등)은 무엇인가?
- NaruMQ는 왜 MQTT 3.1.0을 지원하지 않고 거절하며, 레거시 장비 환경에서는 어떻게 대응해야 하는가?

---

## 문제 상황

신입 플랫폼 엔지니어가 NaruMQ 스펙 문서(`docs/specs/mqtt-protocol-and-session.md`)를 읽다가 다음 조항을 발견했습니다.

> `Protocol Name MQIsdp 또는 level 3: MQTT 3.1.0 비지원으로 거절한다. session을 만들지 않는다.`

엔지니어는 `MQIsdp`를 단순 오타로 의심했으나, 며칠 후 수년 전 공장에 배치된 구형 IoT 센서들이 NaruMQ로 전환된 테스트 브로커에 접속하지 못하고 연결이 즉시 끊어지는 장애를 접수했습니다. 로그에는 `reason="unsupported_protocol_version", protocol="3.1.0"` 메트릭이 카운트되고 있었습니다. 왜 최신 브로커는 이 장비들의 접속을 거부하는 것일까요?

---

## 필수 용어

| 용어 | 뜻 | 이 문서에서 중요한 이유 |
| --- | --- | --- |
| **SCADA** | Supervisory Control and Data Acquisition (원격 감시 제어 시스템) | 고비용 위성 링크와 극도로 대역폭이 좁은 네트워크 환경에서 MQTT가 설계된 최초의 목적 |
| **MQIsdp** | MQ Integrator SCADA Device Protocol | OASIS 표준화 이전 IBM/Arcom이 정의한 초기 MQTT 3.1.0 이하의 공식 프로토콜 이름 문자열 (6바이트) |
| **Protocol Level** | 가변 헤더에서 프로토콜 버전을 나타내는 1바이트 정수 | Level 3은 3.1.0, Level 4는 3.1.1, Level 5는 5.0을 가리킴 |
| **Clean Session** | MQTT 3.1.1 이하에서 클라이언트 연결 시 세션을 초기화할지 유지할지 결정하는 단일 플래그 | 연결 수명과 세션 저장 수명이 강하게 결합되어 유연한 분산 클러스터 처리가 어려웠던 원인 |
| **Clean Start & Session Expiry** | MQTT 5.0에서 분리된 세션 시작 방식과 세션 만료 시간(초) 설정 | 연결 종료와 독립적으로 세션의 수명을 제어할 수 있게 개선된 핵심 기능 |
| **Reason Code** | MQTT 5.0 패킷(CONNACK, DISCONNECT, PUBACK 등)에서 성공·실패 사유를 나타내는 1바이트 코드 | 실패 원인을 클라이언트에 정확히 전달할 수 있게 됨 |

---

## MQTT의 발전 역사

```text
[1999년] IBM & Arcom: 원격 위성 감시용 사유 프로토콜 개발 (SCADA)
   │
   ▼
[2010년] MQTT 3.1 공개 규격 발표 (IBM Royalty-Free)
   │     • Protocol Name: "MQIsdp" (6바이트), Protocol Level: 3
   │     • Username/Password, Basic TLS 지원 추가
   │
   ▼
[2014년] OASIS 표준화: MQTT 3.1.1 채택 (2016년 ISO/IEC 20922 국제표준)
   │     • Protocol Name이 "MQTT"(4바이트)로 공식 변경, Protocol Level: 4
   │     • Client ID 길이 제한(23자) 완화, UTF-8 문자열 규칙 엄격화
   │
   ▼
[2019년] OASIS 표준화: MQTT 5.0 채택
         • Protocol Name: "MQTT", Protocol Level: 5
         • Clean Start와 Session Expiry Interval의 분리
         • Reason Code 전면 도입, User Property(메타데이터 헤더) 추가
         • Shared Subscription, Message Expiry, Flow Control 표준화
```

### 1. 탄생 배경: 1999년의 송유관 감시
MQTT는 1999년 Andy Stanford-Clark(IBM)와 Arlen Nipper(Arcom Control Systems, 현 Cirrus Link)가 개발했습니다. 당시 사막을 가로지르는 원격 송유관 센서들은 위성 전화선으로 연결되어 있었으며, 대역폭은 매우 좁고 통신 요금은 바이트당 청구되었습니다. 따라서 **최소한의 헤더 오버헤드(최소 2바이트)**와 **배터리 소모 최소화**, **불안정한 연결에서의 발행/구독(Pub/Sub)**이 필수적이었습니다.

이 프로토콜은 IBM의 메시지 큐 제품군(MQSeries)과 연동되었기 때문에 프로토콜 식별 이름이 **`MQIsdp`**(MQ Integrator SCADA Device Protocol)로 명명되었습니다.

### 2. MQTT 3.1 (2010)
IBM이 사유 규격을 개방하면서 2010년 로열티 프리 사양으로 배포한 버전입니다.
- 가변 헤더의 Protocol Name 문자열은 여전히 **`MQIsdp`**(길이 6바이트: `0x00 0x06 'M' 'Q' 'I' 's' 'd' 'p'`)를 사용했습니다.
- Protocol Level 바이트 값은 **`3`**이었습니다.
- 보안을 위한 Username/Password 필드가 추가되었습니다.

### 3. MQTT 3.1.1 (2014, OASIS 및 ISO 표준)
OASIS 컨소시엄에서 표준 규격으로 공식 제정하고, 2016년 ISO/IEC 20922 국제 표준으로 승인된 버전입니다.
- **Protocol Name의 변경**: 역사적인 `MQIsdp` 6바이트 대신 공식 프로토콜 명칭인 **`MQTT`**(길이 4바이트: `0x00 0x04 'M' 'Q' 'T' 'T'`)로 통일되었습니다.
- **Protocol Level**: **`4`**로 지정되었습니다.
- **헤더 절약**: 매 CONNECT 패킷마다 2바이트가 절약되었습니다.
- **Client ID 제약 완화**: 이전의 23자 제한이 풀리고 빈 Client ID(임시 세션)가 허용되었습니다.
- 현재 전 세계 IoT 인프라에서 가장 널리 사용되는 기준 버전입니다.

### 4. MQTT 5.0 (2019, 현대적 클라우드 표준)
현대 클라우드 환경과 대규모 분산 브로커 클러스터의 요구사항을 반영하여 대대적으로 개선된 최신 표준입니다.
- **Protocol Level**: **`5`**
- **세션 분리**: `Clean Session` 플래그 대신 `Clean Start` 플래그와 `Session Expiry Interval`(초 단위 타이머)로 분리되어, 연결이 끊겨도 지정된 시간 동안만 세션을 안전하게 유지할 수 있습니다.
- **정교한 피드백**: 이전 버전은 연결 실패 시 CONNACK에 제한적인 반환 코드(0~5)만 있었고 DISCONNECT나 PUBACK은 오류 코드가 없었습니다. MQTT 5.0은 모든 ACK 및 DISCONNECT에 상세한 `Reason Code`와 사유 문자열(`Reason String`)을 실어 보낼 수 있습니다.
- **User Property**: HTTP 헤더처럼 임의의 키-값 쌍을 패킷에 첨부할 수 있어 트레이싱 ID 전파나 메시지 메타데이터 전달이 가능해졌습니다.
- **클러스터 기능 표준화**: 브로커 구현체마다 제각각이던 공유 구독(`$share/group/topic`), Will 지연 발행(`Will Delay Interval`)이 표준화되었습니다.

---

## 버전별 핵심 차이 비교

| 비교 항목 | MQTT 3.1.0 (구형) | MQTT 3.1.1 (OASIS) | MQTT 5.0 (최신) |
| --- | --- | --- | --- |
| **Protocol Name** | `MQIsdp` (6바이트) | `MQTT` (4바이트) | `MQTT` (4바이트) |
| **Protocol Level** | `3` | `4` | `5` |
| **표준화 기구** | IBM 공개 사양 | OASIS, ISO/IEC 20922 | OASIS |
| **세션 제어 방식** | `Clean Session` (0 또는 1) | `Clean Session` (0 또는 1) | `Clean Start` + `Session Expiry Interval` |
| **오류 보고** | CONNACK 반환 코드(0~5)뿐 | CONNACK 반환 코드(0~5)뿐 | 모든 패킷에 `Reason Code` 및 사유 문자열 지원 |
| **메타데이터 헤더** | 지원 안 함 | 지원 안 함 | `User Property` 지원 (Key-Value) |
| **공유 구독** | 벤더 비표준 확장 | 벤더 비표준 확장 | `$share/{group}/{topic}` 공식 표준화 |
| **흐름 제어** | 없음 | 없음 | `Receive Maximum`, `Topic Alias` 지원 |
| **NaruMQ 지원 여부** | **미지원 (연결 거절)** | **공식 지원 (P0)** | **공식 지원 (P0)** |

---

## NaruMQ의 제품 정책: 왜 MQTT 3.1.0을 지원하지 않는가?

NaruMQ 제품 요구사항 정의서([PRD](../requirements/PRD.md) 제5절 비목표 및 제9.1절 `MQTT-13`)는 MQTT 3.1.0 지원을 명시적으로 제외하고 있습니다.

1. **비목표(Non-goals) 규정**: PRD 제5절은 "MQTT 3.1.0(protocol level 3) 지원" 및 "MQTT 3.1.0 호환 코드를 broker core에 포함하는 기능"을 초기 제품의 명확한 비목표로 선언합니다.
2. **표준 규격 중심의 지원 범위**: NaruMQ v1은 공식 국제 표준인 MQTT 3.1.1(Level 4)과 MQTT 5.0(Level 5)만을 제품 지원 범위와 적합성 매트릭스에 포함합니다.
3. **브로커 코어의 단순성 유지**: 레거시 3.1.0 클라이언트 지원 코드를 브로커 코어에 혼합하지 않으며, 구형 장비 수용이 불가피한 경우 브로커 코어가 아닌 별도의 외부 레거시 게이트웨이/프록시로 격리하여 처리하는 정책을 취합니다.

---

## 연결 처리 정상 및 실패 흐름

### 정상 흐름 (MQTT 3.1.1 / 5.0 연결)
1. **클라이언트 송신**: `Protocol Name: "MQTT"`, `Level: 4` 또는 `5`를 담은 `CONNECT` 패킷을 전송합니다.
2. **브로커 검증**: NaruMQ 리스너가 헤더를 파싱하고 지원 버전임을 확인합니다.
3. **세션 및 인증 확인**: Tenant ID, Client ID, 인증 자격증명을 검증하고 세션 소유권을 획득합니다.
4. **CONNACK 응답**: 성공 CONNACK(3.1.1은 `0x00`, 5.0은 Reason Code `0x00`)을 반환하고 통신을 시작합니다.

### 실패 흐름 (MQTT 3.1.0 `MQIsdp` 접속 시도)

| 실패 지점 | 브로커 동작 | 남는 상태 | 클라이언트 관찰 결과 | 재시도 안전 여부 |
| --- | --- | --- | --- | --- |
| **CONNECT 가변 헤더 검사** | Protocol Name이 `MQIsdp`이거나 Level이 `3`임을 감지 | 세션 객체를 생성하지 않음. 연결 객체 즉시 해제 | CONNACK `0x01` (Unacceptable Protocol Version) 수신 후 TCP 연결 종료 | 비안전. 동일 설정으로 무한 재시도 시 브로커에 의해 연결 즉시 거부됨 |

- **남는 상태**: NaruMQ 브로커 내부에 세션, 타이머, 구독 정보가 일절 남지 않습니다.
- **메트릭 기록**:
  `narumq_mqtt_connect_rejected_total{reason="unsupported_protocol_version", protocol="3.1.0"}` 카운터가 증가합니다.
- **보안 조치**: 원문 패킷의 credential이나 password는 로그에 남기지 않습니다.

---

## 레거시 환경 운영 지침

프로덕션 환경에 구형 MQTT 3.1.0 클라이언트가 존재하는 경우, NaruMQ 전환 전에 다음 절차를 따릅니다([PRD](../requirements/PRD.md) 9.1 참조).

1. **사전 관찰**: 기존 EMQX 등에서 최소 90일간 연결 이벤트의 `proto_name`, `proto_ver`를 수집하여 3.1.0 사용 장비를 식별합니다.
2. **클라이언트 갱신**: 펌웨어 또는 클라이언트 SDK 설정을 MQTT 3.1.1 이상으로 업데이트합니다. (예: Flutter `mqtt_client`의 기본 3.1 설정을 3.1.1로 지정)
3. **독립 레거시 게이트웨이 격리**: 펌웨어 원격 업데이트가 불가능한 장비가 남아 있다면, NaruMQ 브로커 코어를 수정하지 않고 외부에 별도의 경량 레거시 게이트웨이(3.1.0 ↔ 3.1.1 변환 프록시)를 배치해 격리 운영합니다.

---

## 프로젝트에서 찾을 위치

- **요구사항 정의**: `docs/requirements/PRD.md` (섹션 9.1 `MQTT-01`, `MQTT-13`)
- **프로토콜 스펙**: `docs/specs/mqtt-protocol-and-session.md` (섹션 2.1 `Protocol version`)
- **호환성 계약**: `docs/contracts/mqtt-client-compatibility-v1.md`
- **관련 기술 해설**: `docs/explainers/mqtt-delivery-guarantees-and-protocol-flows.md`

---

## 확인 질문

1. 클라이언트 CONNECT 패킷에서 프로토콜 버전을 식별하기 위해 검사해야 하는 두 가지 필드는 무엇인가?
   *(답: 가변 헤더의 Protocol Name 문자열과 Protocol Level 1바이트 정수)*
2. MQTT 3.1.1의 Clean Session과 MQTT 5.0의 Clean Start + Session Expiry Interval의 구조적 차이는 무엇인가?
   *(답: 3.1.1은 연결 수명과 세션 수명이 결합되어 연결이 끊기면 즉시 삭제되거나 영구 보존만 가능했으나, 5.0은 연결 재시작 방식(Clean Start)과 연결 단절 후 세션 보존 유효기간(Session Expiry Interval)이 독립적으로 분리됨)*
3. 구형 센서 장비가 `MQIsdp`로 NaruMQ에 연결할 때 브로커 내부에는 어떤 상태가 남는가?
   *(답: 세션 객체나 구독 정보는 전혀 생성되지 않고 메트릭 카운터만 증가한 뒤 TCP 연결이 종료됨)*

---

## 참고 자료

- [OASIS MQTT Version 3.1.1 Standard (2014)](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
- [ISO/IEC 20922:2016 Information technology — Message Queuing Telemetry Transport (MQTT) v3.1.1](https://www.iso.org/standard/69466.html)
- [OASIS MQTT Version 5.0 Standard (2019)](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [IBM MQTT 3.1 Protocol Specification (2010)](https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html)
