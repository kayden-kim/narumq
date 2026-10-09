# ADR-0047: MQTT password 인증

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (SEC-05 적용 범위) |
| 결정일 | 2026-10-09 |
| 결정자 | 승인된 SEC-05 작업 계획 |
| 관련 요구사항 | [SEC-05](../specs/tenancy-and-security.md#24-usernamepassword) |
| 관련 스펙·계약 | [Tenant와 security §2.3·§2.4·§9](../specs/tenancy-and-security.md), [Audit event v1](../contracts/audit-event-v1.md) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md), [Broker core](../architectures/broker-core.md) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

MQTT username/password 인증을 지원한다. Password는 고정 profile의 Argon2id PHC hash로 설정에 저장한다. JWT와 password 인증은 하나의 security configuration에서 상호 배타로 둔다. mTLS는 둘 중 한 방식과 함께 사용할 수 있다. `narumq -hash-password`는 stdin에서 password bytes를 읽어 hash만 stdout에 출력한다.

Password hash는 Argon2id version 19, memory 19,456 KiB, time 2, parallelism 1, 16-byte random salt, 32-byte digest를 사용한다. Provider당 동시 Argon2 계산은 최대 4개다. 자세한 외부 동작과 설정 형식은 [Tenant와 security 스펙](../specs/tenancy-and-security.md#24-usernamepassword)이 기준이다.

## 2. 배경

SEC-05는 MQTT username/password 인증을 요구한다. SEC-05 이전의 인증 port는 MQTT Password bytes를 JWT adapter에 전달했고, CONNECT use case는 JWT method를 전제로 감사·Metric을 기록했다. 새 방식은 application port와 composition root를 재사용하면서 인증 방법의 선택, password 저장과 오류 누출을 명확히 해야 한다.

## 3. 결정 요인

1. Password 원문을 설정·저장소·로그·Metric에 남기지 않는다.
2. 알려진 username과 모르는 username의 거절 결과를 같은 외부 응답으로 만든다.
3. 인증 비용과 동시 자원 사용을 고정하고 제한한다.
4. 현재 application-owned authentication port와 principal·session 형식을 유지한다.
5. 설정과 운영 절차를 명확하게 검증할 수 있게 한다.

## 4. 선택 근거

OWASP Password Storage Cheat Sheet의 Argon2id 권장 profile인 19 MiB, 2회, 1 lane을 사용한다. PHC parsing은 이 단일 profile만 허용하여 임의 memory·CPU 비용이 설정에서 선택되지 않게 한다. Random salt와 고정 길이 digest를 사용한다.

Unknown username에도 같은 profile의 Argon2 계산과 constant-time digest 비교를 수행한다. 동시 계산을 4개로 제한하고, slot이 없으면 대기열 대신 일반 인증 실패를 반환한다. 이 정책은 동시 계산 수를 제한하지만 GC 회수 전 heap 상한을 보장하지 않는다.

Password identity는 신뢰된 static configuration의 username binding으로 tenant와 authorization roles를 결정한다. JWT와 password identity가 동시에 선택될 수 없으므로 credential type을 추측하거나 provider 사이에서 fallback하지 않는다. mTLS certificate가 검증되면 기존 certificate 선택 규칙을 적용한다.

기존 principal과 persisted Will/session metadata 형식에는 credential 만료가 필요하다. Password principal에는 `time.UnixMilli(math.MaxInt64)`를 no-expiry 표현으로 사용한다. Zero expiry 허용이나 replicated schema 변경은 하지 않는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| JWT와 password provider를 모두 설정하고 credential 형식을 추측한다 | 두 방식을 동시에 배포할 수 있다 | Provider fallback과 실패 결과가 모호하고 credential downgrade 경로가 생긴다 | 명시적인 단일 credential provider 선택을 우선한다 |
| 가변 Argon2 parameter를 설정별로 허용한다 | 장비에 맞춰 비용을 조절할 수 있다 | 악의적 또는 잘못된 설정이 CPU·memory 비용을 키우고 identity별 비용 차이를 만든다 | 고정 profile과 명시적인 별도 변경을 선택한다 |
| 평문 password 또는 복호화 가능한 password를 저장한다 | 인증 계산이 간단하다 | 설정·저장 파일 유출 시 즉시 원문 credential이 노출된다 | salted one-way hash를 저장한다 |
| Password 만료를 위해 principal·replicated schema를 확장한다 | Credential 유효 기간을 직접 표현할 수 있다 | 저장 형식과 복구 호환성을 확장한다 | SEC-05 범위에서 schema 변경 대신 기존 expiry 표현을 재사용한다 |

## 6. 결과

### 긍정적 결과

- Password 원문은 startup 뒤 provider 상태에 저장하지 않는다.
- Username binding은 설정에서만 결정되며 Client ID와 Will 검증은 기존 CONNECT 흐름을 따른다.
- MQTT 3.1.1과 MQTT 5에서 인증 실패는 동일한 protocol reason을 사용한다.

### 부정적 결과와 비용

- Argon2 계산은 인증 지연과 process memory 사용을 늘린다.
- 동시 slot 포화 요청은 일반 인증 실패로 거절될 수 있다.
- 단일 hash profile은 비용 변경이나 password rotation을 독립적으로 허용하지 않는다.
- 기존 연결 권한 회수와 운영 중 설정 reload는 제공하지 않는다.

### 후속 작업

- Credential 회전·폐기, 기존 연결 재인증과 blocklist는 SEC-07·SEC-12의 별도 범위로 둔다([Tenant와 security §9](../specs/tenancy-and-security.md#9-수용-시험과-범위)).

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Hash 원문이 남지 않고 실제 broker 인증이 동작한다 | `-hash-password` stdin→stdout과 TLS·3-node MQTT process 시험 | 성공·거절 경계 및 config·저장 파일·log·Metric의 원문 부재 | [§9.5](../specs/tenancy-and-security.md#95-sec-05-password-authentication-실행-증거) |
| Unknown·wrong password 응답이 구분되지 않는다 | MQTT 3.1.1·5 socket 시험, Metric·audit 관찰 | 동일한 CONNACK, `invalid_credentials`와 `password` method | [§9.5](../specs/tenancy-and-security.md#95-sec-05-password-authentication-실행-증거) |
| Hash parser와 자원 경계가 고정된다 | adapter unit·race 시험 | 유효 profile 통과, malformed profile 거절, 최대 4개 계산과 slot 반환 | [§9.5](../specs/tenancy-and-security.md#95-sec-05-password-authentication-실행-증거) |

## 8. 재검토 조건

- 운영에서 hash profile의 비용을 바꿔야 할 때 별도 versioned hash/profile migration을 결정한다.
- Credential 회전·폐기, 연결 재인증, 설정 reload 또는 blocklist를 지원할 때 SEC-07·SEC-12의 별도 범위를 채택한다.
