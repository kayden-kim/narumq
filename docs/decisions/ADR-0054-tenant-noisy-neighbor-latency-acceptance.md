# ADR-0054: TEN-09 noisy-neighbor p99 수용 기준

| 항목 | 내용 |
| --- | --- |
| 상태 | `Accepted` |
| 결정일 | 2026-10-11 |
| 결정자 | 사용자 |
| 관련 요구사항 | [PRD TEN-09·TEN-07](../requirements/PRD.md) |
| 관련 스펙·계약 | [Tenant와 security §5.3](../specs/tenancy-and-security.md#53-noisy-neighbor-완화ten-09) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md#verification-and-integration-test-entrypoints), [Threat model](../architectures/threat-model.md) |
| 관련 ADR | [ADR-0034](ADR-0034-node-local-tenant-connection-and-traffic-limits.md), [ADR-0042](ADR-0042-p0-requirement-traceability-gate.md), [ADR-0064](ADR-0064-ci-gate-logic-only.md)(이 시험을 기본 gate 밖의 opt-in 수용 시험으로 옮긴다. 합격 기준은 유지한다) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

TEN-09의 local 수용 기준은 noisy 구간 victim의 QoS 1 PUBLISH p99가 같은 시험 실행의 baseline p99보다 1.5배를 넘지 않는 것이다.

1. **합격 기준.** 같은 실행에서 `2 × noisyP99 ≤ 3 × baselineP99`여야 한다. 비교는 정수 `time.Duration`으로 수행한다. p99는 nearest-rank 방식으로 계산한다. 표본을 오름차순 정렬한 뒤 index `ceil(0.99 × n) − 1`의 값을 사용한다.
2. **지연 측정.** Victim publisher가 QoS 1 PUBLISH를 쓰기 직전부터 victim subscriber가 그 PUBLISH를 읽을 때까지 측정한다.
3. **시험 profile.** 실제 broker process 세 개로 구성한 Dynamic cluster에서 MQTT 3.1.1을 사용한다. 모든 MQTT client는 node 1에 연결한다.
   - `tenant-victim`은 publisher 한 개가 `victim/latency`에 QoS 1 PUBLISH를 20ms 간격(50 msg/s)으로 보낸다. Subscriber 한 개가 같은 topic을 QoS 1로 구독한다.
   - `tenant-noisy`는 publisher 연결 네 개로 `noisy/load/<n>`에 QoS 1 PUBLISH와 256-byte payload를 보낸다. 각 연결은 8ms 간격(125 msg/s)으로 전송하며, 연결별 in-flight 한도는 16개다. 합계 목표는 500 msg/s다. Noisy subscriber 한 개는 `noisy/#`을 QoS 0으로 구독한다.
   - `tenantLimits.maxWaitMillis`는 1000, `tenantLimits.default.messagesPerSecond`는 200, `tenantLimits.tenants`의 `tenant-noisy.messagesPerSecond`는 50으로 설정한다.
4. **표본.** 처음 victim 측정 50개는 버린다. 이어서 5 round를 수행한다. Round마다 baseline 100개를 측정하고 noisy 부하를 시작한다. 1초 안정화 뒤 noisy 구간 100개를 측정한다. Noisy 부하를 멈추고 publisher in-flight가 0이 될 때까지 기다린 뒤 500ms 휴지한다. 각 구간은 총 500개 표본을 만든다.
5. **완화 전제.** TEN-09의 p99 완화는 운영자가 TEN-07 rate 한도를 설정한 경우에만 보장한다. 한도를 설정하지 않은 tenant에는 지연을 보장하지 않는다.
6. **범위.** 이 결정은 local 수용 기준만 정한다. Production tenant 수와 부하 편차, 절대 지연 SLO는 정하지 않는다. 이 값은 §1.9의 성능 합격선(`SPEC-TBD-08`)에서 계속 추적한다.

## 2. 배경

TEN-06과 TEN-07은 tenant별 연결 상태와 PUBLISH rate 제한을 제공한다. 기존 tenant limit 시험은 제한 동작과 다른 tenant의 독립성을 확인하지만, 공유 자원 부하에서 victim 지연이 어떤 기준을 충족해야 하는지는 정하지 않았다. PRD §18 질문 8의 production tenant 수와 최대 부하 편차도 아직 확정되지 않았다.

## 3. 결정 요인

1. 한 번의 같은 실행 안에서 환경 변동을 공유하는 baseline과 noisy 구간을 비교해야 한다.
2. p99 계산과 경계 비교를 재현할 수 있어야 한다.
3. 시험은 기존 TEN-07 tenant rate limit을 사용하고 broker 정책을 변경하지 않아야 한다.
4. Local acceptance와 production workload·절대 SLO를 구분해야 한다.

## 4. 선택 근거

같은 process 실행에서 baseline과 noisy 구간을 번갈아 측정하면 서로 다른 실행의 host 부하를 비교 기준에 섞지 않는다. 1.5배 상대 기준은 tenant 부하의 영향을 정량화하면서 production 부하 수치가 정해지기 전에 실행 가능한 local acceptance를 제공한다. 정수 duration 비교는 부동소수점 경계 오차를 피한다.

ADR-0034의 node-local token bucket은 설정한 tenant별 PUBLISH rate를 제한하고 초과 시 bounded wait를 적용한다. 이 ADR은 그 기존 동작이 실제 3-node process에서 victim p99에 미치는 영향을 검증한다. Broker 동작이나 한도 값을 변경하지 않는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| p99 수용 기준을 두지 않는다. | 구현과 검증이 단순하다. | TEN-09 합격 여부를 판단할 수 없다. | P0 요구사항의 수용 기준을 시험에 연결해야 한다. |
| 절대 지연 상한을 둔다. | 지연 목표를 직접 표현한다. | Production workload와 배포 자원 profile이 확정되지 않아 임의 값이 된다. | Production 절대 SLO는 `SPEC-TBD-08`에서 정한다. |
| 동일 실행 baseline 대비 1.5배를 적용한다. | Local에서 반복 측정할 수 있고 host 변동을 함께 반영한다. | 상대 기준이므로 baseline 자체가 느려도 통과할 수 있다. Production SLO는 별도로 필요하다. | local acceptance에만 적용하고 production 절대 SLO와 분리한다. |

## 6. 결과

### 긍정적 결과

- TEN-09를 실제 3-node process 시험으로 정량 판정할 수 있다.
- Baseline과 noisy latency를 같은 실행에서 비교한다.
- 기존 tenant별 token bucket의 제한이 실제 부하에 적용되는지도 함께 확인한다.

### 부정적 결과와 비용

- 1.5배 기준은 절대 지연을 보장하지 않는다.
- 모든 client가 node 1에 연결되는 local profile은 production tenant 수·부하 편차·다중 ingress 분포를 대표하지 않는다.

### 후속 작업

- 시험 결과와 traceability를 Tenant와 security 스펙에 기록한다.
- Production workload와 절대 수치 기준은 `SPEC-TBD-08`에서 계속 추적한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| 같은 실행의 baseline 대비 noisy p99를 판정한다. | `TestDynamicClusterTenantNoisyNeighborProcessAcceptance`를 5회 실행한다. | 매 실행 `2 × noisyP99 ≤ 3 × baselineP99` | [Tenant와 security 검증 기록 §9](../verification/tenancy-and-security.md#9-ten-09-noisy-neighbor-실행-기록) |
| Noisy tenant 부하가 설정된 token bucket을 우회하지 않는다. | Noisy subscriber의 수신 시각과 각 round 경계를 비교한다. | 각 round 수신 수는 50 이상이며 `50 + ceil(50 × roundSeconds)` 이하이고 noisy connection error는 없다. | 같은 실행 기록을 검증 기록 §9에 둔다. |

## 8. 재검토 조건

- PRD §18 질문 8에 답이 오거나 production workload가 정해지면 local profile과 production 기준의 관계를 다시 검토한다.
- 시험 결과가 1.5배 기준을 통과하지 못하면 기준을 바꾸지 않고 측정 결과와 broker 원인을 먼저 조사한다.
