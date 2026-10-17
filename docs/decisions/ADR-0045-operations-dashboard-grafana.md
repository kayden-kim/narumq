# ADR-0045: Grafana와 Prometheus로 운영 Dashboard를 제공한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted |
| 결정일 | 2026-10-09 |
| 결정자 | 사용자 |
| 관련 요구사항 | [UI-01~UI-05·UI-10](../requirements/PRD.md#96-대시보드), [API-01](../requirements/PRD.md#95-metric과-관리-api), [API-06](../requirements/PRD.md#95-metric과-관리-api) |
| 관련 스펙·계약 | [Management API와 observability](../specs/management-api-and-observability.md), [Management API v1](../contracts/management-api-v1.md), [Raft membership v2](../contracts/raft-membership-v2.md) |
| 관련 아키텍처 문서 | [Project structure](../architectures/project-structure.md), [Management HTTP adapter](../architectures/management-http-module.md) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

1. 운영 Dashboard는 Grafana OSS `grafana/grafana-oss:13.0.2`와 Infinity datasource plugin `yesoreyeram-infinity-datasource@4.1.1`을 사용한다. 시계열은 Prometheus `prom/prometheus:v3.5.0`에 저장한다.
2. Grafana database에는 Dashboard 정의만 저장한다. 시계열 원본은 Prometheus에만 둔다.
3. Dashboard는 quorum, node 상태와 drain 상세를 Management API에서 읽는다. 추이, rate와 drain 속도·ETA는 Prometheus에서 계산한다.
4. Grafana와 Prometheus는 ClusterAdmin token 대신 `ReadOnly` API key를 `X-API-Key` header로 사용한다.
5. Prometheus scrape target의 `node_id` label은 Management API의 `nodeId`와 같은 값을 사용한다. Dashboard의 `node` 변수에서 All은 cluster 범위이고, 선택한 ID는 node 범위다. API URL 하나는 한 cluster를 가리킨다. Query는 `cluster_id`로 filter하지 않으므로 하나의 Prometheus datasource에는 한 cluster만 연결한다.
6. Grafana가 Dashboard 사용자 인증을 맡는다. Grafana OIDC는 NaruMQ의 SEC-10(OIDC와 4역할 분리)을 충족하지 않는다.
7. 자체 Web UI를 만들지 않는다. `DESIGN.md`, frontend convention과 별도 screen spec을 만들지 않는다. 화면 구성을 Management API 스펙 §8에서 정의한다.

## 2. 배경

PRD UI-01~UI-05는 cluster topology, quorum·replica 위험, traffic, node별 부하와 drain 진행 상황을 보여야 한다. UI-10은 장기 시계열을 외부 TSDB에 저장하도록 요구한다. 현재 Management API는 상태 snapshot을 제공하고 `/metrics`는 node-local 표본을 제공한다. 따라서 Dashboard는 두 읽기 경계를 함께 사용해야 한다.

## 3. 결정 요인

1. 장기 시계열을 broker process와 Dashboard database에 중복 저장하지 않는다.
2. Dashboard는 기존 Management API와 `/metrics`를 사용하고 broker에 자체 frontend를 추가하지 않는다.
3. Dashboard가 사용하는 자격 증명은 조회 전용이어야 한다.
4. API의 `nodeId`와 Prometheus의 node label을 같은 식별자로 연결한다.

## 4. 선택 근거

Grafana는 API snapshot과 Prometheus series를 한 화면에서 조회한다. Prometheus가 시계열 원본을 보유하므로 Grafana database는 Dashboard 정의만 저장한다. API 조회는 `ReadOnly` key를 사용해 운영 mutation을 허용하지 않는다. Target label을 API node ID와 일치시키면 `node` 변수의 All 선택으로 cluster 범위, 특정 ID 선택으로 node 범위를 조회한다. Management API URL은 한 cluster를 선택하며 `cluster_id` label은 target 구분용 metadata다. Query가 이 label로 filter하지 않으므로 Prometheus datasource마다 한 cluster를 설정한다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| NaruMQ 내장 Web UI | 한 제품에서 상태와 조회 권한을 통합할 수 있다. | Frontend, 접근 제어, 배포 artifact와 유지보수 범위가 커진다. | 승인된 형태는 Grafana와 Prometheus이며 자체 frontend는 만들지 않는다. |
| Grafana만 사용하고 Prometheus를 두지 않음 | Dashboard 구성요소 수가 적다. | 자체 외부 시계열 원본이 없어 장기 시계열 요구를 충족하지 못한다. | UI-10과 맞지 않아 제외한다. |

## 6. 결과

### 긍정적 결과

- Management API의 snapshot과 Prometheus의 시계열을 함께 조회한다.
- Broker에는 frontend runtime을 추가하지 않는다.
- 조회 전용 key로 Grafana와 Prometheus를 연결한다.

### 부정적 결과와 비용

- Grafana, Infinity plugin과 Prometheus의 version을 운영자가 관리해야 한다.
- Dashboard에 사용자 인증을 제공해도 NaruMQ의 OIDC와 4역할 분리는 구현되지 않는다.
- Prometheus datasource마다 한 cluster를 연결해야 한다. 현재 Dashboard는 `cluster_id`를 filter하지 않는다.

### 후속 작업

- 완료: Dashboard provisioning, panel contract, ReadOnly key와 운영 절차를 기준 문서 및 배포 artifact에 반영했다. [Management API 스펙 §8·§9](../specs/management-api-and-observability.md#8-dashboard-계약)과 [Operations Dashboard how-to](../how-to/operations-dashboard.md)를 참조한다.
- 완료: UI-01~UI-05·UI-10의 static·process acceptance를 Management API 스펙 §9에 연결했다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| Dashboard query가 API와 Metric contract를 따른다. | `tests/integration/dashboard_stack_test.go::TestDashboardProvisioningMatchesContracts` | 모든 panel source, API GET path와 Metric family가 기준 문서에 연결된다. | 실행 뒤 [Management API 검증 기록](../verification/management-api-and-observability.md)에 기록한다. |
| Provisioned stack이 세 node의 API와 metric을 읽는다. | `tests/integration/dashboard_stack_test.go::TestDashboardStackProcessAcceptance` | Opt-in Docker E2E에서 대상이 up이고 dashboard query가 기대한 상태를 반환한다. | 실행 환경과 로그 경로를 [Management API 검증 기록](../verification/management-api-and-observability.md)에 기록한다. |

## 8. 재검토 조건

- 자체 Web UI가 필요한 별도 요구사항과 유지보수 소유자가 승인되는 경우
- Dashboard용 사용자 인증이 NaruMQ의 role·tenant 정책과 통합되어야 하는 경우
- Prometheus 이외의 승인된 시계열 저장 정책이 생기는 경우

## 9. 위임된 P0/P1 범위 (2026-10-14)

위임된 범위 결정에 따라 기존 P0 읽기 전용 Dashboard를 유지한다. P1 화면·API·tenant 위임 기능은 출시 후 작업으로 남긴다. 이 결정은 현재 release 범위를 넓히거나 frontend 변경을 승인하지 않는다.
