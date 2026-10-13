# NaruMQ Operations Dashboard를 구성하는 방법

Prometheus는 `/metrics`에서 node별 metric(운영 수치)을 정해진 간격으로 읽어 시간에 따른 값을 저장한다. Grafana는 Prometheus 값과 Management API 상태를 datasource(데이터 원본 연결)로 읽어 한 화면에 보여준다. 이 절차는 두 도구를 NaruMQ에 연결한다. Cluster API는 조회만 허용하는 `ReadOnly` API key로 읽는다. Local Docker acceptance에서 설정·수집·Dashboard·권한 경계를 확인했고, 2026-10-13 EKS에서는 stack 배포와 live cluster data frame을 확인했다. Prometheus live scrape는 `X-Api-Key` header 오류로 실패하고 time-series panel은 비어 있어 overall live dashboard acceptance는 미검증이다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | NaruMQ 운영자와 monitoring 담당자 |
| 검증 상태 | Verified (local Docker acceptance 및 EKS stack 배포·Dashboard API provisioning; EKS live scrape/data source integration 미검증) |
| 검증 환경·버전 | Prometheus 3.5.0, Grafana OSS 13.0.2, Infinity datasource 4.1.1, local 3개 NaruMQ process 및 EKS `narumq-eks` |
| 최종 검증일 | 2026-10-13 |
| 관련 기준 문서 | [PRD §9.6](../requirements/PRD.md#96-대시보드), [Management API와 observability §8·§9](../specs/management-api-and-observability.md#8-dashboard-계약), [Management API v1](../contracts/management-api-v1.md), [ADR-0045](../decisions/ADR-0045-operations-dashboard-grafana.md) |

## 1. 대상과 영향

- 대상: Prometheus와 Grafana, 그리고 Management API가 활성화된 NaruMQ cluster.
- 예상 사용자 영향: 조회용 monitoring 경로를 추가한다. MQTT traffic과 cluster state를 변경하지 않는다.
- 현재 범위: Grafana datasource 하나는 NaruMQ cluster 하나를 조회한다. 같은 Prometheus에 여러 cluster를 넣으면 Dashboard query가 cluster를 구분하지 않고 node를 합산한다.
- 작업 허용 시간: Monitoring 설정을 배포할 수 있는 시간에 진행한다. NaruMQ 재시작이 필요하면 별도 운영 절차와 변경 승인을 따른다.

## 2. 전제 조건

- [ ] 대상 cluster의 `clusterId`, node ID와 Management API 주소를 확인했다.
- [ ] Management API용 조회 전용 `ReadOnly` API key를 비밀 저장소에서 발급했다. 이 key는 ClusterAdmin token과 달라야 한다.
- [ ] Grafana가 API key 파일을 읽을 수 있고, Prometheus가 NaruMQ의 `/metrics` endpoint에 접근할 수 있다.
- [ ] Grafana와 Prometheus가 같은 network에서 통신하거나 datasource 주소를 서로 접근 가능한 주소로 바꿀 수 있다.
- [ ] Grafana와 Prometheus의 configuration, target, secret 파일을 read-only mount할 수 있다.
- [ ] Local acceptance를 실행할 때는 Go 1.26.5, Docker Engine과 Linux container image를 실행하는 Docker 환경을 준비했다.

## 3. 절차

- 실행 환경: 배포 플랫폼의 관리 환경. Acceptance 명령은 PowerShell 또는 Go가 동작하는 shell에서 실행한다.
- 실행 위치: 저장소 root. Prometheus와 Grafana의 mount 설정은 해당 배포 repository에서 관리한다. Kubernetes/EKS 배포는 저장소 root에서 Kustomize manifest를 적용한다.
- 바꿔 넣을 값:
  - Prometheus target의 주소는 Prometheus container에서 접근 가능한 Management listener 주소로 바꾼다.
  - `node_id`는 Management API `nodeId`와 같은 decimal string으로 쓴다. 이 Dashboard는 Dynamic cluster를 대상으로 한다. Standalone의 고정 node ID `narumq-node-1`은 이 규칙 대상이 아니다.
  - `cluster_id`는 대상 cluster ID로 바꾼다.
  - `NARUMQ_MANAGEMENT_URL`은 Grafana container에서 접근 가능한 Management API 주소로 설정한다.
  - `NARUMQ_READONLY_API_KEY`와 Prometheus의 API key 파일에는 같은 `ReadOnly` key를 공급한다.
- 최대 대기 시간: Prometheus file service discovery는 target JSON 파일을 30초마다 다시 읽는다. Local E2E acceptance는 target 상태를 최대 3분, Grafana Dashboard provisioning을 최대 4분 동안 확인한다.

1. **Cluster process에 ReadOnly API key 파일을 연결한다.**

   각 node의 cluster configuration `management.apiKeys`에 Dashboard 전용 API key를 등록한다. 모든 node에서 같은 `id`와 key를 사용하고 `ReadOnly` role만 부여한다. `keyFile`은 secret store가 관리하는 파일을 가리킨다.

   다음 JSON은 `management` 설정의 관련 부분만 보인다.

   ```json
   {
     "management": {
      "apiKeys": [
        {
          "id": "dashboard",
          "keyFile": "/run/secrets/narumq-readonly.key",
          "roles": ["ReadOnly"]
        }
      ]
     }
   }
   ```

   Key file이 비어 있거나 ClusterAdmin token 또는 다른 API key와 같으면 process가 시작하지 않는다. 원격 Management listener는 방화벽과 인증 정책으로 제한한다.

   기대 결과: 모든 node process가 비밀 저장소의 같은 ReadOnly key를 사용한다. ReadOnly key의 cluster GET은 `200`, backup POST와 node drain POST는 `403`이어야 한다.

2. **Prometheus target을 실제 cluster에 맞춘다.**

   [`deploy/observability/prometheus/targets/narumq-cluster.json`](../../deploy/observability/prometheus/targets/narumq-cluster.json)의 sample 주소, `cluster_id`, `node_id`를 실제 값으로 바꾼다. 한 node마다 target group 하나를 둔다.

   Prometheus container에는 다음 파일을 지정된 경로에 mount한다.

   | Container 경로 | 저장소 입력 또는 비밀 값 |
   | --- | --- |
   | `/etc/prometheus/prometheus.yml` | `deploy/observability/prometheus/prometheus.yml` |
   | `/etc/prometheus/targets/*.json` | 실제 주소와 label을 넣은 target JSON |
   | `/etc/prometheus/secrets/narumq-readonly.key` | 비밀 저장소가 공급한 ReadOnly API key 파일 |

   설정은 `/metrics`를 15초마다 scrape하고 `X-API-Key` header에 key 파일 값을 사용한다. API key 파일을 저장소에 넣지 않는다.

   기대 결과: Prometheus target에 각 node가 나타나고 상태가 `UP`이다.

3. **Prometheus와 Grafana의 network 주소를 맞춘다.**

   [`deploy/observability/grafana/provisioning/datasources/narumq.yml`](../../deploy/observability/grafana/provisioning/datasources/narumq.yml)은 Prometheus를 `http://prometheus:9090`에서 조회한다. Prometheus service에 `prometheus` DNS alias를 주거나 datasource URL을 실제 service 주소로 바꾼다.

   Prometheus와 Grafana가 NaruMQ node에 접근할 수 있는 network에 연결되어야 한다. Grafana에서 쓰는 `NARUMQ_MANAGEMENT_URL`도 Grafana container 기준의 주소여야 한다. Docker Desktop local test는 `host.docker.internal`을 사용한다.

   기대 결과: Grafana container가 Prometheus service 주소와 NaruMQ Management API 주소에 접근할 수 있다.

4. **Grafana provisioning과 secret을 연결한다.**

   Grafana container에 아래 경로를 mount한다.

   | Container 경로 | 저장소 입력 |
   | --- | --- |
   | `/etc/grafana/provisioning` | `deploy/observability/grafana/provisioning` |
   | `/var/lib/grafana/dashboards/narumq` | `deploy/observability/grafana/dashboards` |

   배포 platform의 secret 기능으로 `NARUMQ_MANAGEMENT_URL`과 `NARUMQ_READONLY_API_KEY`를 Grafana process에 제공한다. Grafana는 Management API datasource를 GET 전용으로 구성하며 key를 `secureJsonData`로 저장한다. Key 값을 command history, manifest, log 또는 저장소 파일에 넣지 않는다.

   기대 결과: Grafana에 Prometheus와 Management API datasource, `NaruMQ Operations` Dashboard가 provision된다.

5. **Kubernetes/EKS에 Dashboard stack을 배포한다.**

   저장소 root에서 다음 명령을 실행한다.

   ```sh
   kubectl apply -k deploy/observability
   ```

   Kustomize는 namespace `narumq-observability`를 적용하고 이 저장소의 Prometheus 설정·target, Grafana provisioning 및 Dashboard 파일로 ConfigMap을 생성한다. Prometheus와 Grafana는 ClusterIP Services로 노출된다.

   기대 결과: `narumq-observability` namespace에 Prometheus와 Grafana Deployment 및 Service가 생성된다. 배포 후 실제 NaruMQ target과 조회 전용 API credential을 환경에 연결해야 scrape와 Management API 조회가 동작한다.

6. **Datasource와 Dashboard를 확인한다.**

   Grafana의 [Dashboard](http://localhost:3000/d/narumq-operations/narumq-operations)를 연다. 운영 URL을 사용하는 경우 `localhost:3000`을 Grafana 주소로 바꾼다.

   `Node` filter는 Prometheus의 `node_id` label을 사용한다. Node load와 Drain 표는 node마다 한 행을 만들고 metric마다 열을 만든다. `Drain node` filter는 drain Metric이 있는 node를 선택한다. `Maximum connection deviation`은 rebalance API의 `maxDeviationConnections`와 같은 값으로 설정한다. ETA는 연결이 실제로 줄어들 때 표시한다.

   기대 결과: Quorum, Leader, node topology, Node load와 Drain progress가 보인다. 각 process의 `/health/ready`는 `200`을 반환해야 한다. Node topology는 조회를 받은 node의 readiness만 보장한다. 원격 node가 `JOINING`으로 표시되면 해당 node의 `/health/ready`를 직접 확인한다. Active drain이 없으면 `Drain 상세`의 panel status에서 `404 Not Found`를 확인한다.

   Traffic이 없으면 message rate와 일부 counter panel이 `No data`로 보일 수 있다. Active drain 기록이 없으면 `Drain 상세` panel은 `No data`를 보인다. Panel의 `Panel status`에서 Management API `404 Not Found`를 확인한다. 이 두 상태는 datasource 오류와 다르다.

## 4. 검증

[검증 기록](../AGENTS.md#검증-기록)에 따라 실제 결과와 성공 조건을 대조한다.

| 관찰 대상 | 성공 조건 | 확인 명령·방법 | 실제 결과 | 실행 증거 위치 |
| --- | --- | --- | --- | --- |
| Static provisioning contract | Dashboard panel, OpenAPI GET path, Metric catalog와 provisioning 설정이 계약과 일치한다. | `go test ./tests/integration -run '^(TestDynamicClusterTenantIsolationPenetrationProcessAcceptance|TestDynamicClusterConnectionRebalanceProcessAcceptance|TestDashboardProvisioningMatchesContracts)$' -count=1 -v -timeout 20m` | 2026-10-10 PASS. | `tests/integration/dashboard_stack_test.go::TestDashboardProvisioningMatchesContracts` |
| Prometheus scrape와 Grafana datasource | 3개 node가 Prometheus target에서 `UP`이고 두 datasource health가 `OK`다. | `NARUMQ_DASHBOARD_E2E=1 NARUMQ_DASHBOARD_E2E_HOLD=90s go test ./tests/integration -run '^TestDashboardStackProcessAcceptance$' -count=1 -v -timeout 20m` | 2026-10-10 PASS, test 228.46s, package 229.503s. | `tests/integration/dashboard_stack_test.go::TestDashboardStackProcessAcceptance` |
| EKS stack 배포와 live Dashboard data | 3 broker pod가 3개 AZ에서 실행되고 Management API datasource가 HTTP `200`과 live cluster frame을 반환한다. Prometheus target과 time-series panel도 실제 data를 반환한다. | EKS에서 `kubectl get deployments,pods,services -n narumq-observability`; Grafana `/api/v1/cluster` datasource frame, Prometheus target 상태와 panel 7 frame 확인 | 2026-10-13 `narumq-eks`에서 broker pod 3개가 3개 AZ에 걸쳐 실행 중이다. Grafana Management API datasource `/api/v1/cluster`는 HTTP `200`을 반환하고 panel 1/2에 live frame(`quorumAvailable=true`, `memberCount=3`, `underReplicated=0`)을 표시했다. 그러나 Prometheus scrape target은 `up=0`이며 `net/http: invalid header field value for "X-Api-Key"` 오류가 났고, time-series metric panel 7은 empty frame을 반환했다. Overall live dashboard acceptance는 open이다. | EKS `narumq-eks`, namespace `narumq-observability`; `deploy/observability/kubernetes/` |
| Dashboard 화면 | Quorum, leader, topology, traffic, node-load와 drain panel이 표시되고 browser error가 없다. | Grafana에서 Dashboard를 열고 첫 화면, `Node 부하`, `Drain` 행을 확인한다. | 2026-10-09 local Docker stack에서 Quorum `정상`, replica 부족 `0`, Leader `1`과 세 node를 확인했다. Grafana `/api/ds/query` frame에서 panel 2는 replica 부족 `0`, panel 4는 Members `3`·quorum `2`·replication factor `3`·write quorum `2`, panel 5는 Ready `1`·Joining `2`, panel 6은 세 node와 원격 `JOINING` readiness reason을 반환했다. Panel 15는 `COMPLETED`, `SAFE_TO_STOP`, remaining `0`, takeover `1`을 반환했다. Node 2는 원격 readiness를 추정하지 않고 `JOINING`으로 표시됐다. `Node=2` 화면에서 Connections, Sent PUBLISH, MQTT 5.0 CONNECT 성공과 거절 panel을 확인했다. Node-scoped query는 node 1의 connection·received PUBLISH rate와 MQTT 3.1.1 성공, node 2의 sent PUBLISH rate와 MQTT 5.0 성공·같은 series의 `not_authorized` 거절을 검증했다. Drain의 node 3은 initial `1`, remaining `0`, takeover `1`, `safeToStop=true`였다. Browser와 console error는 없었다. | `NARUMQ_DASHBOARD_E2E_HOLD=90s` local E2E와 browser visual check. CI 실행 링크와 production deployment 검증은 없다. |

Local Docker E2E는 Prometheus 3.5.0, Grafana OSS 13.0.2와 Infinity datasource 4.1.1 container를 시작하고 종료한다. Production deployment test는 아니다.

Local process acceptance는 세 process의 `/health/ready`가 `200`인지 확인한다. Fixture는 60초 동안 MQTT 3.1.1로 초당 한 건을 publish하고 MQTT 5.0 subscriber에서 메시지를 받는다. 두 protocol의 CONNECT 성공과 잘못된 token의 MQTT 5.0 거절도 metric으로 확인한다. Panels 7–10의 node-scoped query는 node 1과 node 2를 선택한다. Node 1의 connection·received PUBLISH rate, MQTT 3.1.1 성공과 MQTT 5.0 미노출을 확인한다. Node 2의 sent PUBLISH rate, MQTT 5.0 성공과 MQTT 3.1.1 미노출을 확인한다. `not_authorized` 거절 rate는 node 2에 있고 node 1에는 없는지 확인한다. Node 3에 연결한 persistent client를 drain해 `COMPLETED`와 `SAFE_TO_STOP`을 확인한다.

모든 Management API panel은 Grafana backend parser를 쓰고 `json_options`를 설정하지 않는다. E2E는 panel 2, 4, 5, 6, 15를 Grafana `/api/ds/query`로 조회하고 반환 frame의 field와 값을 확인한다. Browser visual check는 실제로 렌더링한 Dashboard를 별도로 확인한다.

저장소 root의 PowerShell에서 다음 명령을 실행한다.

```powershell
$env:NARUMQ_DASHBOARD_E2E = "1"
go test ./tests/integration -run '^TestDashboardStackProcessAcceptance$' -count=1 -v
Remove-Item Env:NARUMQ_DASHBOARD_E2E
```

화면을 확인하려면 test 전에 `$env:NARUMQ_DASHBOARD_E2E_HOLD = "90s"`도 설정한다. Test 출력의 `Grafana dashboard URL`을 열고 90초 안에 화면을 확인한다. 실행이 끝나면 두 environment variable을 지운다.

## 5. 중단 조건

- Prometheus target 하나라도 `DOWN`이면 Dashboard data를 운영에 쓰지 않는다. Target 주소, `node_id`, key file 경로와 network policy를 확인한다.
- Grafana Management API datasource가 `401` 또는 `403`을 반환하면 key와 role 경계를 확인한다. ClusterAdmin token을 Dashboard에 대신 넣지 않는다.
- Grafana가 `prometheus:9090`을 찾지 못하면 network alias 또는 datasource URL을 먼저 수정한다.
- 여러 cluster의 Metric이 같은 Prometheus에 들어가면 설치를 완료로 표시하지 않는다. 이 Dashboard는 현재 cluster filter를 제공하지 않는다.

## 6. 롤백 또는 복구

1. Grafana provisioning과 Dashboard mount를 제거하거나 이전 deployment revision으로 되돌린다.
2. Prometheus target file에서 해당 cluster target을 제거하고 scrape target이 사라졌는지 확인한다.
3. Dashboard가 ReadOnly key를 더 이상 사용하지 않을 때만 key를 회전하거나 폐기한다.
4. 다른 monitoring client가 같은 key를 쓰는지 확인한 뒤 각 node의 `management.apiKeys`에서 `dashboard` 항목을 제거하고 key file을 폐기한다.

## 7. 해결되지 않을 때 전달할 대상

- 전달 대상: NaruMQ on-call 운영자와 monitoring platform 담당자.
- 보존할 증거: Grafana Dashboard UID, panel ID, Prometheus target의 `node_id`·health·last error, HTTP status, 발생 시각.
- 비밀 값 제외 규칙: API key, bearer token, secret file 내용과 private key를 로그·보고서·ticket에 넣지 않는다.
