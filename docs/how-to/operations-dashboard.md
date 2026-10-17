# NaruMQ Operations Dashboard를 구성하는 방법

Prometheus는 `/metrics`에서 node별 metric(운영 수치)을 정해진 간격으로 읽어 시간에 따른 값을 저장한다. Grafana는 Prometheus 값과 Management API 상태를 함께 보여준다. Cluster API에는 조회 전용 `ReadOnly` API key를 사용한다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | NaruMQ 운영자와 monitoring 담당자 |
| 검증 상태 | 제한 범위 검증; local Docker 및 과거 EKS 실행의 결과이며 현재 배포·production 부하·보안 운영 승인은 아니다. |
| 절차 환경·버전 | Prometheus 3.5.0, Grafana OSS 13.0.2, Infinity datasource 4.1.1 |
| 관련 기준 문서 | [PRD §9.6](../requirements/PRD.md#96-대시보드), [Management API와 observability §8·§9](../specs/management-api-and-observability.md#8-dashboard-계약), [Management API v1](../contracts/management-api-v1.md), [ADR-0045](../decisions/ADR-0045-operations-dashboard-grafana.md) |

기존 EKS 시험환경과 Dashboard는 폐기했다. 아래 주소·node ID·credential은 새로 승인된 대상 환경의 값으로 설정한다.

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
- [ ] 운영 배포에는 Prometheus 영속 저장과 Grafana 운영용 인증을 별도로 준비했다. 저장소 manifest의 Prometheus `emptyDir`는 Pod 재배포 뒤 시계열을 보존하지 않는다.

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

   Key는 HTTP header에 넣을 수 있는 문자열로 발급한다. 32-byte 난수를 64자리 16진수 문자열로 바꾸는 방식을 권장한다. 난수의 원본 binary bytes를 key 파일이나 Kubernetes Secret에 그대로 넣지 않는다. Broker, Prometheus 파일과 Grafana 환경 변수에는 줄바꿈 없는 같은 문자열을 공급한다. Kubernetes Secret의 `data`에는 이 문자열을 base64로 인코딩한다. Base64는 저장 형식이며 API key 자체를 바꾸는 규칙이 아니다.

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

   ConfigMap 이름은 고정되어 있고 일부 파일은 `subPath`로 mount된다. 파일이나 Secret을 바꾼 뒤에는 두 Deployment를 재시작한다. Grafana의 Secret 환경 변수도 재시작해야 바뀐다.

   ```sh
   kubectl rollout restart deployment/prometheus deployment/grafana -n narumq-observability
   kubectl rollout status deployment/prometheus -n narumq-observability --timeout=5m
   kubectl rollout status deployment/grafana -n narumq-observability --timeout=5m
   ```

   Broker는 API key 파일을 시작할 때 읽는다. Broker key를 바꾸면 node별 재시작도 필요하다. EKS StatefulSet은 `OnDelete`이므로 Deployment 재시작 명령만으로 broker pod가 바뀌지 않는다. 기존 drain 절차로 한 pod만 교체하고, 전체 3개 pod가 Ready로 돌아온 뒤 다음 pod를 교체한다. Admin token에 제어 문자가 있으면 drain 요청도 실패할 수 있으므로 재시작 전에 token 형식을 확인한다. Secret 원문을 출력해 확인하지 않는다.

6. **Datasource와 Dashboard를 확인한다.**

   Grafana의 [Dashboard](http://localhost:3000/d/narumq-operations/narumq-operations)를 연다. 운영 URL을 사용하는 경우 `localhost:3000`을 Grafana 주소로 바꾼다.

   `Node` filter는 Prometheus의 `node_id` label을 사용한다. Node load와 Drain 표는 node마다 한 행을 만들고 metric마다 열을 만든다. `Drain node` filter는 drain Metric이 있는 node를 선택한다. `Maximum connection deviation`은 rebalance API의 `maxDeviationConnections`와 같은 값으로 설정한다. ETA는 연결이 실제로 줄어들 때 표시한다.

   Node topology는 `membershipPhase`, `queryStatus`, `queryErrorCode`, `observedAt`과 대상 node가 직접 측정한 nullable 상태를 표시한다. `queryStatus=OK`, `state=READY`, `ready=true`이면 조회한 node가 준비된 상태다. `queryStatus`가 `UNREACHABLE`, `UNSUPPORTED`, `RETIRED` 또는 `ERROR`이면 측정값을 추정하지 않고 빈 값으로 표시한다. 이는 node가 실제로 준비되지 않았다는 뜻이 아니다. `queryStatus=OK`인데 `ready=false`이면 대상 node가 준비되지 않은 실제 상태다. `readinessReason`과 대상 node의 `/health/ready`를 확인한다.

   기대 결과: Quorum, Leader, node topology, Node load와 Drain progress가 보인다. 정상 cluster에서는 각 member의 query status가 `OK`이고 준비된 node의 `state`와 `ready`가 각각 `READY`와 `true`다. 조회가 실패하면 `queryStatus`와 `queryErrorCode`를 먼저 확인한다. Active drain이 없으면 `Drain 상세`의 panel status에서 `404 Not Found`를 확인한다.

   Traffic이 없으면 message rate와 일부 counter panel이 `No data`로 보일 수 있다. Active drain 기록이 없으면 `Drain 상세` panel은 `No data`를 보인다. Panel의 `Panel status`에서 Management API `404 Not Found`를 확인한다. 이 두 상태는 datasource 오류와 다르다.

## 4. 검증

[검증 기록](../AGENTS.md#검증-기록)에 따라 아래 조건과 실제 결과를 대조한다. 과거 통과는 현재 코드의 통과가 아니며, 현재 연결·실행 결과는 [시나리오 보고서](../../.artifacts/verification/scenarios/report.md)를 확인한다.

| 관찰 대상 | 성공 조건 | 확인 명령·방법 |
| --- | --- | --- |
| Static provisioning contract | Dashboard panel, OpenAPI GET path, Metric catalog와 provisioning 설정이 계약과 일치한다. | `go test ./tests/integration -run '^TestDashboardProvisioningMatchesContracts$' -count=1 -v` |
| Prometheus scrape와 Grafana datasource | 3개 node가 Prometheus target에서 `UP`이고 두 datasource health가 `OK`다. | `NARUMQ_DASHBOARD_E2E=1 NARUMQ_DASHBOARD_E2E_HOLD=90s go test ./tests/integration -run '^TestDashboardStackProcessAcceptance$' -count=1 -v -timeout 20m` |
| EKS stack 배포와 live data | 3 broker pod가 3개 AZ에서 Ready이고, 두 datasource와 live API·시계열 frame이 정상이다. | Pod·node 조회, Prometheus `/api/v1/targets`·`up` query, Grafana datasource health·`/api/ds/query` |
| Dashboard 화면 | Quorum, leader, topology, traffic, node-load와 drain panel이 표시되고 browser error가 없다. | Dashboard 첫 화면·`Node 부하`·`Drain` 행 확인. 유휴 상태의 `No data`는 traffic 성공 증거가 아니다. |

Local 실행 결과는 [Dashboard 검증 기록](../verification/management-api-and-observability.md), EKS의 환경·명령·확인 범위는 아래 실행 기록을 따른다.

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

### EKS 재배포 실행 기록 (2026-10-14)

- 대상: 당시 작업 트리의 observability 변경을 EKS `narumq-eks`의 3-AZ ARM64 broker와 Prometheus 3.5.0·Grafana OSS 13.0.2·Infinity 4.1.1에 배포했다. 이 환경은 폐기됐으며 현재 접속 대상이 아니다.
- 확인 방법: `kubectl apply -k deploy/kubernetes/eks-ec2-ha` 뒤 broker를 한 개씩 교체했다. Monitoring에는 `kubectl apply -k deploy/observability`, 두 Deployment의 `rollout restart`·`rollout status --timeout=5m`를 실행했다. Prometheus target·`up`, Grafana datasource health·`/api/ds/query`와 browser 화면을 대조했다.
- 대상 image digest: `sha256:09f77ae1e4d2277fb96192abaafbf3e2d1717683746ff8e6acc17efc9ccfa916`. 이 image는 현재 필수 release build argument 도입 전 산출물이다. 새 image는 [개발 가이드](development-guide.md#broker-release-image-metadata)를 따른다.
- 결과: broker 3개 `OK/READY`, quorum 정상·replica 부족 `0`, Prometheus target 3개 `UP`, datasource 2개 `OK`, topology와 panel 7 시계열 frame을 확인했다. Browser page error는 없었고 ReadOnly drain POST는 `403`이었다.
- 실패와 경계: binary API key·consumer 불일치로 최초 scrape가 실패했다. 문자열 key를 일치시키고 재배포한 뒤 통과했다. 초기 인증 복구의 첫 Pod 교체는 drain gate 증거가 아니다. 초기 startup timeout도 통과로 바꾸지 않는다. 후속 hook 실행은 `COMPLETED`·`safeToStop=true`와 동일 operation 재사용을 확인했으며, 상세 수용은 [Drain 검증 기록](../verification/drain-and-rolling-update.md)에 둔다.
- 미검증: MQTT traffic이 없어 message rate의 `No data`는 traffic 수용 증거가 아니다. 운영 최대 부하·장애 주입·100회 rolling·production 보안과 시계열 보존 정책은 이 실행 범위 밖이다.
- 정리 증거: `.artifacts/verification/eks-final-20261014T221024Z/post-terraform-direct-verification.json`과 `preservation.json`. 시험환경은 폐기하고 설정·인증 자료는 암호화 보존했다.


## 5. 중단 조건

- Prometheus target 하나라도 `DOWN`이면 Dashboard data를 운영에 쓰지 않는다. Target 주소, `node_id`, key file 경로와 network policy를 확인한다.
- Prometheus가 `net/http: invalid header field value for "X-Api-Key"`를 보고하면 key 파일에 제어 문자가 있는지 확인한다. 길이와 형식 판정만 출력한다. Binary key를 문자열로 다시 발급한 뒤 broker·Prometheus·Grafana의 key를 함께 교체한다. Header 오류가 없어졌는데 `401`이 나오면 세 경로의 key 일치 여부와 broker 재시작 상태를 확인한다.
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
