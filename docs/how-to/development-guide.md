# NaruMQ 로컬 개발 환경을 구성하고 검증하는 방법

이 절차는 NaruMQ 브로커 코어 및 어댑터 코드를 개발·수정하고, 클린 아키텍처 계층 규칙과 코딩 컨벤션을 준수하며, 단위 테스트, 동시성 레이스 검출, 퍼즈 테스트, 아키텍처 규칙 검사, 규격 추적성 검사, 커버리지 정책 및 뮤테이션 테스트를 포함한 종합 검증 도구 체인(`tools/verify`)을 실행하여 변경 사항을 안전하게 검증하는 방법을 안내한다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | NaruMQ 브로커 코어와 어댑터를 개발하고 수정·기여하려는 백엔드 소프트웨어 엔지니어 |
| 검증 상태 | Draft; 전체 local coverage는 실패 상태이며 수동 로컬 broker smoke는 미실행. 과거 실행 결과는 §4의 검증 기록을 따른다. |
| 절차 환경·버전 | Windows/amd64, Go 1.26.5 |
| 관련 기준 문서 | [Go 코딩 컨벤션](../conventions/go-coding-conventions.md), [Project structure의 코드 지도](../architectures/project-structure.md), [Broker core architecture](../architectures/broker-core.md), [Performance와 verification 스펙](../specs/performance-and-verification.md) |

## 1. 대상과 영향

- 대상: NaruMQ 전체 소스 코드 저장소(`cmd/`, `internal/`, `tests/`, `tools/`).
- 개발 범위:
  - **도메인 계층 (`internal/domain/`)**: 순수 MQTT 비즈니스 불변식과 상태 머신 (외부 프레임워크 및 I/O import 엄격 금지).
  - **애플리케이션 계층 (`internal/application/`)**: 유스케이스 흐름 조율 및 애플리케이션 포트(인터페이스), 경계 DTO.
  - **어댑터 계층 (`internal/adapters/`)**: TCP/TLS 소켓, 관리 HTTP, Raft 합의, 인메모리/디스크 스토리지 등의 구체 I/O 구현체.
  - **조립 루트 (`cmd/narumq/`)**: CLI 옵션 파싱, 어댑터 생성 및 유스케이스 의존성 주입(Composition Root).
  - **검증 도구 (`tools/verify/`)**: 아키텍처, 커버리지, 규격 추적성, 뮤테이션 등을 검증하는 독립 실행 도구.
- **실행 모드의 검증 경계:**
  - 개발용 standalone 리스너(`cmd/narumq/main.go`)는 인메모리 세션/큐/retained 저장소를 사용하며 `qos0_only`로 동작한다. 영속적 세션 복구나 분산 HA 보장은 standalone 단일 프로세스의 범위가 아니다.
- 개발 환경의 안전성: 개발 및 검증 작업은 로컬 워크스페이스에서 격리되어 수행되므로 운영 시스템에 직접적인 영향을 주지 않는다.

## 2. 전제 조건

- [ ] Go 1.26.5 이상의 Go 공식 툴체인이 설치되어 있다. (`go.mod` 파일의 요구 버전 기준)
- [ ] Git 클라이언트가 설치되어 저장소 최신 브랜치를 체크아웃했다.
- [ ] 코드 작성 전 [Go 코딩 컨벤션](../conventions/go-coding-conventions.md)과 [Broker core architecture](../architectures/broker-core.md)를 읽고 숙지했다.
- [ ] 신규 기능이나 동작 변경 시, 관련 [시스템 스펙](../specs/)과 [경계 계약](../contracts/)을 단일 기준(SSOT)으로 삼는다.
- [ ] 비밀 값(개인키, API 토큰 등)을 테스트 파일이나 커밋에 포함하지 않는다.

## 3. 절차

모든 작업은 저장소 루트 디렉터리(`narumq`)에서 실행한다.

### 1단계: 개발 환경 설정 및 모듈 의존성 검증

저장소를 준비하고 정의된 Go 모듈 의존성을 동기화한다.

```sh
# 의존성 모듈 다운로드
go mod download

# 다운로드된 모듈의 해시 무결성 검증
go mod verify
```

기대 결과: `all modules verified`가 출력되며 오류 없이 완료된다.

### 2단계: 아키텍처 계층 원칙 이해 및 코드 작성

NaruMQ는 **클린 아키텍처(Clean Architecture / Ports and Adapters)** 패턴을 철저히 준수한다. 코드를 수정하거나 추가할 때 다음 의존성 규칙을 반드시 지켜야 한다.

```text
[Composition Root: cmd/narumq]
        │                │
        ▼                ▼
[internal/adapters] ──> [internal/application] ──> [internal/domain]
```

1. **`internal/domain` (도메인 정책)**
   - MQTT 패킷 검증, 세션 상태 전이, 토픽 라우팅 매칭, 테넌트 불변식을 소유한다.
   - **절대 규칙:** 표준 라이브러리(`net`, `os` 제외, 순수 로직 관련 라이브러리 위주) 외에 외부 라이브러리나 상위 패키지(`application`, `adapters`)를 일절 import하지 않는다.
2. **`internal/application` (유스케이스 및 포트)**
   - `ConnectContext`, `Publish`, `Subscribe` 등 비즈니스 흐름을 조율한다.
   - 외부 I/O가 필요한 경우 인터페이스(포트, 예: `SessionRepository`, `Clock`)를 직접 정의하여 소유한다.
   - **절대 규칙:** 구체 어댑터(`internal/adapters`)를 import하지 않는다.
3. **`internal/adapters` (인프라 어댑터)**
   - 네트워크 통신, HTTP 핸들러, etcd/raft 연동, 메모리 캐시 등 I/O를 수행하며 애플리케이션의 포트를 구현한다.
   - 외부 데이터 타입을 도메인/애플리케이션 DTO로 변환하여 전달한다.
4. **`cmd/narumq` (Composition Root)**
   - 설정을 읽고 구체 어댑터를 생성하여 유스케이스 포트에 주입(Dependency Injection)하는 유일한 장소다.


#### Broker release image metadata

The repository-root `Dockerfile` requires both build arguments; there is no default release identity. Every build pipeline or manual build must supply the release version and the full canonical, sorted feature list used by the broker binary:

```sh
docker build \
  --build-arg "NARUMQ_RELEASE_VERSION=${RELEASE_VERSION}" \
  --build-arg "NARUMQ_FEATURES=${NARUMQ_FEATURES}" \
  --tag "${BROKER_IMAGE}" .
```

The same values are validated by `tools/release-metadata`, linked into the executable, and written to OCI config labels `org.opencontainers.image.version` and `io.narumq.features`. The environment values must come from the release artifact/build record; do not substitute a locally invented release identity for a published N/N-1 qualification. The separate `tests/benchmark/Dockerfile` and compatibility/chaos Dockerfiles are independent test images and do not call the root Dockerfile.

For a cross-platform registry artifact, use the same root Dockerfile and explicit arguments for the target platform. Publish a task-unique tag, then inspect the immutable platform-manifest digest and confirm the OCI labels against those build inputs before deploying the digest:

```sh
docker buildx build --platform linux/arm64 --provenance=false \
  --build-arg TARGETOS=linux --build-arg TARGETARCH=arm64 \
  --build-arg "NARUMQ_RELEASE_VERSION=${RELEASE_VERSION}" \
  --build-arg "NARUMQ_FEATURES=${NARUMQ_FEATURES}" \
  --tag "${BROKER_IMAGE_TAG}" --push .
```

#### 격리된 EKS 롤아웃 컨트롤러 인수 테스트 번들 (준비 전용)

이는 합성 fixture 전용 절차이며 정식 릴리스가 아니다. 별도 실행 승인과 fixture 담당자의 인계 없이 image를 push하거나 배포하지 않는다. 기존 시험환경은 폐기했으므로 재시험에는 새 격리 fixture가 필요하다. 과거 실행은 다음 인프라 실행의 승인이 아니며, 수용 범위는 [Drain 검증 기록](../verification/drain-and-rolling-update.md)을 따른다. 기존 공유 image는 보존하고, `narumq` ECR 저장소에 UTC 타임스탬프와 임의 접미사가 포함된 작업 전용 run ID 태그를 사용한다.

컨트롤러: `rollout-controller-dep11-sec11-${RUN_ID}`.
브로커 대상: `rollout-target-0.0.1-${RUN_ID}`.

기존 후보 저장소는 `809446814769.dkr.ecr.ap-northeast-2.amazonaws.com/narumq`이며, 롤아웃 Deployment의 허용 registry 정책과 기존 이미지 기록에 지정되어 있습니다. 게시 전에 읽기 전용으로 저장소를 확인하고 정확히 지정한 두 태그가 아직 사용되지 않았는지 검증하세요:

```powershell
$AwsProfile = 'kayden-sandbox-admin'
$RunId = "$([DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ'))-$([guid]::NewGuid().ToString('N').Substring(0,8))"
$Repository = '809446814769.dkr.ecr.ap-northeast-2.amazonaws.com/narumq'
$ControllerTag = "rollout-controller-dep11-sec11-$RunId"
$BrokerTag = "rollout-target-0.0.1-$RunId"
if ((aws sts get-caller-identity --profile $AwsProfile --region ap-northeast-2 --query Account --output text) -ne '809446814769') { throw 'AWS profile account mismatch' }
aws ecr describe-repositories --profile $AwsProfile --region ap-northeast-2 --repository-names narumq --query 'repositories[].{uri:repositoryUri,tagMutability:imageTagMutability,scanOnPush:imageScanningConfiguration.scanOnPush}' --output json
aws ecr describe-images --profile $AwsProfile --region ap-northeast-2 --repository-name narumq --image-ids "imageTag=$ControllerTag" --output json
aws ecr describe-images --profile $AwsProfile --region ap-northeast-2 --repository-name narumq --image-ids "imageTag=$BrokerTag" --output json
```

계정 `809446814769`와 `kayden-sandbox-admin` profile로 읽기 전용 확인을 완료했습니다. 이 기존 공유 저장소의 태그 변경 가능성은 `MUTABLE`이며, scan-on-push는 비활성화되어 있고, 암호화는 AES256입니다. 고유 run ID 태그를 사용할 때만 후보로 삼으세요. 위의 정확한 태그 충돌 확인은 승인된 push 직전에 반드시 수행해야 합니다. 둘 중 하나라도 이미 있거나 저장소를 안전하게 사용할 수 없으면 다른 저장소를 만들거나 공유 저장소 정책을 바꾸지 말고 중단하세요. 작업 전용 저장소를 만들 계획은 없습니다.

인계 이후에만 기존 저장소에 Docker 인증을 하고 서로 다른 `linux/arm64` manifest 두 개를 빌드하세요. `--provenance=false`를 지정하면 각 단일 플랫폼 빌드를 해당 플랫폼 manifest digest로 식별할 수 있습니다.

```powershell
$Registry = $Repository.Split('/')[0]
aws ecr get-login-password --profile $AwsProfile --region ap-northeast-2 | docker login --username AWS --password-stdin $Registry
```

```powershell
$Repository = '809446814769.dkr.ecr.ap-northeast-2.amazonaws.com/narumq'
$Features = 'client-blocklist,cluster-observability-v1,cluster-policy,cluster-policy-tenant-quotas,connection-rebalance,credential-revocation,proposal-recovery,session-metadata,shared-subscription-reroute,snapshot-stream'
$ControllerVersion = "1.0.0-test-only-$RunId"
New-Item -ItemType Directory -Force -Path '.artifacts/verification' | Out-Null

docker buildx build --platform linux/arm64 --provenance=false `
  --build-arg TARGETOS=linux --build-arg TARGETARCH=arm64 `
  --build-arg "NARUMQ_CONTROLLER_VERSION=$ControllerVersion" `
  --metadata-file ".artifacts/verification/$RunId-controller-build.json" `
  --tag "$Repository`:$ControllerTag" --push `
  --file deploy/kubernetes/rollout-controller/Dockerfile .

docker buildx build --platform linux/arm64 --provenance=false `
  --build-arg TARGETOS=linux --build-arg TARGETARCH=arm64 `
  --build-arg NARUMQ_RELEASE_VERSION=0.0.1 `
  --build-arg "NARUMQ_FEATURES=$Features" `
  --metadata-file ".artifacts/verification/$RunId-broker-build.json" `
  --tag "$Repository`:$BrokerTag" --push --file Dockerfile .
```

push한 태그에서 각 immutable manifest digest를 확인해 기록한 뒤, OCI config blob만 검사하세요 (filesystem layer를 다운로드하거나 풀지 마세요). `crane config` 결과는 `os=linux`, `architecture=arm64`여야 합니다. 브로커의 `org.opencontainers.image.version`은 정확히 `0.0.1`이어야 하며 `io.narumq.features`는 `$Features`와 일치해야 합니다. 컨트롤러 config에는 `io.narumq.component=rollout-controller`와 `$ControllerVersion`이 있어야 합니다. 불일치, index digest, mutable tag 참조 또는 검증되지 않은 source-voter metadata가 있으면 인수를 진행하지 마세요.

ECR에서 반환한 `imageDigest`를 기록할 digest로 사용하고 OCI metadata와 대조하세요:

```powershell
$ControllerDigest = (aws ecr describe-images --profile $AwsProfile --region ap-northeast-2 --repository-name narumq --image-ids "imageTag=$ControllerTag" --query 'imageDetails[0].imageDigest' --output text).Trim()
$BrokerDigest = (aws ecr describe-images --profile $AwsProfile --region ap-northeast-2 --repository-name narumq --image-ids "imageTag=$BrokerTag" --query 'imageDetails[0].imageDigest' --output text).Trim()
$ControllerImageRef = "$Repository@$ControllerDigest"
$BrokerImageRef = "$Repository@$BrokerDigest"
$ControllerManifest = crane manifest $ControllerImageRef | ConvertFrom-Json
$BrokerManifest = crane manifest $BrokerImageRef | ConvertFrom-Json
$PlatformManifestTypes = @('application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json')
if ($ControllerManifest.mediaType -notin $PlatformManifestTypes -or $BrokerManifest.mediaType -notin $PlatformManifestTypes) { throw 'Expected platform manifests, not OCI index/list digests' }
$ControllerConfig = crane config $ControllerImageRef | ConvertFrom-Json
$BrokerConfig = crane config $BrokerImageRef | ConvertFrom-Json
if ($ControllerConfig.os -ne 'linux' -or $ControllerConfig.architecture -ne 'arm64' -or $BrokerConfig.os -ne 'linux' -or $BrokerConfig.architecture -ne 'arm64') { throw 'Image config platform mismatch' }
if ($ControllerConfig.config.Labels.'org.opencontainers.image.version' -ne $ControllerVersion -or $ControllerConfig.config.Labels.'io.narumq.component' -ne 'rollout-controller') { throw 'Controller image labels mismatch' }
if ($BrokerConfig.config.Labels.'org.opencontainers.image.version' -ne '0.0.1' -or $BrokerConfig.config.Labels.'io.narumq.features' -ne $Features) { throw 'Broker image labels mismatch' }
```

EKS overlay에서 사용할 컨트롤러 이미지 입력은 검증된 digest reference뿐입니다. 검증이 끝난 뒤에만 폐기 가능한 checkout에서 ignored 입력 파일을 만드세요:

```powershell
$ControllerImage = "$Repository@$ControllerDigest"
if ($ControllerImage -notmatch '^.+@sha256:[0-9a-f]{64}$') { throw 'Controller image must be an immutable platform-manifest digest' }
Set-Content -Path 'deploy/kubernetes/rollout-controller/overlays/eks-fixture/controller-image.env' -Value "CONTROLLER_IMAGE=$ControllerImage" -Encoding Ascii
kubectl kustomize deploy/kubernetes/rollout-controller/overlays/eks-fixture
```

이는 로컬 render일 뿐 배포가 아닙니다. 명시적인 fixture handoff와 CLU-14 증거를 받은 뒤에만 준비된 overlay를 `kubectl --context $FixtureContext apply -k deploy/kubernetes/rollout-controller/overlays/eks-fixture` 명령으로 적용할 수 있습니다. `$FixtureContext`는 격리 fixture의 context여야 하며 `narumq-eks`를 가리켜서는 안 됩니다.

ignored `controller-image.env`는 기본 이미지가 아니라 운영자가 제공해야 하는 필수 릴리스 입력이며, 검증된 controller manifest digest에서 생성해야 합니다. rollout CR은 `$Repository@$BrokerDigest`를 대상으로 하고, 해당 OCI version/features 및 실제로 커밋된 3-voter 브로커 프로필과 일치해야 합니다.

fixture를 폐기하고 증거를 보존한 뒤에만 작업 전용 태그 두 개를 정리하세요:

```powershell
aws ecr describe-images --profile $AwsProfile --region ap-northeast-2 --repository-name narumq --image-ids "imageTag=$ControllerTag" --query 'imageDetails[0].{digest:imageDigest,tags:imageTags}' --output json
aws ecr batch-delete-image --profile $AwsProfile --region ap-northeast-2 --repository-name narumq --image-ids "imageTag=$ControllerTag"
aws ecr describe-images --profile $AwsProfile --region ap-northeast-2 --repository-name narumq --image-ids "imageTag=$BrokerTag" --query 'imageDetails[0].{digest:imageDigest,tags:imageTags}' --output json
aws ecr batch-delete-image --profile $AwsProfile --region ap-northeast-2 --repository-name narumq --image-ids "imageTag=$BrokerTag"
```

삭제 전에 각 태그와 digest가 기록된 작업 산출물과 일치하는지 확인하세요. 소유한 정확한 `imageTag`만 삭제하고, `imageDigest`로는 삭제하지 마세요. 다른 태그나 공유 저장소도 삭제하지 마세요. 동일 manifest에 다른 태그 참조가 있으면 작업 태그만 제거하고 나머지 참조는 그대로 두세요. 작업 전용 저장소는 계획되어 있지 않습니다. 공유 저장소를 안전하게 사용할 수 없다면 별도 정리 계획과 승인을 받기 전까지 중단하세요.

#### Argo CD health mapping 연결과 확인

승인된 격리 EKS fixture에서만 시험한다. 먼저 Argo CD의 application-controller·repo-server·Redis가 Ready인지 확인한다. `NaruMQRollout` CRD와 실제 Git source를 사용하는 Application도 있어야 한다. Status를 직접 주입하는 시험은 NaruMQ controller가 없는 전용 health 시험 CR에만 수행한다. 실제 rollout CR의 status를 바꾸면 안 된다.

저장소 루트의 PowerShell에서 기존 Lua를 Argo CD ConfigMap에 연결한다. `$FixtureKubeconfig`와 `$FixtureContext`는 해당 fixture의 전용 kubeconfig와 context다. 아래 namespace는 시험에서 사용한 `argocd`다. 운영 Argo CD에 적용하는 명령이 아니다.

```powershell
$Lua = Get-Content -Raw 'deploy/kubernetes/rollout-controller/argocd-health.lua'
$Patch = @{ data = @{ 'resource.customizations.health.narumq.io_NaruMQRollout' = $Lua } }
$PatchPath = [System.IO.Path]::GetTempFileName()
try {
    [System.IO.File]::WriteAllText($PatchPath, ($Patch | ConvertTo-Json -Depth 3), [System.Text.UTF8Encoding]::new($false))
    kubectl --kubeconfig $FixtureKubeconfig --context $FixtureContext -n argocd patch configmap argocd-cm --type=merge --patch-file $PatchPath
} finally {
    Remove-Item $PatchPath
}
```

Argo CD `v3.5.5`는 리소스별 health를 기본적으로 Application CR이 아닌 application tree에 저장한다. Kubernetes API로 리소스·Application health를 함께 관찰할 때는 시험 controller에 공식 옵션을 켠다. 이는 관찰 설정이며 NaruMQ Lua 판정은 바꾸지 않는다.

```powershell
kubectl --kubeconfig $FixtureKubeconfig --context $FixtureContext -n argocd set env statefulset/argocd-application-controller ARGOCD_APPLICATION_CONTROLLER_PERSIST_RESOURCE_HEALTH=true
kubectl --kubeconfig $FixtureKubeconfig --context $FixtureContext -n argocd rollout status statefulset/argocd-application-controller --timeout=180s
```

실제 Git sync가 `Synced`이고 비교 오류가 없는 상태에서 리소스와 Application의 health를 함께 확인한다. Repo-server 준비 전 비교가 실패했다면 준비 완료 뒤 해당 Application에 `argocd.argoproj.io/refresh=hard` annotation을 설정한다. 비교 실패로 대상 리소스가 없어도 `Healthy`로 보일 수 있으므로 health만으로 통과를 판정하지 않는다. 상태·전이별 기대값과 실행 증거는 [Argo CD 수용 기록](../verification/drain-and-rolling-update.md#argo-cd-health-소비자-수용-2026-10-15)을 따른다.

Health 시험은 broker 교체 수용과 별개다. 시험 후 전용 CR·Application·Argo CD namespace와 새 AWS fixture 자원을 정리한다.

#### 주요 코딩 컨벤션 요약
- **식별자 명명:** 역할에 맞는 접두사와 접미사를 붙인다.
  - 저장소 포트: `*Repository` (예: `SessionRepository`)
  - 구체 구현체: `WALSessionRepository`, `MemorySessionRepository`
  - 데이터 전송 객체: 반드시 `*DTO` 접미사를 사용한다 (예: `ConnectRequestDTO`). `*Dto`는 금지된다.
  - 약어 대소문자: `ClientID`, `JWKSClient`, `HTTPServer`, `TLSConfig`처럼 전체 대문자를 유지한다.
- **문서화 주석:** 외부로 노출(exported)되는 모든 패키지, 타입, 함수, 메서드에는 대상 이름으로 시작하는 완전한 영문 doc comment를 작성한다.

### 3단계: 개별 단위 테스트 및 레이스 조건 검사

코드를 수정한 후 해당 패키지의 단위 테스트를 먼저 실행하여 빠른 피드백을 얻는다.

1. 특정 패키지 단위 테스트 실행:
   ```sh
   go test ./internal/domain/mqtt/... -count=1
   ```
   기대 결과: `ok   narumq/internal/domain/mqtt ...` 출력.

2. 전체 패키지 단위 테스트 실행:
   ```sh
   go test ./... -count=1
   ```
   기대 결과: 전체 패키지 테스트 통과.

3. 동시성 레이스 조건(Race Detector) 검사:
   ```sh
   go test -race ./... -count=1
   ```
   기대 결과: 데이터 레이스 경고 없이 전체 통과.

S3 helper의 로컬 회귀 시험은 `python -B tests/test_backup_s3.py`로 실행한다. 실제 backup shell의 게시 전후 중단은 별도 Docker opt-in smoke로 확인한다. Python 3, Bash와 GNU coreutils가 있는 Linux container image를 준비해야 한다. 기존 compatibility 시험의 `python-client` image를 사용할 수 있다. 저장소 루트의 PowerShell에서 다음과 같이 실행한다.

```powershell
$env:NARUMQ_BACKUP_PUBLICATION_SMOKE = "1"
$env:NARUMQ_BACKUP_SMOKE_IMAGE = "narumq-compat-python-drain-paho-go-python-will-acl-final:phase1"
python -B tests/test_backup_publication.py
Remove-Item Env:NARUMQ_BACKUP_PUBLICATION_SMOKE
Remove-Item Env:NARUMQ_BACKUP_SMOKE_IMAGE
```

이 smoke는 실제 `backup.sh`를 게시 전후에 강제 종료하고 새 Pod UID의 uploader로 재시도한다. AWS CLI는 checksum을 검사하는 대역이며 container는 `--network none`으로 실행한다. 데이터는 container tmpfs에만 있고 종료 때 container를 제거한다. 실제 AWS 업로드·물리 전원 손실 시험이 아니다. 실행 결과는 [Cluster §11.9](../specs/cluster-session-and-delivery.md#119-offline-queue-replication과-backup-v3-수용-매핑)를 따른다.

#### EKS worker observer 실행

이 도구는 준비된 격리 합성 fixture를 관찰한다. EKS나 인증 입력을 만들지 않는다. `monitor`는 인프라를 변경하지 않지만 합성 MQTT QoS 1 메시지는 발행한다. `fault`는 실제 EC2를 비정상 종료하므로 운영 환경에서 실행하지 않는다.

일반 회귀는 Python 3만 있으면 된다. 저장소 루트에서 `python -B tests/test_eks_worker_recovery.py -v`로 실행한다. AWS·Docker·Paho는 필요 없다. CI는 `tests/run_scenario_unittest.py`로 이 파일을 다른 Python 회귀 파일과 함께 실행하고 method별 JSONL 결과를 기록한다. Go `local`/`ci` 명령 자체에는 Python 시험이 포함되지 않는다.

실제 broker를 관찰하려면 전용 임시 venv에 `paho-mqtt==2.1.0`을 설치한다. `<private-directory>`에는 `admin.token`과 `mqtt.password`를 둔다. 고정 합성 identity는 username `acceptance-device`, client ID `ebs-worker-publisher`다. ACL은 `acceptance/<run-id>/continuity` publish를 허용해야 한다. 다른 시험이 같은 client ID를 사용하고 있으면 먼저 끝낸다. 비밀 원문을 명령줄이나 검증 아티팩트에 넣지 않는다.

`monitor`에서는 관리 API `127.0.0.1:23880`과 MQTT `127.0.0.1:23883`을 같은 Ready broker에 미리 연결한다. 이 mode는 tunnel의 Pod identity를 검증하지 않으며 fault 수용 증거가 아니다. 읽기 전용 관리 조회의 client 제한은 10초다. 다음 명령의 `<venv-python>`은 준비한 venv의 Python 실행 파일이다. 출력 디렉터리는 없어야 하며 마지막 이름 `<run-id>`가 표본의 topic에 쓰인다.

```text
<venv-python> -B tests/chaos/eks-worker-recovery/main.py monitor --private <private-directory> --output .artifacts/verification/worker-observer/<run-id> --seconds 30
```

`management-samples.jsonl`과 `mqtt-samples.jsonl`은 각 표본의 시작·종료·결과를 따로 기록한다. `summary.json`에는 실패와 종료 결과를 남긴다. API 오류 중에도 MQTT probe는 계속 실행한다. Timeout이나 음수 PUBACK을 성공으로 처리하지 않는다. PASS는 표본에만 해당하며 표본 사이의 연속 가용성을 뜻하지 않는다.

실제 EKS `fault` mode는 별도 준비가 필요하다. 현재 wrapper는 Windows 시험 환경의 `<private-directory>/kubectl.exe`·`kubeconfig`를 사용한다. Kubeconfig context는 `<run-id>`와 같아야 한다. AWS CLI의 `kayden-sandbox-admin` profile·`ap-northeast-2`·account `809446814769`에서만 실행한다. 관리·MQTT 관찰용 `23880`·`23883` port는 미리 열지 않는다. 도구가 plan의 생존 Pod 하나에 두 port를 직접 연결한다. 기존 tunnel이 port를 점유하면 재사용하지 않고 실패한다. 기존 drain 기록의 부재도 같은 생존 Pod tunnel로 cluster 전체 조회해 확인한다. 별도의 대상 Pod 관리 tunnel은 필요 없다. Windows에서는 `netsh int ipv4 show excludedportrange protocol=tcp`로 `23880`·`23883`이 예약 범위에 없는지 먼저 확인한다. 예약 범위에 있으면 bind에 실패한다.

`<fault-plan.json>`에는 현재 fixture에서 읽은 아래 값을 넣는다. 과거에 삭제한 환경의 plan을 재사용하지 않는다.

- 소유·cluster: `runId`, `clusterName`, `vpcId`. Namespace `narumq`의 `narumq.io/acceptance-run` label과 broker cluster ID가 `runId`와 같아야 한다. Cluster 이름은 `narumq-ha-acceptance-`로 시작해야 한다.
- 대상: `targetPod`, `targetNodeId`, `oldPodUID`, `oldWorkerName`, `oldNodeUID`, `oldInstanceId`. Broker 3개가 Ready이고 quorum이 있어야 한다. 대상은 follower여야 한다.
- 생존 관찰 경로: `observerPod`, `observerPodUID`, `observerWorkerName`, `observerInstanceId`. 대상 Pod·worker·instance와 각각 달라야 한다. 현재 Pod UID·배치·provider instance가 일치하고 Pod·worker가 Ready이며 EC2가 running이어야 한다. 도구는 tunnel 준비 뒤와 fault 직전에도 identity를 확인한다.
- 복구 원본: `pvcName`, `pvcUID`, `pvName`, `volumeId`, `zone`, `snapshotId`. 대상 instance의 data volume은 `DeleteOnTermination=false`여야 한다. 완료 snapshot에는 같은 `RunId` tag가 있어야 한다.
- 여분 worker: `spareWorkerName`, `spareInstanceId`. 대상과 다른 같은-AZ Ready worker여야 하고 scheduling을 허용해야 한다.

장애 전 데이터를 준비한다. 대상 Pod와 생존 Pod에 각각 임시 `kubectl port-forward`를 열고, 예약 범위가 아닌 대상 port를 쓴다. 생존 Pod의 MQTT tunnel은 `23883`을 쓸 수 있다. 아래 명령은 persistent session을 만든 뒤 retained marker 1개와 오프라인 QoS 1 메시지 3개를 발행한다. 성공 PUBACK 4개와 retained 읽기를 확인한다. 결과 JSON에는 payload hash만 남긴다. 명령이 끝나면 두 tunnel을 닫는다. `fault`가 `23883`을 직접 소유해야 하기 때문이다. `<result-directory>`에는 `mqtt-before-fault.json`이 생긴다.

```text
<venv-python> -B tests/chaos/eks-worker-recovery/payload_oracle.py prepare --private <private-directory> --run-id <run-id> --output <result-directory> --target-port <대상-tunnel-port> --publisher-port <생존-tunnel-port>
```

Oracle은 고정 client ID `ebs-worker-subscriber`·`ebs-worker-publisher`·`ebs-worker-reader`와 `acceptance/<run-id>/` topic을 쓴다. 같은 broker에 이전 session이나 retained 상태가 남아 있으면 결과가 오염된다. 새 격리 fixture나 비어 있는 broker에서만 실행한다. 다른 run ID를 쓰더라도 기존 persistent session은 client ID 때문에 이어질 수 있다. 시험 전 전용 fixture의 이전 oracle 상태가 없음을 확인한다. 저장소의 Docker 검증은 oracle 동작 확인이며 fault 복구 증거가 아니다.

종료 대상과 보호 snapshot을 검토한 뒤에만 아래 명령을 실행한다. 출력 경로는 새 경로여야 하며 마지막 이름은 plan·context와 같은 `<run-id>`다. `--execute`나 `--plan`이 없으면 fault 명령은 종료한다.

```text
<venv-python> -B tests/chaos/eks-worker-recovery/main.py fault --private <private-directory> --output .artifacts/verification/worker-fault-observer/<run-id> --plan <fault-plan.json> --execute
```

이 명령은 EC2 `SkipOsShutdown` 종료 뒤 종료 상태를 확인한다. Node UID·resourceVersion 조건으로 out-of-service taint를 한 번만 시도한다. 명령이 실패하면 실제 적용 여부를 추정하지 않고 새 Node를 읽는다. Taint가 확인되면 도구는 기존 대상 Pod 하나를 Pod UID precondition과 `gracePeriodSeconds=0`으로 한 번만 삭제한다. 삭제 직전에도 EC2 `terminated`, Node UID·provider instance·taint, Pod UID·배치를 다시 확인한다. 하나라도 다르면 삭제하지 않는다. 대체 Pod나 다른 Pod는 삭제하지 않고 실패해도 다시 시도하지 않는다. 이후 대체 Pod·동일 PVC/PV/EBS·attachment를 관찰하고, 복구 표본마다 VolumeAttachment와 EBS attachment 상태를 남긴다. EBS force detach와 VolumeAttachment 삭제는 복구 수단으로 쓰지 않는다. 명령이 PASS여도 데이터 oracle의 PASS는 아니다. 복구한 대상 Pod에 새 tunnel을 열고 `verify`를 실행한다.

```text
<venv-python> -B tests/chaos/eks-worker-recovery/payload_oracle.py verify --private <private-directory> --run-id <run-id> --output <result-directory> --target-port <복구된-대상-tunnel-port>
```

`verify`는 다시 발행하지 않는다. `sessionPresent=true`, 오프라인 payload hash 3개와 retained hash가 같을 때만 통과한다. 실행 후 합성 StatefulSet·PVC·snapshot·추가 node group과 전용 fixture를 정리한다. PVC를 먼저 삭제해 EBS 삭제를 확인한 뒤 Terraform을 실행한다. 임시 입력은 암호화 보존 후 제거한다. 실제 실행 결과와 남은 gate는 [Drain spec의 EKS 재시험 기록](../verification/drain-and-rolling-update.md#생존-pod-고정-observer의-eks-재시험-2026-10-15)을 따른다.

`observer-endpoint.json`과 최종 summary의 `observerEndpoint`는 실제로 확인한 생존 Pod UID·worker·instance와 두 port를 기록한다. Tunnel은 service나 LB를 선택하지 않는다. 끊어진 tunnel을 다시 열거나 다른 Pod로 전환하지 않는다. 관찰·MQTT가 끝난 뒤 tunnel process와 출력 worker도 종료한다. `monitor`의 외부 tunnel 관찰과 `fault`의 도구 소유 tunnel을 혼용하지 않는다.

`fault` 모드는 진단용 `nodes-samples.jsonl`도 남기며, 이 표본은 관찰 전용 부하로 PASS/FAIL 판정에 쓰지 않는다.

`fault` 모드는 fault 요청 전에 `nodes` 첫 표본이 끝나기를 최대 20초 기다린다. 첫 완료 표본이 성공하면 `summary.json`의 `monitor.diagnostics.nodes.baseline.available`이 참이다. 실패하거나 아직 끝나지 않았으면 거짓이며, `firstSampleMayOverlapFault`로 진행 중일 수 있는 표본을 구분한다. 이 대기는 fault와 PASS/FAIL 판정을 바꾸지 않는다. `faultRequestedAt`은 AWS 요청 직전에 기록하므로 요청이 실패해도 남는다.


### 4단계: 바운디드 퍼즈 캠페인(Bounded Fuzz Campaigns) 실행

NaruMQ는 중요한 상태 머신과 파서의 불변식을 보장하기 위해 5개의 핵심 퍼즈 타깃에 대해 고정 작업량(`-fuzztime=10000x`, `-parallel=1`)의 퍼즈 테스트를 필수 관문으로 운영한다.

```sh
# 1. MQTT 디코더 퍼즈
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzDecoder$ -fuzztime=10000x -parallel=1

# 2. MQTT 코덱 왕복(Round-trip) 퍼즈
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzCodecRoundTrip$ -fuzztime=10000x -parallel=1

# 3. 토픽 매칭 및 와일드카드 퍼즈
go test ./internal/domain/routing -run=^$ -fuzz=^FuzzTopicMatching$ -fuzztime=10000x -parallel=1

# 4. 세션 라이프사이클 상태 전이 퍼즈
go test ./internal/domain/session -run=^$ -fuzz=^FuzzLifecycleTransitions$ -fuzztime=10000x -parallel=1

# 5. TCP 연결 입력과 자원 회수 퍼즈
go test ./internal/adapters/tcp -run=^$ -fuzz=^FuzzConnectionInput$ -fuzztime=10000x -parallel=1
```

기대 결과: 각 명령이 실패 입력(crash corpus) 없이 정상 종료된다.

### 5단계: 로컬 종합 검증 도구 체인(`tools/verify local`) 실행

저장소의 로컬 검증 gate는 `go run ./tools/verify local`로 실행한다. CI는 PR diff coverage를 추가하지만 mutation은 실행하지 않는다. 전체 mutation 검증이 필요하면 `go run ./tools/verify mutation`을 별도로 실행한다.

기본 gate는 `Acceptance:` 표시가 있는 opt-in 수용 시험을 요구하지 않는다. 이 시험은 환경 변수가 없으면 건너뛰고 보고서에 `미실행 (게이트 밖)`으로 남는다. 그 밖의 연결 Go 시험은 건너뛰면 통과하지 않는다. Opt-in 수용 시험은 [아래 절차](#opt-in-수용-시험-실행)로 따로 실행한다.

```sh
go run ./tools/verify local
```

이 명령은 먼저 `gofmt`를 검사하고 `branch-instrument`를 실행한다. 그 뒤 아래 11개 command를 병렬 실행한다. 모두 통과하면 시나리오 실행 검사를 하고, 이어서 coverage policy를 평가한다.

1. **Preflight (4개 command):** `go vet ./...`, `go run ./tools/verify traceability`, `go run ./tools/verify scenarios`, `go run ./tools/verify architecture`.
2. **전체 package test와 atomic coverage profile:** 전체 테스트를 `go test -json`으로 실행해 `.artifacts/verification/coverage.out`을 만든다. 테스트 이벤트(stdout)는 `.artifacts/verification/go-test.json`에 저장되고 stderr는 콘솔에 나온다. 병렬 검사가 실패하면 이 파일에서 실패한 테스트와 출력을 골라 콘솔에 보여 준다.
3. **Race suite:** `go test -race ./cmd/... ./internal/... -count=1`.
4. **Fuzz campaign (5개 command):** MQTT decoder·codec round-trip, topic matching, session lifecycle, TCP connection input을 고정 횟수로 검사한다.
5. **Coverage policy:** 병렬 검사가 성공한 뒤 line 85%, function 90% floor/ratchet를 검사한다.

기본 gate는 논리 시험만 실행한다([ADR-0064](../decisions/ADR-0064-ci-gate-logic-only.md)). 지연·처리량·percentile·지연 비율 한도나 부하 강도로 판정하는 부하·품질 시험은 `Acceptance: NARUMQ_LOAD_QUALITY`를 표시해 gate 밖에 둔다. 현재 대상은 네 개다. TEN-09(`TestDynamicClusterTenantNoisyNeighborProcessAcceptance`), CPU 경쟁 아래 persistent subscriber 시험(`TestN1PersistentClose`), BP-01·BP-05의 in-process 부하 실행(`TestRunAgainstInProcessNaruMQ` 두 개)이다. 이 시험들의 통과 증거는 아래 `acceptance` 명령의 결과다.

Local과 CI는 각각 총 14개 verifier command를 실행하며 mutation은 포함하지 않는다. 14개는 `branch-instrument`, 병렬 11개, 실행 검사 `go run ./tools/verify scenarios --go-test-json .artifacts/verification/go-test.json --require-executed`, coverage policy다. Local coverage는 `-parallel=8`, CI coverage는 유효 `GOMAXPROCS`를 쓴다.

Mutation을 실행하려면 `go run ./tools/verify mutation`을 사용한다. 이 명령은 다섯 critical package를 Gremlins worker 1개씩 실행하고, CPU 예산에 맞춰 package 작업을 batch로 나눈다. Coverage, race, fuzz gate는 다시 실행하지 않는다.

기대 결과: 마지막에 `local: PASS` 메시지가 출력되고 종료 코드 0으로 완료된다.

### Opt-in 수용 시험 실행

100회 rolling, Kind, Docker dashboard, release 호환성, durable ACK 장애, WAL lifecycle 시험은 `go run ./tools/verify acceptance`로 실행한다. 이 시험들은 시험 함수의 doc comment에 `Acceptance: <환경 변수>`가 표시되어 있다. 외부 EKS chaos 시험과 게시 release 수용은 별도 수동 결과이며 이 로컬 환경으로 대신하지 않는다.

실행하기 전에 고를 시험의 환경을 준비한다.

| 환경 변수 | 필요한 로컬 환경 |
| --- | --- |
| `NARUMQ_RELEASE_COMPAT=1` | Git historical source를 빌드할 수 있는 checkout과 Go |
| `NARUMQ_DURABLE_ACK_FAULTS=1`, `NARUMQ_WAL_LIFECYCLE=1`, `NARUMQ_ROLLING_100=1` | 격리된 실제 broker process 시험. Rolling은 100회를 그대로 실행한다. |
| `NARUMQ_DASHBOARD_E2E=1` | Linux container를 실행할 Docker와 Grafana·Prometheus image 접근 |
| `NARUMQ_ROLLOUT_KIND_SMOKE=1` | 전용 Kind cluster `rollout-smoke`. 기존 `narumq` namespace가 없는지 확인한다. |
| `NARUMQ_ROLLOUT_REGISTRY`, `NARUMQ_ROLLOUT_HOST_REGISTRY` | Kind node에서 접근할 registry 주소와 host에서 접근할 HTTP URL |
| `NARUMQ_ROLLOUT_CONTROLLER_IMAGE` | 현재 소스의 controller image. Kind에서 pull하거나 미리 load해야 한다. |
| `NARUMQ_ROLLOUT_OLD_DIGEST`, `NARUMQ_ROLLOUT_TARGET_DIGEST` | Registry의 실제 broker platform manifest digest. 합성 version `0.0.0`·`0.0.1`과 binary의 전체 feature 목록을 OCI label에 넣는다. |

부하·품질 시험도 같은 명령으로 실행한다. 네 시험을 한 번에 실행하려면 `NARUMQ_LOAD_QUALITY=1 go run ./tools/verify acceptance --run 'TenantNoisyNeighbor|TestN1PersistentClose|TestRunAgainstInProcessNaruMQ'`를 쓴다. `--run` 없이 실행하면 다른 환경 변수가 필요한 시험까지 고르므로 실패한다. 실행하는 동안 다른 부하가 없는 host에서 실행하고 결과를 [Tenant와 security 검증 기록 §9](../verification/tenancy-and-security.md#9-ten-09-noisy-neighbor-실행-기록)와 [성능 검증 기록 §15](../verification/performance-and-verification.md#15-ci-gate-논리-검증-한정-2026-10-18)에 남긴다.

Kind fixture의 정확한 입력·정리 경계는 `tests/integration/rollout_controller_kind_test.go`를 따른다. Registry를 node에 HTTP로 연결하면 해당 주소의 containerd registry 설정도 준비해야 한다. 시험을 끝내면 자신이 만든 Kind cluster와 registry container만 정리한다. 실제 실행에 사용한 값은 검증 기록에 보존한다.

전체를 실행하려면 저장소 루트에서 다음 명령을 실행한다. 일부만 실행하려면 `--run`에 시험 이름의 Go 정규식을 준다.

| `NARUMQ_LOAD_QUALITY=1` | TEN-09와 N1은 격리된 실제 broker process 시험이고 BP-01·BP-05는 in-process 시험이다. 지연과 schedule lag로 판정하므로 다른 CPU·디스크 부하가 없는 host에서 실행한다. |
```sh
go run ./tools/verify acceptance
go run ./tools/verify acceptance --run 'ReleaseCompatibility|BackupRestore.*Release'
```

이 명령은 고른 시험의 환경 변수가 모두 `1`인지 먼저 확인한다. 빠진 환경 변수가 있으면 시험을 시작하지 않는다. 고른 시험이 모두 통과하면 `acceptance: PASS`를 출력한다. 건너뜀·실패·결과 없음이 하나라도 있으면 해당 시험과 `acceptance: FAIL`을 출력한다. 결과는 `.artifacts/verification/acceptance-go-test.json`에 남는다. 기본 gate 결과와 함께 보고서를 만들려면 다음 명령을 실행한다.

```sh
go run ./tools/verify scenarios --go-test-json .artifacts/verification/go-test.json --go-test-json .artifacts/verification/acceptance-go-test.json
```

### 시나리오 커버리지와 결과 보고서 만들기

스펙 시나리오와 테스트의 연결은 [테스트 컨벤션](../conventions/test-conventions.md)의 메타 정보로 확인한다. `local`·`ci` 게이트는 두 검사를 한다. preflight의 연결 검사(`scenarios`)는 스펙 표와 테스트 메타 정보만 대조하며 `오류`, `미연결`이 있으면 종료 코드 1로 끝난다. 병렬 검사 뒤의 실행 검사(`scenarios --go-test-json ... --require-executed`)는 `go-test.json`에 나온 패키지의 연결 테스트가 모두 통과했는지 확인하며, 건너뛰었거나 결과가 없으면 종료 코드 1로 끝난다. Python·EKS처럼 게이트 밖인 시나리오는 `미실행 (게이트 밖)`으로 보고서에 남고 게이트를 멈추지 않는다. 게이트와 같은 검사를 직접 하려면 아래 명령을 저장소 루트에서 실행한다.

```sh
go test -json -count=1 ./... > .artifacts/verification/go-test.json
go run ./tools/verify scenarios --go-test-json .artifacts/verification/go-test.json --require-executed
```

기대 결과: `.artifacts/verification/scenarios/report.md`와 `report.json`이 만들어지고 `scenarios: PASS (...)`가 출력된다. 보고서의 상태 정의와 결과 파일 형식, CI 아티팩트 위치는 테스트 컨벤션을 따른다. Windows PowerShell 5의 `>`는 UTF-16 파일을 만든다. bash, PowerShell 7 또는 `cmd /c`를 쓴다.

CI는 Go 게이트 뒤에 Python unittest 결과도 합친다. Go JSON이 없으면 Python JSONL만으로 보고서를 갱신하고, 둘 다 없으면 보고 단계가 실패한다. Python runner는 시작할 때 출력 JSONL을 초기화하므로 실행마다 별도 파일명을 쓰면 이전 증거를 보존할 수 있다. 로컬에서 같은 결과 기록 방식을 확인하려면 저장소 루트의 bash에서 다음 명령을 실행한다.

```sh
PYTHONDONTWRITEBYTECODE=1 NARUMQ_SCENARIO_RESULTS=.artifacts/verification/scenarios/python-ci-results.jsonl python tests/run_scenario_unittest.py tests/test_backup_s3.py tests/test_eks_worker_recovery.py tests/test_eks_capacity_controller.py tests/test_scenario_results.py tests/test_run_scenario_unittest.py
go run ./tools/verify scenarios --go-test-json .artifacts/verification/go-test.json --results .artifacts/verification/scenarios/python-ci-results.jsonl --require-executed
```

첫 명령의 성공은 Python unittest 성공이다. 두 번째 명령은 미연결과 실행 범위 안의 건너뜀도 거절한다. 라이브 EKS 시나리오의 통과로 해석하지 않는다.

### Markdown 링크를 수정할 때

저장소 루트에서 로컬 파일과 제목 앵커를 검사한다.

```sh
go run ./tools/verify markdown-links
```

검사 범위는 루트 `AGENTS.md`, `README.md`, `docs/**/*.md`, `.agents/skills/**/*.md`다. 검사기는 링크 대상 파일·폴더와 Markdown 제목 앵커를 확인한다. 외부 URL과 코드 펜스는 검사하지 않는다. 제목 앵커는 Markdown 제목에서 기본 slug를 만들거나 HTML `id`·`name`, `{#id}` 값을 확인한다. 인라인 링크만 검사하며 reference-style 링크는 범위 밖이다. 코드 펜스는 제외하지만 inline code 안의 링크 문법은 제외하지 않는다. 오류가 있으면 실패한 링크를 출력하고 종료 코드 1을 반환한다.

### 6단계: 로컬 브로커 기동 및 수동 확인

실제 네트워크 상에서 동작을 시험해보고 싶다면 로컬 개발 모드로 브로커를 기동한다.

```sh
go run ./cmd/narumq -tcp-listen 127.0.0.1:1883 -http-listen 127.0.0.1:8080 -allow-insecure-anonymous-mqtt
```

- **구현 경계 안내:** 단일 프로세스 standalone 리스너는 개발 및 단일 노드 검증용으로 인메모리 세션/큐/retained 저장소를 사용하며 `qos0_only`로 동작한다.
- 별도 터미널에서 `curl http://127.0.0.1:8080/health/ready`로 준비 상태를 확인한다.
- 상세한 실행 옵션과 보안 설정 적용은 [설치 가이드](installation-guide.md)를 참조한다.

## 4. 검증

완료 여부는 아래 성공 조건으로 판정하고, 실제 대상 버전·환경·명령·결과를 [검증 기록 규칙](../AGENTS.md#검증-기록)에 따라 남긴다. 과거 통과는 현재 코드의 통과가 아니다. 전체 local coverage는 실패 상태이며, 현재 시나리오 연결·실행 결과는 [시나리오 보고서](../../.artifacts/verification/scenarios/report.md)를 확인한다.

Branch coverage는 schema 2 report의 `measured`로 기록한다. Overall·core branch ratchet에는 `branchTolerance=0.20`을 적용한다. Core branch는 90%, 변경 코드 line/function/branch diff coverage는 각각 80%다. Diff 기준은 [ADR-0063](../decisions/ADR-0063-diff-coverage-floor-80.md)을 따른다.

| 관찰 대상 | 성공 조건 | 확인 명령·방법 | 관련 검증 기록 |
| --- | --- | --- | --- |
| 단위 테스트 및 레이스 | 전체 테스트 통과 및 Data Race 0건 | `go run ./tools/verify local` | [성능 검증 기록](../verification/performance-and-verification.md) |
| 아키텍처 규칙 | 계층 간 허용되지 않은 import 0건 | `go run ./tools/verify architecture` | 실행 콘솔 출력 |
| 규격 추적성 | PRD의 모든 P0를 test 함수 또는 유효한 예외에 연결하고 stale 예외 0건. 예외가 있으면 제품 전체 수용 완료가 아니다. | `go run ./tools/verify traceability` | [P0 gate 결정](../decisions/ADR-0042-p0-requirement-traceability-gate.md) |
| 시나리오 연결·실행 | 오류·미연결이 없고 실행 범위 안의 연결 테스트가 모두 통과함 | §3의 `scenarios --require-executed` 명령 | 시나리오 보고서 |
| Markdown 링크 | 지정 범위의 로컬 링크 대상과 제목 앵커가 모두 유효함 | `go run ./tools/verify markdown-links` | 실행 콘솔 출력 |
| 코드 커버리지 | Line/function 기준, overall branch 80%, core branch 90%, ratchet 허용폭, line/function/branch diff 80% 충족 | `go run ./tools/verify local`; `go run ./tools/verify ci --base <merge-base>` | [Coverage 정책 수용 기록](../verification/performance-and-verification.md#84-전체-branch-floor-수용-증거) |
| 뮤테이션 테스트 | Delivery·MQTT·NodeDrain·Security·Session 각각 efficacy·mutant coverage 80% 이상 | `go run ./tools/verify mutation` | [수동 mutation 검증 기록](../verification/performance-and-verification.md#88-수동-mutation-검증-수용-증거) |
| CI 자동 검증 | PR이 최신 `main`을 반영하고 필수 `verify` check가 성공해야 merge할 수 있음 | GitHub Actions `verify`; `go run ./tools/verify ci --base <merge-base>` | [원격 CI·required check 기록](../verification/performance-and-verification.md#89-github-actions-원격-ci와-required-check-증거) |
| 종합 로컬 검증 | 로컬 검증 gate 전체 완료 및 종료 코드 0 | `go run ./tools/verify local` | 해당 실행의 콘솔 출력·coverage report |

## 5. 중단 조건 및 빈발 문제 해결

검증 도구 실행 중 다음 오류가 발생하면 즉시 수정을 진행한다.

### 1. `architecture: FAIL` (아키텍처 규칙 위반)
- **원인:** `internal/domain` 패키지가 외부 라이브러리나 `adapters` 패키지를 import했거나, `application` 계층이 구체 `adapters`를 직접 참조한 경우.
- **해결:** 의존성 방향을 역전시킨다. `application/ports`에 인터페이스를 정의하고, 어댑터가 이를 구현하도록 변경한 후 `cmd/narumq`에서 주입받는다.

### 2. `traceability: FAIL` (규격 추적성 불일치)
- **원인:** MQTT normative statement와 matrix의 연결이 다르거나, Traceability marker의 PRD ID가 없거나, P0 연결과 예외 목록의 상태가 맞지 않는 경우.
- **해결:** MQTT statement 오류는 `tests/conformance/mqtt-compliance-v1.json`과 test comment를 맞춘다. `P0 requirement <ID> has no Traceability test and no exception`이면 관련 test 함수에 `Traceability:`를 추가하거나 이유와 저장소 내 경로를 적어 예외 파일에 추가한다. `P0 requirement <ID> is mapped by ... but still listed`이면 예외 파일에서 해당 ID를 제거한다. `unknown PRD requirement <ID>`이면 marker의 ID를 PRD와 대조해 고친다.

### 3. `coverage: FAIL` (커버리지 기준 미달)
- **원인:** 새로 작성한 비즈니스 로직에 단위 테스트가 충분하지 않아 라인 85% 또는 함수 90% 기준 밑으로 떨어진 경우.
- **해결:** 정상 경로뿐만 아니라 경계 조건(Boundary condition), 예외 처리, 에러 반환 경로를 커버하는 단위 테스트 케이스를 `*_test.go`에 추가한다.

### 4. `gofmt check failed` (포맷팅 불일치)
- **원인:** Go 표준 포맷팅 룰에 맞지 않는 파일이 존재하는 경우.
- **해결:** `gofmt -w .`를 실행하여 포맷팅을 자동으로 정돈한다.

## 6. 이전 상태로 되돌리는 방법

작업 중 발생한 변경 사항을 되돌릴 때는 **저장소 전체를 파괴하는 명령(`git restore .`, `git clean -fd`)을 사용하지 말고**, 변경된 대상을 먼저 확인한 후 안전하게 선택 복구한다.

1. **현재 변경 및 미추적 파일 상태를 먼저 확인한다:**
   ```sh
   git status
   git diff
   ```
2. **자신이 작업한 특정 파일만 선택하여 원래 상태로 복원한다:**
   ```sh
   git restore <수정한_파일_경로>
   ```
3. **테스트 목적으로 직접 생성한 미추적 파일만 지정하여 삭제한다:**
   ```sh
   rm <직접_생성한_임시_파일_경로>
   ```
4. **빌드 및 로컬 검증 중 생성된 산출물은 해당 전용 파일만 정리한다:**
   저장소에 이미 존재하는 `.artifacts/verification/` 디렉터리 전체를 삭제하지 말고, 이번 작업으로 새로 생성된 특정 파일만 개별 정리한다.
   ```sh
   # 빌드 바이너리 삭제
   rm -f bin/narumq bin/narumq.exe

   # 검증 시 새로 생성된 임시 커버리지 파일만 개별 삭제 (기존 베이스라인 증거 보존)
   rm -f .artifacts/verification/coverage.out .artifacts/verification/coverage-summary.json
   ```

## 7. 문제 전달

검증 단계에서 해결하기 어려운 아키텍처 충돌이나 모듈 간 의존성 사이클이 발생할 경우 다음 규칙에 따라 전달한다.

- 전달 대상: 백엔드 테크 리드 및 플랫폼 아키텍트.
- 보존할 증거: `go run ./tools/verify local` 실패 로그 전문, `git diff` 내용, 관련 spec/contract 문서 링크.
- 비밀 값 제외 규칙: 토큰, 패스워드, 개인키 등 보안 자격 증명은 보고서나 이슈 본문에 절대 기입하지 않는다.
