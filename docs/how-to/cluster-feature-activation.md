# Cluster feature activation을 승인하고 실행하는 방법

이 절차는 외부 승인 기록을 만든 뒤 기존 Management API로 feature 한 개를 활성화한다. 해당 FeatureActivation의 commit이 그 feature의 rollback window를 끝낸다. 사람의 승인 기록은 runtime이 검사하지 않는다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | 배포 release 담당자와 ClusterAdmin 운영자 |
| 검증 상태 | 제한 범위 검증 (과거 local 실행) |
| 검증 환경·버전 | Windows PowerShell 5.1.26100.9444, curl.exe 8.21.0, jq 1.8.2; Go 1.26.5 local 3-node mTLS fixture |
| 검증 범위 | Windows local API·JSON 판정·증거 directory ACL의 과거 실행. Membership operation 조회·외부 승인·production credential ACL은 미실행이며 production 실행 승인이 아니다. |
| 관련 기준 문서 | [Release contract §4.1](../contracts/cluster-release-compatibility-v1.md#41-운영-승인과-rollback-window-종료), [ADR-0032](../decisions/ADR-0032-feature-activation-approval-policy.md), [Cluster §11.30](../specs/cluster-session-and-delivery.md#1130-feature-activation-운영-승인-수용-매핑), [Management API v1](../contracts/management-api-v1.md), [Backup v3 §2·§4](../contracts/cluster-backup-v3.md#2-producer와-consumer-동작) |

## 1. 대상과 영향

- 대상은 승인 기록에 적은 cluster ID와 feature token 한 개다. 운영 예시 token은 `session-metadata`다. `compatibility-probe`는 local 시험에만 쓴다.
- Activation 뒤에는 해당 feature를 지원하지 않는 이전 build로 무손실 in-place rollback할 수 없다. 같은 token 재요청은 성공 no-op이며 window를 다시 열지 않는다.
- 예정한 binary 교체·membership 변경·drain을 끝낸 뒤 실행한다. 고정 안정화 시간이나 새 SLO는 정하지 않는다.
- Production storage·backup retention/RPO/RTO와 실제 게시 release별 matrix의 열린 gate는 유지한다. 이 절차로 production activation을 승인하지 않는다.

## 2. 전제 조건

- [ ] 해당 배포의 release 담당자가 승인 책임을 맡았다. 실행자는 `ClusterAdmin`이다. 같은 사람이 두 책임을 맡을 수 있다.
- [ ] 승인 배포 목록에 cluster ID, 모든 non-retired member ID·승인 build·release·features와 node ID별 실제 관리 URL을 적었다. Staged member도 포함한다.
- [ ] Target/rollback build 조합의 release matrix와 feature 기능 시험 증거가 있다. 게시 release에는 해당 release의 matrix가 있다. Synthetic·지정 historical 증거를 미래 게시 release의 증거로 쓰지 않는다.
- [ ] 진행 중인 배포 결함이 없다. 알려진 membership operation ID와 완료 기록, binary 교체·drain 완료 기록을 확보했다.
- [ ] 유효한 commit-boundary backup과 지원 restore build·동일 identity 구성·복구 시험 증거가 있다. Learner 포함 또는 세 bootstrap voter identity 조건 위반이면 승인하지 않는다.
- [ ] 신뢰된 관리 host의 PowerShell 5.1에서 `curl.exe`와 jq-compatible CLI를 실행할 수 있다. `curl`은 PowerShell alias이므로 쓰지 않는다.
- [ ] Secret manager가 만든 `$CurlConfig`는 현재 사용자만 읽을 수 있는 파일 ACL을 사용한다. Token은 명령 인자·history·로그·ticket에 쓰지 않는다. Config에는 승인된 인증/TLS 설정만 있다. 자동 retry·redirect·TLS 검증 해제 설정은 없어야 한다.
- [ ] `$EvidenceDir`는 공유·동기화되지 않는 비공개 directory다. Windows 파일 ACL로 보호한다. `umask`나 chmod만으로 보호했다고 판단하지 않는다.

## 3. 절차

실행 위치는 신뢰된 Windows 관리 host다. 아래 입력은 실제 승인 배포 값으로 설정한다. `$ManagementUrl`은 현재 leader 관리 주소다. 원격 연결은 승인된 TLS 보호 경로를 사용한다. `$Feature`는 token 한 개다. `$EvidenceDir`와 `$CurlConfig`는 절대 경로다. 비밀 파일 내용은 출력하지 않는다. Node별 URL은 승인 배포 목록에서 얻는다.

안전한 proposal 복구를 켤 때는 `$Feature = 'proposal-recovery'`를 사용한다. 아래 승인·backup·member 확인 절차는 그대로 따른다. 활성화 뒤에는 새 receipt와 fence를 읽지 못하는 build로 rollback하지 않는다. BP-06 runner의 자동 활성화는 폐기 가능한 시험 cluster 전용이며 이 운영 절차를 대신하지 않는다.

`credential-revocation`은 활성화된 Dynamic cluster에서만 폐기 API를 제공한다. 활성화 전에 [credential 회전·폐기 how-to](credential-rotation-and-revocation.md)에서 배포 전제와 기존 연결 종료 동작을 확인한다.

```powershell
$ManagementUrl = 'https://leader.example.invalid'
$Feature = 'session-metadata'
$EvidenceDir = 'D:\Private\activation-evidence'
$CurlConfig = 'D:\Private\management.curl'
if ($Feature -cnotmatch '^[a-z][a-z0-9-]{0,62}$') { throw 'Invalid feature token' }
$ManagementUrl = $ManagementUrl.TrimEnd('/')
Get-Command curl.exe, jq -ErrorAction Stop | Select-Object Name, Source
```

1. **입력과 ACL을 확인하고 증거 directory를 준비한다.**

   `$EvidenceDir`는 새 directory여야 한다. 아래 명령은 상속 권한을 제거하고 현재 사용자에게만 접근 권한을 부여한다. ACL 적용 전에는 증거를 쓰지 않는다. 기존 directory를 쓰려면 먼저 동일한 비공개 ACL인지 확인한다. `$CurlConfig` ACL의 owner와 읽기 권한 대상도 현재 사용자여야 한다. 다른 사용자·그룹 또는 불필요한 상속 권한이 있으면 secret manager 담당자에게 수정 요청하고 중단한다. Administrator의 별도 권한 획득까지 막는다는 뜻은 아니다.

   ```powershell
   $Identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
   $null = New-Item -ItemType Directory -Path $EvidenceDir -ErrorAction Stop
   $Acl = New-Object System.Security.AccessControl.DirectorySecurity
   $Acl.SetOwner($Identity)
   $Acl.SetAccessRuleProtection($true, $false)
   $Rule = New-Object System.Security.AccessControl.FileSystemAccessRule($Identity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
   $Acl.AddAccessRule($Rule)
   Set-Acl -LiteralPath $EvidenceDir -AclObject $Acl -ErrorAction Stop
   Get-Acl -LiteralPath $EvidenceDir | Format-List Owner, AccessToString
   Get-Acl -LiteralPath $CurlConfig | Format-List Owner, AccessToString
   ```

   다음 함수는 body·HTTP status·header·UTC 시각·`X-Request-Id`를 별도 파일로 보존한다. `$Prefix`마다 다른 파일 이름을 쓴다. GET과 POST에 같은 curl flags를 사용한다. 5/30초는 client 예시 timeout이다. 제품 기본값·안정화 기간·commit 취소 조건이 아니다. 자동 `--retry`, `--location`, `--insecure`는 쓰지 않는다.

   ```powershell
   function Invoke-FeatureRequest {
       param([string]$Url, [ValidateSet('GET','POST')][string]$Method, [string]$Prefix)
       $BodyFile = "$Prefix.body.json"
       $HeaderFile = "$Prefix.headers"
       $RequestedAtUtc = [DateTime]::UtcNow.ToString('o')
       $HttpStatus = & curl.exe --silent --show-error --config $CurlConfig --request $Method --connect-timeout 5 --max-time 30 --dump-header $HeaderFile --output $BodyFile --write-out '%{http_code}' $Url
       $CurlExitCode = $LASTEXITCODE
       $ObservedAtUtc = [DateTime]::UtcNow.ToString('o')
       $RequestId = $null
       if (Test-Path -LiteralPath $HeaderFile) {
           $RequestId = @(Get-Content -LiteralPath $HeaderFile | Where-Object { $_ -match '^X-Request-Id:' } | ForEach-Object { ($_ -split ':', 2)[1].Trim() }) -join ','
       }
       [pscustomobject]@{ Method=$Method; HttpStatus="$HttpStatus"; CurlExitCode=$CurlExitCode; RequestId=$RequestId; RequestedAtUtc=$RequestedAtUtc; ObservedAtUtc=$ObservedAtUtc } | ConvertTo-Json | Set-Content -LiteralPath "$Prefix.result.json" -Encoding UTF8
       if ($CurlExitCode -ne 0 -or "$HttpStatus" -ne '200') {
           throw "Request stopped: curl=$CurlExitCode HTTP=$HttpStatus; POST may have committed; inspect preserved evidence"
       }
       if (-not (Test-Path -LiteralPath $BodyFile)) { throw 'Missing response; POST commit result unknown' }
       return $BodyFile
   }
   ```

   Cluster summary와 features를 조회한다.

   ```powershell
   $SummaryFile = Invoke-FeatureRequest -Url "$ManagementUrl/api/v1/cluster" -Method GET -Prefix (Join-Path $EvidenceDir 'before-cluster')
   & jq -e '.data | (.clusterId != null and .quorumAvailable == true and .underReplicated == 0)' $SummaryFile
   if ($LASTEXITCODE -ne 0) { throw 'Cluster summary check failed' }
   $FeaturesFile = Invoke-FeatureRequest -Url "$ManagementUrl/api/v1/cluster/features" -Method GET -Prefix (Join-Path $EvidenceDir 'before-features')
   ```

   `clusterId`를 승인 배포 목록과 대조한다. 함수는 HTTP 200만 허용한다. 다른 status는 파일에 남으므로 §5·§6에 따라 판단한다. POST의 성공 body를 검증하지 못하면 결과 불명이다.

2. **각 member의 실제 endpoint와 알려진 operation을 확인한다.**

   아래 두 입력은 승인 배포 목록에서 가져온다. 모든 운용 member에 대해 반복한다. Leader에서 원격 node row만 읽는 방식은 원격 disk health를 확인하지 못한다.

   ```powershell
   $NodeId = '1'
   $NodeUrl = 'https://node1.example.invalid'
   $ReadyFile = Invoke-FeatureRequest -Url "$NodeUrl/health/ready" -Method GET -Prefix (Join-Path $EvidenceDir "before-ready-$NodeId")
   $NodeFile = Invoke-FeatureRequest -Url "$NodeUrl/api/v1/nodes/$NodeId" -Method GET -Prefix (Join-Path $EvidenceDir "before-node-$NodeId")
   ```

   Node filter의 문자열 따옴표를 PowerShell 5.1의 native 인자 전달에서 보존하려고 filter 파일을 사용한다.

   ```powershell
   $NodeFilter = Join-Path $EvidenceDir 'node-check.jq'
   '.data | (.ready == true and .storagePressure == false and .replicationHealth == "HEALTHY")' | Set-Content -LiteralPath $NodeFilter -Encoding ASCII
   & jq -e -f $NodeFilter $NodeFile
   if ($LASTEXITCODE -ne 0) { throw 'Local node health check failed' }
   ```

   알려진 operation ID는 배포 기록에서 얻는다. 각 operation을 조회하고 `data.phase=SUCCEEDED`를 확인한다. 실패·진행 중·누락이면 중단한다. Operation이 없는 배포에는 임의 ID를 만들지 않는다.

   ```powershell
   $OperationId = '배포 기록의 operation ID'
   $OperationFile = Invoke-FeatureRequest -Url "$ManagementUrl/api/v1/cluster/membership-operations/$OperationId" -Method GET -Prefix (Join-Path $EvidenceDir "operation-$OperationId")
   & jq -r '.data.phase' $OperationFile
   if ($LASTEXITCODE -ne 0) { throw 'Membership operation check failed' }
   ```

   `/api/v1/cluster/members` GET은 없다. Feature 응답에도 phase·Raft index가 없다. Binary 교체·drain 완료는 배포 기록과 기존 조회 결과로 확인한다.

3. **전체 feature 광고와 승인 build 목록을 대조한다.**

   ```powershell
   & jq -e --arg feature $Feature '.data.members | (length > 0 and all(.[]; (.features | index($feature)) != null))' $FeaturesFile
   if ($LASTEXITCODE -ne 0) { throw 'Feature advertisement check failed' }
   & jq '.data' $FeaturesFile
   if ($LASTEXITCODE -ne 0) { throw 'Feature response check failed' }
   ```

   모든 non-retired ID·release·features를 승인 목록과 수동 대조한다. Staged member도 포함한다. 누락·예상 밖 member·미지원 feature·승인하지 않은 build이면 POST하지 않는다. Release는 same-major 인접 minor window 안이어야 한다. Patch나 모든 release가 같아야 한다고 요구하지 않는다. 이미 active이면 성공한 GET의 token을 기존 commit 증거로 기록한다. Window를 다시 열거나 새 activation으로 기록하지 않는다.

4. **사전 backup과 지원 복구 경로를 확인한다.**

   [Backup 절차](cluster-backup-restore.md)로 commit-boundary archive를 확보한다. [Backup v3 §2·§4](../contracts/cluster-backup-v3.md#4-호환성-규칙)로 restore build window·active feature 지원·세 bootstrap voter identity·learner 부재를 대조한다. Archive commit index·digest, 보호된 위치와 동일 identity 복구 시험 증거를 기록한다. 오래된 historical consumer의 exact-release 제한도 확인한다. 지원 경로가 없으면 승인을 보류한다. Backup payload나 승인 기록을 feature POST body로 보내지 않는다.

5. **외부 ticket 또는 배포 기록에 승인을 남긴다.**

   | 필수 항목 | 기록할 내용 |
   | --- | --- |
   | 대상 | Cluster ID, feature token 한 개 |
   | 승인 배포 집합 | 모든 non-retired member IDs·관리 URL·승인 build·release·features, 기존 activeFeatures |
   | 포기하는 rollback | 해당 feature를 지원하지 않는 이전 build identity |
   | 시험 증거 | Target/rollback release matrix, feature 기능 시험 링크와 진행 중인 배포 결함 없음 |
   | 열린 gate | Production storage, retention/RPO/RTO, 게시 release 등 해당 배포의 열린 gate |
   | 복구 증거 | 보호된 backup 위치·commit index·digest, restore build·동일 identity 구성·복구 시험 링크 |
   | 책임과 승인 | 승인자·실행자, UTC 승인 시각, “이 feature를 지원하지 않는 이전 build로의 무손실 in-place rollback을 포기한다”는 문구 |

   이 표는 새 JSON schema나 runtime state가 아니다. 승인 시각만으로 window가 종료되지는 않는다.

6. **POST 직전에 재조회하고 같은 배포 집합이면 한 번 실행한다.**

   단계 1–3의 cluster·features·node readiness/local health와 알려진 operation을 새 파일 prefix로 다시 조회한다. Member/release/features·activeFeatures 집합이나 배포 상태가 바뀌면 재확인·재승인한다. Feature 조회는 두 상태 조회이며 atomic snapshot이 아니다. 조회와 commit 사이의 경쟁에는 기존 apply의 전체 member 광고 검증이 적용된다.

   ```powershell
   $ActivationFile = Invoke-FeatureRequest -Url "$ManagementUrl/api/v1/cluster/features/$Feature" -Method POST -Prefix (Join-Path $EvidenceDir 'activation')
   & jq -e --arg feature $Feature '.data.activeFeatures | index($feature) != null' $ActivationFile
   if ($LASTEXITCODE -ne 0) { throw 'POST commit result unknown; do not roll back' }
   ```

   Body, approval field, 새 approval header 또는 `Idempotency-Key`를 보내지 않는다. ClusterAdmin 인증을 사용한다. HTTP 200과 active token filter exit 0을 모두 확인해야 한다.

7. **Commit 상태와 node readiness를 다시 확인하고 기록한다.**

   ```powershell
   $AfterFile = Invoke-FeatureRequest -Url "$ManagementUrl/api/v1/cluster/features" -Method GET -Prefix (Join-Path $EvidenceDir 'after-features')
   & jq -e --arg feature $Feature '.data.activeFeatures | index($feature) != null' $AfterFile
   if ($LASTEXITCODE -ne 0) { throw 'Commit not confirmed by this observation; inspect request history' }
   ```

   각 node의 readiness도 단계 2와 같은 명령으로 새 prefix에 기록한다. 요청 UTC 시각·최초 commit 관찰 UTC 시각·status·request ID와 비밀을 제거한 응답을 승인 기록에 연결한다. API가 반환하지 않는 정확한 commit 시각이나 Raft index를 쓰지 않는다. 불명 POST 뒤 한 번의 GET에서 token이 없다는 이유로 rollback하지 않는다. 실패 분기는 [Release contract §4.1](../contracts/cluster-release-compatibility-v1.md#41-운영-승인과-rollback-window-종료)을 따른다.

## 4. 검증

과거 local 실행은 [Cluster 검증 기록 §11.30](../verification/cluster-session-and-delivery.md#1130-feature-activation-운영-승인-수용-매핑)을 따른다. 이는 현재 코드의 통과가 아니다. 현재 연결·실행 결과는 [시나리오 보고서](../../.artifacts/verification/scenarios/report.md)를 확인한다. 실제 운영 cluster activation과 production 승인·ACL 검토는 미실행이다.

| 관찰 대상 | 성공 조건 | 확인 명령·방법 |
| --- | --- | --- |
| API·member 확인 | Summary·features·각 local node·readiness가 200이고, 광고 filter exit 0. 진행 중 membership operation이 없음 | 단계 1–3과 해당 배포의 operation 조회 |
| Activation·동일 token replay | 200과 active token, replay 뒤 token 하나 유지, GET·세 readiness 200 | 단계 6–7의 curl·jq |
| 승인 분기 | 결과 불명은 rollback 금지, 배포 집합 변경은 재승인 | 외부 승인 기록과 응답 대조. 문서 판정은 runtime 강제 시험이 아님 |
| Production 승인·ACL | 실제 credential·배포·backup이 승인 기록과 일치 | 운영 책임자의 별도 검토. Local smoke는 대신하지 못함 |

## 5. 중단 조건

- Quorum 없음, `underReplicated!=0`, 어느 운용 member의 readiness 비200, local pressure 또는 local health missing·unknown·오류이면 승인·POST를 보류한다.
- Feature 광고 누락, 예상 밖 member, 승인하지 않은 build, 지원 window 밖 release 또는 승인 뒤 집합·배포 상태 변화이면 중단하고 재승인한다.
- 진행 중인 배포 결함, matrix/feature 시험 증거 없음 또는 지원 backup/restore 경로 없음이면 승인하지 않는다.
- 처음 POST의 `409 feature_not_supported_by_all_members`이면 이번 요청은 미활성화다. 배포를 수정하고 새 승인을 받는다. 이전 결과 불명 POST가 있으면 이 409로 이전 요청도 실패했다고 판단하지 않는다.
- 400·401·403이면 입력·권한을 수정한다. `not_leader`이면 현재 leader endpoint를 확인한다. 이전 불명 요청 이력을 유지한다.
- Timeout·연결 종료·500·503·검증 불가능한 성공 응답이면 결과 불명으로 기록한다. 새 target/feature를 승인하거나 미지원 build로 rollback하지 않는다.

## 6. 이전 상태로 되돌리는 롤백 또는 복구

1. 아직 POST하지 않았고 이전 결과 불명 요청도 없으면 승인만으로 window가 닫히지 않는다. Activation 전 기존 release 호환성 규칙을 따른다.
2. 결과 불명이면 quorum을 복구하고 기존 승인의 유효성을 확인한다. 같은 token의 idempotent POST를 재실행한다. 승인 대상이 바뀌면 재확인·재승인하되 이전 요청을 실패로 처리하지 않는다. 200+token 또는 성공 GET+token으로 종료를 확인한다. 확인할 수 없으면 이관한다.
3. Activation 뒤에는 지원 build로 전진 수정하거나 같은 volume에 지원 build를 다시 배포한다. Active feature·snapshot field·WAL·metadata를 지워 downgrade하지 않는다.
4. Activation 전 backup restore는 backup 이후 commit 손실이 가능한 disaster recovery다. 무손실 rollback으로 보고하지 않는다. 별도 장애 복구 승인을 받은 뒤 [Backup 절차](cluster-backup-restore.md)와 최신 contract를 따른다.

## 7. 해결되지 않을 때 전달할 대상

- 전달 대상: 해당 배포의 release 담당자, cluster 운영 당직자와 NaruMQ 개발 담당자.
- 보존할 증거: cluster ID·feature·승인 배포 집합, approval/request/최초 commit 관찰 UTC 시각, status·request ID·비밀을 제거한 body·readiness, 불명 요청 이력, 보호된 backup 위치·commit index·digest와 지원 restore 증거.
- 비밀 값 제외 규칙: bearer token·curl secret config·private key·certificate private data·backup payload를 공유 로그나 ticket에 넣지 않는다. 비공개 증거에서 필요한 항목만 비밀을 제거해 전달한다.
