# MQTT credential을 회전하고 폐기하는 방법

이 절차는 인증 설정을 재시작으로 교체하고, Dynamic cluster에서 이전 `credentialId`를 폐기한다. 보안 설정은 시작할 때만 읽는다. 운영 중 reload와 MQTT 5 `AUTH` 재인증은 제공하지 않는다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | SecurityAdmin·ClusterAdmin과 배포 운영자 |
| 검증 상태 | Draft (저장소 내 3-node process acceptance 범위; production 절차 미검증) |
| 관련 기준 | [SEC-07 §2.3·§2.4·§7·§9](../specs/tenancy-and-security.md), [ADR-0049](../decisions/ADR-0049-mqtt-credential-revocation.md), [Management API v1](../contracts/management-api-v1.md), [Cluster feature activation 승인 절차](cluster-feature-activation.md) |

## 1. 적용 범위와 제한

- `credential-revocation` feature가 활성화된 Dynamic cluster에서만 revoke·restore API를 사용할 수 있다. 먼저 [feature activation 절차](cluster-feature-activation.md)를 따른다.
- `SecurityAdmin` 또는 `ClusterAdmin` role이 필요하다. API는 committed 상태만 반환한다. Quorum이 없으면 revoke·restore와 committed list 조회가 실패한다.
- `credentialId` 전체를 폐기한다. JWT, password와 client mTLS 인증에 같은 규칙을 적용한다. ID는 UTF-8 1~512 bytes여야 한다. Cluster는 최대 4096개 ID를 보관한다.
- Password identity는 같은 `subject`를 공유할 수 있다. 각 identity의 username과 `credentialId`는 고유해야 한다.
- JWT identity는 `(issuer, subject)` binding 하나만 허용한다. 새 JWT `credentialId`로 전환할 때 새 static binding을 설정하고, client token도 그 binding의 subject를 사용해야 한다.
- Certificate mapping selector는 고유해야 한다. 새 certificate identity는 새 static binding으로 배포한다.
- `DELETE`는 이전 `credentialId`를 복구한다. 복구한 credential은 다시 인증에 사용할 수 있으므로 장애 복구 외 용도로 실행하지 않는다.
- Standalone mode에는 revoke·restore API가 없다. JWT `exp`와 client certificate `NotAfter`에 따른 연결 종료는 standalone에도 적용한다.

## 2. 사전 준비

1. 새 credential과 새 `credentialId`를 준비한다. 기존 credential은 아직 설정에서 제거하지 않는다.
2. JWT라면 고유 `(issuer, subject)` binding을, mTLS라면 고유 certificate selector를 준비한다. Password는 고유 username과 `credentialId`를 사용한다.
3. 새 binding을 전체 node의 security configuration에 반영한다. 설정 변경에는 승인된 배포의 rolling-restart 절차를 사용한다. 이 저장소는 security configuration 배포용 rollout controller를 제공하지 않는다.
4. 각 node의 readiness를 확인하고, 각 listener에서 새 credential로 CONNECT가 성공하는지 확인한다.
5. Client를 새 credential로 전환한다. MQTT 5 `AUTH` 재인증은 지원하지 않으므로 client를 새 credential로 다시 연결한다.
6. 이전 `credentialId`를 사용 중인 client와 연결이 남아 있는지 확인한다.

새 credential 인증이나 node readiness가 실패하면 이전 credential을 폐기하지 않는다. 먼저 배포 상태를 복구한다.

## 3. 이전 credential 폐기

API token은 명령 기록에 노출하지 않는다. 승인된 secret manager가 만든 보호된 curl config 파일을 사용한다. `$EvidenceDir`는 현재 운영자만 읽을 수 있는 비공개 directory여야 한다. 아래 요청 body와 revocation list를 공유 위치에 저장하지 않는다.

```powershell
$ManagementUrl = 'https://leader.example.invalid'
$CredentialId = 'old-credential-id'
$CurlConfig = 'D:\Private\management.curl'
$EvidenceDir = 'D:\Private\credential-rotation'
$EncodedCredentialId = [uri]::EscapeDataString($CredentialId)
$RevocationUrl = "$($ManagementUrl.TrimEnd('/'))/api/v1/security/credential-revocations/$EncodedCredentialId"

curl.exe --silent --show-error --config $CurlConfig --connect-timeout 5 --max-time 30 `
  --request POST --output (Join-Path $EvidenceDir 'revoke-response.json') --write-out '%{http_code}' $RevocationUrl
```

HTTP `200`과 응답의 `data.credentialId`, `data.revoked=true`를 모두 확인한다. Timeout, 연결 종료 또는 응답을 판정할 수 없는 오류는 결과 불명이다. 이때 revoke 요청을 실패로 단정하지 말고, 새 요청 전에 GET으로 committed 상태를 조회한다.

```powershell
$NodeUrls = @('https://node1.example.invalid', 'https://node2.example.invalid', 'https://node3.example.invalid')
foreach ($NodeIndex in 0..($NodeUrls.Count - 1)) {
  $NodeListUrl = "$($NodeUrls[$NodeIndex].TrimEnd('/'))/api/v1/security/credential-revocations"
  $ListPath = Join-Path $EvidenceDir "revocations-node-$($NodeIndex + 1).json"
  $HttpStatus = & curl.exe --silent --show-error --config $CurlConfig --connect-timeout 5 --max-time 30 `
    --output $ListPath --write-out '%{http_code}' $NodeListUrl
  if ($LASTEXITCODE -ne 0 -or $HttpStatus -ne '200') { throw "Credential list request failed on node $($NodeIndex + 1)" }
}
```

`data.revocations`에 이전 `credentialId`가 있는지 확인한다. 각 node의 실제 Management endpoint에서 목록을 조회한다. 각 node가 같은 committed entry를 반환해야 한다.

## 4. 적용 결과 확인

- 각 node에서 이전 credential로 새 CONNECT를 시도한다. MQTT 3.1.1은 CONNACK `0x05`, MQTT 5는 CONNACK `0x87`로 거절되어야 한다.
- 이전 credential로 열린 MQTT 3.1.1 연결은 즉시 닫혀야 한다. MQTT 5 연결은 DISCONNECT `0x87`을 받고 2초 안에 닫혀야 한다.
- 폐기된 연결의 Will은 발행되지 않아야 한다. 새 credential의 연결은 계속 사용할 수 있어야 한다.
- MQTT CONNECT 거절과 지연 Will 거절은 data-plane audit의 `credential_revoked` reason으로 기록한다. Management audit에는 `security.credential.revoke` action과 `mqtt_credential` target이 있어야 한다.
- `GET /api/v1/nodes/{nodeId}/audit/events`는 node-local 조회다. 각 node에서 해당 node의 canonical decimal ID를 path에 넣어 조회한다. 원격 node ID로 요청하면 `409 node_not_local`을 반환한다.

각 node의 committed list가 다르거나 이전 credential이 성공하면 추가 폐기를 반복하지 말고 quorum·node readiness와 요청 이력을 확인한다.

## 5. 이전 설정 정리

모든 client가 새 credential을 사용하고, 이전 연결이 종료된 뒤 다음 승인된 rolling restart에서 이전 identity와 secret을 security configuration에서 제거한다. 폐기 상태는 replicated state에 남는다. 이전 ID를 다시 허용해야 할 때만 `DELETE /api/v1/security/credential-revocations/{credentialId}`를 사용한다.

JWT `exp` 또는 client certificate `NotAfter`가 되면 연결을 종료한다. MQTT 5는 DISCONNECT `0xA0` 뒤 2초 이내에 닫는다. MQTT 3.1.1은 즉시 닫고 Will을 기존 비정상 종료 규칙으로 처리한다. Password와 익명 principal에는 credential expiry timer가 없다.

## 6. 보안 경계

API credential, curl config, private key와 security config의 secret 값을 log·ticket·공유 artifact에 넣지 않는다. Process acceptance 결과는 production IdP, secret manager, 배포 rollout controller 또는 실제 certificate lifecycle의 검증이 아니다.
