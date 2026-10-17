"""Run the PowerShell/curl/jq feature-activation smoke against a disposable three-voter fixture.

The caller must provide a fresh synthetic 1.2.0 fixture advertising compatibility-probe,
node IDs 1/2/3, a private curl config and a new private evidence directory. This activates
compatibility-probe irreversibly; never use a production cluster.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

sys.path.append(str(Path(__file__).resolve().parents[2]))
from scenario_results import scenario_result


SCRIPT = r'''
$ErrorActionPreference = 'Stop'
$ManagementUrl = $env:NARUMQ_SMOKE_MANAGEMENT_URL.TrimEnd('/')
$Feature = 'compatibility-probe'
$CurlConfig = $env:NARUMQ_SMOKE_CURL_CONFIG
$EvidenceDir = $env:NARUMQ_SMOKE_EVIDENCE
$Jq = $env:NARUMQ_SMOKE_JQ
$Nodes = ConvertFrom-Json $env:NARUMQ_SMOKE_NODES
$Identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$null = New-Item -ItemType Directory -Path $EvidenceDir -ErrorAction Stop
$Acl = New-Object System.Security.AccessControl.DirectorySecurity
$Acl.SetOwner($Identity)
$Acl.SetAccessRuleProtection($true, $false)
$Rule = New-Object System.Security.AccessControl.FileSystemAccessRule($Identity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
$Acl.AddAccessRule($Rule)
Set-Acl -LiteralPath $EvidenceDir -AclObject $Acl -ErrorAction Stop
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
    if ($CurlExitCode -ne 0 -or "$HttpStatus" -ne '200') { throw 'Request stopped; POST result may be unknown; do not roll back' }
    if (-not (Test-Path -LiteralPath $BodyFile)) { throw 'Missing response; POST commit result unknown' }
    return $BodyFile
}
function Assert-Jq {
    param([string]$Filter, [string]$File, [string]$Name)
    $FilterFile = Join-Path $EvidenceDir "$Name.jq"
    $Filter | Set-Content -LiteralPath $FilterFile -Encoding ASCII
    & $Jq -e --arg feature $Feature -f $FilterFile $File
    if ($LASTEXITCODE -ne 0) { throw "jq check failed: $Name" }
}
foreach ($Phase in @('before', 'recheck')) {
    $Summary = Invoke-FeatureRequest -Url "$ManagementUrl/api/v1/cluster" -Method GET -Prefix (Join-Path $EvidenceDir "$Phase-cluster")
    Assert-Jq '.data | (.clusterId != null and .quorumAvailable == true and .underReplicated == 0)' $Summary "$Phase-summary"
    $Features = Invoke-FeatureRequest -Url "$ManagementUrl/api/v1/cluster/features" -Method GET -Prefix (Join-Path $EvidenceDir "$Phase-features")
    Assert-Jq '.data.members | (length > 0 and all(.[]; (.features | index($feature)) != null))' $Features "$Phase-advertisement"
    Assert-Jq '.data | (.activeFeatures == [] and ([.members[].nodeId] | sort) == ["1","2","3"] and all(.members[]; .releaseVersion == "1.2.0"))' $Features "$Phase-identities"
    foreach ($Node in $Nodes) {
        $null = Invoke-FeatureRequest -Url "$($Node.url)/health/ready" -Method GET -Prefix (Join-Path $EvidenceDir "$Phase-ready-$($Node.id)")
        $Local = Invoke-FeatureRequest -Url "$($Node.url)/api/v1/nodes/$($Node.id)" -Method GET -Prefix (Join-Path $EvidenceDir "$Phase-node-$($Node.id)")
        Assert-Jq '.data | (.ready == true and .storagePressure == false and .replicationHealth == "HEALTHY")' $Local "$Phase-node-$($Node.id)"
    }
}
foreach ($Phase in @('activation', 'replay')) {
    $Activation = Invoke-FeatureRequest -Url "$ManagementUrl/api/v1/cluster/features/$Feature" -Method POST -Prefix (Join-Path $EvidenceDir $Phase)
    Assert-Jq '.data.activeFeatures | index($feature) != null' $Activation "$Phase-active"
    Assert-Jq '.data.activeFeatures == [$feature]' $Activation "$Phase-exact"
}
$After = Invoke-FeatureRequest -Url "$ManagementUrl/api/v1/cluster/features" -Method GET -Prefix (Join-Path $EvidenceDir 'after-features')
Assert-Jq '.data.activeFeatures == [$feature]' $After 'after-exact'
foreach ($Node in $Nodes) {
    $null = Invoke-FeatureRequest -Url "$($Node.url)/health/ready" -Method GET -Prefix (Join-Path $EvidenceDir "after-ready-$($Node.id)")
}
'''


# Scenarios: cluster-session-and-delivery.md:CLU-SCN-220, cluster-session-and-delivery.md:CLU-SCN-221
@scenario_result
def scenario_feature_activation_runbook(args) -> dict:
    """Exercise the runbook's real native commands; any HTTP/jq/PowerShell failure fails the scenario."""
    nodes = []
    for value in args.node_url:
        node_id, url = value.split('=', 1)
        nodes.append({'id': node_id, 'url': url.rstrip('/')})
    if sorted(node['id'] for node in nodes) != ['1', '2', '3']:
        raise ValueError('--node-url must provide exactly IDs 1, 2, 3')
    evidence = args.evidence_dir.resolve()
    if evidence.exists():
        raise ValueError('--evidence-dir must not exist')
    config = args.curl_config.resolve(strict=True)
    environment = dict(os.environ, NARUMQ_SMOKE_MANAGEMENT_URL=args.management_url,
                       NARUMQ_SMOKE_CURL_CONFIG=str(config), NARUMQ_SMOKE_EVIDENCE=str(evidence),
                       NARUMQ_SMOKE_JQ=args.jq_command, NARUMQ_SMOKE_NODES=json.dumps(nodes))
    # Only paths/endpoints are passed here. Credentials remain in the caller-protected curl config.
    with tempfile.TemporaryDirectory(prefix='narumq-feature-smoke-') as directory:
        script = Path(directory) / 'smoke.ps1'
        script.write_text(SCRIPT, encoding='utf-8')
        subprocess.run([args.powershell_command, '-NoProfile', '-NonInteractive', '-File', str(script)],
                       env=environment, check=True, timeout=600)
    return {'result': 'PASS', 'evidenceDirectory': str(evidence)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--management-url', required=True, help='current leader URL of the disposable fixture')
    parser.add_argument('--node-url', action='append', required=True, help='ID=local-management-URL; repeat for IDs 1/2/3')
    parser.add_argument('--curl-config', type=Path, required=True, help='caller-protected private curl config')
    parser.add_argument('--evidence-dir', type=Path, required=True, help='new private directory (ACL applied before responses)')
    parser.add_argument('--powershell-command', default='powershell.exe')
    parser.add_argument('--jq-command', default='jq')
    parser.add_argument('--acknowledge-disposable-fixture', action='store_true', required=True)
    scenario_feature_activation_runbook(parser.parse_args())


if __name__ == '__main__':
    main()
