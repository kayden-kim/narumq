"""EKS acceptance observer for the Argo CD health consumer of NaruMQRollout (DEP-18).

Argo CD must already have synced the test `NaruMQRollout` named `health-probe` from Git into the
namespace and the Application must exist. The observer injects each status of `health_matrix.json`
through the status subresource, requests a hard refresh, and passes a case only when the Git sync is
`Synced`, there is no comparison error, and the resource and the Application both report the expected
health. The CR UID and spec must not change and no broker Pod may run.

The run directory name is the kubectl context. `--private` holds `kubectl.exe` and `kubeconfig`.
AWS credentials come from the environment (`AWS_PROFILE`, `AWS_REGION`) for kubeconfig exec plugins.
Run it only against a disposable fixture. It writes results to the run directory.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.append(str(Path(__file__).resolve().parents[2]))  # tests/ holds scenario_results.py
from scenario_results import scenario_result  # noqa: E402

DEFAULT_CASES = Path(__file__).resolve().parent / "health_matrix.json"
DEFAULT_NAMESPACE = "narumq-argo-health"
DEFAULT_APPLICATION = "narumq-rollout-health"


# Scenarios: drain-and-rolling-update.md:DRN-SCN-025
@scenario_result
def scenario_argo_health(private: Path, out: Path, cases_path: Path = DEFAULT_CASES, ns: str = DEFAULT_NAMESPACE,
                         app_name: str = DEFAULT_APPLICATION) -> dict:
    run = out.name
    env = os.environ.copy()
    env.update(AWS_PAGER='')
    kubectl_binary = private / 'kubectl.exe'
    k = [str(kubectl_binary if kubectl_binary.exists() else 'kubectl'), '--kubeconfig', str(private / 'kubeconfig'), '--context', run]

    def kubectl(args):
        p = subprocess.run(k + args, env=env, capture_output=True, text=True, encoding='utf-8', timeout=360)
        if p.returncode:
            raise RuntimeError(p.stderr)
        return p.stdout

    def read_app():
        return json.loads(kubectl(['get', 'application', app_name, '-n', 'argocd', '-o', 'json']))

    def save(name, data):
        (out / name).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')

    cases = json.loads(cases_path.read_text(encoding='utf-8'))['cases']
    results = []
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    try:
        kubectl(['annotate', 'application', app_name, '-n', 'argocd', 'argocd.argoproj.io/refresh=hard', '--overwrite'])
        print(kubectl(['wait', '--for=create', 'narumqrollout/health-probe', '-n', ns, '--timeout=300s']), flush=True)
        original = json.loads(kubectl(['get', 'narumqrollout', 'health-probe', '-n', ns, '-o', 'json']))
        save('argo-synced-resource.json', original)
        save('argo-initial-application.json', read_app())
        for case in cases:
            desired = case['status']
            if desired is None:
                patch_type = 'merge'
                patch = {'status': None}
            else:
                patch_type = 'json'
                patch = [{'op': 'add', 'path': '/status', 'value': desired}]
            changed = json.loads(kubectl(['patch', 'narumqrollout', 'health-probe', '-n', ns,
                                          '--subresource=status', '--type=' + patch_type,
                                          '-p', json.dumps(patch), '-o', 'json']))
            kubectl(['annotate', 'application', app_name, '-n', 'argocd', 'argocd.argoproj.io/refresh=hard', '--overwrite'])
            t0 = time.monotonic()
            samples = []
            while True:
                app = read_app()
                status = app.get('status', {})
                resources = [r for r in status.get('resources', []) if r.get('kind') == 'NaruMQRollout' and r.get('name') == 'health-probe']
                resource_health = resources[0].get('health', {}) if resources else {}
                app_health = status.get('health', {}).get('status')
                sync = status.get('sync', {}).get('status')
                conditions = status.get('conditions', [])
                samples.append({'seconds': round(time.monotonic() - t0, 3), 'applicationHealth': app_health,
                                'resourceHealth': resource_health, 'sync': sync, 'conditions': conditions})
                if (resource_health.get('status') == case['expectedHealth']
                        and resource_health.get('message', '') == case['expectedMessage']
                        and app_health == case['expectedHealth'] and sync == 'Synced'
                        and not any(c.get('type', '').endswith('Error') for c in conditions)):
                    break
                if time.monotonic() - t0 > 180:
                    save('health-timeout-application.json', app)
                    save('health-timeout-case.json', {'case': case, 'samples': samples, 'patchedResource': changed})
                    raise RuntimeError('health did not converge: ' + case['name'])
                time.sleep(2)
            result = {'name': case['name'], 'result': 'PASS', 'inputStatus': desired,
                      'expectedHealth': case['expectedHealth'], 'expectedMessage': case['expectedMessage'],
                      'applicationHealth': app_health, 'resourceHealth': resource_health, 'sync': sync,
                      'resourceUID': changed['metadata']['uid'], 'resourceVersion': changed['metadata']['resourceVersion'],
                      'applicationResourceVersion': app['metadata']['resourceVersion'],
                      'observedAt': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'samples': samples}
            results.append(result)
            save('health-matrix-progress.json', results)
            print(case['name'] + ': PASS ' + app_health + ' / ' + str(resource_health), flush=True)
        final = json.loads(kubectl(['get', 'narumqrollout', 'health-probe', '-n', ns, '-o', 'json']))
        assert original['metadata']['uid'] == final['metadata']['uid']
        assert original['spec'] == final['spec']
        pods = json.loads(kubectl(['get', 'pods', '-n', ns, '-o', 'json']))['items']
        assert not pods, 'health consumer test must not run broker pods'
        outcome = {'runId': run, 'result': 'PASS', 'startedAt': started,
                   'completedAt': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'cases': results,
                   'sameCRUIDAndSpec': True, 'brokerPods': 0, 'scope': 'Argo CD real Git sync and health consumer; synthetic CR status injection only; no broker drain, delete or release acceptance'}
        save('health-matrix-result.json', outcome)
        print(kubectl(['get', 'application', app_name, '-n', 'argocd']), flush=True)
        return outcome
    except Exception as exc:
        save('health-matrix-failure.json', {'runId': run, 'startedAt': started, 'error': str(exc), 'completedCases': results})
        raise


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--private', type=Path, required=True, help='directory with kubectl.exe and kubeconfig')
    parser.add_argument('--output', type=Path, required=True, help='existing run directory; its name is the kubectl context')
    parser.add_argument('--cases', type=Path, default=DEFAULT_CASES)
    parser.add_argument('--namespace', default=DEFAULT_NAMESPACE)
    parser.add_argument('--application', default=DEFAULT_APPLICATION)
    args = parser.parse_args(argv)
    scenario_argo_health(args.private, args.output, args.cases, args.namespace, args.application)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
