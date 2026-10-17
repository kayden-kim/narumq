"""EKS acceptance observer for a planned cross-worker same-EBS follower relocation (DEP-17, DEP-21).

The run directory holds `relocation-plan.json` (target pod and node, old and new worker and EC2
instance, PVC, PV, EBS volume and zone). Its name is the cluster ID and the kubectl context. `--private`
holds `kubectl.exe`, `kubeconfig`, `admin.token` and `mqtt.password`. AWS credentials come from the
environment (`AWS_PROFILE`, `AWS_REGION`).

The survivor Pod's management and MQTT ports and the target Pod's management port must already be
forwarded to localhost (defaults 23880, 23883 and 28880). The observer cordons the old worker, starts the
real drain, waits for `SAFE_TO_STOP`, and only then deletes the target Pod with UID and resourceVersion
preconditions. It then checks that the new Pod runs on the independent worker with the same PVC, PV and
EBS volume, that three brokers are Ready, and that quorum and PUBACK held in every survivor sample.

Run `mqtt_oracle.py prepare` before and `mqtt_oracle.py verify` after this observer. Run it only against
a disposable fixture. It writes results to the run directory.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

import paho.mqtt.client as mqtt

sys.dont_write_bytecode = True
sys.path.append(str(Path(__file__).resolve().parents[2]))  # tests/ holds scenario_results.py
from scenario_results import scenario_result  # noqa: E402


# Scenarios: drain-and-rolling-update.md:DRN-SCN-034
@scenario_result
def scenario_planned_relocation(private: Path, out: Path, survivor_management_port: int = 23880,
                                survivor_mqtt_port: int = 23883, target_management_port: int = 28880) -> dict:
    run = out.name
    plan = json.loads((out / 'relocation-plan.json').read_text())
    env = os.environ.copy()
    env.update(AWS_PAGER='')
    kubectl_binary = private / 'kubectl.exe'
    K = [str(kubectl_binary if kubectl_binary.exists() else 'kubectl'), '--kubeconfig', str(private / 'kubeconfig'), '--context', run]
    token = (private / 'admin.token').read_text().strip()
    password = (private / 'mqtt.password').read_text().strip()

    def execute(args, stdin=None):
        p = subprocess.run(args, env=env, input=stdin, text=True, encoding='utf-8', capture_output=True, timeout=90)
        if p.returncode:
            raise RuntimeError(p.stderr)
        return p.stdout

    def kubectl(args, stdin=None):
        return execute(K + args, stdin)

    def save(name, obj):
        (out / name).write_text(json.dumps(obj, indent=2), encoding='utf-8')

    def api(port, path, method='GET', body=None):
        headers = {'Authorization': 'Bearer ' + token}
        if body is not None:
            headers.update({'Content-Type': 'application/json', 'Idempotency-Key': run})
        request = urllib.request.Request('http://127.0.0.1:' + str(port) + path,
                  data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.load(response)['data']

    def pod():
        return json.loads(kubectl(['get', 'pod', plan['targetPod'], '-n', 'narumq', '-o', 'json']))

    def pods():
        return json.loads(kubectl(['get', 'pods', '-n', 'narumq', '-l', 'app=narumq', '-o', 'json']))['items']

    def ready(p):
        return any(c['type'] == 'Ready' and c['status'] == 'True' for c in p.get('status', {}).get('conditions', []))

    monitor_stop = threading.Event()
    connected = threading.Event()
    monitor_samples = []
    connect_errors = []
    monitor = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='ebs-worker-publisher',
                          protocol=mqtt.MQTTv5, reconnect_on_failure=False)
    monitor.username_pw_set('acceptance-device', password)

    def connected_callback(client, userdata, flags, reason, properties):
        if reason.is_failure:
            connect_errors.append(str(reason))
        connected.set()
    monitor.on_connect = connected_callback
    puback_reasons = {}

    def published_callback(client, userdata, mid, reason, properties):
        puback_reasons[mid] = {'reason': str(reason), 'reasonValue': reason.value, 'success': not reason.is_failure}
    monitor.on_publish = published_callback
    monitor.connect('127.0.0.1', survivor_mqtt_port, 30, clean_start=True)
    monitor.loop_start()
    assert connected.wait(15) and not connect_errors, connect_errors

    def sample_loop():
        sequence = 0
        while not monitor_stop.is_set():
            sample = {'sequence': sequence, 'at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
            try:
                cluster = api(survivor_management_port, '/api/v1/cluster')
                sample['quorumAvailable'] = cluster['quorumAvailable']
                sample['leaderNodeId'] = cluster['leaderNodeId']
                sample['nodesByState'] = cluster['nodesByState']
                published = monitor.publish('acceptance/' + run + '/continuity', str(sequence).encode(), qos=1)
                published.wait_for_publish(timeout=10)
                ack = puback_reasons.get(published.mid, {})
                sample['pubackReason'] = ack
                sample['puback'] = published.is_published() and published.rc == mqtt.MQTT_ERR_SUCCESS and ack.get('success') is True
            except Exception as exc:
                sample['error'] = str(exc)
            monitor_samples.append(sample)
            sequence += 1
            monitor_stop.wait(2)

    thread = threading.Thread(target=sample_loop, daemon=True)
    thread.start()
    start = time.monotonic()
    try:
        initial = api(survivor_management_port, '/api/v1/cluster')
        assert initial['clusterId'] == run and initial['quorumAvailable'] is True
        assert str(initial['leaderNodeId']) != plan['targetNodeId'], 'target must be a follower'
        old = pod()
        assert old['metadata']['uid'] == plan['oldPodUID'] and old['spec']['nodeName'] == plan['oldWorkerName']
        assert all(ready(p) for p in pods()), 'all voters must be Ready before planned relocation'
        print(kubectl(['cordon', plan['oldWorkerName']]), flush=True)
        drain_path = '/api/v1/nodes/' + plan['targetNodeId'] + '/drain'
        drain = api(target_management_port, drain_path, 'POST', {})
        drain_samples = [drain]
        drain_start = time.monotonic()
        while drain['phase'] != 'COMPLETED':
            assert drain['phase'] not in ['FAILED', 'CANCELLED'], drain
            assert time.monotonic() - drain_start < 180, 'drain did not finish; do not delete'
            time.sleep(2)
            drain = api(target_management_port, drain_path)
            drain_samples.append(drain)
        assert drain['safeToStop'] is True and drain['state'] == 'SAFE_TO_STOP', drain
        assert drain['failure'] is None and drain['connections']['remaining'] == 0
        assert drain['replication']['pendingWal'] == 0 and drain['replication']['underReplicated'] == 0
        current = pod()
        assert current['metadata']['uid'] == plan['oldPodUID']
        save('safe-before-delete.json', {'drain': drain, 'samples': drain_samples,
             'podUID': current['metadata']['uid'], 'podResourceVersion': current['metadata']['resourceVersion'],
             'deleteNotYetRequested': True})
        delete_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        options = {'apiVersion': 'v1', 'kind': 'DeleteOptions', 'preconditions': {
                   'uid': current['metadata']['uid'], 'resourceVersion': current['metadata']['resourceVersion']}}
        result = json.loads(kubectl(['delete', '--raw', '/api/v1/namespaces/narumq/pods/' + plan['targetPod'],
                                     '-f', '-'], json.dumps(options)))
        save('pod-delete-response.json', {'requestedAt': delete_time, 'preconditions': options['preconditions'],
             'responseKind': result.get('kind'), 'responseUID': result.get('metadata', {}).get('uid')})
        pod_samples = []
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            listed = pods()
            candidate = next((p for p in listed if p['metadata']['name'] == plan['targetPod']), None)
            pod_samples.append({'seconds': round(time.monotonic() - start, 3), 'pods': [
                {'name': p['metadata']['name'], 'uid': p['metadata']['uid'],
                 'worker': p.get('spec', {}).get('nodeName'), 'ready': ready(p),
                 'deleting': p['metadata'].get('deletionTimestamp')} for p in listed]})
            if (candidate and candidate['metadata']['uid'] != plan['oldPodUID']
                    and candidate.get('spec', {}).get('nodeName') == plan['newWorkerName']
                    and len(listed) == 3 and all(ready(p) for p in listed)):
                replacement = candidate
                break
            time.sleep(3)
        else:
            save('relocation-timeout-pods.json', pod_samples)
            raise RuntimeError('replacement did not recover on the independent worker')
        pvc = json.loads(kubectl(['get', 'pvc', plan['pvcName'], '-n', 'narumq', '-o', 'json']))
        pv = json.loads(kubectl(['get', 'pv', pvc['spec']['volumeName'], '-o', 'json']))
        assert pvc['metadata']['uid'] == plan['pvcUID'] and pv['metadata']['name'] == plan['pvName']
        assert pv['spec']['csi']['volumeHandle'] == plan['volumeId']
        volume = json.loads(execute(['aws', 'ec2', 'describe-volumes', '--volume-ids', plan['volumeId'], '--output', 'json']))['Volumes'][0]
        assert volume['AvailabilityZone'] == plan['zone'] and volume['Encrypted'] is True
        assert len(volume['Attachments']) == 1 and volume['Attachments'][0]['InstanceId'] == plan['newInstanceId']
        assert volume['Attachments'][0]['State'] == 'attached'
        assert plan['newInstanceId'] != plan['oldInstanceId']
        # Observe the surviving-voter path after the replacement returns, too.
        time.sleep(5)
        monitor_stop.set()
        thread.join(15)
        assert not thread.is_alive()
        save('continuity-observations.json', monitor_samples)
        assert all(s.get('quorumAvailable') is True and s.get('puback') is True and 'error' not in s for s in monitor_samples), monitor_samples
        outcome = {'runId': run, 'result': 'PASS',
                   'elapsedSeconds': round(time.monotonic() - start, 3), 'safeBeforeDelete': True,
                   'oldPodUID': plan['oldPodUID'], 'newPodUID': replacement['metadata']['uid'],
                   'oldWorker': plan['oldWorkerName'], 'newWorker': replacement['spec']['nodeName'],
                   'oldInstanceId': plan['oldInstanceId'], 'newInstanceId': plan['newInstanceId'],
                   'samePVCUID': pvc['metadata']['uid'], 'samePVName': pv['metadata']['name'], 'sameEBS': plan['volumeId'],
                   'newEC2Attachment': volume['Attachments'], 'zone': volume['AvailabilityZone'],
                   'volumeType': volume['VolumeType'], 'encrypted': volume['Encrypted'],
                   'allThreeBrokersReady': True, 'quorumAndPUBACKContinuousInSamples': True,
                   'podSamples': pod_samples, 'MQTTDataOracle': 'separate post-relocation check required',
                   'scope': 'planned cross-worker same-EBS relocation only; no power loss, forced detach, AZ or volume loss'}
        save('physical-relocation-result.json', outcome)
        print('PASS: independent EC2 worker, same PVC/PV/EBS, 3 voters Ready, survivor quorum and PUBACK observations', flush=True)
        return outcome
    except Exception as exc:
        save('relocation-failure.json', {'result': 'FAIL', 'error': str(exc), 'monitorSamples': monitor_samples})
        raise
    finally:
        monitor_stop.set()
        thread.join(15)
        monitor.disconnect()
        monitor.loop_stop()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--private', type=Path, required=True, help='directory with kubectl.exe, kubeconfig, admin.token and mqtt.password')
    parser.add_argument('--output', type=Path, required=True, help='run directory with relocation-plan.json; its name is the cluster ID')
    parser.add_argument('--survivor-management-port', type=int, default=23880)
    parser.add_argument('--survivor-mqtt-port', type=int, default=23883)
    parser.add_argument('--target-management-port', type=int, default=28880)
    args = parser.parse_args(argv)
    scenario_planned_relocation(args.private, args.output, args.survivor_management_port,
                                args.survivor_mqtt_port, args.target_management_port)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
