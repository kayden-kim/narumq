"""Replace a dynamic peer behind a silently dropped old IP and measure replacement readiness (ADR-0059).

Run from the repository root with a Linux Docker engine:
    python -B tests/chaos/peer-blackhole-reconnect/run.py --output .artifacts/verification/<run-id>

The scenario starts three bootstrap voters on one Docker bridge network. Each voter has a stable DNS
alias. The runner keeps only a cluster whose leader is node 2, as in the EKS incident, so every run has the
same topology: the leader dials node 3 and node 1 holds an idle follower link to node 3. The runner
drops all traffic to and from node 3's IP on nodes 1 and 2, kills node 3, and starts a replacement with
the same alias, the same volume and a new IP. PASS requires the replacement /health/ready within
--max-ready-seconds after it starts.
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SUBNET = '172.31.77.0/24'
IPS = {1: '172.31.77.11', 2: '172.31.77.12', 3: '172.31.77.13'}
REPLACEMENT_IP = '172.31.77.23'
PORTS = {1: 44081, 2: 44082, 3: 44083}
FIREWALL_IMAGE = 'alpine:3.20'
# Startup order changes between attempts only to vary which voter wins the first election.
START_ORDERS = ((2, 1, 3), (1, 2, 3), (2, 3, 1), (3, 2, 1), (1, 3, 2), (3, 1, 2))


class LeaderPlacement(RuntimeError):
    """The elected leader is not node 2, so this attempt does not match the fixed topology."""

    def __init__(self, leader: int, initial: dict):
        super().__init__(f'leader is node {leader}')
        self.initial = initial


def utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds')


def run(args: list[str], timeout: int = 120, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(args, text=True, encoding='utf-8', capture_output=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f'{args[0]} {args[1]} exit {result.returncode}: {result.stderr.strip()[-400:]}')
    return result


def docker(*args: str, timeout: int = 120, check: bool = True) -> str:
    return run(['docker', *args], timeout=timeout, check=check).stdout.strip()


def build_image(tag: str, workspace: Path) -> str:
    os_arch = docker('version', '--format', '{{.Server.Os}}/{{.Server.Arch}}')
    target_os, target_arch = os_arch.split('/')
    context = workspace / 'image'
    context.mkdir()
    environment = {'CGO_ENABLED': '0', 'GOOS': target_os, 'GOARCH': target_arch}
    subprocess.run(['go', 'build', '-trimpath', '-o', str(context / 'narumq'), './cmd/narumq'], cwd=ROOT,
                   env={**os.environ, **environment}, check=True, timeout=600)
    (context / 'Dockerfile').write_text('FROM scratch\nCOPY narumq /narumq\nENTRYPOINT ["/narumq"]\n', encoding='utf-8')
    docker('build', '-q', '-t', tag, str(context), timeout=600)
    return tag


class Attempt:
    def __init__(self, image: str, config: Path, token: str, output: Path, prefix: str):
        self.image = image
        self.config = config
        self.token = token
        self.output = output
        self.prefix = prefix
        self.containers: list[str] = []
        self.volumes: list[str] = []

    def api(self, node: int, path: str) -> tuple[int, dict]:
        request = urllib.request.Request(f'http://127.0.0.1:{PORTS[node]}{path}',
                                         headers={'Authorization': 'Bearer ' + self.token})
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, {}
        except Exception as error:  # noqa: BLE001 - every failed sample is recorded
            return 0, {'error': type(error).__name__}

    def start_node(self, node: int, ip: str, suffix: str = '') -> str:
        name = f'{self.prefix}-node{node}{suffix}'
        volume = f'{self.prefix}-data{node}'
        if volume not in self.volumes:
            self.volumes.append(volume)
        self.containers.append(name)
        docker('run', '-d', '--name', name, '--network', self.prefix, '--ip', ip,
               '--network-alias', f'node{node}.peer.test', '--hostname', f'node{node}.peer.test',
               '--read-only', '--tmpfs', '/tmp:size=16m',
               '--mount', f'type=bind,source={self.config / f"node-{node}"},destination=/config,readonly',
               '--mount', f'type=volume,source={volume},destination=/var/lib/narumq',
               '-p', f'127.0.0.1:{PORTS[node]}:8080', self.image, '-cluster-config', '/config/node.json')
        return name

    def firewall(self, node_container: str) -> str:
        name = node_container + '-fw'
        self.containers.append(name)
        docker('run', '-d', '--name', name, '--network', f'container:{node_container}', '--cap-add', 'NET_ADMIN',
               '--entrypoint', '/bin/sh', FIREWALL_IMAGE, '-c',
               'apk add --no-cache iptables >/dev/null && touch /ready && sleep 100000')
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if run(['docker', 'exec', name, 'test', '-f', '/ready'], check=False).returncode == 0:
                return name
            time.sleep(1)
        raise RuntimeError('firewall sidecar did not install iptables')

    def wait_cluster(self) -> dict:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            for node in PORTS:
                self.api(node, '/health/ready')
            status, body = self.api(1, '/api/v1/cluster')
            data = body.get('data', {}) if status == 200 else {}
            if data.get('nodesByState', {}).get('ready') == 3 and data.get('leaderNodeId'):
                return data
            time.sleep(1)
        raise RuntimeError('cluster did not reach ready=3')

    def save_logs(self, names: dict[int, str]) -> None:
        for node, name in names.items():
            logs = run(['docker', 'logs', name], check=False)
            (self.output / f'node{node}.log').write_text(logs.stdout + logs.stderr, encoding='utf-8')

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-243
    def execute(self, start_order: tuple[int, int, int], replacement_delay: float, ready_timeout: float) -> dict:
        docker('network', 'create', '--driver', 'bridge', '--subnet', SUBNET, self.prefix)
        nodes = {}
        for position, node in enumerate(start_order):
            nodes[node] = self.start_node(node, IPS[node])
            if position < 2:
                time.sleep(3 - position)
        initial = self.wait_cluster()
        leader = int(initial['leaderNodeId'])
        if leader != 2:
            raise LeaderPlacement(leader, initial)
        walls = [self.firewall(nodes[node]) for node in (1, 2)]
        stop = threading.Event()

        def probe_survivors() -> None:
            # Kubelet readiness probes keep running on the surviving voters.
            while not stop.wait(2):
                for node in (1, 2):
                    self.api(node, '/health/ready')

        threading.Thread(target=probe_survivors, daemon=True).start()
        events: dict = {'leader': leader, 'initialCluster': initial, 'faultAt': utc()}
        samples = []
        fault = time.monotonic()
        for wall in walls:
            docker('exec', wall, 'iptables', '-I', 'INPUT', '-s', IPS[3], '-j', 'DROP')
            docker('exec', wall, 'iptables', '-I', 'OUTPUT', '-d', IPS[3], '-j', 'DROP')
        docker('kill', nodes[3])
        events['killedAt'] = utc()
        while time.monotonic() - fault < replacement_delay:
            status, body = self.api(leader, '/api/v1/cluster')
            samples.append({'at': utc(), 'phase': 'down', 'status': status,
                            'nodesByState': body.get('data', {}).get('nodesByState')})
            time.sleep(1)
        docker('rm', '-f', nodes[3])
        nodes[3] = self.start_node(3, REPLACEMENT_IP, '-replacement')
        events['replacementStartedAt'] = utc()
        started = time.monotonic()
        ready_seconds = None
        while time.monotonic() - started < ready_timeout:
            status, _ = self.api(3, '/health/ready')
            _, cluster = self.api(leader, '/api/v1/cluster')
            samples.append({'at': utc(), 'phase': 'replacement', 'node3Ready': status,
                            'nodesByState': cluster.get('data', {}).get('nodesByState')})
            if status == 200:
                ready_seconds = time.monotonic() - started
                events['replacementReadyAt'] = utc()
                break
            time.sleep(0.5)
        stop.set()
        events['replacementReadySeconds'] = ready_seconds
        self.save_logs(nodes)
        return {'events': events, 'samples': samples}

    def teardown(self) -> list[str]:
        failures = []
        for name in reversed(self.containers):
            if run(['docker', 'rm', '-f', name], check=False).returncode:
                failures.append(name)
        if run(['docker', 'network', 'rm', self.prefix], check=False).returncode:
            failures.append(self.prefix)
        for volume in self.volumes:
            if run(['docker', 'volume', 'rm', '-f', volume], check=False).returncode:
                failures.append(volume)
        return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--output', required=True, type=Path, help='new directory; its name is the run ID')
    parser.add_argument('--image', help='existing broker image; default builds ./cmd/narumq')
    parser.add_argument('--replacement-delay', type=float, default=20.0, help='seconds from fault to replacement start')
    parser.add_argument('--max-ready-seconds', type=float, default=15.0, help='PASS bound after replacement start')
    parser.add_argument('--ready-timeout', type=float, default=240.0, help='observation bound after replacement start')
    parser.add_argument('--attempts', type=int, default=12, help='fresh clusters to try until node 2 is leader')
    options = parser.parse_args()
    output: Path = options.output
    output.mkdir(parents=True, exist_ok=False)
    run_id = output.name.lower()
    workspace = Path(tempfile.mkdtemp(prefix='narumq-peer-blackhole-'))
    result: dict = {'runId': run_id, 'startedAt': utc(), 'replacementDelaySeconds': options.replacement_delay,
                    'maxReadySeconds': options.max_ready_seconds, 'attempts': []}
    try:
        image = options.image or build_image(f'narumq-peer-blackhole:{run_id}', workspace)
        result['image'] = image
        result['imageId'] = docker('image', 'inspect', '-f', '{{.Id}}', image)
        config = workspace / 'config'
        subprocess.run(['go', 'run', './tests/chaos/peer-blackhole-reconnect/fixture', str(config)],
                       cwd=ROOT, check=True, timeout=600)
        token = (config / 'admin.token').read_text(encoding='utf-8').strip()
        for number in range(1, options.attempts + 1):
            attempt_dir = output / f'attempt-{number}'
            attempt_dir.mkdir()
            attempt = Attempt(image, config, token, attempt_dir, f'peer-blackhole-{run_id}-{number}')
            start_order = START_ORDERS[(number - 1) % len(START_ORDERS)]
            record: dict = {'attempt': number, 'startOrder': list(start_order)}
            try:
                observed = attempt.execute(start_order, options.replacement_delay, options.ready_timeout)
                (attempt_dir / 'observation.json').write_text(json.dumps(observed, indent=2), encoding='utf-8')
                record['replacementReadySeconds'] = observed['events']['replacementReadySeconds']
                record['leader'] = observed['events']['leader']
                result['attempts'].append(record)
                break
            except LeaderPlacement as error:
                record['skipped'] = str(error)
                (attempt_dir / 'startup-cluster.json').write_text(json.dumps(error.initial, indent=2), encoding='utf-8')
                result['attempts'].append(record)
            finally:
                record['cleanupFailures'] = attempt.teardown()
        measured = [item for item in result['attempts'] if 'replacementReadySeconds' in item]
        ready = measured[0]['replacementReadySeconds'] if measured else None
        cleanup_failures = [name for item in result['attempts'] for name in item['cleanupFailures']]
        result['replacementReadySeconds'] = ready
        result['cleanupFailures'] = cleanup_failures
        result['verdict'] = 'PASS' if ready is not None and ready <= options.max_ready_seconds and not cleanup_failures \
            else 'FAIL'
    finally:
        if not options.image:
            docker('image', 'rm', '-f', f'narumq-peer-blackhole:{run_id}', check=False)
        shutil.rmtree(workspace, ignore_errors=True)
        result['finishedAt'] = utc()
        (output / 'result.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps({'verdict': result.get('verdict'), 'replacementReadySeconds': result.get('replacementReadySeconds'),
                      'output': str(output)}))
    return 0 if result.get('verdict') == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
