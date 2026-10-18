from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / 'tests/chaos/eks-volume-reseed'
sys.path.insert(0, str(DIR))
import reseed
import mqtt_helpers
spec = importlib.util.spec_from_file_location('large_reseed_main', DIR / 'main.py')
main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(main)
RUN = 'narumq-ha-acceptance-20261016'


def options(**kw):
    return reseed.large_snapshot_settings({'largeSnapshot': kw})


class Client:
    sent = []
    received_messages = []

    def __init__(self, *args):
        self.acks = []
        self.messages = list(self.received_messages)

    def publish(self, topic, payload, retain=False):
        self.sent.append((topic, hashlib.sha256(payload).hexdigest(), retain))
        self.acks.append({'reasonValue': 0})

    def subscribe(self, topic):
        pass

    def close(self):
        pass


class LargeSnapshotTests(unittest.TestCase):
    def test_settings_defaults_and_idempotent_normalisation(self):
        settings = options()
        self.assertEqual((settings['feature'], settings['totalRetainedBytes'], settings['padEntries']),
                         ('snapshot-stream', 4194304, 1100))
        self.assertEqual(reseed.large_snapshot_settings({'largeSnapshot': settings}), settings)
        self.assertIsNone(reseed.large_snapshot_settings({}))

    def test_rejects_invalid_options(self):
        for kw in ({'feature': 'session-metadata'}, {'padEntries': 0}, {'retainedTopics': True},
                   {'retainedTopics': 64}, {'retainedPayloadBytes': 1000001}, {'retainedTopics': 1},
                   {'minStreamBytes': 1}, {'other': 1}):
            with self.subTest(kw=kw), self.assertRaises(reseed.PlanError):
                options(**kw)

    def test_payload_and_topics(self):
        payload = reseed.large_payload(RUN, 1, 524288)
        self.assertEqual(len(payload), 524288)
        self.assertEqual(payload, reseed.large_payload(RUN, 1, 524288))
        self.assertNotEqual(payload, reseed.large_payload(RUN, 2, 524288))
        self.assertTrue(reseed.large_topic(RUN, 0).endswith('/reseed/large/t/00'))

    def test_publish_and_read_helpers_use_hash_only(self):
        Client.sent.clear()
        settings = options()
        with tempfile.TemporaryDirectory() as text:
            private = Path(text)
            (private / 'mqtt.password').write_text('secret', encoding='utf-8')
            result = mqtt_helpers.publish_large_load(private, RUN, settings, 1, client_factory=Client)
            self.assertEqual((result['topics'], result['padPublished']), (8, 1100))
            self.assertEqual(len(Client.sent), 1108)
            self.assertTrue(all(retain for _, _, retain in Client.sent))
            Client.received_messages = [{'topic': t, 'sha256': h, 'retain': True} for t, h in result['sha256'].items()]
            seen = mqtt_helpers.read_large_retained(private, RUN, settings, 1, client_factory=Client, sleep=lambda _: None)
            self.assertEqual(reseed.judge_large_retained(result['sha256'], seen)['result'], 'PASS')
            seen[0]['sha256'] = 'wrong'
            self.assertEqual(reseed.judge_large_retained(result['sha256'], seen)['result'], 'FAIL')

    def test_stream_logs_require_large_completion_for_new_peer_and_receipt(self):
        sender = json.dumps({'msg': reseed.STREAM_COMPLETED, 'peer_id': 4, 'snapshot_index': 1200,
                             'total_bytes': 4195000, 'chunks': 5})
        receiver = 'msg="raft snapshot stream received" from_peer_id=1 snapshot_index=1200 total_bytes=4195000'
        records = reseed.parse_log_records(sender)
        recv = reseed.parse_log_records(receiver)
        judged = reseed.judge_stream_logs(records, recv, '4', ['1', '2'], 1048576)
        self.assertTrue(judged['completedOverLimit'] and judged['receiverLogged'])
        self.assertFalse(reseed.judge_stream_logs(records, recv, '9', ['1'], 1048576)['completedOverLimit'])
        self.assertFalse(reseed.judge_stream_logs(records, recv, '4', ['1'], 4195000)['completedOverLimit'])
        self.assertEqual(reseed.parse_log_records('{invalid}\nnoise'), [])

    def test_aggregate_requires_r1_criteria(self):
        plan = {'largeSnapshot': options()}
        base = {key: True for key in reseed.CRITERIA}
        self.assertEqual(reseed.aggregate(base)['result'], 'PASS')
        self.assertEqual(reseed.aggregate(base, plan=plan)['result'], 'FAIL')
        base.update({key: True for key in reseed.LARGE_CRITERIA})
        self.assertEqual(reseed.aggregate(base, plan=plan)['result'], 'PASS')

    def test_execute_order_activates_before_load_and_stage(self):
        with tempfile.TemporaryDirectory() as text:
            run = main.Run(None, {'largeSnapshot': options()}, Path(text), Path(text))
            names = ['ports', 'observer', 'preflight', 'feature', 'baseline', 'large_load', 'loss', 'guard', 'reseed', 'verify']
            called = []
            for name in names:
                setattr(run, 'step_' + name, lambda name=name: called.append(name))
            run.execute()
            self.assertEqual(called, names)

    def test_feature_activation_targets_leader_without_idempotency_key(self):
        class Fixture:
            active = False
            requests = []

            def api(self, path, port):
                return {'activeFeatures': ['snapshot-stream'] if self.active else [],
                        'members': [{'nodeId': str(i), 'features': ['snapshot-stream']} for i in (1, 2, 3)]}

            def api_request(self, method, path, body, port, key):
                self.requests.append((method, path, body, port, key))
                self.active = True
                return 200, {'data': self.api(path, port)}

        with tempfile.TemporaryDirectory() as text:
            fixture = Fixture()
            run = main.Run(fixture, {'largeSnapshot': options()}, Path(text), Path(text))
            @contextlib.contextmanager
            def leader():
                yield 22880, '2'
            run.leader_endpoint = leader
            run.step_feature()
            self.assertEqual(fixture.requests, [('POST', '/api/v1/cluster/features/snapshot-stream', None, 22880, None)])
            self.assertTrue(run.criteria['stream.featureActive'])
            run.step_feature()
            self.assertEqual(len(fixture.requests), 1)


if __name__ == '__main__':
    unittest.main()
