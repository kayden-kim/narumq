from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / 'tests/chaos/eks-wal-torn-tail'
sys.path.insert(0, str(DIR))
import wal_frames
import helper_pod
import judge
spec = importlib.util.spec_from_file_location('wal_torn_main', DIR / 'main.py')
main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(main)
RUN = 'narumq-ha-acceptance-20261016'


def plan():
    result = {key: key for key in main.PLAN_STRINGS}
    result.update(runId=RUN, clusterName=RUN, targetPod='narumq-2', observerPod='narumq-0',
                  pvcName='data-narumq-2', targetWorkerName='target', zoneWorkers=[{'name': 'target', 'uid': 'u'}])
    return result


class FrameTests(unittest.TestCase):
    def test_crc32c_standard_vector(self):
        self.assertEqual(wal_frames.crc32c(b'123456789'), 0xe3069283)

    def test_all_cases_match_expected_rules_at_each_alignment(self):
        for alignment in (0, 1, 127, 255, 510, 511):
            base = wal_frames.base_wal(4, alignment)
            self.assertEqual(wal_frames.classify(base)['outcome'], 'ok')
            for name, case in wal_frames.CASES.items():
                with self.subTest(alignment=alignment, name=name):
                    data, info = wal_frames.craft(name, base)
                    expected = 'truncate' if case['group'] == 'T1' else 'fail'
                    self.assertEqual(wal_frames.classify(data)['outcome'], expected)
                    self.assertEqual(wal_frames.decode_parts(wal_frames.encode_parts(data)), data)

    def test_committed_vectors_match_generator(self):
        path = ROOT / 'internal/adapters/raft/testdata/wal-tail-vectors.json'
        self.assertEqual(path.read_text(encoding='utf-8'), wal_frames.render_vectors())

    def test_craft_requires_clean_nonempty_base(self):
        with self.assertRaises(wal_frames.CraftError):
            wal_frames.craft('short-header', b'bad')


class PlanAndHelperTests(unittest.TestCase):
    def test_defaults_and_validation(self):
        good = main.load_plan(plan(), RUN)
        self.assertEqual(good['cases'], ['short-header', 'zero-tail', 'final-crc-no-zero-evidence', 'middle-crc'])
        for changes in ({'runId': 'other'}, {'clusterName': 'production'}, {'targetPod': 'narumq-9'},
                        {'zoneWorkers': []}, {'cases': ['bad']}, {'refuseHoldSeconds': True},
                        {'observerPod': 'narumq-2'}):
            with self.subTest(changes=changes), self.assertRaises(main.PlanError):
                main.load_plan(plan() | changes, RUN)

    def test_helper_is_pinned_and_uses_broker_identity(self):
        obj = helper_pod.manifest(RUN, 'narumq-2', 'data-narumq-2', 'target', 'image@sha256:abc', 10001, 10001)
        self.assertEqual(obj['spec']['nodeName'], 'target')
        self.assertEqual(obj['spec']['securityContext']['runAsUser'], 10001)
        self.assertEqual(obj['spec']['volumes'][0]['persistentVolumeClaim']['claimName'], 'data-narumq-2')
        self.assertEqual(helper_pod.WAL_PATH, '/volume/data/wal.bin')

    def test_preflight_rejects_wrong_account_before_mutation(self):
        class Fixture:
            def aws(self, *args):
                return {'Account': 'wrong'}
        with self.assertRaises(main.reseed.PreflightRejected) as caught:
            main.preflight(Fixture(), plan())
        self.assertEqual(caught.exception.code, 'account_mismatch')


class JudgeTests(unittest.TestCase):
    def test_truncation_requires_prefix_and_tail_removal(self):
        base = wal_frames.base_wal()
        data, info = wal_frames.craft('short-header', base)
        self.assertTrue(judge.truncation_evidence(data, base, info['validBytes'])['cut'])
        self.assertFalse(judge.truncation_evidence(data, data, info['validBytes'])['cut'])
        self.assertFalse(judge.truncation_evidence(data, None, info['validBytes'])['fileRead'])
        self.assertFalse(judge.truncation_evidence(data, b'changed', info['validBytes'])['cut'])

    def test_refusal_requires_nonzero_exit_and_correct_corruption(self):
        status = {'ready': False, 'restartCount': 1, 'lastState': {'terminated': {'exitCode': 1, 'reason': 'Error'}}}
        text = 'corrupt raft WAL: middle WAL frame checksum mismatch'
        self.assertEqual(judge.classify_refusal(status, text, 'middle WAL frame checksum mismatch', 'old', 'new')['verdict'], 'refused')
        self.assertEqual(judge.classify_refusal(status, text, 'other error', 'old', 'new')['verdict'], 'fail')
        self.assertEqual(judge.classify_refusal(status, None, 'x', 'old', 'new')['verdict'], 'waiting')
        self.assertEqual(judge.classify_refusal(status | {'ready': True}, text, 'x', 'old', 'new')['verdict'], 'fail')

    def test_log_and_aggregate(self):
        line = json.dumps({'msg': judge.TORN_TAIL_MESSAGE, 'path': '/data/wal.bin', 'dropped_bytes': 5})
        self.assertEqual(judge.find_torn_tail_log(line)['fields']['dropped_bytes'], 5)
        self.assertFalse(judge.find_torn_tail_log(None)['logRead'])
        cases = ['short-header', 'middle-crc']
        criteria = {name: True for name in judge.criteria_for(cases)}
        self.assertEqual(judge.aggregate(criteria, cases)['result'], 'PASS')
        criteria['middle-crc.neverReady'] = False
        self.assertEqual(judge.aggregate(criteria, cases)['failedCriteria'], ['middle-crc.neverReady'])


class OrchestrationTests(unittest.TestCase):
    def build(self, path):
        private, output = path / 'private', path / 'output'
        private.mkdir()
        output.mkdir()
        run = main.Run(None, main.load_plan(plan(), RUN), output, private, sleep=lambda _: None)
        class Helper:
            data = wal_frames.base_wal()
            def read(self):
                return self.data
            def write(self, data):
                self.data = data
        helper = Helper()
        run.open_helper = lambda: setattr(run, 'helper', helper)
        run.close_helper = lambda: {'gone': True}
        run.stop_broker = lambda label: None
        run.start_broker = lambda evidence: None
        def ready(evidence, key):
            evidence[key] = {'threeReadyAndQuorum': True}
            return {'metadata': {'uid': 'new'}}
        run.wait_ready = ready
        run.marker_intact = lambda uid: True
        run.broker_wal_reader = lambda pod: wal_frames.base_wal()
        return run, helper, private, output

    def test_t1_and_t2_inject_verify_restore_and_hash_evidence(self):
        with tempfile.TemporaryDirectory() as text:
            run, helper, private, output = self.build(Path(text))
            with patch.object(main.reseed, 'fetch_logs', return_value=''):
                run.run_truncate_case('short-header', True)
            self.assertTrue(run.criteria['short-header.tailCut'])
            helper.data = wal_frames.base_wal()
            def refused(name, evidence):
                run.criteria[name + '.refusedStart'] = True
                run.criteria[name + '.neverReady'] = True
                return True
            run.observe_refusal = refused
            run.run_refusal_case('middle-crc', True)
            self.assertTrue(all(run.criteria[name] for name in judge.case_criteria('middle-crc')))
            self.assertIsNone(run.restore_required)
            self.assertEqual(wal_frames.classify(helper.data)['outcome'], 'ok')
            record = json.loads((output / 'torn-case-middle-crc.json').read_text(encoding='utf-8'))
            self.assertTrue(record['restored']['matches'])
            self.assertTrue(any(private.glob('wal-backup-*.bin')))

    def test_cleanup_keeps_workers_cordoned_while_restore_required(self):
        with tempfile.TemporaryDirectory() as text:
            run, helper, private, output = self.build(Path(text))
            run.restore_required = {'backupFile': 'wal-backup-x.bin'}
            run.uncordon_zone = lambda evidence: self.fail('must not uncordon a corrupt WAL')
            report = {}
            run.cleanup(report)
            self.assertTrue(report['teardownFailed'])
            self.assertFalse(run.criteria['cleanup.workersUncordoned'])

    def test_restore_checks_backup_hash_and_restarts(self):
        with tempfile.TemporaryDirectory() as text:
            run, helper, private, output = self.build(Path(text))
            backup = helper.data
            filename = 'wal-backup-test.bin'
            (private / filename).write_bytes(backup)
            helper.data, _ = wal_frames.craft('middle-crc', backup)
            required = {'backupFile': filename, 'backupSha256': helper_pod.sha256(backup),
                        'injectedSha256': helper_pod.sha256(helper.data)}
            run.uncordon_zone = lambda evidence: None
            result = run.restore(required, output)
            self.assertTrue(result['ready'])
            self.assertEqual(helper.data, backup)
            self.assertTrue((output / 'torn-restore.json').is_file())


if __name__ == '__main__':
    unittest.main()
