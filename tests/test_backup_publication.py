"""Opt-in, network-isolated smoke test for the actual backup shell and uploader."""
from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.environ.get("NARUMQ_BACKUP_PUBLICATION_SMOKE") == "1", "requires explicit Docker smoke opt-in")
class BackupPublicationTests(unittest.TestCase):
    def test_interrupted_publication_then_new_pod_upload(self):
        image = os.environ["NARUMQ_BACKUP_SMOKE_IMAGE"]
        script = r'''
import hashlib, json, os, pathlib, signal, subprocess, time
root = pathlib.Path('/backups')
fake = pathlib.Path('/tmp/fake-bin'); fake.mkdir()
pathlib.Path('/secrets').mkdir()
pathlib.Path('/secrets/admin.token').write_text('synthetic-test-token')
body = b'NMQBKUP3' + b'synthetic-archive-body'
archive = body + hashlib.sha256(body).digest()
pathlib.Path('/tmp/archive').write_bytes(archive)
legacy = root / 'backup-20261014000000.nrbak'
legacy.write_bytes(b'legacy-do-not-upload')
for name, contents in {
 'curl': """#!/usr/local/bin/python3
import hashlib, pathlib, sys
args=sys.argv
body=pathlib.Path('/tmp/archive').read_bytes()
pathlib.Path(args[args.index('--dump-header')+1]).write_text('X-NaruMQ-Backup-Commit-Index: 42\\nX-NaruMQ-Backup-SHA256: '+hashlib.sha256(body[:-32]).hexdigest()+'\\n')
sys.stdout.buffer.write(body)
sys.stderr.write('200')
""",
 'aws': """#!/usr/local/bin/python3
import base64,hashlib,json,pathlib,sys
args=sys.argv
body=pathlib.Path(args[args.index('--body')+1]).read_bytes()
assert body == pathlib.Path('/tmp/archive').read_bytes()
key=args[args.index('--key')+1]
checksum=base64.b64encode(hashlib.sha256(body).digest()).decode()
assert args[args.index('--checksum-sha256')+1] == checksum
with open('/tmp/uploads','a') as f: f.write(json.dumps({'key':key,'sha256':hashlib.sha256(body).hexdigest()})+'\\n')
print(checksum)
""",
 'mv': """#!/usr/local/bin/python3
import os,pathlib,signal,subprocess,sys
subprocess.run(['/bin/mv']+sys.argv[1:],check=True)
# Stop the producer after its validated archive moves but before request publication.
if os.environ.get('INTERRUPT_ARCHIVE_MOVE')=='1' and pathlib.Path(sys.argv[-1]).name.endswith('.nrbak'):
    os.kill(os.getppid(),signal.SIGKILL)
if os.environ.get('INTERRUPT_PUBLICATION')=='1' and pathlib.Path(sys.argv[-1]).name.endswith('.staged'):
    os.kill(os.getppid(),signal.SIGKILL)
"""
}.items():
 p=fake/name; p.write_text(contents); p.chmod(0o755)
env=dict(os.environ,PATH=str(fake)+':'+os.environ['PATH'],BACKUP_S3_BUCKET='synthetic-fixture',BACKUP_S3_PREFIX='clusters/fixture',AWS_REGION='ap-northeast-2',PYTHONDONTWRITEBYTECODE='1')
producer='/repo/deploy/kubernetes/eks-ec2-ha/backup.sh'
uploader='/repo/deploy/kubernetes/eks-ec2-ha-s3/s3_uploader.py'
first='11111111-1111-1111-1111-111111111111'
state=pathlib.Path('/run/narumq-backup')/first
state.mkdir(parents=True); (state/'uploader.ready').touch()
result=subprocess.run(['bash',producer],env=dict(env,BACKUP_RUN_ID=first,INTERRUPT_ARCHIVE_MOVE='1'),capture_output=True,text=True,timeout=20)
assert result.returncode == -signal.SIGKILL, (result.returncode,result.stderr)
# An interrupted unpublished transaction must not strand a visible new archive.
assert list(root.glob('backup-*.nrbak')) == [legacy], 'archive became visible before its request'
assert not list(root.glob('backup-*.nrbak.staged')), 'incomplete transaction was published'
second='22222222-2222-2222-2222-222222222222'
state=pathlib.Path('/run/narumq-backup')/second
state.mkdir(parents=True); (state/'uploader.ready').touch()
result=subprocess.run(['bash',producer],env=dict(env,BACKUP_RUN_ID=second,INTERRUPT_PUBLICATION='1'),capture_output=True,text=True,timeout=20)
assert result.returncode == -signal.SIGKILL, (result.returncode,result.stderr)
published=list(root.glob('backup-*.nrbak.staged'))
assert len(published)==1, published
request=next(published[0].glob('*.request.json'))
metadata=json.loads(request.read_text())
assert (published[0]/metadata['filename']).read_bytes()==archive
third='33333333-3333-3333-3333-333333333333'
upload=subprocess.Popen(['python3',uploader],env=dict(env,BACKUP_RUN_ID=third),stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
try:
 result=subprocess.run(['bash',producer],env=dict(env,BACKUP_RUN_ID=third),capture_output=True,text=True,timeout=30)
 assert result.returncode==0,(result.stdout,result.stderr)
 out,err=upload.communicate(timeout=10)
 assert upload.returncode==0,(out,err)
 records=[json.loads(line) for line in pathlib.Path('/tmp/uploads').read_text().splitlines()]
 assert len(records)==2,records
 assert all(record['sha256']==hashlib.sha256(archive).hexdigest() for record in records)
 assert metadata['filename'] in records[0]['key'], 'pending archive was not retried first'
 assert legacy.read_bytes()==b'legacy-do-not-upload'
 assert not list(root.glob('backup-*.nrbak.staged')), 'uploaded transaction was not cleaned'
 print('PASS: interruption before/after publication, new Pod pending retry, checksum, legacy preservation, committed staging cleanup')
finally:
 if upload.poll() is None: upload.kill(); upload.wait()
'''
        result = subprocess.run([
            "docker", "run", "--rm", "--network", "none", "--tmpfs", "/backups",
            "--tmpfs", "/run/narumq-backup", "--mount", f"type=bind,source={ROOT},target=/repo,readonly",
            "-i", "--entrypoint", "python3", image, "-",
        ], input=script, capture_output=True, text=True, timeout=90)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        print(result.stdout.strip())


if __name__ == "__main__":
    unittest.main()
