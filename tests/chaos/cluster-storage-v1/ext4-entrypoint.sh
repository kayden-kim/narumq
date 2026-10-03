#!/bin/sh
set -eu

size=${NARUMQ_EXT4_MIB:-256}
case "$size" in 256|1024) ;; *) echo 'unsupported ext4 image size' >&2; exit 1 ;; esac
image=/images/disk.img
loop=
mounted=false
child=
cleanup_status=passed
cleanup() {
    status=$?
    trap - EXIT INT TERM
    if [ -n "$child" ] && kill -0 "$child" 2>/dev/null; then
        kill -TERM "$child" 2>/dev/null || true
        wait "$child" || true
    fi
    if [ "$mounted" = true ]; then
        if umount /var/lib/narumq; then mounted=false; else status=1; cleanup_status=failed; fi
    fi
    if [ -n "$loop" ] && [ "$mounted" = false ]; then
        if [ -n "$(losetup --associated "$image")" ]; then
            losetup --detach "$loop" || { status=1; cleanup_status=failed; }
        fi
    fi
    if [ -n "$loop" ]; then
        printf '{"event":"ext4-cleanup","status":"%s"}\n' "$cleanup_status"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
trap '[ -z "$child" ] || kill -USR1 "$child" 2>/dev/null || true' USR1
trap '[ -z "$child" ] || kill -USR2 "$child" 2>/dev/null || true' USR2

# Never format a caller-owned device or overwrite an existing image.
set -C
: > "$image"
set +C
truncate -s "${size}M" "$image"
mkfs.ext4 -q -F -m 0 "$image"
loop=$(losetup --find --show "$image")
mkdir -p /var/lib/narumq
mount -t ext4 -o data=ordered "$loop" /var/lib/narumq
mounted=true
# Mark the attached device autoclear so even container termination releases it
# once the private mount namespace disappears.
losetup --detach "$loop"
filesystem=$(findmnt --noheadings --output FSTYPE --mountpoint /var/lib/narumq)
[ "$filesystem" = ext4 ] || { echo 'ext4 mount was not installed' >&2; exit 1; }
options=$(findmnt --noheadings --output OPTIONS --mountpoint /var/lib/narumq)
checksum=$(sha256sum /narumq | cut -d ' ' -f 1)
printf '{"event":"ext4-mount","kernel":"%s","filesystem":"%s","mountOptions":"%s","loopDevice":"%s","imageMiB":%s,"brokerSHA256":"%s"}\n' "$(uname -r)" "$filesystem" "$options" "$loop" "$size" "$checksum"

run_boundaries() {
    export TMPDIR=/var/lib/narumq/test-temp GOCACHE=/tmp/go-cache GOMAXPROCS=4
    export NARUMQ_WAL_LIFECYCLE=1
    mkdir -p "$TMPDIR" "$GOCACHE"
    go test -json ./internal/adapters/raft -count=1 -run '^(TestStableStoreSnapshotCompactsWALToSnapshotTail|TestStableStoreCompactionSplitsLargeTailIntoFrames|TestStableStoreRecoversEveryCompactionCrashState|TestStableStoreCompactionFailureKeepsRecoverableState|TestWALReaderHandlesTornAndCorruptFrames)$' &&
    go test -json ./tests/integration -count=1 -parallel=1 -timeout=20m -run '^(TestSnapshotFinalizationCrashRecoveryAcrossProcessRestart|TestDynamicClusterWALCompactionProcessAcceptance|TestClusterBackupRestoreProcessCrashAndResync|TestDynamicClusterVoterVolumeLossProcessAcceptance|TestDynamicClusterJoinerVoterVolumeLossProcessAcceptance)$'
}

case "${1:-}" in
    supervise) /narumq-chaosnode "$@" & ;;
    recovery-boundaries) run_boundaries & ;;
    *) echo 'expected supervise or recovery-boundaries' >&2; exit 1 ;;
esac
child=$!
# Forwarded signals interrupt shell wait without ending the supervisor.
# Keep waiting until the child itself exits and preserve its actual status.
while :; do
    set +e
    wait "$child"
    status=$?
    set -e
    if ! kill -0 "$child" 2>/dev/null; then break; fi
done
child=
exit "$status"
