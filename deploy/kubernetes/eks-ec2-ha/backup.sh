#!/bin/sh
set -eu
set -o pipefail
umask 077

fail() {
  printf 'backup: %s\n' "$1" >&2
  exit 1
}

S3_ENABLED=0
if [ -n "${BACKUP_S3_BUCKET:-}" ] || [ -n "${BACKUP_S3_PREFIX:-}" ]; then
  [ -n "${BACKUP_S3_BUCKET:-}" ] || fail "required setting BACKUP_S3_BUCKET is missing"
  [ -n "${BACKUP_S3_PREFIX:-}" ] || fail "required setting BACKUP_S3_PREFIX is missing"
  [ -n "${AWS_REGION:-}" ] || fail "required setting AWS_REGION is missing"
  [ -n "${BACKUP_RUN_ID:-}" ] || fail "required setting BACKUP_RUN_ID is missing"
  case "$BACKUP_S3_BUCKET" in
    *[!a-z0-9.-]*|'') fail "BACKUP_S3_BUCKET is invalid" ;;
  esac
  case "$BACKUP_S3_PREFIX" in
    ''|/*|*/|*//*|*..*|*[!A-Za-z0-9._/-]*) fail "BACKUP_S3_PREFIX is invalid" ;;
  esac
  case "$AWS_REGION" in
    *[!a-z0-9-]*|'') fail "AWS_REGION is invalid" ;;
  esac
  case "$BACKUP_RUN_ID" in
    *[!a-f0-9-]*|'') fail "BACKUP_RUN_ID must be the pod UID" ;;
  esac
  [ "${#BACKUP_RUN_ID}" = 36 ] || fail "BACKUP_RUN_ID must be the pod UID"
  S3_ENABLED=1
fi

BACKUP_DIR=/backups
STATE_DIR=
STATUS_DIR=
DEADLINE=0
mkdir -p "$BACKUP_DIR"
if [ "$S3_ENABLED" = 1 ]; then
  STATE_DIR="/run/narumq-backup/$BACKUP_RUN_ID"
  STATUS_DIR="$STATE_DIR/status"
  DEADLINE=$(( $(date +%s) + 3300 ))
  mkdir -p "$STATUS_DIR"
fi

TEMP_ARCHIVE=
TEMP_HEADERS=
TEMP_FULL_HASH=
TEMP_CURL_STATUS=
TEMP_STAGE=
finish() {
  [ -z "$TEMP_ARCHIVE" ] || rm -f "$TEMP_ARCHIVE"
  [ -z "$TEMP_HEADERS" ] || rm -f "$TEMP_HEADERS"
  [ -z "$TEMP_FULL_HASH" ] || rm -f "$TEMP_FULL_HASH"
  [ -z "$TEMP_CURL_STATUS" ] || rm -f "$TEMP_CURL_STATUS"
  [ -z "$TEMP_STAGE" ] || rm -rf "$TEMP_STAGE"
  if [ "$S3_ENABLED" = 1 ]; then
    touch "$STATE_DIR/main.done"
  fi
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

wait_for_upload_sidecar() {
  ready_deadline=$(( $(date +%s) + 120 ))
  while [ "$(date +%s)" -lt "$ready_deadline" ]; do
    [ -f "$STATE_DIR/uploader.ready" ] && return 0
    sleep 1
  done
  fail "S3 uploader did not become ready"
}

upload_request() {
  request=$1
  filename=${request##*/}
  archive_name=${filename%.request.json}
  status_prefix="$STATUS_DIR/$archive_name"
  while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    if [ -f "$status_prefix.failed" ]; then
      fail "S3 upload failed for $archive_name; staged archive and request were retained"
    fi
    if [ -s "$status_prefix.uploaded" ]; then
      printf 'backup: uploaded staged archive %s\n' "$archive_name"
      return 0
    fi
    sleep 1
  done
  fail "S3 upload timed out for $archive_name; staged archive and request were retained"
}

cleanup_legacy_archives() {
  for archive in "$BACKUP_DIR"/backup-*.nrbak; do
    [ -f "$archive" ] || continue
    filename=${archive##*/}
    legacy_stamp=${filename#backup-}
    legacy_stamp=${legacy_stamp%.nrbak}
    case "$legacy_stamp" in
      *[!0-9]*|'') continue ;;
    esac
    [ "${#legacy_stamp}" = 14 ] || continue
    find "$archive" -type f -mtime +30 -delete
  done
}

if [ "$S3_ENABLED" = 1 ]; then
  wait_for_upload_sidecar

  # Only durable request-backed archives are S3 upload candidates. Legacy timestamp-only
  # archives remain local and keep their original mtime-based cleanup.
  for request in "$BACKUP_DIR"/backup-*.nrbak.staged/*.request.json; do
    [ -f "$request" ] || continue
    upload_request "$request"
  done

  # A content-addressed archive without its durable request may be an interrupted
  # staging transaction. Keep it and fail closed instead of treating it as legacy.
  for archive in "$BACKUP_DIR"/backup-*.nrbak; do
    [ -f "$archive" ] || continue
    request="$archive.request.json"
    [ -f "$request" ] && continue
    filename=${archive##*/}
    legacy_stamp=${filename#backup-}
    legacy_stamp=${legacy_stamp%.nrbak}
    case "$legacy_stamp" in
      *[!0-9]*|'') fail "untracked non-legacy archive $filename has no upload request; retained" ;;
    esac
    [ "${#legacy_stamp}" = 14 ] || fail "untracked non-legacy archive $filename has no upload request; retained"
  done
fi

cleanup_legacy_archives

TEMP_ARCHIVE=$(mktemp "$BACKUP_DIR/.backup-XXXXXXXX")
TEMP_HEADERS=$(mktemp "$BACKUP_DIR/.headers-XXXXXXXX")
TEMP_CURL_STATUS=$(mktemp "$BACKUP_DIR/.http-status-XXXXXXXX")
if [ "$S3_ENABLED" = 1 ]; then
  TEMP_FULL_HASH=$(mktemp "$BACKUP_DIR/.sha256-XXXXXXXX")
fi
ADMIN_TOKEN=$(cat /secrets/admin.token)
[ -n "$ADMIN_TOKEN" ] || fail "cluster admin token is empty"
BACKUP_URL=http://narumq-management.narumq.svc.cluster.local:8080/api/v1/cluster/backups
if [ "$S3_ENABLED" = 1 ]; then
  if ! {
    printf 'header = "Authorization: Bearer %s"\n' "$ADMIN_TOKEN" |
      curl --silent --show-error --fail --connect-timeout 10 --max-time 3000 \
        --config - --request POST "$BACKUP_URL" \
        --dump-header "$TEMP_HEADERS" --output - --write-out '%{stderr}%{http_code}' 2>"$TEMP_CURL_STATUS" |
        tee "$TEMP_ARCHIVE" |
        sha256sum > "$TEMP_FULL_HASH"
  }; then
    fail "backup API request failed: $(tr '\n' ' ' < "$TEMP_CURL_STATUS" | cut -c1-500)"
  fi
else
  if ! printf 'header = "Authorization: Bearer %s"\n' "$ADMIN_TOKEN" |
    curl --silent --show-error --fail --connect-timeout 10 --max-time 3000 \
      --config - --request POST "$BACKUP_URL" \
      --dump-header "$TEMP_HEADERS" --output "$TEMP_ARCHIVE" \
      --write-out '%{stderr}%{http_code}' 2>"$TEMP_CURL_STATUS"; then
    fail "backup API request failed: $(tr '\n' ' ' < "$TEMP_CURL_STATUS" | cut -c1-500)"
  fi
fi
HTTP_STATUS=$(tr -cd '0-9' < "$TEMP_CURL_STATUS")
[ "$HTTP_STATUS" = 200 ] || fail "backup API returned HTTP $HTTP_STATUS"

header_value() {
  header_name=$1
  header_count=$(grep -ic "^${header_name}:" "$TEMP_HEADERS" || true)
  [ "$header_count" = 1 ] || fail "backup API returned an invalid $header_name header count"
  grep -i "^${header_name}:" "$TEMP_HEADERS" | tr -d '\r' | cut -d: -f2- | sed 's/^[[:space:]]*//; s/[[:space:]]*$//'
}

COMMIT_INDEX=$(header_value X-NaruMQ-Backup-Commit-Index)
EXPECTED_SHA=$(header_value X-NaruMQ-Backup-SHA256 | tr '[:upper:]' '[:lower:]')
case "$COMMIT_INDEX" in
  ''|*[!0-9]*) fail "backup API returned an invalid commit index" ;;
esac
case "$EXPECTED_SHA" in
  *[!0-9a-f]*|'') fail "backup API returned an invalid archive SHA-256" ;;
esac
[ "${#EXPECTED_SHA}" = 64 ] || fail "backup API returned an invalid archive SHA-256 length"

FILE_BYTES=$(wc -c < "$TEMP_ARCHIVE" | tr -d '[:space:]')
[ "$FILE_BYTES" -ge 40 ] || fail "backup API returned a truncated archive"
[ "$FILE_BYTES" -le 1073741824 ] || fail "backup API archive exceeds the supported 1 GiB limit"
MAGIC=$(dd if="$TEMP_ARCHIVE" bs=8 count=1 2>/dev/null)
[ "$MAGIC" = NMQBKUP3 ] || fail "backup API returned a non-v3 archive"
BODY_BYTES=$((FILE_BYTES - 32))
ACTUAL_BODY_SHA=$(head -c "$BODY_BYTES" "$TEMP_ARCHIVE" | sha256sum | cut -d' ' -f1)
TRAILER_SHA=$(tail -c 32 "$TEMP_ARCHIVE" | od -An -tx1 | tr -d '[:space:]')
[ "$EXPECTED_SHA" = "$ACTUAL_BODY_SHA" ] || fail "backup API archive SHA-256 header mismatch"
[ "$EXPECTED_SHA" = "$TRAILER_SHA" ] || fail "backup API archive SHA-256 trailer mismatch"

if [ "$S3_ENABLED" = 0 ]; then
  UTC_STAMP=$(date -u +%Y%m%d%H%M%S)
  ARCHIVE_NAME="backup-${UTC_STAMP}.nrbak"
  FINAL_ARCHIVE="$BACKUP_DIR/$ARCHIVE_NAME"
  chmod 0660 "$TEMP_ARCHIVE"
  mv "$TEMP_ARCHIVE" "$FINAL_ARCHIVE"
  TEMP_ARCHIVE=
  rm -f "$TEMP_HEADERS" "$TEMP_CURL_STATUS"
  TEMP_HEADERS=
  TEMP_CURL_STATUS=
  printf 'backup: archive commit=%s saved locally as %s\n' "$COMMIT_INDEX" "$ARCHIVE_NAME"
  exit 0
fi

FULL_SHA=$(cut -d' ' -f1 < "$TEMP_FULL_HASH")
case "$FULL_SHA" in
  *[!0-9a-f]*|'') fail "backup API full-file SHA-256 is invalid" ;;
esac
[ "${#FULL_SHA}" = 64 ] || fail "backup API full-file SHA-256 length is invalid"
UTC_STAMP=$(date -u +%Y%m%dT%H%M%SZ)
ARCHIVE_NAME="backup-${UTC_STAMP}-${COMMIT_INDEX}-${FULL_SHA}.nrbak"
TEMP_STAGE=$(mktemp -d "$BACKUP_DIR/.staging-XXXXXXXX")
STAGED_ARCHIVE="$TEMP_STAGE/$ARCHIVE_NAME"
REQUEST_PATH="$TEMP_STAGE/$ARCHIVE_NAME.request.json"
chmod 0660 "$TEMP_ARCHIVE"
mv "$TEMP_ARCHIVE" "$STAGED_ARCHIVE"
TEMP_ARCHIVE=
printf '{"utcTimestamp":"%s","commitIndex":"%s","sha256":"%s","filename":"%s"}\n' \
  "$UTC_STAMP" "$COMMIT_INDEX" "$FULL_SHA" "$ARCHIVE_NAME" > "$REQUEST_PATH"
REQUEST_BYTES=$(wc -c < "$REQUEST_PATH" | tr -d '[:space:]')
[ "$REQUEST_BYTES" -le 512 ] || fail "S3 upload request exceeds 512 bytes"
chmod 0660 "$REQUEST_PATH"
FINAL_STAGE="$BACKUP_DIR/$ARCHIVE_NAME.staged"
mv -T "$TEMP_STAGE" "$FINAL_STAGE"
TEMP_STAGE=
REQUEST_PATH="$FINAL_STAGE/$ARCHIVE_NAME.request.json"
rm -f "$TEMP_HEADERS" "$TEMP_FULL_HASH" "$TEMP_CURL_STATUS"
TEMP_HEADERS=
TEMP_FULL_HASH=
TEMP_CURL_STATUS=
upload_request "$REQUEST_PATH"
printf 'backup: archive commit=%s sha256=%s uploaded\n' "$COMMIT_INDEX" "$FULL_SHA"
