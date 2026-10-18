#!/bin/sh
# Capacity fixture only: keep a signalable broker child without changing the broker image.
set -eu
pid_file=/capacity-runtime/broker.pid
instance_file=/capacity-runtime/broker.instance
mode="$1"
shift

case "$mode" in
  run)
    interrupted=0
    pending_signal=
    broker_pid=
    forward() {
      interrupted=1
      pending_signal="$1"
      [ -z "$broker_pid" ] || kill -"$1" "$broker_pid" 2>/dev/null || true
    }
    trap 'forward TERM' TERM
    trap 'forward INT' INT
    cat /proc/sys/kernel/random/uuid > "$instance_file"
    "$@" &
    broker_pid=$!
    printf '%s\n' "$broker_pid" > "$pid_file"
    [ -z "$pending_signal" ] || forward "$pending_signal"
    while :; do
      interrupted=0
      set +e
      wait "$broker_pid"
      status=$?
      set -e
      [ "$interrupted" -eq 1 ] || exit "$status"
    done
    ;;
  identity|freeze|resume)
    broker_pid="$(cat "$pid_file")"
    case "$broker_pid" in ''|*[!0-9]*) echo 'Invalid broker PID' >&2; exit 1 ;; esac
    [ "$broker_pid" -gt 1 ]
    [ "$(readlink "/proc/$broker_pid/exe")" = /narumq ]
    instance="$(cat "$instance_file")"
    if [ "$mode" = identity ]; then
      printf '{"pid":%s,"instance":"%s","podUID":"%s"}\n' "$broker_pid" "$instance" "${CAPACITY_POD_UID:?}"
      exit 0
    fi
    # The per-start instance also rejects a kubectl exec routed across replacement.
    [ "$#" -eq 1 ] && [ "$1" = "$instance" ]
    if [ "$mode" = resume ]; then
      kill -CONT "$broker_pid"
      exit 0
    fi
    kill -STOP "$broker_pid"
    task_stopped() {
      while read -r field state rest; do
        if [ "$field" = State: ]; then
          [ "$state" = T ]
          return
        fi
      done < "$1"
      return 1
    }
    all_stopped() {
      stopped_threads=0
      for task in /proc/"$broker_pid"/task/*/status; do
        task_stopped "$task" || return 1
        stopped_threads=$((stopped_threads + 1))
      done
      [ "$stopped_threads" -gt 0 ]
    }
    attempt=0
    until all_stopped; do
      attempt=$((attempt + 1))
      [ "$attempt" -lt 100 ] || { echo 'Broker threads did not stop' >&2; exit 1; }
      sleep 0.01
    done
    printf '{"pid":%s,"stoppedThreads":%s}\n' "$broker_pid" "$stopped_threads"
    ;;
  *) echo 'Expected run, identity, freeze or resume' >&2; exit 1 ;;
esac
