#!/usr/bin/env bash
# Run one DAG through the real scheduler (LocalExecutor) and collect diagnostics.
#   CASE=<label> ci/run_case.sh <dag_id> [--kill-manager]
# Cap: 270 s, inside the step's 5-minute timeout, so diagnostics are always written.
set -u
dag=$1; kill_manager=${2:-}
out="diag/${CASE:-$dag}"; mkdir -p "$out"
run="case_$(date +%s)"
sql() { psql -tA "$PG_URL" -c "$1" 2>/dev/null; }
state() { sql "select coalesce(state,'none') from task_instance where dag_id='$dag' and run_id='$run' and task_id='work'"; }

snapshot() {  # $1: label
  d="$out/$1"; mkdir -p "$d"
  ps -eo pid,ppid,pgid,stat,etimes,args --forest > "$d/ps_forest.txt"
  # the task's processes: "airflow worker -- <ti id>" (the task runner, and everything it forked, which
  # inherits that title) and their parents (the supervisor is the runner's parent)
  pids=$(ps -eo pid,args | awk '/airflow worker -- / && !/awk/ {print $1}')
  parents=$(for p in $pids; do ps -o ppid= -p "$p"; done | sort -u)
  for p in $pids $parents; do
    [ -d "/proc/$p" ] || continue
    {
      echo "== pid $p: $(tr '\0' ' ' < /proc/$p/cmdline | cut -c1-200)"
      sudo grep -E '^(State|PPid|Threads|SigCgt|SigIgn):' /proc/$p/status
      sudo ls -l /proc/$p/fd 2>&1  # the supervisor is non-dumpable: root only
    } >> "$d/proc_fd.txt"
    sudo "$PYSPY" dump --pid "$p" > "$d/pyspy_$p.txt" 2>&1 || true
  done
  sudo ss -xp > "$d/ss_unix.txt" 2>&1
  echo "$pids" > "$d/task_pids.txt"; echo "$parents" > "$d/parent_pids.txt"
}

w0=$(grep -ci "open sockets" standalone.log 2>/dev/null || true)
airflow dags unpause "$dag" > /dev/null
airflow dags trigger "$dag" -r "$run" > /dev/null
start=$(date +%s); done_at=""; snap=""; killed=""
while :; do
  t=$(( $(date +%s) - start )); s=$(state)
  log=$(find "$AIRFLOW_HOME/logs" -path "*dag_id=$dag/*run_id=$run*" -name '*.log' 2>/dev/null | head -1)
  if [ -z "$done_at" ] && [ -n "$log" ] && grep -qE "MP_LOOP_DONE|MP_DONE" "$log"; then done_at=$t; fi
  echo "t=${t}s state=${s:-none} work_done_at=${done_at:--}" >> "$out/timeline.txt"
  case "$s" in success|failed|upstream_failed|skipped|up_for_retry) break ;; esac
  if [ -n "$done_at" ] && [ -z "$snap" ] && [ $((t - done_at)) -ge 15 ]; then
    echo "t=${t}s: work done ${done_at}s but task still $s: snapshot" >> "$out/timeline.txt"
    snapshot stuck; snap=1
  fi
  if [ -n "$kill_manager" ] && [ -n "$snap" ] && [ -z "$killed" ] && [ $((t - done_at)) -ge 25 ]; then
    mgr=$(grep -o 'manager_pid=[0-9]*' "$log" | head -1 | cut -d= -f2)
    echo "t=${t}s: kill -9 manager server pid=$mgr" >> "$out/timeline.txt"
    kill -9 "$mgr" 2>> "$out/timeline.txt"; killed=$t
  fi
  if [ "$t" -ge 270 ]; then echo "t=${t}s: case cap reached" >> "$out/timeline.txt"; snapshot cap; break; fi
  sleep 2
done
[ -z "$snap" ] && snapshot end
final=$(state); took=$(( $(date +%s) - start )); strays=""
if [ "$final" = success ] && [ -n "${STRAY_CHECK:-}" ]; then
  # Every process the task forked keeps the title "airflow worker -- <ti id>"; the supervisor's title
  # also names the ti id while it is still servicing the task. Anything left after success is a stray.
  ti=$(sql "select id from task_instance where dag_id='$dag' and run_id='$run' and task_id='work'")
  for at in 5 75; do
    sleep $(( at - ${prev:-0} )); prev=$at
    # forked children carry the task's title; subprocess.run children and spawn workers don't, so match those too
    ps -eo pid,ppid,pgid,stat,etimes,args | grep -E -- "$ti|multiprocessing|parse_xml.py|mp_job.py" | grep -v grep \
      > "$out/strays_${at}s.txt"
    n=$(grep -vc ' Z ' "$out/strays_${at}s.txt"); strays="$strays +${at}s:$n"
    echo "+${at}s after success: $n live process(es) still carry the task's id" >> "$out/timeline.txt"
  done
  # the supervisor force-closes sockets a stray kept open (logs "Process exited with open sockets")
  grep -i "open sockets" standalone.log | tail -n +$(( ${w0:-0} + 1 )) > "$out/socket_cleanup_warnings.txt"
  strays="$strays socket_cleanup_warnings:$(wc -l < "$out/socket_cleanup_warnings.txt" | tr -d ' ')"
fi
echo "final state=$final after ${took}s; work_done_at=${done_at:--}; strays=[${strays# }]" | tee -a "$out/timeline.txt"
[ -n "$log" ] && cp "$log" "$out/task.log"
exit 0
