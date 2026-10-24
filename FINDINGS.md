# Findings: multiprocessing in a PythonOperator on Airflow 3.3.2

**Setup:**
- Airflow 3.3.2 with the official constraints for Python 3.12.15, on an Ubuntu GitHub Actions runner. Python's default start method there is `fork`.
- `airflow standalone`: a real scheduler with the LocalExecutor, plus the api-server and dag-processor. Postgres is the metadata database.
- Every case was triggered through the scheduler, not `dag.test()`.

**Runs:**
- [40043156632](https://github.com/patrickballard/airflow-mp-repro/actions/runs/40043156632): the question's two cases, controls and fixes.
- [40045281074](https://github.com/patrickballard/airflow-mp-repro/actions/runs/40045281074): every case, plus the causality controls.

Both runs gave the same results. Each case is capped at 270 s. Diagnostics are in the `diagnostics` artifact of each run: `ps --forest`, `/proc/<pid>/status` and `/proc/<pid>/fd`, `ss -xp`, `py-spy dump`, timelines and task logs.

## Results

| Case | Result (both runs) |
|---|---|
| **fork**, as in the question (`Manager()` lock and dict, `with Pool(4)`, `imap_unordered`) | **Hangs.** All the work is done, then the task stays `running` until the cap. |
| **spawn**, as in the question | **Fails** in about 2 s: `sqlalchemy.exc.ArgumentError: Could not parse SQLAlchemy URL`. |
| Control: fork, Pool only, no Manager | success, 3 s |
| Control: fork, Manager shut down before returning | **hangs**, like the fork case |
| Causality: fork, SIGTERM reset to `SIG_DFL` in Pool workers (`initializer=`) | **success, 5 s** |
| Causality: fork, `pool.close(); pool.join()` before the `with` block exits | **success, 5 s** |
| **Fix (a):** `subprocess.run([sys.executable, "scripts/mp_job.py"])` running the same fork + Manager + Pool code | **success, 5 s** |
| **Fix (b):** spawn, importable worker (`include/mp_worker.py` on `PYTHONPATH`), no Manager | **does not work.** Every spawned worker dies with the same `ArgumentError`; the Pool keeps replacing them, and the task stays `running` (11,741 log lines in 5 minutes). |
| **Fix (b2):** fix (b), and spawn must not re-run the `airflow` CLI as `__main__` | **success, 14 s** |

## Root cause, fork: the Pool can't stop its workers, because they inherited Airflow's SIGTERM handler

The task doesn't finish because **the task process itself never exits**. Process tree in run 2:

```
airflow worker -- LocalExecutor: <ti>     supervisor          py-spy: select() in _monitor_subprocess, waiting on the child
  └ airflow worker -- <ti>                task runner         py-spy: work → Pool.__exit__ → terminate → _terminate_pool → join → popen_fork.wait
      ├ airflow worker -- <ti>   (Sl)     Manager server      py-spy: serve_forever, idle
      ├ airflow worker -- <ti>   ×3       Pool workers        py-spy: worker → queues.get → synchronize.__enter__ (blocked on the input queue's lock)
      └ [airflow worker] <defunct>        the 4th worker, exited, never reaped
```

1. **The work does complete.** The task runner's stack is in `Pool.__exit__`, which runs only after the `imap_unordered` loop has consumed every result. The same loop in the `close()` + `join()` control prints its completion marker 2 s after the task starts.
2. **Leaving `with Pool(...)` calls `terminate()`.** `_terminate_pool` first takes the input queue's read lock and doesn't release it, so idle workers block on that lock. It then sends each worker SIGTERM and joins them.
3. **The workers survive SIGTERM.** While `execute()` runs, the task runner installs a handler (`airflow/sdk/execution_time/task_runner.py`):
   ```python
   parent_pid = os.getpid()
   def _on_term(signum, frame):
       pid = os.getpid()
       if pid != parent_pid:
           return          # in any other process: do nothing
       ti.task.on_kill()
   signal.signal(signal.SIGTERM, _on_term)
   ```
   Pool workers are forked from the task runner, so they inherit this Python handler. In a worker the pid differs, so the handler returns. The signal is swallowed, and the worker goes back to waiting for the lock.
   - `/proc/<worker>/status` shows `SigCgt: 0000000000006404`: bit `0x4000`, signal 15 (SIGTERM), is caught.
   - The workers' py-spy stacks are still in the lock acquire at the 270 s cap.
4. **`join()` waits forever.** The task runner never returns from `execute()`, so it never reports success and never exits. The supervisor, correctly, keeps waiting on a live child.
   - `[workers] socket_cleanup_timeout` (60 s, since 3.0.3) only applies after the child has exited.
   - `[core] task_success_overtime` only applies after a terminal state.
   - So neither ends the hang. Only `execution_timeout`, or killing the task, would.

**Causality:** both controls that remove one link of this chain succeed.
- Resetting SIGTERM to its default in the workers, via the Pool `initializer=`.
- Never relying on SIGTERM: `close()` + `join()` lets the workers exit on their own.

The Manager is **not** the cause. Shutting it down doesn't help, and its server process is idle. With a Manager the hang showed up in every run. The Pool-only control finished in both runs. **Likely, but not verified:** the hang needs a worker to be parked on the queue lock when `terminate()` runs. The Manager round-trips make that likely; without them the workers had already exited. A Pool-only task may still hang, depending on timing.

**What I expected, refuted:** that a forked Manager process keeps the supervisor's stdout/stderr socketpairs open after the task runner exits. The task runner never exits, so the sockets are irrelevant here.

## Root cause, spawn: the child re-runs the `airflow` CLI, and Airflow blocks DB access in task processes

```
File ".../multiprocessing/spawn.py", line 297, in _fixup_main_from_path
    main_content = runpy.run_path(main_path, ...)
File ".../.venv/bin/airflow", line 4, in <module>
    from airflow.__main__ import main
File ".../airflow/settings.py", line 822, in initialize
    configure_orm()
...
sqlalchemy.exc.ArgumentError: Could not parse SQLAlchemy URL from given URL string
```

- A spawned child first re-runs the parent's `__main__` script. In an Airflow task, that is the `airflow` console script, because the executor and supervisor processes are forked from the CLI.
- Importing `airflow` runs `settings.initialize()` → `configure_orm()`.
- In task processes the supervisor's `block_orm_access()` has set `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=airflow-db-not-allowed:///` on purpose, and SQLAlchemy can't parse it.
- So every spawned child dies before it imports the worker function:
  - With `Manager()`, the Manager server dies and the task **fails**: the question's "spawn = crash".
  - With a Pool alone, the workers die, the Pool keeps respawning them, and `imap_unordered` **hangs**: fix (b).

An importable worker is necessary for spawn, but not enough. Fix (b2) works because it also keeps spawn from re-running `__main__`:

```python
main = sys.modules["__main__"]
saved = main.__dict__.pop("__file__", None)   # spawn then skips the re-run of __main__
try:
    with mp.get_context("spawn").Pool(4) as pool:
        ...
finally:
    if saved is not None:
        main.__file__ = saved
```

## What works

| Fix | Verdict | Why |
|---|---|---|
| (a) `subprocess.run` of a standalone script | **works** (same fork + Manager code) | A fresh interpreter: no inherited Airflow SIGTERM handler, and no `airflow` `__main__`. |
| fork + `pool.close(); pool.join()` inside the `with` block (or no `with`) | **works** | The workers get "no more work" and exit; `terminate()` has nothing to kill. |
| fork + `initializer` that resets SIGTERM to `SIG_DFL` | **works** | `terminate()` can kill the workers again. |
| (b) spawn + importable worker + no Manager lock | **doesn't work** | The child re-runs the `airflow` CLI and dies on the blocked DB URL. |
| (b2) (b), plus hiding `__main__.__file__` while the Pool starts | **works** | Hacky; prefer (a) or close+join. |

## May differ in your setup

- **Python 3.14** changes the Linux default start method to `forkserver`. The forkserver re-runs `__main__` like spawn, so expect the spawn failure. Not tested here.
- **macOS** defaults to spawn, so the spawn failure applies there unless `fork` is forced.
- **Executors:** Celery and Kubernetes use the same Task SDK supervisor and task runner, so the same handler is inherited. Only the LocalExecutor was tested.
- **Versions:** `_on_term` and `block_orm_access` are what 3.3.2 ships. Other 3.x versions weren't checked.
- **A cap:** with `execution_timeout` set, the hang ends in a timeout failure instead of running forever.
- **Harness:** `MP_LOOP_DONE` was meant to be logged after the loop in `mp_fork.py`, but that edit didn't apply before these runs. It's added now. The evidence that the work completed is the py-spy stack in `Pool.__exit__`.

## The fixes, tested end to end

Run [40049049775](https://github.com/patrickballard/airflow-mp-repro/actions/runs/40049049775) (`suite=fixes`) repeats the earlier run [40048118863](https://github.com/patrickballard/airflow-mp-repro/actions/runs/40048118863) with a stronger stray check; both gave the same results.

**What "fully fixed" means:**
- the task reaches `success`;
- **5 s and 75 s after success**, no process is left that carries the task's id in its title, or that is a `multiprocessing` / `parse_xml.py` / `mp_job.py` child;
- the supervisor logs no "Process exited with open sockets" warning. That warning means a leftover process held its pipes until the 60 s `socket_cleanup_timeout`.

**Positive control:** a task that forks a child and leaves it sleeping. The check sees it at both times (2 processes at +5 s, the stray plus the supervisor; 1 at +75 s, the stray orphaned to PID 1). The supervisor logs `Process exited with open sockets; cleaning up after timeout`, with `socket_types=['stdout', 'stderr', 'logs', 'requests']`.

| Fix | Code | Result | Leftover processes (+5 s / +75 s) | Socket-cleanup warnings |
|---|---|---|---|---|
| 1. `Pool(initializer=...)` resets SIGTERM to `SIG_DFL` (Manager left running, as in the question) | `dags/ctl_fork_sigterm_default.py` | success, 8 s | 0 / 0 | 0 |
| 1, with `with ctx.Manager() as manager:` | `dags/fix1_initializer_manager_ctx.py` | success, 4 s | 0 / 0 | 0 |
| 2. `pool.close(); pool.join()` instead of the `with` block (Manager left running) | `dags/fix2_close_join.py` | success, 2 s | 0 / 0 | 0 |
| 2, with `with ctx.Manager() as manager:` | `dags/fix2_close_join_manager_ctx.py` | success, 3 s | 0 / 0 | 0 |
| 3. spawn parser via `subprocess.run([sys.executable, "parse_xml.py", "40"])` | `dags/fix3_spawn_subprocess.py`, `scripts/parse_xml.py` | success, 4 s | 0 / 0 | 0 |
| (a) fork + Manager script via `subprocess.run` | `dags/fix_subprocess.py`, `scripts/mp_job.py` | success, 4 s | 0 / 0 | 0 |
| Positive control: a forked child left sleeping | `dags/ctl_stray_positive.py` | success, 4 s | **2 / 1** | **1** |

**All six fix it fully.** A Manager left running (fixes 1 and 2 as in the question) doesn't leave a stray. When `work()` returns, its `manager` object is garbage-collected, and multiprocessing's `Finalize` callback shuts the server process down. Still, `with ctx.Manager() as manager:` makes that explicit and doesn't depend on garbage collection.

**Spawn:** only fix 3 is tested here, a separate script. The in-task spawn variant (fix b2, which hides `__main__.__file__`) also works, but it's a hack.
