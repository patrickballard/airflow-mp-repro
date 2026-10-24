<!-- DRAFT: not posted. -->

**Short version:** the fork hang isn't your code, and it isn't the Manager. In Airflow 3, `Pool.terminate()` can't stop workers forked from inside a task, so the `with Pool(...)` block never exits. With spawn, every child re-runs the `airflow` CLI, and that crashes inside a task process. Close and join the pool yourself (or reset SIGTERM in the workers), or run the multiprocessing code as its own script.

I reproduced both on Airflow 3.3.2: LocalExecutor, a real scheduler (`airflow standalone`), Python 3.12 on Linux. Evidence and runnable repro: [airflow-mp-repro](LINK) (process trees, `/proc/<pid>/status`, py-spy dumps).

### Why fork "finishes the work but stays running"

While your `python_callable` runs, Airflow's task runner installs its own SIGTERM handler. That handler deliberately does nothing in any process except the task runner itself:

```python
# airflow/sdk/execution_time/task_runner.py (3.3.2)
def _on_term(signum, frame):
    if os.getpid() != parent_pid:
        return            # <- in your Pool workers, SIGTERM is swallowed
    ti.task.on_kill()
```

Pool workers are forked from the task runner, so they inherit that handler. Leaving `with Pool(...) as pool:` calls `pool.terminate()`, which:
1. takes the pool's input-queue lock, so idle workers block on it;
2. sends every worker SIGTERM, which the inherited handler ignores;
3. `join()`s them, forever.

py-spy shows the task stuck in `Pool.__exit__ → terminate → _terminate_pool → join`. The workers are blocked on the queue lock with SIGTERM *caught* (`SigCgt` bit 15 in `/proc/<pid>/status`). All results had already come back, which is why it looks like the work is done. Because the task process never exits, Airflow keeps it `running`. `socket_cleanup_timeout` and `task_success_overtime` don't apply, since neither the process nor the task has finished. Only `execution_timeout`, or killing the task, ends it.

### Why spawn crashes

A spawned child first re-runs the parent's `__main__`. In an Airflow task, that's the `airflow` console script. Importing Airflow then fails inside a task process: Airflow 3 blocks DB access there by setting `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=airflow-db-not-allowed:///`, which SQLAlchemy can't parse:

```
File ".../multiprocessing/spawn.py", in _fixup_main_from_path
File ".../bin/airflow", in <module>
    from airflow.__main__ import main
...
sqlalchemy.exc.ArgumentError: Could not parse SQLAlchemy URL from given URL string
```

So making the worker function importable is not enough.

### Fixes (each verified in the repro: task reaches `success`, no leftover processes)

**1. Fork: don't rely on `terminate()`. Close and join the pool yourself.**

```python
ctx = mp.get_context("fork")
with ctx.Manager() as manager:
    lock, results = manager.Lock(), manager.dict()
    pool = ctx.Pool(4)
    for _ in pool.imap_unordered(functools.partial(worker, lock=lock, results=results), items):
        pass
    pool.close()   # workers get "no more work" and exit on their own
    pool.join()
```

**2. Fork: give the workers back the default SIGTERM.**

```python
import signal

def _default_sigterm():
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

with ctx.Pool(4, initializer=_default_sigterm) as pool:
    ...
```

**3. Spawn, or anything heavier: run it as its own script.** A fresh interpreter has no Airflow handler and no `airflow` `__main__`:

```python
def work():
    subprocess.run([sys.executable, "/path/to/parse_xml.py", ...], check=True)
```

with the usual guard in the script:

```python
if __name__ == "__main__":
    main()
```

I'd also set `execution_timeout` on the task, so any future hang fails instead of running forever.
