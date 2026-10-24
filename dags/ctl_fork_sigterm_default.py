"""Causality: the question's fork code, with SIGTERM reset to default in each Pool worker."""
import functools
import multiprocessing as mp
import signal

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def process(item, lock, shared):
    with lock:
        shared[item] = item * item
    return item


def _default_sigterm():
    # Forked workers inherit the task runner's SIGTERM handler, which returns without doing anything
    # in any pid but its own; restore the default so Pool.terminate() can kill them.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


def work():
    ctx = mp.get_context("fork")
    manager = ctx.Manager()
    lock, shared = manager.Lock(), manager.dict()
    with ctx.Pool(4, initializer=_default_sigterm) as pool:
        for _ in pool.imap_unordered(functools.partial(process, lock=lock, shared=shared), range(40)):
            pass
        print(f"MP_LOOP_DONE items={len(shared)}", flush=True)
    print(f"MP_DONE method=fork(SIGTERM default in workers) items={len(shared)}", flush=True)
    return len(shared)


with DAG("ctl_fork_sigterm_default", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
