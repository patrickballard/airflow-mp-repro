"""Fix 2, Manager shut down: close() + join(), and a with-block Manager."""
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
    # forked workers inherit the task runner's SIGTERM handler, which ignores the signal in other pids
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

def work():
    ctx = mp.get_context("fork")
    with ctx.Manager() as manager:
        lock, shared = manager.Lock(), manager.dict()
        pool = ctx.Pool(4)
        for _ in pool.imap_unordered(functools.partial(process, lock=lock, shared=shared), range(40)):
            pass
        pool.close()
        pool.join()
        n = len(shared)
    print(f"MP_DONE method=fork(close+join, manager ctx) items={n}", flush=True)
    return n

with DAG("fix2_close_join_manager_ctx", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
