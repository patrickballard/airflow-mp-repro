"""Fix 2: pool.close(); pool.join() instead of the with-block (no terminate()). Manager left running, as in the question."""
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
    manager = ctx.Manager()
    lock, shared = manager.Lock(), manager.dict()
    pool = ctx.Pool(4)
    for _ in pool.imap_unordered(functools.partial(process, lock=lock, shared=shared), range(40)):
        pass
    pool.close()
    pool.join()
    print(f"MP_DONE method=fork(close+join) items={len(shared)}", flush=True)
    return len(shared)

with DAG("fix2_close_join", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
