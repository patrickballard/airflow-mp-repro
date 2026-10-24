"""Causality: the question's fork code, but close() + join() the Pool so the with-block exit has nothing to terminate."""
import functools
import multiprocessing as mp

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def process(item, lock, shared):
    with lock:
        shared[item] = item * item
    return item


def work():
    ctx = mp.get_context("fork")
    manager = ctx.Manager()
    lock, shared = manager.Lock(), manager.dict()
    with ctx.Pool(4) as pool:
        for _ in pool.imap_unordered(functools.partial(process, lock=lock, shared=shared), range(40)):
            pass
        print(f"MP_LOOP_DONE items={len(shared)}", flush=True)
        pool.close()  # workers get "no more work" and exit on their own: no SIGTERM needed
        pool.join()
    print(f"MP_DONE method=fork(close+join) items={len(shared)}", flush=True)
    return len(shared)


with DAG("ctl_fork_close_join", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
