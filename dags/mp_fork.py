"""The question's code with fork: a Manager() lock + Pool.imap_unordered, worker defined in this DAG file."""
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


import functools
import multiprocessing as mp


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
        print(f"MP_LOOP_DONE items={len(shared)}", flush=True)  # all work is done; the with-block exit is next
    print(f"MP_DONE method=fork items={len(shared)} manager_pid={manager._process.pid}", flush=True)
    return len(shared)

with DAG("mp_fork", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
