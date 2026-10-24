"""Fix (b): spawn, an importable worker, no Manager lock."""
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


import multiprocessing as mp

from mp_worker import square


def work():
    ctx = mp.get_context("spawn")
    with ctx.Pool(4) as pool:
        n = sum(1 for _ in pool.imap_unordered(square, range(40)))
    print(f"MP_DONE method=spawn(importable, no manager) items={n}", flush=True)
    return n

with DAG("fix_spawn_importable", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
