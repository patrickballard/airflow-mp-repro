"""Control: fork + Pool, no Manager (isolates the Manager)."""
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


import multiprocessing as mp


def square(item):
    return item * item


def work():
    ctx = mp.get_context("fork")
    with ctx.Pool(4) as pool:
        n = sum(1 for _ in pool.imap_unordered(square, range(40)))
    print(f"MP_DONE method=fork(no manager) items={n}", flush=True)
    return n

with DAG("ctl_fork_no_manager", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
