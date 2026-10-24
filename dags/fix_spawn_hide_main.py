"""Fix (b2): spawn + importable worker + no Manager, and spawn must not re-run the airflow CLI as __main__."""
import multiprocessing as mp
import sys

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from mp_worker import square


def work():
    # A spawned child first re-runs the parent's __main__ script, which in a task is the `airflow` CLI.
    # Importing airflow in a task process fails: the supervisor blocks DB access by setting an
    # unparsable SQLAlchemy URL. Hide __main__'s file while the Pool starts its workers.
    main = sys.modules["__main__"]
    saved = main.__dict__.pop("__file__", None)
    try:
        ctx = mp.get_context("spawn")
        with ctx.Pool(4) as pool:
            n = sum(1 for _ in pool.imap_unordered(square, range(40)))
    finally:
        if saved is not None:
            main.__file__ = saved
    print(f"MP_DONE method=spawn(importable, no manager, no __main__ re-run) items={n}", flush=True)
    return n


with DAG("fix_spawn_hide_main", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
