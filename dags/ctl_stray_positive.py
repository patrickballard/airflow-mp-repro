"""Positive control for the stray check: leaves a forked child sleeping after the task succeeds."""
import os
import time

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def work():
    pid = os.fork()
    if pid == 0:  # the stray: inherits the task's title and its stdout/stderr
        time.sleep(300)
        os._exit(0)
    print(f"MP_DONE stray child pid={pid}", flush=True)


with DAG("ctl_stray_positive", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
