"""Fix (a): run the multiprocessing code as its own script."""
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


import os
import subprocess
import sys

SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts", "mp_job.py")


def work():
    subprocess.run([sys.executable, SCRIPT], check=True)

with DAG("fix_subprocess", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
