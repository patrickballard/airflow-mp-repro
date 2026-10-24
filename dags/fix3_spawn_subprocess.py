"""Fix 3: the spawn parser run as its own process: subprocess.run([sys.executable, "parse_xml.py", ...])."""
import os
import subprocess
import sys

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts", "parse_xml.py")


def work():
    subprocess.run([sys.executable, SCRIPT, "40"], check=True)


with DAG("fix3_spawn_subprocess", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
