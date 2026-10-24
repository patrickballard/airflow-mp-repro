"""Control: fork + Manager, shut down before the task returns."""
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


from mp_common import run_with_manager


def work():
    return run_with_manager("fork", shutdown=True)

with DAG("ctl_fork_manager_shutdown", schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False):
    PythonOperator(task_id="work", python_callable=work)
