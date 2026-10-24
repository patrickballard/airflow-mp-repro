# airflow-mp-repro

Reproduces "PythonOperator using multiprocessing `Manager()` + `Pool.imap_unordered`: with fork the task finishes its work but stays running; with spawn it crashes" on Airflow 3.3.2, LocalExecutor, real scheduler (`airflow standalone`), Python 3.12, on an Ubuntu GitHub Actions runner.

Run: `gh workflow run repro.yml`. Each case has a 5-minute limit and the job 30. Diagnostics (`ps --forest`, `/proc/<pid>/fd`, `ss -xp`, `py-spy dump`, timelines, task logs) are uploaded as the `diagnostics` artifact.
