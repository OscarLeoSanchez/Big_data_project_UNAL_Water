from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
import subprocess

import os
import sys


project_root = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(project_root)
os.chdir(project_root)


def run_script(script_name):
    path_script = os.path.join(project_root, "spark_jobs", script_name)
    subprocess.run(["python", path_script], check=True)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "data_pipeline",
    default_args=default_args,
    description="Pipeline de ETL de datos para UnalWater en Spark",
    schedule_interval="*/1 * * * *",
    # schedule_interval='@daily',
)

bronze_sensor = FileSensor(
    task_id="wait_for_bronze_data",
    filepath="/unalwater/bronze/_SUCCESS",
    fs_conn_id="hdfs_default",
    poke_interval=10,
    timeout=300,
    dag=dag,
)

bronze_task = PythonOperator(
    task_id="process_bronze",
    python_callable=run_script,
    op_kwargs={"script_name": "process_bronze.py"},
    dag=dag,
)

silver_task = PythonOperator(
    task_id="process_silver",
    python_callable=run_script,
    op_kwargs={"script_name": "process_silver.py"},
    dag=dag,
)

gold_task = PythonOperator(
    task_id="process_gold",
    python_callable=run_script,
    op_kwargs={"script_name": "process_gold.py"},
    dag=dag,
)

bronze_task >> silver_task.set_upstream(bronze_sensor)
silver_task >> gold_task
