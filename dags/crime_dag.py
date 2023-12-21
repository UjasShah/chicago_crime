from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator
from airflow import DAG, settings
from airflow.models.connection import Connection
from etl_scripts.extract import extract_data
from etl_scripts.load import load_table

from dotenv import load_dotenv
from datetime import datetime

from etl_scripts.extract import extract_data

load_dotenv()

with DAG(
    dag_id="crimes_dag",
    start_date=datetime(2023, 12, 1),
    schedule_interval="@weekly",
    max_active_runs=1 # to help my potato laptop
) as dag:
    
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
        op_kwargs={"date": "{{ ds }}"}
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_name="crimes-cluster",
        project_id="oceanic-hangout-406022",
        region="us-central1",
        cluster_config={
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'n1-standard-2'
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'n1-standard-2'
            }
        }
    )

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id="oceanic-hangout-406022",
        region="us-central1",
        job = {
            'placement': {'cluster_name': 'crimes-cluster'},
            'pyspark_job': {
                'main_python_file_uri': 'gs://chicago_crime_project/transform.py',
                'args': ["{{ ds }}"]}
        }
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name="crimes-cluster",
        project_id="oceanic-hangout-406022",
        region="us-central1"
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_table,
        op_kwargs = {"date": "{{ ds }}"}
    )

    extract >> create_cluster >> submit_pyspark_job >> delete_cluster >> load