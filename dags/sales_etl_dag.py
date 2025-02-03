from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from src.main.transformations.jobs.draft_job import extract_data, transform_data, load_data

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='An ETL pipeline for sales data',
    schedule_interval='@daily',  # Run daily
    catchup=False,  # Disable catchup to avoid backfilling
)


# Task 1: Extract Data
def extract_task(**kwargs):
    logger.info("Starting Extract Task")
    csv_files, error_files = extract_data()
    kwargs['ti'].xcom_push(key='csv_files', value=csv_files)
    kwargs['ti'].xcom_push(key='error_files', value=error_files)
    logger.info("Extract Task Completed")


extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    provide_context=True,
    dag=dag,
)


# Task 2: Transform Data
def transform_task(**kwargs):
    logger.info("Starting Transform Task")
    ti = kwargs['ti']
    csv_files = ti.xcom_pull(task_ids='extract_data', key='csv_files')
    error_files = ti.xcom_pull(task_ids='extract_data', key='error_files')
    final_df_to_process, correct_files = transform_data(csv_files, error_files)
    ti.xcom_push(key='final_df_to_process', value=final_df_to_process)
    ti.xcom_push(key='correct_files', value=correct_files)
    logger.info("Transform Task Completed")


transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)


# Task 3: Load Data
def load_task(**kwargs):
    logger.info("Starting Load Task")
    ti = kwargs['ti']
    final_df_to_process = ti.xcom_pull(task_ids='transform_data', key='final_df_to_process')
    correct_files = ti.xcom_pull(task_ids='transform_data', key='correct_files')
    load_data(final_df_to_process, correct_files)
    logger.info("Load Task Completed")


load = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)
