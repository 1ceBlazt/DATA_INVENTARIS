from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import pandas as pd

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'process_data',
    default_args=default_args,
    description='ETL process from PostgreSQL to CSV using Airflow and Spark',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Function to extract data from PostgreSQL
def extract_data_inventaris_mentahh():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM data_inventaris_mentahh")
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    df.to_csv('/home/achmadakbar/airflow/dags/data_inventaris_mentahh.csv', index=False)  # Sesuaikan path
    cursor.close()
    conn.close()

def extract_data_transaksi_customer():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM data_transaksi_customer")
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    df.to_csv('/home/achmadakbar/airflow/dags/data_transaksi_customer.csv', index=False)  # Sesuaikan path
    cursor.close()
    conn.close()

# Task to extract data from PostgreSQL
extract_task_inventaris = PythonOperator(
    task_id='extract_data_inventaris_mentahh',
    python_callable=extract_data_inventaris_mentahh,
    dag=dag,
)

extract_task_transaksi_penjualan = PythonOperator(
    task_id='extract_data_transaksi_customer',
    python_callable=extract_data_transaksi_customer,
    dag=dag,
)

# Task to process data with Spark
process_data_inventaris = BashOperator(
    task_id='process_data_inventaris_mentahh',
    bash_command='spark-submit --master local /home/achmadakbar/airflow/dags/process_data.py',  # Sesuaikan path
    dag=dag,
)

process_data_transaksi_penjualan = BashOperator(
    task_id='process_data_transaksi_penjualan',
    bash_command='spark-submit --master local /home/airflow/scripts/your_spark_script_transaksi_penjualan.py',  # Sesuaikan path
    dag=dag,
)

# Define task dependencies
extract_task_inventaris >> process_data_inventaris
extract_task_transaksi_penjualan >> process_data_transaksi_penjualan