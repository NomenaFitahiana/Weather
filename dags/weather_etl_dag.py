from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Définir les fonctions Python qui appellent tes scripts
def extract_data():
    os.system("python scripts/extract.py")

def transform_data():
    os.system("python scripts/transform.py")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_etl_dag",
    default_args=default_args,
    schedule='0 10 * * *',  # tous les jours à 10h du matin
    catchup=False,
    description="Extraction et transformation météo",
) as dag:

    extract = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_data
    )

    extract >> transform
