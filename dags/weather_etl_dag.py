from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Ajouter le dossier des scripts au chemin Python
sys.path.append('/home/noums/airflow/dags/weather/scripts')

# Importer les fonctions des scripts
from extract_current_data import extract_weather_data
from merge_data import merge_weather_data
from clean_data import clean_weather_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 27),
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_etl_dag",
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description="ETL pipeline for weather data extraction, merging, and cleaning",
) as dag:

    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data
    )

    merge_task = PythonOperator(
        task_id="merge_weather_data",
        python_callable=merge_weather_data
    )

    clean_task = PythonOperator(
        task_id="clean_weather_data",
        python_callable=clean_weather_data
    )

    # Définir l'ordre des tâches
    extract_task >> merge_task >> clean_task