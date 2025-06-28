import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.extract.historical_data import fetch_historical_data
from scripts.extract.openweather_api import fetch_current_weather
from scripts.transform.clean_data import clean_and_merge
from scripts.transform.calculate_scores import calculate_weather_scores
from scripts.load.load_to_db import load_to_database
from scripts.load.update_sheets import update_all_sheets

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_tourism_recommendation',
    default_args=default_args,
    description='DAG pour recommander les meilleures périodes de voyage',
    schedule='@daily',
    catchup=False
) as dag:

    # Tâches d'extraction
    extract_historical = PythonOperator(
        task_id='extract_historical_data',
        python_callable=fetch_historical_data,
        op_kwargs={'cities': ['Paris', 'London', 'New York', 'Tokyo', 'Antananarivo', 'Rio de Janeiro', 'Sydney']},
        dag=dag
    )

    extract_current = PythonOperator(
        task_id='extract_current_weather',
        python_callable=fetch_current_weather,
        op_kwargs={'cities': ['Paris', 'London', 'New York', 'Tokyo', 'Antananarivo', 'Rio de Janeiro', 'Sydney']},
        dag=dag
    )

    # Tâches de transformation
    clean_data = PythonOperator(
        task_id='clean_and_merge_data',
        python_callable=clean_and_merge,
        dag=dag
    )

    calculate_scores = PythonOperator(
        task_id='calculate_weather_scores',
        python_callable=calculate_weather_scores,
        dag=dag
    )

    # Tâche de chargement
    load_data = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database,
        dag=dag
    )
    
    # Tâche de mise à jour complète des sheets
    update_sheets = PythonOperator(
        task_id='update_all_sheets',
        python_callable=update_all_sheets,
        dag=dag
    )


    # Orchestration des tâches
    [extract_historical, extract_current] >> clean_data >> calculate_scores >> [load_data, update_sheets]