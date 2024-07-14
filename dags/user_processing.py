from airflow import DAG
from datetime import datetime
import json
from pandas import json_normalize
import logging
import pandas as pd

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    logging.info(f"API response: {user}")

    # Adjust the structure of the user dictionary based on the actual response
    user = user['results'][0]  # Accessing the first element of the results list
    user = json_normalize(user)

    processed_user = {
        'firstname': user['name.first'][0],
        'lastname': user['name.last'][0], 
        'country': user['location.country'][0],
        'username': user['login.username'][0],
        'password': user['login.password'][0],
        'email': user['email'][0]
    }

    processed_user_df = pd.DataFrame([processed_user])
    processed_user_df.to_csv('/tmp/processed_user.csv', index=None, header=False)

def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    # Copy file from csv to the table created earlier
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER AS ','",
        filename='/tmp/processed_user.csv'
    )

with DAG(
    'user_processing', 
    start_date=datetime(2024, 6, 16),
    schedule_interval='@daily',
    catchup=False
) as dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS USERS(
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            email TEXT NOT NULL)
        '''
    )
    
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="user_api",
        endpoint="api/"
    )
     
    extract_user = SimpleHttpOperator(
        task_id="extract_user",
        http_conn_id="user_api",
        endpoint="api/",
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_user = PythonOperator(
        task_id="process_user",
        python_callable=_process_user
    )

    store_user = PythonOperator(
        task_id="store_user",
        python_callable=_store_user
    )

    create_table >> is_api_available >> extract_user >> process_user >> store_user