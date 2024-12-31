from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

API_KEY = Variable.get('OGD_API_KEY')  # in airflow variable
SQLLITE_CONN_ID = 'postgres_default'   # in airflow connections 
API_CONN_ID = 'OGD_API'                # in airflow connections 

default_args = {
    'owner' : 'vignesh',
    'start_date' : days_ago(1)
}

##DAG
with DAG(dag_id= 'air_quality_etl_pipeline',
         default_args= default_args,
         schedule_interval= '@daily',
         catchup= False) as dags :
    
    @task()
    def extract_air_quality():
        
        http_hook = HttpHook(http_conn_id= API_CONN_ID,method='GET')

        endpoint = f"/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69?api-key={API_KEY}&format=json"

        response = http_hook.run(endpoint= endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task()
    def transform_air_quality (air_quality):
        current_air_quality = air_quality["records"]
        print(current_air_quality)
        transform_data = {}
        for data in current_air_quality:
            pass
            #need to work on transform logic
        return transform_data
    
    @task()
    def load_air_quality(transform_date):
        sql_hook = PostgresHook(PostgresHook = SQLLITE_CONN_ID)
        conn = sql_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE air_quality (
            id SERIAL PRIMARY KEY,
            country VARCHAR(255),
            state VARCHAR(255),
            city VARCHAR(255),
            station TEXT,
            last_update DATE,
            latitude DECIMAL(10, 8),
            longitude DECIMAL(11, 8),
            pollutant_id VARCHAR(255),
            min_value VARCHAR(10),
            max_value VARCHAR(10),
            avg_value VARCHAR(10)
        );
        """)

        #need to work on insert logic according to transform data
        cursor.execute("""
        INSERT INTO air_quality (result)
                       Values (%s)
        """,(transform_date['result']
             ))
        
        conn.commit()
        cursor.close()

    ##workflow
    air_quality_data = extract_air_quality()
    transformed_Data = transform_air_quality(air_quality_data)
    load_air_quality(transformed_Data)
    