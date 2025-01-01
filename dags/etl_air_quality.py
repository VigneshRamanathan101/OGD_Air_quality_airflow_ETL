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
        transform_data = []
        for i, data in enumerate(current_air_quality):
            # Convert JSON data to Python dictionary
            transformed_data = {
                "country": data["country"],
                "state": data["state"],
                "city": data["city"],
                "station": data["station"],
                "last_update": data["last_update"],
                "latitude": float(data["latitude"]),
                "longitude": float(data["longitude"]),
                "pollutant_id": data["pollutant_id"],
                "min_value": int(data["min_value"]),
                "max_value": int(data["max_value"]),
                "avg_value": int(data["avg_value"])
            }

        # Add transformed data to dictionary
        transform_data.append(transformed_data)
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
            last_update TIMESTAMP,
            latitude DECIMAL(10, 8),
            longitude DECIMAL(11, 8),
            pollutant_id VARCHAR(255),
            min_value SMALLINT,
            max_value SMALLINT,
            avg_value SMALLINT
        );
        """)

        # Insert transformed data into database
        cursor.execute("""
            INSERT INTO air_quality (
                country,
                state,
                city,
                station,
                last_update,
                latitude,
                longitude,
                pollutant_id,
                min_value,
                max_value,
                avg_value
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, transform_date)
        
        conn.commit()
        cursor.close()

    ##workflow
    air_quality_data = extract_air_quality()
    transformed_Data = transform_air_quality(air_quality_data)
    load_air_quality(transformed_Data)
    