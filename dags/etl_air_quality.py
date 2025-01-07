from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.dates import days_ago
from psycopg2.errors import DuplicateTable
from psycopg2 import Error
import datetime

API_KEY = Variable.get('OGD_API_KEY')  # in airflow variable
SQLLITE_CONN_ID = 'postgres_default'   # in airflow connections 
API_CONN_ID = 'OGD_API'                # in airflow connections   
date_format = '%m-%d-%Y %H:%M:%S'


default_args = {
    'owner' : 'vignesh',
    'start_date' : days_ago(1)
}
offset = 0
limit = 20
##DAG
with DAG(dag_id= 'air_quality_etl_pipeline',
         default_args= default_args,
         schedule_interval= '@daily',
         catchup= False) as dags :
    
    @task()
    def extract_air_quality(offset,limit):
        
        http_hook = HttpHook(http_conn_id= API_CONN_ID,method='GET')

        endpoint = f"/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69?api-key={API_KEY}&format=json&offset={offset}&limit={limit}"

        response = http_hook.run(endpoint= endpoint)

        if response.status_code == 200:
            print(response.json())
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
            }
            try:
                transformed_data["min_value"]= int(data.get("min_value"))
            except ValueError:
                transformed_data["min_value"] = -1

            try:
                transformed_data["max_value"]=int(data.get("max_value"))
            except ValueError:
                transformed_data["max_value"] = -1

            try:
                transformed_data["avg_value"]= int(data.get("avg_value"))
            except ValueError:
                transformed_data["avg_value"] = -1

            print(transformed_data)
        # Add transformed data to dictionary
            transform_data.append(transformed_data)

        return transform_data 
    
    @task()
    def load_air_quality(transform_date):
        print("Starting to Load data into SQL......")
        sql_hook = PostgresHook(PostgresHook = SQLLITE_CONN_ID)
        conn = sql_hook.get_conn()
        cursor = conn.cursor()

        try:

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
            
        except DuplicateTable as e:
            print(f"Error: {e}")
            print("Duplicate Table, Table Already Exist")
        except Exception as e:
            print(f"Error: {e}")
            print("Bare Except")
            print("Table Already Exist")
        else:
            # if no exception was raised, the table creation succeeded
            print("Table created successfully") 
        conn.commit()
        

        for i, data in enumerate(transform_date):
            query = f"""
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
                VALUES ('{str(data["country"])}', '{str(data["state"])}', '{data["city"].strip()}', '{str(data["station"])}', 
                        '{datetime.datetime.strptime(data["last_update"].strip(),date_format).strftime('%Y-%m-%d %H:%M:%S')}', {float(data["latitude"])}, {float(data["longitude"])}, 
                        '{str(data["pollutant_id"].strip())}', {data["min_value"]}, {data["max_value"]}, {data["avg_value"]})
            """
                      

            print(f"Query: {query}")

        
        
            try:
                cursor.execute(query)
                print("Inserted succuessfully.....")
            except Error as e:
                conn.rollback()
                print(f"Error: {e}")
                raise e
            conn.commit()
        cursor.close()


    ##workflow

    for _ in range(26):
        air_quality_data = extract_air_quality(offset, limit)
        transformed_Data = transform_air_quality(air_quality_data)
        load_data        = load_air_quality(transformed_Data)
        offset          += limit
    