from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import datetime,timedelta
import requests
import json

default_arg={
    'owner':'ratnesh',
    'retries':1,
    'retry_delay':timedelta(minutes=2)

}
API_CONN_ID='open_meteo_api' # http API connection ID
POSTGRES_CONN_ID='postgres_default'  # postgres connection ID

LATITUDE='51.5074' # manually provided
LONGITUDE='-0.1278' # manually provided

with DAG (
    dag_id='etl_weather',
    description='weather_ETL_Pipeline',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_arg
) as dag:
    @task
    def extracted_data():
        """Extract the data from open meteo weather API using HTTPHooks"""

        # use HTTPHOOK to get the connection details from Airflow connections hooks
        http_hooks=HttpHook(http_conn_id=API_CONN_ID,method='GET')

        #header_url
        #https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&currect_weather=true
        #https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true

        #endpoint
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # make the request to HTTPHOOK to get the data
        response=http_hooks.run(endpoint=endpoint)

        if response.status_code==200:
            return response.json()
        else:
            raise Exception (f'Falied to fetch the weather data via HTTPHOOKS {response.status_code}')
    @task
    def transfomation_weather_data(weather_data):
        """Transfom the wheather data"""
        current_weather=weather_data['current_weather']
        transformed_data={
            'longitude':LONGITUDE,
            'latitude':LATITUDE,
            'temperature':current_weather['temperature'],
            'windspeed':current_weather['windspeed'],
            'winddirection':current_weather['winddirection'],
            'is_day':current_weather['is_day'],
            'weathercode':current_weather['weathercode']

        }
        return transformed_data
    @task
    def Load_transfomed_data(transformed_data):
        """Load the transformed data into Postgres using Postgre_hooks"""

        Postgres_hook=PostgresHook(POSTGRES_CONN_ID=POSTGRES_CONN_ID)
        conn=Postgres_hook.get_conn()
        cursor=conn.cursor()

        # create table if it does not exists
        cursor.execute("""
                        create table if not exists weather_data(
                       longitutde float,
                       latitude float,
                       temperature float,
                       windspeed float,
                       winddirection INT,
                       is_day INT,
                       weathercode INT,
                       timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                       );

                """)
        # insert transformed data into the table
        cursor.execute("""
                        insert into weather_data(longitutde,latitude,temperature,windspeed,winddirection,is_day,weathercode)
                       values (%s, %s, %s, %s, %s, %s, %s)""",(
                           transformed_data['longitude'],
                           transformed_data['latitude'],
                           transformed_data['temperature'],
                           transformed_data['windspeed'],
                           transformed_data['winddirection'],
                           transformed_data['is_day'],
                           transformed_data['weathercode']
                       ))
        conn.commit()
        cursor.close()

    # ETL Pipeline
    weather_data=extracted_data()
    transformed_data=transfomation_weather_data(weather_data)
    Load_transfomed_data(transformed_data)











