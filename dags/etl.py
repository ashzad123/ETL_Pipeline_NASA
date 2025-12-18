from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from airflow.utils.dates import days_ago


#Define the DAG
with DAG(
    dag_id='nasa_apod_etl_postgres',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # step 1: Create the table if it doesn't exist



    # step 2: Fetch data from NASA APOD(Astronomy Image of the Day) API


    # step 3: transform the data (extract relevant fields) 


    # step 4: Load the data into Postgres SQL database

    # step 5: Verify the db in DBView 

    # step 6: Define task dependencies 


