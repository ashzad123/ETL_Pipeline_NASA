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
    @task
    def create_table():
        #Initialize the postgres hook
        pg_hook = PostgresHook(postgres_conn_id='postgres_connection')

        #SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS nasa_apod_data(
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """

        # Execute the query
        pg_hook.run(create_table_query)



    # step 2: Fetch data from NASA APOD(Astronomy Image of the Day) API


    # step 3: transform the data (extract relevant fields) 


    # step 4: Load the data into Postgres SQL database

    # step 5: Verify the db in DBView 

    # step 6: Define task dependencies 


