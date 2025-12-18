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



    # step 2: Fetch data from NASA APOD(Astronomy Picture of the Day) API
    #sample API endpoint: https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY
    extract_apod = SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', #Connection ID in Airflow
        endpoint='planetary/apod', # Nasa APOD endpoint
        method='GET',
        data={"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"}, # Using API key from connection
        response_filter=lambda response: response.json(), # Parse the response as JSON
    )



    # step 3: transform the data (extract relevant fields)
    @task
    def transform_apod_data(response):
        apod_data={
            'title': response.get('title' , ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data

    # step 4: Load the data into Postgres SQL database
    @task
    def load_data_to_postgres(apod_data):
        #initialize the postgres hook
        pg_hook = PostgresHook(postgres_conn_id='postgres_connection')

        #SQL query to insert data
        insert_query = """
        INSERT INTO nasa_apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        #Execute the insert query
        pg_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    # step 5: Verify the db in DBViewer
    

    # step 6: Define task dependencies 


