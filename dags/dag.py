from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import requests


def execute_zeppelin_notebook_1(**kwargs):
    # Zeppelin API endpoint to execute a notebook
    zeppelin_api_url = 'http://localhost:8082/api/notebook/job/2JHG36R93'

    # Make a POST request to execute the Zeppelin notebook
    response = requests.post(zeppelin_api_url)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        print("Zeppelin notebook executed successfully 1.")
    else:
        print(f"Failed to execute Zeppelin notebook 1. Status code: {response.status_code}")
        print(response.text)


def execute_zeppelin_notebook_2(**kwargs):
    # Zeppelin API endpoint to execute a notebook
    zeppelin_api_url = 'http://localhost:8082/api/notebook/job/2JHBW3XEZ'

    # Make a POST request to execute the Zeppelin notebook
    response = requests.post(zeppelin_api_url)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        print("Zeppelin notebook executed successfully 2.")
    else:
        print(f"Failed to execute Zeppelin notebook 2. Status code: {response.status_code}")
        print(response.text)


def execute_zeppelin_notebook_3(**kwargs):
    # Zeppelin API endpoint to execute a notebook
    zeppelin_api_url = 'http://localhost:8082/api/notebook/job/2JKGE5HMJ'

    # Make a POST request to execute the Zeppelin notebook
    response = requests.post(zeppelin_api_url)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        print("Zeppelin notebook executed successfully 2.")
    else:
        print(f"Failed to execute Zeppelin notebook 3. Status code: {response.status_code}")
        print(response.text)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the main DAG
dag = DAG(
    'Twitter_DAG',
    default_args=default_args,
    description='An Airflow DAG to fetch Twitter data and update the Hive Data',
    schedule_interval='*/5 * * * * *',  # Run every 5 minutes
    max_active_runs=1,  # Ensure only one run at a time
    catchup=False,  # Do not run backfill for the intervals between start_date and the current date
)

with dag:
    # Use the PythonOperator to execute the Zeppelin notebook
    kafka_stream = PythonOperator(
        task_id='Kafka_Stream_From_Twitter',
        python_callable=execute_zeppelin_notebook_1,
        provide_context=True,  # Pass the context to the Python function

    )

    spark_stream = PythonOperator(
        task_id='Spark_Stream_To_Hive',
        python_callable=execute_zeppelin_notebook_2,
        provide_context=True,  # Pass the context to the Python function

    )
    
    # model_prediction = PythonOperator(
    #     task_id='model_prediction',
    #     python_callable=execute_zeppelin_notebook_3,
    #     provide_context=True,  # Pass the context to the Python function

    # )


    # Set task dependencies
    kafka_stream >> spark_stream 