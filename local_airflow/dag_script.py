import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv

# Add the directory containing your script to the system path
#sys.path.append(os.path.abspath("."))

# Import ETL functions from your script
from etl import extract_data, transform_data, load_data_to_azure

# Load environment variables
load_dotenv()

# Load Azure and API details from environment variables
CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
API_KEY = os.getenv("API_KEY")

# Symbols are hardcoded here (not sensitive data)
SYMBOLS = ['AAPL', 'GOOG', 'MSFT', 'AMZN', 'IBM']

# Default DAG arguments
default_args = {
    'owner': 'Ahmad',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="Capstone_etl_pipeline_stock_data",
    default_args=default_args,
    description="ETL pipeline for extracting, transforming, and loading stock data to Azure Blob Storage",
    schedule_interval=timedelta(days=1),  # Runs daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def extract_task(**kwargs):
        ti = kwargs['ti']
        extracted_df = extract_data(SYMBOLS, API_KEY)

        # Convert Timestamps to ISO format (string) for JSON serialization
        extracted_df['Date'] = extracted_df['Date'].dt.strftime('%Y-%m-%d')

        # Push transformed data to XCom
        ti.xcom_push(key='extracted_data', value=extracted_df.to_dict(orient='records'))


    def transform_task(**kwargs):
        """Task to transform extracted stock data."""
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(task_ids='extract_task', key='extracted_data')
        df = pd.DataFrame(extracted_data)
        transformed_df = transform_data(df)
        ti.xcom_push(key='transformed_data', value=transformed_df.to_dict(orient='records'))

    def load_task(**kwargs):
        """Task to load transformed data to Azure Blob Storage."""
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids='transform_task', key='transformed_data')
        df = pd.DataFrame(transformed_data)
        load_data_to_azure(df, CONNECTION_STRING, CONTAINER_NAME)

    # Define tasks
    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_task,
        provide_context=True
    )

    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load_task,
        provide_context=True
    )

    # Set task dependencies
    extract >> transform >> load

