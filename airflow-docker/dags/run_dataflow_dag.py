from airflow.sdk import dag, task, chain
from datetime import datetime, timedelta
import subprocess
import os

# Run script to extract data and persist it raw into S3 bucket 

@dag(
    schedule='@daily',  # Run every day at midnight
    start_date=datetime(2025, 5, 2),  # Set start date to today or whenever you want
    catchup=False,  # To avoid running the DAG for past dates
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=10)}
)

def run_dataflow_dag():
    @task
    def run_bronze_layer_script():
        script_path = os.path.join(os.path.dirname(__file__), 'scripts/bronze-layer', 'data-extraction.py')
        subprocess.run(['python', script_path], check=True)

    # Run script that transforms data from bronze layer and load it into silver S3 bucket

    @task
    def run_silver_layer_script():
        script_path = os.path.join(os.path.dirname(__file__), 'scripts/silver-layer', 'data-transformation.py')
        subprocess.run(['python', script_path], check=True) 
    
    chain(run_bronze_layer_script(), run_silver_layer_script())

dag_instance = run_dataflow_dag()



