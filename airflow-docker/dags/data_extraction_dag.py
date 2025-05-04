from airflow.decorators import dag, task
from datetime import datetime, timedelta
import subprocess
import os

# Function to run the Python script
@task
def run_python_script():
    script_path = os.path.join(os.path.dirname(__file__), 'scripts/bronze-layer', 'data-extraction.py')
    subprocess.run(['python', script_path], check=True)

# Define the DAG using @dag decorator
@dag(
    schedule='@daily',  # Run every day at midnight
    start_date=datetime(2025, 5, 2),  # Set start date to today or whenever you want
    catchup=False,  # To avoid running the DAG for past dates
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)}
)
def daily_python_script_dag():
    run_python_script()  # Call the task within the DAG

# This will define the DAG automatically
dag_instance = daily_python_script_dag()
