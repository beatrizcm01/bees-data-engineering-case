import requests
import time
from pyspark.sql import Row, SparkSession
import boto3
import os
import sys
from datetime import datetime
import logging

# Gets the current date of extraction as it will be extracted daily

date_of_extraction = datetime.today().strftime('%Y-%m-%d')

# Configures logging 

logging.basicConfig(
     filename='log_file_name.log',
     level=logging.INFO, 
     format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
     datefmt='%H:%M:%S'
 )

# Sets the enviroment variables

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def fetch_data(current_page_number: int):

    """
    Makes a call to the API endpoint and try to extract the data if the status_code is 200.
    Proceeds to extract the data with an incremental page number parameter until the json is empty.
    Returns the data as a list of dictionaries.

    Arguments:
    current_page_number: integer, should start as 1 as default
    """

    start_time = time.time()

    url = f'https://api.openbrewerydb.org/v1/breweries?page={current_page_number}&per_page=200'
    s = requests.Session()
    response = s.get(url)

    if response.status_code != 200:
        raise Exception(response.status_code)
    elif response.status_code == 200:
        try:
            raw = response.json()  

            end_time = time.time() - start_time
            print(f'Fetched data for page {current_page_number}. \nRunning time: {end_time}s.')

            if len(raw) != 0:
                current_page_number = current_page_number + 1
                return raw + fetch_data(current_page_number)
            
            print(f'There is no data for page {current_page_number}. Extracting process finalized.')
            print(f'Max number of pages retrieved: {current_page_number -1}')
            logging.info("Data extraction completed successfully.")

            return raw 
        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)
            raise

# extracts the data starting at page 1

raw_data = fetch_data(1)

# creates a Spark Session with the necessary parameters


spark = SparkSession.builder \
    .appName("DataFrame") \
    .master("local[*]") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size","16g")  \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.520") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
   
# inserts data into the Dataframe, each dictionary being a row

df = spark.createDataFrame(Row(**x) for x in raw_data) 

# connects to AWS S3 client

client = boto3.client('s3')

# writes the extracted data as a parquet file into a folder with the date of its extraction

logging.info("Starting to compile data into S3 bucket.")

df.write.parquet(f"s3a://openbrewerydb-bronze-layer/{date_of_extraction}", mode="overwrite")

logging.info("Data inserted with success!")
