import logging, sys, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from datetime import datetime

# sys.path.append(os.path.abspath('../quality'))
sys.path.append(os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'quality')))
from data_quality import check_ids
# import data_quality

# passes parameters to fetch data from S3
logging.info("Begin.")

folder_name = datetime.today().strftime('%Y-%m-%d')
destination_bucket_name = 'openbrewerydb-gold-layer'
origin_bucket_name = 'openbrewerydb-silver-layer'

# Creates spark session

logging.info("Trying to create a Spark Session.")

spark = SparkSession.builder \
    .appName("DataFrame") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.520") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Reading data into DataFrame

logging.info(f"Reading data from {origin_bucket_name}")

df = spark.read.parquet(f"s3a://{origin_bucket_name}/{folder_name}/")

logging.info("Performing a data quality check.")

# Checking ids for inconsistency

check_ids(df)

# Performing aggregation per location and brewery type

logging.info("Creating aggregated view.")

brewery_agg_vw = df.groupBy("brewery_type", "country", "region").agg(count("id").alias("brewery_count"))

# Storing data in S3

logging.info(f"Storing the data in {destination_bucket_name}.")

brewery_agg_vw.write.parquet(f"s3a://{destination_bucket_name}/{folder_name}",
                 mode='overwrite')

logging.info("Data persisted with success.")

spark.stop()