import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce
import boto3
import os
import sys
from datetime import datetime

folder_name = datetime.today().strftime('%Y-%m-%d')
origin_bucket_name = 'openbrewerydb-bronze-layer'
destination_bucket_name = 'openbrewerydb-silver-layer'

spark = SparkSession.builder \
    .appName("DataFrame") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.520") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

df = spark.read.parquet(f"s3a://{origin_bucket_name}/{folder_name}")

coalesced_df = df.select("id", "name", "brewery_type", "country",
                          coalesce(col("state_province"), col("state")).alias("region"),
                          "city", "postal_code", "street", "address_1", "address_2",
                          "address_3", "longitude", "latitude", "phone", "website_url")
coalesced_df.show()



