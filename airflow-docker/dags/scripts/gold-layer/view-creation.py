from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from datetime import datetime

# passes parameters to fetch data from S3

folder_name = datetime.today().strftime('%Y-%m-%d')
destination_bucket_name = 'openbrewerydb-gold-layer'
origin_bucket_name = 'openbrewerydb-silver-layer'

# Creates spark session

spark = SparkSession.builder \
    .appName("DataFrame") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.520") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Reading data into DataFrame

df = spark.read.parquet(f"s3a://{origin_bucket_name}/{folder_name}/")

# Performing aggregation per location and brewery type

brewery_agg_vw = df.groupBy("brewery_type", "country", "region").agg(count("id").alias("brewery_count")).show()

# Storing data in S3

brewery_agg_vw.write.parquet(f"s3a://{destination_bucket_name}/{folder_name}",
                 mode='overwrite')