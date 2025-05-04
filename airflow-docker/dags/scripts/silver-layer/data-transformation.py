from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, concat_ws, lit
from datetime import datetime

# gets the folder name based on the current date (date of extraction)
# passes the origin and destination bucket names

folder_name = datetime.today().strftime('%Y-%m-%d')
origin_bucket_name = 'openbrewerydb-bronze-layer'
destination_bucket_name = 'openbrewerydb-silver-layer'

# sets Spark Session

spark = SparkSession.builder \
    .appName("DataFrame") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.520") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# reads parquet into DataFrame

df = spark.read.parquet(f"s3a://{origin_bucket_name}/{folder_name}")

# transforms the DataFrame
"""
    Region: coalesces the values from state_province and state
    since they have the same values throughout the DataFrame and are redundant.
    The column region can then refer to either a state province or state,
    depending on the country.
    Address_1: coalesces values from columns address_1 and street, since
    it was verified they also refer to the same values and were redudant.
    Address_2: concats values from address_2 and address_3, since it's an 
    optional address complement and had a lot of NULL values in both columns.
"""

transformed_df = df \
    .filter(col("address_2").isNotNull()) \
    .withColumn("address_2", 
        when(
            col("address_2").isNotNull() | col("address_3").isNotNull(), 
            concat_ws(" ",
                coalesce(col("address_2"), lit("")),
                coalesce(col("address_3"), lit(""))
            )
        )
    ) \
    .select(
        "id", 
        "name", 
        "brewery_type", 
        "country",
        coalesce(col("state_province"), col("state")).alias("region"),
        "city", 
        "postal_code", 
        coalesce(col("address_1"), col("street")).alias("address_1"),
        "address_2",
        "longitude", 
        "latitude", 
        "phone", 
        "website_url"
    )

"""
    Since the database has various location-related columns, the granularity chosen
    was to partition it by country and region.
"""

# partition the transformed DataFrame by location
# stores it into S3 silver bucket

partitioned_by_location_df = transformed_df.repartition("country", "region")
partitioned_by_location_df \
    .write.partitionBy("country", "region") \
        .parquet(f"s3a://{destination_bucket_name}/{folder_name}",
                 mode='overwrite')