from pyspark.sql.functions import count, when, col

# defines a function to check ids

def check_ids(df):
    """
        This function makes a data quality check to ensure that the ID column
        contains only non-NULL, unique values.
    """
    total_unique_id = df.select("id").distinct().count()
    total_id = df.select("id").count()
    null_id = df.select(count(when(col("id").isNull(), 1)).alias("null_id_count")) \
                  .collect()[0][0]
    
    if total_id - total_unique_id > 0:
        raise ValueError(f"Id column contains non-unique values.")
    if null_id > 0:
        raise ValueError(f"Id column contains {null_id} null values.")
    
    print(f"Column id passed validation: {total_unique_id} unique, non-null IDs.")