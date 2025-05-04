## Data Engineering Case for Ab-Inbev - BEES :beer: :honeybee:
The goal of this case is to extract data from the [Open Brewery DB ](https://www.openbrewerydb.org/) connecting to its API. We need to return a list of breweries with all its information and populate a Data Lake following the Medallion Architecture:
- Bronze Layer: Layer: The raw data from the API is persisted as a parquet file in a S3 bucket.
- Silver Layer: The data is transformed into a columnar storage format such as
parquet or delta, and partitioned by brewery location. 
- Gold Layer: The data is then aggregated  as a view with the quantity of breweries per type and location.
