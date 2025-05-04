# Data Engineering Case for Ab-Inbev - BEES :beer: :honeybee:

The goal of this case is to extract data from the [Open Brewery DB ](https://www.openbrewerydb.org/) connecting to its API. We need to return a list of breweries with all its information and populate a Data Lake following the Medallion Architecture:

- Bronze Layer: Layer: The raw data from the API is persisted as a parquet file in a S3 bucket.
- Silver Layer: The data is transformed into a columnar storage format such as
parquet or delta, and partitioned by brewery location. 
- Gold Layer: The data is then aggregated  as a view with the quantity of breweries per type and location.

## Technologies involved: 

- Docker
- Airflow
- PySpark
- AWS S3
- Dremio


## Pipeline Architecture

This project runs AirFlow in a Docker container in order to orchestrate the OpenBreweryDB data pipeline. The DAG in Airflow extracts the data from the API and populate it into the layers. The data is persisted into S3 buckets as parquet files. A virtualization layer is created with Dremio which connects to S3 and formats the parquet files as tables. A view is created on top of the silver layer with SQL.

![diagram](architecture_diagram.png)

> :warning:  Note: the Dremio integration was not fully implemented, but the image is included in the Docker Container (commented) and the instance was connected to my S3 bucket. I virtualized the bronze and silver layers formatting it as tables from the parquet files. I then proceeded to create a view on top of the silver layer table with Dremio's SQL engine and stored it as a view. It is currently commented because I was facing some memory issues.

## Tutorial 

To run the contents in this project, first you need to clone the repo as follows:

```
git clone https://github.com/beatrizcm01/bees-data-engineering-case.git
cd bees-data-engineering-case
```

Then you need to have Docker Desktop installed. With Docker ready to go, navigate to the airflow-docker folder in this repo and initialize it:

```
docker-compose up --build
```

Once you have the docker container up and running you should be able to access AirFlow and Dremio via:

```
AirFlow: http://localhost:8080/
Dremio: http://localhost:9047/
```
The layers can be consumed directly from S3 with PySpark. They'll be virtualized in Dremio Lakehouse in a future implementation.

## Improvement Points

The following architecture was developed with free-tier and open-source resources in mind, but they could've been built entirely with AWS resources (Lake Formation, Glue, Athena) or with Azure (Azure Data Lake Storage, Databricks). I've chosen S3 because of familiarity, but proceeded with Dremio because I saw it as a great alternative to Databricks (since my Databricks free-tier had networks limitations, I decided not to use it). To improve the current state of this project I'd add Nessie as the data catalog - which connects with Dremio to virtualize the data in the layers - and DBT (which has a good integration with Dremio) to make the data transformation more scalable. 
