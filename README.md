# chicago_crime

This repo is fundamentally an excercise in using various big data tools (Airflow, docker, GCS, spark, Looker, etc.). The goal is to create a data pipeline that will ingest data from the Chicago crime database, transform it, and load it into a data warehouse. The data in the warehouse will then be used to create a dashboard in Looker. 

## File descriptions
- docker-compose.yml: docker-compose file for spinning up the airflow containers that run all the code. This file is a slight modification of the file found [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- dockerfile: This is the companion for the docker compose file. It is used to build the airflow image with the necessary dependencies.
- dags/crime_dag.py: This is the file where the airflow DAG is defined, which is reponsible for orchestrating the entire pipeline.
- dags/etl_scripts/extract.py: This python script extracts data from the Chicago crime database through an API and loads it onto a GCS bucket.
- dags/etl_scripts/transform.py: This python script transforms the data from the GCS bucket using spark on Dataproc, and loads it again onto the GCS bucket as transformed data.
- dags/etl_scripts/load.py: This python script loads the transformed data from the GCS bucket into a BigQuery table. Data from this BigQuery table is then used to create a dashboard in Looker.
- looker_link.txt: This file contains the link to the Looker dashboard.

## How to run
1. Clone this repo
2. Install docker
3. Create an account with Socrata and get an app token and save it to a .env file
4. Set up Google Cloud and create a service account that can access the BigQuery, GCS and Dataproc APIs. Download the service account key as a JSON file and save the path in a .env file
5. Create a GCS project and save the name of the bucket to a .env file
6. Run `docker compose up` to spool up the containers
7. Add a Google Cloud connection to airflow
8. Open airflow Web UI and trigger the DAG

## Architecture
```

+------------------------------------+
| Chicago Crime Database             |
| - Primary source of crime data     |
| - Comprehensive crime statistics   |
+------------------------------------+
                 ||
                 \/
+------------------------------------+
| extract.py Script                  |
| - Extracts data from the database  |
| - Loads data into GCS bucket       |
+------------------------------------+
                 ||
                 \/
+------------------------------------+
| Google Cloud Storage Bucket (GCS)  |
| - Temporary storage for raw data   |
| - Acts as a data staging area      |
+------------------------------------+
                 ||
                 \/
+------------------------------------+
| transform.py Script                |
| - Processes data with Spark        |
| - Uses Google Cloud Dataproc       |
| - Transformed data returned to GCS |
+------------------------------------+
                 ||
                 \/
+------------------------------------+
| Google Cloud Storage Bucket (GCS)  |
| - Receives transformed data        |
+------------------------------------+
                 ||
                 \/
+------------------------------------+
| load.py Script                     |
| - Moves data to BigQuery table     |
+------------------------------------+
                 ||
                 \/
+------------------------------------+
| Google BigQuery Table              |
| - Final data storage               |
+------------------------------------+
                 ||
                 \/
+------------------------------------+
| Looker Dashboard                   |
| - Visualizes data for insights     |
| - Interactive data exploration     |
+------------------------------------+

```

## Data sources
- [Chicago crime database](https://data.cityofchicago.org/Public-Safety/Crimes-2023/xguy-4ndq/about_data)


