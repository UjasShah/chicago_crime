from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

load_dotenv() #load environment variables

# GCS stuff
storage_client = storage.Client('oceanic-hangout-406022')
bucket = storage_client.bucket('chicago_crime_project')

client = bigquery.Client("oceanic-hangout-406022")

def load_table(date):
    #Checking if transformed data exists on GCS
    blob = bucket.blob(f'transformed/{date}_crimes.csv/')
    if blob.exists() == False:
        return

    data = f"gs://chicago_crime_project/transformed/{date}_crimes.csv/*"
    table_id = "oceanic-hangout-406022.crime_dataset.crime_table"
    try:
        client.get_table(table_id)
        #append data
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            max_bad_records = 10 # can't figure out why but there are some bad rows in the dataset
        )
        load_job = client.load_table_from_uri(
            data, table_id, job_config=job_config
        ) 
        load_job.result()
    except NotFound:
        # create new table
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            max_bad_records = 10 # can't figure out why but there are some bad rows in the dataset
        )
        load_job = client.load_table_from_uri(
            data, table_id, job_config=job_config
        )
        load_job.result()
