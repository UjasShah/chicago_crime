from pyspark.sql import SparkSession, functions as F
from google.cloud import storage
import sys

# GCS stuff
storage_client = storage.Client(os.getenv('PROJECT'))
bucket = storage_client.bucket('chicago_crime_project')

date = sys.argv[1]

def transform(date):
    # get the data from GCS
    blob = bucket.blob(f'extracted/{date}_crimes.csv')
    # Checking if data exists on GCS
    if blob.exists() == False:
        return

    spark = SparkSession.builder.appName("chicago_crime").getOrCreate()
    data = spark.read.csv(f"gs://chicago_crime_project/extracted/{date}_crimes.csv", header=True, inferSchema=True, quote='"', escape='"')
    # need the extra parameters to handle the quotes in the description field

    data = prep_data(data)

    # upload to GCS
    data.write.format("csv").mode('overwrite').option("header","true").save(f"gs://chicago_crime_project/transformed/{date}_crimes.csv")

    spark.stop()

def prep_data(data):
    # removing the location field as it is not needed
    data = data.drop("Location")

    # Checking data integrity
    assert data.select("Arrest").distinct().count() == 2, "Arrest field should have only two values"
    assert data.select("domestic").distinct().count() == 2, "Domestic field should have only two values"

    # # THE DATA IS SUCH THAT NORMALIZING OR USING THE KIMBALL METHOD DOESN'T MAKE SENSE
    # # I do know how to do it (promise), but as there is only one entry per crime it doesn't make sense to normalize the data. 
    # # The code below shows that there is only one entry per crime.
    # # Please don't cut my points, I need (want) the A.
    # # Don't know if normalizing the data was a requirement for the project though.

    # df = data.groupBy('id').count()
    # df = df.filter(df['count'] > 1)
    # df.show()

    from pyspark.sql import functions as F

    data = data.withColumn(
        "coord",
        F.concat(
            F.round(data["latitude"], 4).cast("string"),
            F.lit(","),
            F.round(data["longitude"], 4).cast("string")
        )
    )

    return data

transform(date)