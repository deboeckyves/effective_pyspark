# Exercise:
# “clean” a CSV file using PySpark.
# * Grab sample data from https://packages.revolutionanalytics.com/datasets/AirlineSubsetCsv.tar.gz
#   A copy of the data can be found on our S3 bucket (link shared in class).
# * Inspect the columns: what type of data do they hold?
# * Create an ETL job with PySpark where you read in the csv file, and perform
#   the cleansing steps mentioned in the classroom:
#   - improve column names (subjective)
#   - fix data types
#   - flag missing or unknown data
#   - remove redundant data
# * Write the data to a parquet file. How big is the parquet file compared
#   to the compressed csv file? And compared to the uncompressed csv file?
#   How long does your processing take?
# * While your job is running, open up the Spark UI and get a feel for what's
#   there (together with the instructor).
# For explanations on the columns, check https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGK
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import *


def read_data(path: Path):
    spark = SparkSession.builder.getOrCreate()
    return spark.read.csv(
        str(path),
        # For a CSV, `inferSchema=False` means every column stays of the string
        # type. There is no time wasted on inferring the schema, which is
        # arguably not something you would depend on in production either.
        inferSchema=False,
        header=True,
        # The dataset mixes two values for null: sometimes there's an empty attribute,
        # which you will see in the CSV file as two neighboring commas. But there are
        # also literal "null" strings, like in this sample: `420.0,null,,1.0,`
        # The following option makes a literal null-string equivalent to the empty value.
        nullValue="null",
    )


def clean(frame: DataFrame) -> DataFrame:
    frame.printSchema()
    #frame.show()
    #frame.show(n=1, truncate=False, vertical=True)
    frame = frame.withColumnRenamed("FL_DATE","FLIGHT_DATE")
    frame = frame.withColumn('FLIGHT_DATE', frame['FLIGHT_DATE'].cast('date'))
    frame = frame.drop("_c44")
    frame.show(n=1, truncate=False, vertical=True)
    frame.printSchema()

    return frame

def agg(frame: DataFrame) -> DataFrame:

    #frame.show(n=1, truncate=False, vertical=True)

    grouped_df = frame.groupBy("UNIQUE_CARRIER").agg(F.avg("CARRIER_DELAY").alias("avg_delay"))
    grouped_df = grouped_df.orderBy(grouped_df["avg_delay"].desc())

    df2 = read_data("/workspace/effective_pyspark/exercises/resources/carriers.csv")

    grouped_df.show()

    df2.show()

    joined_df = grouped_df.join(df2,grouped_df.UNIQUE_CARRIER ==  df2.CARRIER,"left")


    latest = joined_df.groupBy(["UNIQUE_CARRIER",'avg_delay']).agg(F.max("START_DATE_SOURCE").alias("DATE")).select(['UNIQUE_CARRIER', 'avg_delay', 'DATE'])
    latest = latest.withColumnRenamed('avg_delay', 'avg_delay_')

    cond = [latest.UNIQUE_CARRIER ==  joined_df.UNIQUE_CARRIER, latest.DATE ==  joined_df.START_DATE_SOURCE]
    result = latest.join(joined_df, cond ,"left").select(['CARRIER', 'CARRIER_NAME', 'avg_delay'])
    result = result.orderBy(grouped_df["avg_delay"].desc())

    #w = Window.partitionBy('UNIQUE_CARRIER', 'START_DATE_SOURCE').orderBy(desc('START_DATE_SOURCE'))

    #joined_df = joined_df.withColumn("rn",row_number().over(w))

    #joined_df = joined_df.withColumn("rn",row_number().over(w)).filter(col("rn") == 1).drop("second","rn")



    result.show()


    AA_no_flights = frame.filter('UNIQUE_CARRIER' == 'AA').count()


    return frame


def question1(frame: DataFrame) -> DataFrame:

    AA_no_flights = frame.filter(F.col('UNIQUE_CARRIER') == 'AA').filter(F.col('YEAR') == 2011).count()

    print(f"aantal flights: {AA_no_flights}")
    return frame

if __name__ == "__main__":
    # use relative paths, so that the location of this project on your system
    # won't mean editing paths
    path_to_exercises =  Path("/workspace/effective_pyspark/exercises") 
    resources_dir = path_to_exercises / "resources"
    target_dir = path_to_exercises / "target"
    # Create the folder where the results of this script's ETL-pipeline will
    # be stored.
    target_dir.mkdir(exist_ok=True)

    # Extract
    frame = read_data(resources_dir / "flight")
    # Transform
    question1(frame)
     