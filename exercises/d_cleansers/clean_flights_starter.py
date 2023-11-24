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
    cleaned_frame = clean(frame)
    # Load
    compression_type = 'gzip'
    target_path = str(target_dir / "cleaned_flights")
    cleaned_frame.write.parquet(
        path=target_path,
        mode="overwrite",
        # Exercise: how much bigger are the files when the compression codec is set to "uncompressed"? And 'gzip'?
        compression=compression_type,
    )

    # import module
    import os
    # assign size
    size = 0
    
    # assign folder path
    Folderpath = '.'  
    
    # get size
    for ele in os.scandir(target_path):
        size+=os.stat(ele).st_size
        
    dict = {'snappy': 63147169, 'uncompressed': 67053921, 'gzip': 55223112}
    for compression_type, value in dict.items():
        print(f"size for {compression_type}: {value}")
        print(f"this is {value / dict['uncompressed'] *100}% of uncompressed")


