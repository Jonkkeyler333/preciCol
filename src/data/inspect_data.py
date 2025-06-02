import sys
sys.path.insert(0, '/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession, functions
from pyspark.errors.exceptions.captured import AnalysisException

def inspect_data(spark,city):
    """Inspect the data for a specific city.
    This function reads the data from HDFS for the specified city,
    prints the schema, shows sample rows, and counts the records per month.
    It also prints the total count of records.
    The data is expected to be stored in Parquet format in HDFS under a specific path.
    The path is constructed using the city name, and the data is read into a Spark DataFrame.

    :param spark: SparkSession object used to read data
    :type spark: SparkSession
    :param city: the city for which to inspect the data
    :type city: str
    :raises AnalysisException: if the data for the specified city does not exist in HDFS
    """
    hdfs_path=f'hdfs:///user/hadoop/data_project/{city}/'
    print(f"Reading data from: {hdfs_path} for city: {city}")
    df=spark.read.parquet(hdfs_path)
    print("Schema:")
    df.printSchema()
    print("Sample rows:")
    df.show(10)
    print("Count of records per month:")
    df.groupBy("month").count().orderBy("month").show()
    total_count=df.count()
    print(f"Total records: {total_count}")
    
if __name__ == "__main__":
    spark=SparkSession.builder.appName("Inspect Data").getOrCreate()
    CITIES=['BOGOTA D.C','SAN ANDRÉS','SOLEDAD','CARTAGENA DE INDIAS','SOGAMOSO','VALLEDUPAR','NEIVA','RIOHACHA','PASTO','CÚCUTA','ARMENIA']
    for city in CITIES:
        try:
            print(f"Processing data for {city}")
            inspect_data(spark,city)
        except AnalysisException as e:
            print(f"The data not exists {city}: {e}")
    spark.stop()
    print("Data inspection completed.")
