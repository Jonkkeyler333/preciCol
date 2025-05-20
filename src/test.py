import sys
sys.path.insert(0, '/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession, functions

if __name__ == "__main__":
    city = "NEIVA"
    hdfs_path = f"hdfs:///user/hadoop/data_project/{city}/"

    spark = SparkSession.builder.appName("Verify Data").getOrCreate()

    print(f"Reading data from: {hdfs_path}")
    df = spark.read.parquet(hdfs_path)

    print("Schema:")
    df.printSchema()

    print("Sample rows:")
    df.show(10)
    
    df=df.orderBy("fechaobservacion",ascending=True)
    
    print("Sample rows after ordering:")
    df.show(10)

    # print("Count of records per month:")
    # df.groupBy("month").count().orderBy("month").show()

    # total_count = df.count()
    # print(f"Total records: {total_count}")
