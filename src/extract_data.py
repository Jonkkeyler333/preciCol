import sys,os
sys.path.insert(0,'/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession,functions
import pandas as pd
import numpy as np
import json,os,requests


def fetch_data(offset,limit,url):
    params={"$offset":offset,'$limit':limit,'$order':'fechaobservacion',
            "$where":"fechaobservacion > '2010-01-01T00:00:00.000' and fechaobservacion < '2025-01-01T00:00.000'"}
    r=requests.get(url=url,params=params)
    if r.status_code != 200:
        return []
    return r.json()

hdfs_output_path ="hdfs:///user/hadoop/data_project/"
url='https://www.datos.gov.co/resource/s54a-sgyg.json'
spark_s=(SparkSession.builder.appName("Extract Data").getOrCreate())
print(spark_s.sparkContext)
offset=0
limit=10000
batch_number=0

while True:
    data=fetch_data(offset=offset,limit=limit,url=url)
    if not data:
        break
    batch_number+=1
    print(f"Batch number: {batch_number} with offset: {offset} and limit: {limit}")
    spark_df=spark_s.createDataFrame(data)
    spark_df=spark_df.withColumn('fechaobservacion',spark_df['fechaobservacion'].cast("timestamp"))
    spark_df=spark_df.orderBy(spark_df['fechaobservacion'],ascending=True)
    spark_df.printSchema()
    spark_df.show(10)
    spark_df=spark_df.withColumn('year',functions.year(spark_df['fechaobservacion']))
    spark_df.write.mode("append").partitionBy('year').parquet(hdfs_output_path)
    offset+=limit