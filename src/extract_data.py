from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import json,os,requests

def fetch_data(offset,limit,url):
    params={'$offset':offset,'$limit':limit}
    r=requests.get(url=url,params=params)
    return r.json()

spark_s=(SparkSession.builder.appName("Extract Data").getOrCreate())
print(spark_s.sparkContext)
data=fetch_data(offset=0,limit=10000,url='https://www.datos.gov.co/resource/s54a-sgyg.json')
print(type(data))
spark_df=spark_s.createDataFrame(data)
spark_df=spark_df.withColumn('fechaobservacion',spark_df['fechaobservacion'].cast("timestamp"))
spark_df=spark_df.orderBy(spark_df['fechaobservacion'],ascending=True)
spark_df.printSchema()
spark_df.show(10)
hdfs_output_path ="hdfs:///user/hadoop"
spark_df.write.mode("overwrite").parquet(hdfs_output_path)