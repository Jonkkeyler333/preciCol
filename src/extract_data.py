from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import json,os,requests

def fetch_data(offset,limit,url):
    params={'$offset':offset,'$limit':limit}
    r=requests.get(url=url,params=params)
    return r.json

spark_s=(SparkSession.builder.appName("Extract Data").getOrCreate())
print(spark_s.sparkContext)
data=fetch_data(offset=0,limit=10000,url='https://www.datos.gov.co/resource/s54a-sgyg.json')
pdf=pd.DataFrame(data)
pdf['fecha_iso'] = pd.to_datetime(pdf['fechaobservacion'])
pdf.sort_values(by='fecha_iso', ascending=True, inplace=True)
spark_df = spark_s.createDataFrame(pdf)
hdfs_output_path ="hdfs:///user/hadoop"
spark_df.write.mode("overwrite").parquet(hdfs_output_path)