import sys
sys.path.insert(0,'/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.errors import AnalysisException
import pandas as pd
import numpy as np

if __name__ == "__main__":
    spark=SparkSession.builder.appName("Aggregation").getOrCreate()
    path='./drive/MyDrive/data_project/features/full_data'
    df=spark.read.parquet(path)
    df_daily=df.withColumn('date',F.to_date(F.col('time')))
    df_daily=df_daily.groupBy('city_id','date').agg(
        F.sum('precipitacion').alias('precipitacion_d'),
        F.avg('precipitacion').alias('precipitacion_avg'),
        F.min('precipitacion').alias('precipitacion_min'),
        F.max('precipitacion').alias('precipitacion_max'),
        F.avg("temp").alias("temp_mean"),
        F.min("temp").alias("temp_min"),
        F.max("temp").alias("temp_max"),
        F.avg("rhum").alias("rhum_mean"),
        F.avg("wspd").alias("wspd_mean"),
        F.avg("wdir").alias("wdir_mean"),
        F.avg("dwpt").alias("dwpt_mean"),
        F.max('pres').alias('pres_max'),
        F.min('pres').alias('pres_min'),
        F.max('coco').alias('coco_max') )
        