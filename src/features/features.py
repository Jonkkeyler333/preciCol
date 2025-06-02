import sys
sys.path.insert(0,'/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.errors import AnalysisException
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np

if __name__ == "__main__":
    spark=SparkSession.builder.appName("Aggregation").getOrCreate()
    print('Starting aggregation process...')
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
    
    df_p=df_daily.toPandas()
    print(df_p.info())
    print(df_p.head())
    df_p['date']=pd.to_datetime(df_p['date'])
    df_p['day_of_year']=df_p['date'].dt.dayofyear.astype('int')
    df_p['day_of_week']=df_p['date'].dt.dayofweek.astype('int')
    df_p['month']=df_p['date'].dt.month.astype('int')
    df_p["doy_sin"]=np.sin(2*np.pi*df_p["day_of_year"]/365)
    df_p["doy_cos"]=np.cos(2*np.pi*df_p["day_of_year"]/365)
    df_p["dow_sin"]=np.sin(2*np.pi*df_p["day_of_week"]/7)
    df_p["dow_cos"]=np.cos(2*np.pi*df_p["day_of_week"]/7)
    
    treshold_test=pd.Timestamp('2024-10-01')
    train=df_p[df_p['date']<treshold_test].copy()
    val=df_p[df_p['date']>=treshold_test].copy()
    
    features_scaler=['precipitacion_d', 'precipitacion_avg', 'precipitacion_min', 'precipitacion_max','temp_mean', 'temp_min', 'temp_max', 'rhum_mean', 'wspd_mean','wdir_mean', 'dwpt_mean', 'pres_max', 'pres_min', 'coco_max','day_of_year', 'day_of_week']
    
    scaler=StandardScaler()
    scaler.fit(train[features_scaler])
    
    train[features_scaler]=scaler.transform(train[features_scaler])
    val[features_scaler]=scaler.transform(val[features_scaler])
    train.drop(columns=['date'],inplace=True)
    val.drop(columns=['date'],inplace=True)
    
    train.to_csv('./drive/MyDrive/data_project/features/train.csv')
    val.to_csv('./drive/MyDrive/data_project/features/val.csv')
    print("Aggregation and feature engineering completed successfully.")
    print("Train data sample:")
    print(train.head())
    print("Validation data sample:")
    print(val.head())