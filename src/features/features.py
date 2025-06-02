import sys
sys.path.insert(0,'/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.errors import AnalysisException
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np

if __name__ == "__main__":
    spark=SparkSession.builder.appName("FeatureEngineering").getOrCreate()
    print('Starting feature engineering process...')
    path='hdfs:///user/hadoop/data_project/features/full_data'
    df=spark.read.parquet(path)
    
    df_hourly=df.withColumn('hour',F.date_trunc('hour',F.col('time')))
    df_hourly=df_hourly.groupBy('city_id','hour').agg(
        F.sum('precipitacion').alias('precipitacion_h'),
        F.min('precipitacion').alias('precipitacion_min'),
        F.max('precipitacion').alias('precipitacion_max'),
        F.avg("temp").alias("temp_mean"),
        F.min("temp").alias("temp_min"),
        F.max("temp").alias("temp_max"),
        F.avg("rhum").alias("rhum_mean"),
        F.max('wspd').alias('wspd_max'),
        F.min('wspd').alias('wspd_min'),
        F.avg("wspd").alias("wspd_avg"),
        F.avg("wdir").alias("wdir_avg"),
        F.avg("dwpt").alias("dwpt_avg"),
        F.max('pres').alias('pres_max'),
        F.min('pres').alias('pres_min'),
        F.avg('pres').alias('pres_avg'),
        F.max('coco').alias('coco_max')
    )
    df_hourly=df_hourly.orderBy('city_id','hour')
    
    df_p=df_hourly.toPandas()
    print(df_p.info())
    print(df_p.head())
 
    df_p['hour_timestamp']=pd.to_datetime(df_p['hour'])
    df_p['day_of_year']=df_p['hour_timestamp'].dt.dayofyear.astype('int')
    df_p['day_of_week']=df_p['hour_timestamp'].dt.dayofweek.astype('int')
    df_p['month']=df_p['hour_timestamp'].dt.month.astype('int')
    df_p['hour_of_day']=df_p['hour_timestamp'].dt.hour.astype('int')
    
    df_p["doy_sin"]=np.sin(2*np.pi*df_p["day_of_year"]/365)
    df_p["doy_cos"]=np.cos(2*np.pi*df_p["day_of_year"]/365)
    df_p["dow_sin"]=np.sin(2*np.pi*df_p["day_of_week"]/7)
    df_p["dow_cos"]=np.cos(2*np.pi*df_p["day_of_week"]/7)
    df_p["hod_sin"]=np.sin(2*np.pi*df_p["hour_of_day"]/24) 
    df_p["hod_cos"]=np.cos(2*np.pi*df_p["hour_of_day"]/24)
    
    threshold_test=pd.Timestamp('2024-10-01')
    train=df_p[df_p['hour_timestamp']<threshold_test].copy()
    val=df_p[df_p['hour_timestamp']>=threshold_test].copy()
    
    print(train[['hour_timestamp', 'precipitacion_h','hour']].head())
    
    features_scaler=['precipitacion_min','precipitacion_max','temp_mean','temp_min','temp_max','rhum_mean','wspd_max', 'wspd_min', 'wspd_avg','wdir_avg','dwpt_avg','pres_max','pres_min','pres_avg', 'coco_max','day_of_year', 'day_of_week', 'hour_of_day']
    
    train_target=train['precipitacion_h'].copy()
    val_target=val['precipitacion_h'].copy()
    
    scaler=StandardScaler()
    scaler.fit(train[features_scaler])  #Ajustar solo con train para evitar data leakage
    
    train[features_scaler]=scaler.transform(train[features_scaler])
    val[features_scaler]=scaler.transform(val[features_scaler])
    
    train.drop(columns=['hour_timestamp'],inplace=True)
    val.drop(columns=['hour_timestamp'],inplace=True)
    
    train.to_csv('hdfs:///user/hadoop/data_project/features/train_hourly.csv')
    val.to_csv('hdfs:///user/hadoop/data_project/features/val_hourly.csv')
    
    train_features=train.drop(columns=['precipitacion_h'])
    val_features=val.drop(columns=['precipitacion_h'])
    
    train_features.to_csv('hdfs:///user/hadoop/data_project/features/train_hourly_features.csv')
    val_features.to_csv('hdfs:///user/hadoop/data_project/features/val_hourly_features.csv')
    train_target.to_csv('hdfs:///user/hadoop/data_project/features/train_hourly_target.csv')
    val_target.to_csv('hdfs:///user/hadoop/data_project/features/val_hourly_target.csv')
    
    print("Feature engineering completed successfully.")
    print("Train data sample:")
    print(train.head())
    print("Validation data sample:")
    print(val.head())
    print("Target variable distribution:")
    print(train_target.describe())