import sys
sys.path.insert(0,'/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np
import joblib

if __name__ == "__main__":
    spark = SparkSession.builder.appName("FeatureEngineeringTest").getOrCreate()
    print('Starting feature engineering for test set...')
    path = 'hdfs:///user/hadoop/data_project/features/full_data'
    df = spark.read.parquet(path)

    df_hourly = df.withColumn('hour', F.date_trunc('hour', F.col('time')))
    df_hourly = df_hourly.groupBy('city_id', 'hour').agg(
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
    df_hourly = df_hourly.orderBy('city_id', 'hour')

    df_p = df_hourly.toPandas()
    df_p['hour_timestamp'] = pd.to_datetime(df_p['hour'])
    df_p['day_of_year'] = df_p['hour_timestamp'].dt.dayofyear.astype('int')
    df_p['day_of_week'] = df_p['hour_timestamp'].dt.dayofweek.astype('int')
    df_p['month'] = df_p['hour_timestamp'].dt.month.astype('int')
    df_p['hour_of_day'] = df_p['hour_timestamp'].dt.hour.astype('int')

    df_p["doy_sin"] = np.sin(2 * np.pi * df_p["day_of_year"] / 365)
    df_p["doy_cos"] = np.cos(2 * np.pi * df_p["day_of_year"] / 365)
    df_p["dow_sin"] = np.sin(2 * np.pi * df_p["day_of_week"] / 7)
    df_p["dow_cos"] = np.cos(2 * np.pi * df_p["day_of_week"] / 7)
    df_p["hod_sin"] = np.sin(2 * np.pi * df_p["hour_of_day"] / 24)
    df_p["hod_cos"] = np.cos(2 * np.pi * df_p["hour_of_day"] / 24)

    # Definir columnas a escalar
    features_scaler = [
        'precipitacion_min','precipitacion_max','temp_mean','temp_min','temp_max',
        'rhum_mean','wspd_max', 'wspd_min', 'wspd_avg','wdir_avg','dwpt_avg',
        'pres_max','pres_min','pres_avg', 'coco_max',
        'day_of_year', 'day_of_week', 'hour_of_day'
    ]

    # Cargar scaler previamente ajustado con train
    scaler = joblib.load('hdfs:///user/hadoop/data_project/features/standard_scaler.joblib')

    # Filtrar solo datos de test (por ejemplo, desde 2024-10-01 en adelante)
    threshold_test = pd.Timestamp('2024-10-01')
    test = df_p[df_p['hour_timestamp'] >= threshold_test].copy()

    # Aplicar el scaler
    test[features_scaler] = scaler.transform(test[features_scaler])

    # Quitar columna innecesaria
    test.drop(columns=['hour_timestamp'], inplace=True)

    # Guardar un CSV por mes
    for (year, month), df_month in test.groupby([test['hour'].dt.year, test['hour'].dt.month]):
        fname = f'hdfs:///user/hadoop/data_project/features/test_{year}_{month:02d}.csv'
        df_month.to_csv(fname, index=False)
        print(f"Saved {fname} with shape {df_month.shape}")

    print("Feature engineering para test completado.")