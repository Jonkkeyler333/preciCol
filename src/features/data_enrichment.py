import sys
import os

os.environ['HOME'] = '/tmp/hadoop_home'
os.makedirs(os.environ['HOME'], exist_ok=True)
os.environ["METEOSTAT_CACHE"] = os.path.join(os.environ['HOME'], '.meteostat')
os.makedirs(os.environ["METEOSTAT_CACHE"], exist_ok=True)

sys.path.insert(0,'/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, functions as F
from pyspark.errors import AnalysisException
import pandas as pd
import numpy as np
from datetime import datetime
from meteostat import Point, Hourly
import shutil
import time

def clear_meteostat_cache():
    cache_dir=os.environ.get('METEOSTAT_CACHE', '')
    if cache_dir and os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
        os.makedirs(cache_dir)

def get_meteostat_data(lat:float,lon:float,start_date,end_date):
    ubicacion=Point(lat,lon)
    datos_horarios=Hourly(loc=ubicacion,start=start_date,end=end_date)
    df_mete=datos_horarios.fetch()
    df_mete.drop_duplicates(inplace=True)
    df_mete.drop(columns=['snow','wpgt','tsun'],inplace=True)
    return df_mete.resample('10min').interpolate(method="time")
    
def enrich_data(path,lat:float,lon:float,start_date,end_date):
    df_original=spark.read.parquet(path)
    df_or=df_original.toPandas()
    df_or['fecha_observacion'] = pd.to_datetime(df_or['fecha_observacion'])
    df_or.sort_values(by='fecha_observacion',inplace=True)
    df_or.set_index('fecha_observacion', inplace=True)
    print('Muestra de datos originales:')
    print(df_or.head())
    df_mete=get_meteostat_data(lat,lon,start_date,end_date)
    df_final=df_mete.join(df_or,how='left',rsuffix='_original')
    print('Muestra de datos enriquecidos:')
    print(df_final.head())
    print(df_final.info())
    df_final.fillna(0,inplace=True)
    df_final['precipitacion_f']=np.where(df_final['prcp']-df_final['precipitacion'].abs()>=1,df_final['prcp'],df_final['precipitacion'])
    df_final.drop(columns=['prcp','precipitacion','latitud','longitud'],inplace=True)
    df_final.rename(columns={'precipitacion_f':'precipitacion'},inplace=True)
    print('Datos enriquecidos finales:')
    print(df_final.head())
    df_final.reset_index(inplace=True)
    spark_df = spark.createDataFrame(df_final)
    return spark_df
    
if __name__=='__main__':
    CITIES=['SOLEDAD','CARTAGENA DE INDIAS','VALLEDUPAR','BOGOTA D.C','NEIVA','RIOHACHA','PASTO','CÚCUTA','ARMENIA','SAN ANDRÉS']
    LATITUDES=[10.9097,10.3997,10.4631,4.6097,2.9275, 11.5442, 1.2136, 7.8939, 4.5339, 12.5847]
    LONGITUDES=[-74.7858,-75.5144,-73.2532,-74.0818,-75.2875,-72.9069,-77.2811,-72.5078, -75.6811,-81.7006]
    START_DATE=datetime(2024,1,1)
    END_DATE=datetime(2025,1,1)
    spark = SparkSession.builder.appName('data_enrichment').getOrCreate()
    print("Iniciando el proceso de enriquecimiento de datos...")
    for city,lat,lon in zip(CITIES,LATITUDES,LONGITUDES):
        clear_meteostat_cache()
        print(f"Enriqueciendo datos para la ciudad: {city}")
        hdfs_path=f'hdfs:///user/hadoop/data_project/processed/{city}'
        output_path=f'hdfs:///user/hadoop/data_project/enriched/{city}'
        try:
            df_final=enrich_data(hdfs_path,lat,lon,START_DATE,END_DATE)
            df_final.write.mode('append').parquet(output_path)
            time.sleep(5)
        except AnalysisException as e:
            print(f"Error processing {city}: {e}")
    print("Proceso de enriquecimiento de datos completado.")
    
    
    