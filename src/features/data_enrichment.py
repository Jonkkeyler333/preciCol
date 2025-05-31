import sys
import os

os.environ['HOME'] = '/tmp/hadoop_home'  # Directorio temporal escribible
os.makedirs(os.environ['HOME'], exist_ok=True)

# 2. Configurar caché de meteostat
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

def get_meteostat_data(lat:float,lon:float,alt:int,start_date,end_date):
    ubicacion = Point(lat,lon,alt)
    datos_horarios = Hourly(ubicacion,start_date,end_date)
    df_mete = datos_horarios.fetch()
    df_mete.drop_duplicates(inplace=True)
    df_mete.drop(columns=['snow','wpgt','tsun'],inplace=True)
    return df_mete.resample('10min').interpolate(method="time")
    
def enrich_data(path,lat:float,lon:float,alt:int,start_date,end_date):
    df_original=spark.read.parquet(path)
    df_or=df_original.toPandas()
    df_or['fecha_observacion'] = pd.to_datetime(df_or['fecha_observacion'])
    df_or.sort_values(by='fecha_observacion',inplace=True)
    df_or.set_index('fecha_observacion', inplace=True)
    print('Muestra de datos originales:')
    print(df_or.head())
    df_mete=get_meteostat_data(lat,lon,alt,start_date,end_date)
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
    spark_df = spark.createDataFrame(df_final)
    return spark_df
    
if __name__=='__main__':
    CITIES=['SOLEDAD','CARTAGENA DE INDIAS','SOGAMOSO','VALLEDUPAR','BOGOTA D.C','NEIVA','RIOHACHA','PASTO','CÚCUTA','ARMENIA','SAN ANDRÉS']
    LATITUDES=[10.9097,10.3997,5.7148,10.4631,4.6097,2.9275, 11.5442, 1.2136, 7.8939, 4.5339, 12.5847]
    LONGITUDES=[-74.7858,-75.5144,-72.9279,-73.2532,-74.0818,-75.2875,-72.9069,-77.2811,-72.5078, -75.6811,-81.7006]
    ALTITUDES=[5,2,2579,168,2625,442,3,2527,320,1483,6]
    START_DATE=datetime(2024,1,1)
    END_DATE=datetime(2025,1,1)
    spark = SparkSession.builder.appName('data_enrichment').getOrCreate()
    for city,lat,lon,alt in zip(CITIES,LATITUDES,LONGITUDES,ALTITUDES):
        print(f"Enriqueciendo datos para la ciudad: {city}")
        hdfs_path=f'hdfs:///user/hadoop/data_project/processed/{city}'
        output_path=f'hdfs:///user/hadoop/data_project/enriched/{city}'
        try:
            df_final=enrich_data(hdfs_path,lat,lon,alt,START_DATE,END_DATE)
            output_path=f'hdfs:///user/hadoop/data_project/enriched/{city}'
            df_final.write.mode('append').parquet(output_path)
        except AnalysisException as e:
            print(f"Error processing {city}: {e}")
    
    
    