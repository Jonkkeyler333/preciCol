import sys
sys.path.insert(0, '/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np

def preprocess(path):
    df=spark.read.parquet(path)
    df.printSchema()
    df=df.orderBy('fechaobservacion',ascending=True)
    null_counts=df.select([F.sum(c).isNull().cast('int').alias(c) for c in df.columns])
    print('Missing values',null_counts,sep='\n')
    df = df.drop('codigoestacion', 'codigosensor', 'departamento', 'descripcionsensor', 'nombreestacion', 'unidadmedida')
    df_t=df.groupby(F.window(F.col('fechaobservacion'),"10 minutes")
    ).agg(F.sum('valorobservado').alias()
    ).selectExpr("window.start as fecha_10min","precip_10min")
    df_t=df_t.orderBy("fecha_10min")
    df_t.show(5,False)
    

if __name__=='__main__':
    spark=SparkSession.builder.appName('ventana').getOrCreate()
    city='NEIVA'
    hdfs_path=f'hdfs:///user/hadoop/data_project/{city}/'
    preprocess(hdfs_path)
