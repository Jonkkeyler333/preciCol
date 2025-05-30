import sys
sys.path.insert(0,'/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np

def preprocess(path):
    df=spark.read.parquet(path)
    df.printSchema()
    df=df.orderBy('fechaobservacion',ascending=True)
    null_counts=df.select([F.sum(c).isNull().cast('int').alias(c) for c in df.columns])
    print('Missing values')
    null_counts.show()
    df = df.drop('codigoestacion', 'codigosensor', 'departamento', 'descripcionsensor', 'nombreestacion', 'unidadmedida')
    df_t=df.groupby(F.window(F.col('fechaobservacion'),"10 minutes")
    ).agg(F.sum('valorobservado').alias('precipitacion'),
          F.first('latitud').alias('latitud'),
          F.first('longitud').alias('longitud'),
    ).selectExpr("window.start as fecha_observacion","precipitacion","latitud","longitud")
    df_t=df_t.orderBy("fecha_observacion")
    df_t=df_t.dropna()
    df_t.show(5,False)
    return df_t

if __name__=='__main__':
    CITIES=['SOLEDAD','CARTAGENA DE INDIAS','SOGAMOSO','VALLEDUPAR','BOGOTA D.C','NEIVA','RIOHACHA','PASTO','CÚCUTA','ARMENIA','SAN ANDRÉS']
    spark=SparkSession.builder.appName('ventana').getOrCreate()
    for city in CITIES:
        hdfs_path=f'hdfs:///user/hadoop/data_project/{city}/'
        df_temp=preprocess(hdfs_path)
        output_path=f'hdfs:///user/hadoop/data_project/processed/{city}'
        df_temp.write.mode('overwrite').parquet(output_path)
    