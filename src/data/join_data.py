import sys
sys.path.insert(0,'/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.errors import AnalysisException
import pandas as pd
import numpy as np

def city_column(df,cid):
    df=df.withColumn('city_id',lit(cid))
    return df

if __name__ == "__main__":
    CITIES=['SOLEDAD','CARTAGENA DE INDIAS','VALLEDUPAR','BOGOTA D.C','NEIVA','RIOHACHA','PASTO','CÚCUTA','ARMENIA','SAN ANDRÉS']
    spark = SparkSession.builder.appName('UnirCiudadesConID').getOrCreate()
    output_path='hdfs:///user/hadoop/data_project/features/full_data'
    dfs=[]
    for i,city in enumerate(CITIES):
        path=f'hdfs:///user/hadoop/data_project/enriched/{city}'
        try:
            df_city = spark.read.parquet(path)
            df_city = city_column(df_city,i)
            dfs.append(df_city)
        except AnalysisException as e:
            print(f"Error processing {city}: {e}")
    df_join=dfs[0]
    for df in dfs[1:]:
        df_join=df_join.unionByName(df)
    print("Datos unidos:")
    print(df_join.show(5))
    df_join.printSchema()
    df_join.write.mode('overwrite').parquet(output_path)
    