import sys,os
sys.path.insert(0,'/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession,functions
from pyspark.errors import AnalysisException
import pandas as pd
import numpy as np
import json,os,requests,time

def hdfs_exists(spark_s,path):
    try:
        spark_s.read.parquet(path)
        return True
    except AnalysisException:
        return False

def fetch_data(offset,limit,url:str,max_retries=4,city:str=''):
    if not city:
        raise ValueError("City parameter is required.")
    params={"$offset":offset,
            '$limit':limit,
            '$order':'fechaobservacion',
            "$where":f"fechaobservacion > '2025-01-01T00:00.000' and municipio = '{city.upper()}'"}
    backoff=1
    for i in range(max_retries):
        try:
            r=requests.get(url=url,params=params,timeout=200)
            if r.status_code==429:
                print("Rate limit exceeded.Sleep and retrying...")
                time.sleep(backoff)
                backoff *= 2
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}. Retrying in {backoff} seconds...")
            time.sleep(backoff)
            backoff *= 2
    print(f"Request failed after {max_retries} retries.")
    print(f"Response status code: {r.status_code}")
    print(f"Max retries exceeded. Returning empty list.")
    return []

if __name__ == "__main__":
    CITIES=['SOLEDAD','CARTAGENA DE INDIAS','VALLEDUPAR','BOGOTA D.C','NEIVA','RIOHACHA','PASTO','CÚCUTA','ARMENIA','SAN ANDRÉS']
    spark_s=(SparkSession.builder.appName("Extract Data").getOrCreate())
    print(spark_s.sparkContext)
    for city in CITIES:
        if hdfs_exists(spark_s,f"hdfs:///user/hadoop/data_project/test/{city}/"):
            print(f"Data for {city} already exists. Skipping...")
            continue
        print(f"Processing data for {city}")
        hdfs_output_path =f"hdfs:///user/hadoop/data_project/{city}/"
        url='https://www.datos.gov.co/resource/s54a-sgyg.json'
        offset=0
        limit=10000
        batch_number=0
        while True:
            data=fetch_data(offset=offset,limit=limit,url=url,city=city)
            if not data:
                print("No more data to fetch. Exiting...")
                break
            batch_number+=1
            print(f"Batch number: {batch_number} with offset: {offset} and limit: {limit}")
            spark_df=spark_s.createDataFrame(data)
            spark_df=spark_df.withColumn('fechaobservacion',spark_df['fechaobservacion'].cast("timestamp"))
            spark_df=spark_df.orderBy(spark_df['fechaobservacion'],ascending=True)
            spark_df.printSchema()
            spark_df.show(10)
            spark_df.write.mode("append").parquet(hdfs_output_path)
            offset+=limit
        print(f"Data for {city} processed successfully",end='\n')
        time.sleep(20)
    spark_s.stop()
    print("Data extraction completed successfully.")