import sys,os
sys.path.insert(0,'/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession,functions
from pyspark.errors import AnalysisException
import pandas as pd
import numpy as np
import json,os,requests,time

def hdfs_exists(spark_s,path):
    """Check if a given path exists in HDFS using Spark.
    This function attempts to read a Parquet file from the specified path.
    If the file exists, it returns True; otherwise, it catches an AnalysisException
    :param spark_s: SparkSession object used to interact with Spark
    :type spark_s: SparkSession
    :param path: Path to check in HDFS
    :type path: str
    :return: True if the path exists, False otherwise
    :rtype: bool
    """
    try:
        spark_s.read.parquet(path)
        return True
    except AnalysisException:
        return False


def fetch_data(offset,limit,url:str,max_retries=4,city:str=''):
    """Fetch data from a specified URL with pagination.
    This function retrieves data from a given URL using the specified offset and limit.
    It handles rate limiting by implementing exponential backoff for retries.
    
    :param offset: the offset for pagination
    :type offset: int
    :param limit: size of the data to fetch in each request (batch size)
    :type limit: int
    :param url: the URL to fetch data from
    :type url: str
    :param max_retries: max retries for do the requests, defaults to 4
    :type max_retries: int, optional
    :param city: the city for fetch the data, defaults to ''
    :type city: str, optional
    :raises ValueError: if the city parameter is not provided
    :raises requests.exceptions.RequestException: if the request fails
    :return: JSON response from the API or an empty list if the request fails after retries
    :rtype: list
    """
    if not city:
        raise ValueError("City parameter is required.")
    params={"$offset":offset,
            '$limit':limit,
            '$order':'fechaobservacion',
            "$where":f"fechaobservacion > '2024-01-01T00:00:00.000' and fechaobservacion < '2025-01-01T00:00.000' and municipio = '{city.upper()}'"}
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
    capitales_departamentos = [
        'ARAUCA',        
        'SOLEDAD', 
        'CARTAGENA DE INDIAS',  
        'SOGAMOSO',          
        'MANIZALES',      
        'FLORENCIA',      
        'YOPAL',         
        'POPAYÁN',        
        'VALLEDUPAR',     
        'QUIBDÓ',         
        'MONTERÍA',       
        'BOGOTA D.C',    
        'NEIVA',          
        'RIOHACHA',       
        'SANTA MARTA',    
        'VILLAVICENCIO',  
        'PASTO',          
        'CÚCUTA',        
        'MOCOA',          
        'ARMENIA',        
        'PEREIRA',        
        'SAN ANDRÉS',     
        'SINCELEJO',      
        'IBAGUÉ',         
        'CALI',           
        'MITÚ',         
        'CUMARIBO',
        'SAN JOSÉ DEL GUAVIARE', 
        'BUCARAMANGA',
        'MEDELLÍN',
        'INÍRIDA' 
    ]
    spark_s=(SparkSession.builder.appName("Extract Data").getOrCreate())
    print(spark_s.sparkContext)
    for city in capitales_departamentos:
        if hdfs_exists(spark_s,f"hdfs:///user/hadoop/data_project/{city}/"):
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
            spark_df=spark_df.withColumn('month',functions.month(spark_df['fechaobservacion']))
            spark_df.write.mode("append").partitionBy('month').parquet(hdfs_output_path)
            offset+=limit
        print(f"Data for {city} processed successfully",end='\n')
        time.sleep(20)
    spark_s.stop()
    print("Data extraction completed successfully.")