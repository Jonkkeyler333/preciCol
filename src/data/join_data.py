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
    
    