from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import json,os,requests

spark_s=(SparkSession.builder.appName("Extract Data").getOrCreate())
print(spark_s.context)