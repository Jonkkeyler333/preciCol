from pyspark.sql import SparkSession
import json,os,requests

capitales_departamentos = [
    'LETICIA',        # Amazonas
    'MEDELLÍN',       # Antioquia
    'ARAUCA',         # Arauca
    'BARRANQUILLA',   # Atlántico
    'CARTAGENA',      # Bolívar
    'TUNJA',          # Boyacá
    'MANIZALES',      # Caldas
    'FLORENCIA',      # Caquetá
    'YOPAL',          # Casanare
    'POPAYÁN',        # Cauca
    'VALLEDUPAR',     # Cesar
    'QUIBDÓ',         # Chocó
    'MONTERÍA',       # Córdoba
    'BOGOTA, D.C',    # Cundinamarca (distrito capital)
    'INÍRIDA',        # Guainía
    'SAN JOSÉ DEL GUAVIARE', # Guaviare
    'NEIVA',          # Huila
    'RIOHACHA',       # La Guajira
    'SANTA MARTA',    # Magdalena
    'VILLAVICENCIO',  # Meta
    'PASTO',          # Nariño
    'CÚCUTA',         # Norte de Santander
    'MOCOA',          # Putumayo
    'ARMENIA',        # Quindío
    'PEREIRA',        # Risaralda
    'SAN ANDRÉS',     # San Andrés y Providencia
    'BUCARAMANGA',    # Santander
    'SINCELEJO',      # Sucre
    'IBAGUÉ',         # Tolima
    'CALI',           # Valle del Cauca
    'MITÚ',           # Vaupés
    'PUERTO CARREÑO'  # Vichada
]


def fetch_data(offset,limit,url,city):
      params={"$offset":offset,"$limit":limit,"$order":'fechaobservacion',"$where":f"municipio = '{city}'"}
      r=requests.get(url=url,params=params)
      print(r.status_code)
      return r.json()

spark_s=(SparkSession.builder.appName("Extract Data").getOrCreate())
print(spark_s.sparkContext)


for capital in capitales_departamentos:
  print(capital)
  data=fetch_data(offset=0,limit=10,url='https://www.datos.gov.co/resource/s54a-sgyg.json',city=capital)
  print(data)
  spark_df=spark_s.createDataFrame(data)
  spark_df=spark_df.withColumn('fechaobservacion',spark_df['fechaobservacion'].cast("timestamp"))
  spark_df=spark_df.orderBy(spark_df['fechaobservacion'],ascending=True)
  print(spark_df.show(5))