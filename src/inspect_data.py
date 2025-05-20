import sys
sys.path.insert(0, '/home/hadoop/myenv/lib/python3.12/site-packages')

from pyspark.sql import SparkSession, functions

def inspect_data(spark,city,txt_output=None):
    hdfs_path=f'"hdfs:///user/hadoop/data_project/{city}/"'
    print(f"Reading data from: {hdfs_path} for city: {city}")
    df=spark.read.parquet(hdfs_path)
    print("Schema:")
    df.printSchema()
    print("Sample rows:")
    df.show(10)
    print("Count of records per month:")
    df.groupBy("month").count().orderBy("month").show()
    total_count=df.count()
    print(f"Total records: {total_count}")
    
if __name__ == "__main__":
    spark=SparkSession.builder.appName("Inspect Data").getOrCreate()
    capitales_departamentos = [
        'Venezuela',        # Amazonas
        'ARAUCA',         # Arauca
        'SOLEDAD',   # Atlántico
        'CARTAGENA DE INDIAS',      # Bolívar
        'SOGAMOSO',          # Boyacá
        'MANIZALES',      # Caldas
        'FLORENCIA',      # Caquetá
        'YOPAL',          # Casanare
        'POPAYÁN',        # Cauca
        'VALLEDUPAR',     # Cesar
        'QUIBDÓ',         # Chocó
        'MONTERÍA',       # Córdoba
        'BOGOTA D.C',    # Cundinamarca (distrito capital)
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
        'SINCELEJO',      # Sucre
        'IBAGUÉ',         # Tolima
        'CALI',           # Valle del Cauca
        'MITÚ',           # Vaupés
        'CUMARIBO',# Vichada
        'SAN JOSÉ DEL GUAVIARE', # Guaviare
        'INÍRIDA'        # Guainía
    ]
    for city in capitales_departamentos:
        inspect_data(spark,city)
    spark.stop()
    print("Data inspection completed.")
