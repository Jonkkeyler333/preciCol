import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import when, col

def preprocess(in_path):
    df = (
        spark.read.parquet(in_path)
        .withColumn("valorobservado", col("valorobservado").cast("double"))
        .withColumn("latitud",        col("latitud")       .cast("double"))
        .withColumn("longitud",       col("longitud")      .cast("double"))
    )
    df.printSchema()
    null_exprs = [F.sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
                  for c in df.columns]
    print("Missing values:")
    df.agg(*null_exprs).show(truncate=False)
    cols_to_drop = [
        'codigoestacion','codigosensor','departamento',
        'descripcionsensor','nombreestacion','unidadmedida'
    ]
    df2 = df.drop(*cols_to_drop)
    df_t = (
        df2
        .groupBy(F.window(col("fechaobservacion"), "10 minutes"))
        .agg(
            F.sum("valorobservado") .alias("precipitacion"),
            F.first("latitud")      .alias("latitud"),
            F.first("longitud")     .alias("longitud")
        )
        .selectExpr(
            "window.start AS fecha_observacion",
            "precipitacion",
            "latitud",
            "longitud"
        )
        .orderBy("fecha_observacion")
        .na.drop()
    )
    df_t.printSchema()
    df_t.show(5, False)
    return df_t

if __name__ == "__main__":
    CITIES = [
        'SOLEDAD','CARTAGENA DE INDIAS','SOGAMOSO','VALLEDUPAR',
        'BOGOTA D.C','NEIVA','RIOHACHA','PASTO','CÚCUTA',
        'ARMENIA','SAN ANDRÉS'
    ]
    spark = SparkSession.builder.appName("ventana").getOrCreate()

    for city in CITIES:
        in_path  = f"hdfs:///user/hadoop/data_project/{city}/"
        out_path = f"hdfs:///user/hadoop/data_project/processed/{city}"

        df_temp = preprocess(in_path)

        # df_temp = df_temp.repartition(1, "fecha_observacion")

        df_temp.write.mode("append").parquet(out_path)
