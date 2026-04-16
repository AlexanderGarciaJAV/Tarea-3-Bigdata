from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    avg, max, min, count, round as spark_round,
    window, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType
)

json_schema = StructType([
    StructField("Fecha",           StringType(),  True),
    StructField("Valor_TRM",       DoubleType(),  True),
    StructField("Unidad",          StringType(),  True),
    StructField("Variacion_Pct",   DoubleType(),  True),
    StructField("Tendencia",       StringType(),  True),
    StructField("Alerta",          StringType(),  True),
    StructField("Timestamp_Envio", DoubleType(),  True),
])

spark = SparkSession.builder \
    .appName("TRM_Streaming_Consumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trm_stream") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df \
    .select(col("value").cast("string").alias("json_data")) \
    .select(from_json(col("json_data"), json_schema).alias("data")) \
    .select("data.*") \
    .withColumn("Fecha_TRM",        to_timestamp(col("Fecha"), "yyyy-MM-dd")) \
    .withColumn("Tiempo_Procesado", current_timestamp()) \
    .withColumn("Clasificacion_Valor",
        when(col("Valor_TRM") >= 4500, "TRM_MUY_ALTA")
        .when(col("Valor_TRM") >= 4000, "TRM_ALTA")
        .when(col("Valor_TRM") >= 3000, "TRM_MEDIA")
        .when(col("Valor_TRM") >= 2000, "TRM_BAJA")
        .otherwise("TRM_HISTORICA")
    )

stats_df = parsed_df \
    .groupBy(
        window(col("Tiempo_Procesado"), "30 seconds"),
        col("Fecha").alias("Fecha_Moneda"),
        col("Tendencia"),
        col("Clasificacion_Valor")
    ) \
    .agg(
        count("Valor_TRM").alias("Total_Registros"),
        spark_round(avg("Valor_TRM"), 2).alias("TRM_Promedio"),
        spark_round(min("Valor_TRM"), 2).alias("TRM_Minima"),
        spark_round(max("Valor_TRM"), 2).alias("TRM_Maxima"),
        spark_round(avg("Variacion_Pct"), 4).alias("Var_Promedio_Pct")
    )

query = stats_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 50) \
    .start()

query.awaitTermination()
