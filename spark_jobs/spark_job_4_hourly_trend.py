"""
spark_job_4_hourly_trend.py

JOB: S4 - HourlyHateTrend
QUE HACE: agrupa todos los comentarios historicos por hora del dia (UTC-5,
          hora de Peru) y calcula cuantos fueron discriminatorios en cada
          franja horaria, revelando en que horas se concentra este contenido
ENTRADA: s3a://bryan-bigdata-proyecto-final/datos/comentarios_procesados.parquet
SALIDA:  s3a://bryan-bigdata-proyecto-final/resultados/s4_hourly_hate_trend.parquet
CAPACIDAD QUE DEMUESTRA: funciones de fecha/hora (to_timestamp, expr para
          ajuste de zona horaria, hour) + agregacion temporal sobre el
          historico completo del corpus
POR QUE SPARK Y NO FLINK: la tendencia por hora requiere acumular
          comentarios de TODAS las horas del dia para comparar entre si.
          En streaming solo se ve la hora actual, no el patron completo.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, expr, sum as spark_sum, count,
    round as spark_round, to_timestamp, coalesce, lit
)

BUCKET       = "bryan-bigdata-proyecto-final"
RUTA_ENTRADA = f"s3a://{BUCKET}/datos/comentarios_procesados.parquet"
RUTA_SALIDA  = f"s3a://{BUCKET}/resultados/s4_hourly_hate_trend.parquet"

FORMATOS_FECHA = [
    "yyyy-MM-dd'T'HH:mm:ss'Z'",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd",
]


def main():
    spark = SparkSession.builder \
        .appName("S4-HourlyHateTrend") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[S4] Leyendo: {RUTA_ENTRADA}")
    df = spark.read.parquet(RUTA_ENTRADA)
    print(f"[S4] Total comentarios: {df.count()}")

    ts_expr = coalesce(*[to_timestamp(col("fecha"), fmt) for fmt in FORMATOS_FECHA])
    df = df.withColumn("fecha_ts", ts_expr)

    df = df.withColumn("fecha_peru", expr("fecha_ts - INTERVAL 5 HOURS"))
    df = df.withColumn("hora_dia", hour(col("fecha_peru")))

    df_valido = df.filter(col("hora_dia").isNotNull())
    n_valido = df_valido.count()
    print(f"[S4] Comentarios con fecha valida: {n_valido}")

    if n_valido == 0:
        df_valido = df.withColumn("hora_dia", lit(0))

    tendencia = df_valido.groupBy("hora_dia").agg(
        count("*").alias("total_comentarios"),
        spark_sum(col("es_odio").cast("int")).alias("comentarios_odio"),
    ).withColumn(
        "porcentaje_odio",
        spark_round((col("comentarios_odio") / col("total_comentarios")) * 100, 2)
    ).orderBy("hora_dia")

    print("\n[S4] Tendencia de discurso discriminatorio por hora (Peru UTC-5):")
    tendencia.show(24)

    tendencia.write.mode("overwrite").parquet(RUTA_SALIDA)
    print(f"[S4] Guardado en: {RUTA_SALIDA}")
    spark.stop()


if __name__ == "__main__":
    main()
