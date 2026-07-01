"""
spark_job_2_user_risk_score.py

JOB: S2 - UserRiskScore
QUE HACE: calcula un score de riesgo por autor combinando cuantos de sus
          comentarios fueron clasificados como discriminatorios y cuantos
          likes recibieron (mayor alcance = mayor riesgo social)
ENTRADA: s3a://bryan-bigdata-proyecto-final/datos/comentarios_procesados.parquet
SALIDA:  s3a://bryan-bigdata-proyecto-final/resultados/s2_user_risk_score.parquet
CAPACIDAD QUE DEMUESTRA: agregacion por GRUPO (groupBy autor) aplicando
          una formula personalizada con multiples columnas (window function
          implicita al comparar el historico completo de cada usuario)
POR QUE SPARK Y NO FLINK: el riesgo de un usuario depende de TODO su
          historial acumulado. En streaming solo se ve un comentario a la
          vez y no se puede calcular su ratio historico definitivo.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, when, round as spark_round
)

BUCKET       = "bryan-bigdata-proyecto-final"
RUTA_ENTRADA = f"s3a://{BUCKET}/datos/comentarios_procesados.parquet"
RUTA_SALIDA  = f"s3a://{BUCKET}/resultados/s2_user_risk_score.parquet"


def main():
    spark = SparkSession.builder \
        .appName("S2-UserRiskScore") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[S2] Leyendo: {RUTA_ENTRADA}")
    df = spark.read.parquet(RUTA_ENTRADA)
    print(f"[S2] Total comentarios: {df.count()}")

    por_usuario = df.groupBy("autor").agg(
        count("*").alias("total_comentarios"),
        spark_sum(when(col("es_odio") == True, 1).otherwise(0)).alias("comentarios_odio"),
        spark_sum(when(col("es_odio") == True, col("likes")).otherwise(0)).alias("likes_en_odio"),
        spark_sum("likes").alias("likes_totales"),
    )

    por_usuario = por_usuario \
        .withColumn(
            "porcentaje_odio",
            spark_round((col("comentarios_odio") / col("total_comentarios")) * 100, 2)
        ) \
        .withColumn(
            "score_riesgo",
            spark_round(col("porcentaje_odio") * (1 + col("likes_en_odio") * 0.01), 2)
        )

    resultado = por_usuario \
        .filter(col("total_comentarios") >= 2) \
        .orderBy(col("score_riesgo").desc())

    print("\n[S2] TOP 20 usuarios por score de riesgo:")
    resultado.show(20, truncate=False)

    resultado.write.mode("overwrite").parquet(RUTA_SALIDA)
    print(f"[S2] Guardado en: {RUTA_SALIDA}")
    spark.stop()


if __name__ == "__main__":
    main()
