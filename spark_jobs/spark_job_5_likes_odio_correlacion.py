"""
spark_job_5_likes_odio_correlacion.py

JOB: S5 - LikesOdioCorrelacion
QUE HACE: calcula la correlacion estadistica de Pearson entre la cantidad
          de likes de un comentario y si fue clasificado como discriminatorio,
          y compara el promedio de likes entre comentarios de odio vs normales
ENTRADA: s3a://bryan-bigdata-proyecto-final/datos/comentarios_procesados.parquet
SALIDA:  s3a://bryan-bigdata-proyecto-final/resultados/s5_likes_odio_correlacion.parquet
CAPACIDAD QUE DEMUESTRA: calculo estadistico (correlacion de Pearson) que
          requiere TODA la muestra para ser estadisticamente valido. Con
          streaming se obtendria una correlacion parcial que cambia en cada
          mensaje, nunca convergiendo al valor real del dataset completo.
POR QUE SPARK Y NO FLINK: la correlacion necesita todos los pares (likes, odio)
          simultaneamente para calcular medias y desviaciones sobre el total.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round as spark_round, when, lit

BUCKET       = "bryan-bigdata-proyecto-final"
RUTA_ENTRADA = f"s3a://{BUCKET}/datos/comentarios_procesados.parquet"
RUTA_SALIDA  = f"s3a://{BUCKET}/resultados/s5_likes_odio_correlacion.parquet"


def main():
    spark = SparkSession.builder \
        .appName("S5-LikesOdioCorrelacion") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[S5] Leyendo: {RUTA_ENTRADA}")
    df = spark.read.parquet(RUTA_ENTRADA)
    total = df.count()
    print(f"[S5] Total comentarios: {total}")

    promedios = df.groupBy("es_odio").agg(
        count("*").alias("cantidad"),
        spark_round(avg("likes"), 2).alias("promedio_likes"),
        spark_round(avg("prob_odio"), 3).alias("prob_odio_promedio"),
    ).orderBy("es_odio")

    print("\n[S5] Promedio de likes por tipo:")
    promedios.show()

    df_num = df.withColumn("es_odio_num", col("es_odio").cast("int"))
    correlacion = df_num.stat.corr("es_odio_num", "likes")
    print(f"[S5] Correlacion de Pearson (es_odio vs likes): {round(correlacion, 4)}")

    if correlacion > 0.1:
        interpretacion = "Los comentarios discriminatorios tienden a recibir MAS likes"
    elif correlacion < -0.1:
        interpretacion = "Los comentarios discriminatorios tienden a recibir MENOS likes"
    else:
        interpretacion = "No hay correlacion significativa entre likes y discurso discriminatorio"
    print(f"[S5] Interpretacion: {interpretacion}")

    resultado = promedios \
        .withColumn("correlacion_pearson", lit(round(correlacion, 4))) \
        .withColumn("interpretacion", lit(interpretacion))

    resultado.write.mode("overwrite").parquet(RUTA_SALIDA)
    print(f"[S5] Guardado en: {RUTA_SALIDA}")
    spark.stop()


if __name__ == "__main__":
    main()
