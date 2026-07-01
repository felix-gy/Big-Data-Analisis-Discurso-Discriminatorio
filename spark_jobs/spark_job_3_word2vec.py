"""
spark_job_3_word2vec.py

JOB: S3 - Word2VecModel
QUE HACE: entrena un modelo Word2Vec sobre todos los comentarios del corpus
          historico para aprender que palabras aparecen en contextos similares
          en el discurso electoral peruano. Luego busca palabras semanticamente
          relacionadas con terminos clave de polarizacion politica.
ENTRADA: s3a://bryan-bigdata-proyecto-final/datos/comentarios_procesados.parquet
SALIDA:  s3a://bryan-bigdata-proyecto-final/resultados/s3_word2vec_similares.parquet
         s3a://bryan-bigdata-proyecto-final/modelos/word2vec_model  (modelo guardado)
CAPACIDAD QUE DEMUESTRA: entrenamiento de un modelo de Machine Learning (MLlib)
          que requiere multiples pasadas sobre TODO el corpus para actualizar
          los vectores en cada epoch (imposible en streaming por definicion)
POR QUE SPARK Y NO FLINK: Word2Vec necesita ver el corpus completo varias
          veces (epochs). En streaming los datos llegan uno a uno y no se
          puede hacer multiples pasadas sobre los datos ya vistos.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim
from pyspark.ml.feature import Word2Vec

BUCKET        = "bryan-bigdata-proyecto-final"
RUTA_ENTRADA  = f"s3a://{BUCKET}/datos/comentarios_procesados.parquet"
RUTA_SIMILARES = f"s3a://{BUCKET}/resultados/s3_word2vec_similares.parquet"
RUTA_MODELO   = f"s3a://{BUCKET}/modelos/word2vec_model"

TERMINOS_CLAVE = ["terruco", "comunista", "candidato", "corrupto", "voto", "presidente"]


def main():
    spark = SparkSession.builder \
        .appName("S3-Word2VecModel") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[S3] Leyendo: {RUTA_ENTRADA}")
    df = spark.read.parquet(RUTA_ENTRADA)

    df_tokens = df \
        .filter(col("comentario_limpio").isNotNull()) \
        .withColumn("tokens", split(trim(col("comentario_limpio")), r"\s+")) \
        .filter(col("tokens").isNotNull())

    n = df_tokens.count()
    print(f"[S3] Comentarios para entrenar: {n}")

    word2vec = Word2Vec(
        vectorSize=50,
        minCount=1,
        numPartitions=2,
        inputCol="tokens",
        outputCol="vector",
        seed=42,
    )

    print("[S3] Entrenando modelo Word2Vec ...")
    modelo = word2vec.fit(df_tokens)

    modelo.write().overwrite().save(RUTA_MODELO)
    print(f"[S3] Modelo guardado en: {RUTA_MODELO}")

    resultados = []
    for termino in TERMINOS_CLAVE:
        try:
            similares = modelo.findSynonyms(termino, 5)
            for fila in similares.collect():
                resultados.append({
                    "palabra_clave":    termino,
                    "palabra_similar":  fila["word"],
                    "similitud":        round(float(fila["similarity"]), 4),
                })
        except Exception as exc:
            print(f"[S3] '{termino}' no encontrado en el vocabulario: {exc}")

    if resultados:
        df_sim = spark.createDataFrame(resultados)
        df_sim.orderBy("palabra_clave", col("similitud").desc()).show(40, truncate=False)
        df_sim.write.mode("overwrite").parquet(RUTA_SIMILARES)
        print(f"[S3] Similares guardados en: {RUTA_SIMILARES}")
    else:
        print("[S3] Ninguno de los terminos clave aparecio en el vocabulario.")

    spark.stop()


if __name__ == "__main__":
    main()
