"""
spark_job_1_top_hate_terms.py

JOB: S1 - TopHateTerms
QUE HACE: cuenta la frecuencia de cada palabra en los comentarios marcados
          como discurso discriminatorio y devuelve el top 30 mas frecuentes
ENTRADA: s3a://bryan-bigdata-proyecto-final/datos/comentarios_procesados.parquet
SALIDA:  s3a://bryan-bigdata-proyecto-final/resultados/s1_top_hate_terms.parquet
CAPACIDAD QUE DEMUESTRA: transformacion de texto (split, explode) + agregacion
          (groupBy + count) sobre el corpus historico completo
POR QUE SPARK Y NO FLINK: para saber el top de palabras hay que comparar
          la frecuencia de cada termino contra TODOS los demas del historial.
          En streaming no se puede saber el ranking definitivo porque siempre
          puede llegar un nuevo mensaje que lo cambie.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, trim, length

BUCKET      = "bryan-bigdata-proyecto-final"
RUTA_ENTRADA = f"s3a://{BUCKET}/datos/comentarios_procesados.parquet"
RUTA_SALIDA  = f"s3a://{BUCKET}/resultados/s1_top_hate_terms.parquet"

STOPWORDS = {
    # articulos, preposiciones, conjunciones
    "el","la","de","que","y","a","en","un","ser","se","no","haber","por",
    "con","su","para","como","estar","tener","le","lo","todo","pero","mas",
    "hacer","o","poder","decir","este","ir","otro","ese","si","me","ya",
    "ver","porque","dar","cuando","muy","sin","vez","mucho","saber","los",
    "las","del","al","una","unos","unas","es","son","fue","ha","te","mi",
    "tu","su","nos","hay","les","han","era","eso","aqui","ni","más","también",
    "bien","solo","sino","sino","hasta","sobre","entre","durante","tras",
    "antes","después","aunque","mientras","donde","quien","cuál","cual",
    "cuándo","como","así","tan","tanto","cada","otro","esta","estas","estos",
    # verbos comunes sin valor discriminatorio
    "tiene","hacer","hacer","puede","van","voy","vamos","van","son","están",
    "están","siendo","siendo","hizo","hará","haría","dijo","dice","dijeron",
    "quiero","quiere","queremos","sabe","saben","sé","deben","debe","ir",
    # sustantivos y adjetivos neutros que aparecen en el corpus
    "cara","risa","llorando","sonriendo","gente","señora","señor","persona",
    "personas","todos","todas","nada","algo","alguien","nadie","vez","veces",
    "año","años","día","días","parte","lado","tipo","caso","forma","manera",
    "lugar","país","perú","ciudad","mundo","tiempo","momento","hora","horas",
    "solo","sola","solos","solas","nuevo","nueva","gran","grande","mismo",
    "misma","igual","menos","menos","más","poco","poco","mucho","mucha",
    "sus","sus","les","nos","nos","me","te","se","lo","la","los","las",
    "nunca","siempre","ahora","luego","después","antes","aquí","allí","donde",
    "qué","quién","cómo","cuándo","cuánto","cuál",
}


def main():
    spark = SparkSession.builder \
        .appName("S1-TopHateTerms") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[S1] Leyendo: {RUTA_ENTRADA}")
    df = spark.read.parquet(RUTA_ENTRADA)
    total = df.count()
    print(f"[S1] Total comentarios: {total}")

    df_odio = df.filter(col("es_odio") == True)
    n_odio = df_odio.count()
    print(f"[S1] Comentarios discriminatorios: {n_odio} ({round(n_odio/total*100,1)}%)")

    df_palabras = df_odio.select(
        explode(split(trim(lower(col("comentario_limpio"))), r"\s+")).alias("palabra")
    )
    df_palabras = df_palabras \
        .filter(length(col("palabra")) > 2) \
        .filter(~col("palabra").isin(list(STOPWORDS)))

    top = df_palabras.groupBy("palabra") \
        .count() \
        .orderBy(col("count").desc()) \
        .limit(30)

    print("\n[S1] TOP 30 palabras en discurso discriminatorio:")
    top.show(30, truncate=False)

    top.write.mode("overwrite").parquet(RUTA_SALIDA)
    print(f"[S1] Guardado en: {RUTA_SALIDA}")
    spark.stop()


if __name__ == "__main__":
    main()
