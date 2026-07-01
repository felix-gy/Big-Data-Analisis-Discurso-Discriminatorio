import os
"""
flink_job_3_sentiment.py

JOB: F3 - SentimentClassifier
QUE HACE: usa pysentimiento (sentiment) para clasificar cada comentario
          como POSITIVO, NEGATIVO o NEUTRAL
ENTRADA: topic kafka "comentarios-clasificados" (ya tiene info de odio del F2)
SALIDA: topic kafka "comentarios-finales" (con sentimiento agregado)
CAPACIDAD QUE DEMUESTRA: segunda aplicacion de modelo NLP distinta (sentiment
          analysis, no hate speech), encadenamiento de streams
POR QUE FLINK Y NO SPARK: el sentimiento de cada comentario debe calcularse
          en el momento que llega para poder usarse despues en la ventana
          de tiempo (job F5), que es streaming puro
"""

import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Types

# ===== configuracion =====
KAFKA_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "172.31.45.213:9092")
JAR_KAFKA = "file:///home/ec2-user/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar"

# IMPORTANTE: inicializacion perezosa del modelo para evitar OutOfMemoryError
# en el cliente Java al serializar el modelo via Py4J durante el envio del job.
_sent_analyzer = None


def _get_sent_analyzer():
    global _sent_analyzer
    if _sent_analyzer is None:
        from pysentimiento import create_analyzer
        _sent_analyzer = create_analyzer(task="sentiment", lang="es")
    return _sent_analyzer


def clasificar_sentimiento(mensaje_json):
    data = json.loads(mensaje_json)
    texto = data.get("comentario_limpio", "")

    if texto.strip() == "":
        data["sentimiento"] = "NEU"
        data["prob_sentimiento"] = 0.0
        return json.dumps(data)

    sent_analyzer = _get_sent_analyzer()
    resultado = sent_analyzer.predict(texto)
    data["sentimiento"] = resultado.output
    data["prob_sentimiento"] = round(max(resultado.probas.values()), 3)

    return json.dumps(data)


def main():
    print("[F3-SentimentClassifier] Iniciando job...", flush=True)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(JAR_KAFKA)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_SERVER) \
        .set_topics("comentarios-clasificados") \
        .set_group_id("flink-sentiment") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")
    stream_final = stream.map(clasificar_sentimiento, output_type=Types.STRING())

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_SERVER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("comentarios-finales")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

    stream_final.sink_to(sink)

    print("[F3-SentimentClassifier] Job enviado al cluster. Escuchando 'comentarios-clasificados'...", flush=True)
    env.execute("F3-SentimentClassifier")


if __name__ == "__main__":
    main()
