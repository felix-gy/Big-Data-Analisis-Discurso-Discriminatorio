import os
"""
flink_job_1_preprocessor.py

JOB: F1 - TextPreprocessor
QUE HACE: limpia el texto del comentario (minusculas, quita URLs, convierte
          emojis a texto, quita signos repetidos) antes de que los demas
          jobs lo analicen
ENTRADA: topic kafka "comentarios-raw" (texto sucio)
SALIDA: topic kafka "comentarios-limpios" (texto limpio)
CAPACIDAD QUE DEMUESTRA: transformacion de stream en tiempo real (map function)
POR QUE FLINK Y NO SPARK: el texto debe limpiarse apenas llega, antes de que
          los siguientes jobs lo analicen en tiempo real. esperar a tener
          todo el dataset (batch) rompe el pipeline en tiempo real.
"""

import json
import re
import emoji
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Types

# ===== configuracion =====
KAFKA_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "172.31.45.213:9092")
JAR_KAFKA = "file:///home/ec2-user/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar"


def limpiar_texto(comentario):
    texto = comentario.lower()
    texto = re.sub(r"http\S+", "", texto)
    texto = re.sub(r"@\w+", "", texto)
    texto = emoji.demojize(texto, language="es")
    texto = texto.replace(":", " ").replace("_", " ")
    texto = re.sub(r"([!?.])\1+", r"\1", texto)
    texto = re.sub(r"[^a-zA-Z0-9ñáéíóúü\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def procesar_mensaje(mensaje_json):
    data = json.loads(mensaje_json)
    data["comentario_limpio"] = limpiar_texto(data["comentario"])
    return json.dumps(data)


def main():
    print("[F1-TextPreprocessor] Iniciando job...", flush=True)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(JAR_KAFKA)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_SERVER) \
        .set_topics("comentarios-raw") \
        .set_group_id("flink-preprocessor") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")
    stream_limpio = stream.map(procesar_mensaje, output_type=Types.STRING())

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_SERVER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("comentarios-limpios")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

    stream_limpio.sink_to(sink)

    print("[F1-TextPreprocessor] Job enviado al cluster. Escuchando 'comentarios-raw'...", flush=True)
    env.execute("F1-TextPreprocessor")


if __name__ == "__main__":
    main()
