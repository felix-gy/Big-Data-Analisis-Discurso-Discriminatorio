import os
"""
flink_job_2_hate_speech.py

JOB: F2 - HateSpeechDetector
QUE HACE: usa pysentimiento (context_hate_speech) para calcular probabilidad
          de odio por categoria (CALLS, CRIMINAL, RACISM, CLASS, APPEARANCE,
          POLITICS, WOMEN, LGBTI) y arma un puntaje agrupado de discurso de odio
ENTRADA: topic kafka "comentarios-limpios" (texto ya limpio)
SALIDA: topic kafka "comentarios-clasificados" (con etiqueta de odio agregada)
CAPACIDAD QUE DEMUESTRA: aplicacion de modelo NLP por mensaje en tiempo real
POR QUE FLINK Y NO SPARK: la deteccion de odio debe alertar al instante
          que llega el comentario, no tiene sentido esperar a acumular
          miles de comentarios para saber si uno especifico es ofensivo
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
UMBRAL = 0.3

# IMPORTANTE: el modelo NO se carga aqui al nivel del modulo.
# Si se cargara aqui, cloudpickle intentaria serializarlo (~500MB) al
# enviarlo al cluster via Py4J y causaria OutOfMemoryError en el cliente Java.
# En su lugar se usa inicializacion perezosa: se carga la primera vez que
# llega un mensaje y se reutiliza en el mismo proceso worker.
_context_analyzer = None


def _get_context_analyzer():
    global _context_analyzer
    if _context_analyzer is None:
        from pysentimiento import create_analyzer
        _context_analyzer = create_analyzer(task="context_hate_speech", lang="es")
    return _context_analyzer


def detectar_odio(mensaje_json):
    data = json.loads(mensaje_json)
    texto = data.get("comentario_limpio", "")

    if texto.strip() == "":
        data["es_odio"] = False
        data["categoria_odio"] = "sin_texto"
        data["prob_odio"] = 0.0
        return json.dumps(data)

    context_analyzer = _get_context_analyzer()
    resultado = context_analyzer.predict(texto)
    p = resultado.probas

    cats = {cat: p.get(cat, 0.0)
            for cat in ["CALLS","CRIMINAL","RACISM","CLASS","APPEARANCE","POLITICS","WOMEN","LGBTI"]}
    max_prob = max(cats.values())
    categoria_principal = max(cats, key=cats.get)

    data["es_odio"] = max_prob > UMBRAL
    data["prob_odio"] = round(max_prob, 3)
    data["categoria_odio"] = categoria_principal if max_prob > UMBRAL else "ninguna"

    return json.dumps(data)


def main():
    print("[F2-HateSpeechDetector] Iniciando job...", flush=True)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(JAR_KAFKA)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_SERVER) \
        .set_topics("comentarios-limpios") \
        .set_group_id("flink-hate-speech") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")
    stream_clasificado = stream.map(detectar_odio, output_type=Types.STRING())

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_SERVER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("comentarios-clasificados")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

    stream_clasificado.sink_to(sink)

    print("[F2-HateSpeechDetector] Job enviado al cluster. Escuchando 'comentarios-limpios'...", flush=True)
    env.execute("F2-HateSpeechDetector")


if __name__ == "__main__":
    main()
