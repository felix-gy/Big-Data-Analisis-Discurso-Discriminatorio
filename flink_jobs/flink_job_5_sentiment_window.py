import os
"""
flink_job_5_sentiment_window.py

JOB: F5 - SentimentWindow
QUE HACE: cada 60 segundos cuenta cuantos comentarios fueron POSITIVOS,
          NEGATIVOS, NEUTRALES y cuantos fueron marcados como odio
ENTRADA: topic kafka "comentarios-finales" (ya tiene sentimiento y odio)
SALIDA: topic kafka "metricas-ventana-sentimiento"
CAPACIDAD QUE DEMUESTRA: ventana de tiempo con AGREGACION POR CATEGORIA
          (distinto a F4 que solo cuenta total, aqui se agrupa por tipo
          de sentimiento), demuestra deteccion de picos/tendencias en vivo
POR QUE FLINK Y NO SPARK: detectar un pico de negatividad o de odio en los
          ultimos 60 segundos solo tiene valor si se sabe AHORA, no minutos
          despues cuando ya paso el momento critico
"""

import json
import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import AllWindowFunction
from pyflink.common import Types

# ===== configuracion =====
KAFKA_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "172.31.45.213:9092")
JAR_KAFKA = "file:///home/ec2-user/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar"
VENTANA_SEGUNDOS = 60


class ContarSentimientos(AllWindowFunction):
    """
    se ejecuta cada vez que se cierra una ventana de 60 segundos
    cuenta por categoria, no solo el total
    """

    def apply(self, window, elements):
        positivos = 0
        negativos = 0
        neutrales = 0
        odio = 0
        total = 0

        for elemento in elements:
            data = json.loads(elemento)
            total += 1

            if data.get("sentimiento") == "POS":
                positivos += 1
            elif data.get("sentimiento") == "NEG":
                negativos += 1
            else:
                neutrales += 1

            if data.get("es_odio", False):
                odio += 1

        resultado = {
            "ventana_fin": int(time.time()),
            "total_comentarios": total,
            "positivos": positivos,
            "negativos": negativos,
            "neutrales": neutrales,
            "odio": odio,
            "porcentaje_odio": round((odio / total * 100), 2) if total > 0 else 0
        }

        yield json.dumps(resultado)


def main():
    print("[F5-SentimentWindow] Iniciando job...", flush=True)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(JAR_KAFKA)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_SERVER) \
        .set_topics("comentarios-finales") \
        .set_group_id("flink-sentiment-window") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")

    stream_ventana = stream \
        .window_all(TumblingProcessingTimeWindows.of(Time.seconds(VENTANA_SEGUNDOS))) \
        .apply(ContarSentimientos(), Types.STRING())

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_SERVER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("metricas-ventana-sentimiento")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

    stream_ventana.sink_to(sink)

    print("[F5-SentimentWindow] Job enviado al cluster. Escuchando 'comentarios-finales'...", flush=True)
    env.execute("F5-SentimentWindow")


if __name__ == "__main__":
    main()
