import os
"""
flink_job_4_throughput.py

JOB: F4 - ThroughputMonitor
QUE HACE: cuenta cuantos mensajes llegan en ventanas fijas de 10 segundos
          y calcula throughput (mensajes por segundo)
ENTRADA: topic kafka "comentarios-raw" (cuenta todo lo que entra al sistema)
SALIDA: topic kafka "metricas-throughput"
CAPACIDAD QUE DEMUESTRA: ventana de tiempo tipo TUMBLING (ventanas fijas que
          no se solapan) + agregacion (count) sobre el stream completo
POR QUE FLINK Y NO SPARK: el throughput es una metrica que solo tiene sentido
          medida en vivo, mientras el sistema esta recibiendo datos. en batch
          no se puede medir "mensajes por segundo" porque ya no hay flujo
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
VENTANA_SEGUNDOS = 10


class ContarVentana(AllWindowFunction):
    """
    se ejecuta una vez por cada ventana cerrada (cada 10 segundos)
    elements = todos los mensajes que llegaron en esos 10 segundos
    """

    def apply(self, window, elements):
        cantidad = 0
        for _ in elements:
            cantidad += 1

        throughput = cantidad / VENTANA_SEGUNDOS

        resultado = {
            "ventana_fin": int(time.time()),
            "cantidad_mensajes": cantidad,
            "throughput_msg_seg": round(throughput, 2)
        }

        yield json.dumps(resultado)


def main():
    print("[F4-ThroughputMonitor] Iniciando job...", flush=True)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(JAR_KAFKA)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_SERVER) \
        .set_topics("comentarios-raw") \
        .set_group_id("flink-throughput") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")

    stream_throughput = stream \
        .window_all(TumblingProcessingTimeWindows.of(Time.seconds(VENTANA_SEGUNDOS))) \
        .apply(ContarVentana(), Types.STRING())

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_SERVER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("metricas-throughput")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

    stream_throughput.sink_to(sink)

    print("[F4-ThroughputMonitor] Job enviado al cluster. Escuchando 'comentarios-raw'...", flush=True)
    env.execute("F4-ThroughputMonitor")


if __name__ == "__main__":
    main()
