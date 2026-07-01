"""
productor.py

lee el parquet de comentarios y los manda a kafka uno por uno
simulando que llegan en tiempo real (como si fuera un live)

topic de salida: comentarios-raw

USO:
  python3 productor.py                    -> usa el archivo local por defecto
  python3 productor.py ruta/archivo.parquet -> usa ese archivo
  python3 productor.py s3://bucket/ruta.parquet -> lee desde S3

ANTES DE CORRER: asegurarse de que los 5 jobs de Flink esten en estado
RUNNING en el dashboard (http://<IP_MASTER>:8081). Si no estan corriendo,
los mensajes llegan a Kafka pero nadie los procesa.
"""

import pandas as pd
import time
import json
import sys
import os
from kafka import KafkaProducer

# ===== configuracion =====
KAFKA_SERVER = "localhost:9092"
TOPIC = "comentarios-raw"
DELAY_SEGUNDOS  = 0.2   # 1 mensaje cada 0.2s = chat en vivo natural
LOTE            = 10    # flush cada 10 mensajes
DELAY_LOTE      = 0.0   # sin pausa extra entre lotes

# ruta del parquet: primero argumento de linea de comandos, luego archivo local,
# luego S3 como ultimo recurso
if len(sys.argv) > 1:
    ARCHIVO_PARQUET = sys.argv[1]
elif os.path.exists(os.path.expanduser("~/combined.parquet")):
    ARCHIVO_PARQUET = os.path.expanduser("~/combined.parquet")
elif os.path.exists(os.path.expanduser("~/64GeXAfDQeg_comentarios.parquet")):
    ARCHIVO_PARQUET = os.path.expanduser("~/64GeXAfDQeg_comentarios.parquet")
else:
    BUCKET_S3 = os.environ.get("BUCKET_S3", "bryan-bigdata-proyecto-final")
    ARCHIVO_PARQUET = f"s3://{BUCKET_S3}/datos/combined.parquet"

print(f"[productor] Leyendo datos desde: {ARCHIVO_PARQUET}")

# ===== conectar a kafka =====
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"[productor] Conectado a Kafka en {KAFKA_SERVER}")

# ===== leer el dataset =====
df = pd.read_parquet(ARCHIVO_PARQUET)
print(f"[productor] Dataset cargado: {len(df)} filas totales")
print(f"[productor] Columnas: {list(df.columns)}")

# El dataset combined.parquet tiene dos tipos de filas:
#   - Comentarios YouTube: texto en columna 'comentario'
#   - Chat en vivo:        texto en columna 'mensaje'
# Unificamos ambos en 'comentario' y descartamos filas sin texto.
if "mensaje" in df.columns:
    df["comentario"] = df["comentario"].fillna(df["mensaje"])

df = df[df["comentario"].notna() & (df["comentario"].str.strip() != "")]
df = df.reset_index(drop=True)
print(f"[productor] Filas con texto valido: {len(df)}")
print(f"[productor] Enviando en lotes de {LOTE} mensajes (pausa {DELAY_LOTE}s entre lotes)...")

def safe_int(val):
    try:
        v = float(val)
        return 0 if (v != v) else int(v)
    except Exception:
        return 0

# ===== publicar en lotes rápidos =====
contador = 0
inicio = time.time()

for index, fila in df.iterrows():
    texto = str(fila.get("comentario", "")).strip()
    if not texto:
        continue

    mensaje = {
        "autor":      str(fila.get("autor", "desconocido")),
        "comentario": texto,
        "likes":      safe_int(fila.get("likes", 0)),
        "fecha":      str(fila.get("fecha", "") or ""),
        "respuestas": safe_int(fila.get("respuestas", 0)),
    }

    producer.send(TOPIC, value=mensaje)
    contador += 1

    if contador % LOTE == 0:
        producer.flush()
        elapsed = time.time() - inicio
        rate = contador / elapsed if elapsed > 0 else 0
        print(f"[productor] {contador:,} / {len(df):,} comentarios  ({rate:.0f} msg/s)")
        time.sleep(DELAY_LOTE)

producer.flush()
producer.close()

elapsed = time.time() - inicio
print(f"[productor] Listo. {contador:,} comentarios en {elapsed:.1f}s ({contador/elapsed:.0f} msg/s) → topic '{TOPIC}'")
