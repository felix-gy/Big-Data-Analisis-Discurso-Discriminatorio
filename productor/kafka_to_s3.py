"""
kafka_to_s3.py — Lee comentarios-finales y guarda en parquet local + S3
"""
import json, os, subprocess, sys
import pandas as pd
from io import BytesIO
from pathlib import Path

KAFKA_SERVER = "172.31.45.213:9092"
TOPIC        = "comentarios-finales"
KAFKA_HOME   = os.path.expanduser("~/kafka")
BUCKET_S3    = os.environ.get("BUCKET_S3", "bryan-bigdata-proyecto-final")
RUTA_LOCAL   = os.path.expanduser("~/datos/comentarios_procesados.parquet")
RUTA_S3_KEY  = "datos/comentarios_procesados.parquet"

Path(os.path.dirname(RUTA_LOCAL)).mkdir(parents=True, exist_ok=True)
print(f"[kafka_to_s3] Leyendo '{TOPIC}' desde {KAFKA_SERVER} ...")

cmd = [
    f"{KAFKA_HOME}/bin/kafka-console-consumer.sh",
    "--bootstrap-server", KAFKA_SERVER,
    "--topic", TOPIC,
    "--from-beginning",
    "--timeout-ms", "8000",
    "--max-messages", "200000",
]

# Leer línea a línea con Popen para evitar deadlock de buffer
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
mensajes = []
for linea in proc.stdout:
    linea = linea.strip()
    if not linea:
        continue
    try:
        mensajes.append(json.loads(linea))
    except Exception:
        continue
    if len(mensajes) % 1000 == 0:
        print(f"[kafka_to_s3] Leidos: {len(mensajes)}")
proc.wait()

total = len(mensajes)
print(f"[kafka_to_s3] Total leido: {total} mensajes")

if total == 0:
    print("[kafka_to_s3] ADVERTENCIA: topic vacio.")
    sys.exit(1)

df = pd.DataFrame(mensajes)
for col in ["likes", "respuestas"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
if "es_odio" in df.columns:
    df["es_odio"] = df["es_odio"].astype(bool)
for col in ["prob_odio", "prob_sentimiento"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

print(f"[kafka_to_s3] Columnas: {list(df.columns)}")
print(f"[kafka_to_s3] Con odio: {df['es_odio'].sum() if 'es_odio' in df.columns else '?'} / {total}")

df.to_parquet(RUTA_LOCAL, index=False)
print(f"[kafka_to_s3] Guardado localmente: {RUTA_LOCAL}")

try:
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=BUCKET_S3, Key=RUTA_S3_KEY, Body=buf.getvalue())
    print(f"[kafka_to_s3] Subido a s3://{BUCKET_S3}/{RUTA_S3_KEY}")
except Exception as e:
    print(f"[kafka_to_s3] S3 no disponible ({e}) — solo local.")

print(f"\n[kafka_to_s3] LISTO. {total} comentarios guardados.")
