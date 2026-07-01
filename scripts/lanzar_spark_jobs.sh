#!/bin/bash
# lanzar_spark_jobs.sh
#
# Genera el parquet de datos etiquetados en S3 y corre los 5 jobs de Spark
# en el cluster standalone distribuido (master + 2 workers).
# Los jobs leen y escriben directamente en S3 para que todos los nodos
# puedan acceder a los datos sin depender del disco local del master.
#
# PREREQUISITOS:
#   - Flink corriendo con los 5 jobs en RUNNING
#   - El productor haya enviado datos (comentarios-finales con mensajes)
#   - Spark cluster corriendo (http://localhost:8080)
#   - LabInstanceProfile adjunto a los 3 nodos de EC2
#
# Uso:
#   bash ~/proyecto_bigdata/scripts/lanzar_spark_jobs.sh

set -e

SPARK_SUBMIT="$HOME/spark/bin/spark-submit"
JOBS_DIR="$HOME/proyecto_bigdata/spark_jobs"
MASTER_IP=$(hostname -I | awk '{print $1}')
SPARK_MASTER="spark://${MASTER_IP}:7077"
BUCKET="bryan-bigdata-proyecto-final"

echo "=========================================="
echo "Verificando cluster Spark: $SPARK_MASTER"
echo "=========================================="
WORKERS=$(curl -s http://localhost:8080 | grep -oP '(?<=<strong>Alive Workers:</strong> )\d+' || echo "0")
echo "  Workers activos: $WORKERS"
if [ "$WORKERS" -lt 1 ]; then
    echo "ERROR: No hay workers Spark. Inicia el cluster primero."
    exit 1
fi

echo "=========================================="
echo "Paso 1: Generar parquet de datos etiquetados en S3"
echo "=========================================="
python3 "$HOME/proyecto_bigdata/productor/kafka_to_s3.py"

# Verificar que el archivo llego a S3
FILAS=$(python3 -c "
import boto3, io, pandas as pd
s3 = boto3.client('s3', region_name='us-east-1')
obj = s3.get_object(Bucket='$BUCKET', Key='datos/comentarios_procesados.parquet')
df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
print(len(df))
" 2>/dev/null || echo 0)
echo "  Parquet en S3 listo: $FILAS filas"

if [ "$FILAS" -lt 10 ]; then
    echo "ERROR: Muy pocos datos ($FILAS filas). Verifica que Flink y el productor esten corriendo."
    exit 1
fi

echo ""
echo "=========================================="
echo "Paso 2: Correr los 5 jobs de Spark (cluster distribuido)"
echo "=========================================="

correr_job() {
    local nombre=$1
    local archivo=$2
    echo ""
    echo "--- $nombre ---"
    "$SPARK_SUBMIT" \
        --master "$SPARK_MASTER" \
        --deploy-mode client \
        --driver-memory 2g \
        --executor-memory 2g \
        --executor-cores 1 \
        --conf "spark.pyspark.python=python3" \
        --conf "spark.pyspark.driver.python=python3" \
        "$JOBS_DIR/$archivo"
    echo "  $nombre: COMPLETADO"
}

correr_job "S1 - TopHateTerms"          "spark_job_1_top_hate_terms.py"
correr_job "S2 - UserRiskScore"         "spark_job_2_user_risk_score.py"
correr_job "S3 - Word2VecModel"         "spark_job_3_word2vec.py"
correr_job "S4 - HourlyHateTrend"       "spark_job_4_hourly_trend.py"
correr_job "S5 - LikesOdioCorrelacion"  "spark_job_5_likes_odio_correlacion.py"

echo ""
echo "=========================================="
echo "TODOS LOS JOBS COMPLETADOS"
echo "=========================================="
echo ""
echo "Resultados en S3: s3://$BUCKET/resultados/"
aws s3 ls s3://$BUCKET/resultados/ 2>/dev/null || echo "  (verificar manualmente en consola S3)"
echo ""
echo "Modelo Word2Vec en S3: s3://$BUCKET/modelos/word2vec_model"
echo "Dashboard: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null):8501"
