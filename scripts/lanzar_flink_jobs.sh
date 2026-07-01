#!/bin/bash
# lanzar_flink_jobs.sh
#
# Lanza los 5 jobs de Flink al cluster en modo detached (-d).
# Cada job queda corriendo en background escuchando su topic de Kafka.
#
# PREREQUISITOS:
#   - instalar_todo.sh ejecutado exitosamente
#   - Flink cluster corriendo (verificar: curl http://localhost:8081/overview)
#   - Kafka corriendo con los topics ya creados
#
# Uso:
#   bash ~/proyecto_bigdata/scripts/lanzar_flink_jobs.sh

set -e

JOBS_DIR="/home/ec2-user/proyecto_bigdata/flink_jobs"
FLINK="/home/ec2-user/flink/bin/flink"

echo "=========================================="
echo "Asegurando TaskManager en el master..."
echo "=========================================="
cd /home/ec2-user/flink
TM_RUNNING=$(ps aux | grep TaskManagerRunner | grep -v grep | wc -l)
if [ "$TM_RUNNING" -eq 0 ]; then
    ./bin/taskmanager.sh start
    sleep 5
    echo "TaskManager del master arrancado"
else
    echo "TaskManager del master ya estaba corriendo"
fi
cd -

echo "=========================================="
echo "Verificando cluster Flink..."
echo "=========================================="

OVERVIEW=$(curl -s --max-time 5 http://localhost:8081/overview 2>/dev/null) || {
    echo "ERROR: No se puede conectar al cluster Flink (http://localhost:8081)"
    echo "Asegurate de haber corrido instalar_todo.sh primero."
    exit 1
}

SLOTS=$(echo "$OVERVIEW" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['slots-available'])")
TMS=$(echo "$OVERVIEW" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['taskmanagers'])")

echo "  TaskManagers activos: $TMS"
echo "  Slots disponibles:    $SLOTS"

if [ "$SLOTS" -lt 5 ]; then
    echo "ADVERTENCIA: Solo hay $SLOTS slots disponibles. Se necesitan 5 para los 5 jobs."
    echo "Asegurate de que el master tenga taskmanager.numberOfTaskSlots=5"
    echo "Continuando de todas formas..."
fi

echo ""
echo "=========================================="
echo "Lanzando jobs de Flink..."
echo "=========================================="

# Usar IP privada del master para que los workers puedan conectarse a Kafka
MASTER_IP=$(hostname -I | awk '{print $1}')
export KAFKA_BOOTSTRAP_SERVERS="${MASTER_IP}:9092"
echo "Kafka bootstrap servers: $KAFKA_BOOTSTRAP_SERVERS"

lanzar_job() {
    local nombre=$1
    local archivo=$2
    echo ""
    echo "--- $nombre ---"
    JOB_ID=$("$FLINK" run -d -py "$JOBS_DIR/$archivo" 2>&1 | grep "Job has been submitted" | awk '{print $NF}')
    if [ -n "$JOB_ID" ]; then
        echo "  Job ID: $JOB_ID  [OK]"
    else
        echo "  ERROR al lanzar $nombre. Revisa los logs de Flink."
    fi
}

lanzar_job "F1-TextPreprocessor"    "flink_job_1_preprocessor.py"
lanzar_job "F2-HateSpeechDetector"  "flink_job_2_hate_speech.py"
lanzar_job "F3-SentimentClassifier" "flink_job_3_sentiment.py"
lanzar_job "F4-ThroughputMonitor"   "flink_job_4_throughput.py"
lanzar_job "F5-SentimentWindow"     "flink_job_5_sentiment_window.py"

echo ""
echo "=========================================="
echo "Estado final del cluster:"
echo "=========================================="
sleep 5
curl -s http://localhost:8081/overview | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'  Jobs corriendo:    {d[\"jobs-running\"]}')
print(f'  Jobs fallidos:     {d[\"jobs-failed\"]}')
print(f'  Slots en uso:      {d[\"slots-total\"] - d[\"slots-available\"]}')
print(f'  Slots libres:      {d[\"slots-available\"]}')
"

MASTER_IP_PUB=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "<ip-publica>")
echo ""
echo "Dashboard Flink: http://${MASTER_IP_PUB}:8081"
echo ""
echo "Si los 5 jobs estan en RUNNING, el sistema esta listo."
echo "Siguiente paso: correr kafka_to_s3.py y luego productor.py"
