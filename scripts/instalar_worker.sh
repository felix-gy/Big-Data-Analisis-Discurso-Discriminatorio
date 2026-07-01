#!/bin/bash
# instalar_worker.sh
#
# Instala y configura un nodo WORKER de Flink.
# Instala Java, Flink, Python, librerias y modelos de IA
# para procesamiento distribuido real.
#
# Uso:
#   bash instalar_worker.sh <IP_PRIVADA_DEL_MASTER>
#
# Ejemplo:
#   bash instalar_worker.sh 172.31.45.213
#
# La IP privada del master se ve en la consola de AWS EC2,
# en el campo "Private IPv4 addresses" de la instancia master.

set -e

# ===== verificar argumento =====
if [ -z "$1" ]; then
    echo "ERROR: Debes pasar la IP privada del master como argumento."
    echo "Uso: bash instalar_worker.sh <IP_PRIVADA_DEL_MASTER>"
    echo "Ejemplo: bash instalar_worker.sh 172.31.45.213"
    exit 1
fi

MASTER_IP="$1"
echo "Master IP: $MASTER_IP"

echo "=========================================="
echo "Paso 1: actualizar paquetes del sistema"
echo "=========================================="
sudo yum update -y

echo "=========================================="
echo "Paso 2: instalar Java 11"
echo "=========================================="
sudo yum install -y java-11-amazon-corretto
java -version

echo "=========================================="
echo "Paso 3: instalar Flink 1.18.1"
echo "=========================================="
cd /home/ec2-user
if [ -d "/home/ec2-user/flink" ]; then
    echo "Flink ya instalado, saltando descarga"
else
    wget -q https://downloads.apache.org/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz
    tar -xzf flink-1.18.1-bin-scala_2.12.tgz
    mv flink-1.18.1 flink
    rm flink-1.18.1-bin-scala_2.12.tgz
    echo "Flink instalado en /home/ec2-user/flink"
fi
ls /home/ec2-user/flink/bin/taskmanager.sh

echo "=========================================="
echo "Paso 4: configurar Flink para conectarse al master"
echo "=========================================="
cd /home/ec2-user/flink
WORKER_IP=$(hostname -I | awk '{print $1}')
echo "IP privada de este worker: $WORKER_IP"

set_flink_param() {
    local clave=$1
    local valor=$2
    local archivo="conf/flink-conf.yaml"
    if grep -q "^${clave}:" "$archivo"; then
        sed -i "s|^${clave}:.*|${clave}: ${valor}|" "$archivo"
    else
        echo "${clave}: ${valor}" >> "$archivo"
    fi
    echo "  $clave: $valor"
}

echo "Aplicando configuracion:"
set_flink_param "jobmanager.rpc.address" "$MASTER_IP"
set_flink_param "taskmanager.bind-host"  "0.0.0.0"
set_flink_param "taskmanager.host"       "$WORKER_IP"
set_flink_param "taskmanager.numberOfTaskSlots" "2"

echo "--- Configuracion aplicada ---"
grep -E "^jobmanager\.|^taskmanager\." conf/flink-conf.yaml | grep -v "^#"

echo "=========================================="
echo "Paso 5: instalar Python y librerias de IA"
echo "=========================================="
cd /home/ec2-user
sudo yum install -y python3-pip gcc gcc-c++ make

if [ ! -f /usr/bin/python ]; then
    sudo ln -s /usr/bin/python3 /usr/bin/python
fi

if python3 -c "import kafka, emoji, pandas, pyarrow" 2>/dev/null; then
    echo "Librerias base ya instaladas, saltando"
else
    pip3 install --user kafka-python emoji pandas pyarrow
fi

echo "--- instalando torch CPU-only + transformers ---"
if python3 -c "import torch, transformers" 2>/dev/null; then
    echo "torch/transformers ya instalados, saltando"
else
    pip3 install --user torch --index-url https://download.pytorch.org/whl/cpu
    pip3 install --user transformers datasets accelerate
fi

echo "--- instalando pysentimiento ---"
if python3 -c "import pysentimiento" 2>/dev/null; then
    echo "pysentimiento ya instalado, saltando"
else
    pip3 install --user --no-deps pysentimiento
fi

echo "--- pre-descargando modelos de IA (puede tardar 10-20 min) ---"
python3 -c "
from pysentimiento import create_analyzer
print('Descargando modelo context_hate_speech...')
create_analyzer(task='context_hate_speech', lang='es')
print('Descargando modelo sentiment...')
create_analyzer(task='sentiment', lang='es')
print('Modelos listos.')
"

echo "=========================================="
echo "Paso 6: iniciar Flink TaskManager"
echo "=========================================="
if ps aux | grep "TaskManagerRunner" | grep -v grep > /dev/null 2>&1; then
    echo "TaskManager ya esta corriendo. Reiniciando..."
    ./bin/taskmanager.sh stop
    sleep 3
fi
./bin/taskmanager.sh start
sleep 5

echo "--- Verificando TaskManager ---"
if ps aux | grep "TaskManagerRunner" | grep -v grep > /dev/null 2>&1; then
    echo "Flink TaskManager corriendo OK"
    ps aux | grep "TaskManagerRunner" | grep -v grep | awk '{print "  PID:"$2}'
else
    echo "ERROR: TaskManager no inicio correctamente"
    echo "Revisa el log: tail -50 /home/ec2-user/flink/log/flink-*-taskexecutor-*.log"
    exit 1
fi

echo "=========================================="
echo "Paso 7: instalar Spark 3.5.1"
echo "=========================================="
cd /home/ec2-user
if [ -d "/home/ec2-user/spark" ]; then
    echo "Spark ya instalado, saltando descarga"
else
    wget -q https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
    tar -xzf spark-3.5.1-bin-hadoop3.tgz
    mv spark-3.5.1-bin-hadoop3 spark
    rm spark-3.5.1-bin-hadoop3.tgz
    echo "Spark instalado en /home/ec2-user/spark"
fi

# Variables de entorno
grep -q "SPARK_HOME" ~/.bashrc || {
    echo 'export SPARK_HOME=$HOME/spark' >> ~/.bashrc
    echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
    echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
}
export SPARK_HOME=/home/ec2-user/spark
WORKER_IP=$(hostname -I | awk '{print $1}')

# Configurar spark-env.sh
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh 2>/dev/null || true
grep -q "SPARK_MASTER_HOST" $SPARK_HOME/conf/spark-env.sh || {
    echo "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))" >> $SPARK_HOME/conf/spark-env.sh
    echo "export PYSPARK_PYTHON=python3" >> $SPARK_HOME/conf/spark-env.sh
}

echo "=========================================="
echo "Paso 8: iniciar Spark Worker"
echo "=========================================="
if ps aux | grep "spark.deploy.worker.Worker" | grep -v grep > /dev/null 2>&1; then
    echo "Spark Worker ya esta corriendo. Reiniciando..."
    $SPARK_HOME/sbin/stop-worker.sh
    sleep 3
fi

$SPARK_HOME/sbin/start-worker.sh spark://${MASTER_IP}:7077
sleep 5

echo "--- Verificando Spark Worker ---"
if ps aux | grep "spark.deploy.worker.Worker" | grep -v grep > /dev/null 2>&1; then
    echo "Spark Worker corriendo OK"
    ps aux | grep "spark.deploy.worker.Worker" | grep -v grep | awk '{print "  PID:"$2}'
else
    echo "ERROR: Spark Worker no inicio. Revisa: tail -50 ~/spark/logs/*.out"
fi

echo "=========================================="
echo "INSTALACION DEL WORKER COMPLETA"
echo "=========================================="
echo ""
echo "Este worker debe aparecer en:"
echo "  Flink:  http://<IP_MASTER>:8081  -> Task Managers"
echo "  Spark:  http://<IP_MASTER>:8080  -> Workers"
echo ""
echo "Si no aparece en 30 segundos revisar Security Group:"
echo "  Puerto 6123 TCP para Flink"
echo "  Puerto 7077 TCP para Spark"
