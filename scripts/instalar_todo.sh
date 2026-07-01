#!/bin/bash
# instalar_todo.sh
#
# Instala y configura TODO en el nodo MASTER de tu cluster:
# Java, Kafka, Flink, Python, librerias, modelos de IA, topics de Kafka
# y prepara el entorno para lanzar los 5 jobs de Flink.
#
# Correr SOLO en el nodo master (kafka-flink-server).
# Los workers usan instalar_worker.sh
#
# Uso:
#   bash instalar_todo.sh
#
# Tiempo estimado: 20-40 minutos (principalmente descarga de modelos de IA)

set -e  # si algun comando falla, el script se detiene

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
echo "Paso 3: instalar Kafka 3.9.1"
echo "=========================================="
cd /home/ec2-user
if [ -d "/home/ec2-user/kafka" ]; then
    echo "Kafka ya instalado, saltando descarga"
else
    wget -q https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz
    tar -xzf kafka_2.13-3.9.1.tgz
    mv kafka_2.13-3.9.1 kafka
    rm kafka_2.13-3.9.1.tgz
    echo "Kafka instalado en /home/ec2-user/kafka"
fi
ls /home/ec2-user/kafka/bin/kafka-server-start.sh

echo "=========================================="
echo "Paso 4: instalar Flink 1.18.1"
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
ls /home/ec2-user/flink/bin/start-cluster.sh

echo "=========================================="
echo "Paso 4b: descargar conector Kafka-Flink"
echo "=========================================="
JAR_PATH="/home/ec2-user/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar"
if [ -f "$JAR_PATH" ]; then
    echo "Conector Kafka-Flink ya existe, saltando"
else
    wget -q -O "$JAR_PATH" \
        "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar"
    echo "Conector descargado en $JAR_PATH"
fi
ls -lh "$JAR_PATH"

echo "=========================================="
echo "Paso 5: instalar pip3 y herramientas de compilacion"
echo "=========================================="
sudo yum install -y python3-pip gcc gcc-c++ make

# symlink python -> python3 (necesario para que Flink pueda lanzar workers)
if [ ! -f /usr/bin/python ]; then
    sudo ln -s /usr/bin/python3 /usr/bin/python
    echo "Symlink /usr/bin/python -> python3 creado"
fi
python --version

echo "=========================================="
echo "Paso 5b: instalar librerias Python base"
echo "=========================================="
if python3 -c "import kafka, pyflink, emoji, pandas, pyarrow, s3fs" 2>/dev/null; then
    echo "Librerias base ya instaladas, saltando"
else
    pip3 install --user \
        kafka-python \
        "apache-flink==1.18.1" \
        emoji \
        pandas \
        pyarrow \
        s3fs
fi

echo "--- instalando torch CPU-only + transformers + datasets + accelerate ---"
if python3 -c "import torch, transformers, datasets, accelerate" 2>/dev/null; then
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

echo "--- instalando boto3 ---"
if python3 -c "import boto3" 2>/dev/null; then
    echo "boto3 ya instalado, saltando"
else
    pip3 install --user --no-deps boto3 s3transfer
fi

echo "--- instalando dashboard (streamlit + plotly) ---"
if python3 -c "import streamlit, plotly" 2>/dev/null; then
    echo "streamlit/plotly ya instalados, saltando"
else
    pip3 install --user "streamlit>=1.35.0" "plotly>=5.20.0"
fi

echo "--- verificando imports ---"
python3 -c "import kafka, pyflink, pysentimiento, emoji, pandas, pyarrow, boto3, streamlit, plotly; print('TODAS LAS LIBRERIAS OK')"

echo "=========================================="
echo "Paso 6: pre-descargar modelos de pysentimiento"
echo "=========================================="
echo "Esto puede tardar 10-20 minutos la primera vez (descarga ~1GB de modelos)"
if [ -d "/home/ec2-user/.cache/huggingface" ] && python3 -c "
from pysentimiento import create_analyzer
create_analyzer(task='context_hate_speech', lang='es')
create_analyzer(task='sentiment', lang='es')
" 2>/dev/null; then
    echo "Modelos ya descargados, saltando"
else
    python3 -c "
from pysentimiento import create_analyzer
print('Descargando modelo context_hate_speech...')
create_analyzer(task='context_hate_speech', lang='es')
print('Descargando modelo sentiment...')
create_analyzer(task='sentiment', lang='es')
print('Modelos listos y cacheados.')
"
fi

echo "=========================================="
echo "Paso 7: iniciar Zookeeper"
echo "=========================================="
cd /home/ec2-user/kafka
if ps aux | grep -i "zookeeper.server.quorum" | grep -v grep > /dev/null 2>&1; then
    echo "Zookeeper ya esta corriendo"
else
    nohup bin/zookeeper-server-start.sh config/zookeeper.properties \
        > /home/ec2-user/zookeeper.log 2>&1 &
    sleep 6
    echo "Zookeeper iniciado. Log: /home/ec2-user/zookeeper.log"
fi
ps aux | grep -i "zookeeper.server.quorum" | grep -v grep | awk '{print "  PID:"$2, $11}'

echo "=========================================="
echo "Paso 8: iniciar Kafka"
echo "=========================================="
# Asegurar que los logs de Kafka van a disco EBS (no a /tmp que se borra al apagar)
mkdir -p /home/ec2-user/kafka-logs
sed -i 's|log.dirs=/tmp/kafka-logs|log.dirs=/home/ec2-user/kafka-logs|g' config/server.properties

# advertised.listeners con IP privada para que workers de Flink puedan conectarse
if ! grep -q '^advertised.listeners' config/server.properties; then
    echo "advertised.listeners=PLAINTEXT://${MASTER_IP}:9092" >> config/server.properties
fi

if ps aux | grep -i "kafka.Kafka" | grep -v grep > /dev/null 2>&1; then
    echo "Kafka ya esta corriendo"
else
    nohup bin/kafka-server-start.sh config/server.properties \
        > /home/ec2-user/kafka.log 2>&1 &
    sleep 10
    echo "Kafka iniciado. Log: /home/ec2-user/kafka.log"
fi
ps aux | grep -i "kafka.Kafka" | grep -v grep | awk '{print "  PID:"$2, $11}'

echo "=========================================="
echo "Paso 9: crear topics de Kafka"
echo "=========================================="
crear_topic() {
    local topic=$1
    local particiones=$2
    if bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null | grep -qx "$topic"; then
        echo "  Topic '$topic' ya existe"
    else
        bin/kafka-topics.sh --create --topic "$topic" \
            --bootstrap-server localhost:9092 \
            --partitions "$particiones" \
            --replication-factor 1
        echo "  Topic '$topic' creado con $particiones particion(es)"
    fi
}
crear_topic comentarios-raw 3
crear_topic comentarios-limpios 3
crear_topic comentarios-clasificados 3
crear_topic comentarios-finales 3
crear_topic metricas-throughput 1
crear_topic metricas-ventana-sentimiento 1
echo "--- Topics disponibles ---"
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

echo "=========================================="
echo "Paso 10: configurar Flink"
echo "=========================================="
cd /home/ec2-user/flink
MASTER_IP=$(hostname -I | awk '{print $1}')
echo "IP privada del master detectada: $MASTER_IP"

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
# rest.address: localhost -> el cliente flink run se conecta via localhost
# (si se pone 0.0.0.0, flink run -py falla porque 0.0.0.0 no es valido como destino)
set_flink_param "rest.address"            "localhost"
# rest.bind-address: 0.0.0.0 -> el servidor escucha en todas las interfaces
# (necesario para acceder al dashboard desde el navegador de tu PC)
set_flink_param "rest.bind-address"       "0.0.0.0"
set_flink_param "jobmanager.rpc.address"  "$MASTER_IP"
set_flink_param "jobmanager.bind-host"    "0.0.0.0"
set_flink_param "taskmanager.bind-host"   "0.0.0.0"
set_flink_param "taskmanager.host"        "$MASTER_IP"
# 5 slots en el master para poder correr los 5 jobs de Flink simultaneamente
set_flink_param "taskmanager.numberOfTaskSlots" "2"

echo "--- Configuracion aplicada ---"
grep -E "^rest\.|^jobmanager\.|^taskmanager\." conf/flink-conf.yaml | grep -v "^#"

echo "=========================================="
echo "Paso 10b: iniciar cluster Flink"
echo "=========================================="
# Vaciar workers para que start-cluster.sh no arranque TaskManagers en el master
# (los workers arrancan su propio TaskManager con instalar_worker.sh)
echo "" > conf/workers

if ps aux | grep "StandaloneSessionClusterEntrypoint" | grep -v grep > /dev/null 2>&1; then
    echo "Flink ya esta corriendo. Reiniciando para aplicar configuracion..."
    ./bin/stop-cluster.sh
    sleep 3
fi
./bin/start-cluster.sh
sleep 8

echo "--- Verificando Flink ---"
curl -s http://localhost:8081/overview | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'  TaskManagers: {d[\"taskmanagers\"]}')
print(f'  Slots totales: {d[\"slots-total\"]}')
print(f'  Slots libres: {d[\"slots-available\"]}')
print(f'  Jobs corriendo: {d[\"jobs-running\"]}')
"

echo "=========================================="
echo "Paso 11: instalar Spark 3.5.1"
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
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3

# Configurar spark-env.sh
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh 2>/dev/null || true
grep -q "SPARK_MASTER_HOST" $SPARK_HOME/conf/spark-env.sh || {
    echo "export SPARK_MASTER_HOST=$MASTER_IP" >> $SPARK_HOME/conf/spark-env.sh
    echo "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))" >> $SPARK_HOME/conf/spark-env.sh
    echo "export PYSPARK_PYTHON=python3" >> $SPARK_HOME/conf/spark-env.sh
}

# Worker en el mismo master
echo "$MASTER_IP" > $SPARK_HOME/conf/workers

# spark-defaults.conf
cat > $SPARK_HOME/conf/spark-defaults.conf << EOF
spark.master                     spark://${MASTER_IP}:7077
spark.ui.port                    4040
spark.driver.memory              2g
spark.executor.memory            2g
spark.pyspark.python             python3
EOF

echo "Configuracion de Spark aplicada"

echo "=========================================="
echo "Paso 11b: iniciar Spark standalone"
echo "=========================================="
if ps aux | grep "spark.deploy.master.Master" | grep -v grep > /dev/null 2>&1; then
    echo "Spark Master ya esta corriendo"
else
    $SPARK_HOME/sbin/start-master.sh
    sleep 4
    echo "Spark Master iniciado"
fi

if ps aux | grep "spark.deploy.worker.Worker" | grep -v grep > /dev/null 2>&1; then
    echo "Spark Worker ya esta corriendo"
else
    $SPARK_HOME/sbin/start-worker.sh spark://${MASTER_IP}:7077
    sleep 3
    echo "Spark Worker iniciado"
fi

echo "--- Verificando Spark ---"
ps aux | grep "spark.deploy.master.Master" | grep -v grep | awk '{print "  Spark Master PID:"$2}'
ps aux | grep "spark.deploy.worker.Worker" | grep -v grep | awk '{print "  Spark Worker PID:"$2}'

echo "=========================================="
echo "INSTALACION DEL MASTER COMPLETA"
echo "=========================================="
MASTER_IP_PUB=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "<ip-publica>")
echo ""
echo "  Kafka:            localhost:9092"
echo "  Flink UI:         http://${MASTER_IP_PUB}:8081"
echo "  Spark UI:         http://${MASTER_IP_PUB}:8080"
echo "  Dashboard datos:  http://${MASTER_IP_PUB}:8501"
echo "  IP privada:       $MASTER_IP"
echo ""
echo "IMPORTANTE: Abrir estos puertos en el Security Group del master:"
echo "  - 8080 TCP  (Spark UI)"
echo "  - 8081 TCP  (Flink UI)"
echo "  - 8501 TCP  (Dashboard Streamlit)"
echo ""
echo "Proximos pasos:"
echo "  1. Correr instalar_worker.sh en CADA worker pasando la IP privada:"
echo "     bash instalar_worker.sh $MASTER_IP"
echo "  2. Lanzar los 5 jobs de Flink:"
echo "     bash ~/proyecto_bigdata/scripts/lanzar_flink_jobs.sh"
echo "  3. Lanzar el dashboard:"
echo "     nohup ~/.local/bin/streamlit run ~/proyecto_bigdata/dashboard/dashboard.py --server.port 8501 --server.headless true > ~/dashboard.log 2>&1 &"
echo "  4. Lanzar el productor:"
echo "     nohup python3 ~/proyecto_bigdata/productor/productor.py > ~/productor.log 2>&1 &"
echo "  5. Cuando haya data acumulada, correr los jobs de Spark:"
echo "     export BUCKET_S3=tu-bucket && bash ~/proyecto_bigdata/scripts/lanzar_spark_jobs.sh"
