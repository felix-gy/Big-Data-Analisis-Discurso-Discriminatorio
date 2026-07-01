# Proyecto Final Big Data — Detección de discurso discriminatorio

## Estructura del proyecto

```
proyecto_bigdata/
├── productor/
│   ├── productor.py              → publica comentarios a Kafka (simula tiempo real)
│   └── kafka_to_s3.py            → acumula comentarios-finales y guarda en S3
├── flink_jobs/
│   ├── flink_job_1_preprocessor.py      → F1: limpieza de texto
│   ├── flink_job_2_hate_speech.py       → F2: detección de odio (pysentimiento)
│   ├── flink_job_3_sentiment.py         → F3: análisis de sentimiento (pysentimiento)
│   ├── flink_job_4_throughput.py        → F4: throughput en ventanas de 10s
│   └── flink_job_5_sentiment_window.py  → F5: sentimiento agrupado en ventanas de 60s
├── spark_jobs/
│   ├── spark_job_1_top_hate_terms.py         → S1: top palabras en discurso discriminatorio
│   ├── spark_job_2_user_risk_score.py        → S2: score de riesgo por usuario
│   ├── spark_job_3_word2vec.py               → S3: modelo Word2Vec
│   ├── spark_job_4_hourly_trend.py           → S4: tendencia de odio por hora
│   └── spark_job_5_likes_odio_correlacion.py → S5: correlación likes vs odio
├── dashboard/
│   └── dashboard.py              → dashboard Streamlit (puerto 8501)
└── scripts/
    ├── instalar_todo.sh          → instala TODO en el nodo MASTER
    ├── instalar_worker.sh        → instala Flink en cada WORKER
    ├── lanzar_flink_jobs.sh      → lanza los 5 jobs de Flink al cluster
    └── lanzar_spark_jobs.sh      → corre los 5 jobs de Spark en secuencia
```

---

## ARQUITECTURA DEL CLUSTER

| Nodo | Rol | Servicios |
|------|-----|-----------|
| kafka-flink-server (MASTER) | Coordinador | Kafka, ZooKeeper, Flink JobManager, Flink TaskManager, Dashboard Streamlit |
| kafka-flink-worker-1 (WORKER 1) | Procesamiento | Flink TaskManager |
| kafka-flink-worker-2 (WORKER 2) | Procesamiento | Flink TaskManager |

Slots de Flink: 2 por nodo × 3 nodos = **6 slots totales** (necesitas 5 para los 5 jobs).

---

## PASO 0 — Desde tu PC local: subir archivos al master

Desde la carpeta donde está `labsuser.pem`:

```bash
# Subir todo el proyecto al master
scp -i labsuser.pem -r proyecto_bigdata/ ec2-user@<IP_PUBLICA_MASTER>:~/

# Subir el dataset parquet
scp -i labsuser.pem combined.parquet ec2-user@<IP_PUBLICA_MASTER>:~/
```

Reemplaza `<IP_PUBLICA_MASTER>` con la IP pública del nodo master (ej: 54.164.200.132).

---

## PASO 1 — En el MASTER: instalar todo

Conectarse al master:
```bash
ssh -i labsuser.pem ec2-user@<IP_PUBLICA_MASTER>
```

Correr el script de instalación (tarda 20-40 min por la descarga de modelos de IA):
```bash
bash ~/proyecto_bigdata/scripts/instalar_todo.sh
```

Este script hace automáticamente:
- Instala Java 11, Kafka 3.9.1, Flink 1.18.1
- Instala Python, PyFlink, pysentimiento, torch, streamlit, boto3
- Descarga los modelos de IA (context_hate_speech + sentiment en español)
- Inicia ZooKeeper y Kafka
- Crea los 6 topics de Kafka
- Configura e inicia el cluster Flink

Al terminar verás:
```
INSTALACION DEL MASTER COMPLETA
  Kafka:            localhost:9092
  Flink dashboard:  http://<IP>:8081
```

Anota la **IP privada del master** que imprime el script (la necesitas para los workers).

---

## PASO 2 — En cada WORKER: instalar Flink

> Hacer esto en WORKER-1 y en WORKER-2 por separado.

Abrir una nueva terminal y conectarse al worker:
```bash
# Para worker 1
ssh -i labsuser.pem ec2-user@<IP_PUBLICA_WORKER1>

# Para worker 2 (en otra terminal)
ssh -i labsuser.pem ec2-user@<IP_PUBLICA_WORKER2>
```

En CADA worker, pasar la IP privada del master como argumento:
```bash
# Descargar el script de instalación del worker
curl -s https://raw.githubusercontent.com/... 
# O copiarlo desde el master:
# scp -i labsuser.pem ec2-user@<IP_MASTER>:~/proyecto_bigdata/scripts/instalar_worker.sh .

bash instalar_worker.sh <IP_PRIVADA_DEL_MASTER>
```

Ejemplo:
```bash
bash instalar_worker.sh 172.31.45.213
```

Cada worker:
- Instala Java 11 y Flink 1.18.1
- Instala Python, pysentimiento y modelos de IA (necesarios porque Flink ejecuta el código Python en cada worker)
- Configura Flink para conectarse al master
- Inicia el TaskManager (se registra automáticamente con el master)

Al terminar verás:
```
INSTALACION DEL WORKER COMPLETA
TaskManager corriendo OK
```

**Verificar en el master que ambos workers se registraron:**
```bash
curl -s http://localhost:8081/overview | python3 -c "
import json,sys; d=json.load(sys.stdin)
print('TaskManagers:', d['taskmanagers'])
print('Slots totales:', d['slots-total'])
"
```
Debes ver `TaskManagers: 3` (master + 2 workers) y `Slots totales: 6`.

También puedes verlo visualmente en: `http://<IP_PUBLICA_MASTER>:8081`

---

## PASO 3 — En el MASTER: transferir el script a los workers (alternativa)

Si no puedes copiar `instalar_worker.sh` con curl, hazlo desde el master:
```bash
# Desde el master, copiar el script a los workers
scp ~/proyecto_bigdata/scripts/instalar_worker.sh ec2-user@<IP_PRIVADA_WORKER1>:~/
scp ~/proyecto_bigdata/scripts/instalar_worker.sh ec2-user@<IP_PRIVADA_WORKER2>:~/
```

---

## PASO 4 — En el MASTER: lanzar los 5 jobs de Flink

```bash
bash ~/proyecto_bigdata/scripts/lanzar_flink_jobs.sh
```

Este script:
1. Verifica que el cluster Flink esté disponible
2. Lanza F1, F2, F3, F4, F5 en modo background (`-d`)
3. Muestra el estado final del cluster

Al terminar verás:
```
Jobs corriendo:    5
Jobs fallidos:     0
Slots en uso:      5
```

Confirmar en el dashboard Flink: `http://<IP_PUBLICA_MASTER>:8081`
Los 5 jobs deben aparecer en estado **RUNNING**.

---

## PASO 5 — En el MASTER: lanzar el dashboard

```bash
nohup ~/.local/bin/streamlit run ~/proyecto_bigdata/dashboard/dashboard.py \
    --server.port 8501 --server.headless true > ~/dashboard.log 2>&1 &
```

Verificar que está corriendo:
```bash
ps aux | grep streamlit | grep -v grep
```

Dashboard disponible en: `http://<IP_PUBLICA_MASTER>:8501`

---

## PASO 6 — En el MASTER: lanzar el productor

```bash
nohup python3 ~/proyecto_bigdata/productor/productor.py > ~/productor.log 2>&1 &
```

Esto empieza a publicar los 156,479 comentarios al topic `comentarios-raw` a razón de 2 mensajes/segundo.
El pipeline completo: `productor → comentarios-raw → F1 → F2 → F3 → comentarios-finales`.

Verificar que está llegando data:
```bash
tail -f ~/productor.log
```

---

## PASO 7 — En el MASTER: acumular datos en S3 (para Spark)

> Espera a que el pipeline haya procesado al menos 1000 mensajes (unos 10 minutos) antes de este paso.

Crear el bucket S3 primero en la consola de AWS (S3 → Create bucket → región us-east-1).

```bash
export BUCKET_S3="bigdata-final-tugrupo"
nohup python3 ~/proyecto_bigdata/productor/kafka_to_s3.py > ~/kafka_to_s3.log 2>&1 &
```

Este script:
- Lee todo el topic `comentarios-finales` (que ya tiene etiquetas de odio y sentimiento de F2/F3)
- Guarda el resultado en `s3://$BUCKET_S3/datos/comentarios_procesados.parquet`
- Se puede dejar corriendo en background; cada 5 minutos actualiza el archivo en S3

Verificar:
```bash
tail -f ~/kafka_to_s3.log
```

---

## PASO 8 — En el MASTER: correr los 5 jobs de Spark (batch)

```bash
export BUCKET_S3="bigdata-final-tugrupo"
bash ~/proyecto_bigdata/scripts/lanzar_spark_jobs.sh
```

Los jobs se ejecutan en secuencia. Cada uno lee desde S3 y guarda resultados en `s3://$BUCKET_S3/resultados/`.

---

## PUERTOS QUE DEBEN ESTAR ABIERTOS (Security Groups AWS)

En el nodo MASTER:
| Puerto | Protocolo | Para qué |
|--------|-----------|----------|
| 22 | TCP | SSH desde tu PC |
| 8081 | TCP | Flink dashboard (desde cualquier IP) |
| 8501 | TCP | Dashboard Streamlit (desde cualquier IP) |
| 9092 | TCP | Kafka (desde los workers, IP privada) |
| 6123 | TCP | Flink RPC (desde los workers, IP privada) |

En los WORKERS:
| Puerto | Protocolo | Para qué |
|--------|-----------|----------|
| 22 | TCP | SSH desde tu PC |
| 6121-6125 | TCP | Flink comunicación interna (desde el master) |

---

## REINICIO TRAS REBOOT DE EC2

Las instancias EC2 no conservan los procesos al reiniciar. Después de cada reinicio:

### En el MASTER:
```bash
# 1. Iniciar ZooKeeper
cd ~/kafka
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > ~/zookeeper.log 2>&1 &
sleep 8

# 2. Iniciar Kafka
nohup bin/kafka-server-start.sh config/server.properties > ~/kafka.log 2>&1 &
sleep 12

# 3. Iniciar Flink (JobManager + TaskManager del master)
cd ~/flink
./bin/start-cluster.sh
./bin/taskmanager.sh start
sleep 5
```

### En CADA WORKER (después de que el master esté listo):
```bash
cd ~/flink
./bin/taskmanager.sh start
```

### De vuelta en el MASTER:
```bash
# 4. Verificar 3 nodos conectados
curl -s http://localhost:8081/overview | python3 -c "import json,sys; d=json.load(sys.stdin); print('TMs:', d['taskmanagers'], '| Slots:', d['slots-total'])"

# 5. Lanzar los 5 jobs de Flink
bash ~/proyecto_bigdata/scripts/lanzar_flink_jobs.sh

# 6. Lanzar dashboard
nohup ~/.local/bin/streamlit run ~/proyecto_bigdata/dashboard/dashboard.py --server.port 8501 --server.headless true > ~/dashboard.log 2>&1 &

# 7. Lanzar productor
nohup python3 ~/proyecto_bigdata/productor/productor.py > ~/productor.log 2>&1 &
```

---

## RESUMEN VISUAL DEL PIPELINE

```
Dataset (156k comentarios)
         │
         ▼
   [PRODUCTOR]  ──────────────────────────────────────────────────────────
   productor.py                                                          │
   (0.5s delay)                                                          │
         │                                                               │
         ▼ topic: comentarios-raw                                        │
   [F1 - TextPreprocessor]                                               │
   limpia texto, quita URLs,                                             │
   convierte emojis                                                      │
         │                                                               │
         ▼ topic: comentarios-limpios                                    ▼
   [F2 - HateSpeechDetector]    [F4 - ThroughputMonitor]
   detecta odio por categoría   cuenta mensajes cada 10s
   (CALLS, RACISM, POLITICS...) → topic: metricas-throughput
         │
         ▼ topic: comentarios-clasificados
   [F3 - SentimentClassifier]
   POS / NEG / NEU
         │
         ▼ topic: comentarios-finales
   [F5 - SentimentWindow]       [kafka_to_s3.py]
   ventana 60s:                 acumula y sube a S3
   sentimiento + % odio                │
   → metricas-ventana-sentimiento      ▼
                                   [S3 - Parquet]
                                       │
                          ┌────────────┼────────────────┐
                          ▼            ▼                 ▼
                    [S1 Spark]    [S2 Spark]    ... [S5 Spark]
                    top palabras  riesgo usuario    correlación
                          │
                          ▼
                   [DASHBOARD]
                   Streamlit :8501
```

---

## NOTAS IMPORTANTES

- Los topics de Kafka retienen datos por defecto 7 días. No se pierden al reiniciar EC2.
- El umbral de detección de odio es **0.3** (en `flink_job_2_hate_speech.py`). Bajar para detectar más.
- Si un job de Flink aparece como FAILED, relanzar con `lanzar_flink_jobs.sh`.
- El dashboard Streamlit refresca el chat cada 3 segundos y los gráficos F1-F5 cada 20 segundos.
- Para verificar logs de Flink: `tail -100 ~/flink/log/flink-ec2-user-standalonesession-*.log`
