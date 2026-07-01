#!/bin/bash
# instalar_dashboard.sh
# Instala dependencias del dashboard Streamlit en el master de EC2.
# Correr DESPUES de instalar_todo.sh (que ya instala pip3).
#
# Uso:
#   bash ~/proyecto_bigdata/dashboard/instalar_dashboard.sh

set -e

echo "=========================================="
echo "Instalando dependencias del dashboard..."
echo "=========================================="
pip3 install --user \
    "streamlit>=1.35.0" \
    "plotly>=5.20.0" \
    "kafka-python>=2.0.2"

echo ""
echo "=========================================="
echo "LISTO. Para lanzar el dashboard:"
echo "=========================================="
echo ""
echo "  # Desde el master de EC2 (localhost):"
echo "  streamlit run ~/proyecto_bigdata/dashboard/dashboard.py --server.port 8501"
echo ""
echo "  # Abrir en navegador:"
MASTER_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "<IP_PUBLICA>")
echo "  http://${MASTER_IP}:8501"
echo ""
echo "NOTA: Abrir el puerto 8501 TCP en el Security Group del master (Inbound)."
echo ""
echo "  # Si Kafka esta en otra IP:"
echo "  KAFKA_BOOTSTRAP_SERVERS=<IP>:9092 streamlit run dashboard.py"
