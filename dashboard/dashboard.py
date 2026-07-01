"""
dashboard.py — Dashboard Discurso Discriminatorio PE
KAFKA_BOOTSTRAP_SERVERS=<IP>:9092 streamlit run dashboard.py
"""

import os, json, time, re, socket, random, html
from datetime import datetime
from collections import Counter, deque

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Config ───────────────────────────────────────────────────
KAFKA_SERVERS  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FLINK_URL      = os.environ.get("FLINK_REST_URL", "http://localhost:8081")
REFRESH_S      = 3
CHART_CACHE_S  = 20
MAX_CHAT_MSGS  = 60
MAX_KAFKA_MSGS = 300

TOPIC_FINALES    = "comentarios-finales"
TOPIC_THROUGHPUT = "metricas-throughput"
TOPIC_VENTANA    = "metricas-ventana-sentimiento"

CATS_ODIO = {
    "CALLS":"Incitación","CRIMINAL":"Criminalización","RACISM":"Racismo",
    "CLASS":"Clasismo","APPEARANCE":"Apariencia","POLITICS":"Política",
    "WOMEN":"Misoginia","LGBTI":"LGBTI+","ninguna":"Sin odio",
}
STOPWORDS = {
    "de","la","el","en","y","a","los","del","se","las","por","un","para","con",
    "una","su","al","lo","como","más","pero","sus","le","ya","o","este","porque",
    "esta","entre","cuando","muy","sin","sobre","también","me","hasta","hay",
    "han","desde","todo","nos","todos","uno","les","ni","otros","ese","eso",
    "ante","ellos","e","esto","antes","yo","otro","otras","otra","mucho","nada",
    "cual","poco","ella","estas","si","te","que","es","son","no","ha","he",
    "mi","tu","ser","bien","puede","vez","hoy","va","q","al","el","en",
}

# ── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard — Discurso Discriminatorio PE",
    page_icon="🔍", layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown("""
<style>
  .bloque{background:linear-gradient(90deg,#1a237e,#283593);color:#fff;
    padding:8px 16px;border-radius:6px;font-size:15px;font-weight:bold;margin:14px 0 6px 0;}
  .vivo{color:#27ae60;font-size:12px;font-weight:bold;}
  .muest{color:#95a5a6;font-size:12px;}
  div[data-testid="metric-container"]{background:#f8f9fa;border-radius:8px;
    padding:10px;border-left:4px solid #1a237e;}
  header[data-testid="stHeader"]{background:transparent;}
</style>
""", unsafe_allow_html=True)

# ── Session state ────────────────────────────────────────────
def _init():
    if "chat_msgs"    not in st.session_state: st.session_state.chat_msgs    = []
    if "cola_msgs"    not in st.session_state: st.session_state.cola_msgs    = deque()
    if "kafka_ok"     not in st.session_state: st.session_state.kafka_ok     = None
    if "kafka_ok_ts"  not in st.session_state: st.session_state.kafka_ok_ts  = 0
    if "kafka_offset" not in st.session_state: st.session_state.kafka_offset = {}
    if "_kafka_ok"    not in st.session_state: st.session_state._kafka_ok    = False
    if "_flink_info"  not in st.session_state: st.session_state._flink_info  = None
_init()

# ── Chequeo TCP Kafka ─────────────────────────────────────────
def kafka_disponible() -> bool:
    if time.time() - st.session_state.kafka_ok_ts < 15:
        return st.session_state.kafka_ok
    try:
        h, p = KAFKA_SERVERS.rsplit(":", 1)
        s = socket.create_connection((h, int(p)), timeout=0.5)
        s.close()
        st.session_state.kafka_ok = True
    except Exception:
        st.session_state.kafka_ok = False
    st.session_state.kafka_ok_ts = time.time()
    return st.session_state.kafka_ok

# ── Leer NUEVOS mensajes de Kafka ────────────────────────────
def leer_nuevos_kafka(topic: str, max_new: int = 10) -> list:
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_SERVERS,
            enable_auto_commit=False,
            consumer_timeout_ms=700,
            value_deserializer=lambda v: json.loads(v.decode("utf-8", errors="replace")),
        )
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        end = consumer.end_offsets([tp])[tp]
        last = st.session_state.kafka_offset.get(topic, max(0, end - 20))
        consumer.seek(tp, last)
        msgs = []
        for msg in consumer:
            msgs.append(msg.value)
            last = msg.offset + 1
            if len(msgs) >= max_new:
                break
        consumer.close()
        st.session_state.kafka_offset[topic] = last
        return msgs
    except Exception:
        return []

# ── Leer los ÚLTIMOS N mensajes de un topic ──────────────────
@st.cache_data(ttl=CHART_CACHE_S)
def leer_completo(topic: str, max_msgs: int = MAX_KAFKA_MSGS) -> list:
    if not kafka_disponible():
        return []
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_SERVERS,
            enable_auto_commit=False,
            consumer_timeout_ms=2000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8", errors="replace")),
        )
        parts = consumer.partitions_for_topic(topic) or {0}
        tps   = [TopicPartition(topic, p) for p in sorted(parts)]
        consumer.assign(tps)
        ends  = consumer.end_offsets(tps)
        per_p = max(1, max_msgs // len(tps))
        for tp in tps:
            consumer.seek(tp, max(0, ends[tp] - per_p))
        msgs = []
        for msg in consumer:
            msgs.append(msg.value)
            if len(msgs) >= max_msgs:
                break
        consumer.close()
        return msgs
    except Exception:
        return []

@st.cache_data(ttl=CHART_CACHE_S)
def flink_status():
    try:
        import urllib.request
        with urllib.request.urlopen(f"{FLINK_URL}/overview", timeout=2) as r:
            return json.loads(r.read())
    except Exception:
        return None

# ── Datos de muestra ─────────────────────────────────────────
TEXTOS_RAW = [
    ("ese candidato es igual que todos los terroristas de su partido",   "NEG", True,  "POLITICS"),
    ("los serranos no entienden cómo funciona la economía",              "NEG", True,  "RACISM"),
    ("vota por la democracia o por el comunismo, no hay más",            "NEG", True,  "POLITICS"),
    ("los de lima son unos caviares que no conocen el perú real",        "NEG", True,  "CLASS"),
    ("es hora de que este país deje de ser gobernado por terrucos",      "NEG", True,  "CALLS"),
    ("ese político es un comunista disfrazado de demócrata",             "NEG", True,  "POLITICS"),
    ("los cholos nunca van a poder manejar este país de verdad",         "NEG", True,  "RACISM"),
    ("fuera los golpistas del poder el pueblo no olvida",                "NEG", True,  "CRIMINAL"),
    ("pituco miserable, nunca vivió en pobreza y quiere gobernar",       "NEG", True,  "CLASS"),
    ("que vergüenza el nivel del debate político en este país",          "NEG", False, "ninguna"),
    ("el congreso debe trabajar por el bien común, no por intereses",    "NEU", False, "ninguna"),
    ("paren de terruquear entre hermanos peruanos, ya basta",            "NEU", False, "ninguna"),
    ("hay que respetar los derechos de todos los peruanos",              "POS", False, "ninguna"),
    ("espero que las elecciones sean transparentes y justas",            "POS", False, "ninguna"),
    ("la democracia necesita ciudadanos informados y participativos",    "POS", False, "ninguna"),
    ("qué buenos resultados, apoyamos al equipo peruano",                "POS", False, "ninguna"),
    ("buen discurso aunque no comparto todo lo que dijo el candidato",   "NEU", False, "ninguna"),
    ("apoyamos a todos los que proponen políticas honestas para el país","POS", False, "ninguna"),
]
AUTORES = [f"user_{random.randint(100,999)}" for _ in range(50)]
random.shuffle(AUTORES)

def gen_msg() -> dict:
    txt, sent, odio, cat = random.choice(TEXTOS_RAW)
    return {
        "autor":            random.choice(AUTORES),
        "comentario":       txt,
        "comentario_limpio": txt,
        "likes":            random.randint(0, 800),
        "es_odio":          odio,
        "prob_odio":        round(random.uniform(0.62, 0.97) if odio else random.uniform(0.03, 0.28), 3),
        "categoria_odio":   cat,
        "sentimiento":      sent,
        "prob_sentimiento": round(random.uniform(0.65, 0.99), 3),
        "_ts":              time.time(),
    }

def gen_throughput(n=24) -> list:
    now = int(time.time())
    return [{"ventana_fin": now-(n-i)*10, "cantidad_mensajes": random.randint(3,28),
             "throughput_msg_seg": round(random.uniform(0.3, 2.8), 2)} for i in range(n)]

def gen_ventana(n=12) -> list:
    now = int(time.time())
    rows = []
    for i in range(n):
        total = random.randint(30,90)
        pos   = random.randint(5, max(6, total-15))
        neg   = random.randint(3, max(4, total-pos-5))
        neu   = max(0, total-pos-neg)
        odio  = random.randint(2, max(3, int(total*0.45)))
        rows.append({"ventana_fin": now-(n-i)*60, "total_comentarios": total,
                     "positivos": pos, "negativos": neg, "neutrales": neu,
                     "odio": odio, "porcentaje_odio": round(odio/total*100, 1)})
    return rows

# ── Helpers ──────────────────────────────────────────────────
def ts_hms(ts):
    try:    return datetime.fromtimestamp(float(ts)).strftime("%H:%M:%S")
    except: return datetime.now().strftime("%H:%M:%S")

def top_palabras(textos, n=15):
    words = []
    for t in textos:
        for w in re.sub(r"[^a-záéíóúñü\s]", " ", str(t).lower()).split():
            if len(w) > 3 and w not in STOPWORDS:
                words.append(w)
    return pd.DataFrame(Counter(words).most_common(n), columns=["palabra","frecuencia"])

def user_color(nombre: str) -> str:
    paleta = ["#ff6b6b","#ffd93d","#6bcb77","#4d96ff","#c77dff",
              "#ff9f1c","#06d6a0","#ef476f","#118ab2","#ffc8dd"]
    return paleta[hash(nombre) % len(paleta)]

def seccion(titulo, src=""):
    b = ('<span class="vivo">● EN VIVO</span>' if src == "kafka"
         else '<span class="muest">○ muestra</span>')
    st.markdown(f'<div class="bloque">{titulo} &nbsp;{b}</div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
#  SIDEBAR
# ════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("Panel de control")
    # Estado real comprobado aquí directamente
    kafka_ok_now  = kafka_disponible()
    flink_now     = flink_status()
    if kafka_ok_now:
        st.success(f"**Kafka: CONECTADO**\n`{KAFKA_SERVERS}`")
    else:
        st.warning("**Kafka:** sin conexión\nMostrando datos de muestra.")
    if flink_now:
        st.success("**Flink: ACTIVO**")
        c1,c2 = st.columns(2)
        c1.metric("Jobs", flink_now.get("jobs-running","?"))
        c2.metric("Slots", flink_now.get("slots-available","?"))
    else:
        st.warning("**Flink:** no accesible")
    st.divider()
    if st.button("🔄 Reiniciar chat", use_container_width=True):
        st.session_state.chat_msgs = []
        st.session_state.cola_msgs = deque()
        st.session_state.kafka_offset = {}
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption(
        "**Pipeline:**\nKafka → Flink (F1–F5)\n\n"
        "**NLP:** pysentimiento\ncontext_hate_speech + sentiment (ES)\n\n"
        "**Datos:** Comentarios YouTube — elecciones peruanas"
    )

# ════════════════════════════════════════════════════════════
#  TÍTULO
# ════════════════════════════════════════════════════════════
SPARK_BUCKET = "bryan-bigdata-proyecto-final"

@st.cache_data(ttl=120)
def leer_spark_s3(key: str) -> pd.DataFrame:
    try:
        import boto3, io
        s3 = boto3.client("s3", region_name="us-east-1")
        paginator = s3.get_paginator("list_objects_v2")
        parts = []
        for page in paginator.paginate(Bucket=SPARK_BUCKET, Prefix=key):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    buf = io.BytesIO(s3.get_object(Bucket=SPARK_BUCKET, Key=obj["Key"])["Body"].read())
                    parts.append(pd.read_parquet(buf))
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()

st.title("🔍 Detección de Discurso Discriminatorio en Tiempo Real")

# ════════════════════════════════════════════════════════════
#  SECCIÓN EN VIVO — fragment: solo esta parte se refresca
#  cada 3 segundos, sin parpadear el resto de la página
# ════════════════════════════════════════════════════════════
@st.fragment(run_every=REFRESH_S)
def seccion_live():
    kafka_ok = kafka_disponible()
    st.session_state._kafka_ok   = kafka_ok
    st.session_state._flink_info = flink_status()
    flink_info = st.session_state._flink_info

    # ── Encolar mensajes nuevos ──────────────────────────────
    if kafka_ok:
        batch = leer_nuevos_kafka(TOPIC_FINALES, max_new=10)
        st.session_state.cola_msgs.extend(batch)
        fuente_live = "kafka"
    else:
        if len(st.session_state.cola_msgs) < 2:
            st.session_state.cola_msgs.append(gen_msg())
        fuente_live = "muestra"

    if st.session_state.cola_msgs:
        nuevo = st.session_state.cola_msgs.popleft()
        nuevo.setdefault("_ts", time.time())
        st.session_state.chat_msgs.append(nuevo)
        if len(st.session_state.chat_msgs) > MAX_CHAT_MSGS:
            st.session_state.chat_msgs = st.session_state.chat_msgs[-MAX_CHAT_MSGS:]

    # ── KPIs ─────────────────────────────────────────────────
    msgs_tp = leer_completo(TOPIC_THROUGHPUT, 60) or gen_throughput(24)
    msgs_v5 = leer_completo(TOPIC_VENTANA, 30)    or gen_ventana(12)
    df_tp   = pd.DataFrame(msgs_tp)
    df_v5   = pd.DataFrame(msgs_v5)

    total_chat  = len(st.session_state.chat_msgs)
    src_txt     = "🟢 Kafka en vivo" if kafka_ok else "🟡 Datos de muestra"
    n_cola      = len(st.session_state.cola_msgs)
    st.caption(f"{src_txt} &nbsp;·&nbsp; {total_chat} comentarios &nbsp;·&nbsp; actualización cada {REFRESH_S} s"
               + (f" &nbsp;·&nbsp; cola: {n_cola}" if n_cola else ""))

    last_tp     = df_tp.iloc[-1] if not df_tp.empty else {}
    last_v5     = df_v5.iloc[-1] if not df_v5.empty else {}
    throughput  = float(last_tp.get("throughput_msg_seg", 0)) if isinstance(last_tp, pd.Series) else 0
    pct_odio_v5 = float(last_v5.get("porcentaje_odio", 0))    if isinstance(last_v5, pd.Series) else 0
    if isinstance(last_v5, pd.Series):
        dom = max([("Positivo 😊",int(last_v5.get("positivos",0))),
                   ("Negativo 😠",int(last_v5.get("negativos",0))),
                   ("Neutral 😐", int(last_v5.get("neutrales",0)))], key=lambda x:x[1])[0]
    else:
        dom = "—"

    n_odio_chat = sum(1 for m in st.session_state.chat_msgs if m.get("es_odio"))
    n_pos_chat  = sum(1 for m in st.session_state.chat_msgs if m.get("sentimiento") == "POS")
    n_neg_chat  = sum(1 for m in st.session_state.chat_msgs if m.get("sentimiento") == "NEG")
    n_neu_chat  = total_chat - n_pos_chat - n_neg_chat
    pct_o   = round(n_odio_chat / total_chat * 100, 1) if total_chat > 0 else 0.0
    pct_p   = round(n_pos_chat  / total_chat * 100, 1) if total_chat > 0 else 0.0
    pct_n   = round(n_neg_chat  / total_chat * 100, 1) if total_chat > 0 else 0.0
    pct_neu = round(n_neu_chat  / total_chat * 100, 1) if total_chat > 0 else 0.0

    k1,k2,k3,k4,k5,k6 = st.columns(6)
    k1.metric("⚡ Throughput",          f"{throughput:.2f} msg/s")
    k2.metric("💬 Mensajes recibidos",  total_chat)
    k3.metric("🚨 Disc. odio (chat)",   f"{pct_o:.1f}%")
    k4.metric("🚨 Disc. odio (60 s)",   f"{pct_odio_v5:.1f}%")
    k5.metric("📊 Sentimiento dom.",    dom)
    k6.metric("🖥️ Flink jobs",
              f"{flink_info.get('jobs-running','?')} activos" if flink_info else "—")

    st.divider()

    # ── Chat en vivo ──────────────────────────────────────────
    st.markdown(
        "### 💬 Chat en vivo &nbsp;"
        '<span style="color:#e74c3c;font-weight:bold;font-size:13px;">● EN VIVO</span>',
        unsafe_allow_html=True,
    )

    msgs_display = list(reversed(st.session_state.chat_msgs[-30:]))
    filas = []
    for m in msgs_display:
        autor  = html.escape(str(m.get("autor", "usuario")))
        texto  = html.escape(str(m.get("comentario", m.get("comentario_limpio", "")))[:130])
        hora   = ts_hms(m.get("_ts", time.time()))
        color  = user_color(autor)
        filas.append(
            f'<div style="padding:5px 0;border-bottom:1px solid #1a1a3a;line-height:1.5;">'
            f'<span style="color:{color};font-weight:700;font-size:12.5px;">{autor}</span>'
            f'<span style="color:#444;font-size:10px;margin-left:8px;">{hora}</span><br>'
            f'<span style="color:#d4d4d4;font-size:13px;">{texto}</span></div>'
        )
    inner = "\n".join(filas) if filas else '<span style="color:#555;font-size:13px;">Esperando comentarios...</span>'
    st.markdown(
        '<div style="background:#0d0d1a;border:1px solid #2d2d5e;border-radius:10px;'
        'height:340px;overflow-y:auto;padding:12px 16px;'
        'font-family:Segoe UI,Arial,sans-serif;">'
        + inner + "</div>",
        unsafe_allow_html=True,
    )

    st.divider()

    # ── Estadísticas + últimos procesados ────────────────────
    col_stats, col_bubbles = st.columns([1, 1])

    with col_stats:
        st.caption("**📊 Estadísticas del chat:**")
        s1, s2 = st.columns(2)
        s1.metric("😐 Neutral", f"{pct_neu}%")
        s2.metric("🚨 Disc. odio", f"{pct_o}%")
        s3, s4 = st.columns(2)
        s3.metric("😊 Positivos", f"{pct_p}%")
        s4.metric("😠 Negativos", f"{pct_n}%")
        df_donut = pd.DataFrame({
            "tipo": ["Positivo 😊", "Negativo 😠", "Neutral 😐"],
            "n":    [max(n_pos_chat, 0), max(n_neg_chat, 0), max(n_neu_chat, 1)],
        })
        fig_d = px.pie(df_donut, names="tipo", values="n", hole=0.55, height=200,
                       color="tipo",
                       color_discrete_map={"Positivo 😊":"#2ecc71","Negativo 😠":"#e74c3c","Neutral 😐":"#95a5a6"})
        fig_d.update_traces(textinfo="percent", textfont_size=10)
        fig_d.update_layout(margin=dict(t=0,b=0,l=0,r=0),
                            showlegend=True, legend=dict(font_size=9, y=0.5))
        st.plotly_chart(fig_d, use_container_width=True, key="donut_stats")

    with col_bubbles:
        st.caption("**🔬 Últimos procesados por el pipeline:**")
        filas_proc = []
        for m in reversed(st.session_state.chat_msgs[-6:]):
            sent    = m.get("sentimiento", "NEU")
            es_odio = m.get("es_odio", False)
            autor   = html.escape(str(m.get("autor", "?")))
            texto   = html.escape(str(m.get("comentario_limpio") or m.get("comentario", ""))[:120])
            prob    = m.get("prob_odio", 0)
            cat     = CATS_ODIO.get(m.get("categoria_odio","ninguna"), "Sin odio")
            hora    = ts_hms(m.get("_ts", time.time()))
            emoji_s = {"POS":"😊","NEG":"😠","NEU":"😐"}.get(sent,"💬")
            if es_odio:
                bg, border = "#3d0000", "#c0392b"
                badge = (f'<span style="background:#c0392b;color:#fff;padding:1px 6px;'
                         f'border-radius:4px;font-size:10px;margin-left:6px;">🚨 {html.escape(cat)} {prob:.2f}</span>')
            elif sent == "NEG":
                bg, border, badge = "#2d1a00", "#e67e22", ""
            elif sent == "POS":
                bg, border, badge = "#003d00", "#27ae60", ""
            else:
                bg, border, badge = "#1a1a2e", "#4a4a8a", ""
            filas_proc.append(
                f'<div style="background:{bg};border-left:3px solid {border};border-radius:6px;'
                f'padding:7px 10px;margin-bottom:6px;">'
                f'<div style="font-size:11px;color:#aaa;">{emoji_s} '
                f'<b style="color:#ddd;">{autor}</b> · {hora}{badge}</div>'
                f'<div style="font-size:12.5px;color:#e0e0e0;margin-top:3px;">{texto}</div>'
                f'</div>'
            )
        inner_proc = "\n".join(filas_proc) if filas_proc else '<span style="color:#555;font-size:13px;">Esperando mensajes...</span>'
        st.markdown(
            '<div style="background:#0d0d1a;border:1px solid #2d2d5e;border-radius:10px;'
            'height:340px;overflow-y:auto;padding:10px 12px;">'
            + inner_proc + "</div>",
            unsafe_allow_html=True,
        )

# ════════════════════════════════════════════════════════════
#  SECCIONES F1–F5 — fragment independiente, refresca cada 20s
# ════════════════════════════════════════════════════════════
@st.fragment(run_every=20)
def seccion_charts():
    msgs_fin = leer_completo(TOPIC_FINALES)         or [gen_msg() for _ in range(120)]
    msgs_tp  = leer_completo(TOPIC_THROUGHPUT, 60)  or gen_throughput(24)
    msgs_v5  = leer_completo(TOPIC_VENTANA, 30)     or gen_ventana(12)

    df_fin = pd.DataFrame(msgs_fin)
    df_tp  = pd.DataFrame(msgs_tp)
    df_v5  = pd.DataFrame(msgs_v5)

    # ── F1 ──────────────────────────────────────────────────
    seccion("F1 · TextPreprocessor — Limpieza y normalización de texto")
    if not df_fin.empty and "comentario_limpio" in df_fin.columns:
        f1a, f1b = st.columns([1, 2])
        with f1a:
            total_proc = len(df_fin)
            con_limpio = df_fin["comentario_limpio"].notna().sum()
            st.metric("📥 Comentarios recibidos", total_proc)
            st.metric("✅ Comentarios limpios",   con_limpio)
            st.metric("⚙️ Tasa procesamiento",    f"{con_limpio/total_proc*100:.1f}%" if total_proc else "0%")
            st.caption("F1 limpia el texto: minúsculas, quita URLs, convierte emojis, elimina signos repetidos.")
        with f1b:
            muestra = df_fin[["comentario","comentario_limpio"]].dropna().head(6).copy()
            muestra.columns = ["Texto original", "Texto limpio (F1)"]
            st.dataframe(muestra, use_container_width=True, hide_index=True)

    st.divider()

    # ── F2 ──────────────────────────────────────────────────
    seccion("F2 · HateSpeechDetector — Distribución de discurso discriminatorio")
    if not df_fin.empty and "es_odio" in df_fin.columns:
        f2a, f2b = st.columns(2)
        odio_df = df_fin["es_odio"].value_counts().reset_index()
        odio_df.columns = ["valor","cantidad"]
        odio_df["etiqueta"] = odio_df["valor"].apply(lambda x: "Discriminatorio 🚨" if x else "Sin discriminación ✅")
        with f2a:
            fig = px.pie(odio_df, names="etiqueta", values="cantidad",
                         title=f"Discriminatorio vs Sin discriminación — {len(df_fin)} comentarios",
                         color="etiqueta",
                         color_discrete_map={"Discriminatorio 🚨":"#c0392b","Sin discriminación ✅":"#27ae60"},
                         hole=0.42)
            fig.update_traces(textinfo="percent+value", textfont_size=13)
            st.plotly_chart(fig, use_container_width=True)
        df_odio = df_fin[df_fin["es_odio"] == True]
        if not df_odio.empty and "categoria_odio" in df_odio.columns:
            cc = df_odio["categoria_odio"].value_counts().reset_index()
            cc.columns = ["cat_raw","cantidad"]
            cc["categoria"] = cc["cat_raw"].map(lambda x: CATS_ODIO.get(x,x))
            with f2b:
                fig2 = px.bar(cc.sort_values("cantidad"), x="cantidad", y="categoria",
                              orientation="h", title="Tipos de discurso discriminatorio detectados",
                              color="cantidad", color_continuous_scale="Reds", text="cantidad")
                fig2.update_traces(textposition="outside")
                fig2.update_layout(showlegend=False, coloraxis_showscale=False)
                st.plotly_chart(fig2, use_container_width=True)
        if "prob_odio" in df_fin.columns:
            st.subheader("Top 10 — mayor probabilidad de discurso discriminatorio")
            t10 = (df_fin[df_fin["es_odio"]==True].sort_values("prob_odio",ascending=False).head(10)
                   [["autor","comentario_limpio","categoria_odio","prob_odio"]]
                   .rename(columns={"autor":"Autor","comentario_limpio":"Comentario",
                                     "categoria_odio":"Tipo","prob_odio":"Prob."}))
            t10["Tipo"] = t10["Tipo"].map(lambda x: CATS_ODIO.get(x,x))
            st.dataframe(t10, use_container_width=True, hide_index=True)

    st.divider()

    # ── F3 ──────────────────────────────────────────────────
    seccion("F3 · SentimentClassifier — Análisis de sentimiento NLP")
    if not df_fin.empty and "sentimiento" in df_fin.columns:
        f3a, f3b = st.columns(2)
        with f3a:
            sc = df_fin["sentimiento"].value_counts().reset_index()
            sc.columns = ["sentimiento","cantidad"]
            sc["label"] = sc["sentimiento"].map({"POS":"Positivo 😊","NEG":"Negativo 😠","NEU":"Neutral 😐"})
            fig3 = px.pie(sc, names="label", values="cantidad", title="Distribución de sentimiento",
                          color="sentimiento",
                          color_discrete_map={"POS":"#2ecc71","NEG":"#e74c3c","NEU":"#95a5a6"}, hole=0.4)
            fig3.update_traces(textinfo="percent+value", textfont_size=13)
            st.plotly_chart(fig3, use_container_width=True)
        with f3b:
            txts = df_fin[df_fin["sentimiento"]=="NEG"]["comentario_limpio"].dropna().tolist()
            df_pal = top_palabras(txts, 15)
            if not df_pal.empty:
                fig4 = px.bar(df_pal.sort_values("frecuencia"), x="frecuencia", y="palabra",
                              orientation="h", title="Top 15 palabras en mensajes negativos",
                              color="frecuencia", color_continuous_scale="OrRd", text="frecuencia")
                fig4.update_traces(textposition="outside")
                fig4.update_layout(showlegend=False, coloraxis_showscale=False)
                st.plotly_chart(fig4, use_container_width=True)
        if {"prob_sentimiento","prob_odio"}.issubset(df_fin.columns):
            df_sc = df_fin[["comentario_limpio","prob_sentimiento","prob_odio","sentimiento","es_odio"]].dropna().copy()
            df_sc["Sentimiento"] = df_sc["sentimiento"].map({"POS":"Positivo","NEG":"Negativo","NEU":"Neutral"})
            fig5 = px.scatter(df_sc, x="prob_sentimiento", y="prob_odio",
                              color="Sentimiento", symbol="es_odio",
                              hover_data={"comentario_limpio":True},
                              title="Confianza de sentimiento vs probabilidad de discurso discriminatorio",
                              labels={"prob_sentimiento":"Confianza sent.","prob_odio":"Prob. discriminatorio"},
                              color_discrete_map={"Positivo":"#2ecc71","Negativo":"#e74c3c","Neutral":"#95a5a6"},
                              opacity=0.7)
            st.plotly_chart(fig5, use_container_width=True)

    st.divider()

    # ── F4 ──────────────────────────────────────────────────
    seccion("F4 · ThroughputMonitor — Throughput (ventanas de 10 s)")
    if not df_tp.empty and "ventana_fin" in df_tp.columns:
        df_tp = df_tp.copy()
        df_tp["hora"] = df_tp["ventana_fin"].apply(ts_hms)
        avg_tp = df_tp["throughput_msg_seg"].mean()
        f4a,f4b = st.columns([3,1])
        with f4a:
            fig_a = px.area(df_tp, x="hora", y="throughput_msg_seg",
                            title="Mensajes/segundo por ventana de 10 s",
                            labels={"throughput_msg_seg":"msg/s","hora":"Hora"},
                            color_discrete_sequence=["#f39c12"])
            fig_a.add_hline(y=avg_tp, line_dash="dot", line_color="#7f8c8d",
                            annotation_text=f"Prom: {avg_tp:.2f} msg/s", annotation_position="top right")
            fig_a.update_layout(xaxis_tickangle=-40)
            st.plotly_chart(fig_a, use_container_width=True)
        with f4b:
            st.metric("Throughput actual",  f"{float(df_tp.iloc[-1]['throughput_msg_seg']):.2f} msg/s")
            st.metric("Máximo",             f"{df_tp['throughput_msg_seg'].max():.2f} msg/s")
            st.metric("Promedio",           f"{avg_tp:.2f} msg/s")
            st.metric("Total procesados",   f"{int(df_tp['cantidad_mensajes'].sum()):,}")
        fig_b = px.bar(df_tp, x="hora", y="cantidad_mensajes",
                       title="Mensajes por ventana de 10 s",
                       labels={"cantidad_mensajes":"Mensajes","hora":"Hora"},
                       color="cantidad_mensajes", color_continuous_scale="Blues", text="cantidad_mensajes")
        fig_b.update_traces(textposition="outside")
        fig_b.update_layout(xaxis_tickangle=-40, coloraxis_showscale=False)
        st.plotly_chart(fig_b, use_container_width=True)

    st.divider()

    # ── F5 ──────────────────────────────────────────────────
    seccion("F5 · SentimentWindow — Ventana de sentimiento y discurso discriminatorio (60 s)")
    if not df_v5.empty and "ventana_fin" in df_v5.columns:
        df_v5 = df_v5.copy()
        df_v5["hora"] = df_v5["ventana_fin"].apply(ts_hms)
        avg_odio = df_v5["porcentaje_odio"].mean()
        f5a,f5b = st.columns(2)
        with f5a:
            dm = df_v5.melt(id_vars=["hora"], value_vars=["positivos","negativos","neutrales"],
                            var_name="tipo", value_name="cantidad")
            dm["tipo"] = dm["tipo"].map({"positivos":"Positivo 😊","negativos":"Negativo 😠","neutrales":"Neutral 😐"})
            fig_s = px.bar(dm, x="hora", y="cantidad", color="tipo", barmode="stack",
                           title="Sentimiento por ventana de 60 s",
                           labels={"cantidad":"Comentarios","hora":"Hora","tipo":""},
                           color_discrete_map={"Positivo 😊":"#2ecc71","Negativo 😠":"#e74c3c","Neutral 😐":"#95a5a6"})
            fig_s.update_layout(xaxis_tickangle=-40, legend_title_text="")
            st.plotly_chart(fig_s, use_container_width=True)
        with f5b:
            fig_o = px.line(df_v5, x="hora", y="porcentaje_odio", markers=True,
                            title="% de discurso discriminatorio por ventana de 60 s",
                            labels={"porcentaje_odio":"% discriminatorio","hora":"Hora"},
                            color_discrete_sequence=["#c0392b"])
            fig_o.add_hline(y=avg_odio, line_dash="dot", line_color="#7f8c8d",
                            annotation_text=f"Prom: {avg_odio:.1f}%", annotation_position="top right")
            fig_o.update_layout(xaxis_tickangle=-40)
            st.plotly_chart(fig_o, use_container_width=True)
        lv = df_v5.iloc[-1]
        lv_t = int(lv.get("total_comentarios", 0))
        st.subheader(f"Última ventana — {ts_hms(lv['ventana_fin'])} · {lv_t} comentarios")
        lv1,lv2,lv3,lv4,lv5 = st.columns(5)
        lv1.metric("Total",               lv_t)
        lv2.metric("Positivos 😊",        int(lv.get("positivos",0)))
        lv3.metric("Negativos 😠",        int(lv.get("negativos",0)))
        lv4.metric("Neutrales 😐",        int(lv.get("neutrales",0)))
        lv5.metric("Discriminatorio 🚨",  f"{int(lv.get('odio',0))} ({lv.get('porcentaje_odio',0):.1f}%)")
        pct_g = float(lv.get("porcentaje_odio", 0))
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number+delta", value=pct_g,
            delta={"reference":avg_odio,"valueformat":".1f",
                   "increasing":{"color":"#c0392b"},"decreasing":{"color":"#27ae60"}},
            number={"suffix":"%","valueformat":".1f","font":{"size":42}},
            title={"text":"% Discurso discriminatorio — última ventana","font":{"size":13}},
            gauge={"axis":{"range":[0,100],"ticksuffix":"%"},
                   "bar":{"color":"#c0392b","thickness":0.3},
                   "steps":[{"range":[0,25],"color":"#d5f5e3"},
                            {"range":[25,50],"color":"#fef9e7"},
                            {"range":[50,100],"color":"#fadbd8"}],
                   "threshold":{"line":{"color":"#922b21","width":3},"thickness":0.75,"value":avg_odio}},
        ))
        fig_gauge.update_layout(height=270, margin=dict(t=50,b=10,l=50,r=50))
        cg,cr = st.columns([1,2])
        with cg: st.plotly_chart(fig_gauge, use_container_width=True)
        with cr:
            dr = df_v5[["hora","total_comentarios","odio","porcentaje_odio"]].copy()
            dr.columns = ["Hora","Total","Discriminatorio","% Discrim."]
            st.dataframe(dr, use_container_width=True, hide_index=True, height=270)

    st.divider()
    with st.expander("🔧 Debug"):
        st.write(f"Kafka `{KAFKA_SERVERS}` → {'✅' if st.session_state._kafka_ok else '❌'} &nbsp; Flink `{FLINK_URL}` → {'✅' if st.session_state._flink_info else '❌'}")
        st.write(f"Chat: {len(st.session_state.chat_msgs)} msgs · Cola: {len(st.session_state.cola_msgs)} pendientes")


# ════════════════════════════════════════════════════════════
#  TABS: Flink (tiempo real) | Spark (análisis batch)
# ════════════════════════════════════════════════════════════
tab_flink, tab_spark = st.tabs(["▶ Flink · Tiempo Real", "⚡ Spark · Análisis Batch"])

with tab_flink:
    seccion_live()
    st.divider()
    seccion_charts()

with tab_spark:
    st.markdown("### ⚡ Análisis Batch con Apache Spark 3.5.1")
    st.caption(f"Resultados almacenados en S3: `s3://{SPARK_BUCKET}/resultados/` · Cache 5 min")

    # ── S1 ───────────────────────────────────────────────────
    seccion("S1 · TopHateTerms — Palabras más frecuentes en discurso discriminatorio")
    df_s1 = leer_spark_s3("resultados/s1_top_hate_terms.parquet/")
    if not df_s1.empty:
        df_s1 = df_s1.sort_values("count", ascending=False).head(30)
        s1a, s1b = st.columns([2, 1])
        with s1a:
            fig_s1 = px.bar(
                df_s1.sort_values("count"), x="count", y="palabra",
                orientation="h",
                title="Top 30 palabras en comentarios discriminatorios",
                labels={"count": "Frecuencia", "palabra": "Palabra"},
                color="count", color_continuous_scale="Reds", text="count",
            )
            fig_s1.update_traces(textposition="outside")
            fig_s1.update_layout(showlegend=False, coloraxis_showscale=False, height=600)
            st.plotly_chart(fig_s1, use_container_width=True)
        with s1b:
            st.dataframe(df_s1[["palabra", "count"]].rename(columns={"palabra": "Palabra", "count": "Frecuencia"}),
                         use_container_width=True, hide_index=True, height=600)
    else:
        st.info("Ejecuta los jobs de Spark para ver resultados.")

    st.divider()

    # ── S2 ───────────────────────────────────────────────────
    seccion("S2 · UserRiskScore — Score de riesgo por usuario")
    df_s2 = leer_spark_s3("resultados/s2_user_risk_score.parquet/")
    if not df_s2.empty:
        df_s2 = df_s2.sort_values("score_riesgo", ascending=False).head(20)
        s2a, s2b = st.columns([2, 1])
        with s2a:
            fig_s2 = px.bar(
                df_s2.head(15).sort_values("score_riesgo"), x="score_riesgo", y="autor",
                orientation="h",
                title="Top 15 usuarios por score de riesgo",
                labels={"score_riesgo": "Score de riesgo", "autor": "Usuario"},
                color="porcentaje_odio", color_continuous_scale="OrRd", text="score_riesgo",
            )
            fig_s2.update_traces(textposition="outside")
            fig_s2.update_layout(coloraxis_showscale=True,
                                  coloraxis_colorbar=dict(title="% Discrim."), height=500)
            st.plotly_chart(fig_s2, use_container_width=True)
        with s2b:
            cols_show = ["autor", "total_comentarios", "comentarios_odio", "porcentaje_odio", "score_riesgo"]
            cols_show = [c for c in cols_show if c in df_s2.columns]
            st.dataframe(df_s2[cols_show].rename(columns={
                "autor": "Usuario", "total_comentarios": "Total",
                "comentarios_odio": "Discrimin.", "porcentaje_odio": "% Discrim.",
                "score_riesgo": "Score"
            }), use_container_width=True, hide_index=True, height=500)
    else:
        st.info("Ejecuta los jobs de Spark para ver resultados.")

    st.divider()

    # ── S3 ───────────────────────────────────────────────────
    seccion("S3 · Word2Vec — Palabras semánticamente similares (MLlib)")
    df_s3 = leer_spark_s3("resultados/s3_word2vec_similares.parquet/")
    if not df_s3.empty:
        terminos = sorted(df_s3["palabra_clave"].unique().tolist())
        cols_s3 = st.columns(min(len(terminos), 3))
        for i, termino in enumerate(terminos):
            sub = df_s3[df_s3["palabra_clave"] == termino].sort_values("similitud", ascending=False)
            with cols_s3[i % 3]:
                st.markdown(f"**{termino}**")
                fig_t = px.bar(
                    sub.sort_values("similitud"), x="similitud", y="palabra_similar",
                    orientation="h",
                    labels={"similitud": "Similitud", "palabra_similar": ""},
                    color="similitud", color_continuous_scale="Blues",
                    range_x=[0, 1],
                )
                fig_t.update_layout(showlegend=False, coloraxis_showscale=False,
                                    height=220, margin=dict(t=10, b=10, l=10, r=10))
                st.plotly_chart(fig_t, use_container_width=True)
    else:
        st.info("Ejecuta los jobs de Spark para ver resultados.")

    st.divider()

    # ── S4 ───────────────────────────────────────────────────
    seccion("S4 · HourlyHateTrend — Tendencia horaria de discurso discriminatorio (UTC-5)")
    df_s4 = leer_spark_s3("resultados/s4_hourly_hate_trend.parquet/")
    if not df_s4.empty:
        df_s4 = df_s4.sort_values("hora_dia")
        s4a, s4b = st.columns(2)
        with s4a:
            fig_s4a = px.bar(
                df_s4, x="hora_dia", y="total_comentarios",
                title="Total de comentarios por hora del día",
                labels={"hora_dia": "Hora (Peru UTC-5)", "total_comentarios": "Comentarios"},
                color="porcentaje_odio", color_continuous_scale="RdYlGn_r",
                text="total_comentarios",
            )
            fig_s4a.update_traces(textposition="outside")
            fig_s4a.update_layout(coloraxis_colorbar=dict(title="% Discrim."))
            st.plotly_chart(fig_s4a, use_container_width=True)
        with s4b:
            fig_s4b = px.line(
                df_s4, x="hora_dia", y="porcentaje_odio", markers=True,
                title="% de discurso discriminatorio por hora",
                labels={"hora_dia": "Hora (Peru UTC-5)", "porcentaje_odio": "% Discriminatorio"},
                color_discrete_sequence=["#c0392b"],
            )
            avg_s4 = df_s4["porcentaje_odio"].mean()
            fig_s4b.add_hline(y=avg_s4, line_dash="dot", line_color="#7f8c8d",
                               annotation_text=f"Prom: {avg_s4:.1f}%", annotation_position="top right")
            st.plotly_chart(fig_s4b, use_container_width=True)
        hora_pico = df_s4.loc[df_s4["porcentaje_odio"].idxmax()]
        st.info(f"Hora pico: **{int(hora_pico['hora_dia']):02d}:00** con **{hora_pico['porcentaje_odio']:.1f}%** de comentarios discriminatorios")
    else:
        st.info("Ejecuta los jobs de Spark para ver resultados.")

    st.divider()

    # ── S5 ───────────────────────────────────────────────────
    seccion("S5 · LikesOdioCorrelacion — Correlación estadística likes vs discurso discriminatorio")
    df_s5 = leer_spark_s3("resultados/s5_likes_odio_correlacion.parquet/")
    if not df_s5.empty:
        s5a, s5b, s5c = st.columns(3)
        row_odio     = df_s5[df_s5["es_odio"] == True].iloc[0]  if (df_s5["es_odio"] == True).any()  else None
        row_normal   = df_s5[df_s5["es_odio"] == False].iloc[0] if (df_s5["es_odio"] == False).any() else None
        correlacion  = float(df_s5["correlacion_pearson"].iloc[0]) if "correlacion_pearson" in df_s5.columns else 0
        interpretacion = str(df_s5["interpretacion"].iloc[0]) if "interpretacion" in df_s5.columns else ""

        if row_odio is not None:
            s5a.metric("Likes promedio — Discriminatorio", f"{float(row_odio.get('promedio_likes', 0)):.2f}")
            s5a.metric("Cantidad discriminatorios", f"{int(row_odio.get('cantidad', 0)):,}")
        if row_normal is not None:
            s5b.metric("Likes promedio — Normal", f"{float(row_normal.get('promedio_likes', 0)):.2f}")
            s5b.metric("Cantidad normales", f"{int(row_normal.get('cantidad', 0)):,}")
        s5c.metric("Correlacion de Pearson", f"{correlacion:.4f}")
        s5c.metric("Interpretacion", "Sin correlación" if abs(correlacion) < 0.1 else ("Positiva" if correlacion > 0 else "Negativa"))

        fig_s5 = px.bar(
            df_s5.assign(tipo=df_s5["es_odio"].map({True: "Discriminatorio", False: "Normal"})),
            x="tipo", y="promedio_likes",
            title="Promedio de likes: discriminatorio vs normal",
            labels={"tipo": "Tipo", "promedio_likes": "Promedio de likes"},
            color="tipo", color_discrete_map={"Discriminatorio": "#c0392b", "Normal": "#27ae60"},
            text="promedio_likes",
        )
        fig_s5.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig_s5.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig_s5, use_container_width=True)
        st.success(f"**Conclusión:** {interpretacion} (r = {correlacion:.4f})")
    else:
        st.info("Ejecuta los jobs de Spark para ver resultados.")
