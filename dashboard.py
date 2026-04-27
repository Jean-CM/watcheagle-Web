import streamlit as st
import pandas as pd
import plotly.express as px
from helpers import get_conn, init_db, PLATFORM_RATES

# 1. Configuración Pro
st.set_page_config(page_title="WatchEagle Control", layout="wide")

# Inicializar DB (como en tus scripts)
init_db() [cite: 6]

# Estilo personalizado para que se vea "duro"
st.markdown("""
    <style>
    .main { background-color: #061126; }
    div[data-testid="stMetricValue"] { font-size: 28px; font-weight: 900; color: #60a5fa; }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR (Filtros) ---
st.sidebar.title("🦅 WatchEagle Menu")
view = st.sidebar.radio("Ir a:", ["Monitor Operativo", "Análisis de Ganancias"])
selected_platform = st.sidebar.selectbox("Plataforma", ["Todas", "Spotify", "Apple Music", "Tidal", "YouTube"])

# --- DATA LOADING ---
def load_data():
    conn = get_conn() [cite: 6]
    query = "SELECT * FROM scrobbles"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

df = load_data()

# --- CUERPO PRINCIPAL ---
st.title(f"WatchEagle: {view}")

if view == "Monitor Operativo":
    # Aquí recreamos tu 'render_monitor'
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Scrobbles", len(df))
    with col2:
        # Lógica de 'monitor-plays': cuantos bajo 1000 
        under_1k = df.groupby('track_name').size()
        under_1k = under_1k[under_1k < 1000].count()
        st.metric("Tracks < 1000 Plays", under_1k, delta="-2", delta_color="inverse")
    
    # Gráfica interactiva de reproducción por hora
    df['hora'] = pd.to_datetime(df['scrobble_time']).dt.hour
    fig = px.histogram(df, x="hora", title="Actividad por Hora (Heatmap de Granja)", color_discrete_sequence=['#a855f7'])
    st.plotly_chart(fig, use_container_width=True)

elif view == "Análisis de Ganancias":
    # Aquí usamos tus PLATFORM_RATES 
    st.subheader("Estimación de Royalties")
    # ... lógica de cálculos ...
