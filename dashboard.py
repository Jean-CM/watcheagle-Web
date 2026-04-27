import streamlit as st
import pandas as pd
import plotly.express as px
from helpers import get_conn, init_db, PLATFORM_RATES

# Configuración de la interfaz
st.set_page_config(
    page_title="WatchEagle Pro Dashboard",
    page_icon="🦅",
    layout="wide", # Usa todo el ancho de la pantalla
    initial_sidebar_state="expanded"
)

# Inicializar DB y Cargar Datos
def load_data():
    init_db() # Asegura que las tablas existen 
    conn = get_conn()
    # Traemos los scrobbles y unimos con metadata si existe
    query = """
        SELECT s.*, am.distributor 
        FROM scrobbles s
        LEFT JOIN artist_metadata am ON LOWER(s.artist_name) = LOWER(am.artist_name)
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# --- INTERFAZ ---
st.title("🦅 WatchEagle: Inteligencia de Streaming")

try:
    df = load_data()
    
    # Barra Lateral con Filtros Interactivos
    st.sidebar.header("Filtros Globales")
    
    # Filtro de Plataforma 
    plataformas = ["Todas"] + sorted(df['app_name'].unique().tolist())
    sel_platform = st.sidebar.selectbox("Selecciona Plataforma", plataformas)
    
    # Filtro de Distribuidora
    distribuidores = ["Todas"] + sorted(df['distributor'].dropna().unique().tolist())
    sel_dist = st.sidebar.selectbox("Selecciona Distribuidora", distribuidores)

    # Aplicar Filtros al DataFrame
    filtered_df = df.copy()
    if sel_platform != "Todas":
        filtered_df = filtered_df[filtered_df['app_name'] == sel_platform]
    if sel_dist != "Todas":
        filtered_df = filtered_df[filtered_df['distributor'] == sel_dist]

    # Mostrar métricas rápidas (Solo el esqueleto por ahora)
    st.subheader("Métricas en Tiempo Real")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Plays", len(filtered_df))
    c2.metric("Artistas Únicos", filtered_df['artist_name'].nunique())
    c3.metric("Equipos Activos", filtered_df['lastfm_user'].nunique())

except Exception as e:
    st.error(f"Error al conectar con la base de datos: {e}")
