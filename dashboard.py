import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timezone
import os

# Importamos tus funciones de confianza
from app import get_conn, init_db, PLATFORM_RATES

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    page_title="WatchEagle Pro | Dashboard",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- ESTILO CSS PARA ALTA DENSIDAD ---
st.markdown("""
    <style>
    .stApp { background-color: #0e1117; }
    [data-testid="stMetricCard"] {
        background-color: #1a1c24;
        border: 1px solid #2d2f39;
        border-radius: 10px;
    }
    .status-ok { color: #00ffc8; font-weight: bold; }
    .status-warning { color: #ffcc00; font-weight: bold; }
    .status-danger { color: #ff4b4b; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# --- LÓGICA DE DATOS ---
@st.cache_data(ttl=60) # Se actualiza cada minuto
def load_all_data():
    init_db()
    conn = get_conn()
    query = """
        SELECT s.*, am.distributor 
        FROM scrobbles s
        LEFT JOIN artist_metadata am ON LOWER(s.artist_name) = LOWER(am.artist_name)
        ORDER BY s.scrobble_time DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Asegurar que la fecha sea objeto de pandas
    df['scrobble_time'] = pd.to_datetime(df['scrobble_time'])
    return df

def calcular_ganancias(df_filtrado):
    ganancia = 0.0
    for plat, rates in PLATFORM_RATES.items():
        count = len(df_filtrado[df_filtrado['app_name'].str.lower() == plat])
        avg_rate = (rates['min'] + rates['max']) / 2
        ganancia += count * avg_rate
    return ganancia

# --- MAIN APP ---
try:
    df = load_all_data()

    # --- SIDEBAR ---
    st.sidebar.image("https://cdn-icons-png.flaticon.com/512/1534/1534004.png", width=80) # Icono de aguila/eagle
    st.sidebar.title("WatchEagle HQ")
    
    st.sidebar.divider()
    
    # Filtros
    plataforma = st.sidebar.multiselect("Plataformas", options=df['app_name'].unique(), default=df['app_name'].unique())
    distribuidora = st.sidebar.multiselect("Distribuidoras", options=df['distributor'].dropna().unique(), default=df['distributor'].dropna().unique())
    
    # Filtrado dinámico
    mask = df['app_name'].isin(plataforma)
    if distribuidora:
        mask &= df['distributor'].isin(distribuidora)
    
    f_df = df[mask]

    # --- DASHBOARD PRINCIPAL ---
    st.title("🦅 Panel de Control Operativo")
    
    # METRICAS KPI
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Reproducciones", f"{len(f_df):,}")
    with c2:
        st.metric("Artistas Activos", f_df['artist_name'].nunique())
    with c3:
        ganancia_total = calcular_ganancias(f_df)
        st.metric("Ganancia Est. (USD)", f"${ganancia_total:,.2f}")
    with c4:
        # Equipos que han scrobbleado en la última hora
        activos = f_df[f_df['scrobble_time'] > (pd.Timestamp.now() - pd.Timedelta(hours=1))]['lastfm_user'].nunique()
        st.metric("Equipos Online", activos)

    st.divider()

    # --- FILA 2: GRÁFICO Y MONITOR 1K ---
    col_main, col_side = st.columns([2, 1])

    with col_main:
        st.subheader("📊 Flujo de Actividad (24h)")
        # Agrupar por hora
        df_24h = f_df.set_index('scrobble_time').resample('H').size().reset_index(name='count')
        fig = px.area(df_24h, x='scrobble_time', y='count', template="plotly_dark",
                     color_discrete_sequence=['#00ffc8'])
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=300)
        st.plotly_chart(fig, use_container_width=True)

    with col_side:
        st.subheader("🔥 Meta 1,000 Plays")
        tracks_1k = f_df.groupby(['artist_name', 'track_name']).size().reset_index(name='plays')
        tracks_1k = tracks_1k[tracks_1k['plays'] < 1000].sort_values('plays', ascending=False).head(5)
        
        for _, row in tracks_1k.iterrows():
            pct = row['plays'] / 1000
            st.write(f"**{row['track_name']}**")
            st.progress(pct, text=f"{row['plays']}/1000")

    st.divider()

    # --- FILA 3: ESTADO DE LA GRANJA (TABLA DE ALTA DENSIDAD) ---
    st.subheader("🤖 Monitor de Equipos (Real-Time)")
    
    # Cálculo de inactividad
    now = pd.Timestamp.now().tz_localize(None)
    status_df = df.groupby(['lastfm_user', 'app_name']).agg(
        ultimo_play=('scrobble_time', 'max'),
        total_plays=('scrobble_time', 'count')
    ).reset_index()
    
    status_df['ultimo_play'] = status_df['ultimo_play'].dt.tz_localize(None)
    status_df['idle_min'] = (now - status_df['ultimo_play']).dt.total_seconds() / 60
    
    def get_status(minutos):
        if minutos < 15: return "🟢 OPERATIVO"
        if minutos < 30: return "🟡 STANDBY"
        return "🔴 INCIDENTE"

    status_df['Estado'] = status_df['idle_min'].apply(get_status)

    st.dataframe(
        status_df.sort_values('idle_min'),
        column_config={
            "lastfm_user": "Usuario Last.fm",
            "app_name": "App",
            "ultimo_play": st.column_config.DatetimeColumn("Último Scrobble", format="D MMM, h:mm a"),
            "total_plays": "Total",
            "idle_min": st.column_config.NumberColumn("Inactivo (min)", format="%d"),
            "Estado": "Status"
        },
        hide_index=True,
        use_container_width=True
    )

except Exception as e:
    st.error(f"⚠️ Error crítico: {str(e)}")
    st.info("Asegúrate de tener configurada la variable DATABASE_URL.")
