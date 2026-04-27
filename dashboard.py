# --- SECCIÓN: ESTADO DE EQUIPOS (TABLA DE ALTA DENSIDAD) ---
st.subheader("🤖 Monitor de Equipos (Farm Status)")

# Simulamos o calculamos el estado basado en el último play
# (Asumiendo que tienes una columna 'scrobble_time')
df['scrobble_time'] = pd.to_datetime(df['scrobble_time'])
ultimo_update = df.groupby('lastfm_user')['scrobble_time'].max().reset_index()
ultimo_update['minutos_idle'] = (pd.Timestamp.now() - ultimo_update['scrobble_time']).dt.total_seconds() / 60

# Función para poner color al estado
def get_status_color(minutos):
    if minutos < 20: return "🟢 Activo"
    if minutos < 60: return "🟡 Warning"
    return "🔴 Offline"

ultimo_update['Estado'] = ultimo_update['minutos_idle'].apply(get_status_color)

# Mostramos una tabla limpia y compacta como en tu foto
st.dataframe(
    ultimo_update[['lastfm_user', 'scrobble_time', 'Estado']],
    column_config={
        "lastfm_user": "Equipo / Usuario",
        "scrobble_time": "Último Play",
        "Estado": st.column_config.TextColumn("Status")
    },
    use_container_width=True,
    hide_index=True
)

# --- SECCIÓN: GRÁFICA DE REPRODUCCIÓN (VISUAL) ---
st.subheader("📈 Actividad de las últimas 24 Horas")
# Agrupamos por hora para la gráfica
df_h = filtered_df.resample('H', on='scrobble_time').size().reset_index(name='plays')
fig = px.area(df_h, x='scrobble_time', y='plays', 
              title="Flujo de Reproducción",
              template="plotly_dark", 
              color_discrete_sequence=['#00ffc8']) # Verde neón tipo matriz

st.plotly_chart(fig, use_container_width=True)
