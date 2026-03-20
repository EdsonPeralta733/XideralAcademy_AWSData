import streamlit as st
import plotly.express as px
import pandas as pd
import sys
sys.path.append('..')
from utils.data_loader import load_year_analysis, get_available_years, format_number

st.set_page_config(
    page_title="Patrones Horarios - NYC Taxi",
    page_icon="🕐",
    layout="wide"
)

st.title("🕐 Patrones Horarios")
st.markdown("---")

# Selector de año
years = get_available_years()
if not years:
    st.error("❌ No se encontraron datos disponibles")
    st.stop()

selected_year = st.selectbox("📅 Selecciona un año:", years, index=len(years)-1)

# Cargar datos
data = load_year_analysis(selected_year)
if not data or 'hourly_patterns' not in data:
    st.error("No hay datos de patrones horarios disponibles")
    st.stop()

hourly = data['hourly_patterns']
df_hourly = pd.DataFrame(hourly)

# Gráfico de viajes por hora
fig_trips = px.bar(
    df_hourly,
    x='hour',
    y='total_trips',
    title=f'Viajes por Hora del Día - {selected_year}',
    labels={'hour': 'Hora del Día', 'trip_count': 'Cantidad de Viajes'},
    color='total_trips',
    color_continuous_scale='Viridis'
)
fig_trips.update_layout(showlegend=False)
st.plotly_chart(fig_trips, use_container_width=True)

# Gráfico de tarifa promedio por hora
fig_fare = px.line(
    df_hourly,
    x='hour',
    y='avg_fare',
    title=f'Tarifa Promedio por Hora - {selected_year}',
    labels={'hour': 'Hora del Día', 'avg_fare': 'Tarifa Promedio ($)'},
    markers=True
)
fig_fare.update_traces(line_color='#EF553B', line_width=3)
st.plotly_chart(fig_fare, use_container_width=True)

# Identificar horas pico
st.subheader("⏰ Horas Pico")
top_hours = df_hourly.nlargest(5, 'total_trips')[['hour', 'total_trips', 'avg_fare']]
col1, col2 = st.columns(2)

with col1:
    st.metric("🏆 Hora con Más Viajes", f"{int(top_hours.iloc[0]['hour']):02d}:00",
            format_number(int(top_hours.iloc[0]['total_trips'])))

with col2:
    max_fare_row = df_hourly.loc[df_hourly['avg_fare'].idxmax()]
    st.metric("💰 Hora con Mayor Tarifa", f"{int(max_fare_row['hour']):02d}:00",
            f"${max_fare_row['avg_fare']:.2f}")

# Tabla detallada
st.subheader("📊 Tabla Detallada por Hora")
df_display = df_hourly.copy()
df_display['hour'] = df_display['hour'].apply(lambda x: f"{int(x):02d}:00")
df_display['total_trips'] = df_display['total_trips'].apply(lambda x: format_number(int(x)))
df_display['avg_fare'] = df_display['avg_fare'].apply(lambda x: f"${x:.2f}")
df_display['avg_distance'] = df_display['avg_distance'].apply(lambda x: f"{x:.2f} mi")

st.dataframe(df_display, use_container_width=True)

st.markdown("---")
st.caption(f"Patrones horarios de taxis - Año {selected_year}")