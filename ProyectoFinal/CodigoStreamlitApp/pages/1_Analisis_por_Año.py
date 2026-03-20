import streamlit as st
import plotly.express as px
import pandas as pd
import sys
sys.path.append('..')
from utils.data_loader import load_year_analysis, get_available_years, format_currency, format_number

st.set_page_config(
    page_title="Análisis por Año - NYC Taxi",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Análisis por Año")
st.markdown("---")

# Selector de año
years = get_available_years()
if not years:
    st.error("❌ No se encontraron datos disponibles")
    st.stop()

selected_year = st.selectbox("📅 Selecciona un año:", years, index=len(years)-1)

# Cargar datos
with st.spinner(f"Cargando datos de {selected_year}..."):
    data = load_year_analysis(selected_year)

if not data:
    st.error(f"No se pudieron cargar los datos de {selected_year}")
    st.stop()

# Resumen general
st.subheader(f"📋 Resumen General - {selected_year}")
summary = data['summary']

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("🚕 Total Viajes", format_number(summary['total_trips']))
with col2:
    st.metric("💰 Ingresos Totales", format_currency(summary['total_revenue']))
with col3:
    st.metric("💵 Tarifa Promedio", format_currency(summary['avg_fare']))
with col4:
    st.metric("📏 Distancia Promedio", f"{summary['avg_distance']:.2f} mi")

col5, col6, col7 = st.columns(3)
with col5:
    st.metric("⏱️ Duración Promedio", f"{summary['avg_duration']:.1f} min")
with col6:
    if 'avg_tips' in summary:
        st.metric("💸 Propina Promedio", format_currency(summary['avg_tips']))
with col7:
    if 'quality_score' in summary:
        st.metric("✅ Calidad de Datos", f"{summary['quality_score']:.1f}%")

st.markdown("---")

# Métricas mensuales
st.subheader("📈 Tendencias Mensuales")

monthly = data['monthly_metrics']
df_monthly = pd.DataFrame(monthly)

# Gráfico de viajes por mes
fig_trips = px.bar(
    df_monthly,
    x='month',
    y='total_trips',
    title=f'Viajes por Mes - {selected_year}',
    labels={'month': 'Mes', 'total_trips': 'Total de Viajes'},
    color='total_trips',
    color_continuous_scale='Blues'
)
fig_trips.update_layout(showlegend=False)
st.plotly_chart(fig_trips, use_container_width=True)

# Gráfico de ingresos por mes
fig_revenue = px.line(
    df_monthly,
    x='month',
    y='total_revenue',
    title=f'Ingresos por Mes - {selected_year}',
    labels={'month': 'Mes', 'total_revenue': 'Ingresos ($)'},
    markers=True
)
fig_revenue.update_traces(line_color='#00CC96', line_width=3)
st.plotly_chart(fig_revenue, use_container_width=True)

# Tabla detallada
st.subheader("📊 Tabla Detallada por Mes")
df_display = df_monthly.copy()
df_display['total_trips'] = df_display['total_trips'].apply(lambda x: format_number(int(x)))
df_display['total_revenue'] = df_display['total_revenue'].apply(format_currency)
df_display['avg_fare'] = df_display['avg_fare'].apply(format_currency)
df_display['avg_distance'] = df_display['avg_distance'].apply(lambda x: f"{x:.2f} mi")
df_display['avg_duration'] = df_display['avg_duration'].apply(lambda x: f"{x:.1f} min")

st.dataframe(df_display, use_container_width=True)

st.markdown("---")
st.caption(f"Datos de NYC Taxi - Año {selected_year}")