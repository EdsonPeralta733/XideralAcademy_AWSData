import streamlit as st
import plotly.express as px
import pandas as pd
import sys
sys.path.append('..')
from utils.data_loader import load_year_analysis, get_available_years, format_number, format_currency

st.set_page_config(
    page_title="Patrones Semanales - NYC Taxi",
    page_icon="📅",
    layout="wide"
)

st.title("📅 Patrones Semanales")
st.markdown("---")

# Selector de año
years = get_available_years()
if not years:
    st.error("❌ No se encontraron datos disponibles")
    st.stop()

selected_year = st.selectbox("📅 Selecciona un año:", years, index=len(years)-1)

# Cargar datos
data = load_year_analysis(selected_year)
if not data or 'day_of_week_patterns' not in data:
    st.error("No hay datos de patrones semanales disponibles")
    st.stop()

weekly = data['day_of_week_patterns']
df_weekly = pd.DataFrame(weekly)

# CALCULAR total_revenue (total_trips * avg_fare)
df_weekly['total_revenue'] = df_weekly['total_trips'] * df_weekly['avg_fare']

# Mapear días a nombres
day_names = {
    1: 'Lunes', 2: 'Martes', 3: 'Miércoles', 4: 'Jueves',
    5: 'Viernes', 6: 'Sábado', 7: 'Domingo'
}
df_weekly['day_name'] = df_weekly['day_of_week'].map(day_names)

# Gráfico de viajes por día
fig_trips = px.bar(
    df_weekly,
    x='day_name',
    y='total_trips',
    title=f'Viajes por Día de la Semana - {selected_year}',
    labels={'day_name': 'Día', 'trip_count': 'Cantidad de Viajes'},
    color='total_trips',
    color_continuous_scale='Teal'
)
fig_trips.update_layout(showlegend=False)
st.plotly_chart(fig_trips, use_container_width=True)

# Comparativa de ingresos
fig_revenue = px.bar(
    df_weekly,
    x='day_name',
    y='total_revenue',
    title=f'Ingresos por Día de la Semana - {selected_year}',
    labels={'day_name': 'Día', 'total_revenue': 'Ingresos ($)'},
    color='total_revenue',
    color_continuous_scale='Purples'
)
fig_revenue.update_layout(showlegend=False)
st.plotly_chart(fig_revenue, use_container_width=True)

# Métricas clave
st.subheader("📊 Comparativa Weekday vs Weekend")
col1, col2 = st.columns(2)

# Separar weekday y weekend
weekday_data = df_weekly[df_weekly['day_of_week'] <= 5]
weekend_data = df_weekly[df_weekly['day_of_week'] > 5]

weekday_trips = int(weekday_data['total_trips'].sum())
weekend_trips = int(weekend_data['total_trips'].sum())

with col1:
    st.metric("📅 Viajes en Días de Semana", format_number(weekday_trips))
    weekday_avg_revenue = (weekday_data['total_trips'] * weekday_data['avg_fare']).sum() / len(weekday_data)
    st.metric("💰 Ingreso Promedio/Día", format_currency(weekday_avg_revenue))

with col2:
    st.metric("🎉 Viajes en Fin de Semana", format_number(weekend_trips))
    weekend_avg_revenue = (weekend_data['total_trips'] * weekend_data['avg_fare']).sum() / len(weekend_data)
    st.metric("💰 Ingreso Promedio/Día", format_currency(weekend_avg_revenue))

# Tabla detallada
st.subheader("📋 Detalle por Día")
df_display = df_weekly[['day_name', 'total_trips', 'total_revenue', 'avg_fare', 'avg_distance']].copy()
df_display.columns = ['Día', 'Viajes', 'Ingresos', 'Tarifa Promedio', 'Distancia Promedio']
df_display['Viajes'] = df_display['Viajes'].apply(lambda x: format_number(int(x)))
df_display['Ingresos'] = df_display['Ingresos'].apply(format_currency)
df_display['Tarifa Promedio'] = df_display['Tarifa Promedio'].apply(format_currency)
df_display['Distancia Promedio'] = df_display['Distancia Promedio'].apply(lambda x: f"{x:.2f} mi")

st.dataframe(df_display, use_container_width=True)

st.markdown("---")
st.caption(f"Patrones semanales de taxis - Año {selected_year}")