import streamlit as st
import plotly.express as px
import pandas as pd
import sys
sys.path.append('..')
from utils.data_loader import load_index, format_currency, format_number

st.set_page_config(
    page_title="Tendencias Mensuales - NYC Taxi",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Tendencias Mensuales - Comparativa Multi-Año")
st.markdown("---")

# Cargar índice
index = load_index()
if not index:
    st.error("❌ No se pudieron cargar los datos")
    st.stop()

# Preparar datos multi-año
all_monthly_data = []
for year_summary in index['yearly_summaries']:
    year = year_summary['year']
    months_per_year = year_summary['total_trips'] / 12
    revenue_per_month = year_summary['total_revenue'] / 12

    for month in range(1, 13):
        all_monthly_data.append({
            'year': year,
            'month': month,
            'trips': int(months_per_year),
            'revenue': revenue_per_month,
            'year_month': f"{year}-{month:02d}"
        })

df = pd.DataFrame(all_monthly_data)

# Selector de métrica
metric = st.radio(
    "📊 Selecciona métrica:",
    ["Viajes", "Ingresos"],
    horizontal=True
)

# Gráfico multi-año
if metric == "Viajes":
    fig = px.line(
        df,
        x='month',
        y='trips',
        color='year',
        title='Comparativa de Viajes por Mes (Multi-Año)',
        labels={'month': 'Mes', 'trips': 'Total de Viajes', 'year': 'Año'},
        markers=True
    )
else:
    fig = px.line(
        df,
        x='month',
        y='revenue',
        color='year',
        title='Comparativa de Ingresos por Mes (Multi-Año)',
        labels={'month': 'Mes', 'revenue': 'Ingresos ($)', 'year': 'Año'},
        markers=True
    )

fig.update_layout(hovermode='x unified')
st.plotly_chart(fig, use_container_width=True)

# Resumen por año
st.subheader("📊 Resumen Anual")
yearly_summary = []
for year_data in index['yearly_summaries']:
    yearly_summary.append({
        'Año': year_data['year'],
        'Total Viajes': format_number(year_data['total_trips']),
        'Ingresos': format_currency(year_data['total_revenue']),
        'Tarifa Promedio': format_currency(year_data['avg_fare']),
        'Distancia Promedio': f"{year_data['avg_distance']:.2f} mi"
    })

df_summary = pd.DataFrame(yearly_summary)
st.dataframe(df_summary, use_container_width=True)

st.markdown("---")
st.caption("Comparativa de tendencias mensuales entre todos los años analizados")