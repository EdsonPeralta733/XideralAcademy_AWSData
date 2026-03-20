import streamlit as st
import json
import boto3
from datetime import datetime

# Configuración de la página
st.set_page_config(
    page_title="NYC Taxi Analysis Dashboard",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuración S3
BUCKET = "xideralaws-curso-edson"
s3_client = boto3.client('s3')

def load_index():
    """Carga el índice general de análisis"""
    try:
        response = s3_client.get_object(
            Bucket=BUCKET,
            Key='nyc-taxi/analysis-results/index.json'
        )
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        st.error(f"Error cargando índice: {str(e)}")
        return None

# Header
st.title("🚕 NYC Taxi Analysis Dashboard")
st.markdown("---")

# Cargar datos
with st.spinner("Cargando datos..."):
    index = load_index()

if index:
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)

    total_trips = sum([year['total_trips'] for year in index['yearly_summaries']])
    total_revenue = sum([year['total_revenue'] for year in index['yearly_summaries']])
    avg_fare = total_revenue / total_trips if total_trips > 0 else 0
    years_count = index['total_years']

    with col1:
        st.metric("📊 Total de Viajes", f"{total_trips:,}")

    with col2:
        st.metric("💰 Ingresos Totales", f"${total_revenue:,.2f}")

    with col3:
        st.metric("💵 Tarifa Promedio", f"${avg_fare:.2f}")

    with col4:
        st.metric("📅 Años Analizados", years_count)

    st.markdown("---")

    # Resumen por año
    st.subheader("📊 Resumen por Año")

    for year_summary in index['yearly_summaries']:
        with st.expander(f"Año {year_summary['year']}"):
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Viajes", f"{year_summary['total_trips']:,}")
            with col2:
                st.metric("Ingresos", f"${year_summary['total_revenue']:,.2f}")
            with col3:
                st.metric("Tarifa Promedio", f"${year_summary['avg_fare']:.2f}")

            col4, col5, col6 = st.columns(3)
            with col4:
                st.metric("Distancia Promedio", f"{year_summary['avg_distance']:.2f} mi")
            with col5:
                st.metric("Duración Promedio", f"{year_summary['avg_duration']:.1f} min")
            if 'avg_tips' in year_summary:
                with col6:
                    st.metric("Propina Promedio", f"${year_summary['avg_tips']:.2f}")

    # Footer
    st.markdown("---")
    st.info("👈 Usa el menú lateral para explorar análisis detallados por año")

else:
    st.error("No se pudieron cargar los datos del análisis")

# Sidebar
st.sidebar.title("Navegación")
st.sidebar.info("""
**Páginas disponibles:**
- 🏠 Home (esta página)
- 📊 Análisis por Año
- 📈 Tendencias Mensuales
- 🕐 Patrones Horarios
- 📅 Patrones Semanales
""")

st.sidebar.markdown("---")
st.sidebar.caption(f"Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M')}")