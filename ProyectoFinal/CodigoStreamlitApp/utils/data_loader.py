"""
Utilidades para cargar datos desde S3
"""
import json
import boto3
import streamlit as st
from typing import Optional, Dict, List

# Configuración S3
BUCKET = "xideralaws-curso-edson"
s3_client = boto3.client('s3')

@st.cache_data(ttl=300)  # Cache por 5 minutos
def load_index() -> Optional[Dict]:
    """Carga el índice general de análisis"""
    try:
        response = s3_client.get_object(
            Bucket=BUCKET,
            Key='nyc-taxi/analysis-results/index.json'
        )
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        st.error(f"❌ Error cargando índice: {str(e)}")
        return None

@st.cache_data(ttl=300)
def load_year_analysis(year: int) -> Optional[Dict]:
    """Carga análisis de un año específico"""
    try:
        response = s3_client.get_object(
            Bucket=BUCKET,
            Key=f'nyc-taxi/analysis-results/by-year/{year}_analysis.json'
        )
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        st.error(f"❌ Error cargando datos de {year}: {str(e)}")
        return None

def get_available_years() -> List[int]:
    """Obtiene la lista de años disponibles"""
    try:
        index = load_index()
        if index and 'yearly_summaries' in index:
            return sorted([y['year'] for y in index['yearly_summaries']])
        return []
    except:
        return []

def format_currency(value: float) -> str:
    """Formatea valores monetarios"""
    return f"${value:,.2f}"

def format_number(value: int) -> str:
    """Formatea números grandes con comas"""
    return f"{value:,}"

def format_percentage(value: float) -> str:
    """Formatea porcentajes"""
    return f"{value:.1f}%"