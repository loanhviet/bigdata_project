"""
Elasticsearch client shared across pages
"""
import streamlit as st
from elasticsearch import Elasticsearch

@st.cache_resource
def get_es_client():
    """Get cached Elasticsearch client"""
    return Elasticsearch(["http://localhost:9200"])
