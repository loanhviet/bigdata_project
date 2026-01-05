#!/usr/bin/env python3
"""
ES Stats Page - Thống kê Elasticsearch
"""
import streamlit as st
from utils.es_client import get_es_client

# Page config
st.set_page_config(
    page_title="ES Stats - Wikipedia",
    page_icon="📊",
    layout="wide"
)

# Get ES client
es = get_es_client()

# Header
st.title("Thống kê Elasticsearch")
st.markdown("---")

try:
    if es.indices.exists(index="wiki_docs"):
        stats = es.indices.stats(index="wiki_docs")
        count_response = es.count(index="wiki_docs")
        
        # Overview metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Tổng documents", f"{count_response['count']:,}")
        
        with col2:
            size_bytes = stats['_all']['total']['store']['size_in_bytes']
            size_mb = size_bytes / (1024 * 1024)
            st.metric("Dung lượng", f"{size_mb:.1f} MB")
        
        with col3:
            segments = stats['_all']['total']['segments']['count']
            st.metric("Segments", f"{segments}")
        
        with col4:
            docs_deleted = stats['_all']['total']['docs'].get('deleted', 0)
            st.metric("Docs deleted", f"{docs_deleted}")
        
        st.markdown("---")
        
        # Top categories
        st.subheader("Top 20 danh mục phổ biến")
        
        agg_query = {
            "size": 0,
            "aggs": {
                "top_categories": {
                    "terms": {
                        "field": "categories",
                        "size": 20
                    }
                }
            }
        }
        
        agg_response = es.search(index="wiki_docs", body=agg_query)
        buckets = agg_response["aggregations"]["top_categories"]["buckets"]
        
        if buckets:
            # Show in 2 columns
            col_left, col_right = st.columns(2)
            
            mid_point = len(buckets) // 2
            
            with col_left:
                for i, bucket in enumerate(buckets[:mid_point], 1):
                    col_cat1, col_cat2 = st.columns([3, 1])
                    with col_cat1:
                        st.text(f"{i}. {bucket['key']}")
                    with col_cat2:
                        st.text(f"{bucket['doc_count']:,} docs")
            
            with col_right:
                for i, bucket in enumerate(buckets[mid_point:], mid_point + 1):
                    col_cat1, col_cat2 = st.columns([3, 1])
                    with col_cat1:
                        st.text(f"{i}. {bucket['key']}")
                    with col_cat2:
                        st.text(f"{bucket['doc_count']:,} docs")
        else:
            st.info("Chưa có dữ liệu thống kê danh mục")
        
        st.markdown("---")
        
        # Index health
        st.subheader("Chi tiết Index")
        
        index_info = es.indices.get(index="wiki_docs")
        settings = index_info['wiki_docs']['settings']['index']
        
        col_info1, col_info2, col_info3 = st.columns(3)
        
        with col_info1:
            st.metric("Shards", settings.get('number_of_shards', 'N/A'))
        
        with col_info2:
            st.metric("Replicas", settings.get('number_of_replicas', 'N/A'))
        
        with col_info3:
            st.metric("Created", settings.get('creation_date', 'N/A'))
            
    else:
        st.warning("Index 'wiki_docs' chưa được tạo")
        st.info("Chạy `python elasticsearch/ingest_wiki_docs.py` để tạo index")
        
except Exception as e:
    st.error(f"Lỗi: {str(e)}")
    st.info("Đảm bảo Elasticsearch đang chạy tại http://localhost:9200")
