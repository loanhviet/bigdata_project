#!/usr/bin/env python3
"""
WordCount Page - Phân tích tần suất từ khóa
"""
import streamlit as st
import pandas as pd
from utils.es_client import get_es_client

# Page config
st.set_page_config(
    page_title="WordCount - Wikipedia",
    page_icon="🔤",
    layout="wide"
)

# Get ES client
es = get_es_client()

# Header
st.title("Phân tích tần suất từ khóa")
st.markdown("---")

@st.cache_data(ttl=3600)
def load_wordcount_data():
    """Query wordcount từ Elasticsearch"""
    try:
        query = {
            "size": 10000,
            "_source": ["word", "count"],
            "sort": [{"count": "desc"}]
        }
        
        response = es.search(index="wiki_wordcount", body=query)
        
        data = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            data.append({'word': source['word'], 'count': source['count']})
        
        if not data:
            return None, "Không có dữ liệu"
        
        df = pd.DataFrame(data)
        return df, None
        
    except Exception as e:
        return None, str(e)

# Load data
with st.spinner("Đang tải dữ liệu..."):
    df_wc, error = load_wordcount_data()

if error:
    st.error(f"Lỗi: {error}")
    st.info("Chạy `python elasticsearch/index_all_data.py` để index dữ liệu")
elif df_wc is not None and len(df_wc) > 0:
    
    # Overview metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Tổng từ khác nhau", f"{len(df_wc):,}")
    with col2:
        st.metric("Tổng từ xuất hiện", f"{df_wc['count'].sum():,}")
    with col3:
        st.metric("Từ phổ biến nhất", f"{df_wc.iloc[0]['word']}")
    
    st.markdown("---")
    
    # Filters
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        top_n = st.slider("Số từ hiển thị:", 10, 200, 50)
    
    with col_f2:
        min_count = st.number_input("Tần suất tối thiểu:", 1, 10000, 100)
    
    with col_f3:
        search_word = st.text_input("Tìm từ cụ thể:", "")
    
    # Apply filters
    df_filtered = df_wc[df_wc['count'] >= min_count]
    
    if search_word:
        df_filtered = df_filtered[df_filtered['word'].str.contains(search_word, case=False, na=False)]
    
    df_filtered = df_filtered.head(top_n)
    
    st.markdown("---")
    
    # Chart
    st.subheader(f"Top {len(df_filtered)} từ khóa")
    
    if len(df_filtered) > 0:
        st.bar_chart(df_filtered.set_index('word')['count'], height=500)
        
        st.markdown("---")
        
        # Table
        st.subheader("Bảng chi tiết")
        
        df_display = df_filtered.rename(columns={'word': 'Từ', 'count': 'Số lần xuất hiện'})
        
        st.dataframe(
            df_display,
            use_container_width=True,
            height=400
        )
        
        # Download
        csv = df_display.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "Tải xuống CSV",
            csv,
            "wordcount.csv",
            "text/csv",
            use_container_width=True
        )
    else:
        st.warning("Không có từ nào phù hợp với bộ lọc")
        
else:
    st.info("Chưa có dữ liệu. Chạy `python elasticsearch/index_all_data.py`")
