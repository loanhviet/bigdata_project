#!/usr/bin/env python3
"""
Wikipedia Search System - Home Page
"""
import streamlit as st
from utils.es_client import get_es_client

# Cấu hình page
st.set_page_config(
    page_title="Wikipedia Search System",
    page_icon="🔍",
    layout="wide"
)

# Get ES client
es = get_es_client()

# Header
st.title("Hệ thống Tìm kiếm Wikipedia tiếng Việt")
st.markdown("---")

# Welcome section
st.markdown("""
### Chào mừng đến với hệ thống tìm kiếm Wikipedia

Hệ thống này cung cấp các chức năng:

**Search** - Tìm kiếm toàn văn trên Wikipedia tiếng Việt  
**ES Stats** - Thống kê chi tiết về Elasticsearch index  
**Trend Analysis** - Phân tích xu hướng từ khóa theo thời gian  
**WordCount** - Thống kê tần suất xuất hiện của từ khóa  
**Categories** - Phân tích theo danh mục Wikipedia  
**Info** - Thông tin về hệ thống và kiến trúc

### Bắt đầu

Chọn một mục từ **sidebar** bên trái để bắt đầu sử dụng hệ thống.

""")

st.markdown("---")

# System status
st.subheader("Trạng thái hệ thống")

col1, col2, col3 = st.columns(3)

with col1:
    try:
        info = es.info()
        st.success(f"**Elasticsearch**  \nv{info['version']['number']}")
    except:
        st.error("**Elasticsearch**  \nKhông kết nối được")

with col2:
    try:
        if es.indices.exists(index="wiki_docs"):
            count = es.count(index="wiki_docs")['count']
            st.success(f"**wiki_docs**  \n{count:,} documents")
        else:
            st.warning("**wiki_docs**  \nChưa được tạo")
    except:
        st.error("**wiki_docs**  \nKhông kiểm tra được")

with col3:
    indices = ["wiki_wordcount", "wiki_trend", "wiki_cat_kwlist", "wiki_cat_docs"]
    exists_count = 0
    try:
        for idx in indices:
            if es.indices.exists(index=idx):
                exists_count += 1
        st.success(f"**MapReduce Indices**  \n{exists_count}/{len(indices)} indices")
    except:
        st.error("**MapReduce Indices**  \nKhông kiểm tra được")

st.markdown("---")

# Quick links
st.subheader("Truy cập nhanh")

col_link1, col_link2, col_link3 = st.columns(3)

with col_link1:
    st.page_link("pages/01_Search.py", label="Tìm kiếm Wikipedia", use_container_width=True)
    st.page_link("pages/03_Trend_Analysis.py", label="Phân tích xu hướng", use_container_width=True)

with col_link2:
    st.page_link("pages/02_ES_Stats.py", label="Thống kê ES", use_container_width=True)
    st.page_link("pages/04_WordCount.py", label="Phân tích từ khóa", use_container_width=True)

with col_link3:
    st.page_link("pages/05_Categories.py", label="Phân tích danh mục", use_container_width=True)
    st.page_link("pages/06_Info.py", label="Thông tin hệ thống", use_container_width=True)

# Footer
st.markdown("---")
st.caption("Wikipedia Search System | Powered by Hadoop + Elasticsearch + Streamlit")

