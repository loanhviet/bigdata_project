#!/usr/bin/env python3
"""
Categories Page - Phân tích danh mục
"""
import streamlit as st
import pandas as pd
from utils.es_client import get_es_client

# Page config
st.set_page_config(
    page_title="Categories - Wikipedia",
    page_icon="🏷️",
    layout="wide"
)

# Get ES client
es = get_es_client()

# Header
st.title("Phân tích danh mục Wikipedia")
st.markdown("---")

# Tabs cho 2 loại phân tích
tab1, tab2 = st.tabs(["Category Documents", "Category Keywords"])

# ==================== TAB 1: CATEGORY DOCUMENTS ====================
with tab1:
    st.header("Documents by Category")
    st.markdown("Phân tích số lượng tài liệu trong mỗi danh mục Wikipedia")
    
    @st.cache_data(ttl=3600)
    def load_category_docs():
        try:
            query = {
                "size": 10000,
                "_source": ["category", "doc_count"],
                "sort": [{"doc_count": "desc"}]
            }
            response = es.search(index="wiki_cat_docs", body=query)
            
            data = []
            for hit in response["hits"]["hits"]:
                s = hit["_source"]
                data.append({
                    'category': s['category'],
                    'doc_count': s['doc_count']
                })
            
            return pd.DataFrame(data) if data else None, None
            
        except Exception as e:
            return None, str(e)
    
    with st.spinner("Loading category documents data..."):
        df_docs, error_docs = load_category_docs()
    
    if error_docs:
        st.error(f"Lỗi: {error_docs}")
        st.info("Chạy: `python elasticsearch/index_all_data.py`")
    elif df_docs is not None and len(df_docs) > 0:
        
        # Metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Tổng danh mục", f"{len(df_docs):,}")
        with col2:
            st.metric("Tổng documents", f"{df_docs['doc_count'].sum():,}")
        with col3:
            avg_docs = df_docs['doc_count'].mean()
            st.metric("TB docs/danh mục", f"{avg_docs:.0f}")
        
        st.markdown("---")
        
        # Filters
        col_f1, col_f2 = st.columns([2, 1])
        with col_f1:
            top_n = st.slider("Hiển thị top danh mục:", 5, 100, 30, key="docs_slider")
        with col_f2:
            min_docs = st.number_input("Tối thiểu documents:", 1, 10000, 1, key="docs_min")
        
        # Filter
        df_filtered = df_docs[df_docs['doc_count'] >= min_docs].head(top_n)
        
        # Chart
        st.subheader(f"Top {len(df_filtered)} danh mục theo số lượng documents")
        st.bar_chart(df_filtered.set_index('category')['doc_count'], height=500)
        
        # Table
        st.subheader("Bảng chi tiết")
        st.dataframe(
            df_filtered.rename(columns={'category': 'Danh mục', 'doc_count': 'Số Documents'}),
            use_container_width=True,
            height=400
        )
        
        # Download
        csv = df_filtered.to_csv(index=False, encoding='utf-8-sig')
        st.download_button("Tải CSV", csv, "category_docs.csv", "text/csv")
    
    else:
        st.info("Chưa có dữ liệu")

# ==================== TAB 2: CATEGORY KEYWORDS ====================
with tab2:
    st.header("Keywords by Category")
    st.markdown("Phân tích từ khóa công nghệ xuất hiện trong các danh mục")
    
    @st.cache_data(ttl=3600)
    def load_category_keywords():
        try:
            query = {
                "size": 10000,
                "_source": ["category", "keyword", "count"],
                "sort": [{"count": "desc"}]
            }
            response = es.search(index="wiki_cat_kwlist", body=query)
            
            data = []
            for hit in response["hits"]["hits"]:
                s = hit["_source"]
                data.append({
                    'category': s['category'],
                    'keyword': s['keyword'],
                    'count': s['count']
                })
            
            return pd.DataFrame(data) if data else None, None
            
        except Exception as e:
            return None, str(e)
    
    with st.spinner("Loading category keywords data..."):
        df_kw, error_kw = load_category_keywords()
    
    if error_kw:
        st.error(f"Lỗi: {error_kw}")
    elif df_kw is not None and len(df_kw) > 0:
        
        # Overview metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Danh mục có keywords", f"{df_kw['category'].nunique():,}")
        with col2:
            st.metric("Từ khóa unique", f"{df_kw['keyword'].nunique()}")
        with col3:
            st.metric("Tổng xuất hiện", f"{df_kw['count'].sum():,}")
        
        st.markdown("---")
        
        # === SECTION 1: Top Keywords Overall ===
        st.subheader("Top từ khóa (tất cả danh mục)")
        
        top_keywords = df_kw.groupby('keyword')['count'].sum().sort_values(ascending=False)
        
        col_chart1, col_chart2 = st.columns([2, 1])
        
        with col_chart1:
            st.bar_chart(top_keywords.head(10), height=300)
        
        with col_chart2:
            st.dataframe(
                top_keywords.head(10).reset_index().rename(columns={'keyword': 'Từ khóa', 'count': 'Tổng'}),
                use_container_width=True,
                height=300
            )
        
        st.markdown("---")
        
        # === SECTION 2: Top Categories by Keyword Activity ===
        st.subheader("Danh mục hoạt động nhất")
        
        cat_activity = df_kw.groupby('category')['count'].sum().sort_values(ascending=False).head(20)
        
        st.bar_chart(cat_activity, height=400)
        
        st.markdown("---")
        
        # === SECTION 3: Analyze by Selected Category ===
        st.subheader("Phân tích theo danh mục")
        
        categories = sorted(df_kw['category'].unique())
        
        col_sel1, col_sel2 = st.columns([3, 1])
        
        with col_sel1:
            selected_cat = st.selectbox("Chọn danh mục:", categories, key="cat_selector")
        
        with col_sel2:
            top_n_kw = st.slider("Top keywords:", 5, 30, 15, key="kw_slider")
        
        if selected_cat:
            df_cat = df_kw[df_kw['category'] == selected_cat].sort_values('count', ascending=False)
            
            # Metrics for selected category
            col_m1, col_m2, col_m3 = st.columns(3)
            with col_m1:
                st.metric("Danh mục", selected_cat)
            with col_m2:
                st.metric("Từ khóa tìm thấy", len(df_cat))
            with col_m3:
                st.metric("Tổng xuất hiện", df_cat['count'].sum())
            
            # Chart
            df_cat_top = df_cat.head(top_n_kw)
            st.bar_chart(df_cat_top.set_index('keyword')['count'], height=400)
            
            # Table
            st.dataframe(
                df_cat_top.rename(columns={'keyword': 'Từ khóa', 'count': 'Số lần'}),
                use_container_width=True
            )
            
            # Download
            csv_cat = df_cat.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                "Tải CSV danh mục",
                csv_cat,
                f"keywords_{selected_cat[:30]}.csv",
                "text/csv",
                key="download_cat_kw"
            )
        
        st.markdown("---")
        
        # === SECTION 4: Keyword Distribution ===
        st.subheader("Phân bố từ khóa theo danh mục")
        
        selected_kw = st.selectbox(
            "Chọn từ khóa:",
            sorted(df_kw['keyword'].unique()),
            key="kw_dist_selector"
        )
        
        if selected_kw:
            df_kw_dist = df_kw[df_kw['keyword'] == selected_kw].sort_values('count', ascending=False).head(20)
            
            st.markdown(f"**Top 20 danh mục chứa '{selected_kw}':**")
            st.bar_chart(df_kw_dist.set_index('category')['count'], height=400)
            
            st.dataframe(
                df_kw_dist.rename(columns={'category': 'Danh mục', 'count': 'Số lần xuất hiện'}),
                use_container_width=True
            )
    
    else:
        st.info("Chưa có dữ liệu keywords")

st.markdown("---")
st.caption("Category Analysis | Powered by Hadoop + Elasticsearch")
