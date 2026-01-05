#!/usr/bin/env python3
"""
Trend Analysis Page - Phân tích xu hướng từ khóa
"""
import streamlit as st
import pandas as pd
from utils.es_client import get_es_client

# Page config
st.set_page_config(
    page_title="Trend Analysis - Wikipedia",
    page_icon="📈",
    layout="wide"
)

# Get ES client
es = get_es_client()

# Header
st.title("Phân tích xu hướng từ khóa")
st.markdown("---")

@st.cache_data(ttl=3600)
def load_trend_data():
    """Query dữ liệu trend từ Elasticsearch"""
    try:
        query = {
            "size": 10000,
            "_source": ["year", "month", "keyword", "count", "date"],
            "sort": [{"date": "asc"}]
        }
        
        response = es.search(index="wiki_trend", body=query)
        
        data = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            data.append({
                'month': source['date'],
                'keyword': source['keyword'],
                'count': source['count']
            })
        
        if not data:
            return None, "Không có dữ liệu"
        
        df = pd.DataFrame(data)
        df['month'] = pd.to_datetime(df['month'])
        
        return df, None
        
    except Exception as e:
        return None, str(e)

# Load data
with st.spinner("Đang tải dữ liệu..."):
    df_trend, error = load_trend_data()

if error:
    st.error(f"Lỗi: {error}")
    st.info("Đảm bảo đã chạy: `python elasticsearch/index_all_data.py`")
elif df_trend is not None and len(df_trend) > 0:
    
    # Overview metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Tổng từ khóa", f"{df_trend['keyword'].nunique():,}")
    with col2:
        st.metric("Số tháng", f"{df_trend['month'].nunique()}")
    with col3:
        st.metric("Tổng xuất hiện", f"{df_trend['count'].sum():,}")
    with col4:
        avg = df_trend.groupby('month')['count'].sum().mean()
        st.metric("TB/tháng", f"{avg:,.0f}")
    
    st.markdown("---")
    
    # Top keywords
    st.subheader("Top 20 từ khóa phổ biến nhất")
    
    top_keywords = df_trend.groupby('keyword')['count'].sum().sort_values(ascending=False).head(20)
    
    col_chart1, col_chart2 = st.columns([2, 1])
    
    with col_chart1:
        st.bar_chart(top_keywords, height=400)
    
    with col_chart2:
        st.dataframe(
            top_keywords.reset_index().rename(columns={'keyword': 'Từ khóa', 'count': 'Số lần'}),
            use_container_width=True,
            height=400
        )
    
    st.markdown("---")
    
    # Keyword comparison
    st.subheader("So sánh xu hướng từ khóa")
    
    available_keywords = top_keywords.head(50).index.tolist()
    
    col_filter1, col_filter2 = st.columns([3, 1])
    
    with col_filter1:
        selected_keywords = st.multiselect(
            "Chọn từ khóa (tối đa 5):",
            options=available_keywords,
            default=available_keywords[:3] if len(available_keywords) >= 3 else available_keywords,
            max_selections=5
        )
    
    with col_filter2:
        chart_type = st.radio("Loại biểu đồ:", ["Line", "Area"], horizontal=True)
    
    if selected_keywords:
        df_selected = df_trend[df_trend['keyword'].isin(selected_keywords)]
        
        df_pivot = df_selected.pivot_table(
            index='month',
            columns='keyword',
            values='count',
            fill_value=0
        ).sort_index()
        
        if chart_type == "Line":
            st.line_chart(df_pivot, height=400)
        else:
            st.area_chart(df_pivot, height=400)
        
        st.markdown("---")
        
        # Detailed stats
        st.subheader("Thống kê chi tiết")
        
        cols = st.columns(len(selected_keywords))
        
        for idx, keyword in enumerate(selected_keywords):
            df_kw = df_selected[df_selected['keyword'] == keyword].sort_values('month')
            
            with cols[idx]:
                st.markdown(f"**{keyword}**")
                
                total = df_kw['count'].sum()
                st.metric("Tổng", f"{total:,}")
                
                avg = df_kw['count'].mean()
                st.metric("TB/tháng", f"{avg:.0f}")
                
                max_val = df_kw['count'].max()
                max_month = df_kw[df_kw['count'] == max_val].iloc[0]['month'].strftime('%Y-%m')
                st.metric("Cao nhất", f"{max_val:,}", delta=f"{max_month}")
                
                # Growth indicator
                if len(df_kw) >= 2:
                    first_avg = df_kw.head(3)['count'].mean()
                    last_avg = df_kw.tail(3)['count'].mean()
                    growth = ((last_avg - first_avg) / first_avg * 100) if first_avg > 0 else 0
                    
                    if growth > 10:
                        st.success(f"Tăng {growth:.1f}%")
                    elif growth < -10:
                        st.error(f"Giảm {growth:.1f}%")
                    else:
                        st.info(f"Ổn định {growth:.1f}%")
        
        st.markdown("---")
        
        # Data table
        st.subheader("Bảng dữ liệu")
        
        df_display = df_selected.copy()
        df_display['month'] = df_display['month'].dt.strftime('%Y-%m')
        df_display = df_display.sort_values(['keyword', 'month'])
        df_display = df_display.rename(columns={
            'month': 'Tháng',
            'keyword': 'Từ khóa',
            'count': 'Số lần'
        })
        
        st.dataframe(df_display, use_container_width=True, height=400)
        
        # Download
        csv = df_display.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "Tải CSV",
            csv,
            f"trend_{'-'.join(selected_keywords[:3])}.csv",
            "text/csv"
        )
    else:
        st.info("Chọn ít nhất 1 từ khóa để xem xu hướng")
    
    st.markdown("---")
    
    # Monthly activity
    st.subheader("Hoạt động theo tháng")
    
    monthly_total = df_trend.groupby('month')['count'].sum().reset_index()
    monthly_total['month_str'] = monthly_total['month'].dt.strftime('%Y-%m')
    
    st.bar_chart(monthly_total.set_index('month')['count'], height=250)
    
    top_months = monthly_total.nlargest(5, 'count')
    
    st.markdown("**Top 5 tháng hoạt động nhất:**")
    for idx, row in top_months.iterrows():
        st.text(f"  {row['month_str']}: {row['count']:,} lượt")
else:
    st.info("Chưa có dữ liệu. Chạy `python elasticsearch/index_all_data.py`")
