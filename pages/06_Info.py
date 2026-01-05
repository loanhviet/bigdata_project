#!/usr/bin/env python3
"""
Info Page - Thông tin hệ thống
"""
import streamlit as st
from utils.es_client import get_es_client

# Page config
st.set_page_config(
    page_title="Info - Wikipedia",
    page_icon="ℹ️",
    layout="wide"
)

# Get ES client
es = get_es_client()

# Header
st.title("Thông tin hệ thống")
st.markdown("---")

# System info
st.markdown("""
### Mục tiêu đề tài

**Xây dựng hệ thống tìm kiếm và phân tích văn bản sử dụng Hadoop MapReduce và Elasticsearch**

### Kiến trúc hệ thống

```
┌─────────────┐
│   HDFS      │ ← Lưu trữ Wikipedia XML dump (10GB+)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  MapReduce  │ ← Xử lý & phân tích dữ liệu
└──────┬──────┘
       │
       ▼
┌──────────────────────────────────┐
│      Elasticsearch               │ ← Full-text search + Analytics
│  ┌──────────────────────────┐   │
│  │ wiki_docs                │   │ ← Documents gốc
│  │ wiki_wordcount           │   │ ← Tần suất từ khóa
│  │ wiki_trend               │   │ ← Xu hướng theo thời gian
│  │ wiki_cat_kwlist          │   │ ← Từ khóa theo danh mục
│  │ wiki_cat_docs            │   │ ← Documents theo danh mục
│  └──────────────────────────┘   │
└──────┬───────────────────────────┘
       │
       ▼
┌─────────────┐
│  Streamlit  │ ← Web UI
└─────────────┘
```

### MapReduce Jobs

**1. Mapper Clean (`mapper_clean.py`)**
- Làm sạch dữ liệu Wikipedia XML
- Parse và extract: title, text, categories, timestamp
- Output: JSONL format

**2. WordCount (`mapper_wc.py`)**
- Đếm tần suất từ khóa tiếng Việt
- Loại bỏ stopwords
- Output: `word \\t count`

**3. Trend Analysis (`mapper_trend_kwlist.py`)**
- Phân tích xu hướng từ khóa theo thời gian
- Format: `YYYY-MM-keyword`
- Output: `date-keyword \\t count`

**4. Category Keywords (`mapper_cat_kwlist.py`)**
- Từ khóa trong mỗi danh mục
- Output: `category-keyword \\t count`

**5. Category Docs (`mapper_cat_docs.py`)**
- Số lượng documents theo danh mục
- Output: `category \\t doc_count`

### Tech Stack

| Công nghệ | Version | Mục đích |
|-----------|---------|----------|
| **Hadoop** | 3.x | Distributed storage & processing |
| **Elasticsearch** | 8.x | Full-text search engine |
| **Python** | 3.x | MapReduce scripts & processing |
| **Streamlit** | Latest | Web UI framework |

### Nguồn dữ liệu

- **Nguồn**: Wikipedia tiếng Việt dump file
- **Format**: XML compressed (bz2)
- **Số lượng**: 100,000+ articles
- **Dung lượng**: ~10GB raw data

### Workflow

1. **Download Wikipedia dump**
   ```bash
   wget https://dumps.wikimedia.org/viwiki/latest/viwiki-latest-pages-articles.xml.bz2
   ```

2. **Upload to HDFS**
   ```bash
   hdfs dfs -put viwiki-latest-pages-articles.xml /data/wiki/raw/
   ```

3. **Run MapReduce jobs**
   ```bash
   # Clean data
   hadoop jar hadoop-streaming.jar \\
     -mapper mapper_clean.py \\
     -input /data/wiki/raw \\
     -output /data/wiki/clean
   
   # WordCount
   hadoop jar hadoop-streaming.jar \\
     -mapper mapper_wc.py \\
     -reducer reducer_sum.py \\
     -input /data/wiki/clean \\
     -output /data/wiki/mr/wordcount
   
   # Trend analysis, category analysis...
   ```

4. **Index to Elasticsearch**
   ```bash
   python elasticsearch/create_index.py
   python elasticsearch/ingest_wiki_docs.py
   python elasticsearch/index_all_data.py
   ```

5. **Launch Streamlit UI**
   ```bash
   streamlit run streamlit_app.py
   ```

### Nhóm thực hiện

Đề tài Big Data - Học kỳ 2024-2025

""")

st.markdown("---")

# System status
st.subheader("Trạng thái hệ thống")

col1, col2 = st.columns(2)

with col1:
    try:
        info = es.info()
        st.success("**Elasticsearch**")
        st.code(f"""
Version: {info['version']['number']}
Cluster: {info['cluster_name']}
Name: {info['name']}
        """)
    except Exception as e:
        st.error("**Elasticsearch**")
        st.code(f"Error: {str(e)}")

with col2:
    st.info("**Indices Status**")
    
    indices = ["wiki_docs", "wiki_wordcount", "wiki_trend", "wiki_cat_kwlist", "wiki_cat_docs"]
    
    for idx in indices:
        try:
            if es.indices.exists(index=idx):
                count = es.count(index=idx)['count']
                st.text(f"✓ {idx}: {count:,} docs")
            else:
                st.text(f"⚠ {idx}: Not created")
        except:
            st.text(f"✗ {idx}: Error")

st.markdown("---")

# Quick stats
st.subheader("Thống kê nhanh")

try:
    col_stat1, col_stat2, col_stat3 = st.columns(3)
    
    with col_stat1:
        if es.indices.exists(index="wiki_docs"):
            count = es.count(index="wiki_docs")['count']
            st.metric("Total Documents", f"{count:,}")
        else:
            st.metric("Total Documents", "N/A")
    
    with col_stat2:
        if es.indices.exists(index="wiki_wordcount"):
            count = es.count(index="wiki_wordcount")['count']
            st.metric("Unique Words", f"{count:,}")
        else:
            st.metric("Unique Words", "N/A")
    
    with col_stat3:
        if es.indices.exists(index="wiki_cat_docs"):
            count = es.count(index="wiki_cat_docs")['count']
            st.metric("Categories", f"{count:,}")
        else:
            st.metric("Categories", "N/A")
            
except Exception as e:
    st.error(f"Không thể lấy thống kê: {str(e)}")

st.markdown("---")
st.caption("Wikipedia Search System | Powered by Hadoop + Elasticsearch + Streamlit")
