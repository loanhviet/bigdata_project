#!/usr/bin/env python3
"""
Search Page - Tìm kiếm toàn văn Wikipedia
"""
import streamlit as st
from datetime import datetime
from utils.es_client import get_es_client

# Page config
st.set_page_config(
    page_title="Search - Wikipedia",
    page_icon="🔍",
    layout="wide"
)

# Get ES client
es = get_es_client()

# Header
st.title("Tìm kiếm Wikipedia")
st.markdown("---")

# Input form
col1, col2, col3 = st.columns([4, 2, 1])

with col1:
    query_text = st.text_input(
        "Nhập từ khóa tìm kiếm:",
        placeholder="Ví dụ: Hà Nội, Hồ Chí Minh, lịch sử Việt Nam...",
        key="search_input"
    )

with col2:
    search_type = st.selectbox(
        "Loại tìm kiếm:",
        ["Tiêu đề + Nội dung", "Chỉ tiêu đề", "Chỉ nội dung"],
        key="search_type"
    )

with col3:
    num_results = st.number_input(
        "Số kết quả:",
        min_value=5,
        max_value=100,
        value=10,
        step=5,
        key="num_results"
    )

# Advanced options
with st.expander("Tùy chọn nâng cao"):
    col_adv1, col_adv2 = st.columns(2)
    with col_adv1:
        use_fuzzy = st.checkbox("Tìm kiếm mờ (fuzzy)", value=True, help="Cho phép tìm các từ tương tự")
    with col_adv2:
        highlight = st.checkbox("Highlight từ khóa", value=True, help="Đánh dấu từ khóa trong kết quả")

# Search button
if st.button("Tìm kiếm", type="primary", use_container_width=True):
    if query_text:
        with st.spinner("Đang tìm kiếm..."):
            try:
                # Xây dựng query
                if search_type == "Tiêu đề + Nội dung":
                    query_body = {
                        "multi_match": {
                            "query": query_text,
                            "fields": ["title^3", "text"],
                            "fuzziness": "AUTO" if use_fuzzy else "0"
                        }
                    }
                elif search_type == "Chỉ tiêu đề":
                    query_body = {
                        "match": {
                            "title": {
                                "query": query_text,
                                "fuzziness": "AUTO" if use_fuzzy else "0"
                            }
                        }
                    }
                else:
                    query_body = {
                        "match": {
                            "text": {
                                "query": query_text,
                                "fuzziness": "AUTO" if use_fuzzy else "0"
                            }
                        }
                    }
                
                # Search request
                search_request = {
                    "query": query_body,
                    "size": num_results
                }
                
                # Add highlight
                if highlight:
                    search_request["highlight"] = {
                        "fields": {
                            "title": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
                            "text": {
                                "fragment_size": 200,
                                "number_of_fragments": 3,
                                "pre_tags": ["<mark>"],
                                "post_tags": ["</mark>"]
                            }
                        }
                    }
                
                # Execute search
                response = es.search(index="wiki_docs", body=search_request)
                
                hits = response["hits"]["hits"]
                total = response["hits"]["total"]["value"]
                took = response["took"]
                
                st.success(f"Tìm thấy **{total:,}** kết quả trong **{took}ms**")
                st.markdown("---")
                
                # Display results
                if hits:
                    for i, hit in enumerate(hits, 1):
                        source = hit["_source"]
                        score = hit["_score"]
                        
                        with st.container():
                            st.markdown(f"### {i}. {source.get('title', 'Không có tiêu đề')}")
                            
                            # Metadata
                            meta_cols = st.columns(4)
                            with meta_cols[0]:
                                st.caption(f"Điểm: {score:.2f}")
                            with meta_cols[1]:
                                timestamp = source.get('timestamp', 'N/A')
                                if timestamp and timestamp != 'N/A':
                                    try:
                                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                        timestamp = dt.strftime('%Y-%m-%d %H:%M')
                                    except:
                                        pass
                                st.caption(f"Ngày: {timestamp}")
                            with meta_cols[2]:
                                num_cats = len(source.get('categories', []))
                                st.caption(f"Danh mục: {num_cats}")
                            with meta_cols[3]:
                                page_id = source.get('page_id', 'N/A')
                                st.caption(f"ID: {page_id}")
                            
                            # Highlight or snippet
                            if highlight and "highlight" in hit:
                                if "title" in hit["highlight"]:
                                    st.markdown(f"**Tiêu đề khớp:** {hit['highlight']['title'][0]}", unsafe_allow_html=True)
                                
                                if "text" in hit["highlight"]:
                                    st.markdown("**Nội dung khớp:**")
                                    for fragment in hit["highlight"]["text"][:2]:
                                        st.markdown(f"> ...{fragment}...", unsafe_allow_html=True)
                            else:
                                text = source.get('text', '')
                                if text:
                                    snippet = text[:300] + "..." if len(text) > 300 else text
                                    st.markdown(f"**Nội dung:** {snippet}")
                            
                            # Categories
                            categories = source.get('categories', [])
                            if categories:
                                cats_display = categories[:8]
                                remaining = len(categories) - len(cats_display)
                                cats_str = ", ".join(f"`{c}`" for c in cats_display)
                                if remaining > 0:
                                    cats_str += f" _(+{remaining} danh mục khác)_"
                                st.markdown(f"**Danh mục:** {cats_str}")
                            
                            st.markdown("---")
                else:
                    st.info("Không tìm thấy kết quả phù hợp.")
                    
            except Exception as e:
                st.error(f"Lỗi: {str(e)}")
                st.info("Đảm bảo Elasticsearch đang chạy và index 'wiki_docs' đã được tạo.")
    else:
        st.warning("Vui lòng nhập từ khóa tìm kiếm!")
