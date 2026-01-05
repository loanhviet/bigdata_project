#!/usr/bin/env python3
"""
Index all data to Elasticsearch:
1. Wikipedia documents (from cleaned data)
2. MapReduce results (wordcount, trends, categories)
"""
from elasticsearch import Elasticsearch, helpers
import subprocess
import sys
import re
import json

# Kết nối ES
es = Elasticsearch(['http://localhost:9200'])

print("Connected to Elasticsearch")

# Đọc keywords từ file
def load_keywords(filepath='mapreduce/keywords.txt'):
    keywords = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip()]
    except:
        keywords = ['internet', 'mạng', 'website', 'google', 'facebook', 
                   'phần', 'mềm', 'dữ', 'liệu', 'bảo', 'mật']
    # Sort by length descending để match từ dài trước
    keywords.sort(key=len, reverse=True)
    return keywords

KEYWORDS = load_keywords()

# ============== INDEX 0: WIKI DOCS (Full-text search) ==============
def index_wiki_docs():
    index_name = "wiki_docs"
    
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted old index: {index_name}")
    
    es.indices.create(index=index_name, body={
        "settings": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "analysis": {
                "analyzer": {
                    "vietnamese": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "page_id": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "vietnamese",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "timestamp": {"type": "date", "format": "iso8601"},
                "categories": {"type": "keyword"},
                "text": {"type": "text", "analyzer": "vietnamese"}
            }
        }
    })
    
    print(f"Created index: {index_name}")
    
    def read_data():
        cmd = "hdfs dfs -cat /data/wiki/clean/docs/part-*"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        
        count = 0
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                try:
                    doc = json.loads(line)
                    count += 1
                    yield {
                        "_index": index_name,
                        "_id": doc.get("page_id", count),
                        "_source": {
                            "page_id": doc.get("page_id"),
                            "title": doc.get("title"),
                            "timestamp": doc.get("timestamp"),
                            "categories": doc.get("categories", []),
                            "text": doc.get("text", "")
                        }
                    }
                    if count % 1000 == 0:
                        print(f"  Processed {count} documents...")
                except:
                    pass
    
    success, failed = helpers.bulk(es, read_data(), raise_on_error=False, stats_only=True)
    print(f"Indexed {success} documents to {index_name}")
    if failed:
        print(f"Failed: {failed}")

# ============== INDEX 1: WORDCOUNT ==============
def index_wordcount():
    index_name = "wiki_wordcount"
    
    # Xóa index cũ nếu có
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted old index: {index_name}")
    
    # Tạo index mới
    es.indices.create(index=index_name, body={
        "mappings": {
            "properties": {
                "word": {"type": "keyword"},
                "count": {"type": "integer"}
            }
        }
    })
    
    print(f"Created index: {index_name}")
    
    # Đọc dữ liệu
    def read_data():
        cmd = "hdfs dfs -cat /data/wiki/mr/wordcount/part-*"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                parts = line.split('\t')
                if len(parts) == 2:
                    yield {
                        "_index": index_name,
                        "_source": {
                            "word": parts[0],
                            "count": int(parts[1])
                        }
                    }
    
    # Bulk index
    success, failed = helpers.bulk(es, read_data(), raise_on_error=False, stats_only=True)
    print(f"Indexed {success} documents to {index_name}")
    if failed:
        print(f"Failed: {failed}")


# ============== INDEX 2: TREND_KWLIST ==============
def index_trend_kwlist():
    index_name = "wiki_trend"
    
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted old index: {index_name}")
    
    es.indices.create(index=index_name, body={
        "mappings": {
            "properties": {
                "year": {"type": "keyword"},
                "month": {"type": "keyword"},
                "keyword": {"type": "keyword"},
                "count": {"type": "integer"},
                "date": {"type": "date", "format": "yyyy-MM"}
            }
        }
    })
    
    print(f"Created index: {index_name}")
    
    def read_data():
        cmd = "hdfs dfs -cat /data/wiki/mr/trend/part-*"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        
        # Pattern: 2025-11website → year=2025, month=11, keyword=website
        pattern = re.compile(r'^(\d{4})-(\d{2})(.+)$')
        
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                parts = line.split('\t')
                if len(parts) == 2:
                    match = pattern.match(parts[0])
                    if match:
                        year = match.group(1)
                        month = match.group(2)
                        keyword = match.group(3)
                        
                        yield {
                            "_index": index_name,
                            "_source": {
                                "year": year,
                                "month": month,
                                "keyword": keyword,
                                "count": int(parts[1]),
                                "date": f"{year}-{month}"
                            }
                        }
    
    success, failed = helpers.bulk(es, read_data(), raise_on_error=False, stats_only=True)
    print(f"Indexed {success} documents to {index_name}")
    if failed:
        print(f"Failed: {failed}")


# ============== INDEX 3: CAT_KWLIST ==============
def index_cat_kwlist():
    index_name = "wiki_cat_kwlist"
    
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted old index: {index_name}")
    
    es.indices.create(index=index_name, body={
        "mappings": {
            "properties": {
                "category": {"type": "keyword"},
                "keyword": {"type": "keyword"},
                "count": {"type": "integer"}
            }
        }
    })
    
    print(f"Created index: {index_name}")
    
    def read_data():
        cmd = "hdfs dfs -cat /data/wiki/mr/category_keyword/part-*"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                parts = line.split('\t')
                if len(parts) == 2:
                    cat_keyword = parts[0]
                    
                    # Tìm keyword khớp từ cuối string (Googlegoogle → Google + google)
                    matched_keyword = None
                    for kw in KEYWORDS:
                        if cat_keyword.endswith(kw):
                            matched_keyword = kw
                            break
                    
                    if matched_keyword:
                        category = cat_keyword[:-len(matched_keyword)]
                        
                        if category:  # Đảm bảo có category
                            yield {
                                "_index": index_name,
                                "_source": {
                                    "category": category,
                                    "keyword": matched_keyword,
                                    "count": int(parts[1])
                                }
                            }
    
    success, failed = helpers.bulk(es, read_data(), raise_on_error=False, stats_only=True)
    print(f"Indexed {success} documents to {index_name}")
    if failed:
        print(f"Failed: {failed}")


# ============== INDEX 4: CAT_DOCS ==============
def index_cat_docs():
    index_name = "wiki_cat_docs"
    
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted old index: {index_name}")
    
    es.indices.create(index=index_name, body={
        "mappings": {
            "properties": {
                "category": {"type": "keyword"},
                "doc_count": {"type": "integer"}
            }
        }
    })
    
    print(f"Created index: {index_name}")
    
    def read_data():
        cmd = "hdfs dfs -cat /data/wiki/mr/category_stats/part-*"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                parts = line.split('\t')
                if len(parts) == 2:
                    yield {
                        "_index": index_name,
                        "_source": {
                            "category": parts[0],
                            "doc_count": int(parts[1])
                        }
                    }
    
    success, failed = helpers.bulk(es, read_data(), raise_on_error=False, stats_only=True)
    print(f"Indexed {success} documents to {index_name}")
    if failed:
        print(f"Failed: {failed}")


# ============== MAIN ==============
if __name__ == "__main__":
    print("=" * 60)
    print("INDEXING ALL DATA TO ELASTICSEARCH")
    print("=" * 60)
    
    try:
        print("\n[0/5] Indexing Wikipedia documents...")
        index_wiki_docs()
        
        print("\n[1/5] Indexing WordCount...")
        index_wordcount()
        
        print("\n[2/5] Indexing Trend Keywords...")
        index_trend_kwlist()
        
        print("\n[3/5] Indexing Category Keywords...")
        index_cat_kwlist()
        
        print("\n[4/5] Indexing Category Documents...")
        index_cat_docs()
        
        print("\n" + "=" * 60)
        print("ALL DONE! Successfully indexed all data to Elasticsearch")
        print("=" * 60)
        
        # Kiểm tra số lượng documents
        print("\nIndex Statistics:")
        indices = ["wiki_docs", "wiki_wordcount", "wiki_trend", "wiki_cat_kwlist", "wiki_cat_docs"]
        for idx in indices:
            try:
                count = es.count(index=idx)['count']
                print(f"  - {idx}: {count:,} documents")
            except:
                print(f"  - {idx}: (not found)")
    
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        sys.exit(1)
