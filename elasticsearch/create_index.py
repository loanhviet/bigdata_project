#!/usr/bin/env python3
"""
Script tạo index Elasticsearch cho Wikipedia documents
"""
from elasticsearch import Elasticsearch

es = Elasticsearch(["http://localhost:9200"])

def create_wiki_index():
    index_name = "wiki_docs"
    
    # Xóa index cũ nếu tồn tại
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Đã xóa index cũ: {index_name}")
    
    # Tạo index mới
    index_body = {
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
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "timestamp": {"type": "date", "format": "iso8601"},
                "categories": {"type": "keyword"},
                "text": {
                    "type": "text",
                    "analyzer": "vietnamese"
                }
            }
        }
    }
    
    es.indices.create(index=index_name, body=index_body)
    print(f"✓ Đã tạo index: {index_name}")
    print(f"  - Shards: 3")
    print(f"  - Replicas: 1")
    print(f"  - Analyzer: Vietnamese (custom)")

if __name__ == "__main__":
    try:
        create_wiki_index()
    except Exception as e:
        print(f"❌ Lỗi: {e}")
