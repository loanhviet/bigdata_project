#!/usr/bin/env python3
"""
Script đẩy dữ liệu Wikipedia từ HDFS lên Elasticsearch
"""
import json
import subprocess
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch(["http://localhost:9200"])

def ingest_wiki_docs(hdfs_path="/data/wiki/clean/docs", batch_size=500):
    """
    Đẩy dữ liệu từ HDFS lên Elasticsearch
    
    Args:
        hdfs_path: Đường dẫn HDFS chứa data clean
        batch_size: Số document mỗi batch
    """
    cmd = f"hdfs dfs -cat {hdfs_path}/part-*"
    print(f"Đang đọc dữ liệu từ HDFS: {hdfs_path}")
    
    try:
        proc = subprocess.Popen(
            cmd, 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            encoding="utf-8"
        )
        
        def gen_docs():
            count = 0
            errors = 0
            
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    doc = json.loads(line)
                    count += 1
                    
                    yield {
                        "_index": "wiki_docs",
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
                        print(f"  Đã xử lý {count} documents...")
                        
                except json.JSONDecodeError as e:
                    errors += 1
                    if errors <= 5:
                        print(f"  ⚠ Lỗi JSON (dòng {count + errors}): {str(e)[:50]}...")
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"  ⚠ Lỗi khác: {str(e)[:50]}...")
            
            print(f"\n📊 Tổng kết:")
            print(f"  - Documents hợp lệ: {count}")
            print(f"  - Lỗi: {errors}")
        
        # Bulk insert vào Elasticsearch
        success, failed = helpers.bulk(
            es, 
            gen_docs(), 
            chunk_size=batch_size,
            raise_on_error=False,
            stats_only=True
        )
        
        print(f"\n✓ Hoàn thành!")
        print(f"  - Thành công: {success}")
        print(f"  - Thất bại: {failed}")
        
        # Kiểm tra số lượng documents trong index
        count = es.count(index="wiki_docs")["count"]
        print(f"  - Tổng documents trong ES: {count}")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi HDFS command: {e}")
    except Exception as e:
        print(f"❌ Lỗi: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("INGEST WIKIPEDIA DATA TO ELASTICSEARCH")
    print("=" * 60)
    ingest_wiki_docs()
