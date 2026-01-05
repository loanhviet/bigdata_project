# Elasticsearch Setup Guide

## 1. Cài đặt Elasticsearch

### Windows:

```bash
# Download Elasticsearch 8.x từ https://www.elastic.co/downloads/elasticsearch
# Giải nén và chạy:
cd elasticsearch-8.x.x\bin
elasticsearch.bat
```

### Kiểm tra Elasticsearch đang chạy:

```bash
curl http://localhost:9200
```

## 2. Cài đặt Python dependencies

```bash
pip install -r requirements.txt
```

## 3. Tạo Index

```bash
python create_index.py
```

Output:

```
Đã xóa index cũ: wiki_docs
✓ Đã tạo index: wiki_docs
  - Shards: 3
  - Replicas: 1
  - Analyzer: Vietnamese (custom)
```

## 4. Đẩy dữ liệu từ HDFS lên Elasticsearch

```bash
python ingest_wiki_docs.py
```

Output:

```
============================================================
INGEST WIKIPEDIA DATA TO ELASTICSEARCH
============================================================
Đang đọc dữ liệu từ HDFS: /data/wiki/clean/docs
  Đã xử lý 1000 documents...
  Đã xử lý 2000 documents...
  ...

📊 Tổng kết:
  - Documents hợp lệ: 10523
  - Lỗi: 0

✓ Hoàn thành!
  - Thành công: 10523
  - Thất bại: 0
  - Tổng documents trong ES: 10523
```

## 5. Chạy Streamlit App

```bash
cd ..
streamlit run streamlit_app.py
```

Giao diện sẽ mở tại: http://localhost:8501

## Các lệnh hữu ích

### Kiểm tra số lượng documents:

```bash
curl -X GET "localhost:9200/wiki_docs/_count"
```

### Xem mapping:

```bash
curl -X GET "localhost:9200/wiki_docs/_mapping"
```

### Xóa index:

```bash
curl -X DELETE "localhost:9200/wiki_docs"
```

### Test tìm kiếm:

```bash
curl -X GET "localhost:9200/wiki_docs/_search?q=Hà Nội&size=5"
```
