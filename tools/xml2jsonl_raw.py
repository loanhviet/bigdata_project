import json
import xml.etree.ElementTree as ET


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def fix_mojibake(s: str) -> str:
    """
    Fix trường hợp UTF-8 bị đọc nhầm latin1/cp1252 (mojibake), ví dụ:
    'Trang ChÃ­nh' -> 'Trang Chính'
    """
    if not s:
        return ""
    if "Ã" in s or "Â" in s:
        try:
            return s.encode("latin1").decode("utf-8")
        except Exception:
            return s
    return s


def iter_pages(xml_path: str):
    # Streaming parse: không load cả file vào RAM
    context = ET.iterparse(xml_path, events=("end",))
    for _, elem in context:
        if strip_ns(elem.tag) != "page":
            continue

        title = elem.findtext(".//{*}title") or ""
        ns = elem.findtext(".//{*}ns") or ""
        page_id = elem.findtext(".//{*}id") or ""
        timestamp = elem.findtext(".//{*}revision/{*}timestamp") or ""
        text = elem.findtext(".//{*}revision/{*}text") or ""

        # bỏ redirect + chỉ lấy ns=0 (bài viết chính)
        if elem.find(".//{*}redirect") is not None or ns != "0":
            elem.clear()
            continue

        yield {
            "page_id": page_id,
            "title": fix_mojibake(title),
            "timestamp": timestamp,
            "wikitext": fix_mojibake(text),
        }

        elem.clear()


if __name__ == "__main__":
    in_path = r"D:\bd-project\data\viwiki-latest-pages-articles.xml"
    out_path = r"D:\bd-project\data\wiki_raw.jsonl"

    with open(out_path, "w", encoding="utf-8") as f:
        for doc in iter_pages(in_path):
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    print("Wrote:", out_path)
