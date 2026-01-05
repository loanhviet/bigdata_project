import sys, json, re

sys.stdin.reconfigure(encoding="utf-8", errors="replace")
sys.stdout.reconfigure(encoding="utf-8")

cat_re = re.compile(r"\[\[\s*Thể\s*loại\s*:\s*([^\]|]+)", re.I)

def clean_wikitext(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<!--.*?-->", " ", text, flags=re.S)
    text = re.sub(r"<ref.*?>.*?</ref>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<ref[^>/]*/?>", " ", text, flags=re.I)
    text = re.sub(r"</?[^>]+>", " ", text)

    # bỏ template đơn giản (gỡ lớp nông)
    for _ in range(5):
        new = re.sub(r"\{\{[^{}]*\}\}", " ", text)
        if new == text:
            break
        text = new

    # wiki links
    text = re.sub(r"\[\[([^|\]]+)\|([^\]]+)\]\]", r"\2", text)
    text = re.sub(r"\[\[([^\]]+)\]\]", r"\1", text)

    text = text.replace("''", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        doc = json.loads(line)
    except Exception:
        continue

    wikitext = doc.get("wikitext", "") or ""
    categories = sorted({c.strip() for c in cat_re.findall(wikitext) if c.strip()})

    out = {
        "page_id": doc.get("page_id"),
        "title": doc.get("title"),
        "timestamp": doc.get("timestamp"),
        "categories": categories,
        "text": clean_wikitext(wikitext),
    }

    print(json.dumps(out, ensure_ascii=False))
