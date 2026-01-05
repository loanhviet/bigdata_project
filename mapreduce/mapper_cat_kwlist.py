#!/usr/bin/env python3
import sys, json, re, unicodedata

TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)
SEP = "\x01"

def norm(s: str) -> str:
    return unicodedata.normalize("NFKC", s).casefold()

def load_keywords(path="keywords.txt"):
    ks = set()
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            w = line.strip()
            if w:
                ks.add(norm(w))
    return ks

KEYS = load_keywords()

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except:
            continue

        cats = obj.get("categories") or []
        if not cats:
            continue

        text = norm(obj.get("text") or "")
        cnt = {}
        for tok in TOKEN_RE.findall(text):
            if tok in KEYS:
                cnt[tok] = cnt.get(tok, 0) + 1
        if not cnt:
            continue

        for c in cats:
            c = str(c).strip()
            if not c:
                continue
            for k, v in cnt.items():
                sys.stdout.write(f"{c}{SEP}{k}\t{v}\n")

if __name__ == "__main__":
    main()
