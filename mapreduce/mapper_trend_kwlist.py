#!/usr/bin/env python3
import sys, json, re, unicodedata

TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)
SEP = "\x01"

def norm(s: str) -> str:
    return unicodedata.normalize("NFKC", s).casefold()

def month_from_ts(ts: str):
    return ts[:7] if ts and len(ts) >= 7 else None

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

        m = month_from_ts(obj.get("timestamp") or "")
        if not m:
            continue

        text = norm(obj.get("text") or "")
        cnt = {}
        for tok in TOKEN_RE.findall(text):
            if tok in KEYS:
                cnt[tok] = cnt.get(tok, 0) + 1

        for k, v in cnt.items():
            sys.stdout.write(f"{m}{SEP}{k}\t{v}\n")

if __name__ == "__main__":
    main()
