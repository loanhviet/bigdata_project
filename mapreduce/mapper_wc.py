#!/usr/bin/env python3
import sys, json, re, unicodedata

# Force UTF-8 stdin/stdout/stderr on Windows so Streaming won't crash on non-ASCII tokens
sys.stdin.reconfigure(encoding="utf-8", errors="replace")
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Letters only (no digits/underscore). Includes unicode letters + combining marks.
TOKEN_RE = re.compile(r"[^\W\d_]+", re.UNICODE)

# Vietnamese combining marks (tone + vowel marks)
VN_MARKS = {
    "\u0300",  # grave
    "\u0301",  # acute
    "\u0303",  # tilde
    "\u0309",  # hook above
    "\u0323",  # dot below
    "\u0302",  # circumflex
    "\u0306",  # breve
    "\u031B",  # horn
}

# Allowed base letters for Vietnamese (exclude f, j, w, z to cut non-Vietnamese noise)
VN_BASE = set("abcdeghiklmnopqrstuvxy") | {"đ"}

def norm(s: str) -> str:
    # normalize + casefold for consistent matching
    return unicodedata.normalize("NFKC", s).casefold()

def load_stopwords(path="stopwords_vi.txt"):
    sw = set()
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            for line in f:
                w = line.strip()
                if w:
                    sw.add(norm(w))
    except Exception:
        pass
    return sw

STOP = load_stopwords()

def is_vietnamese_word(tok: str) -> bool:
    # basic length guard
    if len(tok) < 2 or len(tok) > 40:
        return False

    # reject anything containing digits (IDs, codes)
    if any(ch.isdigit() for ch in tok):
        return False

    # decompose to base letters + combining marks to validate "Vietnamese-ness"
    t = unicodedata.normalize("NFD", tok)

    has_vn_mark = False
    has_vn_specific = False  # đ or VN vowel marks presence

    for ch in t:
        cat = unicodedata.category(ch)
        if cat.startswith("M"):  # combining mark
            if ch in VN_MARKS:
                has_vn_mark = True
            else:
                # weird mark -> likely not Vietnamese
                return False
            continue

        if cat.startswith("L"):  # letter
            # keep Latin letters only (via base set)
            if ch not in VN_BASE:
                return False
            if ch == "đ":
                has_vn_specific = True
            continue

        # anything else (symbols/punct) shouldn't be inside token anyway
        return False

    # Require some Vietnamese signal:
    # - either has Vietnamese combining mark (dấu) OR contains 'đ'
    # This removes most English/Latin names without diacritics.
    return has_vn_mark or has_vn_specific

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            obj = json.loads(line)
        except Exception:
            continue

        text = norm(obj.get("text") or "")
        for tok in TOKEN_RE.findall(text):
            if tok in STOP:
                continue
            if not is_vietnamese_word(tok):
                continue
            sys.stdout.write(f"{tok}\t1\n")

if __name__ == "__main__":
    main()
