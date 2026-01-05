#!/usr/bin/env python3
import sys, json

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except:
            continue
        for c in (obj.get("categories") or []):
            c = str(c).strip()
            if c:
                sys.stdout.write(f"{c}\t1\n")

if __name__ == "__main__":
    main()
