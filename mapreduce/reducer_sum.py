#!/usr/bin/env python3
import sys

# Force UTF-8 for Streaming on Windows
sys.stdin.reconfigure(encoding="utf-8", errors="replace")
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

def main():
    current_key = None
    current_sum = 0

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) < 2:
            continue

        key = parts[0]
        try:
            val = int(parts[1])
        except:
            continue

        if current_key is None:
            current_key = key
            current_sum = val
        elif key == current_key:
            current_sum += val
        else:
            sys.stdout.write(f"{current_key}\t{current_sum}\n")
            current_key = key
            current_sum = val

    if current_key is not None:
        sys.stdout.write(f"{current_key}\t{current_sum}\n")

if __name__ == "__main__":
    main()
