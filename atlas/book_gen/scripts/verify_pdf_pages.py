#!/usr/bin/env python
"""Print PDF page count (stdlib only). Usage: py verify_pdf_pages.py file.pdf"""
import sys
from pathlib import Path

path = Path(sys.argv[1])
data = path.read_bytes()
# Count /Type /Page entries (not /Pages tree only)
count = data.count(b"/Type /Page")
# Subtract /Pages dictionary occurrences (heuristic)
pages = max(1, count - data.count(b"/Type /Pages"))
print(f"{path.name}: ~{pages} page object(s)")
