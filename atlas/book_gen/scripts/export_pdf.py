#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Export the open Scribus document to PDF.

Usage (inside Scribus; -py must be the last CLI option, no "--" before args):
  scribus -g -py export_pdf.py input.sla [output.pdf]
"""
from __future__ import print_function

import os
import sys

try:
    import scribus
except ImportError:
    if len(sys.argv) >= 2:
        sla = sys.argv[-2] if sys.argv[-1].endswith(".pdf") else sys.argv[-1]
        os.system('scribus -g -py "%s" "%s"' % (sys.argv[0], sla))
    sys.exit(1)


def main():
    args = [a for a in sys.argv[1:] if a != "--"]
    if len(args) < 1:
        print("Usage: scribus -g -py export_pdf.py file.sla [out.pdf]", file=sys.stderr)
        sys.exit(1)
    sla_path = os.path.abspath(args[0])
    pdf_path = os.path.abspath(
        args[1] if len(args) > 1 else os.path.splitext(sla_path)[0] + ".pdf"
    )

    if not os.path.isfile(sla_path):
        print("SLA not found:", sla_path, file=sys.stderr)
        sys.exit(1)

    out_dir = os.path.dirname(pdf_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    if not scribus.haveDoc():
        scribus.openDoc(sla_path)
    if not scribus.haveDoc():
        print("Could not open", sla_path, file=sys.stderr)
        sys.exit(1)

    page_count = scribus.pageCount()
    if page_count < 1:
        print("Document has no pages", file=sys.stderr)
        sys.exit(1)

    # Scribus page numbers for PDF export are 1-based.
    pdf = scribus.PDFfile()
    pdf.file = pdf_path.replace("\\", "/")
    if page_count > 1:
        pdf.pages = list(range(1, page_count + 1))
    pdf.save()
    if not os.path.isfile(pdf_path):
        print("PDF was not written:", pdf_path, file=sys.stderr)
        sys.exit(1)
    print("Wrote", pdf_path)
    try:
        scribus.closeDoc()
    except Exception:
        pass


if __name__ == "__main__":
    main()
