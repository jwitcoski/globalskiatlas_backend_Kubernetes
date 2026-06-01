#!/usr/bin/env python
"""Run inside Scribus: print page count to stdout."""
from __future__ import print_function

import os
import sys

import scribus

args = [a for a in sys.argv[1:] if a != "--"]
sla = os.path.abspath(args[0])
if not scribus.haveDoc():
    scribus.openDoc(sla)
print("PAGECOUNT", scribus.pageCount())
