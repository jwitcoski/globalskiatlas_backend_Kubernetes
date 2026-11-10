#!/usr/bin/env python3
"""
Short atlas resort blurbs on a local Ollama 8B. Entry point only.

The code lives in scripts/resort_copy/ (see its README.md).

Usage:
  python scripts/generate_resort_copy_ollama.py --list-models
  python scripts/generate_resort_copy_ollama.py --smoke
  python scripts/generate_resort_copy_ollama.py --demo-query "Massanutten" --demo-state Virginia --tier small
  python scripts/generate_resort_copy_ollama.py -i output/combined/ski_areas_analyzed.parquet --batch 50 --dry-batch
  python scripts/generate_resort_copy_ollama.py -i output/combined/ski_areas_analyzed.parquet --batch 50
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from resort_copy.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
