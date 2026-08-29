#!/usr/bin/env python3
"""CLI wrapper: python scripts/game_export.py ..."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from game_export.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
