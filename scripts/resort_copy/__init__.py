"""Local Ollama resort copy pipeline. See README.md in this folder."""
from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import generate_resort_copy_bedrock as bedrock  # noqa: E402,F401
