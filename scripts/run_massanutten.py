#!/usr/bin/env python3
"""Convenient wrapper to generate the Massanutten ski‑resort copy.

It invokes the existing `generate_resort_copy_ollama.py` CLI with arguments
that target the Massanutten resort (Virginia, USA) and use a small tier so the
run is fast. Adjust `--tier` (small/medium/large/mega) or add `--dry‑batch` if
you only want to see the snippet‑fetching without writing output.
"""
import subprocess
import sys
from pathlib import Path

def main() -> int:
    # Resolve the path to the original generator script
    gen_script = Path(__file__).resolve().parent.parent / "scripts" / "generate_resort_copy_ollama.py"
    if not gen_script.is_file():
        print(f"Error: generator script not found at {gen_script}", file=sys.stderr)
        return 1

    # Base command – use the Python interpreter that runs this wrapper
    cmd = [sys.executable, str(gen_script)]

    # Input parquet that contains all ski‑area rows (generated earlier in the pipeline)
    input_parquet = Path(__file__).resolve().parent.parent / "output" / "combined" / "ski_areas_analyzed.parquet"
    cmd += ["-i", str(input_parquet)]

    # Restrict the run to Massanutten only via the demo‑query shortcut
    cmd += ["--demo-query", "Massanutten", "--demo-state", "Virginia", "--tier", "small"]

    # Optional flags – uncomment one of the following if you need it:
    # cmd += ["--dry-batch"]          # fetch snippets, no output files
    # cmd += ["--smoke"]               # process a single batch for quick test

    print("Running:", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())
