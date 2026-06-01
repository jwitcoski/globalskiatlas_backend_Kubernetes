"""Console progress logging (always flushed)."""

from __future__ import annotations

import sys
import time
from contextlib import contextmanager
from typing import Iterator, TextIO


def log(msg: str, *, file: TextIO | None = None) -> None:
    print(msg, file=file or sys.stdout, flush=True)


def log_step(step: int, total: int, msg: str) -> None:
    if total > 0:
        log(f"  [{step}/{total}] {msg}")
    else:
        log(f"  [{step}] {msg}")


@contextmanager
def log_phase(name: str) -> Iterator[None]:
    log(f"-- {name} --")
    t0 = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - t0
        log(f"-- {name} done ({elapsed:.1f}s) --")
