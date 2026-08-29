"""JSON dumps that never emit NaN/Infinity (invalid in browsers)."""
from __future__ import annotations

import json
import math
from typing import Any


def json_safe(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe(v) for v in obj]
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int) and not isinstance(obj, bool):
        return int(obj)
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    try:
        import numpy as np

        if isinstance(obj, np.generic):
            return json_safe(obj.item())
    except ImportError:
        pass
    if isinstance(obj, str):
        if obj.lower() in {"nan", "nat", "none"}:
            return None
        return obj
    # pandas NA
    try:
        if obj != obj:  # noqa: PLR0124
            return None
    except Exception:
        pass
    return obj


def dumps(obj: Any, **kwargs) -> str:
    kwargs.setdefault("indent", 2)
    kwargs["allow_nan"] = False
    return json.dumps(json_safe(obj), **kwargs)
