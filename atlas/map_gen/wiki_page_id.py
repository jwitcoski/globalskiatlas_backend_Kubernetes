"""Wiki pageId rules — must match GlobalSkiAtlas_2/scripts/wiki-ingest-parquet.js."""

from __future__ import annotations

import re
from typing import Any


def wiki_slug(value: Any) -> str:
    if value is None:
        return "unknown"
    s = str(value).strip()
    if not s:
        return "unknown"
    t = s.lower()
    t = re.sub(r"\s+", "-", t)
    t = re.sub(r"[^a-z0-9-]", "", t)
    t = re.sub(r"-+", "-", t).strip("-")
    return t or "unknown"


def wiki_page_id_from_row(row: dict[str, Any]) -> str:
    """DynamoDB WikiPages key pageId (wiki resort.html static map URL)."""
    name_raw = row.get("english_name") or row.get("name") or row.get("Name")
    name = str(name_raw).strip() if name_raw is not None else ""
    if not name:
        name = "unknown"
    state_keys = (
        "state",
        "State",
        "addr:state",
        "province",
        "addr:province",
        "state_province",
        "region",
    )
    country_keys = ("country", "Country", "addr:country", "country_name")
    state_val = None
    for k in state_keys:
        v = row.get(k)
        if v is not None and str(v).strip():
            state_val = str(v).strip()
            break
    country_val = None
    for k in country_keys:
        v = row.get(k)
        if v is not None and str(v).strip():
            country_val = str(v).strip()
            break
    name_slug = wiki_slug(name)
    state_slug = wiki_slug(state_val) if state_val else ""
    country_slug = wiki_slug(country_val) if country_val else ""
    if state_slug:
        page_id = f"{name_slug}-{state_slug}"
    elif country_slug:
        page_id = f"{name_slug}-{country_slug}"
    else:
        page_id = name_slug
    return page_id or "unknown"


def wiki_row_from_parquet(row: Any, *, name_col: str, state_col: str | None) -> dict[str, Any]:
    """Build a dict suitable for wiki_page_id_from_row from a GeoPandas/pandas row."""
    out: dict[str, Any] = {
        "name": row.get(name_col),
        "english_name": row.get(name_col),
    }
    if state_col:
        out["state"] = row.get(state_col)
    for k in ("Country", "country"):
        try:
            v = row[k]
        except (KeyError, TypeError):
            continue
        if v is not None and str(v).strip():
            out["country"] = v
            break
    return out
