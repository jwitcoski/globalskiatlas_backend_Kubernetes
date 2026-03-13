#!/usr/bin/env python3
"""
Translate ski area names to English using Google Translate (googletrans).

Reads ski_areas_analyzed.parquet, fills english_name when missing and name is
non-Latin or has non-ASCII. Skips countries where names are assumed English
(US, Canada, UK, Australia, NZ, Ireland). Uses cache to avoid re-translating.
Run after combine_regions.

Usage:
  python scripts/translate_resort_names.py
  python scripts/translate_resort_names.py -i output/combined/ski_areas_analyzed.parquet --limit 100
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Countries where names are assumed English (skip translation)
SKIP_COUNTRIES = frozenset({
    "united states", "united states of america", "usa", "us",
    "canada", "united kingdom", "uk", "great britain", "britain",
    "australia", "new zealand", "ireland",
})


def _normalize_country(s: str) -> str:
    if pd.isna(s) or s is None:
        return ""
    return str(s).strip().lower()


def _skip_country(country: str) -> bool:
    return _normalize_country(country) in SKIP_COUNTRIES


def _needs_translation(name: str, country: str) -> bool:
    """True if we should try to translate (not skipped by country, and non-Latin or has non-ASCII)."""
    if _skip_country(country):
        return False
    s = str(name) if name else ""
    if not s.strip():
        return False
    # Non-Latin script (CJK, Cyrillic, etc.)
    non_latin = sum(1 for c in s if ord(c) > 0x0370 and c.isalpha())
    if non_latin >= len(s) * 0.3:
        return True
    # Extended Latin / diacritics (e.g. Azerbaijani Şahdağ, Turkish)
    if any(ord(c) > 127 for c in s):
        return True
    return False


def _load_cache(cache_path: Path) -> dict:
    if not cache_path.exists():
        return {}
    try:
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_cache(cache_path: Path, cache: dict) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _translate(text: str) -> Optional[str]:
    """Translate text to English using googletrans (auto-detects source language)."""
    try:
        import asyncio
        from googletrans import Translator
        t = Translator()

        async def _do():
            r = await t.translate(text, dest="en")
            return r.text if r and r.text else None

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(_do())
    except Exception:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Translate ski area names to English using Google Translate",
    )
    parser.add_argument(
        "-i", "--input",
        default="output/combined/ski_areas_analyzed.parquet",
        help="Input parquet path",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output parquet path (default: overwrite input)",
    )
    parser.add_argument(
        "--cache",
        default="cache/name_translations.json",
        help="Translation cache path",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit rows to process (for testing)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path
    cache_path = Path(args.cache)

    if not input_path.exists():
        print(f"Input not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_parquet(input_path)
    if args.limit:
        df = df.head(args.limit)

    if "english_name" not in df.columns:
        df["english_name"] = ""
    else:
        # Parquet may have saved all-NaN as float64; ensure we can assign strings
        df["english_name"] = df["english_name"].astype(object).fillna("")

    country_col = "Country" if "Country" in df.columns else "country"
    if country_col not in df.columns:
        country_col = None

    name_col = "name"
    if name_col not in df.columns:
        print("No 'name' column found", file=sys.stderr)
        sys.exit(1)

    cache = _load_cache(cache_path)

    # Collect (row_idx, name) for names we need to translate
    to_translate: list[tuple[int, str]] = []
    for i, row in df.iterrows():
        name = row.get(name_col)
        eng = row.get("english_name")
        if pd.isna(eng) or (isinstance(eng, str) and not eng.strip()):
            country = row.get(country_col, "") if country_col else ""
            if _needs_translation(name, country):
                to_translate.append((int(i), str(name).strip() if name else ""))

    # Dedupe by name
    unique: dict[str, list[int]] = {}
    for idx, name in to_translate:
        if name:
            unique.setdefault(name, []).append(idx)

    if not unique:
        print("No names need translation")
        # Still copy name -> english_name where empty
        for i, row in df.iterrows():
            eng = row.get("english_name")
            if pd.isna(eng) or (isinstance(eng, str) and not eng.strip()):
                n = row.get(name_col)
                if pd.notna(n) and str(n).strip():
                    df.at[i, "english_name"] = str(n).strip()
        df.to_parquet(output_path, index=False)
        print(f"Wrote {output_path}")
        return

    translated: dict[str, str] = {}
    for name, indices in unique.items():
        if name in cache:
            translated[name] = cache[name]
            continue
        result = _translate(name)
        if result:
            translated[name] = result
            cache[name] = result

    for name, result in translated.items():
        for idx in unique[name]:
            df.at[idx, "english_name"] = result

    # Where no translation (skipped or failed), use original name so english_name is always set
    for i, row in df.iterrows():
        eng = row.get("english_name")
        if pd.isna(eng) or (isinstance(eng, str) and not eng.strip()):
            n = row.get(name_col)
            if pd.notna(n) and str(n).strip():
                df.at[i, "english_name"] = str(n).strip()

    _save_cache(cache_path, cache)
    df.to_parquet(output_path, index=False)
    print(f"Translated {len(translated)} unique names, wrote {output_path}")


if __name__ == "__main__":
    main()
