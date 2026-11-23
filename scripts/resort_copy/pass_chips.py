"""One true/false/null chip per known multi-resort pass.

true  = a trusted page says THIS resort is on that pass
false = a page says this resort is not on that pass
null  = unknown (a roundup of every pass does not count)

Shipped names live in pass_catalog.json. Regional passes (PA Pass, VT Pass)
go in extra_passes.json or via add_pass() / facts_table.py --add-pass.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import output_path


@dataclass(frozen=True)
class PassChip:
    key: str
    label: str
    pattern: str
    hosts: tuple[str, ...] = ()


CATALOG_PATH = Path(__file__).with_name("pass_catalog.json")
_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SLUG_RE = re.compile(r"[^a-z0-9]+")
_RESERVED = {
    "website",
    "passes",
    "price_band",
    "winter_sports_id",
    "page_id",
    "name",
    "country",
    "state",
    "tier",
}

# Mutated by reload_catalog so importers keep the same list/dict objects.
PASS_CHIPS: list[PassChip] = []
PASS_COLUMNS: list[str] = []
PASS_LABELS: dict[str, str] = {}
_BY_KEY: dict[str, PassChip] = {}

_NO = re.compile(
    r"\b(?:not (?:on|included in|part of)|no longer on|isn'?t on|does not (?:include|accept))\b",
    re.IGNORECASE,
)
_ROUNDUP_HOSTS = (
    "zrankings.com",
    "mtninfo.com",
    "onthesnow.com",
    "ski.com",
    "wikipedia.org",
    "wikidata.org",
)


def extra_catalog_path() -> Path:
    override = (os.environ.get("PASS_CHIPS_EXTRA") or "").strip()
    if override:
        return Path(override)
    return output_path("resort_facts", "extra_passes.json")


def slug_pass_key(label: str) -> str:
    key = _SLUG_RE.sub("_", (label or "").lower()).strip("_")
    if key and key[0].isdigit():
        key = f"pass_{key}"
    return key


def _chip_from_dict(raw: dict[str, Any]) -> PassChip:
    key = str(raw.get("key") or slug_pass_key(str(raw.get("label") or ""))).strip()
    label = str(raw.get("label") or key).strip()
    pattern = str(raw.get("pattern") or "").strip() or rf"\b{re.escape(label)}\b"
    hosts = tuple(str(h).strip().lower() for h in (raw.get("hosts") or []) if str(h).strip())
    if not _KEY_RE.match(key):
        raise ValueError(f"invalid pass key {key!r}")
    if key in _RESERVED:
        raise ValueError(f"pass key {key} is reserved")
    return PassChip(key=key, label=label, pattern=pattern, hosts=hosts)


def _load_json(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        rows = data.get("passes") or []
    elif isinstance(data, list):
        rows = data
    else:
        rows = []
    return [row for row in rows if isinstance(row, dict)]


def reload_catalog() -> list[PassChip]:
    merged: dict[str, PassChip] = {}
    for row in (*_load_json(CATALOG_PATH), *_load_json(extra_catalog_path())):
        chip = _chip_from_dict(row)
        merged[chip.key] = chip
    PASS_CHIPS[:] = list(merged.values())
    PASS_COLUMNS[:] = [chip.key for chip in PASS_CHIPS]
    PASS_LABELS.clear()
    PASS_LABELS.update({chip.key: chip.label for chip in PASS_CHIPS})
    _BY_KEY.clear()
    _BY_KEY.update({chip.key: chip for chip in PASS_CHIPS})
    return list(PASS_CHIPS)


def add_pass(
    label: str,
    *,
    key: str | None = None,
    pattern: str | None = None,
    hosts: list[str] | tuple[str, ...] = (),
) -> PassChip:
    """Register another multi-resort pass. Persists in extra_passes.json."""
    chip = _chip_from_dict(
        {
            "key": (key or slug_pass_key(label)).strip().lower(),
            "label": label.strip(),
            "pattern": (pattern or "").strip(),
            "hosts": list(hosts),
        }
    )
    existing = _BY_KEY.get(chip.key)
    if existing and existing.label.lower() != chip.label.lower():
        raise ValueError(f"pass key {chip.key} is already {existing.label}")
    path = extra_catalog_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    extras = _load_json(path)
    extras = [row for row in extras if str(row.get("key") or "") != chip.key]
    extras.append(
        {
            "key": chip.key,
            "label": chip.label,
            "pattern": chip.pattern,
            "hosts": list(chip.hosts),
        }
    )
    path.write_text(json.dumps({"passes": extras}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    reload_catalog()
    return _BY_KEY[chip.key]


def pass_hosts() -> tuple[str, ...]:
    return tuple(dict.fromkeys(host for chip in PASS_CHIPS for host in chip.hosts))


def _host(url: str) -> str:
    match = re.search(r"https?://([^/]+)", (url or "").lower())
    if not match:
        return ""
    return match.group(1).removeprefix("www.").split(":")[0]


def pass_host_trusted(url: str, official_host: str, chip: PassChip) -> bool:
    host = _host(url)
    if not host:
        return False
    if official_host and (host == official_host or host.endswith("." + official_host)):
        return True
    if any(host == h or host.endswith("." + h) for h in _ROUNDUP_HOSTS):
        return False
    path = (url or "").lower()
    if re.search(r"/our-resorts/?$|/passes/?$|/ski-resorts/season-passes", path):
        return False
    if chip.hosts and any(host == h or host.endswith("." + h) for h in chip.hosts):
        return "destination" in path or "resort" in path or len(path) > 40
    return False


def match_pass_labels(text: str) -> list[str]:
    found: list[str] = []
    blob = text or ""
    for chip in PASS_CHIPS:
        if re.search(chip.pattern, blob, re.IGNORECASE) and chip.key not in found:
            found.append(chip.key)
    return found


def harvest_pass_chips(
    *,
    key_points: dict[str, Any] | None = None,
    notes: list[dict[str, Any]] | None = None,
    snippets: list[dict[str, Any]] | None = None,
    official_host: str = "",
) -> dict[str, dict[str, Any]]:
    """key -> {value, evidence, sources} for every catalog pass."""
    flags: dict[str, dict[str, Any]] = {
        chip.key: {"value": None, "evidence": "", "sources": []} for chip in PASS_CHIPS
    }
    chunks: list[tuple[str, list[str]]] = []

    def add_chunk(text: str, urls: list[str]) -> None:
        if text.strip():
            chunks.append((text, urls))

    points = key_points or {}
    aff = points.get("pass_affiliation")
    if isinstance(aff, dict):
        add_chunk(str(aff.get("value") or ""), [str(u) for u in (aff.get("sources") or []) if u])
    for product in points.get("pass_products") or []:
        if isinstance(product, dict):
            add_chunk(str(product.get("value") or ""), [str(u) for u in (product.get("sources") or []) if u])
    index = {
        str(s.get("id")): str(s.get("url") or "")
        for s in (snippets or [])
        if s.get("id") and s.get("url")
    }
    for note in notes or []:
        ids = note.get("snippet_ids") or []
        urls = [index[str(i)] for i in ids if str(i) in index]
        add_chunk(str(note.get("text") or ""), urls)

    for text, urls in chunks:
        keys = match_pass_labels(text)
        if not keys:
            continue
        denied = bool(_NO.search(text))
        for key in keys:
            chip = _BY_KEY[key]
            trusted = any(pass_host_trusted(url, official_host, chip) for url in urls) if urls else False
            if denied and trusted:
                flags[key] = {"value": False, "evidence": text[:180], "sources": urls}
            elif trusted and flags[key]["value"] is not False:
                flags[key] = {"value": True, "evidence": chip.label, "sources": urls}
    return flags


def true_pass_labels(flags: dict[str, dict[str, Any]]) -> list[str]:
    return [PASS_LABELS[key] for key in PASS_COLUMNS if (flags.get(key) or {}).get("value") is True]


_OTHER_JUNK = re.compile(
    r"not found|one pass this resort|local area pass|schema|ski course|lessons?",
    re.IGNORECASE,
)
_OTHER_GENERIC = re.compile(
    r"^(?:the\s+)?(?:adult\s+)?(?:season|day|lift|weekend|night|multi[-\s]?day)\s+pass(?:es)?$",
    re.IGNORECASE,
)


def leftover_pass_label(text: str) -> str | None:
    """Name of a pass that is not one of the catalog chips."""
    blob = (text or "").strip(" .;|")
    if not blob or len(blob) > 80 or _OTHER_JUNK.search(blob) or _OTHER_GENERIC.match(blob):
        return None
    remainder = blob
    for chip in PASS_CHIPS:
        remainder = re.sub(chip.pattern, " ", remainder, flags=re.IGNORECASE)
    remainder = re.sub(r"\s*(?:,|/|\||\band\b|&)\s*", " ", remainder, flags=re.IGNORECASE)
    remainder = re.sub(r"\s+", " ", remainder).strip(" -,/")
    if len(remainder) < 4:
        return None
    if remainder.lower() in {chip.label.lower() for chip in PASS_CHIPS}:
        return None
    return remainder


def harvest_other_passes(
    *,
    key_points: dict[str, Any] | None = None,
    notes: list[dict[str, Any]] | None = None,
    snippets: list[dict[str, Any]] | None = None,
    official_host: str = "",
) -> list[str]:
    """Trusted names that are not Epic/Ikon/etc. Empty when the source is a roundup."""
    names: list[str] = []
    chunks: list[tuple[str, list[str]]] = []
    points = key_points or {}
    aff = points.get("pass_affiliation")
    if isinstance(aff, dict):
        chunks.append((str(aff.get("value") or ""), [str(u) for u in (aff.get("sources") or []) if u]))
    for product in points.get("pass_products") or []:
        if isinstance(product, dict):
            chunks.append((str(product.get("value") or ""), [str(u) for u in (product.get("sources") or []) if u]))
    index = {
        str(s.get("id")): str(s.get("url") or "")
        for s in (snippets or [])
        if s.get("id") and s.get("url")
    }
    for note in notes or []:
        ids = note.get("snippet_ids") or []
        urls = [index[str(i)] for i in ids if str(i) in index]
        chunks.append((str(note.get("text") or ""), urls))

    for text, urls in chunks:
        leftover = leftover_pass_label(text)
        if not leftover:
            continue
        trusted = False
        for url in urls:
            host = _host(url)
            if official_host and host and (host == official_host or host.endswith("." + official_host)):
                trusted = True
                break
        if trusted and leftover not in names:
            names.append(leftover)
    return names


reload_catalog()
