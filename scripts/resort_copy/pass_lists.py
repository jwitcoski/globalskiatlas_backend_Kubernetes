"""Build resort lists from each mega-pass operator page, then match atlas names.

The per-resort search is a bad place to learn Ikon vs Epic. The operator already
publishes the member list. This harvests that list, fuzzy-matches slightly
different names (Killington vs Killington Resort), and writes merge tables.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from difflib import SequenceMatcher
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
import urllib.request

from .config import output_path
from .entity import GENERIC_WORDS, squash
from .pass_chips import PASS_CHIPS, PASS_COLUMNS, PassChip, reload_catalog
from .web_fetch import _fetch_archived, _fetch_html


ROSTER_DIR = output_path("pass_rosters")
DEFAULT_BY_ID = ROSTER_DIR / "by_winter_sports_id.json"
EPIC_SUPPLEMENT = Path(__file__).with_name("data") / "epic_destinations.jsonl"
_SKIP_SLUGS = {
    "destinations",
    "destination",
    "resorts",
    "resort",
    "mountains",
    "mountain",
    "our-resorts",
    "stations",
    "en",
    "fr",
    "de",
    "it",
    "info",
    "pass",
    "passes",
    "account",
    "cart",
    "blog",
    "news",
    "faq",
    "buy",
    "login",
    "search",
}
_STRIP_TAIL = re.compile(
    r"\s+(?:ski(?:\s+and\s+snowboard)?(?:\s+area|\s+resort)?|(?:ski\s+)?(?:area|resort|village))\s*$",
    re.IGNORECASE,
)
_HREF_HINT = re.compile(
    r"/(?:destinations?|our-resorts|resorts?|mountains?|stations?|ski-destinations?|regions)/",
    re.IGNORECASE,
)
_NOT_A_NAME = re.compile(
    r"^(?:destination|opening date|closing date|open|tba|closed|view resort details|"
    r"skip to(?: (?:main )?content)?|skip to footer|"
    r"united states|canada|europe|asia|oceania|south america|rockies|west coast|"
    r"west|east|midwest|mid-atlantic|japan|china|france|italy|switzerland|austria|"
    r"australia|chile|nov|dec|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|"
    r"\d{1,2}\s*(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)?)$",
    re.IGNORECASE,
)
_PLACE_TAIL = re.compile(
    r",\s*(?:[A-Z]{2}|ON|QC|BC|AB|NS|NB|PE|NL|SK|MB|YT|NT|NU|"
    r"Japan|China|Chile|France|Italy|Switzerland|Austria|Australia|"
    r"Canada|Vermont|Colorado|Utah|California)\s*$",
    re.IGNORECASE,
)


class _TextParser(HTMLParser):
    """Links plus short table and heading cells from destination list pages."""

    _SKIP = {"script", "style", "noscript", "svg"}
    _CELL = {"td", "th", "h1", "h2", "h3", "li"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str, str]] = []
        self.cells: list[str] = []
        self._href: str | None = None
        self._link_text: list[str] = []
        self._cell: str | None = None
        self._cell_text: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self._SKIP:
            self._skip += 1
            return
        if tag == "a":
            href = dict(attrs).get("href") or ""
            if href and not href.startswith(("#", "javascript:", "mailto:")):
                self._href = href
                self._link_text = []
        if tag in self._CELL:
            self._cell = tag
            self._cell_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP:
            self._skip = max(0, self._skip - 1)
            return
        if tag == "a" and self._href is not None:
            text = re.sub(r"\s+", " ", "".join(self._link_text)).strip()
            self.links.append((self._href, text))
            self._href = None
            self._link_text = []
        if tag in self._CELL and self._cell == tag:
            text = re.sub(r"\s+", " ", "".join(self._cell_text)).strip()
            if text:
                self.cells.append(text)
            self._cell = None
            self._cell_text = []

    def handle_data(self, data: str) -> None:
        if self._skip:
            return
        if self._href is not None:
            self._link_text.append(data)
        if self._cell is not None:
            self._cell_text.append(data)


def core_name(name: str) -> str:
    text = _STRIP_TAIL.sub("", (name or "").strip())
    return re.sub(r"\s+", " ", text).strip() or (name or "").strip()


def listed_from_slug(path: str) -> str:
    parts = [p for p in (path or "").strip("/").split("/") if p and p.lower() not in _SKIP_SLUGS]
    if not parts:
        return ""
    slug = re.sub(r"\.(html?|aspx)$", "", parts[-1], flags=re.IGNORECASE)
    slug = re.sub(r"[-_]+", " ", slug)
    return slug.strip().title()


def _walk_json_names(node: Any, found: list[str]) -> None:
    if isinstance(node, dict):
        name = node.get("name") or node.get("title") or node.get("resortName")
        if isinstance(name, str) and 2 <= len(name) <= 80:
            blob = " ".join(str(node.get(k) or "") for k in ("url", "href", "slug", "path", "type"))
            if _HREF_HINT.search(blob) or node.get("destination") or node.get("resort"):
                found.append(name.strip())
        for value in node.values():
            _walk_json_names(value, found)
    elif isinstance(node, list):
        for item in node:
            _walk_json_names(item, found)


def _clean_listed_name(name: str) -> str:
    label = re.sub(r"\s+", " ", (name or "").strip())
    label = _PLACE_TAIL.sub("", label).strip()
    return label


def extract_listed_resorts(html: str, page_url: str) -> list[dict[str, str]]:
    """Names and destination URLs from one operator list page."""
    parser = _TextParser()
    try:
        parser.feed(html or "")
        parser.close()
    except Exception:
        parser.links = []
        parser.cells = []
    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    def add(name: str, url: str) -> None:
        label = _clean_listed_name(name)
        if len(label) < 3 or len(label) > 80:
            return
        if label.lower() in _SKIP_SLUGS or _NOT_A_NAME.match(label):
            return
        if re.search(r"\d{4}|opening|closing|pass holder|blackout|vertical|trails \d|lifts \d|skip to", label, re.I):
            return
        if len(label.split()) > 8:
            return
        key = squash(core_name(label))
        if not key or key in seen:
            return
        seen.add(key)
        rows.append({"listed_name": label, "listed_url": url})

    for href, text in parser.links:
        absolute = urljoin(page_url, href)
        path = urlparse(absolute).path or ""
        if not _HREF_HINT.search(path) and not _HREF_HINT.search(href):
            continue
        slug_name = listed_from_slug(path)
        add(text or slug_name, absolute)
        if slug_name and squash(core_name(slug_name)) != squash(core_name(text or "")):
            add(slug_name, absolute)

    for cell in parser.cells:
        add(cell, page_url)

    for match in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html or "", re.I | re.S):
        try:
            data = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        names: list[str] = []
        _walk_json_names(data, names)
        for name in names:
            add(name, page_url)

    for match in re.finditer(r"<script[^>]*>(.*?)</script>", html or "", re.I | re.S):
        blob = match.group(1)
        if "destinations" not in blob.lower() and "__NEXT_DATA__" not in blob:
            continue
        start = blob.find("{")
        end = blob.rfind("}")
        if start < 0 or end <= start or end - start > 2_000_000:
            continue
        try:
            json_blob = json.loads(blob[start : end + 1])
        except json.JSONDecodeError:
            continue
        names: list[str] = []
        _walk_json_names(json_blob, names)
        for name in names:
            add(name, page_url)

    for slug in extract_destination_slugs(html or ""):
        add(listed_from_slug(slug), page_url)
    return rows


def extract_destination_slugs(blob: str) -> list[str]:
    """Quoted hyphenated slugs from SPA bundles, e.g. killington-resort."""
    found: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"\[((?:\"[a-z0-9-]+\",)+\"[a-z0-9-]+\")\]", blob):
        slugs = re.findall(r"\"([a-z0-9-]+)\"", match.group(1))
        if len(slugs) < 6:
            continue
        for slug in slugs:
            if slug in _SKIP_SLUGS or slug in seen:
                continue
            if "-" not in slug or slug.startswith(("fa-", "auto-", "top-", "right-", "left-", "flex-")):
                continue
            seen.add(slug)
            found.append(slug)
    return found


def _fetch_text(url: str, timeout: float = 25.0) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read(8_000_000).decode("utf-8", "replace")
    except Exception:
        return ""


def same_origin_scripts(html: str, page_url: str) -> list[str]:
    page_host = (urlparse(page_url).hostname or "").lower()
    blocked = ("google", "adobe", "cookie", "fontawesome", "inbenta", "facebook", "googletagmanager")
    urls: list[str] = []
    for src in re.findall(r"<script[^>]+src=[\"']([^\"']+)[\"']", html or "", re.I):
        absolute = urljoin(page_url, src)
        host = (urlparse(absolute).hostname or "").lower()
        if not host or any(bit in host for bit in blocked):
            continue
        if host != page_host and not host.endswith("." + ".".join(page_host.split(".")[-2:])):
            continue
        if ".js" not in src and "/assets/" not in src:
            continue
        urls.append(absolute)
    return urls


def _fetch_jina(url: str, timeout: float = 60.0) -> str:
    """Jina Reader unwraps SPA destination lists (Epic and similar)."""
    target = url.strip()
    if not target.startswith("http"):
        return ""
    # Prefer http upstream; Jina accepts either.
    jina = "https://r.jina.ai/" + target.replace("https://", "http://", 1)
    return _fetch_text(jina, timeout=timeout)


def fetch_list_html(url: str) -> str:
    html = _fetch_html(url) or ""
    if len(html) < 800:
        html = _fetch_archived(url) or html
    # Epic and other SPA list pages often return an empty shell without JS.
    if len(html) < 4000 or ("epicpass.com" in url and "vail" not in html.lower() and "resort" not in html.lower()):
        jina = _fetch_jina(url)
        if len(jina) > len(html):
            html = jina
    return html


def harvest_pass_list(chip: PassChip, *, html_by_url: dict[str, str] | None = None) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for url in chip.list_urls:
        html = (html_by_url or {}).get(url)
        if html is None:
            html = fetch_list_html(url)
            for script_url in same_origin_scripts(html, url)[:4]:
                html += "\n" + _fetch_text(script_url)
        for item in extract_listed_resorts(html, url):
            key = squash(core_name(item["listed_name"]))
            if key in seen:
                continue
            seen.add(key)
            rows.append({"pass_key": chip.key, "pass_label": chip.label, **item, "list_url": url})
    return rows


def atlas_index(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    indexed: list[dict[str, Any]] = []
    for row in rows:
        name = str(row.get("english_name") or row.get("name") or "").strip()
        local = str(row.get("name") or "").strip()
        if not name:
            continue
        wid = str(row.get("winter_sports_id") or row.get("winterSportsId") or "").strip()
        aliases = [name]
        if local and local != name:
            aliases.append(local)
        keys = {squash(core_name(alias)) for alias in aliases if squash(core_name(alias))}
        indexed.append(
            {
                "winter_sports_id": wid,
                "atlas_name": name,
                "local_name": local,
                "state": str(row.get("state") or "").strip(),
                "country": str(row.get("country") or "").strip(),
                "keys": keys,
                "row": row,
            }
        )
    return indexed


def _score_pair(listed: str, atlas: dict[str, Any]) -> float:
    listed_core = squash(core_name(listed))
    if not listed_core:
        return 0.0
    best = 0.0
    for key in atlas["keys"]:
        if listed_core == key:
            return 1.0
        if listed_core in key or key in listed_core:
            shorter, longer = (listed_core, key) if len(listed_core) <= len(key) else (key, listed_core)
            if shorter in GENERIC_WORDS:
                continue
            if len(shorter) >= 6 and (longer.startswith(shorter) or longer.endswith(shorter)):
                best = max(best, 0.94)
        best = max(best, SequenceMatcher(None, listed_core, key).ratio())
    return best


def _atlas_key_index(atlas: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_key: dict[str, list[dict[str, Any]]] = {}
    for item in atlas:
        for key in item.get("keys") or set():
            by_key.setdefault(key, []).append(item)
    return by_key


def match_listed(
    listed_name: str,
    atlas: list[dict[str, Any]],
    *,
    key_index: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[dict[str, Any] | None, float, list[dict[str, Any]]]:
    listed_core = squash(core_name(listed_name))
    if listed_core and key_index is not None:
        exact = key_index.get(listed_core) or []
        if len(exact) == 1:
            return exact[0], 1.0, []
        if len(exact) > 1:
            # Prefer ski/resort-labelled rows, then shorter names.
            ranked_exact = sorted(
                exact,
                key=lambda item: (
                    int("resort" in (item["atlas_name"] + " " + item["local_name"]).lower()
                        or "ski" in (item["atlas_name"] + " " + item["local_name"]).lower()),
                    -len(item["atlas_name"]),
                ),
                reverse=True,
            )
            if ranked_exact[0]["winter_sports_id"] != ranked_exact[1]["winter_sports_id"]:
                return ranked_exact[0], 1.0, ranked_exact[1:3]
            return None, 1.0, ranked_exact[:3]
    ranked = sorted(
        ((_score_pair(listed_name, item), item) for item in atlas),
        key=lambda pair: pair[0],
        reverse=True,
    )
    if not ranked or ranked[0][0] < 0.86:
        return None, ranked[0][0] if ranked else 0.0, []
    top_score, top = ranked[0]
    close = [item for score, item in ranked[:4] if score >= top_score - 0.02 and score >= 0.86]
    if len(close) > 1:
        def prefer(item: dict[str, Any]) -> tuple[int, int]:
            blob = (item["atlas_name"] + " " + item["local_name"]).lower()
            return (int("resort" in blob or "ski" in blob), len(item["atlas_name"]))

        close.sort(key=prefer, reverse=True)
        if prefer(close[0]) == prefer(close[1]):
            return None, top_score, close[:3]
        top = close[0]
    return top, top_score, []


def merge_rosters(
    listed: list[dict[str, str]],
    atlas_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    atlas = atlas_index(atlas_rows)
    key_index = _atlas_key_index(atlas)
    merged: list[dict[str, Any]] = []
    for item in listed:
        hit, score, rivals = match_listed(item["listed_name"], atlas, key_index=key_index)
        row = dict(item)
        row["match_score"] = round(score, 3)
        if hit:
            row["status"] = "matched"
            row["winter_sports_id"] = hit["winter_sports_id"]
            row["atlas_name"] = hit["atlas_name"]
            row["state"] = hit["state"]
            row["country"] = hit["country"]
        else:
            row["status"] = "unmatched" if not rivals else "ambiguous"
            row["winter_sports_id"] = ""
            row["atlas_name"] = ""
            row["candidates"] = [c["atlas_name"] for c in rivals]
        merged.append(row)
    return merged


def write_rosters(merged: list[dict[str, Any]], directory: Path = ROSTER_DIR) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    by_pass: dict[str, list[dict[str, Any]]] = {}
    for row in merged:
        by_pass.setdefault(row["pass_key"], []).append(row)
    for key, rows in by_pass.items():
        path = directory / f"{key}.jsonl"
        path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
    csv_path = directory / "merged.csv"
    fields = [
        "pass_key",
        "pass_label",
        "listed_name",
        "atlas_name",
        "winter_sports_id",
        "status",
        "match_score",
        "state",
        "country",
        "listed_url",
        "list_url",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(merged)
    chips: dict[str, dict[str, Any]] = {}
    for row in merged:
        if row.get("status") != "matched" or not row.get("winter_sports_id"):
            continue
        item = chips.setdefault(
            row["winter_sports_id"],
            {"winterSportsId": row["winter_sports_id"], "atlas_name": row.get("atlas_name") or ""},
        )
        item[row["pass_key"]] = True
        item.setdefault("listed", {})[row["pass_key"]] = {
            "name": row["listed_name"],
            "url": row.get("listed_url") or row.get("list_url") or "",
        }
    chip_path = directory / "chips.jsonl"
    chip_path.write_text(
        "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in chips.values()),
        encoding="utf-8",
    )
    by_id: dict[str, dict[str, Any]] = {}
    for row in merged:
        if row.get("status") != "matched" or not row.get("winter_sports_id"):
            continue
        wid = str(row["winter_sports_id"])
        item = by_id.setdefault(
            wid,
            {
                "winter_sports_id": wid,
                "atlas_name": row.get("atlas_name") or "",
                "state": row.get("state") or "",
                "country": row.get("country") or "",
                "passes": [],
            },
        )
        listed_url = str(row.get("listed_url") or "").strip()
        list_url = str(row.get("list_url") or "").strip()
        pass_key = str(row.get("pass_key") or "")
        pass_label = str(row.get("pass_label") or pass_key)
        # Prefer the resort-specific listed URL (Ikon destination page, Epic resort
        # site, Indy resort card). Fall back to the operator list page.
        chip = next((c for c in PASS_CHIPS if c.key == pass_key), None)
        hosts = tuple(chip.hosts) if chip else ()
        provider_url = ""
        for cand in (listed_url, list_url):
            host = (urlparse(cand).hostname or "").lower()
            if cand and hosts and any(host == h or host.endswith("." + h) for h in hosts):
                provider_url = cand
                break
        seed = listed_url or provider_url or list_url
        entry = {
            "pass_key": pass_key,
            "pass_label": pass_label,
            "listed_name": row.get("listed_name") or "",
            "url": seed,
            "provider_url": provider_url or list_url,
            "listed_url": listed_url,
            "list_url": list_url,
            "match_score": row.get("match_score"),
        }
        existing = {p["pass_key"]: i for i, p in enumerate(item["passes"])}
        if pass_key in existing:
            item["passes"][existing[pass_key]] = entry
        else:
            item["passes"].append(entry)
        item[pass_key] = True
    by_path = directory / "by_winter_sports_id.json"
    by_path.write_text(json.dumps(by_id, ensure_ascii=False, indent=2), encoding="utf-8")
    unmatched = [row for row in merged if row.get("status") != "matched"]
    (directory / "unmatched.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in unmatched),
        encoding="utf-8",
    )
    return csv_path


def load_chip_overlay(path: Path | None = None) -> dict[str, dict[str, Any]]:
    overlay: dict[str, dict[str, Any]] = {}
    chip_path = path or (ROSTER_DIR / "chips.jsonl")
    if not chip_path.is_file():
        return overlay
    for line in chip_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        wid = str(item.get("winterSportsId") or item.get("winter_sports_id") or "").strip()
        if wid:
            overlay[wid] = item
    return overlay


def load_pass_index(path: Path | None = None) -> dict[str, dict[str, Any]]:
    """winter_sports_id → pass memberships with destination URLs for research."""
    by_path = path or DEFAULT_BY_ID
    if by_path.is_file():
        try:
            data = json.loads(by_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            data = {}
        if isinstance(data, dict):
            return {str(k): v for k, v in data.items()}
    # Fall back to chips.jsonl
    out: dict[str, dict[str, Any]] = {}
    for wid, item in load_chip_overlay().items():
        passes = []
        listed = item.get("listed") or {}
        for key, meta in listed.items():
            if not isinstance(meta, dict):
                continue
            passes.append(
                {
                    "pass_key": key,
                    "pass_label": key.replace("_", " ").title(),
                    "url": meta.get("url") or "",
                    "listed_url": meta.get("url") or "",
                    "listed_name": meta.get("name") or "",
                }
            )
        out[wid] = {
            "winter_sports_id": wid,
            "atlas_name": item.get("atlas_name") or "",
            "passes": passes,
            **{k: True for k in PASS_COLUMNS if item.get(k) is True},
        }
    return out


def lookup_pass_memberships(winter_sports_id: str | None) -> list[dict[str, Any]]:
    """Pass chips + destination URLs for one atlas resort."""
    wid = str(winter_sports_id or "").strip()
    if not wid:
        return []
    item = load_pass_index().get(wid) or {}
    passes = item.get("passes") or []
    return [p for p in passes if isinstance(p, dict) and (p.get("url") or p.get("pass_key"))]


def apply_overlay(amenities: dict[str, Any], winter_sports_id: str, overlay: dict[str, dict[str, Any]] | None = None) -> None:
    item = (overlay or load_chip_overlay()).get(str(winter_sports_id or "").strip())
    if not item:
        item = load_pass_index().get(str(winter_sports_id or "").strip())
    if not item:
        return
    for key in PASS_COLUMNS:
        if item.get(key) is True:
            amenities[key] = True


def _load_epic_supplement() -> list[dict[str, str]]:
    if not EPIC_SUPPLEMENT.is_file():
        return []
    rows: list[dict[str, str]] = []
    for line in EPIC_SUPPLEMENT.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = str(row.get("listed_name") or row.get("name") or "").strip()
        url = str(row.get("listed_url") or row.get("url") or "").strip()
        if not name or not url:
            continue
        rows.append(
            {
                "pass_key": "epic",
                "pass_label": "Epic",
                "listed_name": name,
                "listed_url": url,
                "list_url": str(row.get("list_url") or ""),
            }
        )
    return rows


def _load_parquet(path: Path) -> list[dict[str, Any]]:
    import pandas as pd

    frame = pd.read_parquet(path)
    return [row.to_dict() for _, row in frame.iterrows()]


def main(argv: list[str] | None = None) -> int:
    reload_catalog()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", type=Path, default=Path("output/combined/ski_areas_analyzed.parquet"))
    parser.add_argument("--out", type=Path, default=ROSTER_DIR)
    parser.add_argument("--pass-key", action="append", default=[], help="Only these catalog keys (repeatable)")
    parser.add_argument("--html", nargs=2, action="append", default=[], metavar=("URL", "FILE"), help="Use a saved HTML file instead of fetching")
    parser.add_argument("--no-epic-supplement", action="store_true")
    args = parser.parse_args(argv)
    html_by_url = {url: Path(file).read_text(encoding="utf-8") for url, file in args.html}
    wanted = set(args.pass_key)
    chips = [chip for chip in PASS_CHIPS if chip.list_urls and (not wanted or chip.key in wanted)]
    listed: list[dict[str, str]] = []
    for chip in chips:
        rows = harvest_pass_list(chip, html_by_url=html_by_url or None)
        print(f"{chip.key}: {len(rows)} listed names", file=sys.stderr)
        listed.extend(rows)
    if not args.no_epic_supplement and (not wanted or "epic" in wanted):
        extra = _load_epic_supplement()
        if extra:
            print(f"epic supplement: {len(extra)} listed names", file=sys.stderr)
            listed.extend(extra)
    atlas_rows = _load_parquet(args.parquet) if args.parquet.is_file() else []
    merged = merge_rosters(listed, atlas_rows)
    path = write_rosters(merged, args.out)
    matched = sum(1 for row in merged if row.get("status") == "matched")
    linked = len(json.loads((args.out / "by_winter_sports_id.json").read_text(encoding="utf-8"))) if (args.out / "by_winter_sports_id.json").is_file() else 0
    print(f"matched {matched}/{len(merged)} rows; {linked} atlas resorts linked -> {path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
