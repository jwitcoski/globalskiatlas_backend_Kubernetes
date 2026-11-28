"""Resort URL reference layer: sheet hyperlinks + mega-pass lists seed research.

Blind SearXNG discovery is the fallback. This module builds a name→URL table from:

1. The Active U.S. Ski Areas spreadsheet (hyperlinks on the Ski Area name).
2. Harvested mega-pass destination lists (`pass_rosters/*.jsonl`).
3. SnowGuide / OpenSkiData worldwide website column (Europe + Japan dense).
4. ski-ski-ski.com European directory name→URL list.
5. China directory (Huaxue + SnowGuide CN + ski-ski-ski China.html).
6. Canada directory (ski-ski-ski Western/Eastern Canada province lists).
7. Optional parquet / OSM website cells (looked up at runtime).

Deep ticket/pass links are normalized to the site home. Epic/Ikon/Indy destination
pages are recorded as pass evidence, not as the resort's official domain.
Facebook / municipal / club URLs are kept as thin primary sources for small hills.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import zipfile
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse
from xml.etree import ElementTree as ET

from .config import output_path
from .entity import squash
from .pass_lists import atlas_index, core_name, match_listed
from .sources import host_of

REF_DIR = output_path("resort_url_refs")
_DATA = Path(__file__).with_name("data")
DEFAULT_SHEET = _DATA / "active_us_ski_areas.xlsx"
DEFAULT_SNOWGUIDE = _DATA / "snowguide_resorts.csv"
DEFAULT_EU_DIR = _DATA / "european_ski_resorts.jsonl"
DEFAULT_EU_HTML = _DATA / "european_ski_resorts.html"
DEFAULT_CHINA_DIR = _DATA / "china_ski_resorts.jsonl"
DEFAULT_CANADA_DIR = _DATA / "canada_ski_resorts.jsonl"
DEFAULT_JSONL = REF_DIR / "refs.jsonl"
DEFAULT_BY_ID = REF_DIR / "by_winter_sports_id.json"

_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"

PASS_NETWORK_HOSTS = (
    "epicpass.com",
    "ikonpass.com",
    "indyskipass.com",
    "mountaincollective.com",
    "thepowerpass.ski",
    "powderalliance.com",
    "peakresorts.com",
)

DIRECTORY_SEED_HOSTS = (
    "onthesnow.",
    "skiresort.info",
    "skiresort.com",
    "snow-forecast.com",
    "tripadvisor.",
    "bergfex.",
    "tiscover.",
    "myswitzerland.",
    "stations-de-ski.",
    "andorraescapes.",
    "andorraresorts.",
    "ski-ski-ski.com",
    "canadatrails.",
)

# ISO 3166-1 alpha-2 → atlas `country` strings used in ski_areas_analyzed.parquet.
_ISO_COUNTRY = {
    "AD": "Andorra",
    "AL": "Albania",
    "AM": "Armenia",
    "AT": "Austria",
    "AU": "Australia",
    "AX": "Aland",
    "AZ": "Azerbaijan",
    "BA": "Bosnia and Herzegovina",
    "BE": "Belgium",
    "BG": "Bulgaria",
    "BY": "Belarus",
    "CA": "Canada",
    "CH": "Switzerland",
    "CL": "Chile",
    "CN": "China",
    "CZ": "Czechia",
    "DE": "Germany",
    "DK": "Denmark",
    "EE": "Estonia",
    "ES": "Spain",
    "FI": "Finland",
    "FR": "France",
    "GB": "United Kingdom",
    "GE": "Georgia",
    "GR": "Greece",
    "HR": "Croatia",
    "HU": "Hungary",
    "IE": "Ireland",
    "IN": "India",
    "IR": "Iran",
    "IS": "Iceland",
    "IT": "Italy",
    "JP": "Japan",
    "KG": "Kyrgyzstan",
    "KZ": "Kazakhstan",
    "LI": "Liechtenstein",
    "LK": "Sri Lanka",
    "LS": "Lesotho",
    "LT": "Lithuania",
    "LV": "Latvia",
    "MA": "Morocco",
    "MD": "Moldova",
    "ME": "Montenegro",
    "MK": "North Macedonia",
    "MN": "Mongolia",
    "NL": "Netherlands",
    "NO": "Norway",
    "NP": "Nepal",
    "NZ": "New Zealand",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "RS": "Serbia",
    "RU": "Russia",
    "SE": "Sweden",
    "SI": "Slovenia",
    "SK": "Slovakia",
    "TR": "Turkey",
    "UA": "Ukraine",
    "US": "United States of America",
    "XK": "Kosovo",
    "ZA": "South Africa",
}

_EU_SECTION_COUNTRY = {
    "Czech Republic": "Czechia",
    "Macedonia": "North Macedonia",
    "Scotland": "United Kingdom",
    "England": "United Kingdom",
    "Wales": "United Kingdom",
    "Bosnia": "Bosnia and Herzegovina",
    "Yugoslavia": None,
}

_LOCATION_FILL = re.compile(r"^U\.S\.\s*-\s*(.+)$", re.I)
_STATE_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI",
    "wyoming": "WY", "district of columbia": "DC",
}


@dataclass
class UrlRef:
    name: str
    state: str | None
    country: str
    url: str
    home_url: str
    host: str
    kind: str  # official | facebook | community | pass_network | directory | other
    source: str  # us_sheet | snowguide | eu_directory | china_directory | canada_directory | pass_roster | parquet
    pass_key: str | None = None
    winter_sports_id: str | None = None
    match_score: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def classify_url(url: str) -> str:
    host = host_of(url)
    if not host:
        return "other"
    if "facebook.com" in host:
        return "facebook"
    if any(host == h or host.endswith("." + h) for h in PASS_NETWORK_HOSTS):
        return "pass_network"
    if any(d in host for d in DIRECTORY_SEED_HOSTS):
        return "directory"
    if host.endswith(".gov") or "armymwr.com" in host or host.endswith(".mil"):
        return "community"
    if host.endswith(".edu") or host.endswith(".org"):
        return "community"
    if re.search(r"county|cityof|parksandrec|recreation\.|townof", host):
        return "community"
    return "official"


def home_url_for(url: str) -> str:
    """Ticket/pass deep links become the site root; Facebook keeps the full path."""
    raw = (url or "").strip()
    if not raw:
        return ""
    if not raw.startswith("http"):
        raw = "https://" + raw
    kind = classify_url(raw)
    parts = urlparse(raw)
    host = (parts.hostname or "").lower()
    if not host:
        return raw
    if kind == "facebook":
        # Keep /page path; drop query noise.
        path = parts.path.rstrip("/") or ""
        return urlunparse(("https", host, path, "", "", ""))
    if kind == "pass_network":
        return urlunparse(("https", host, "/", "", "", ""))
    # Prefer apex home for season-pass / tickets deep links.
    return urlunparse(("https", host, "/", "", "", ""))


def seed_url_for(ref: UrlRef) -> str | None:
    """URL research should fetch first. Pass-network and directory rows are not site seeds."""
    if ref.kind in {"pass_network", "directory"}:
        return None
    if ref.kind == "facebook":
        return ref.url
    return ref.home_url or ref.url


def _col_row(ref: str) -> tuple[str, int] | None:
    m = re.match(r"^([A-Z]+)(\d+)$", ref or "")
    if not m:
        return None
    return m.group(1), int(m.group(2))


def _shared_strings(z: zipfile.ZipFile) -> list[str]:
    root = ET.fromstring(z.read("xl/sharedStrings.xml"))
    out: list[str] = []
    for si in root.findall("m:si", _NS):
        texts = [t.text or "" for t in si.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")]
        out.append("".join(texts))
    return out


def read_sheet_name_links(xlsx_path: Path) -> list[dict[str, str]]:
    """Location + Ski Area name + hyperlink from column B of the Active U.S. sheet."""
    rows: list[dict[str, str]] = []
    with zipfile.ZipFile(xlsx_path) as z:
        shared = _shared_strings(z)
        sheet = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))
        rels: dict[str, str] = {}
        for rel in ET.fromstring(z.read("xl/worksheets/_rels/sheet1.xml.rels")):
            rid = rel.attrib.get("Id")
            tgt = rel.attrib.get("Target")
            if rid and tgt:
                rels[rid] = tgt
        href_by_ref: dict[str, str] = {}
        for hl in sheet.iter():
            if not hl.tag.endswith("hyperlink"):
                continue
            ref = hl.attrib.get("ref") or ""
            rid = hl.attrib.get(_REL_NS + "id")
            tgt = rels.get(rid or "", "")
            if ref.startswith("B") and tgt.startswith("http"):
                href_by_ref[ref] = tgt
        cells: dict[str, str] = {}
        for cell in sheet.findall(".//m:c", _NS):
            ref = cell.attrib.get("r")
            node = cell.find("m:v", _NS)
            if not ref or node is None or node.text is None:
                continue
            if cell.attrib.get("t") == "s":
                cells[ref] = shared[int(node.text)]
            else:
                cells[ref] = node.text

    fill_state = ""
    fill_country = "United States of America"
    # Data starts at Excel row 9 (header is row 8).
    max_row = max(( _col_row(r)[1] for r in cells if _col_row(r)), default=0)
    for row_i in range(9, max_row + 1):
        loc = (cells.get(f"A{row_i}") or "").strip()
        name = (cells.get(f"B{row_i}") or "").strip()
        url = href_by_ref.get(f"B{row_i}", "").strip()
        if loc:
            m = _LOCATION_FILL.match(loc)
            if m:
                fill_state = m.group(1).strip()
                fill_country = "United States of America"
            elif loc.lower().startswith("canada"):
                fill_state = loc.split("-", 1)[-1].strip() if "-" in loc else ""
                fill_country = "Canada"
            else:
                fill_state = loc
        if not name or not url:
            continue
        rows.append(
            {
                "name": name,
                "state": fill_state,
                "country": fill_country,
                "url": url,
            }
        )
    return rows


def refs_from_sheet(xlsx_path: Path) -> list[UrlRef]:
    out: list[UrlRef] = []
    for row in read_sheet_name_links(xlsx_path):
        url = row["url"]
        kind = classify_url(url)
        out.append(
            UrlRef(
                name=row["name"],
                state=row["state"] or None,
                country=row["country"],
                url=url,
                home_url=home_url_for(url),
                host=host_of(home_url_for(url) or url),
                kind=kind,
                source="us_sheet",
            )
        )
    return out


def refs_from_pass_rosters(roster_dir: Path) -> list[UrlRef]:
    out: list[UrlRef] = []
    if not roster_dir.is_dir():
        return out
    for path in sorted(roster_dir.glob("*.jsonl")):
        if path.name in {"chips.jsonl"}:
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            url = str(row.get("listed_url") or "").strip()
            name = str(row.get("listed_name") or row.get("atlas_name") or "").strip()
            if not url or not name:
                continue
            kind = classify_url(url)
            # Destination list pages on the pass operator are pass_network; keep them tagged.
            if path.stem and kind == "official" and any(
                host_of(url) == h or (host_of(url) or "").endswith("." + h) for h in PASS_NETWORK_HOSTS
            ):
                kind = "pass_network"
            out.append(
                UrlRef(
                    name=name,
                    state=(str(row.get("state") or "").strip() or None),
                    country=str(row.get("country") or "United States of America").strip() or "United States of America",
                    url=url,
                    home_url=home_url_for(url),
                    host=host_of(home_url_for(url) or url),
                    kind=kind,
                    source="pass_roster",
                    pass_key=str(row.get("pass_key") or path.stem),
                    winter_sports_id=str(row.get("winter_sports_id") or "").strip() or None,
                    match_score=float(row["match_score"]) if row.get("match_score") is not None else None,
                )
            )
    return out


def _clean_listed_url(raw: str) -> str:
    href = (raw or "").strip().rstrip("\"'")
    if href.startswith("hhttps://"):
        href = "https://" + href[9:]
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        return "https:" + href
    return ""


def parse_european_directory_html(html_path: Path) -> list[dict[str, str]]:
    """Country-section name→URL rows from ski-ski-ski European-Ski-Resorts.html."""
    text = html_path.read_text(encoding="latin-1", errors="replace")
    parts = re.split(r"<b>([^<]{2,40})</b>", text, flags=re.I)
    skip_headers = {
        "Ski Shops",
        "Contact Us",
        "Resorts",
        "Travel",
        "Weather",
        "News ",
        "Clubs ",
        "Tips ",
        "Site Map",
        "News",
        "Clubs",
        "Tips",
    }
    rows: list[dict[str, str]] = []
    i = 1
    while i + 1 < len(parts):
        section = parts[i].strip()
        body = parts[i + 1]
        i += 2
        if section in skip_headers:
            continue
        country = _EU_SECTION_COUNTRY.get(section, section)
        if not country:
            continue
        for m in re.finditer(
            r'<a\s+HREF\s*=\s*["\']?([^"\'>\s]+)["\']?\s+[^>]*>([^<]+)</a>',
            body,
            re.I,
        ):
            href = _clean_listed_url(m.group(1))
            name = re.sub(r"\s+", " ", m.group(2)).strip()
            if not href or not name:
                continue
            if "ski-ski-ski.com" in href.lower():
                continue
            if href.lower().endswith(".html") and "://" not in href:
                continue
            rows.append({"name": name, "country": country, "url": href, "section": section})
    return rows


def refs_from_european_directory(
    path: Path | None = DEFAULT_EU_DIR,
    *,
    html_path: Path | None = DEFAULT_EU_HTML,
) -> list[UrlRef]:
    """ski-ski-ski.com European directory seeds (JSONL preferred; HTML fallback)."""
    raw_rows: list[dict[str, str]] = []
    if path and path.is_file():
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            name = str(row.get("name") or "").strip()
            url = _clean_listed_url(str(row.get("url") or ""))
            country = str(row.get("country") or "").strip()
            if name and url and country:
                raw_rows.append({"name": name, "country": country, "url": url})
    elif html_path and html_path.is_file():
        raw_rows = parse_european_directory_html(html_path)

    out: list[UrlRef] = []
    for row in raw_rows:
        url = row["url"]
        kind = classify_url(url)
        out.append(
            UrlRef(
                name=row["name"],
                state=None,
                country=row["country"],
                url=url,
                home_url=home_url_for(url),
                host=host_of(home_url_for(url) or url),
                kind=kind,
                source="eu_directory",
            )
        )
    return out


def _snowguide_rows(csv_path: Path) -> list[dict[str, str]]:
    text = csv_path.read_text(encoding="utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    return list(csv.DictReader(lines))


def refs_from_snowguide(csv_path: Path | None = DEFAULT_SNOWGUIDE) -> list[UrlRef]:
    """OpenSkiData / SnowGuide website column — worldwide, strongest for EU + Japan."""
    if not csv_path or not csv_path.is_file():
        return []
    out: list[UrlRef] = []
    for row in _snowguide_rows(csv_path):
        url = _clean_listed_url(str(row.get("website") or "").strip())
        if not url:
            continue
        iso = (row.get("country") or "").strip().upper()
        country = _ISO_COUNTRY.get(iso, iso)
        name = str(row.get("display_name") or "").strip()
        if not name:
            # "日本語, English Name" → prefer Latin side.
            raw = str(row.get("name") or "").strip()
            if "," in raw:
                left, right = raw.split(",", 1)
                name = right.strip() or left.strip()
            else:
                name = raw
        if not name:
            continue
        region = str(row.get("region") or "").strip() or None
        kind = classify_url(url)
        out.append(
            UrlRef(
                name=name,
                state=region,
                country=country,
                url=url,
                home_url=home_url_for(url),
                host=host_of(home_url_for(url) or url),
                kind=kind,
                source="snowguide",
            )
        )
        # Also index local / alt names so JP kanji atlas rows still hit.
        alt = str(row.get("alt_names") or "").strip()
        local = str(row.get("name") or "").strip()
        extras: list[str] = []
        if alt and alt != name:
            extras.append(alt)
        if local and local != name and "," in local:
            extras.append(local.split(",", 1)[0].strip())
        elif local and local != name:
            extras.append(local)
        for extra in extras:
            if not extra or squash(core_name(extra)) == squash(core_name(name)):
                continue
            out.append(
                UrlRef(
                    name=extra,
                    state=region,
                    country=country,
                    url=url,
                    home_url=home_url_for(url),
                    host=host_of(home_url_for(url) or url),
                    kind=kind,
                    source="snowguide",
                )
            )
    return out


def _state_key(state: str | None) -> str:
    if not state:
        return ""
    low = state.strip().lower()
    if len(low) == 2:
        return low
    return _STATE_ABBR.get(low, low).lower()


def _prefer_key(ref: UrlRef) -> tuple[int, int, int]:
    """Higher is better when several refs match one resort."""
    kind_rank = {
        "official": 50,
        "community": 40,
        "facebook": 20,
        "other": 10,
        "pass_network": 0,
        "directory": 0,
    }
    source_rank = {
        "us_sheet": 40,
        "china_directory": 35,  # Huaxue-curated CN sites beat older OSM websites
        "canada_directory": 32,
        "snowguide": 30,
        "eu_directory": 25,
        "parquet": 20,
        "pass_roster": 10,
    }
    return (kind_rank.get(ref.kind, 0), source_rank.get(ref.source, 0), int(1000 * (ref.match_score or 0)))


def _country_key(country: str | None) -> str:
    return re.sub(r"[^a-z]", "", (country or "").lower())


def _name_key(name: str | None) -> str:
    """ASCII squash when possible; keep CJK/local script otherwise (squash drops non-ASCII)."""
    raw = (name or "").strip()
    if not raw:
        return ""
    key = squash(core_name(raw))
    if key:
        return key
    return re.sub(r"\s+", "", raw.casefold())


def refs_from_china_directory(path: Path | None = DEFAULT_CHINA_DIR) -> list[UrlRef]:
    """Huaxue / SnowGuide-CN / ski-ski-ski China name→URL seeds."""
    if not path or not path.is_file():
        return []
    out: list[UrlRef] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        url = _clean_listed_url(str(row.get("url") or ""))
        name = str(row.get("name") or "").strip()
        if not url or not name:
            continue
        kind = classify_url(url)
        region = str(row.get("region") or "").strip() or None
        out.append(
            UrlRef(
                name=name,
                state=region,
                country="China",
                url=url,
                home_url=home_url_for(url),
                host=host_of(home_url_for(url) or url),
                kind=kind,
                source="china_directory",
            )
        )
        zh = str(row.get("name_zh") or "").strip()
        if zh and squash(core_name(zh)) != squash(core_name(name)):
            out.append(
                UrlRef(
                    name=zh,
                    state=region,
                    country="China",
                    url=url,
                    home_url=home_url_for(url),
                    host=host_of(home_url_for(url) or url),
                    kind=kind,
                    source="china_directory",
                )
            )
    return out


def refs_from_canada_directory(path: Path | None = DEFAULT_CANADA_DIR) -> list[UrlRef]:
    """ski-ski-ski Western/Eastern Canada province name→URL seeds."""
    if not path or not path.is_file():
        return []
    out: list[UrlRef] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        url = _clean_listed_url(str(row.get("url") or ""))
        name = str(row.get("name") or "").strip()
        if not url or not name:
            continue
        # Strip trailing "(XC)" marker for matching; keep URL as thin seed.
        match_name = re.sub(r"\s*\(XC\)\s*$", "", name, flags=re.I).strip() or name
        kind = classify_url(url)
        if host_of(url) and (host_of(url) or "").endswith(".gc.ca"):
            kind = "community"
        state = str(row.get("state") or "").strip() or None
        out.append(
            UrlRef(
                name=match_name,
                state=state,
                country="Canada",
                url=url,
                home_url=home_url_for(url),
                host=host_of(home_url_for(url) or url),
                kind=kind,
                source="canada_directory",
            )
        )
    return out


def match_refs_to_atlas(refs: list[UrlRef], atlas_rows: list[dict[str, Any]]) -> list[UrlRef]:
    atlas = atlas_index(atlas_rows)
    by_country: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in atlas:
        by_country[_country_key(str(item.get("country") or ""))].append(item)
    matched: list[UrlRef] = []
    for ref in refs:
        pool = atlas
        ck = _country_key(ref.country)
        if ck and by_country.get(ck):
            pool = by_country[ck]
        hit, score, _rivals = match_listed(ref.name, pool)
        if hit is None:
            # Keep unmatched sheet rows for name/state lookup at runtime.
            matched.append(ref)
            continue
        # Prefer same-state when the sheet filled a state.
        if ref.state and hit.get("state"):
            if _state_key(ref.state) and _state_key(str(hit["state"])) and _state_key(ref.state) != _state_key(str(hit["state"])):
                # Soft reject cross-state exact-ish matches below perfect.
                if score < 0.98:
                    matched.append(ref)
                    continue
        # Soft reject cross-country collisions (common short names: Alps, Park, etc.).
        if ref.country and hit.get("country"):
            if _country_key(ref.country) and _country_key(str(hit["country"])):
                if _country_key(ref.country) != _country_key(str(hit["country"])) and score < 0.98:
                    matched.append(ref)
                    continue
        updated = UrlRef(
            name=ref.name,
            state=ref.state or (str(hit.get("state") or "").strip() or None),
            country=ref.country or str(hit.get("country") or "") or "United States of America",
            url=ref.url,
            home_url=ref.home_url,
            host=ref.host,
            kind=ref.kind,
            source=ref.source,
            pass_key=ref.pass_key,
            winter_sports_id=str(hit.get("winter_sports_id") or "").strip() or None,
            match_score=round(score, 3),
        )
        matched.append(updated)
    return matched


def write_refs(refs: list[UrlRef], directory: Path = REF_DIR) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    jsonl = directory / "refs.jsonl"
    jsonl.write_text(
        "".join(json.dumps(ref.to_dict(), ensure_ascii=False) + "\n" for ref in refs),
        encoding="utf-8",
    )
    by_id: dict[str, dict[str, Any]] = {}
    for ref in refs:
        seed = seed_url_for(ref)
        if not ref.winter_sports_id or not seed:
            continue
        payload = ref.to_dict()
        payload["seed_url"] = seed
        prev = by_id.get(ref.winter_sports_id)
        if prev is None:
            by_id[ref.winter_sports_id] = payload
            continue
        prev_ref = UrlRef(**{k: prev.get(k) for k in UrlRef.__dataclass_fields__})
        if _prefer_key(ref) > _prefer_key(prev_ref):
            by_id[ref.winter_sports_id] = payload
    by_path = directory / "by_winter_sports_id.json"
    by_path.write_text(json.dumps(by_id, ensure_ascii=False, indent=2), encoding="utf-8")
    return jsonl, by_path


class UrlRefIndex:
    """In-memory lookup used by the pipeline at resort start."""

    def __init__(self, refs: list[UrlRef] | None = None) -> None:
        self.by_id: dict[str, UrlRef] = {}
        self.by_key: dict[str, list[UrlRef]] = defaultdict(list)
        for ref in refs or []:
            self.add(ref)

    def add(self, ref: UrlRef) -> None:
        if ref.winter_sports_id and seed_url_for(ref):
            prev = self.by_id.get(ref.winter_sports_id)
            if prev is None or _prefer_key(ref) > _prefer_key(prev):
                self.by_id[ref.winter_sports_id] = ref
        key = _name_key(ref.name)
        if key:
            self.by_key[key].append(ref)

    @classmethod
    def load(cls, path: Path | None = None) -> "UrlRefIndex":
        jsonl = path or DEFAULT_JSONL
        refs: list[UrlRef] = []
        if jsonl.is_file():
            for line in jsonl.read_text(encoding="utf-8", errors="replace").splitlines():
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                refs.append(UrlRef(**{k: row.get(k) for k in UrlRef.__dataclass_fields__}))
        return cls(refs)

    def lookup(
        self,
        *,
        name: str | None = None,
        state: str | None = None,
        country: str | None = None,
        winter_sports_id: str | None = None,
    ) -> UrlRef | None:
        if winter_sports_id and winter_sports_id in self.by_id:
            return self.by_id[winter_sports_id]
        if not name:
            return None
        key = _name_key(name)
        candidates = list(self.by_key.get(key) or [])
        if not candidates and key:
            # Containment keys only (Loveland Basin ↔ Loveland / CJK substrings).
            for other, group in self.by_key.items():
                if key in other or other in key:
                    if min(len(key), len(other)) < 6:
                        continue
                    candidates.extend(group)
        if not candidates:
            return None
        scored: list[tuple[tuple[int, int, int, float, int], UrlRef]] = []
        for ref in candidates:
            if not seed_url_for(ref):
                continue
            if state and ref.state and _state_key(state) and _state_key(ref.state):
                if _state_key(state) != _state_key(ref.state):
                    continue
            country_ok = 1
            if country and ref.country and _country_key(country) and _country_key(ref.country):
                if _country_key(country) != _country_key(ref.country):
                    continue
            name_score = 1.0 if _name_key(ref.name) == key else 0.9
            prefer = _prefer_key(ref)
            scored.append(((prefer[0], prefer[1], prefer[2], name_score, country_ok), ref))
        if not scored:
            return None
        scored.sort(key=lambda p: p[0], reverse=True)
        return scored[0][1]


_INDEX: UrlRefIndex | None = None


def get_index() -> UrlRefIndex:
    global _INDEX
    if _INDEX is None:
        _INDEX = UrlRefIndex.load()
    return _INDEX


def lookup_seed_url(
    *,
    name: str | None = None,
    state: str | None = None,
    country: str | None = None,
    winter_sports_id: str | None = None,
) -> str | None:
    ref = get_index().lookup(
        name=name, state=state, country=country, winter_sports_id=winter_sports_id
    )
    return seed_url_for(ref) if ref else None


def build_refs(
    *,
    sheet: Path | None = DEFAULT_SHEET,
    snowguide: Path | None = DEFAULT_SNOWGUIDE,
    eu_directory: Path | None = DEFAULT_EU_DIR,
    china_directory: Path | None = DEFAULT_CHINA_DIR,
    canada_directory: Path | None = DEFAULT_CANADA_DIR,
    roster_dir: Path | None = None,
    parquet_rows: list[dict[str, Any]] | None = None,
    out_dir: Path = REF_DIR,
) -> dict[str, Any]:
    refs: list[UrlRef] = []
    if sheet and sheet.is_file():
        refs.extend(refs_from_sheet(sheet))
    if snowguide and snowguide.is_file():
        refs.extend(refs_from_snowguide(snowguide))
    if eu_directory and (eu_directory.is_file() or DEFAULT_EU_HTML.is_file()):
        refs.extend(refs_from_european_directory(eu_directory if eu_directory.is_file() else None))
    if china_directory and china_directory.is_file():
        refs.extend(refs_from_china_directory(china_directory))
    if canada_directory and canada_directory.is_file():
        refs.extend(refs_from_canada_directory(canada_directory))
    roster = roster_dir or output_path("pass_rosters")
    refs.extend(refs_from_pass_rosters(roster))
    if parquet_rows:
        refs = match_refs_to_atlas(refs, parquet_rows)
        # Parquet website cells as last-resort seeds.
        for row in parquet_rows:
            name = str(row.get("english_name") or row.get("name") or "").strip()
            url = ""
            for key in ("website", "official_website", "osm_website", "contact_website", "url"):
                val = str(row.get(key) or "").strip()
                if val and ("." in val or val.startswith("http")):
                    url = val if val.startswith("http") else f"https://{val}"
                    break
            if not name or not url:
                continue
            refs.append(
                UrlRef(
                    name=name,
                    state=str(row.get("state") or "").strip() or None,
                    country=str(row.get("country") or "").strip() or "United States of America",
                    url=url,
                    home_url=home_url_for(url),
                    host=host_of(home_url_for(url) or url),
                    kind=classify_url(url),
                    source="parquet",
                    winter_sports_id=str(row.get("winter_sports_id") or "").strip() or None,
                    match_score=1.0,
                )
            )
    jsonl, by_id = write_refs(refs, out_dir)
    seedable = sum(1 for r in refs if seed_url_for(r))
    with_id = sum(1 for r in refs if r.winter_sports_id and seed_url_for(r))
    kinds = defaultdict(int)
    sources = defaultdict(int)
    for r in refs:
        kinds[r.kind] += 1
        sources[r.source] += 1
    summary = {
        "refs": len(refs),
        "seedable": seedable,
        "matched_ids": with_id,
        "kinds": dict(kinds),
        "sources": dict(sources),
        "jsonl": str(jsonl),
        "by_id": str(by_id),
    }
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sheet", type=Path, default=DEFAULT_SHEET)
    parser.add_argument("--snowguide", type=Path, default=DEFAULT_SNOWGUIDE)
    parser.add_argument("--eu-directory", type=Path, default=DEFAULT_EU_DIR)
    parser.add_argument("--china-directory", type=Path, default=DEFAULT_CHINA_DIR)
    parser.add_argument("--canada-directory", type=Path, default=DEFAULT_CANADA_DIR)
    parser.add_argument("--rosters", type=Path, default=None)
    parser.add_argument("--parquet", type=Path, default=Path("output/combined/ski_areas_analyzed.parquet"))
    parser.add_argument("--out", type=Path, default=REF_DIR)
    parser.add_argument("--no-parquet", action="store_true")
    parser.add_argument("--no-snowguide", action="store_true")
    parser.add_argument("--no-eu-directory", action="store_true")
    parser.add_argument("--no-china-directory", action="store_true")
    parser.add_argument("--no-canada-directory", action="store_true")
    args = parser.parse_args(argv)

    rows: list[dict[str, Any]] | None = None
    if not args.no_parquet and args.parquet.is_file():
        try:
            import pandas as pd  # type: ignore

            frame = pd.read_parquet(args.parquet)
            rows = frame.to_dict(orient="records")
        except Exception as exc:
            print(f"parquet load failed ({exc}); writing unmatched sheet/roster refs only", file=sys.stderr)

    summary = build_refs(
        sheet=args.sheet if args.sheet.is_file() else None,
        snowguide=None if args.no_snowguide else (args.snowguide if args.snowguide.is_file() else None),
        eu_directory=None if args.no_eu_directory else args.eu_directory,
        china_directory=None
        if args.no_china_directory
        else (args.china_directory if args.china_directory.is_file() else None),
        canada_directory=None
        if args.no_canada_directory
        else (args.canada_directory if args.canada_directory.is_file() else None),
        roster_dir=args.rosters,
        parquet_rows=rows,
        out_dir=args.out,
    )
    print(json.dumps(summary, indent=2))
    # Smoke known NA misses + a few EU/JP/CN/CA seeds.
    idx = UrlRefIndex.load(args.out / "refs.jsonl")
    for name, state, country in (
        ("Loveland Basin", "Colorado", "United States of America"),
        ("Whiteface", "New York", "United States of America"),
        ("Dry Hill", "New York", "United States of America"),
        ("Brantling", "New York", "United States of America"),
        ("Cloudmont", "Alabama", "United States of America"),
        ("Grandvalira", None, "Andorra"),
        ("Avoriaz", None, "France"),
        ("Ischgl-Samnaun", None, "Switzerland"),
        ("Adatara Kogen Ski Resort", None, "Japan"),
        ("Niseko United", None, "Japan"),
        ("Beidahu Ski Resort", None, "China"),
        ("Wanlong Ski Resort", None, "China"),
        ("Nanshan Ski Resort", None, "China"),
        ("北大湖滑雪度假区", None, "China"),
        ("Whistler Blackcomb", "British Columbia", "Canada"),
        ("Banff Mount Norquay", "Alberta", "Canada"),
        ("Blue Mountain Resort", "Ontario", "Canada"),
        ("Mont Tremblant", "Quebec", "Canada"),
    ):
        ref = idx.lookup(name=name, state=state, country=country)
        print(
            f"lookup {name}, {state or country}:",
            seed_url_for(ref) if ref else None,
            getattr(ref, "kind", None),
            getattr(ref, "source", None),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
