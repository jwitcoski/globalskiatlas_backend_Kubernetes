"""Fetch and read resort pages (live and archived)."""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from typing import Any

from .config import (
    ARCHIVE_PAGES,
    PAGE_EXCERPT_CHARS,
)
from .key_points import (
    _host,
    _host_matches_resort,
)
from .snippets import (
    _DOSSIER_WORDS,
    _STAT_RECAP,
    _name_needles,
    _page_is_this_resort,
    _snippet_score,
)


# Bot walls, login walls, maps, booking engines, and video. Their HTML has no usable prose.
_NO_FETCH = (
    "tripadvisor.",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "tiktok.com",
    "twitter.com",
    "x.com/",
    "pinterest.",
    "linkedin.com",
    "mapquest.com",
    "mapcarta.com",
    "booking.com",
    "expedia.",
    "travelocity.",
    "hotels.com",
    "cozycozy.com",
    "allevents.in",
    "google.",
)


# Directory pages built from the same stats the atlas already prints. Read them last.
_STATS_SITES = (
    "snow-forecast.com",
    "skiresort.com",
    "skiday.co",
    "sknowed.com",
    "skifactor.net",
    "skicentral.com",
    "onthesnow.",
    "j2ski.com",
    "snowskiing.com",
    "peakvisor.com",
    "skivio.org",
    "mapcarta.com",
)


_BOILERPLATE = re.compile(
    r"cookie|subscribe|newsletter|sign up|log in|all rights reserved|privacy policy|"
    r"enable javascript|terms of use|skip to (main )?content|personal data|upgrade your browser|"
    r"playback is not supported",
    re.IGNORECASE,
)


_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)


class _BlockText(HTMLParser):
    """Visible text split at block tags. Scripts, menus, and footers are dropped."""

    _SKIP = {"script", "style", "noscript", "nav", "footer", "header", "form", "svg", "aside", "button", "select", "iframe"}
    _BLOCK = {
        "p", "li", "h1", "h2", "h3", "h4", "h5", "td", "th", "dd", "dt",
        "blockquote", "div", "section", "article", "br", "tr",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.blocks: list[str] = []
        self._buf: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        if tag in self._SKIP:
            self._skip += 1
        elif tag in self._BLOCK:
            self._flush()

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP:
            self._skip = max(0, self._skip - 1)
        elif tag in self._BLOCK:
            self._flush()

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self._buf.append(data)

    def _flush(self) -> None:
        text = re.sub(r"\s+", " ", " ".join(self._buf)).strip()
        self._buf = []
        if text:
            self.blocks.append(text)

    def close(self) -> None:
        super().close()
        self._flush()


def _fetch_html(url: str, timeout: float = 15.0) -> str | None:
    req = urllib.request.Request(url, headers={"User-Agent": _BROWSER_UA, "Accept": "text/html"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if "html" not in (resp.headers.get("Content-Type") or "").lower():
                return None
            raw = resp.read(2_000_000)
            charset = resp.headers.get_content_charset() or "utf-8"
    except Exception:
        return None
    if raw[:2] == b"\x1f\x8b":
        # Archive copies keep the site's original gzip bytes.
        try:
            raw = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(raw, 8_000_000)
        except zlib.error:
            return None
    return raw.decode(charset, errors="replace")


def _fetch_archived(url: str, timeout: float = 25.0) -> str | None:
    """Latest Internet Archive copy. Some resort sites refuse anything but a real browser."""
    api = "https://archive.org/wayback/available?url=" + urllib.parse.quote(url, safe=":/")
    for attempt in range(2):
        try:
            with urllib.request.urlopen(api, timeout=timeout) as resp:
                closest = (json.loads(resp.read()).get("archived_snapshots") or {}).get("closest") or {}
            break
        except urllib.error.HTTPError as exc:
            if exc.code != 429 or attempt:
                return None
            time.sleep(10)
        except Exception:
            return None
    stamp = closest.get("timestamp")
    if not closest.get("available") or not stamp:
        return None
    snapshot = f"https://web.archive.org/web/{stamp}id_/{url}"
    html = _fetch_html(snapshot, timeout=timeout)
    if html is None:
        time.sleep(10)
        html = _fetch_html(snapshot, timeout=timeout)
    return html


def _page_blocks(html: str) -> list[str]:
    parser = _BlockText()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        return []
    seen: set[str] = set()
    blocks: list[str] = []
    for block in parser.blocks:
        if len(block) < 60 or len(block.split()) < 8 or _BOILERPLATE.search(block):
            continue
        key = block.lower()
        if key in seen:
            continue
        seen.add(key)
        blocks.append(block)
    return blocks


def _squash(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _distinct_tokens(name: str) -> list[str]:
    return [needle for needle in _name_needles(name) if " " not in needle]


def _page_about_resort(snippet: dict[str, Any], name: str) -> bool:
    """The whole page is about this resort, not a town page or a list of every hill."""
    url = str(snippet.get("url") or "")
    title = str(snippet.get("title") or "").lower()
    skiing = bool(re.search(r"\bski|snow", title))
    # experiencemiddlebury.com shares the town name. The resort's own pages say ski or snow.
    if _host_matches_resort(_host(url), url, name) and skiing:
        return True
    if _squash(name) and _squash(name) in _squash(title):
        return True
    tokens = _distinct_tokens(name)
    return bool(tokens) and all(t in title for t in tokens) and skiing


def _block_mentions_resort(block: str, name: str) -> bool:
    low = block.lower()
    if _squash(name) and _squash(name) in _squash(low):
        return True
    tokens = _distinct_tokens(name)
    return bool(tokens) and all(t in low for t in tokens) and bool(
        re.search(r"\bski|snow|slope|lift|trail|lodge", low)
    )


def _page_excerpt(blocks: list[str], *, name: str, about_resort: bool, chars: int) -> str:
    """The best paragraphs for a visitor, back in page order. A list page keeps only its lines about this resort."""
    scored: list[tuple[int, int, str]] = []
    for index, block in enumerate(blocks):
        low = block.lower()
        mentions = _block_mentions_resort(block, name)
        if not about_resort and not mentions:
            continue
        score = sum(1 for word in _DOSSIER_WORDS if word in low) + (3 if mentions else 0)
        if re.search(r"\$\s?\d|\b(18|19|20)\d{2}\b|\bmiles?\b|\bminutes\b|\bkm\b", low):
            score += 2
        if _STAT_RECAP.search(low):
            score -= 1
        scored.append((score, index, block))
    scored.sort(key=lambda item: (-item[0], item[1]))
    picked: list[tuple[int, str]] = []
    used = 0
    for score, index, block in scored:
        if score <= 0:
            break
        piece = block if len(block) <= chars else block[:chars].rsplit(" ", 1)[0] + "…"
        if used + len(piece) > chars:
            continue
        picked.append((index, piece))
        used += len(piece) + 1
    picked.sort()
    return "\n".join(piece for _, piece in picked)


def fetch_resort_pages(snippets: list[dict[str, Any]], *, name: str, state: str | None, limit: int) -> int:
    """Replace the search blurb of the best resort pages with paragraphs read from the page."""
    remaining = limit - sum(1 for s in snippets if s.get("fetched"))
    if remaining <= 0:
        return 0
    candidates: list[tuple[int, dict[str, Any]]] = []
    seen: set[str] = set()
    for snippet in snippets:
        url = str(snippet.get("url") or "")
        low = url.lower()
        if snippet.get("fetched") is not None or not low.startswith("http") or low.endswith(".pdf"):
            continue
        if any(block in low for block in _NO_FETCH):
            continue
        key = low.split("#", 1)[0].rstrip("/")
        if key in seen:
            continue
        if not _page_is_this_resort(f"{snippet.get('title') or ''} {snippet.get('text') or ''}", name, state):
            continue
        seen.add(key)
        score = _snippet_score(snippet, name)
        about = _page_about_resort(snippet, name)
        if "wikipedia.org" in low:
            score += 14
        if about:
            score += 8
        if about and _host_matches_resort(_host(url), url, name):
            score += 6
        if any(site in low for site in _STATS_SITES):
            score -= 10
        candidates.append((score, snippet))
    candidates.sort(key=lambda item: -item[0])
    chosen = [snippet for _, snippet in candidates[: remaining + 4]]
    with ThreadPoolExecutor(max_workers=6) as pool:
        pages = list(pool.map(lambda s: _fetch_html(str(s["url"])), chosen))
    # The archive rate-limits hard, so only the resort's own refused pages go there, one at a time.
    archived = 0
    for i, snippet in enumerate(chosen):
        url = str(snippet["url"])
        if pages[i] or archived >= ARCHIVE_PAGES or not _host_matches_resort(_host(url), url, name):
            continue
        archived += 1
        pages[i] = _fetch_archived(url)
    fetched = 0
    for snippet, html in zip(chosen, pages):
        snippet["fetched"] = False
        if fetched >= remaining or not html:
            continue
        excerpt = _page_excerpt(
            _page_blocks(html),
            name=name,
            about_resort=_page_about_resort(snippet, name),
            chars=PAGE_EXCERPT_CHARS,
        )
        if len(excerpt) < 200:
            continue
        snippet["blurb"] = snippet.get("text")
        snippet["text"] = excerpt
        snippet["fetched"] = True
        fetched += 1
    return fetched
