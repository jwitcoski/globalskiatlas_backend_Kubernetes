"""Durable source registry, bounded fetching, and the search adapter.

Search results and fetched pages are kept in separate lists. Source ids (S0001...) are assigned once,
are never renumbered, and are the only ids a fact may cite. Page text is untrusted evidence.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 GlobalSkiAtlasResearch/1.0"
)

# Bot walls, login walls, maps, and booking engines. Not evidence and not worth a request.
NO_FETCH_HOSTS = (
    "tripadvisor.", "facebook.com", "instagram.com", "youtube.com", "tiktok.com", "twitter.com",
    "x.com", "pinterest.", "linkedin.com", "mapquest.com", "mapcarta.com", "booking.com", "expedia.",
    "travelocity.", "hotels.com", "cozycozy.com", "allevents.in", "google.", "yelp.com", "reddit.com",
)

# Directory and aggregator sites: never the official site, lowest source tier.
DIRECTORY_HOSTS = (
    "snow-forecast.com", "skiresort.info", "skiresort.com", "skiday.co", "sknowed.com", "skifactor.net",
    "skicentral.com", "onthesnow.", "j2ski.com", "snowskiing.com", "peakvisor.com", "skivio.org",
    "grokipedia.com", "wikidata.org", "snow-online.com", "liftopia.com", "vrbo.com", "airbnb.",
)

CHALLENGE_MARKERS = re.compile(
    r"verify you are (?:a )?human|are you a robot|captcha|access denied|attention required|"
    r"checking your browser|enable javascript and cookies|request blocked",
    re.IGNORECASE,
)

INJECTION = re.compile(
    r"ignore (?:all |any |the )?(?:previous|prior|above) (?:instructions|prompts?)|disregard (?:all |the )?(?:previous|prior)|"
    r"you are (?:now )?an? (?:ai|assistant|language model)|system prompt|as an ai\b|new instructions:",
    re.IGNORECASE,
)

_BOILERPLATE = re.compile(
    r"^(?:cookie|accept (?:all )?cookies|subscribe|sign up|log ?in|skip to (?:main )?content|menu|search)\b|"
    r"all rights reserved|privacy policy|terms (?:of use|and conditions)|enable javascript|upgrade your browser",
    re.IGNORECASE,
)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def host_of(url: str) -> str:
    try:
        host = urllib.parse.urlsplit(url).hostname or ""
    except ValueError:
        return ""
    return host.lower().removeprefix("www.")


def host_in(host: str, domain: str | None) -> bool:
    return bool(domain) and (host == domain or host.endswith("." + str(domain)))


def canonicalize(url: str) -> str:
    """Dedup key: scheme-less, no fragment, no tracking params, no trailing slash, no www."""
    try:
        parts = urllib.parse.urlsplit(url.strip())
    except ValueError:
        return url.strip().lower()
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=False)
    kept = sorted((k, v) for k, v in query if not k.lower().startswith(("utm_", "fbclid", "gclid", "mc_")))
    path = parts.path.rstrip("/") or ""
    return urllib.parse.urlunsplit(
        ("", (parts.hostname or "").lower().removeprefix("www."), path, urllib.parse.urlencode(kept), "")
    ).lstrip("/")


# ----- HTML parsing -------------------------------------------------------------------------------------------


class PageParser(HTMLParser):
    """Title, canonical link, meta description, visible text blocks, and every anchor."""

    _SKIP = {"script", "style", "noscript", "svg", "iframe", "template"}
    _CHROME = {"nav", "footer", "header", "aside", "form", "button", "select"}
    _BLOCK = {
        "p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th", "dd", "dt", "blockquote", "div",
        "section", "article", "br", "tr", "figcaption", "summary",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title = ""
        self.canonical = ""
        self.description = ""
        self.blocks: list[str] = []
        self.links: list[tuple[str, str]] = []
        self.script_count = 0
        self._buf: list[str] = []
        self._skip = 0
        self._chrome = 0
        self._in_title = False
        self._anchor: list[Any] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = dict(attrs)
        if tag == "script":
            self.script_count += 1
        if tag in self._SKIP:
            self._skip += 1
            return
        if tag == "title":
            self._in_title = True
        elif tag == "link" and (a.get("rel") or "").lower() == "canonical" and a.get("href"):
            self.canonical = a["href"] or ""
        elif tag == "meta" and (a.get("name") or a.get("property") or "").lower() in {"description", "og:description"}:
            if a.get("content") and not self.description:
                self.description = re.sub(r"\s+", " ", a["content"] or "").strip()
        elif tag == "a" and a.get("href"):
            self._anchor = [a["href"], []]
        if tag in self._CHROME:
            self._chrome += 1
            self._flush()
        elif tag in self._BLOCK:
            self._flush()

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP:
            self._skip = max(0, self._skip - 1)
            return
        if tag == "title":
            self._in_title = False
        elif tag == "a" and self._anchor is not None:
            text = re.sub(r"\s+", " ", " ".join(self._anchor[1])).strip()
            self.links.append((str(self._anchor[0]), text))
            self._anchor = None
        if tag in self._CHROME:
            self._chrome = max(0, self._chrome - 1)
            self._flush()
        elif tag in self._BLOCK:
            self._flush()

    def handle_data(self, data: str) -> None:
        if self._skip:
            return
        if self._in_title:
            self.title += data
            return
        if self._anchor is not None:
            self._anchor[1].append(data)
        if not self._chrome:
            self._buf.append(data)

    def _flush(self) -> None:
        text = re.sub(r"\s+", " ", " ".join(self._buf)).strip()
        self._buf = []
        if text:
            self.blocks.append(text)

    def close(self) -> None:
        super().close()
        self._flush()


def parse_page(html: str, base_url: str) -> dict[str, Any]:
    parser = PageParser()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        pass
    seen: set[str] = set()
    blocks: list[str] = []
    for block in ([parser.description] if parser.description else []) + parser.blocks:
        if len(block) < 12 or len(block.split()) < 2 or _BOILERPLATE.search(block):
            continue
        key = block.lower()
        if key in seen:
            continue
        seen.add(key)
        blocks.append(block)
    links: list[dict[str, str]] = []
    seen_links: set[str] = set()
    for href, text in parser.links:
        if href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        absolute = urllib.parse.urljoin(base_url, href).split("#", 1)[0]
        if not absolute.startswith("http") or absolute in seen_links:
            continue
        seen_links.add(absolute)
        links.append({"url": absolute, "text": text})
    canonical = urllib.parse.urljoin(base_url, parser.canonical) if parser.canonical else ""
    return {
        "title": re.sub(r"\s+", " ", parser.title).strip(),
        "canonical_url": canonical,
        "blocks": blocks,
        "text": "\n".join(blocks)[:60000],
        "links": links[:400],
        "script_count": parser.script_count,
    }


# ----- HTTP ---------------------------------------------------------------------------------------------------


@dataclass
class HttpResult:
    status_code: int | None = None
    content_type: str = ""
    body: str = ""
    final_url: str = ""
    error: str = ""


def http_get(url: str, timeout: float = 15.0) -> HttpResult:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(2_500_000)
            ctype = resp.headers.get("Content-Type") or ""
            charset = resp.headers.get_content_charset() or "utf-8"
            if raw[:2] == b"\x1f\x8b":
                raw = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(raw, 8_000_000)
            return HttpResult(resp.status, ctype, raw.decode(charset, errors="replace"), resp.geturl())
    except urllib.error.HTTPError as exc:
        return HttpResult(exc.code, exc.headers.get("Content-Type") or "" if exc.headers else "", "", url, f"HTTP {exc.code}")
    except Exception as exc:  # timeouts, DNS, TLS
        return HttpResult(None, "", "", url, f"{type(exc).__name__}: {exc}")


# ----- Search adapter -----------------------------------------------------------------------------------------


class DdgsSearch:
    """DuckDuckGo through the ddgs package already used by the rest of the pipeline."""

    def __init__(self, *, min_interval: float = 1.5, retries: int = 1, timeout: int = 20) -> None:
        try:
            from ddgs import DDGS  # type: ignore
        except ImportError:
            try:
                from duckduckgo_search import DDGS  # type: ignore
            except ImportError:
                DDGS = None  # type: ignore
        self._ddgs = DDGS
        self.min_interval = min_interval
        self.retries = retries
        self.timeout = timeout
        self._last = 0.0

    @property
    def available(self) -> bool:
        return self._ddgs is not None

    def __call__(self, query: str, max_results: int) -> list[dict[str, str]]:
        if self._ddgs is None:
            return []
        for attempt in range(self.retries + 1):
            wait = self.min_interval - (time.time() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()
            try:
                with self._ddgs(timeout=self.timeout) as ddgs:
                    rows = list(ddgs.text(query, max_results=max_results))
                return [
                    {
                        "url": str(r.get("href") or r.get("url") or "").strip(),
                        "title": str(r.get("title") or "").strip(),
                        "snippet": str(r.get("body") or "").strip(),
                    }
                    for r in rows
                ]
            except Exception as exc:
                print(f"  search failed ({query!r}, try {attempt + 1}): {exc}", file=sys.stderr, flush=True)
                time.sleep(3 * (attempt + 1))
        return []


# ----- Registry -----------------------------------------------------------------------------------------------


@dataclass
class Budgets:
    max_queries: int = 70
    max_fetches: int = 45
    results_per_query: int = 8
    urls_per_query: int = 2
    nav_pages: int = 12
    fetch_timeout: float = 15.0
    fetch_retries: int = 1
    host_interval: float = 1.0
    cache_ttl_days: float = 7.0


class SourceRegistry:
    def __init__(
        self,
        *,
        entity_id: str,
        budgets: Budgets | None = None,
        fetcher: Callable[[str, float], HttpResult] = http_get,
        cache_path: Path | None = None,
        log: Callable[[str], None] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.entity_id = entity_id
        self.budgets = budgets or Budgets()
        self.fetcher = fetcher
        self.cache_path = cache_path
        self.log = log or (lambda m: print(m, file=sys.stderr, flush=True))
        self._sleep = sleep
        self.search_results: list[dict[str, Any]] = []
        self.sources: dict[str, dict[str, Any]] = {}
        self.queries_run: list[str] = []
        self.fetches_used = 0
        self._by_url: dict[str, str] = {}
        self._by_hash: dict[str, str] = {}
        self._next_source = 1
        self._next_result = 1
        self._host_last: dict[str, float] = {}
        self._robots: dict[str, urllib.robotparser.RobotFileParser | None] = {}
        self._load()

    # -- persistence
    def _load(self) -> None:
        if not self.cache_path or not self.cache_path.is_file():
            return
        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if data.get("entity_id") != self.entity_id:
            return
        self.search_results = list(data.get("search_results") or [])
        self.sources = dict(data.get("sources") or {})
        self._next_result = len(self.search_results) + 1
        self._next_source = 1 + max((int(s[1:]) for s in self.sources if s[1:].isdigit()), default=0)
        for sid, rec in self.sources.items():
            self._by_url[canonicalize(rec.get("url", ""))] = sid
            if rec.get("canonical_url"):
                self._by_url[canonicalize(rec["canonical_url"])] = sid
            if rec.get("content_hash") and not rec.get("duplicate_of"):
                self._by_hash.setdefault(rec["content_hash"], sid)

    def save(self) -> None:
        if not self.cache_path:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"entity_id": self.entity_id, "search_results": self.search_results, "sources": self.sources}
        tmp = self.cache_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(self.cache_path)

    # -- search
    def queries_left(self) -> int:
        return self.budgets.max_queries - len(self.queries_run)

    def record_search(self, query: str, rows: list[dict[str, str]], *, topic: str, round_index: int) -> list[dict[str, Any]]:
        self.queries_run.append(query)
        out: list[dict[str, Any]] = []
        for rank, row in enumerate(rows, start=1):
            if not row.get("url"):
                continue
            rec = {
                "result_id": f"R{self._next_result:04d}",
                "query": query,
                "topic": topic,
                "round": round_index,
                "rank": rank,
                "url": row["url"],
                "title": row.get("title", ""),
                "snippet": row.get("snippet", ""),
                "retrieved_at": now_iso(),
            }
            self._next_result += 1
            self.search_results.append(rec)
            out.append(rec)
        return out

    # -- fetch
    def known_url(self, url: str) -> str | None:
        return self._by_url.get(canonicalize(url))

    def fetches_left(self) -> int:
        return self.budgets.max_fetches - self.fetches_used

    def _robots_allows(self, url: str) -> bool:
        parts = urllib.parse.urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        if origin not in self._robots:
            parser = urllib.robotparser.RobotFileParser()
            res = self.fetcher(origin + "/robots.txt", 6.0)
            if res.status_code == 200 and res.body:
                parser.parse(res.body.splitlines())
                self._robots[origin] = parser
            else:
                self._robots[origin] = None
        parser = self._robots[origin]
        return parser is None or parser.can_fetch(USER_AGENT, url)

    def _throttle(self, host: str) -> None:
        wait = self.budgets.host_interval - (time.time() - self._host_last.get(host, 0.0))
        if wait > 0:
            self._sleep(wait)
        self._host_last[host] = time.time()

    def fetch(self, url: str, *, reason: str, topic: str = "", force: bool = False) -> dict[str, Any] | None:
        """Fetch one page into the registry. Returns the source record, or None when the budget or policy refuses."""
        known = self.known_url(url)
        if known and not force:
            rec = self.sources[known]
            if self._fresh(rec):
                return rec
        host = host_of(url)
        if not url.lower().startswith("http") or url.lower().split("?")[0].endswith((".pdf", ".jpg", ".png", ".zip")):
            return None
        if any(bad in host + "/" for bad in NO_FETCH_HOSTS):
            return None
        if self.fetches_left() <= 0:
            self.log(f"    fetch budget exhausted, skipped {url}")
            return None
        self.fetches_used += 1
        if not self._robots_allows(url):
            return self._store(url, HttpResult(None, "", "", url, "disallowed by robots.txt"), None, "blocked", reason, topic)
        result = HttpResult()
        for attempt in range(self.budgets.fetch_retries + 1):
            self._throttle(host)
            result = self.fetcher(url, self.budgets.fetch_timeout)
            retryable = result.status_code is None or result.status_code >= 500
            if not retryable:
                break
            self._sleep(2.0 * (attempt + 1))
        return self._classify_and_store(url, result, reason, topic)

    def _fresh(self, rec: dict[str, Any]) -> bool:
        try:
            age = time.time() - datetime.strptime(rec["retrieval_timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
        except (KeyError, ValueError):
            return False
        ttl = self.budgets.cache_ttl_days * 86400
        return age < (ttl if rec.get("fetch_status") == "ok" else min(ttl, 86400))

    def _classify_and_store(self, url: str, res: HttpResult, reason: str, topic: str) -> dict[str, Any]:
        if res.status_code in (401, 403, 429):
            return self._store(url, res, None, "blocked", reason, topic)
        if res.status_code is None or res.status_code >= 400:
            return self._store(url, res, None, "failed", reason, topic)
        if res.content_type and "html" not in res.content_type.lower() and "xml" not in res.content_type.lower():
            return self._store(url, res, None, "non_html", reason, topic)
        page = parse_page(res.body, res.final_url or url)
        text = page["text"]
        if CHALLENGE_MARKERS.search(text[:1500]) and len(text) < 1500:
            return self._store(url, res, page, "blocked", reason, topic)
        if len(text) < 200:
            js = page["script_count"] >= 5 or re.search(r"__NEXT_DATA__|id=\"(?:root|app|__nuxt)\"|ng-app", res.body[:200000])
            return self._store(url, res, page, "js_dependent" if js else "empty", reason, topic)
        return self._store(url, res, page, "ok", reason, topic)

    def _store(
        self, url: str, res: HttpResult, page: dict[str, Any] | None, status: str, reason: str, topic: str
    ) -> dict[str, Any]:
        text = (page or {}).get("text", "") if status == "ok" else ""
        digest = hashlib.sha256(re.sub(r"\s+", " ", text).strip().lower().encode()).hexdigest() if text else ""
        duplicate_of = self._by_hash.get(digest) if digest else None
        existing = self.known_url(url)
        sid = existing or f"S{self._next_source:04d}"
        if not existing:
            self._next_source += 1
        rec = {
            "source_id": sid,
            "entity_id": self.entity_id,
            "url": url,
            "final_url": res.final_url or url,
            "canonical_url": (page or {}).get("canonical_url") or "",
            "title": (page or {}).get("title", ""),
            "retrieval_timestamp": now_iso(),
            "source_type": "unclassified",
            "text": text,
            "links": (page or {}).get("links", []) if status in ("ok", "js_dependent", "empty") else [],
            "fetch_status": status,
            "http_status": res.status_code,
            "error": res.error,
            "content_hash": digest,
            "duplicate_of": duplicate_of if duplicate_of != sid else None,
            "relevance": {"decision": "unassessed", "reason": ""},
            "fetch_reason": reason,
            "topic": topic,
        }
        self.sources[sid] = rec
        for key in (url, res.final_url, rec["canonical_url"]):
            if key:
                self._by_url[canonicalize(key)] = sid
        if digest and not duplicate_of:
            self._by_hash.setdefault(digest, sid)
        if status != "ok":
            self.log(f"    {status}: {url} {res.error}".rstrip())
        elif duplicate_of:
            self.log(f"    duplicate content: {url} (same as {duplicate_of})")
        return rec

    def get(self, source_id: str) -> dict[str, Any] | None:
        return self.sources.get(source_id)

    def summary(self) -> list[dict[str, Any]]:
        keep = ("source_id", "url", "canonical_url", "title", "retrieval_timestamp", "source_type", "fetch_status",
                "content_hash", "duplicate_of", "relevance", "fetch_reason", "topic")
        return [{**{k: r.get(k) for k in keep}, "text_chars": len(r.get("text") or "")} for r in self.sources.values()]
