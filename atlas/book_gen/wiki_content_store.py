"""Load Bedrock-generated wiki JSON and merge copy into book_gen page dicts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from atlas.book_gen.log_util import log
from atlas.book_gen.render_resort_fields import markdown_to_plain
from atlas.map_gen.wiki_page_id import wiki_state_page_id

_PLACEHOLDER_SNIPPET = "Add a description for this resort"


def resolve_wiki_content_path(
    repo_root: Path,
    book_config: dict[str, Any],
    key: str,
    default: str,
) -> Path | None:
    raw = book_config.get(key) or default
    if not raw:
        return None
    p = Path(str(raw))
    if not p.is_absolute():
        p = repo_root / p
    return p if p.is_file() else None


def _load_index(path: Path) -> dict[str, dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    items = data.get("items") or data.get("pages") or []
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        pid = item.get("pageId")
        if pid:
            out[str(pid)] = item
    return out


def _is_placeholder_content(content: str | None) -> bool:
    return _PLACEHOLDER_SNIPPET in (content or "")


def merge_resort_page(page: dict[str, Any], item: dict[str, Any] | None) -> bool:
    """Merge Bedrock resort copy into a page dict when copy is missing or placeholder."""
    if not item:
        return False
    md = (item.get("contentMarkdown") or "").strip()
    existing = (page.get("content") or "").strip()
    if md and (not existing or _is_placeholder_content(existing)):
        page["content"] = md
    elif not md:
        return False
    cat = item.get("resortSizeCategory")
    if cat and page.get("resortSizeCategory") in (None, "", "unknown"):
        page["resortSizeCategory"] = cat
    page["wikiContentSource"] = "bedrock"
    return bool(md)


class WikiContentStore:
    """Lazy-loaded indexes for resort and region Bedrock JSON exports."""

    def __init__(self, repo_root: Path, book_config: dict[str, Any]):
        self.repo_root = repo_root
        self.book_config = book_config
        self._resort_index: dict[str, dict[str, Any]] | None = None
        self._region_index: dict[str, dict[str, Any]] | None = None

    @classmethod
    def from_config(cls, repo_root: Path, book_config: dict[str, Any]) -> WikiContentStore:
        return cls(repo_root, book_config)

    def _ensure_resorts(self) -> dict[str, dict[str, Any]]:
        if self._resort_index is None:
            path = resolve_wiki_content_path(
                self.repo_root,
                self.book_config,
                "resort_wiki_content_file",
                "output/resort_wiki_content_full.json",
            )
            if path:
                log(f"  wiki content: loading resorts from {path} ...")
                self._resort_index = _load_index(path)
                log(f"  wiki content: {len(self._resort_index)} resort page(s) indexed")
            else:
                log(
                    "  wiki content: resort copy file not found "
                    "(set resort_wiki_content_file in book.yaml)"
                )
                self._resort_index = {}
        return self._resort_index

    def _ensure_regions(self) -> dict[str, dict[str, Any]]:
        if self._region_index is None:
            path = resolve_wiki_content_path(
                self.repo_root,
                self.book_config,
                "wiki_regions_content_file",
                "output/wiki_regions_full.json",
            )
            if path:
                log(f"  wiki content: loading regions from {path} ...")
                self._region_index = _load_index(path)
                log(f"  wiki content: {len(self._region_index)} region page(s) indexed")
            else:
                log(
                    "  wiki content: region copy file not found "
                    "(set wiki_regions_content_file in book.yaml)"
                )
                self._region_index = {}
        return self._region_index

    def apply_to_pages(self, pages: list[dict[str, Any]]) -> tuple[int, int]:
        """Merge resort copy into page dicts. Returns (merged, missing)."""
        index = self._ensure_resorts()
        if not index:
            return 0, len(pages)
        merged = 0
        for page in pages:
            pid = page.get("pageId") or ""
            if merge_resort_page(page, index.get(str(pid))):
                merged += 1
        return merged, len(pages) - merged

    def state_overview_text(
        self,
        state: str,
        country: str = "United States of America",
    ) -> tuple[str, str]:
        """Return (title, plain body) for the regional overview page."""
        index = self._ensure_regions()
        pid = wiki_state_page_id(state, country)
        item = index.get(pid)
        if not item:
            return state, ""
        title = str(item.get("title") or state).strip()
        body = markdown_to_plain(item.get("contentMarkdown") or "")
        if body.lower().startswith(title.lower()):
            body = body[len(title) :].lstrip(":\n ")
        return title, body
