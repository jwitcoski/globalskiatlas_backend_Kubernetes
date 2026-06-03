#!/usr/bin/env python3
"""
End-to-end Scribus chapter build: manifest → layout_plan → chapter.sla → PDF (optional).

  py -m atlas.book_gen.run_scribus_book --state Virginia --region north-america/us/virginia
  py -m atlas.book_gen.run_scribus_book --state Virginia --parquet-only --limit 20
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from atlas.book_gen.build_chapter_manifest import build_manifest
from atlas.book_gen.pack_pages import (
    content_area_pt,
    load_layout_plan,
    pack_manifest_entries,
    write_layout_plan,
)
from atlas.book_gen.constants import slot_body_char_limits
from atlas.book_gen.sla_compose import compose_chapter_sla, write_sla
from atlas.book_gen.log_util import log, log_phase
from atlas.book_gen.wiki_client import load_atlas_config, load_book_config


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_overview_image_path(
    state: str,
    repo_root: Path,
    book_config: dict[str, Any],
) -> str | None:
    """Return overview PNG path for page 1, exporting from QGZ if needed."""
    state_slug = state.strip().lower().replace(" ", "-")
    overview_dir = (
        repo_root
        / "atlas_work"
        / "overview"
        / "states"
        / "united-states-of-america"
        / state_slug
    )
    qgz = overview_dir / f"{state_slug}_overview_map.qgz"
    png = overview_dir / f"{state_slug}_overview_export.png"
    if not png.is_file() and qgz.is_file():
        try:
            from atlas.map_gen.export_layouts import (
                ensure_headless_qgis_initialized,
                export_overview_qgz,
                shutdown_headless_qgis_if_initialized,
            )

            ensure_headless_qgis_initialized(None)
            export_overview_qgz(
                qgz,
                dpi=int(book_config.get("map_export_dpi") or 300),
                overwrite=True,
            )
        finally:
            try:
                shutdown_headless_qgis_if_initialized()
            except Exception:
                pass
    if png.is_file():
        return str(png.resolve()).replace("\\", "/")
    return None


def resolve_region_facts(
    state: str,
    region: str | None,
    repo_root: Path,
    book_config: dict[str, Any],
    *,
    book_resort_count: int | None = None,
    charts_dir: Path | None = None,
):
    """Compute regional facts from analyzed parquet when region_facts_page is enabled."""
    from atlas.book_gen.regional_facts import compute_regional_facts_from_parquet
    from atlas.book_gen.wiki_client import parquet_path_for_config

    parquet_path = parquet_path_for_config(book_config, repo_root)
    if not parquet_path.is_file():
        log(f"Regional facts page skipped: parquet not found ({parquet_path})")
        return None
    facts = compute_regional_facts_from_parquet(
        parquet_path,
        state=state,
        region_filter=region,
        book_resort_count=book_resort_count,
        charts_dir=charts_dir,
    )
    log(
        f"Regional facts page: {facts.resort_count} resorts, "
        f"{facts.total_trails:,} trails, {facts.total_lifts:,} lifts"
    )
    if facts.chart_paths:
        log(f"  charts: {', '.join(sorted(facts.chart_paths))}")
    elif charts_dir is not None:
        log("  charts: none (text-only facts page)")
    return facts


def _find_scribus() -> str | None:
    import shutil

    for name in ("scribus", "Scribus.exe", "scribus.exe"):
        p = shutil.which(name)
        if p:
            return p
    candidates = [
        Path(r"C:\Program Files\Scribus 1.6.6\Scribus.exe"),
        Path(r"C:\Program Files\Scribus 1.6.3\Scribus.exe"),
        Path(r"C:\Program Files\Scribus 1.6.2\Scribus.exe"),
        Path(r"C:\Program Files\Scribus 1.6.1\Scribus.exe"),
        Path(r"C:\Program Files\Scribus 1.6.0\Scribus.exe"),
    ]
    for c in candidates:
        if c.is_file():
            return str(c)
    return None


def export_pdf_scribus(scribus_bin: str, sla_path: Path, pdf_path: Path) -> bool:
    repo_root = _repo_root()
    script = repo_root / "atlas" / "book_gen" / "scripts" / "export_pdf.py"
    # Do not use "--" before script args: Scribus treats trailing paths as files to open.
    cmd = [scribus_bin, "-g", "-py", str(script), str(sla_path), str(pdf_path)]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if r.returncode != 0:
            print(r.stdout, r.stderr, file=sys.stderr)
            return False
        return pdf_path.is_file()
    except (subprocess.TimeoutExpired, OSError) as e:
        print(f"Scribus export failed: {e}", file=sys.stderr)
        return False


def run_chapter(
    *,
    state: str,
    region: str | None,
    book: str | None,
    limit: int | None,
    parquet_only: bool,
    local_only: bool = True,
    skip_manifest: bool,
    skip_pdf: bool,
    merge_pdf_only: bool = False,
    pdf_page_limit: int | None = None,
    overview_image_path: str | None = None,
    overview_body: str | None = None,
    region_facts=None,
    use_region_facts: bool = True,
    output_dir: Path | None,
    wiki_api_base: str | None,
    no_maps: bool = False,
) -> int:
    repo_root = _repo_root()
    book_config = load_book_config(repo_root / "atlas" / "book_gen" / "config" / "book.yaml")
    atlas_config = load_atlas_config(repo_root)
    if wiki_api_base:
        book_config["wiki_api_base"] = wiki_api_base

    state_slug = state.strip().lower().replace(" ", "-")
    out_dir = output_dir or (
        Path(book_config.get("work_dir", "atlas_work")) / "book" / state_slug
    )
    if not out_dir.is_absolute():
        out_dir = repo_root / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if merge_pdf_only:
        from atlas.book_gen.pdf_export import merge_pdf_pages_dir

        pdf_path = out_dir / "chapter.pdf"
        with log_phase("Merge PDF pages"):
            if merge_pdf_pages_dir(out_dir, pdf_path):
                log(f"PDF: {pdf_path}")
                return 0
            return 1

    log("=" * 60)
    log("Ski Atlas - Scribus chapter build")
    log(f"  state={state!r}  region={region!r}  limit={limit}")
    log(f"  local_only={local_only}  no_maps={no_maps}  skip_pdf={skip_pdf}")
    log(f"  output: {out_dir}")
    if overview_image_path:
        log(f"  overview page: {overview_image_path}")
    if overview_body:
        log(f"  overview copy: {len(overview_body)} chars")
    log("=" * 60)

    manifest_path = out_dir / "manifest.json"
    if not skip_manifest:
        with log_phase("Build manifest"):
            manifest, source = build_manifest(
                state=state,
                region=region,
                book=book,
                book_config=book_config,
                atlas_config=atlas_config,
                repo_root=repo_root,
                limit=limit,
                api_base=book_config.get("wiki_api_base", "http://localhost:3000"),
                parquet_only=parquet_only,
                local_only=local_only,
                no_maps=no_maps,
            )
        payload = {
            "state": state,
            "region": region,
            "source": source,
            "resort_count": len(manifest),
            "entries": manifest,
        }
        manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log(f"Saved manifest.json ({len(manifest)} resorts, source={source})")
    else:
        log(f"Loading existing manifest: {manifest_path}")
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest = payload.get("entries") or []
        log(f"  {len(manifest)} entries loaded")

    if not manifest:
        log("ERROR: No manifest entries; nothing to layout.", file=sys.stderr)
        return 1

    if use_region_facts and region_facts is None:
        charts_dir = None
        if book_config.get("region_facts_charts", True):
            charts_dir = out_dir / "_facts_charts"
        region_facts = resolve_region_facts(
            state,
            region,
            repo_root,
            book_config,
            book_resort_count=len(manifest),
            charts_dir=charts_dir,
        )

    trim = book_config.get("trim_in", [8.5, 11.0])
    margin = book_config.get("margin_in", [0.5, 0.5, 0.5, 0.5])
    map_dpi = int(book_config.get("map_export_dpi") or book_config.get("dpi") or 300)
    _, content_h_pt = content_area_pt(
        (float(trim[0]), float(trim[1])),
        tuple(float(x) for x in margin),
    )

    with log_phase("Pack pages"):
        qps = int(book_config.get("quarters_per_sheet", 4))
        plan = pack_manifest_entries(
            manifest,
            quarters_per_sheet=qps,
            content_h_pt=content_h_pt,
            map_export_dpi=map_dpi,
        )
        plan_path = out_dir / "layout_plan.json"
        write_layout_plan(plan_path, plan)
        log(f"Saved layout_plan.json -> {plan_path}")

    if region_facts is not None:
        facts_path = out_dir / "region_facts.json"
        facts_path.write_text(
            json.dumps(region_facts.to_dict(), indent=2),
            encoding="utf-8",
        )
        log(f"Saved region_facts.json")

    by_id = {e["pageId"]: e for e in manifest}
    chapter_title = f"{state} - Ski Atlas"
    body_limits = slot_body_char_limits(book_config)
    sla_path = out_dir / "chapter.sla"
    with log_phase("Compose Scribus SLA"):
        tree = compose_chapter_sla(
            plan,
            by_id,
            trim_in=(float(trim[0]), float(trim[1])),
            margin_in=tuple(float(x) for x in margin),
            chapter_title=chapter_title,
            sla_output_path=sla_path,
            map_export_dpi=map_dpi,
            slot_body_char_limits=body_limits,
            overview_image_path=overview_image_path,
            overview_body=overview_body,
            region_facts=region_facts,
            region_facts_elevation_only=bool(
                book_config.get("region_facts_elevation_only", True)
            ),
        )
        write_sla(tree, sla_path)

    csv_path = out_dir / "chapter_data.csv"
    _write_csv(csv_path, manifest)
    log(f"Wrote chapter_data.csv")

    if skip_pdf:
        log("Done (PDF export skipped).")
        return 0

    with log_phase("Export PDF via Scribus"):
        scribus = _find_scribus()
        pdf_path = out_dir / "chapter.pdf"
        if not scribus:
            log(
                "Scribus not found; open chapter.sla manually to export PDF."
            )
            return 0
        log(f"  using: {scribus}")
        from atlas.book_gen.pdf_export import export_chapter_pdf

        if export_chapter_pdf(
            scribus,
            pdf_path,
            layout_plan=plan,
            manifest_by_id=by_id,
            trim_in=(float(trim[0]), float(trim[1])),
            margin_in=tuple(float(x) for x in margin),
            chapter_title=chapter_title,
            work_dir=out_dir,
            map_export_dpi=map_dpi,
            page_limit=pdf_page_limit,
            overview_image_path=overview_image_path,
            overview_body=overview_body,
            region_facts=region_facts,
            slot_body_char_limits=body_limits,
            region_facts_elevation_only=bool(
                book_config.get("region_facts_elevation_only", True)
            ),
        ):
            log(f"PDF: {pdf_path}")
        else:
            log(
                f"PDF export failed; open {sla_path} in Scribus. "
                f"If {out_dir / '_pdf_pages'} has page_*.pdf, run with --merge-pdf-only "
                f"(requires: pip install pypdf).",
                file=sys.stderr,
            )
    log("All done.")
    return 0


def _write_csv(path: Path, manifest: list[dict[str, Any]]) -> None:
    import csv

    if not manifest:
        return
    fields = manifest[0].get("scribusFields") or {}
    keys = ["pageId", "mapPath", "slot", "resortSizeCategory"] + list(fields.keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for e in manifest:
            row = {
                "pageId": e.get("pageId"),
                "mapPath": e.get("mapPath"),
                "slot": e.get("slot"),
                "resortSizeCategory": e.get("resortSizeCategory"),
            }
            row.update(e.get("scribusFields") or {})
            w.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Scribus atlas chapter")
    parser.add_argument("--state", required=True)
    parser.add_argument("--region", default="north-america/us/virginia")
    parser.add_argument("--book", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--local-only", action="store_true", default=None)
    parser.add_argument("--no-local-only", action="store_true", help="Allow wiki API + S3")
    parser.add_argument("--parquet-only", action="store_true", help="Same as --local-only")
    parser.add_argument("--skip-manifest", action="store_true")
    parser.add_argument("--skip-pdf", action="store_true")
    parser.add_argument(
        "--pdf-page-limit",
        type=int,
        default=None,
        help="Export only the first N logical pages to chapter.pdf (includes title page).",
    )
    parser.add_argument(
        "--overview-first",
        action="store_true",
        help="Force regional overview as page 1 (default when overview_first in book.yaml).",
    )
    parser.add_argument(
        "--no-overview-first",
        action="store_true",
        help="Skip the regional overview page even if the PNG exists.",
    )
    parser.add_argument(
        "--no-region-facts",
        action="store_true",
        help="Skip the regional facts page (page_01) even if enabled in book.yaml.",
    )
    parser.add_argument(
        "--region-facts",
        action="store_true",
        help="Force regional facts page after overview (default when region_facts_page in book.yaml).",
    )
    parser.add_argument(
        "--merge-pdf-only",
        action="store_true",
        help="Merge existing _pdf_pages/page_*.pdf into chapter.pdf (needs pypdf; no Scribus)",
    )
    parser.add_argument("--no-maps", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--wiki-api-base", default=None)
    args = parser.parse_args()

    repo_root = _repo_root()
    book_config = load_book_config(repo_root / "atlas" / "book_gen" / "config" / "book.yaml")
    local_only = bool(book_config.get("local_only", True))
    if args.no_local_only:
        local_only = False
    if args.local_only or args.parquet_only:
        local_only = True

    use_overview = bool(book_config.get("overview_first", True))
    if args.no_overview_first:
        use_overview = False
    elif args.overview_first:
        use_overview = True

    overview_image_path = None
    overview_body: str | None = None
    if use_overview:
        from atlas.book_gen.wiki_content_store import WikiContentStore

        wiki_store = WikiContentStore.from_config(repo_root, book_config)
        _, overview_body = wiki_store.state_overview_text(args.state)
        overview_image_path = resolve_overview_image_path(
            args.state, repo_root, book_config
        )
        if overview_image_path:
            log(f"Overview page 1: {overview_image_path}")
        else:
            state_slug = args.state.strip().lower().replace(" ", "-")
            log(
                f"No overview PNG for {args.state!r} "
                f"(expected atlas_work/overview/states/united-states-of-america/"
                f"{state_slug}/{state_slug}_overview_export.png)"
            )

    use_region_facts = bool(book_config.get("region_facts_page", True))
    if args.no_region_facts:
        use_region_facts = False
    elif args.region_facts:
        use_region_facts = True

    return run_chapter(
        state=args.state,
        region=args.region,
        book=args.book,
        limit=args.limit,
        parquet_only=args.parquet_only,
        local_only=local_only,
        skip_manifest=args.skip_manifest,
        skip_pdf=args.skip_pdf,
        merge_pdf_only=args.merge_pdf_only,
        pdf_page_limit=args.pdf_page_limit,
        overview_image_path=overview_image_path,
        overview_body=overview_body,
        use_region_facts=use_region_facts,
        output_dir=args.output_dir,
        wiki_api_base=args.wiki_api_base,
        no_maps=args.no_maps,
    )


if __name__ == "__main__":
    raise SystemExit(main())
