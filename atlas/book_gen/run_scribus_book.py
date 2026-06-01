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
from atlas.book_gen.sla_compose import compose_chapter_sla, write_sla
from atlas.book_gen.log_util import log, log_phase
from atlas.book_gen.wiki_client import load_atlas_config, load_book_config


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


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

    by_id = {e["pageId"]: e for e in manifest}
    chapter_title = f"{state} - Ski Atlas"
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
        help="Add a full-page regional overview map as the first page (if available).",
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

    overview_image_path = None
    if args.overview_first:
        # Convention: overview exports live under atlas_work/overview/states/<country_slug>/<state_slug>/...
        state_slug = args.state.strip().lower().replace(" ", "-")
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
            # Try exporting it now (headless QGIS).
            try:
                from atlas.map_gen.export_layouts import (
                    ensure_headless_qgis_initialized,
                    export_overview_qgz,
                    shutdown_headless_qgis_if_initialized,
                )

                ensure_headless_qgis_initialized(None)
                export_overview_qgz(qgz, dpi=int(book_config.get("map_export_dpi") or 300), overwrite=True)
            finally:
                try:
                    shutdown_headless_qgis_if_initialized()
                except Exception:
                    pass
        if png.is_file():
            overview_image_path = str(png.resolve())

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
        output_dir=args.output_dir,
        wiki_api_base=args.wiki_api_base,
        no_maps=args.no_maps,
    )


if __name__ == "__main__":
    raise SystemExit(main())
