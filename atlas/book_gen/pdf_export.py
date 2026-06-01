"""Export chapter PDF via one-page Scribus jobs merged with pypdf."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Any

from atlas.book_gen.log_util import log
from atlas.book_gen.sla_compose import compose_single_page_sla, write_sla


def merge_pdfs(parts: list[Path], out_path: Path) -> bool:
    try:
        from pypdf import PdfReader, PdfWriter
    except ImportError:
        log(
            "PDF merge needs pypdf (listed in requirements.txt): "
            "pip install pypdf",
            file=sys.stderr,
        )
        return False
    writer = PdfWriter()
    for part in parts:
        if not part.is_file():
            log(f"Missing PDF part: {part}", file=sys.stderr)
            return False
        writer.append(PdfReader(str(part)))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        writer.write(f)
    return out_path.is_file()


def export_chapter_pdf(
    scribus_bin: str,
    out_pdf: Path,
    *,
    layout_plan: dict[str, Any],
    manifest_by_id: dict[str, dict[str, Any]],
    trim_in: tuple[float, float],
    margin_in: tuple[float, float, float, float],
    chapter_title: str,
    work_dir: Path,
    map_export_dpi: int = 300,
    page_limit: int | None = None,
    overview_image_path: str | None = None,
) -> bool:
    """
    Scribus often flattens multi-page SLA into one sheet on PDF export.
    Export each logical page as its own 1-page SLA/PDF, then merge.
    """
    temp = work_dir / "_pdf_pages"
    if temp.is_dir():
        shutil.rmtree(temp)
    temp.mkdir(parents=True)

    parts: list[Path] = []
    logical: list[tuple[str, dict[str, Any] | None]] = []
    if overview_image_path:
        logical.append(("overview", {"image": overview_image_path}))
    if chapter_title:
        logical.append(("title", None))
    for page_info in layout_plan.get("pages") or []:
        logical.append(("content", page_info))

    from atlas.book_gen.run_scribus_book import export_pdf_scribus

    if page_limit is not None:
        logical = logical[: max(0, int(page_limit))]

    log(f"pdf_export: {len(logical)} page(s) via Scribus (1 SLA + 1 PDF each) ...")
    for i, (kind, page_info) in enumerate(logical):
        sla_path = temp / f"page_{i:02d}.sla"
        pdf_path = temp / f"page_{i:02d}.pdf"
        tree = compose_single_page_sla(
            is_title=(kind == "title"),
            chapter_title=chapter_title,
            overview_image_path=(
                (page_info or {}).get("image") if kind == "overview" else None
            ),
            page_info=(page_info if kind == "content" else None),
            manifest_by_id=manifest_by_id,
            trim_in=trim_in,
            margin_in=margin_in,
            sla_output_path=sla_path,
            map_export_dpi=map_export_dpi,
        )
        write_sla(tree, sla_path)
        log(f"  pdf_export: page {i + 1}/{len(logical)} -> {pdf_path.name}")
        if not export_pdf_scribus(scribus_bin, sla_path, pdf_path):
            log(f"PDF export failed for {sla_path.name}", file=sys.stderr)
            return False
        parts.append(pdf_path)

    log(f"pdf_export: merging {len(parts)} PDF(s) -> {out_pdf}")
    if not merge_pdfs(parts, out_pdf):
        return False
    log(f"pdf_export: wrote {out_pdf.stat().st_size / 1024:.1f} KB")
    return True


def merge_pdf_pages_dir(work_dir: Path, out_pdf: Path | None = None) -> bool:
    """Merge existing ``work_dir/_pdf_pages/page_*.pdf`` into ``chapter.pdf`` (no Scribus)."""
    temp = work_dir / "_pdf_pages"
    if not temp.is_dir():
        log(f"No _pdf_pages folder at {temp}", file=sys.stderr)
        return False
    parts = sorted(temp.glob("page_*.pdf"))
    if not parts:
        log(f"No page_*.pdf files in {temp}", file=sys.stderr)
        return False
    dest = out_pdf or (work_dir / "chapter.pdf")
    log(f"pdf_export: merging {len(parts)} PDF(s) from {temp.name} -> {dest.name}")
    if not merge_pdfs(parts, dest):
        return False
    log(f"pdf_export: wrote {dest.stat().st_size / 1024:.1f} KB")
    return True
