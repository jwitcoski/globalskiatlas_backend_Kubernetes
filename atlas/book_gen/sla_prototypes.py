"""Valid Scribus PAGEOBJECT prototypes cloned from a real .sla file."""

from __future__ import annotations

import copy
import html
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Sequence

from atlas.book_gen.wiki_style import DOCUMENT_COLORS, FONT_SANS, TextRun

_REF = Path(__file__).resolve().parent / "templates" / "_ref_business_card.sla"

_TEXT_PROTO: ET.Element | None = None
_IMAGE_PROTO: ET.Element | None = None
_SHAPE_PROTO: ET.Element | None = None
_PAGE_PROTO: ET.Element | None = None
_DOC_CHILDREN: list[ET.Element] | None = None


def _rect_pocoor(w: float, h: float) -> str:
    w = max(1.0, w)
    h = max(1.0, h)
    return (
        f"0 0 0 0 {w} 0 {w} 0 {w} 0 {w} 0 "
        f"{w} {h} {w} {h} {w} {h} {w} {h} "
        f"0 {h} 0 {h} 0 {h} 0 {h} 0 0 0 0"
    )


def _ensure_loaded() -> None:
    global _TEXT_PROTO, _IMAGE_PROTO, _SHAPE_PROTO, _PAGE_PROTO, _DOC_CHILDREN
    if _TEXT_PROTO is not None:
        return
    if not _REF.is_file():
        raise FileNotFoundError(
            f"Missing Scribus reference template: {_REF}\n"
            "Run: Invoke-WebRequest to download Business_Card.sla (see README)"
        )
    root = ET.parse(_REF).getroot()
    doc = root.find("DOCUMENT")
    if doc is None:
        raise ValueError(f"No DOCUMENT in {_REF}")

    _DOC_CHILDREN = [
        copy.deepcopy(child)
        for child in doc
        if child.tag not in ("PAGE", "PAGEOBJECT", "MASTERPAGE")
    ]

    for po in doc.iter("PAGEOBJECT"):
        ptype = po.get("PTYPE")
        if ptype == "4" and _TEXT_PROTO is None:
            _TEXT_PROTO = copy.deepcopy(po)
        elif ptype == "2" and _IMAGE_PROTO is None:
            _IMAGE_PROTO = copy.deepcopy(po)
        elif ptype == "6" and _SHAPE_PROTO is None:
            _SHAPE_PROTO = copy.deepcopy(po)

    for page in doc.findall("PAGE"):
        _PAGE_PROTO = copy.deepcopy(page)
        break

    if _TEXT_PROTO is None or _IMAGE_PROTO is None or _PAGE_PROTO is None:
        raise ValueError(f"Could not find text/image PAGEOBJECT prototypes in {_REF}")


def document_preamble() -> list[ET.Element]:
    _ensure_loaded()
    assert _DOC_CHILDREN is not None
    return copy.deepcopy(_DOC_CHILDREN)


def make_page(
    *,
    num: int,
    width_pt: float,
    height_pt: float,
    margin_pt: float,
    page_xpos: float = 0.0,
    page_ypos: float = 0.0,
) -> ET.Element:
    _ensure_loaded()
    assert _PAGE_PROTO is not None
    page = copy.deepcopy(_PAGE_PROTO)
    page.set("NUM", str(num))
    page.set("PAGEXPOS", str(page_xpos))
    page.set("PAGEYPOS", str(page_ypos))
    page.set("PAGEWIDTH", str(width_pt))
    page.set("PAGEHEIGHT", str(height_pt))
    page.set("BORDERLEFT", str(margin_pt))
    page.set("BORDERRIGHT", str(margin_pt))
    page.set("BORDERTOP", str(margin_pt))
    page.set("BORDERBOTTOM", str(margin_pt))
    # Remove any objects from prototype page
    for child in list(page):
        if child.tag == "PAGEOBJECT":
            page.remove(child)
    return page


def _clear_pageobject_content(po: ET.Element) -> None:
    for child in list(po):
        if child.tag in ("StoryText", "ITEXT", "para", "trail", "PageItemAttributes"):
            po.remove(child)


def _story_runs(content: str | Sequence[TextRun]) -> list[TextRun]:
    if isinstance(content, str):
        return [TextRun(content, 10.0)]
    return list(content)


def _set_story(
    po: ET.Element,
    content: str | Sequence[TextRun],
    fontsize: float | None = None,
    *,
    font: str = FONT_SANS,
    fcolor: str = "Black",
) -> None:
    _clear_pageobject_content(po)
    runs = _story_runs(content)
    story = ET.SubElement(po, "StoryText")
    default_font = runs[0].font if runs else font
    ET.SubElement(story, "DefaultStyle", FONT=default_font)
    last_fs = fontsize or (runs[-1].fontsize if runs else 10.0)
    last_color = fcolor
    last_font = font
    for run in runs:
        if not run.text:
            continue
        parts = run.text.split("\n")
        for i, chunk in enumerate(parts):
            if chunk:
                safe = html.escape(chunk, quote=False)
                fs = int(round(run.fontsize if fontsize is None else fontsize))
                ET.SubElement(
                    story,
                    "ITEXT",
                    CH=safe,
                    FONTSIZE=str(fs),
                    FONT=run.font,
                    FCOLOR=run.fcolor,
                )
                last_fs = fs
                last_color = run.fcolor
                last_font = run.font
            if i < len(parts) - 1:
                ET.SubElement(story, "para")
    trail = ET.SubElement(
        story,
        "trail",
        FONT=last_font,
        FONTSIZE=str(int(round(last_fs))),
        FCOLOR=last_color,
    )
    trail.set("CH", "")
    ET.SubElement(po, "PageItemAttributes")


def _append_document_colors(doc: ET.Element) -> None:
    existing = {c.get("NAME") for c in doc.findall("COLOR")}
    insert_at = 0
    for i, child in enumerate(doc):
        if child.tag == "COLOR":
            insert_at = i + 1
    for name, r, g, b in DOCUMENT_COLORS:
        if name in existing:
            continue
        el = ET.Element(
            "COLOR",
            NAME=name,
            SPACE="RGB",
            R=str(r),
            G=str(g),
            B=str(b),
        )
        doc.insert(insert_at, el)
        insert_at += 1


def make_text_frame(
    *,
    x: float,
    y: float,
    w: float,
    h: float,
    text: str | Sequence[TextRun],
    fontsize: float = 10.0,
    own_page: int,
    item_id: int,
    linesp: float | None = None,
    fcolor: str = "Black",
    font: str = FONT_SANS,
) -> ET.Element:
    _ensure_loaded()
    assert _TEXT_PROTO is not None
    po = copy.deepcopy(_TEXT_PROTO)
    po.set("XPOS", str(x))
    po.set("YPOS", str(y))
    po.set("WIDTH", str(max(4.0, w)))
    po.set("HEIGHT", str(max(4.0, h)))
    po.set("OwnPage", str(own_page))
    po.set("Pagenumber", str(own_page))
    po.set("ItemID", str(item_id))
    po.set("ANNAME", "TextFrame")
    po.set("TXTOUT", "1")
    if linesp is not None:
        po.set("LINESP", str(int(round(linesp))))
    poc = _rect_pocoor(max(4.0, w), max(4.0, h))
    po.set("POCOOR", poc)
    po.set("COCOOR", poc)
    po.set("NUMPO", "16")
    _set_story(po, text, fontsize, font=font, fcolor=fcolor)
    if not any(c.tag == "PageItemAttributes" for c in po):
        ET.SubElement(po, "PageItemAttributes")
    return po


def make_shape_frame(
    *,
    x: float,
    y: float,
    w: float,
    h: float,
    fill_color: str,
    own_page: int,
    item_id: int,
) -> ET.Element:
    _ensure_loaded()
    if _SHAPE_PROTO is not None:
        po = copy.deepcopy(_SHAPE_PROTO)
        po.set("PTYPE", "6")
    else:
        assert _TEXT_PROTO is not None
        po = copy.deepcopy(_TEXT_PROTO)
        po.set("PTYPE", "6")
    po.set("XPOS", str(x))
    po.set("YPOS", str(y))
    po.set("WIDTH", str(max(4.0, w)))
    po.set("HEIGHT", str(max(4.0, h)))
    po.set("OwnPage", str(own_page))
    po.set("Pagenumber", str(own_page))
    po.set("ItemID", str(item_id))
    po.set("ANNAME", "StatsBg")
    po.set("FRTYPE", "3")
    po.set("PCOLOR", fill_color)
    po.set("PCOLOR2", "None")
    po.set("PLINEART", "0")
    po.set("PWIDTH", "0")
    poc = _rect_pocoor(max(4.0, w), max(4.0, h))
    po.set("POCOOR", poc)
    po.set("COCOOR", poc)
    po.set("NUMPO", "16")
    _clear_pageobject_content(po)
    ET.SubElement(po, "trail")
    ET.SubElement(po, "PageItemAttributes")
    return po


def make_image_frame(
    *,
    x: float,
    y: float,
    w: float,
    h: float,
    image_path: str,
    own_page: int,
    item_id: int,
    sla_output: Path | None = None,
) -> ET.Element:
    _ensure_loaded()
    assert _IMAGE_PROTO is not None
    po = copy.deepcopy(_IMAGE_PROTO)
    po.set("XPOS", str(x))
    po.set("YPOS", str(y))
    po.set("WIDTH", str(max(4.0, w)))
    po.set("HEIGHT", str(max(4.0, h)))
    po.set("OwnPage", str(own_page))
    po.set("Pagenumber", str(own_page))
    po.set("ItemID", str(item_id))
    po.set("ANNAME", "MapFrame")

    pfile = image_path.replace("\\", "/")
    if sla_output is not None and image_path:
        try:
            rel = Path(image_path).resolve().relative_to(sla_output.parent.resolve())
            pfile = rel.as_posix()
        except ValueError:
            pass
    po.set("PFILE", pfile)
    fw = max(4.0, w)
    fh = max(4.0, h)
    # Frame matches export plate size; image at 100% (maps are pre-sized for the book).
    po.set("SCALETYPE", "0")
    po.set("RATIO", "1")
    po.set("CLIPEDIT", "1")
    po.set("LOCALSCX", "1")
    po.set("LOCALSCY", "1")
    po.set("LOCALX", "0")
    po.set("LOCALY", "0")
    poc = _rect_pocoor(fw, fh)
    po.set("POCOOR", poc)
    po.set("COCOOR", poc)
    po.set("NUMPO", "16")
    _clear_pageobject_content(po)
    ET.SubElement(po, "trail")
    ET.SubElement(po, "PageItemAttributes")
    return po


def build_document_root(
    *,
    width_pt: float,
    height_pt: float,
    margin_pt: float,
    page_count: int,
) -> ET.Element:
    root = ET.Element("SCRIBUSUTF8NEW", Version="1.6.6")
    doc = ET.SubElement(root, "DOCUMENT")
    # Copy attrs from reference DOCUMENT if present
    if _REF.is_file():
        ref_doc = ET.parse(_REF).getroot().find("DOCUMENT")
        if ref_doc is not None:
            for k, v in ref_doc.attrib.items():
                doc.set(k, v)
    doc.set("ANZPAGES", str(page_count))
    doc.set("PAGEWIDTH", str(width_pt))
    doc.set("PAGEHEIGHT", str(height_pt))
    doc.set("BORDERLEFT", str(margin_pt))
    doc.set("BORDERRIGHT", str(margin_pt))
    doc.set("BORDERTOP", str(margin_pt))
    doc.set("BORDERBOTTOM", str(margin_pt))
    doc.set("ORIENTATION", "0")
    doc.set("PAGESIZE", "Custom")
    doc.set("ScratchLeft", "100")
    doc.set("ScratchTop", "20")
    doc.set("GapVertical", "40")
    for child in document_preamble():
        doc.append(child)
    _append_document_colors(doc)
    return root
