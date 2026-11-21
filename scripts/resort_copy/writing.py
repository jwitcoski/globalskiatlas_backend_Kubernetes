"""Outline, writer, critic, editor, and extend passes."""
from __future__ import annotations

import json
import re
from typing import Any

from . import bedrock

from .config import (
    _tier_key,
    section_writing_for,
)
from .grounding import (
    _GROUNDING_RULE,
    _strip_ungrounded,
)
from .ollama_client import (
    OllamaJsonError,
    _chat_json,
    _num_predict,
    ollama_chat,
)
from .validators import (
    assign_sections,
    deterministic_critique,
    drop_foreign_sentences,
    note_chars,
    paragraph_budgets,
    reconcile_distances,
    short_reason_for,
    unused_notes,
)


_STYLE = (
    "The page title is already the resort name. Do not start the page or a paragraph with that name. "
    "Do not use nestled, hidden gem, world-class, or other brochure cliches. "
    "Use only facts from NOTES. If a fact is missing, leave it out. "
    "Do not state superlatives or review-style praise as fact. "
    "Keep the extra clauses in NOTES. Do not compress a long note into one short sentence. "
)


_PREAMBLE_RE = re.compile(
    r"^(?:"
    r"here(?:'s| is) (?:the |a |your )?(?:paragraph|copy|blurb|draft|prose)[:\s]*|"
    r"atlas (?:paragraph|copy)[:\s]*|"
    r"sure[,!]?\s*(?:here(?:'s| is)[^:\n]*:?\s*)?"
    r")",
    re.IGNORECASE,
)


def outline_beats(
    notes: list[dict[str, Any]],
    *,
    resort_label: str,
    beat_cap: int,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> list[str]:
    system = (
        "You are the outline pass. Order the NOTES into beats the writer will turn into paragraphs. "
        "Output one JSON object and nothing else. "
        '{"beats":["short beat, 15 words or fewer"]}. '
        f"Use up to {beat_cap} beats. One beat is one paragraph. "
        + (
            "This blurb is a single paragraph, so few beats. "
            if beat_cap <= 6
            else "This is half a page or more. Use a separate beat for each paragraph. Do not collapse the page into one beat. "
        )
        + "Use only NOTES. Do not add facts. "
        "The page title is already the resort name, so do not spend a beat on the name or the state. "
        "One beat is the setting, not the town name. "
        "When NOTES support them, give separate beats to who skis there, how a visitor gets there, who the hill is for, and what a day costs. "
        "If notes name linked villages, a race, or which base to stay in, give those their own beats. "
        "This is not an essay outline. No headings like Historically or Overall."
    )
    try:
        payload = _chat_json(
            base_url=base_url,
            model=model,
            system_text=system,
            user_text=f"Resort: {resort_label}\n\nNOTES:\n{json.dumps(notes, ensure_ascii=False)}\n",
            temperature=0.2,
            num_predict=max(400, beat_cap * 40),
            num_ctx=num_ctx,
            timeout=timeout,
        )
    except (json.JSONDecodeError, OllamaJsonError):
        return [n["text"] for n in notes[:beat_cap]]
    raw = payload.get("beats")
    if not isinstance(raw, list):
        return [n["text"] for n in notes[:beat_cap]]
    beats = [str(b).strip() for b in raw if str(b).strip()]
    return (beats or [n["text"] for n in notes])[:beat_cap]


def _clean_prose(text: str) -> str:
    s = text.strip()
    s = re.sub(r"^```(?:\w+)?\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    s = _PREAMBLE_RE.sub("", s).strip()
    s = re.sub(r"^#+\s+.*\n+", "", s).strip()
    return bedrock.strip_banned_travel_cliches(s)


def _reader_fixes(draft: str, notes: list[dict[str, Any]], name: str = "") -> list[str]:
    """Tell the editor to put back a reader fact the draft dropped."""
    blob = " ".join(str(note.get("text") or "") for note in notes)
    draft_l = draft.lower()
    fixes: list[str] = []
    opened = draft_l.lstrip()
    if name and opened.startswith(name.lower()):
        fixes.append("The page title is the resort name. Rewrite the opening so it does not start with that name.")
    if re.search(r"\b(18|19|20)\d{2}\b", blob) and not re.search(r"\b(18|19|20)\d{2}\b", draft):
        fixes.append("Put the opening year from NOTES back into the paragraph.")
    if re.search(r"day pass|lift ticket|\$|cad\s?\d|€\s?\d", blob, re.IGNORECASE) and not re.search(
        r"\d|\$|cad|€|£|day pass|ticket", draft, re.IGNORECASE
    ):
        fixes.append("Include the day-pass or ticket price from NOTES. Use the number, not a cheapest ranking.")
    if re.search(r"minutes|km from|kilomet|drive from|airport|southwest", blob, re.IGNORECASE) and not re.search(
        r"minutes|\bkm\b|kilomet|drive|airport|southwest", draft_l
    ):
        fixes.append("Include how a visitor gets there, using the distance or city in NOTES.")
    if re.search(r"not-for-profit|nonprofit|ski club|members|beginners|locals|families", blob, re.IGNORECASE) and not re.search(
        r"not-for-profit|nonprofit|club|member|beginner|local|famil", draft_l
    ):
        fixes.append("Say who skis there and who the hill is for, using NOTES.")
    return fixes


def _fit_to_layout(text: str, tier: str) -> str:
    """Deterministic cut if the model will not cut itself. Keeps paragraph breaks."""
    goal = bedrock.copy_goal_for_tier(tier)
    paras = [re.split(r"(?<=[.!?])\s+", p) for p in bedrock._paragraphs(text)]

    def joined() -> str:
        return "\n\n".join(" ".join(s) for s in paras if s)

    while len(paras) > goal.paragraphs[1]:
        paras[-2].extend(paras.pop())
    while bedrock._copy_over_max(bedrock.measure_copy(joined(), tier), goal):
        if len(paras[-1]) > 1:
            paras[-1].pop()
        elif len(paras) > 1:
            paras.pop()
        else:
            break
    return joined()


def write_draft(
    notes: list[dict[str, Any]],
    beats: list[str],
    *,
    resort_label: str,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> str:
    key = _tier_key(tier)
    length_rule = bedrock.layout_instruction(key) + " "
    system = (
        "You are the writer. Follow OUTLINE order. Use only NOTES. "
        "When notes include the opening year, a pass, private versus public, or the setting, use them. "
        "The town is already on the map, so do not write a sentence that only says where it is. "
        "Prefer notes about who skied there, the club, where to stay, whether the scene is family events or nightlife, and one reviewer observation. "
        "When NOTES include them, the paragraph must say who skis there, how to get there, who the hill is for, and what a day or a membership costs. "
        "Keep the opening year. Quote a price as a number from NOTES, not as a cheapest-in-the-country ranking. "
        "Cut a nearby golf club, banquet hall, or event-space detail before you cut the drive, the price, the year, or who it is for. "
        + length_rule
        + "Use the NOTES almost in full. Do not compress the page to a few hundred words when NOTES are longer. "
        + "Do not add years, prices, pass names, or places that are not in NOTES. "
        + "Do not recite vertical, trails, lifts, or acreage. "
        + "The resort name, the lifts, the trails, and the vertical drop are already printed on the page from the database. "
        + "Do not open with the resort name, the state, or the country. Do not repeat those database facts. "
        + "Start with who the hill is for, how a person gets there, what a day costs, or what the place is like. "
        + _STYLE
        + "One blank line between paragraphs. No title, no bullets, no markdown. "
        + _GROUNDING_RULE
        + " "
        + bedrock._BANNED_TRAVEL_CLICHES_RULE
        + " "
        + bedrock._ATLAS_PROSE_LEADIN_RULE
    )
    user = (
        f"Resort: {resort_label}\n\nOUTLINE:\n{json.dumps(beats, ensure_ascii=False)}\n\n"
        f"NOTES:\n{json.dumps(notes, ensure_ascii=False)}\n"
    )
    raw = ollama_chat(
        base_url=base_url,
        model=model,
        system_text=system,
        user_text=user,
        temperature=0.4,
        num_predict=_num_predict(key),
        num_ctx=num_ctx,
        timeout=timeout,
    )
    return _strip_ungrounded(_clean_prose(raw), notes, tier, resort_label)


def critique_draft(
    draft: str,
    notes: list[dict[str, Any]],
    *,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> dict[str, Any]:
    goal = bedrock.copy_goal_for_tier(tier)
    measured = bedrock.measure_copy(draft, tier)
    over = bedrock._copy_over_max(measured, goal)
    if over:
        length_fix = "One fix must say which sentences or paragraph to cut to get under every maximum. "
    elif bedrock._copy_under_min(measured, goal):
        length_fix = "Do not ask for a cut. One fix may name an unused NOTES item to add. "
    else:
        length_fix = "The length is in range. Do not ask for a cut or an addition. "
    system = (
        "You are the critic and fact checker. Compare DRAFT to NOTES. "
        "Output one JSON object and nothing else. No extra keys. Schema: "
        '{"unsupported":["exact wording from the draft that NOTES do not support"],'
        '"cliches":["banned phrase that appears in the draft"],'
        '"fixes":["one imperative fix"]}. '
        f"{bedrock._measured_brief(measured, goal)} "
        f"{length_fix}"
        "fixes must not introduce a new fact. "
        "Flag nestled, hidden gem, world-class, and essay openers such as Overall or Historically. "
        "Also list under unsupported any sentence about weather, snow, scenery, mood, fireplaces, or atmosphere, and any sentence no NOTES item backs. "
        "If the draft is fine, return empty arrays."
    )
    user = f"NOTES:\n{json.dumps(notes, ensure_ascii=False)}\n\nDRAFT:\n{draft}\n"
    try:
        payload = _chat_json(
            base_url=base_url,
            model=model,
            system_text=system,
            user_text=user,
            temperature=0.1,
            num_predict=500,
            num_ctx=num_ctx,
            timeout=timeout,
        )
    except json.JSONDecodeError:
        return {"unsupported": [], "cliches": [], "fixes": []}
    out: dict[str, Any] = {}
    for key in ("unsupported", "cliches", "fixes"):
        raw = payload.get(key)
        if isinstance(raw, list):
            out[key] = [str(x).strip() for x in raw if str(x).strip()]
        else:
            out[key] = []
    draft_lower = draft.lower()
    if not over:
        out["fixes"] = [fix for fix in out["fixes"] if not re.search(r"\bcuts?\b", fix, re.IGNORECASE)]
    out["cliches"] = [phrase for phrase in out["cliches"] if phrase.lower() in draft_lower]
    out["unsupported"] = [
        claim
        for claim in out["unsupported"]
        if _claim_in_draft(claim, draft)
    ]
    out["checker"] = "llm"
    return out


def _claim_in_draft(claim: str, draft: str) -> bool:
    """Drop critic items that are not phrases from the draft ('sentence about mood')."""
    if claim.lower() in {"claim in the draft that notes do not support"}:
        return False
    blob = draft.lower()
    if claim.lower() in blob:
        return True
    words = [w for w in re.findall(r"[a-z0-9']+", claim.lower()) if len(w) > 3]
    if not words:
        return False
    return sum(w in blob for w in words) >= max(2, int(0.6 * len(words)))


def edit_draft(
    draft: str,
    notes: list[dict[str, Any]],
    critique: dict[str, Any],
    *,
    resort_label: str,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> str:
    goal = bedrock.copy_goal_for_tier(tier)
    system = (
        "You are the editor. Apply CRITIQUE fixes. "
        f"{bedrock._measured_brief(bedrock.measure_copy(draft, tier), goal)} "
        f"{bedrock.layout_instruction(tier)} "
        "If the draft is already inside the range, do not shorten it. "
        "Do not add any fact that is not already in NOTES or the draft. "
        "Delete unsupported claims and banned phrases. "
        + _GROUNDING_RULE
        + " Never invent facts to reach length. Keep NOTES detail instead of cutting it. "
        "The page title is already the resort name, and the fact block already lists lifts, trails, and vertical drop. "
        "If the draft opens with the resort name, the state, or the country, rewrite the opening. Do not repeat those mountain facts. "
        "Plain paragraphs only, one blank line between paragraphs. No title, no markdown. "
        + bedrock._BANNED_TRAVEL_CLICHES_RULE
    )
    user = (
        f"Resort: {resort_label}\n\nNOTES:\n{json.dumps(notes, ensure_ascii=False)}\n\n"
        f"CRITIQUE:\n{json.dumps(critique, ensure_ascii=False)}\n\nDRAFT:\n{draft}\n"
    )
    raw = ollama_chat(
        base_url=base_url,
        model=model,
        system_text=system,
        user_text=user,
        temperature=0.2,
        num_predict=_num_predict(tier),
        num_ctx=num_ctx,
        timeout=timeout,
    )
    return _fit_to_layout(_strip_ungrounded(_clean_prose(raw), notes, tier, resort_label), tier)


def extend_draft(
    draft: str,
    notes: list[dict[str, Any]],
    *,
    resort_label: str,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> str:
    """One pass that adds NOTES material when the edit lands under a minimum."""
    goal = bedrock.copy_goal_for_tier(tier)
    if not bedrock._copy_under_min(bedrock.measure_copy(draft, tier), goal):
        return draft
    system = (
        "You are the editor. The draft is too short for its print column. "
        f"{bedrock._measured_brief(bedrock.measure_copy(draft, tier), goal)} "
        f"{bedrock.layout_instruction(tier)} "
        "Keep every existing sentence. Add sentences and paragraphs that use NOTES items the draft does not use yet. "
        "Copy the unused NOTES almost in full: names, years, and extra clauses. Do not boil them down. "
        "Do not repeat an idea already in the draft, and do not add any fact that is not in NOTES. "
        "Do not recite vertical, trails, lifts, or acreage. "
        "Plain paragraphs only, one blank line between paragraphs. No title, no markdown. "
        + _GROUNDING_RULE
        + " If every NOTES item is already used, return the draft unchanged. "
        + bedrock._BANNED_TRAVEL_CLICHES_RULE
        + " "
        + bedrock._ATLAS_PROSE_LEADIN_RULE
    )
    user = (
        f"Resort: {resort_label}\n\nNOTES:\n{json.dumps(notes, ensure_ascii=False)}\n\n"
        f"DRAFT:\n{draft}\n"
    )
    raw = ollama_chat(
        base_url=base_url,
        model=model,
        system_text=system,
        user_text=user,
        temperature=0.3,
        num_predict=_num_predict(tier),
        num_ctx=num_ctx,
        timeout=timeout,
    )
    longer = _fit_to_layout(_strip_ungrounded(_clean_prose(raw), notes, tier, resort_label), tier)
    if bedrock._char_count(longer) <= bedrock._char_count(draft):
        return draft
    return longer


def write_by_section(
    notes: list[dict[str, Any]],
    *,
    resort_label: str,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
    name: str = "",
) -> str:
    """One paragraph per topic group, each with a character budget from layout_goal."""
    budgets = paragraph_budgets(tier)
    grouped = assign_sections(notes, [key for key, _budget in budgets])
    parts: list[str] = []
    for section, budget in budgets:
        group = grouped.get(section) or []
        if not group:
            continue
        room = max(budget, min(note_chars(group) + 120, 1600))
        system = (
            f"You are the writer. Write one paragraph of about {room} characters "
            f"(stay between {max(180, room - 80)} and {room + 120}). "
            f"The paragraph covers only this topic: {section}. "
            "Keep the extra clauses in these NOTES. Do not summarize them into one sentence. "
            + _STYLE
            + "Plain prose. No title, no markdown, no bullet list. "
            + _GROUNDING_RULE
        )
        raw = ollama_chat(
            base_url=base_url,
            model=model,
            system_text=system,
            user_text=(
                f"Resort: {resort_label}\n\nNOTES:\n{json.dumps(group, ensure_ascii=False)}\n"
            ),
            temperature=0.3,
            num_predict=max(400, room // 2),
            num_ctx=num_ctx,
            timeout=timeout,
        )
        paragraph = _clean_prose(raw).replace("\n", " ").strip()
        if paragraph:
            parts.append(paragraph)
    text = "\n\n".join(parts)
    return _strip_ungrounded(text, notes, tier, name or resort_label)


def produce_draft(
    notes: list[dict[str, Any]],
    beats: list[str],
    *,
    resort_label: str,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
    name: str = "",
    section_writing: bool | None = None,
) -> str:
    use_sections = section_writing_for(tier) if section_writing is None else section_writing
    if use_sections:
        return write_by_section(
            notes,
            resort_label=resort_label,
            tier=tier,
            base_url=base_url,
            model=model,
            num_ctx=num_ctx,
            timeout=timeout,
            name=name,
        )
    return write_draft(
        notes,
        beats,
        resort_label=resort_label,
        tier=tier,
        base_url=base_url,
        model=model,
        num_ctx=num_ctx,
        timeout=timeout,
    )


def _stitch_unused_notes(text: str, leftover: list[dict[str, Any]], tier: str) -> str:
    """Append leftover note text as paragraphs so the model cannot throw the pile away."""
    if not leftover:
        return text
    goal = bedrock.copy_goal_for_tier(tier)
    paras = list(bedrock._paragraphs(text)) if str(text or "").strip() else []
    for note in leftover:
        body = str(note.get("text") or "").strip()
        if not body:
            continue
        if body[-1] not in ".!?":
            body += "."
        paras.append(body)
        joined = "\n\n".join(paras)
        if not bedrock._copy_under_min(bedrock.measure_copy(joined, tier), goal):
            break
    return "\n\n".join(paras)


def length_control_loop(
    draft: str,
    notes: list[dict[str, Any]],
    *,
    resort_label: str,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
    passes: int = 4,
) -> tuple[str, str | None, list[dict[str, Any]]]:
    """Ask the editor to use leftover notes. Stop when the page is long enough or the notes run out."""
    text = draft
    for _ in range(max(0, passes)):
        if short_reason_for(text, notes, tier) is None:
            return text, None, unused_notes(text, notes)
        leftover = unused_notes(text, notes)
        if not leftover:
            return text, "facts_exhausted", []
        longer = extend_draft(
            text,
            leftover,
            resort_label=resort_label,
            tier=tier,
            base_url=base_url,
            model=model,
            num_ctx=num_ctx,
            timeout=timeout,
        )
        if bedrock._char_count(longer) <= bedrock._char_count(text):
            stitched = _fit_to_layout(
                _strip_ungrounded(
                    _stitch_unused_notes(text, leftover, tier),
                    notes,
                    tier,
                    resort_label,
                ),
                tier,
            )
            if bedrock._char_count(stitched) > bedrock._char_count(text):
                text = stitched
                break
            return text, "writer_underused_notes", leftover
        text = longer
    leftover = unused_notes(text, notes)
    if short_reason_for(text, notes, tier) and leftover:
        stitched = _fit_to_layout(
            _strip_ungrounded(
                _stitch_unused_notes(text, leftover, tier),
                notes,
                tier,
                resort_label,
            ),
            tier,
        )
        if bedrock._char_count(stitched) > bedrock._char_count(text):
            text = stitched
    reason = short_reason_for(text, notes, tier)
    return text, reason, unused_notes(text, notes)


def finish_prose(
    text: str,
    notes: list[dict[str, Any]],
    *,
    name: str,
    country: str | None,
    state: str | None,
    tier: str,
) -> str:
    """Apply note distances and drop sentences that name some other place."""
    text = reconcile_distances(text, notes)
    text = drop_foreign_sentences(text, notes, name=name, country=country, state=state)
    return _fit_to_layout(text, tier) if text.strip() else text


def critique_for_draft(
    draft: str,
    notes: list[dict[str, Any]],
    *,
    tier: str,
    name: str,
    country: str | None,
    state: str | None,
    base_url: str,
    model: str | None,
    num_ctx: int,
    timeout: float,
    use_llm: bool,
) -> dict[str, Any]:
    """Deterministic checks always run. The model critic is optional and cannot add free-form items."""
    det = deterministic_critique(draft, notes, name=name, country=country, state=state, tier=tier)
    if not use_llm or not model or model == "deterministic":
        return det
    llm = critique_draft(
        draft,
        notes,
        tier=tier,
        base_url=base_url,
        model=model,
        num_ctx=num_ctx,
        timeout=timeout,
    )
    llm["deterministic"] = {
        "unsupported": det["unsupported"],
        "contradictions": det["contradictions"],
        "unused_notes": det["unused_notes"],
    }
    llm["fixes"] = list(dict.fromkeys([*llm.get("fixes", []), *det.get("fixes", [])]))
    llm["contradictions"] = det["contradictions"]
    llm["unused_notes"] = det["unused_notes"]
    return llm
