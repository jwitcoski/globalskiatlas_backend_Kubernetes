"""Guards that keep medium and large copy to what NOTES support."""
from __future__ import annotations

import re
from typing import Any

from . import bedrock

from .config import (
    _tier_key,
)
from .notes import (
    _content_words,
)


_GROUNDING_RULE = (
    "Every sentence must restate something a NOTES item says. "
    "Do not describe weather, snow conditions, scenery, mood, or atmosphere. "
    "Do not write about fireplaces, cozy lodges, hot cocoa, apres-ski, crisp air, powder, or how the place feels. "
    "Do not invent trail names, lifts, restaurants, events, or amenities. "
    "If NOTES run out, stop. A shorter page is correct; padding is wrong."
)


_ATMOSPHERE = re.compile(
    r"\b(?:fire(?:place|side)?|crackling|cozy|cosy|comfy|comfortable|hot (?:cocoa|chocolate)|cocoa|"
    r"snow-?(?:covered|capped|dusted)|snowy|blanket of snow|fresh powder|powder days?|crisp|"
    r"winter wonderland|breathtaking|stunning|picturesque|scenic|charming|magical|unforgettable|"
    r"memories|adventure awaits|whether you(?:'re| are)|perfect (?:for|spot|place|getaway)|"
    r"après|apres|warm up|sipping|bustling|vibrant|thrill-?seekers?|"
    r"leave you|sense of|atmosphere|ambiance|ambience)\b",
    re.IGNORECASE,
)


def _strip_ungrounded(text: str, notes: list[dict[str, Any]], tier: str, name: str = "") -> str:
    """Medium and large pages: drop sentences that are mood filler or not backed by NOTES."""
    if _tier_key(tier) == "small":
        return text
    blob = " ".join(str(n.get("text") or "") for n in notes) + " " + name
    known = _content_words(blob)
    known_nums = set(re.findall(r"\d[\d,.]*", blob))
    paras: list[str] = []
    for para in bedrock._paragraphs(text):
        kept: list[str] = []
        for sentence in re.split(r"(?<=[.!?])\s+", para):
            if _ATMOSPHERE.search(sentence):
                continue
            nums = set(re.findall(r"\d[\d,.]*", sentence))
            if any(n.rstrip(".,") not in {k.rstrip(".,") for k in known_nums} for n in nums):
                continue
            words = _content_words(sentence)
            if len(words) >= 5 and len(words & known) < 0.4 * len(words):
                continue
            kept.append(sentence)
        if kept:
            paras.append(" ".join(kept))
    return "\n\n".join(paras)
