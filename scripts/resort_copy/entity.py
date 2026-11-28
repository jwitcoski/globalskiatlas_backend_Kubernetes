"""Canonical entity record, official-domain discovery, and per-source relevance decisions."""
from __future__ import annotations

import re
import unicodedata
from dataclasses import asdict, dataclass, field
from typing import Any, Callable

from .sources import DIRECTORY_HOSTS, NO_FETCH_HOSTS, SourceRegistry, host_in, host_of

# Hosts that publish pass-product information. A page here is evidence only for pass affiliation.
PASS_PROVIDERS: dict[str, str] = {
    "epic": "epicpass.com",
    "ikon": "ikonpass.com",
    "indy": "indyskipass.com",
    "mountain collective": "mountaincollective.com",
    "powder alliance": "powderalliance.com",
    "peak pass": "peakresorts.com",
    "rcr rockies": "rcrrockiespass.com",
    "magic pass": "magicpass.ch",
    "skistar": "skistar.com",
    "niseko united": "niseko.ne.jp",
}

SKI_TERMS = re.compile(r"\b(?:ski|skiing|snowboard\w*|snow|slopes?|lifts?|trails?|resort|terrain|mountain)\b", re.IGNORECASE)

# "Massanutten Military Academy" is not "Massanutten Resort". These follow the resort name directly.
NAMESAKE_SUFFIX = (
    "military academy", "academy", "school", "high school", "elementary", "college", "university", "church",
    "hospital", "cemetery", "library", "township", "county", "river", "creek", "brewery", "bank", "fire department",
    "regional library", "park authority", "story",
)

# Optional per-resort seeds. Discovery adds more at run time.
SEED_NAMESAKES: dict[str, tuple[str, ...]] = {
    "massanutten": ("Massanutten Military Academy", "Massanutten Mountain", "Massanutten Regional Library"),
}

PLACE_PAGE = re.compile(r"census-designated place|unincorporated community|\bis a (?:town|city|village|county)\b", re.IGNORECASE)

US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
    "maine": "ME", "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}

GENERIC_WORDS = frozenset(
    "ski area resort mountain mountains mount club snow hill hills valley peak ridge lake park the and of de la le "
    "recreation center centre winter basin bowl".split()
)


def squash(text: str) -> str:
    text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def name_tokens(name: str) -> list[str]:
    """Distinctive words of the resort name."""
    return [t for t in re.findall(r"[a-z0-9]+", squash_keep_space(name)) if t not in GENERIC_WORDS and len(t) >= 4]


def squash_keep_space(text: str) -> str:
    return unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode().lower()


def mentions_name(text: str, name: str) -> bool:
    low = squash_keep_space(text)
    if squash(name) and squash(name) in squash(low):
        return True
    tokens = name_tokens(name)
    return bool(tokens) and all(t in low for t in tokens)


@dataclass
class EntityRecord:
    entity_id: str
    canonical_name: str
    entity_type: str
    geographic_region: str
    state: str | None = None
    country: str | None = None
    official_domain: str | None = None
    official_url: str | None = None
    official_source_id: str | None = None
    aliases: list[str] = field(default_factory=list)
    known_unrelated_namesakes: list[str] = field(default_factory=list)
    resolution_notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def make_entity_id(name: str, state: str | None, country: str | None) -> str:
    parts = (re.sub(r"[^a-z0-9]+", "-", squash_keep_space(p)).strip("-") for p in (name, state or "", country or ""))
    return "-".join(p for p in parts if p)


def new_entity(name: str, state: str | None, country: str | None) -> EntityRecord:
    region = ", ".join(p for p in (state, country) if p)
    aliases = [name]
    if not re.search(r"\b(?:resort|ski area|mountain)\b", name, re.IGNORECASE):
        aliases.append(f"{name} Resort")
    seeds = [n for key, vals in SEED_NAMESAKES.items() if key in squash_keep_space(name) for n in vals]
    return EntityRecord(
        entity_id=make_entity_id(name, state, country),
        canonical_name=name,
        entity_type="ski_resort",
        geographic_region=region,
        state=state,
        country=country,
        aliases=aliases,
        known_unrelated_namesakes=list(dict.fromkeys(seeds)),
    )


def _suffix_hit(entity: EntityRecord, text: str) -> str | None:
    low = squash_keep_space(text)
    base = re.escape(squash_keep_space(entity.canonical_name).strip())
    m = re.search(rf"{base}\s+(?:{'|'.join(NAMESAKE_SUFFIX)})\b", low)
    return m.group(0) if m else None


def note_namesake(entity: EntityRecord, title: str, url: str = "") -> str | None:
    """Record an unrelated namesake seen in a result title or URL so later results are rejected quickly."""
    hit = _suffix_hit(entity, f"{title} {url.replace('_', ' ').replace('-', ' ')}")
    if hit and not any(hit == squash_keep_space(n) for n in entity.known_unrelated_namesakes):
        entity.known_unrelated_namesakes.append(hit.title())
    return hit


def _state_mentioned(entity: EntityRecord, text: str) -> bool:
    if not entity.state:
        return True
    low = text.lower()
    state = entity.state.lower()
    if state in low:
        return True
    abbr = US_STATES.get(state)
    return bool(abbr and re.search(rf"\b{abbr}\b", text))


def assess_source(entity: EntityRecord, rec: dict[str, Any]) -> dict[str, Any]:
    """Relevance decision for one fetched page: official, provider, entity, regional, namesake, unrelated.

    Deliberately not a title-name filter: official pages whose titles omit the resort name pass on host,
    and pass-provider pages pass as provider evidence.
    """
    url = rec.get("final_url") or rec.get("url") or ""
    host = host_of(url)
    title = rec.get("title") or ""
    text = rec.get("text") or ""
    lead = text[:600]
    if host_in(host, entity.official_domain):
        return _decision(rec, "official", "official", "host matches verified official domain")
    for provider, domain in PASS_PROVIDERS.items():
        if host_in(host, domain):
            return _decision(rec, "pass_provider", "provider", f"pass provider {provider}")
    if rec.get("fetch_status") != "ok":
        return _decision(rec, "unclassified", "unusable", f"fetch status {rec.get('fetch_status')}")
    typed = _third_party_type(host)
    head = f"{title} {url}".replace("_", " ").replace("-", " ")
    for namesake in entity.known_unrelated_namesakes:
        if squash(namesake) and squash(namesake) in squash(head + " " + lead):
            return _decision(rec, typed, "namesake", f"matches known unrelated namesake '{namesake}'")
    if _suffix_hit(entity, head + " " + lead):
        note_namesake(entity, head)
        return _decision(rec, typed, "namesake", "title or lead names an unrelated entity sharing the resort name")
    named = mentions_name(f"{title} {text}", entity.canonical_name)
    ski = len(SKI_TERMS.findall(text[:4000]))
    if named and ski >= 3 and not PLACE_PAGE.search(lead):
        if _state_mentioned(entity, f"{title} {text}"):
            return _decision(rec, typed, "entity", "names the resort with ski content and matching region")
        return _decision(rec, typed, "unrelated", "names the resort but never mentions its region (possible namesake)")
    if named and PLACE_PAGE.search(lead):
        return _decision(rec, typed, "regional", "page describes the surrounding place, not the resort")
    region_hit = bool(entity.state and entity.state.lower() in text.lower())
    if region_hit and ski >= 3 and typed in {"tourism", "reputable_secondary"}:
        return _decision(rec, typed, "regional", "regional or tourism page without the resort name")
    return _decision(rec, typed, "unrelated", "does not describe this resort")


def _decision(rec: dict[str, Any], source_type: str, decision: str, reason: str) -> dict[str, Any]:
    rec["source_type"] = source_type
    rec["relevance"] = {"decision": decision, "reason": reason}
    return rec["relevance"]


def _third_party_type(host: str) -> str:
    if "wikipedia.org" in host:
        return "wikipedia"
    if any(d in host for d in DIRECTORY_HOSTS):
        return "directory"
    if re.search(r"visit|tourism|travel|vacation|explore|destination|countyva|\.gov$|chamber|\.va\.us", host):
        return "tourism"
    return "reputable_secondary"


SOURCE_TIER = {
    "official": 0, "pass_provider": 1, "tourism": 2, "reputable_secondary": 3, "wikipedia": 4, "directory": 5,
    "unclassified": 6,
}


# ----- Official-domain discovery ------------------------------------------------------------------------------

SearchFn = Callable[[str, int], list[dict[str, str]]]


def resolve_entity(
    name: str,
    state: str | None,
    country: str | None,
    *,
    search: SearchFn,
    registry: SourceRegistry,
    round_index: int = 0,
    seed_official_url: str | None = None,
) -> EntityRecord:
    """Find and verify the official domain before any broad fact extraction."""
    entity = new_entity(name, state, country)
    region = ", ".join(p for p in (state, country) if p)
    if seed_official_url:
        seed = seed_official_url.strip()
        if seed and not seed.startswith("http"):
            seed = f"https://{seed}"
        host = host_of(seed)
        thin = bool(host and any(b in host + "/" for b in NO_FETCH_HOSTS))
        if host and not any(d in host for d in DIRECTORY_HOSTS):
            if thin:
                # Facebook / social: keep the full seeded URL; do not claim an official domain.
                home = registry.fetch(seed, reason="entity_seed_thin", topic="entity", allow_blocked=True)
                if home is not None and home.get("fetch_status") == "ok":
                    entity.official_url = home.get("final_url") or home.get("url") or seed
                    entity.official_source_id = home["source_id"]
                    entity.resolution_notes.append(
                        f"thin primary source {host} seeded ({home['source_id']}); no official domain"
                    )
                    assess_source(entity, home)
                    return entity
                entity.resolution_notes.append(
                    f"thin seed {host} could not be read ({(home or {}).get('fetch_status', 'skipped')})"
                )
            else:
                target = seed if urlparse_depth(seed) <= 1 else f"https://{host}/"
                home = registry.fetch(target, reason="entity_seed", topic="entity")
                # Also keep the deep seed (tickets/passes) when the home page verified.
                if home is not None and home.get("fetch_status") == "ok":
                    head = f"{home.get('title', '')} {home.get('text', '')[:1500]}"
                    if (mentions_name(head, name) or squash(name) in squash(host)) and SKI_TERMS.search(head):
                        entity.official_domain = host
                        entity.official_url = home.get("final_url") or home.get("url") or seed
                        entity.official_source_id = home["source_id"]
                        entity.resolution_notes.append(
                            f"official domain {host} seeded and verified ({home['source_id']})"
                        )
                        assess_source(entity, home)
                        if seed.rstrip("/") != target.rstrip("/"):
                            registry.fetch(seed, reason="entity_seed_deep", topic="entity")
                        return entity
                    entity.resolution_notes.append(f"seed {host} rejected: homepage does not describe {name}")
                else:
                    entity.resolution_notes.append(
                        f"seed {host or seed} could not be read ({(home or {}).get('fetch_status', 'skipped')})"
                    )
    queries = [
        f'"{name}" "{state or country or ""}" official resort'.replace(' ""', ""),
        f'"{name}" ski resort {region} official website'.strip(),
        ]
    candidates: dict[str, float] = {}
    for query in queries:
        if registry.queries_left() <= 0:
            break
        rows = registry.record_search(query, search(query, registry.budgets.results_per_query), topic="entity", round_index=round_index)
        for row in rows:
            url, title = row["url"], row["title"]
            host = host_of(url)
            if not host or any(b in host + "/" for b in NO_FETCH_HOSTS) or any(d in host for d in DIRECTORY_HOSTS):
                continue
            # Record a namesake before discarding the row: the academy/lake/mountain sharing the
            # resort's name is exactly the page that must never become evidence later. Fetching it
            # (rather than only noting the name) leaves an auditable record that it was seen,
            # classified, and refused, so a later run cannot quietly cite it.
            if note_namesake(entity, title, url):
                known = registry.known_url(url)
                rec = registry.get(known) if known else None
                if rec is None:
                    rec = registry.fetch(url, reason="entity_namesake_rejected", topic="entity")
                if rec is not None:
                    assess_source(entity, rec)
                continue
            if "wikipedia.org" in host or any(host_in(host, d) for d in PASS_PROVIDERS.values()):
                continue
            blob = f"{title} {row['snippet']}"
            score = 0.0
            if squash(name) and squash(name) in squash(host):
                score += 5
            elif any(t in squash(host) for t in name_tokens(name)):
                score += 3
            if mentions_name(title, name):
                score += 3
            elif mentions_name(blob, name):
                score += 1
            if SKI_TERMS.search(blob):
                score += 2
            if _state_mentioned(entity, blob):
                score += 1
            if urlparse_depth(url) <= 1:
                score += 1
            if re.search(r"\b(?:review|reviews|deals?|best)\b", title, re.IGNORECASE):
                score -= 2
            if score >= 5:
                candidates[host] = max(candidates.get(host, 0), score)
    for host, _score in sorted(candidates.items(), key=lambda kv: -kv[1])[:3]:
        home = registry.fetch(f"https://{host}/", reason="entity_probe", topic="entity")
        if home is None or home["fetch_status"] not in ("ok",):
            entity.resolution_notes.append(f"candidate {host} could not be read ({(home or {}).get('fetch_status', 'skipped')})")
            continue
        head = f"{home.get('title', '')} {home['text'][:1500]}"
        if (mentions_name(head, name) or squash(name) in squash(host)) and SKI_TERMS.search(head):
            entity.official_domain = host
            entity.official_url = home["url"]
            entity.official_source_id = home["source_id"]
            entity.resolution_notes.append(f"official domain {host} verified from homepage {home['source_id']}")
            assess_source(entity, home)
            break
        entity.resolution_notes.append(f"candidate {host} rejected: homepage does not describe {name}")
    if not entity.official_domain:
        entity.resolution_notes.append("official domain not established; operating facts must come from other verified sources")
    return entity


def urlparse_depth(url: str) -> int:
    from urllib.parse import urlsplit

    return len([p for p in urlsplit(url).path.split("/") if p])
