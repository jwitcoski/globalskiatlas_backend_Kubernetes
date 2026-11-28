"""Research orchestrator: resolve the entity, cover every category, keep only evidence-backed facts."""
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .entity import (
    PASS_PROVIDERS,
    SOURCE_TIER,
    EntityRecord,
    assess_source,
    host_in,
    host_of,
    mentions_name,
    note_namesake,
    resolve_entity,
    make_entity_id,
)
from .evidence import (
    Candidate,
    FactStore,
    LlmJson,
    _with_sources,
    build_fact,
    candidates_for,
    norm,
    pass_names,
    units,
    validate_fact,
)
from .sources import (
    DIRECTORY_HOSTS,
    NO_FETCH_HOSTS,
    Budgets,
    DdgsSearch,
    SourceRegistry,
    http_get,
    now_iso,
)
from .topics import (
    ATTRACTION_NAV,
    PASS_TOPIC,
    TOPIC_BY_KEY,
    TOPICS,
    Topic,
    max_rounds,
    nav_covers_topic,
    nav_terms_for,
    queries_for,
)

SearchFn = Callable[[str, int], list[dict[str, str]]]

_PROVIDER_NEGATIVE = re.compile(r"\b(?:leav\w+|no longer|removed|dropped|ending|former|formerly|left)\b", re.IGNORECASE)
_PROVIDER_NAME = {domain: label for label, domain in PASS_PROVIDERS.items()}
_PRIMARY_PROVIDERS = ("indy", "epic", "ikon", "mountain collective", "powder alliance")


@dataclass
class ResearchResult:
    entity: EntityRecord
    registry: SourceRegistry
    store: FactStore
    events: list[str] = field(default_factory=list)
    unverified_passes: list[dict[str, Any]] = field(default_factory=list)
    started_at: str = field(default_factory=now_iso)

    # -- completeness
    def verified_count(self) -> int:
        """Verified facts in required categories. Background trivia and unverified claims do not count."""
        return sum(1 for f in self.store.verified() if TOPIC_BY_KEY.get(f["topic"], Topic("", "", "", (), "", counts=False)).counts)

    def gaps(self) -> list[str]:
        return [t.key for t in TOPICS if t.counts and not self.store.has(t.key)]

    def is_complete(self, minimum: int) -> bool:
        return self.verified_count() >= minimum

    # -- views for the existing pipeline
    def to_notes(self) -> list[dict[str, Any]]:
        """Writer input: verified facts only, each carrying resolvable source ids and its quote."""
        notes: list[dict[str, Any]] = []
        for fact in self.store.verified():
            topic = TOPIC_BY_KEY.get(fact["topic"])
            legacy = topic.legacy_topic if topic else "scene"
            quote = fact["evidence_quotes"][0] if fact["evidence_quotes"] else ""
            if fact["value"] is False:
                text = f"{fact['label']}: not offered. Source says: {quote}"
            elif fact["value"] is True or not isinstance(fact["value"], (str, list)):
                text = f"{fact['label']}: {quote}"
            else:
                value = ", ".join(fact["value"]) if isinstance(fact["value"], list) else fact["value"]
                text = f"{fact['label']}: {value}"
            notes.append(
                {
                    "topic": legacy,
                    "fact_topic": fact["topic"],
                    "text": text,
                    "snippet_ids": list(fact["source_ids"]),
                    "source_urls": list(fact["source_urls"]),
                    "evidence_quotes": list(fact["evidence_quotes"]),
                    "season": fact.get("season"),
                }
            )
        return notes

    def to_key_points(self) -> dict[str, Any]:
        from .key_points import _KEY_POINT_FIELDS

        points: dict[str, Any] = {field_: None for field_ in _KEY_POINT_FIELDS}
        points.update({"social": [], "pass_products": [], "audience": []})

        def entry(fact: dict[str, Any], value: Any) -> dict[str, Any]:
            return {
                "value": value,
                "snippet_ids": list(fact["source_ids"]),
                "sources": list(fact["source_urls"]),
                "evidence_quotes": list(fact["evidence_quotes"]),
            }

        if self.entity.official_url:
            points["website"] = {
                "value": f"https://{self.entity.official_domain}/",
                "snippet_ids": [self.entity.official_source_id] if self.entity.official_source_id else [],
                "sources": [self.entity.official_url],
                "evidence_quotes": [],
            }
        for fact in self.store.verified():
            topic = TOPIC_BY_KEY.get(fact["topic"])
            if topic is None or not topic.legacy_field:
                continue
            if fact["topic"] == PASS_TOPIC:
                names = fact["value"] if isinstance(fact["value"], list) else [fact["value"]]
                points["pass_products"] = [entry(fact, n) for n in names]
                points["pass_affiliation"] = entry(fact, ", ".join(names))
                continue
            value: Any = fact["value"]
            if topic.kind == "bool":
                value = "yes" if value is True else "no"
            points[topic.legacy_field] = entry(fact, value)
        return points

    def snippets(self) -> list[dict[str, Any]]:
        """Snippet-shaped rows for the sticky/top-up pile (full body when fetch succeeded)."""
        out: list[dict[str, Any]] = []
        for r in self.registry.sources.values():
            ok = r.get("fetch_status") == "ok"
            text = r.get("text") or ""
            out.append(
                {
                    "id": r["source_id"],
                    "source": r.get("source_type") or "research",
                    "title": r.get("title"),
                    "url": r.get("final_url") or r.get("url"),
                    "read_in_full": ok,
                    "fetched": ok,
                    "relevance": r.get("relevance"),
                    "text": text[:1500],
                    "body": text if ok else "",
                }
            )
        return out

    def to_checkpoint(self) -> dict[str, Any]:
        return {
            "entity": self.entity.to_dict(),
            "facts": list(self.store.facts.values()),
            "rejected_facts": self.store.rejected,
            "unverified_pass_candidates": self.unverified_passes,
            "sources": self.registry.summary(),
            "search_results": self.registry.search_results,
            "queries_run": self.registry.queries_run,
            "verified_count": self.verified_count(),
            "gaps": self.gaps(),
            "started_at": self.started_at,
        }


# ----- helpers ------------------------------------------------------------------------------------------------


def _url_priority(entity: EntityRecord, topic: Topic, row: dict[str, Any]) -> int | None:
    """Lower is fetched first. None means do not fetch."""
    url, title = row["url"], row.get("title", "")
    host = host_of(url)
    if not host or any(b in host + "/" for b in NO_FETCH_HOSTS):
        return None
    blob = f"{title} {row.get('snippet', '')}"
    if note_namesake(entity, title, url):
        return None
    if host_in(host, entity.official_domain):
        return 0
    for domain in PASS_PROVIDERS.values():
        if host_in(host, domain):
            # Pass-operator destination pages also carry terrain/access/lodging copy.
            if topic.key == PASS_TOPIC:
                return 1
            if mentions_name(blob, entity.canonical_name):
                return 2
            return None
    if any(d in host for d in DIRECTORY_HOSTS):
        return 6 if mentions_name(blob, entity.canonical_name) else None
    if not mentions_name(blob, entity.canonical_name):
        # Regional pages may support setting, but only for topics that allow it.
        return 4 if topic.allow_regional and entity.state and entity.state.lower() in blob.lower() else None
    if "wikipedia.org" in host:
        return 5
    return 2 if re.search(r"visit|tourism|travel|explore|destination|chamber|\.gov", host) else 3


def _assess_all(entity: EntityRecord, registry: SourceRegistry) -> None:
    for rec in registry.sources.values():
        if (rec.get("relevance") or {}).get("decision") in (None, "unassessed"):
            decision = assess_source(entity, rec)
            registry.log(f"    {rec['source_id']} {decision['decision']}: {rec['url']} ({decision['reason']})")


def make_llm(base_url: str, model: str, num_ctx: int, timeout: float) -> LlmJson:
    from .ollama_client import _chat_json

    def call(system: str, user: str) -> dict[str, Any]:
        return _chat_json(
            base_url=base_url, model=model, system_text=system, user_text=user, temperature=0.0,
            num_predict=300, num_ctx=num_ctx, timeout=timeout, json_format=True,
        )

    return call


# ----- main ---------------------------------------------------------------------------------------------------


def run_research(
    *,
    name: str,
    state: str | None,
    country: str | None,
    search: SearchFn | None = None,
    llm: LlmJson | None = None,
    budgets: Budgets | None = None,
    cache_dir: Path | None = None,
    fetcher: Callable[..., Any] = http_get,
    log: Callable[[str], None] | None = None,
    sleep: Callable[[float], None] | None = None,
    seed_official_url: str | None = None,
    winter_sports_id: str | None = None,
    searxng_url: str | None = None,
    use_ddg: bool = True,
) -> ResearchResult:
    events: list[str] = []

    def emit(message: str) -> None:
        events.append(message)
        (log or (lambda m: print(m, file=sys.stderr, flush=True)))(message)

    entity_id = make_entity_id(name, state, country)
    cache_path = (cache_dir / entity_id / "registry.json") if cache_dir else None
    kwargs: dict[str, Any] = {"entity_id": entity_id, "budgets": budgets, "fetcher": fetcher, "cache_path": cache_path, "log": emit}
    if sleep is not None:
        kwargs["sleep"] = sleep
    registry = SourceRegistry(**kwargs)
    if search is None:
        from .sources import default_search

        search = default_search(searxng_url=searxng_url, use_ddg=use_ddg)
    region = ", ".join(p for p in (state, country) if p)

    emit(f"research: resolving entity for {name} ({region})")
    entity = resolve_entity(
        name,
        state,
        country,
        search=search,
        registry=registry,
        seed_official_url=seed_official_url,
    )
    for note in entity.resolution_notes:
        emit(f"  entity: {note}")
    store = FactStore(registry, entity, log=emit)
    result = ResearchResult(entity, registry, store, events)

    topic_queries: dict[str, list[str]] = {t.key: [] for t in TOPICS}
    checked: dict[str, list[str]] = {t.key: [] for t in TOPICS}
    pass_state: dict[str, Any] = {"probed": set(), "candidates": {}}

    _assess_all(entity, registry)
    _official_website_fact(entity, registry, store)
    _seed_pass_memberships(entity, registry, store, pass_state, winter_sports_id, emit)
    attraction_terms: dict[str, str] = {}
    if entity.official_domain:
        _fetch_navigation(entity, registry, attraction_terms, emit)
    _assess_all(entity, registry)

    for round_index in range(max_rounds()):
        pending = [t for t in TOPICS if not store.has(t.key)]
        if not pending:
            break
        emit(f"research round {round_index + 1}: {len(pending)} categories open, {registry.queries_left()} queries left")
        fetched_new = _discover(entity, registry, search, pending, round_index, region, topic_queries)
        _assess_all(entity, registry)
        _extract(entity, registry, store, [t for t in pending if t.key != PASS_TOPIC], llm, topic_queries, checked)
        _refresh_pass(entity, registry, store, llm, topic_queries, checked, pass_state)
        _verify_passes(entity, registry, store, search, pass_state, topic_queries, emit, round_index)
        _assess_all(entity, registry)
        _refresh_pass(entity, registry, store, llm, topic_queries, checked, pass_state)
        _attraction_facts(entity, registry, store, attraction_terms)
        registry.save()
        if not fetched_new and round_index > 0:
            emit("  no new sources this round; stopping")
            break

    for topic in TOPICS:
        if store.has(topic.key):
            continue
        fact = build_fact(topic, [], entity, registry, llm, queries=topic_queries[topic.key], checked=checked[topic.key])
        if topic.key == PASS_TOPIC and pass_state["candidates"]:
            fact["notes"] = "affiliation claimed but not confirmed on a pass provider site: " + ", ".join(sorted(pass_state["candidates"]))
        store.upsert(fact)
    result.unverified_passes = [
        {"pass": p, "claimed_in": [{"source_id": c.source_id, "quote": c.quote} for c in cs]}
        for p, cs in pass_state["candidates"].items()
        if not _pass_verified(store, p)
    ]
    registry.save()
    if cache_dir:
        out = cache_dir / entity_id / "facts.json"
        out.write_text(json.dumps(result.to_checkpoint(), ensure_ascii=False, indent=1), encoding="utf-8")
    emit(f"research: {result.verified_count()} verified categories, gaps: {', '.join(result.gaps()) or 'none'}")
    return result


def _official_website_fact(entity: EntityRecord, registry: SourceRegistry, store: FactStore) -> None:
    rec = registry.get(entity.official_source_id or "")
    if not rec:
        return
    for unit in units(rec["text"]):
        if mentions_name(unit, entity.canonical_name):
            cand = Candidate("official_website", rec["source_id"], unit, 1.0, 0)
            fact = _with_sources(
                {"topic": "official_website", "field": "official_website", "label": "Official website", "kind": "text",
                 "entity_id": entity.entity_id, "retrieved_at": now_iso(), "season": None, "queries_attempted": [],
                 "sources_checked": [rec["source_id"]], "notes": "verified official domain"},
                registry, f"https://{entity.official_domain}/", "verified", [cand], "verified official domain",
            )
            store.upsert(fact)
            return


def _seed_pass_memberships(
    entity: EntityRecord,
    registry: SourceRegistry,
    store: FactStore,
    pass_state: dict[str, Any],
    winter_sports_id: str | None,
    emit: Callable[[str], None],
) -> None:
    """Prefetch mega-pass destination pages linked to this atlas resort.

    Membership comes from harvested operator lists (Epic/Ikon/Indy/…). Those pages
    also carry terrain, access, and lodging copy useful for the essay.
    """
    if not winter_sports_id:
        return
    try:
        from .pass_lists import lookup_pass_memberships
    except Exception:
        return
    memberships = lookup_pass_memberships(winter_sports_id)
    if not memberships:
        return
    labels: list[str] = []
    for item in memberships:
        label = str(item.get("pass_label") or item.get("pass_key") or "").strip()
        urls = []
        for key in ("url", "provider_url", "listed_url", "list_url"):
            cand = str(item.get(key) or "").strip()
            if cand.startswith("http") and cand not in urls:
                urls.append(cand)
        if label:
            labels.append(label)
            # Mark as claimed so verify prefers these providers.
            key = label.lower()
            pass_state["candidates"].setdefault(key if key in _PRIMARY_PROVIDERS else label, [])
            pass_state["probed"].add(key if key in _PRIMARY_PROVIDERS else label.split()[0].lower())
        for url in urls[:2]:
            host = host_of(url)
            is_provider = any(host_in(host, domain) for domain in PASS_PROVIDERS.values())
            reason = "pass_seed_provider" if is_provider else "pass_seed_listed"
            rec = registry.fetch(url, reason=reason, topic=PASS_TOPIC)
            if rec is not None and rec.get("fetch_status") == "ok":
                emit(f"  pass seed: {label or host} → {url}")
            elif url:
                emit(f"  pass seed skipped: {label or host} ({(rec or {}).get('fetch_status', 'none')})")
    if labels and not store.has(PASS_TOPIC):
        # Roster match is operator-published membership; treat as verified list seed.
        topic = TOPIC_BY_KEY[PASS_TOPIC]
        unique = list(dict.fromkeys(labels))
        fact = {
            "topic": PASS_TOPIC,
            "field": PASS_TOPIC,
            "label": topic.label,
            "kind": "list",
            "entity_id": entity.entity_id,
            "value": unique,
            "status": "verified",
            "retrieved_at": now_iso(),
            "season": None,
            "queries_attempted": [],
            "sources_checked": [],
            "notes": "seeded from harvested mega-pass destination lists",
            "evidence": [{"quote": f"Listed on {', '.join(unique)}", "source_id": "pass_roster"}],
        }
        store.facts[PASS_TOPIC] = fact
        emit(f"  pass membership: {', '.join(unique)}")


def _fetch_navigation(entity: EntityRecord, registry: SourceRegistry, attractions: dict[str, str], emit: Callable[[str], None]) -> None:
    """Follow the resort's own navigation to category pages and attraction pages."""
    home = registry.get(entity.official_source_id or "")
    if not home:
        return
    scored: list[tuple[int, str, str]] = []
    for link in home.get("links", []):
        url, text = link["url"], norm(link["text"])
        if not host_in(host_of(url), entity.official_domain) or not text or len(text) > 60:
            continue
        label = f"{text} {url.rsplit('/', 2)[-1] if url.rstrip('/').count('/') > 2 else ''}".lower()
        # A menu says "Ski & Snowboard", not "snowmaking": match the label against nav vocabulary,
        # not against each topic's body-text keywords, or the site's best page is never fetched.
        hits = max(1, nav_terms_for(label))
        term = next((a for a in ATTRACTION_NAV if a in label), None)
        if term:
            attractions[term] = url
        scored.append((hits, url, text))
    scored.sort(key=lambda s: -s[0])
    seen: set[str] = set()
    for _hits, url, text in scored:
        if len(seen) >= registry.budgets.nav_pages:
            break
        if url in seen or registry.known_url(url):
            continue
        seen.add(url)
        emit(f"  navigation: following '{text}'")
        registry.fetch(url, reason="official_navigation", topic="navigation")


def _attraction_facts(entity: EntityRecord, registry: SourceRegistry, store: FactStore, attractions: dict[str, str]) -> None:
    for term, url in attractions.items():
        key = f"attraction_{re.sub(r'[^a-z]+', '_', term)}"
        if key in store.facts:
            continue
        sid = registry.known_url(url)
        rec = registry.get(sid) if sid else None
        if not rec or rec.get("fetch_status") != "ok" or (rec.get("relevance") or {}).get("decision") != "official":
            continue
        unit = next((u for u in units(rec["text"]) if term in u.lower() and len(u) >= 20), None)
        if unit:
            cand = Candidate(key, rec["source_id"], unit, 1.0, 0)
            store.upsert(
                _with_sources(
                    {"topic": key, "field": key, "label": f"Resort attraction: {term}", "kind": "bool",
                     "entity_id": entity.entity_id, "retrieved_at": now_iso(), "season": None,
                     "queries_attempted": [], "sources_checked": [rec["source_id"]], "notes": "from official navigation"},
                    registry, True, "verified", [cand], "from official navigation",
                )
            )


def _discover(
    entity: EntityRecord,
    registry: SourceRegistry,
    search: SearchFn,
    pending: list[Topic],
    round_index: int,
    region: str,
    topic_queries: dict[str, list[str]],
) -> int:
    new = 0
    for topic in pending:
        for query in queries_for(topic, round_index, name=entity.canonical_name, region=region, domain=entity.official_domain):
            if query in registry.queries_run:
                continue
            if registry.queries_left() <= 0:
                return new
            rows = registry.record_search(query, search(query, registry.budgets.results_per_query), topic=topic.key, round_index=round_index)
            topic_queries[topic.key].append(query)
            ranked = sorted(
                ((p, row) for row in rows if (p := _url_priority(entity, topic, row)) is not None), key=lambda x: x[0]
            )
            taken = 0
            for _priority, row in ranked:
                if taken >= registry.budgets.urls_per_query:
                    break
                if registry.known_url(row["url"]):
                    continue
                if registry.fetch(row["url"], reason=f"search:{topic.key}", topic=topic.key) is not None:
                    taken += 1
                    new += 1
    return new


def _extract(
    entity: EntityRecord,
    registry: SourceRegistry,
    store: FactStore,
    topics: list[Topic],
    llm: LlmJson | None,
    topic_queries: dict[str, list[str]],
    checked: dict[str, list[str]],
) -> None:
    for topic in topics:
        if store.has(topic.key):
            continue
        cands = _collect(topic, entity, registry, checked)
        if cands:
            fact = build_fact(topic, cands, entity, registry, llm, queries=topic_queries[topic.key], checked=checked[topic.key])
            if fact["status"] != "not_found":
                store.upsert(fact)


def _collect(topic: Topic, entity: EntityRecord, registry: SourceRegistry, checked: dict[str, list[str]]) -> list[Candidate]:
    cands: list[Candidate] = []
    for rec in registry.sources.values():
        if rec.get("fetch_status") == "ok" and (rec.get("relevance") or {}).get("decision") in ("official", "entity", "regional", "provider"):
            if rec["source_id"] not in checked[topic.key]:
                checked[topic.key].append(rec["source_id"])
        cands.extend(candidates_for(topic, rec, entity))
    return cands


def _refresh_pass(
    entity: EntityRecord,
    registry: SourceRegistry,
    store: FactStore,
    llm: LlmJson | None,
    topic_queries: dict[str, list[str]],
    checked: dict[str, list[str]],
    pass_state: dict[str, Any],
) -> None:
    cands = _collect(TOPIC_BY_KEY[PASS_TOPIC], entity, registry, checked)
    _extract_pass(entity, registry, store, cands, pass_state, topic_queries, checked)

def _pass_verified(store: FactStore, pass_name: str) -> bool:
    fact = store.facts.get(PASS_TOPIC)
    return bool(fact and fact["status"] == "verified" and pass_name in (fact["value"] or []))


def _extract_pass(
    entity: EntityRecord,
    registry: SourceRegistry,
    store: FactStore,
    cands: list[Candidate],
    pass_state: dict[str, Any],
    topic_queries: dict[str, list[str]],
    checked: dict[str, list[str]],
) -> None:
    """Resort and third-party claims are candidates. Only a pass provider page naming the resort verifies one."""
    for c in cands:
        rec = registry.sources[c.source_id]
        if (rec.get("relevance") or {}).get("decision") == "provider":
            continue
        for p in pass_names(c.quote):
            pass_state["candidates"].setdefault(p, [])
            if all(x.quote != c.quote for x in pass_state["candidates"][p]):
                pass_state["candidates"][p].append(c)
    confirmed: dict[str, Candidate] = {}
    for rec in registry.sources.values():
        if (rec.get("relevance") or {}).get("decision") != "provider" or rec.get("fetch_status") != "ok" or rec.get("duplicate_of"):
            continue
        host = host_of(rec.get("final_url") or rec["url"])
        pass_label = next((label for domain, label in _PROVIDER_NAME.items() if host_in(host, domain)), None)
        if not pass_label:
            continue
        for unit in units(rec["text"]):
            if mentions_name(unit, entity.canonical_name) and not _PROVIDER_NEGATIVE.search(unit):
                key = pass_label.title()
                confirmed.setdefault(key, Candidate(PASS_TOPIC, rec["source_id"], unit, 3.0, SOURCE_TIER["pass_provider"]))
                break
    if confirmed:
        topic = TOPIC_BY_KEY[PASS_TOPIC]
        fact = _with_sources(
            {"topic": PASS_TOPIC, "field": PASS_TOPIC, "label": topic.label, "kind": "list", "entity_id": entity.entity_id,
             "retrieved_at": now_iso(), "season": None, "queries_attempted": topic_queries[PASS_TOPIC],
             "sources_checked": checked[PASS_TOPIC], "notes": "confirmed on the pass provider's website"},
            registry, sorted(confirmed), "verified", list(confirmed.values()), "confirmed on the pass provider's website",
        )
        if not validate_fact(fact, registry):
            store.facts[PASS_TOPIC] = fact


def _verify_passes(
    entity: EntityRecord,
    registry: SourceRegistry,
    store: FactStore,
    search: SearchFn,
    pass_state: dict[str, Any],
    topic_queries: dict[str, list[str]],
    emit: Callable[[str], None],
    round_index: int,
) -> None:
    """Look for the resort on each provider's own site: claimed passes first, then the other major ones, neutrally."""
    claimed = [p.lower() for p in pass_state["candidates"]]
    order = [p for p in _PRIMARY_PROVIDERS if p in claimed] + [p for p in _PRIMARY_PROVIDERS if p not in claimed]
    for label in order:
        if label in pass_state["probed"] or registry.queries_left() <= 0:
            continue
        # A provider nobody claimed is probed only while nothing is confirmed.
        if label not in claimed and store.has(PASS_TOPIC):
            continue
        domain = PASS_PROVIDERS[label]
        pass_state["probed"].add(label)
        query = f'site:{domain} "{entity.canonical_name}"'
        rows = registry.record_search(query, search(query, registry.budgets.results_per_query), topic=PASS_TOPIC, round_index=round_index)
        topic_queries[PASS_TOPIC].append(query)
        taken = 0
        for row in rows:
            if taken >= registry.budgets.urls_per_query:
                break
            if host_in(host_of(row["url"]), domain) and not registry.known_url(row["url"]):
                if registry.fetch(row["url"], reason="pass_provider_verify", topic=PASS_TOPIC):
                    taken += 1
