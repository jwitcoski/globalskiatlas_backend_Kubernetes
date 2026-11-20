"""Stay22 lodging search for the resort-copy search pass.

Northern resorts use a mid-February weekend; southern resorts use a mid-August
weekend. Same bookmark every run so lodging notes stay comparable.
"""
from __future__ import annotations

import calendar
import math
import urllib.parse
from datetime import date, timedelta
from typing import Any

from . import bedrock

# Same affiliate id as GlobalSkiAtlas_2 wiki / playable stay links.
STAY22_AID = "6abe4ecf20efb8597f4c40dc"


def _nth_saturday(year: int, month: int, n: int) -> date:
    weeks = calendar.monthcalendar(year, month)
    saturdays = [week[calendar.SATURDAY] for week in weeks if week[calendar.SATURDAY]]
    if n < 1 or n > len(saturdays):
        raise ValueError(f"no Saturday #{n} in {year}-{month:02d}")
    return date(year, month, saturdays[n - 1])


def lodging_weekend(*, lat: float, today: date | None = None) -> tuple[date, date]:
    """Two-night weekend stay: 3rd Sat–Mon in February (north) or 2nd Sat–Mon in August (south)."""
    today = today or date.today()
    southern = lat < 0
    month = 8 if southern else 2
    nth = 2 if southern else 3
    year = today.year
    checkin = _nth_saturday(year, month, nth)
    if checkin <= today:
        checkin = _nth_saturday(year + 1, month, nth)
    return checkin, checkin + timedelta(days=2)


def stay22_map_url(
    *,
    lat: float,
    lon: float,
    name: str,
    checkin: date,
    checkout: date,
    campaign: str = "resort-copy",
) -> str:
    """Stay22 map search used as a lodging source in research (same query shape as the frontend)."""
    query = urllib.parse.urlencode(
        {
            "aid": STAY22_AID,
            "lat": f"{lat:.5f}",
            "lng": f"{lon:.5f}",
            "checkin": checkin.isoformat(),
            "checkout": checkout.isoformat(),
            "adults": "2",
            "campaign": campaign,
            "address": name,
        }
    )
    # /embed/gm is the on-site widget; /map resolves to the product page we can fetch.
    return f"https://www.stay22.com/map?{query}"


def _coords(row: dict[str, Any] | None) -> tuple[float, float] | None:
    if not row:
        return None
    try:
        lat = float(row.get("centroid_lat"))
        lon = float(row.get("centroid_lon"))
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(lat) and math.isfinite(lon)):
        return None
    if abs(lat) > 90 or abs(lon) > 180:
        return None
    return lat, lon


def add_stay22_lodging_snippet(
    snippets: list[dict[str, Any]],
    *,
    name: str,
    row: dict[str, Any] | None = None,
    lat: float | None = None,
    lon: float | None = None,
    today: date | None = None,
) -> list[dict[str, Any]]:
    """Append one Stay22 lodging search snippet so fetch/search can mine hotel facts."""
    if lat is None or lon is None:
        coords = _coords(row)
        if coords is None:
            return snippets
        lat, lon = coords
    checkin, checkout = lodging_weekend(lat=lat, today=today)
    url = stay22_map_url(lat=lat, lon=lon, name=name, checkin=checkin, checkout=checkout)
    if any(str(s.get("url") or "") == url for s in snippets):
        return snippets
    hemisphere = "southern" if lat < 0 else "northern"
    text = (
        f"Stay22 lodging map for {name} ({hemisphere} mid-season weekend). "
        f"Fixed search dates: check-in {checkin.isoformat()}, check-out {checkout.isoformat()}, "
        f"2 adults. Use nearby hotel and Airbnb results for nightly prices, pools, "
        f"hot tubs, and ski-in ski-out stays when the page states them. "
        f"Do not invent a rate that is not on the page. URL: {url}"
    )
    snippets.append(
        {
            "id": "S22",
            "source": "stay22",
            "title": f"{name} lodging Stay22 {checkin.isoformat()} to {checkout.isoformat()}",
            "url": url,
            "text": text,
        }
    )
    bedrock._renumber_snippet_ids(snippets)
    return snippets
