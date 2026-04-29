"""Pull upcoming tennis fixtures from Sofascore's unofficial API.

Endpoint: https://api.sofascore.com/api/v1/sport/tennis/scheduled-events/{YYYY-MM-DD}

Returns every scheduled tennis match for a date — singles, doubles, all tours.
We filter to ATP/WTA singles, drop completed matches, and persist as Fixture rows.

Sofascore is unofficial, no auth, but they do rate-limit and 403 if hammered.
We always send a browser-ish User-Agent and never poll faster than once per
minute. If it 403s, the call returns [] and we move on — better silent than
crashing the bot.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

import requests
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ..db import Fixture, init_db, session
from ..name_resolver import resolve

log = logging.getLogger(__name__)

BASE = "https://api.sofascore.com/api/v1"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 "
                  "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Accept": "application/json",
    "Referer": "https://www.sofascore.com/",
}


def _classify_tour(category_name: str | None, tournament_name: str | None) -> str | None:
    s = " ".join(filter(None, [category_name, tournament_name])).lower()
    if "atp" in s or "challenger" in s:
        return "atp"
    if "wta" in s:
        return "wta"
    return None


def _surface_from_ground(ground: str | None) -> str | None:
    if not ground:
        return None
    g = ground.lower()
    if "clay" in g:
        return "Clay"
    if "grass" in g:
        return "Grass"
    if "carpet" in g:
        return "Carpet"
    if "hard" in g or "indoor" in g or "hardcourt" in g:
        return "Hard"
    return None


def fetch_scheduled(d: date) -> list[dict]:
    url = f"{BASE}/sport/tennis/scheduled-events/{d.isoformat()}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            log.warning("sofascore %s -> %s", d, r.status_code)
            return []
        return r.json().get("events", [])
    except requests.RequestException as e:
        log.warning("sofascore fetch failed for %s: %s", d, e)
        return []


def _parse_event(ev: dict) -> dict | None:
    if ev.get("status", {}).get("type") in ("finished", "interrupted", "canceled"):
        return None
    home = ev.get("homeTeam", {})
    away = ev.get("awayTeam", {})
    if home.get("type") != 1 or away.get("type") != 1:  # 1 = singles, 2 = doubles
        return None

    tournament = ev.get("tournament", {}) or {}
    category = tournament.get("category", {}) or {}
    season = ev.get("season", {}) or {}
    tour = _classify_tour(category.get("name"), tournament.get("name"))
    if tour not in ("atp", "wta"):
        return None

    start_ts = ev.get("startTimestamp")
    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc) if start_ts else None

    ground = (ev.get("groundType") or tournament.get("groundType")
              or (category.get("slug") if category else None))
    return {
        "source": "sofascore",
        "source_id": str(ev["id"]),
        "tour": tour,
        "date": start_dt.date() if start_dt else None,
        "start_ts": start_dt.replace(tzinfo=None) if start_dt else None,
        "tourney_name": tournament.get("name"),
        "round": (ev.get("roundInfo") or {}).get("name"),
        "surface": _surface_from_ground(ground),
        "indoor": 1 if "indoor" in (ground or "").lower() else 0,
        "player_a_name": home.get("name") or home.get("shortName"),
        "player_b_name": away.get("name") or away.get("shortName"),
        "season": season.get("year"),
    }


def ingest_window(start: date | None = None, days: int = 5, engine=None) -> int:
    engine = engine or init_db()
    start = start or date.today()
    rows = []
    for i in range(days):
        d = start + timedelta(days=i)
        for ev in fetch_scheduled(d):
            parsed = _parse_event(ev)
            if not parsed or not parsed["player_a_name"] or not parsed["player_b_name"]:
                continue
            parsed["player_a_id"] = resolve(parsed["player_a_name"], parsed["tour"])
            parsed["player_b_id"] = resolve(parsed["player_b_name"], parsed["tour"])
            parsed.pop("season", None)
            rows.append(parsed)

    if not rows:
        log.info("no fixtures in window starting %s", start)
        return 0

    stmt = sqlite_insert(Fixture).values(rows).prefix_with("OR IGNORE")
    with session(engine) as s:
        result = s.execute(stmt)
        s.commit()
    inserted = result.rowcount or 0
    log.info("inserted %d fixtures (of %d candidates)", inserted, len(rows))
    return inserted
