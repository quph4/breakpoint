"""The Odds API client. Fetches H2H tennis odds and attaches them to Fixtures.

Free tier: 500 requests/month. We respect that ruthlessly:
  - Discover active tennis sport keys once per run.
  - For each active key, one /odds call covers all matches.
  - Skip if `BREAKPOINT_ODDS_API_KEY` env var is missing — bot still runs,
    just without live odds, no bets get placed.
  - Cache responses for 30 minutes on disk.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from sqlalchemy import select, update

from ..db import Fixture, init_db, session
from ..name_resolver import resolve

log = logging.getLogger(__name__)

API_BASE = "https://api.the-odds-api.com/v4"
CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "odds_cache"
CACHE_TTL_MIN = 30


def _api_key() -> str | None:
    return os.environ.get("BREAKPOINT_ODDS_API_KEY")


def _cache_get(name: str) -> list | dict | None:
    p = CACHE_DIR / f"{name}.json"
    if not p.exists():
        return None
    try:
        payload = json.loads(p.read_text())
        ts = datetime.fromisoformat(payload["ts"])
        if datetime.utcnow() - ts > timedelta(minutes=CACHE_TTL_MIN):
            return None
        return payload["data"]
    except Exception:
        return None


def _cache_put(name: str, data) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = CACHE_DIR / f"{name}.json"
    p.write_text(json.dumps({"ts": datetime.utcnow().isoformat(), "data": data}))


def _get(path: str, params: dict, cache_key: str | None = None) -> list | dict | None:
    if cache_key:
        cached = _cache_get(cache_key)
        if cached is not None:
            log.info("odds-api cache hit: %s", cache_key)
            return cached

    key = _api_key()
    if not key:
        log.warning("BREAKPOINT_ODDS_API_KEY not set; skipping live odds")
        return None
    params = {**params, "apiKey": key}
    try:
        r = requests.get(f"{API_BASE}{path}", params=params, timeout=20)
    except requests.RequestException as e:
        log.warning("odds-api request failed: %s", e)
        return None

    remaining = r.headers.get("x-requests-remaining")
    used = r.headers.get("x-requests-used")
    log.info("odds-api %s status=%s remaining=%s used=%s", path, r.status_code, remaining, used)

    if r.status_code != 200:
        log.warning("odds-api error: %s %s", r.status_code, r.text[:200])
        return None

    data = r.json()
    if cache_key:
        _cache_put(cache_key, data)
    return data


def active_tennis_sports() -> list[str]:
    sports = _get("/sports", {"all": "false"}, cache_key="sports")
    if not sports:
        return []
    keys = [s["key"] for s in sports if s.get("group", "").lower() == "tennis" and s.get("active")]
    log.info("active tennis sports: %s", keys)
    return keys


def fetch_odds_for_sport(sport_key: str) -> list[dict]:
    data = _get(f"/sports/{sport_key}/odds", {
        "regions": "eu",
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }, cache_key=f"odds_{sport_key}")
    return data or []


def _best_h2h_price(event: dict, player_name: str) -> tuple[float | None, str | None]:
    """Pick the best (highest) decimal price across bookmakers for a side."""
    best, best_book = None, None
    for bm in event.get("bookmakers", []):
        for m in bm.get("markets", []):
            if m.get("key") != "h2h":
                continue
            for o in m.get("outcomes", []):
                if o.get("name") == player_name:
                    price = o.get("price")
                    if price is not None and (best is None or price > best):
                        best, best_book = price, bm.get("title")
    return best, best_book


def attach_odds_to_fixtures(engine=None) -> int:
    engine = engine or init_db()
    if not _api_key():
        log.warning("no API key — skipping odds attach")
        return 0

    sports = active_tennis_sports()
    if not sports:
        return 0

    matched = 0
    with session(engine) as s:
        # Build a map of fixtures we'd like to price: scheduled, future, with both player_ids resolved.
        from datetime import date as _date
        future_q = select(Fixture).where(
            Fixture.status == "scheduled",
            Fixture.date >= _date.today(),
            Fixture.player_a_id.is_not(None),
            Fixture.player_b_id.is_not(None),
        )
        fixtures = list(s.scalars(future_q))
        log.info("priceable fixtures: %d", len(fixtures))

        # Index fixtures by (player_a_id, player_b_id) and sorted variant
        fix_index: dict[tuple[int, int], Fixture] = {}
        for f in fixtures:
            fix_index[(f.player_a_id, f.player_b_id)] = f
            fix_index[(f.player_b_id, f.player_a_id)] = f

        for sport_key in sports:
            tour = "wta" if "wta" in sport_key.lower() else "atp"
            for event in fetch_odds_for_sport(sport_key):
                home = event.get("home_team")
                away = event.get("away_team")
                if not home or not away:
                    continue
                a_id = resolve(home, tour)
                b_id = resolve(away, tour)
                if not a_id or not b_id:
                    continue
                fx = fix_index.get((a_id, b_id))
                if not fx:
                    continue
                price_a, book_a = _best_h2h_price(event, home)
                price_b, book_b = _best_h2h_price(event, away)
                if not price_a or not price_b:
                    continue
                # Align odds with the fixture's stored player order
                if (fx.player_a_id, fx.player_b_id) == (a_id, b_id):
                    fx.odds_a, fx.odds_b = price_a, price_b
                else:
                    fx.odds_a, fx.odds_b = price_b, price_a
                fx.odds_book = book_a or book_b
                fx.odds_fetched_at = datetime.utcnow()
                matched += 1
        s.commit()

    log.info("attached odds to %d fixtures", matched)
    return matched
