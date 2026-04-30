"""The Odds API client. Single source of truth for both fixtures and odds.

The /odds endpoint returns matchups (home_team, away_team, commence_time)
alongside bookmaker prices, so we use it for both. Sofascore would have been
nice for richer surface metadata, but GitHub Actions runner IPs get 403'd.

Free tier: 500 requests/month. We respect that:
  - One /sports call to discover active tennis keys.
  - One /odds call per active key (typically 2-4 active at once).
  - 30-min disk cache.
  - No-op if BREAKPOINT_ODDS_API_KEY missing.

Surface is derived from sport_key heuristics (madrid_open → Clay etc.)
because the API doesn't expose it directly.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ..db import Fixture, init_db, session
from ..name_resolver import resolve

log = logging.getLogger(__name__)

API_BASE = "https://api.the-odds-api.com/v4"
CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "odds_cache"
CACHE_TTL_MIN = 30

# sport_key fragment → (surface, tour_hint)
# Order matters: more specific patterns first.
_SURFACE_HINTS: list[tuple[str, str]] = [
    ("french_open", "Clay"),
    ("roland_garros", "Clay"),
    ("madrid_open", "Clay"),
    ("italian_open", "Clay"),
    ("monte_carlo", "Clay"),
    ("rome", "Clay"),
    ("hamburg", "Clay"),
    ("barcelona", "Clay"),
    ("wimbledon", "Grass"),
    ("queens", "Grass"),
    ("halle", "Grass"),
    ("eastbourne", "Grass"),
    ("us_open", "Hard"),
    ("australian_open", "Hard"),
    ("aus_open", "Hard"),
    ("indian_wells", "Hard"),
    ("miami_open", "Hard"),
    ("cincinnati", "Hard"),
    ("canadian_open", "Hard"),
]


def _surface_for(sport_key: str, title: str = "") -> str | None:
    needle = (sport_key + " " + title).lower()
    for frag, surf in _SURFACE_HINTS:
        if frag in needle:
            return surf
    if "clay" in needle:
        return "Clay"
    if "grass" in needle:
        return "Grass"
    if "hard" in needle or "indoor" in needle:
        return "Hard"
    return None


def _tour_for(sport_key: str) -> str:
    return "wta" if "wta" in sport_key.lower() else "atp"


def _api_key() -> str | None:
    return os.environ.get("BREAKPOINT_ODDS_API_KEY")


def _cache_get(name: str):
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
    (CACHE_DIR / f"{name}.json").write_text(
        json.dumps({"ts": datetime.utcnow().isoformat(), "data": data})
    )


def _get(path: str, params: dict, cache_key: str | None = None):
    if cache_key:
        cached = _cache_get(cache_key)
        if cached is not None:
            log.info("odds-api cache hit: %s", cache_key)
            return cached

    key = _api_key()
    if not key:
        log.warning("BREAKPOINT_ODDS_API_KEY not set; skipping live odds")
        return None
    try:
        r = requests.get(f"{API_BASE}{path}", params={**params, "apiKey": key}, timeout=20)
    except requests.RequestException as e:
        log.warning("odds-api request failed: %s", e)
        return None

    log.info("odds-api %s status=%s remaining=%s used=%s",
             path, r.status_code,
             r.headers.get("x-requests-remaining"), r.headers.get("x-requests-used"))

    if r.status_code != 200:
        log.warning("odds-api error: %s %s", r.status_code, r.text[:200])
        return None

    data = r.json()
    if cache_key:
        _cache_put(cache_key, data)
    return data


def active_tennis_sports() -> list[dict]:
    sports = _get("/sports", {"all": "false"}, cache_key="sports")
    if not sports:
        return []
    return [s for s in sports if s.get("group", "").lower() == "tennis" and s.get("active")]


def fetch_scores_for_sport(sport_key: str, days_from: int = 3) -> list[dict]:
    """Completed events with scores from the last `days_from` days.

    Free tier: this endpoint costs 2 requests per call (more than /odds).
    Cache for 30 min like everything else. Used by the settle fallback when
    Sackmann hasn't published a result yet.
    """
    return _get(f"/sports/{sport_key}/scores", {
        "daysFrom": str(days_from),
        "dateFormat": "iso",
    }, cache_key=f"scores_{sport_key}_{days_from}d") or []


def _best_h2h_price(event: dict, player_name: str) -> tuple[float | None, str | None]:
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


def sync_fixtures_and_odds(engine=None) -> dict:
    """Pull active tennis events + prices from The Odds API and upsert as Fixtures."""
    engine = engine or init_db()
    if not _api_key():
        log.warning("no API key — skipping")
        return {"events_seen": 0, "fixtures_upserted": 0, "priced": 0, "unresolved": 0}

    sports = active_tennis_sports()
    log.info("active tennis sports: %s", [s["key"] for s in sports])

    events_seen = priced = unresolved = 0
    rows: list[dict] = []
    price_updates: list[dict] = []

    for sport in sports:
        sport_key = sport["key"]
        title = sport.get("title", "")
        surface = _surface_for(sport_key, title)
        tour = _tour_for(sport_key)

        for event in _get(f"/sports/{sport_key}/odds", {
            "regions": "eu",
            "markets": "h2h",
            "oddsFormat": "decimal",
            "dateFormat": "iso",
        }, cache_key=f"odds_{sport_key}") or []:
            events_seen += 1
            home, away = event.get("home_team"), event.get("away_team")
            if not home or not away:
                continue

            commence = event.get("commence_time")
            start_dt = datetime.fromisoformat(commence.replace("Z", "+00:00")) if commence else None
            start_naive = start_dt.replace(tzinfo=None) if start_dt else None
            match_date = start_dt.date() if start_dt else None

            a_id = resolve(home, tour)
            b_id = resolve(away, tour)
            if not a_id or not b_id:
                unresolved += 1
                log.info("unresolved: %s vs %s (sport=%s)", home, away, sport_key)

            price_a, book_a = _best_h2h_price(event, home)
            price_b, book_b = _best_h2h_price(event, away)
            if price_a and price_b:
                priced += 1

            rows.append({
                "source": "odds_api",
                "source_id": event["id"],
                "tour": tour,
                "date": match_date,
                "start_ts": start_naive,
                "tourney_name": title,
                "round": None,
                "surface": surface,
                "indoor": 0,
                "player_a_name": home,
                "player_b_name": away,
                "player_a_id": a_id,
                "player_b_id": b_id,
                "odds_a": price_a,
                "odds_b": price_b,
                "odds_book": book_a or book_b,
                "odds_fetched_at": datetime.utcnow() if (price_a and price_b) else None,
                "status": "scheduled",
            })

    if not rows:
        log.info("no events returned")
        return {"events_seen": 0, "fixtures_upserted": 0, "priced": 0, "unresolved": 0}

    # Upsert: insert new, update prices on existing ones (status not yet finalized).
    inserted = 0
    with session(engine) as s:
        # Insert-or-ignore for new rows
        ins = sqlite_insert(Fixture).values(rows).prefix_with("OR IGNORE")
        result = s.execute(ins)
        inserted = result.rowcount or 0

        # For existing rows, refresh prices + resolved IDs (in case re-resolved)
        for r in rows:
            existing = s.scalar(
                select(Fixture).where(
                    Fixture.source == r["source"], Fixture.source_id == r["source_id"]
                )
            )
            if existing and existing.status == "scheduled":
                if r["odds_a"] and r["odds_b"]:
                    existing.odds_a = r["odds_a"]
                    existing.odds_b = r["odds_b"]
                    existing.odds_book = r["odds_book"]
                    existing.odds_fetched_at = r["odds_fetched_at"]
                if r["player_a_id"] and not existing.player_a_id:
                    existing.player_a_id = r["player_a_id"]
                if r["player_b_id"] and not existing.player_b_id:
                    existing.player_b_id = r["player_b_id"]
                if r["surface"] and not existing.surface:
                    existing.surface = r["surface"]
        s.commit()

    log.info("events=%d upserted=%d priced=%d unresolved=%d",
             events_seen, inserted, priced, unresolved)
    return {
        "events_seen": events_seen,
        "fixtures_upserted": inserted,
        "priced": priced,
        "unresolved": unresolved,
    }


# Back-compat shim for the old workflow call
def attach_odds_to_fixtures(engine=None) -> int:
    return sync_fixtures_and_odds(engine)["priced"]
