"""Pull historical closing odds from tennis-data.co.uk and join to matches.

URL pattern (verified against the site's data archive):
  ATP: http://www.tennis-data.co.uk/{year}/{year}.xlsx
  WTA: http://www.tennis-data.co.uk/{year}w/{year}.xlsx

Columns we care about:
  Date, Surface, Winner, Loser, WRank, LRank,
  B365W, B365L, PSW, PSL, AvgW, AvgL

Joining strategy:
  1. Pull the year's file, normalize column names.
  2. For each row, resolve Winner and Loser names to player_ids via
     `name_resolver.resolve` (which already handles "Lastname F." → first-last).
  3. Find the matching Match row in our DB by (tour, date ±1 day, winner_id, loser_id).
  4. Upsert into Odds table, keyed by match_id.

Rows that don't resolve are skipped silently (the file has plenty of
qualifying-round players who never made it to Sackmann's main-tour CSVs).
"""
from __future__ import annotations

import io
import logging
from datetime import date, timedelta
from typing import Iterable

import pandas as pd
import requests
from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ..db import Match, Odds, init_db, session
from ..name_resolver import resolve

log = logging.getLogger(__name__)

ATP_URL_TMPL = "http://www.tennis-data.co.uk/{year}/{year}.xlsx"
WTA_URL_TMPL = "http://www.tennis-data.co.uk/{year}w/{year}.xlsx"


def _fetch_xlsx(url: str) -> pd.DataFrame | None:
    log.info("fetch %s", url)
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
    except requests.RequestException as e:
        log.warning("tennis-data.co.uk %s: %s", url, e)
        return None
    return pd.read_excel(io.BytesIO(r.content))


def _normalize(df: pd.DataFrame, tour: str) -> pd.DataFrame:
    cols = {c: c.strip() for c in df.columns}
    df = df.rename(columns=cols)
    df["tour"] = tour
    df["date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    keep = ["tour", "date", "Surface", "Winner", "Loser",
            "B365W", "B365L", "PSW", "PSL", "AvgW", "AvgL"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].dropna(subset=["date", "Winner", "Loser"])
    return df


def _year_already_ingested(s, tour: str, year: int, min_rows: int = 50) -> bool:
    """Heuristic: if we've already imported a meaningful number of odds rows for
    matches in this tour+year, skip the file. We use 50 to allow partial early-
    season data not to count as 'done' (Jan still being imported in late Feb)."""
    n = s.scalar(
        select(func.count(Odds.match_id))
        .join(Match, Match.id == Odds.match_id)
        .where(
            Match.tour == tour,
            Match.date >= date(year, 1, 1),
            Match.date <= date(year, 12, 31),
        )
    ) or 0
    return n >= min_rows


def _build_match_index(s, tour: str, year: int) -> dict[tuple[int, int], list[tuple[date, int]]]:
    """Pre-load all matches for the tour+year (with ±1 day buffer) into a dict
    keyed by (winner_id, loser_id) → [(match_date, match_id)]. This kills the
    per-row SQL roundtrip — for ~3000 rows, that's ~3000 round-trips collapsed
    to a single bulk SELECT plus dict lookups."""
    rows = s.execute(
        select(Match.id, Match.winner_id, Match.loser_id, Match.date).where(
            Match.tour == tour,
            Match.date >= date(year, 1, 1) - timedelta(days=2),
            Match.date <= date(year, 12, 31) + timedelta(days=2),
        )
    ).all()
    idx: dict[tuple[int, int], list[tuple[date, int]]] = {}
    for mid, w, l, d in rows:
        idx.setdefault((w, l), []).append((d, mid))
    return idx


def _find_match(idx: dict, w_id: int, l_id: int, target: date) -> int | None:
    candidates = idx.get((w_id, l_id))
    if not candidates:
        return None
    for d, mid in candidates:
        if abs((d - target).days) <= 1:
            return mid
    return None


def ingest_year(tour: str, year: int, engine=None, refresh: bool = False) -> int:
    engine = engine or init_db()
    current_year = date.today().year

    with session(engine) as s:
        # Always refresh the rolling window (current and previous year) for
        # late-arriving rows; skip everything older if it's already done.
        if not refresh and year < current_year - 1 and _year_already_ingested(s, tour, year):
            log.info("[%s %d] already ingested, skipping", tour, year)
            return 0

    url = (ATP_URL_TMPL if tour == "atp" else WTA_URL_TMPL).format(year=year)
    df = _fetch_xlsx(url)
    if df is None or df.empty:
        return 0
    df = _normalize(df, tour)

    inserted = 0
    skipped_unresolved = 0
    skipped_no_match = 0
    payloads: list[dict] = []

    with session(engine) as s:
        existing = set(s.execute(select(Odds.match_id)).scalars())
        match_idx = _build_match_index(s, tour, year)

        for row in df.itertuples(index=False):
            w_id = resolve(row.Winner, tour)
            l_id = resolve(row.Loser, tour)
            if not w_id or not l_id:
                skipped_unresolved += 1
                continue

            match_id = _find_match(match_idx, w_id, l_id, row.date)
            if match_id is None or match_id in existing:
                skipped_no_match += 1
                continue

            payload = {
                "match_id": match_id,
                "b365_w": getattr(row, "B365W", None),
                "b365_l": getattr(row, "B365L", None),
                "ps_w": getattr(row, "PSW", None),
                "ps_l": getattr(row, "PSL", None),
                "avg_w": getattr(row, "AvgW", None),
                "avg_l": getattr(row, "AvgL", None),
            }
            payload = {k: (None if pd.isna(v) else v) for k, v in payload.items()}
            payloads.append(payload)
            existing.add(match_id)

        if payloads:
            stmt = sqlite_insert(Odds).values(payloads).prefix_with("OR IGNORE")
            result = s.execute(stmt)
            s.commit()
            inserted = result.rowcount or len(payloads)

    log.info("[%s %d] inserted %d odds rows (%d unresolved names, %d no-match)",
             tour, year, inserted, skipped_unresolved, skipped_no_match)
    return inserted


def ingest_all(tours: Iterable[str] = ("atp", "wta"),
               start_year: int = 2015, end_year: int | None = None,
               refresh: bool = False) -> int:
    end_year = end_year or date.today().year
    total = 0
    for tour in tours:
        for year in range(start_year, end_year + 1):
            total += ingest_year(tour, year, refresh=refresh)
    return total
