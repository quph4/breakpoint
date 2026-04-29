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
from sqlalchemy import select
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


def ingest_year(tour: str, year: int, engine=None) -> int:
    engine = engine or init_db()
    url = (ATP_URL_TMPL if tour == "atp" else WTA_URL_TMPL).format(year=year)
    df = _fetch_xlsx(url)
    if df is None or df.empty:
        return 0
    df = _normalize(df, tour)

    inserted = 0
    skipped_unresolved = 0
    skipped_no_match = 0

    with session(engine) as s:
        # Cache existing Odds match_ids for fast skip
        existing = set(s.execute(select(Odds.match_id)).scalars())

        for row in df.itertuples(index=False):
            w_id = resolve(row.Winner, tour)
            l_id = resolve(row.Loser, tour)
            if not w_id or not l_id:
                skipped_unresolved += 1
                continue

            # Find a Match within ±1 day (tennis-data.co.uk dates the start of the day,
            # Sackmann uses tournament start; both are usually the same but slips happen).
            match = s.scalar(
                select(Match).where(
                    Match.tour == tour,
                    Match.winner_id == w_id,
                    Match.loser_id == l_id,
                    Match.date >= row.date - timedelta(days=1),
                    Match.date <= row.date + timedelta(days=1),
                )
            )
            if not match or match.id in existing:
                skipped_no_match += 1
                continue

            payload = {
                "match_id": match.id,
                "b365_w": getattr(row, "B365W", None),
                "b365_l": getattr(row, "B365L", None),
                "ps_w": getattr(row, "PSW", None),
                "ps_l": getattr(row, "PSL", None),
                "avg_w": getattr(row, "AvgW", None),
                "avg_l": getattr(row, "AvgL", None),
            }
            # Coerce NaN → None
            payload = {k: (None if pd.isna(v) else v) for k, v in payload.items()}
            stmt = sqlite_insert(Odds).values(payload).prefix_with("OR IGNORE")
            s.execute(stmt)
            inserted += 1
            existing.add(match.id)
        s.commit()

    log.info("[%s %d] inserted %d odds rows (%d unresolved names, %d no-match)",
             tour, year, inserted, skipped_unresolved, skipped_no_match)
    return inserted


def ingest_all(tours: Iterable[str] = ("atp", "wta"),
               start_year: int = 2015, end_year: int | None = None) -> int:
    end_year = end_year or date.today().year
    total = 0
    for tour in tours:
        for year in range(start_year, end_year + 1):
            total += ingest_year(tour, year)
    return total
