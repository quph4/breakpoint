"""Pull Jeff Sackmann's ATP/WTA match CSVs from GitHub and load into the DB.

Repos:
  https://github.com/JeffSackmann/tennis_atp
  https://github.com/JeffSackmann/tennis_wta

We fetch raw CSVs directly via raw.githubusercontent.com — no clone needed.
"""
from __future__ import annotations

import io
import logging
from datetime import date, datetime
from typing import Iterable

import pandas as pd
import requests
from sqlalchemy import select

from ..db import Match, Player, get_engine, init_db, session

log = logging.getLogger(__name__)

ATP_BASE = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"
WTA_BASE = "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master"

PLAYER_FILES = {
    "atp": f"{ATP_BASE}/atp_players.csv",
    "wta": f"{WTA_BASE}/wta_players.csv",
}

def matches_url(tour: str, year: int) -> str:
    base = ATP_BASE if tour == "atp" else WTA_BASE
    return f"{base}/{tour}_matches_{year}.csv"


def _fetch_csv(url: str) -> pd.DataFrame:
    log.info("fetch %s", url)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content), low_memory=False, encoding_errors="replace")


def _parse_date(v) -> date | None:
    if pd.isna(v):
        return None
    s = str(int(v)) if isinstance(v, (int, float)) else str(v)
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return None


def ingest_players(tour: str, engine=None) -> int:
    engine = engine or init_db()
    df = _fetch_csv(PLAYER_FILES[tour])
    # Sackmann columns: player_id, name_first, name_last, hand, dob, ioc, height, wikidata_id
    df = df.rename(columns={"ioc": "country", "height": "height_cm"})
    df["name"] = (df["name_first"].fillna("") + " " + df["name_last"].fillna("")).str.strip()
    df["dob"] = df["dob"].apply(_parse_date)
    df["tour"] = tour

    cols = ["player_id", "name", "tour", "country", "hand", "height_cm", "dob"]
    df = df[cols].rename(columns={"player_id": "id"})
    df = df.dropna(subset=["id", "name"])
    df["id"] = df["id"].astype(int)

    with session(engine) as s:
        existing = {p for (p,) in s.execute(select(Player.id).where(Player.tour == tour))}
        new_rows = df[~df["id"].isin(existing)].to_dict("records")
        for row in new_rows:
            s.add(Player(**row))
        s.commit()
    log.info("[%s] inserted %d players", tour, len(new_rows))
    return len(new_rows)


_MATCH_COLS = [
    "tourney_name", "tourney_level", "tourney_date", "surface", "round", "best_of",
    "winner_id", "loser_id", "score", "minutes",
    "w_ace", "w_df", "w_svpt", "w_1stIn", "w_1stWon", "w_2ndWon",
    "w_SvGms", "w_bpSaved", "w_bpFaced",
    "l_ace", "l_df", "l_svpt", "l_1stIn", "l_1stWon", "l_2ndWon",
    "l_SvGms", "l_bpSaved", "l_bpFaced",
    "winner_rank", "loser_rank",
]


def ingest_matches_year(tour: str, year: int, engine=None) -> int:
    engine = engine or init_db()
    try:
        df = _fetch_csv(matches_url(tour, year))
    except requests.HTTPError as e:
        log.warning("no matches file for %s %d: %s", tour, year, e)
        return 0

    df["date"] = df["tourney_date"].apply(_parse_date)
    df = df.dropna(subset=["date", "winner_id", "loser_id"])
    df["winner_id"] = df["winner_id"].astype(int)
    df["loser_id"] = df["loser_id"].astype(int)
    df["tour"] = tour

    keep = ["tour", "date"] + [c for c in _MATCH_COLS if c in df.columns and c != "tourney_date"]
    df = df[keep]

    int_cols = [c for c in df.columns if c.startswith(("w_", "l_")) or c in ("best_of",)]
    for c in int_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    with session(engine) as s:
        # naive bulk insert; UniqueConstraint dedupes via integrity error, so guard with a query.
        existing_keys = set()
        rows = s.execute(
            select(Match.tour, Match.date, Match.winner_id, Match.loser_id, Match.tourney_name)
            .where(Match.tour == tour, Match.date >= date(year, 1, 1), Match.date <= date(year, 12, 31))
        ).all()
        for row in rows:
            existing_keys.add(tuple(row))

        records = df.to_dict("records")
        seen = set(existing_keys)
        new = []
        for r in records:
            key = (r["tour"], r["date"], r["winner_id"], r["loser_id"], r.get("tourney_name"))
            if key in seen:
                continue
            seen.add(key)
            new.append(r)
        for r in new:
            s.add(Match(**{k: v for k, v in r.items() if v is not None or k in ("tourney_name",)}))
        s.commit()
    log.info("[%s %d] inserted %d matches", tour, year, len(new))
    return len(new)


def ingest_all(tours: Iterable[str] = ("atp", "wta"), start_year: int = 2000, end_year: int | None = None) -> None:
    end_year = end_year or date.today().year
    engine = init_db()
    for tour in tours:
        ingest_players(tour, engine)
        for year in range(start_year, end_year + 1):
            ingest_matches_year(tour, year, engine)
