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
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ..db import Match, Player, Ranking, get_engine, init_db, session

log = logging.getLogger(__name__)

ATP_BASE = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"
WTA_BASE = "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master"

PLAYER_FILES = {
    "atp": f"{ATP_BASE}/atp_players.csv",
    "wta": f"{WTA_BASE}/wta_players.csv",
}

# Sackmann's ATP and WTA player IDs both start from low integers and overlap.
# We namespace WTA by adding this offset to every WTA player_id everywhere
# (player table + match winner/loser columns).
WTA_ID_OFFSET = 10_000_000


def _offset_for(tour: str) -> int:
    return WTA_ID_OFFSET if tour == "wta" else 0

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
    df["id"] = df["id"].astype(int) + _offset_for(tour)
    df = df.drop_duplicates(subset=["id"], keep="first")

    with session(engine) as s:
        existing = {p for (p,) in s.execute(select(Player.id))}
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
    offset = _offset_for(tour)
    df["winner_id"] = df["winner_id"].astype(int) + offset
    df["loser_id"] = df["loser_id"].astype(int) + offset
    df["tour"] = tour

    keep = ["tour", "date"] + [c for c in _MATCH_COLS if c in df.columns and c != "tourney_date"]
    df = df[keep]

    int_cols = [c for c in df.columns if c.startswith(("w_", "l_")) or c in ("best_of",)]
    for c in int_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # Cast Int64 NaN → None so SQLite inserts NULL.
    df = df.where(pd.notnull(df), None)

    # In-batch dedup so we don't hit the unique constraint mid-flush
    # (Sackmann CSVs occasionally include the same match twice).
    seen = set()
    deduped = []
    for r in df.to_dict("records"):
        key = (r["tour"], r["date"], r["winner_id"], r["loser_id"], r.get("tourney_name"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    if not deduped:
        log.info("[%s %d] inserted 0 matches", tour, year)
        return 0

    # INSERT OR IGNORE handles cross-year duplicates (Brisbane etc.) without a pre-query.
    stmt = sqlite_insert(Match).values(deduped).prefix_with("OR IGNORE")
    with session(engine) as s:
        result = s.execute(stmt)
        s.commit()
    inserted = result.rowcount or 0
    log.info("[%s %d] inserted %d matches", tour, year, inserted)
    return inserted


RANKING_FILES = {
    "atp": [
        f"{ATP_BASE}/atp_rankings_00s.csv",
        f"{ATP_BASE}/atp_rankings_10s.csv",
        f"{ATP_BASE}/atp_rankings_20s.csv",
        f"{ATP_BASE}/atp_rankings_current.csv",
    ],
    "wta": [
        f"{WTA_BASE}/wta_rankings_00s.csv",
        f"{WTA_BASE}/wta_rankings_10s.csv",
        f"{WTA_BASE}/wta_rankings_20s.csv",
        f"{WTA_BASE}/wta_rankings_current.csv",
    ],
}


def ingest_rankings(tour: str, since: date | None = None, engine=None) -> int:
    engine = engine or init_db()
    offset = _offset_for(tour)
    since = since or date(2010, 1, 1)
    total = 0
    for url in RANKING_FILES[tour]:
        try:
            df = _fetch_csv(url)
        except requests.HTTPError as e:
            log.warning("rankings file missing: %s (%s)", url, e)
            continue
        df["date"] = df["ranking_date"].apply(_parse_date)
        df = df.dropna(subset=["date", "player", "rank"])
        df = df[df["date"] >= since]
        df["player_id"] = df["player"].astype(int) + offset
        df["tour"] = tour
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce").astype("Int64")
        df["points"] = pd.to_numeric(df.get("points"), errors="coerce").astype("Int64") if "points" in df.columns else None
        records = df[["player_id", "tour", "date", "rank", "points"]].to_dict("records")
        records = [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in records]
        if not records:
            continue
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert
        # SQLite caps placeholders at 32766; chunk to stay well under (5 cols × 5000 = 25000).
        CHUNK = 5000
        with session(engine) as s:
            for i in range(0, len(records), CHUNK):
                stmt = sqlite_insert(Ranking).values(records[i:i + CHUNK]).prefix_with("OR IGNORE")
                result = s.execute(stmt)
                total += result.rowcount or 0
            s.commit()
    log.info("[%s rankings] inserted %d rows", tour, total)
    return total


def ingest_all(tours: Iterable[str] = ("atp", "wta"), start_year: int = 2000, end_year: int | None = None) -> None:
    end_year = end_year or date.today().year
    engine = init_db()
    for tour in tours:
        ingest_players(tour, engine)
        for year in range(start_year, end_year + 1):
            ingest_matches_year(tour, year, engine)
        ingest_rankings(tour, since=date(start_year, 1, 1), engine=engine)
