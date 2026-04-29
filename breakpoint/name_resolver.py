"""Map a free-form player name string to a Player.id from the DB.

Tennis name encodings vary wildly across sources:
  Sackmann:     "Carlos Alcaraz"
  Sofascore:    "Alcaraz C." or "Alcaraz Carlos"
  Odds API:     "Carlos Alcaraz Garfia"
  Tennis-data:  "Alcaraz C."

Strategy: build an in-memory index of `Player` rows once per process,
match by exact name first, then `rapidfuzz.token_set_ratio` with a high
threshold. Cache resolved names to avoid recomputing.
"""
from __future__ import annotations

from functools import lru_cache

from rapidfuzz import fuzz, process
from sqlalchemy import select

from .db import Player, init_db, session


@lru_cache(maxsize=1)
def _player_index(tour: str | None = None) -> list[tuple[int, str, str]]:
    engine = init_db()
    with session(engine) as s:
        q = select(Player.id, Player.name, Player.tour)
        if tour:
            q = q.where(Player.tour == tour)
        return list(s.execute(q).all())


def _normalize(name: str) -> str:
    return " ".join(name.lower().replace(".", " ").replace(",", " ").split())


@lru_cache(maxsize=20000)
def resolve(name: str, tour: str | None = None, min_score: int = 88) -> int | None:
    """Best-effort player_id for a display name. Returns None on no confident match.

    Cached because tennis-data.co.uk and Sofascore both hit the same names
    repeatedly across files; rapidfuzz against ~5000 candidates is cheap but
    not free, and resolution-by-name is now on the hot path of every ingest.
    """
    if not name:
        return None
    candidates = _player_index(tour)
    if not candidates:
        return None

    target = _normalize(name)

    # Fast path: exact normalized match
    for pid, pname, _ in candidates:
        if _normalize(pname) == target:
            return pid

    # "Lastname F." pattern: rebuild as "F Lastname" if needed
    parts = target.split()
    if len(parts) == 2 and len(parts[1]) <= 2:
        target = f"{parts[1]} {parts[0]}"

    choices = {pid: _normalize(pname) for pid, pname, _ in candidates}
    best = process.extractOne(target, choices, scorer=fuzz.token_set_ratio)
    if best and best[1] >= min_score:
        return best[2]  # the key (pid)
    return None


def reset_cache() -> None:
    _player_index.cache_clear()
    resolve.cache_clear()
