"""Map a free-form player name string to a Player.id from the DB.

Tennis name encodings vary wildly across sources:
  Sackmann:        "Carlos Alcaraz"
  Sofascore:       "Alcaraz C." or "Alcaraz Carlos"
  Odds API:        "Carlos Alcaraz Garfia"
  tennis-data:     "Alcaraz C."  (also: "Bautista Agut R.", "Auger-Aliassime F.",
                                  "Davidovich Fokina A.", accented chars stripped)

Strategy in priority order:
  1. Exact match on normalized (lowercased, accent-stripped) name.
  2. Detect "Lastname F." pattern (final token is one letter), look up against
     a (lastname, initial) index. Handles multi-word last names natively.
  3. Fuzzy fallback via rapidfuzz.token_set_ratio.

Each layer is cached. The lru_cache on `resolve` itself is critical for the
hot ingest paths where the same names appear thousands of times.
"""
from __future__ import annotations

import unicodedata
from functools import lru_cache

from rapidfuzz import fuzz, process
from sqlalchemy import select

from .db import Player, init_db, session


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _normalize(name: str) -> str:
    s = _strip_accents(name).lower()
    s = s.replace(".", " ").replace(",", " ").replace("'", " ")
    return " ".join(s.split())


@lru_cache(maxsize=4)
def _player_index(tour: str | None = None) -> list[tuple[int, str, str]]:
    engine = init_db()
    with session(engine) as s:
        q = select(Player.id, Player.name, Player.tour)
        if tour:
            q = q.where(Player.tour == tour)
        return list(s.execute(q).all())


@lru_cache(maxsize=4)
def _lastname_initial_index(tour: str | None = None) -> dict[tuple[str, str], list[int]]:
    """(normalized_lastname, first_initial_upper) -> list of player_ids.

    Sackmann names are "Firstname Lastname" or "Firstname Lastname1 Lastname2".
    We treat the first whitespace token as the first name and everything after
    as the last name; that handles multi-word last names like "Auger-Aliassime",
    "Bautista Agut", "Davidovich Fokina", "Van De Zandschulp" cleanly.
    """
    idx: dict[tuple[str, str], list[int]] = {}
    for pid, name, _ in _player_index(tour):
        norm = _normalize(name)
        tokens = norm.split()
        if len(tokens) < 2 or not tokens[0]:
            continue
        first, last = tokens[0], " ".join(tokens[1:])
        idx.setdefault((last, first[0].upper()), []).append(pid)
    return idx


def _try_lastname_initial(name: str, tour: str | None) -> int | None:
    """Match tennis-data-style "Lastname F." into the (lastname, initial) index.

    Trailing single-letter token (with optional period) is the initial; the
    rest is the last name. Returns None when ambiguous (multiple matches) or
    no match — caller falls back to fuzzy.
    """
    s = _strip_accents(name).strip().rstrip(".").strip()
    parts = s.split()
    if len(parts) < 2:
        return None
    last_token = parts[-1].rstrip(".")
    if len(last_token) != 1 or not last_token.isalpha():
        return None
    last_name = " ".join(parts[:-1]).lower()
    initial = last_token.upper()
    candidates = _lastname_initial_index(tour).get((last_name, initial))
    if candidates and len(candidates) == 1:
        return candidates[0]
    return None


@lru_cache(maxsize=20000)
def resolve(name: str, tour: str | None = None, min_score: int = 88) -> int | None:
    """Best-effort player_id for a display name. Returns None on no confident match."""
    if not name:
        return None
    candidates = _player_index(tour)
    if not candidates:
        return None

    target = _normalize(name)

    # 1. Exact normalized match
    for pid, pname, _ in candidates:
        if _normalize(pname) == target:
            return pid

    # 2. Lastname + first-initial fast path (handles tennis-data encoding cleanly)
    pid = _try_lastname_initial(name, tour)
    if pid is not None:
        return pid

    # 3. Fuzzy fallback. The "Lastname F." → "F Lastname" rewrite still helps
    # for the rare case where the index lookup was ambiguous.
    parts = target.split()
    if len(parts) >= 2 and len(parts[-1]) == 1:
        target = parts[-1] + " " + " ".join(parts[:-1])

    choices = {pid: _normalize(pname) for pid, pname, _ in candidates}
    best = process.extractOne(target, choices, scorer=fuzz.token_set_ratio)
    if best and best[1] >= min_score:
        return best[2]
    return None


def reset_cache() -> None:
    _player_index.cache_clear()
    _lastname_initial_index.cache_clear()
    resolve.cache_clear()
