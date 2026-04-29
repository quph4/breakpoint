"""Surface-aware Elo. One overall rating + one per surface (Hard / Clay / Grass).

K-factor follows the standard tennis-Elo decay used in most public implementations
(originally Sackmann / Riles): K = 250 / (matches_played + 5)^0.4.
Initial rating 1500. Surface ratings only update for matches on that surface.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date

import pandas as pd
from sqlalchemy import select

from ..db import Match, Rating, init_db, session

INITIAL = 1500.0
K_NUM = 250.0
K_OFFSET = 5.0
K_EXP = 0.4

SURFACES = ("Hard", "Clay", "Grass")


def _k(n_played: int) -> float:
    return K_NUM / pow(n_played + K_OFFSET, K_EXP)


def _expected(ra: float, rb: float) -> float:
    return 1.0 / (1.0 + 10 ** ((rb - ra) / 400.0))


@dataclass
class PlayerElo:
    overall: float = INITIAL
    hard: float = INITIAL
    clay: float = INITIAL
    grass: float = INITIAL
    n_overall: int = 0
    n_hard: int = 0
    n_clay: int = 0
    n_grass: int = 0


def compute_all(engine=None, tour: str | None = None) -> pd.DataFrame:
    """Walk every match in chronological order, update ratings, return per-player current snapshot.

    Also writes per-(player, date) snapshots to the ratings table — full rebuild each run.
    """
    engine = engine or init_db()
    with session(engine) as s:
        q = select(Match).order_by(Match.date, Match.id)
        if tour:
            q = q.where(Match.tour == tour)
        matches = list(s.scalars(q))

        # Wipe existing ratings — we recompute deterministically.
        s.query(Rating).delete()
        s.commit()

        ratings: dict[int, PlayerElo] = defaultdict(PlayerElo)
        snapshots: list[Rating] = []

        for m in matches:
            w, l = m.winner_id, m.loser_id
            if w is None or l is None:
                continue
            rw, rl = ratings[w], ratings[l]
            surface = m.surface if m.surface in SURFACES else None

            # Overall update
            kw = _k(rw.n_overall); kl = _k(rl.n_overall)
            ew = _expected(rw.overall, rl.overall)
            rw.overall += kw * (1 - ew)
            rl.overall -= kl * (1 - ew)
            rw.n_overall += 1; rl.n_overall += 1

            # Surface update
            if surface == "Hard":
                kw_s = _k(rw.n_hard); kl_s = _k(rl.n_hard)
                ew_s = _expected(rw.hard, rl.hard)
                rw.hard += kw_s * (1 - ew_s); rl.hard -= kl_s * (1 - ew_s)
                rw.n_hard += 1; rl.n_hard += 1
            elif surface == "Clay":
                kw_s = _k(rw.n_clay); kl_s = _k(rl.n_clay)
                ew_s = _expected(rw.clay, rl.clay)
                rw.clay += kw_s * (1 - ew_s); rl.clay -= kl_s * (1 - ew_s)
                rw.n_clay += 1; rl.n_clay += 1
            elif surface == "Grass":
                kw_s = _k(rw.n_grass); kl_s = _k(rl.n_grass)
                ew_s = _expected(rw.grass, rl.grass)
                rw.grass += kw_s * (1 - ew_s); rl.grass -= kl_s * (1 - ew_s)
                rw.n_grass += 1; rl.n_grass += 1

            for pid, r in ((w, rw), (l, rl)):
                snapshots.append(Rating(
                    player_id=pid, date=m.date,
                    elo_overall=r.overall, elo_hard=r.hard, elo_clay=r.clay, elo_grass=r.grass,
                    matches_played=r.n_overall,
                ))

        # Bulk write
        s.bulk_save_objects(snapshots)
        s.commit()

    return pd.DataFrame([
        {"player_id": pid, "elo_overall": r.overall, "elo_hard": r.hard,
         "elo_clay": r.clay, "elo_grass": r.grass, "matches_played": r.n_overall}
        for pid, r in ratings.items()
    ])


def win_probability(elo_a: float, elo_b: float) -> float:
    """Pure Elo win probability for player A — used as the model baseline."""
    return _expected(elo_a, elo_b)
