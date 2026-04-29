"""Construct per-match feature rows for training.

For every historical match we know who won, so we can build a (features → label) row.
The trick: features must reflect what was known *before* the match. We use ratings
snapshotted on or before each match date, never after.

We always randomize whether 'player_a' is the winner or loser so the model doesn't learn
'player_a always wins'. Label is 1 if player_a won, 0 otherwise.
"""
from __future__ import annotations

from datetime import date, timedelta
from collections import defaultdict, deque

import numpy as np
import pandas as pd
from sqlalchemy import select

from ..db import Match, Player, init_db, session


def _last_n_winrate(history: deque, n: int) -> float:
    if not history:
        return 0.5
    sub = list(history)[-n:]
    return sum(sub) / len(sub)


def build_training_frame(engine=None, tour: str | None = None,
                         min_year: int = 2005, surfaces=("Hard", "Clay", "Grass")) -> pd.DataFrame:
    """One pass through history; for each match compute features from running state."""
    from .elo import PlayerElo, _k, _expected

    engine = engine or init_db()
    rows: list[dict] = []

    with session(engine) as s:
        q = select(Match).order_by(Match.date, Match.id)
        if tour:
            q = q.where(Match.tour == tour)
        matches = list(s.scalars(q))

    elo: dict[int, PlayerElo] = defaultdict(PlayerElo)
    form: dict[int, deque] = defaultdict(lambda: deque(maxlen=20))
    surface_form: dict[tuple[int, str], deque] = defaultdict(lambda: deque(maxlen=10))
    last_match: dict[int, date] = {}
    h2h: dict[tuple[int, int], list[int]] = defaultdict(list)  # key=(min_id, max_id), values=1 if min won

    for m in matches:
        if m.surface not in surfaces or m.winner_id is None or m.loser_id is None:
            continue

        w, l = m.winner_id, m.loser_id
        rw, rl = elo[w], elo[l]
        surf = m.surface

        if m.date.year >= min_year:
            # Randomize side assignment for the training row
            flip = (hash((m.id, w, l)) & 1) == 1
            a, b = (l, w) if flip else (w, l)
            label = 0 if flip else 1
            ra, rb = elo[a], elo[b]

            elo_a_surf = getattr(ra, surf.lower())
            elo_b_surf = getattr(rb, surf.lower())

            days_since_a = (m.date - last_match[a]).days if a in last_match else 60
            days_since_b = (m.date - last_match[b]).days if b in last_match else 60

            key = (min(a, b), max(a, b))
            h2h_history = h2h[key]
            if h2h_history:
                a_wins = sum(h2h_history) if a == key[0] else len(h2h_history) - sum(h2h_history)
                h2h_diff = (2 * a_wins - len(h2h_history)) / len(h2h_history)
            else:
                h2h_diff = 0.0

            rows.append({
                "match_id": m.id,
                "date": m.date,
                "tour": m.tour,
                "surface": surf,
                "elo_diff": ra.overall - rb.overall,
                "elo_surf_diff": elo_a_surf - elo_b_surf,
                "form10_diff": _last_n_winrate(form[a], 10) - _last_n_winrate(form[b], 10),
                "surf_form_diff": _last_n_winrate(surface_form[(a, surf)], 10)
                                  - _last_n_winrate(surface_form[(b, surf)], 10),
                "rest_diff": days_since_a - days_since_b,
                "h2h_diff": h2h_diff,
                "matches_played_diff": ra.n_overall - rb.n_overall,
                "label": label,
            })

        # Now update running state with this match's outcome
        kw = _k(rw.n_overall); kl = _k(rl.n_overall)
        ew = _expected(rw.overall, rl.overall)
        rw.overall += kw * (1 - ew); rl.overall -= kl * (1 - ew)
        rw.n_overall += 1; rl.n_overall += 1

        for surf_name, attr, n_attr in (
            ("Hard", "hard", "n_hard"), ("Clay", "clay", "n_clay"), ("Grass", "grass", "n_grass")
        ):
            if surf == surf_name:
                rw_s = getattr(rw, attr); rl_s = getattr(rl, attr)
                kw_s = _k(getattr(rw, n_attr)); kl_s = _k(getattr(rl, n_attr))
                ew_s = _expected(rw_s, rl_s)
                setattr(rw, attr, rw_s + kw_s * (1 - ew_s))
                setattr(rl, attr, rl_s - kl_s * (1 - ew_s))
                setattr(rw, n_attr, getattr(rw, n_attr) + 1)
                setattr(rl, n_attr, getattr(rl, n_attr) + 1)

        form[w].append(1); form[l].append(0)
        surface_form[(w, surf)].append(1); surface_form[(l, surf)].append(0)
        last_match[w] = m.date; last_match[l] = m.date
        key = (min(w, l), max(w, l))
        h2h[key].append(1 if w == key[0] else 0)

    return pd.DataFrame(rows)
