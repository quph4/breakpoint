"""Build features for an upcoming match using current DB state.

For training we walk history; for live we snapshot the current per-player
state (latest Elo, last-N form, days since last match, H2H against the
specific opponent). Returns a single-row pandas DataFrame matching the
schema expected by `models.baseline.predict_proba`.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from sqlalchemy import and_, desc, or_, select

from ..db import Match, Rating, init_db, session


def _latest_rating(s, player_id: int) -> Rating | None:
    return s.scalar(
        select(Rating).where(Rating.player_id == player_id).order_by(desc(Rating.date)).limit(1)
    )


def _last_n_form(s, player_id: int, surface: str | None, n: int = 10) -> float:
    q = select(Match).where(
        or_(Match.winner_id == player_id, Match.loser_id == player_id)
    ).order_by(desc(Match.date)).limit(n * 2)  # filter surface client-side
    matches = list(s.scalars(q))
    if surface:
        matches = [m for m in matches if m.surface == surface]
    matches = matches[:n]
    if not matches:
        return 0.5
    wins = sum(1 for m in matches if m.winner_id == player_id)
    return wins / len(matches)


def _days_since_last(s, player_id: int, ref_date: date) -> int:
    last = s.scalar(
        select(Match.date).where(
            or_(Match.winner_id == player_id, Match.loser_id == player_id)
        ).order_by(desc(Match.date)).limit(1)
    )
    return (ref_date - last).days if last else 60


def _h2h_diff(s, a: int, b: int) -> float:
    matches = list(s.scalars(
        select(Match).where(
            or_(
                and_(Match.winner_id == a, Match.loser_id == b),
                and_(Match.winner_id == b, Match.loser_id == a),
            )
        )
    ))
    if not matches:
        return 0.0
    a_wins = sum(1 for m in matches if m.winner_id == a)
    return (2 * a_wins - len(matches)) / len(matches)


def build_live_row(player_a_id: int, player_b_id: int, surface: str,
                   match_date: date | None = None, engine=None) -> pd.DataFrame | None:
    engine = engine or init_db()
    match_date = match_date or date.today()

    with session(engine) as s:
        ra = _latest_rating(s, player_a_id)
        rb = _latest_rating(s, player_b_id)
        if not ra or not rb:
            return None

        elo_attr = {"Hard": "elo_hard", "Clay": "elo_clay", "Grass": "elo_grass"}.get(surface, "elo_overall")
        elo_a_surf = getattr(ra, elo_attr) or ra.elo_overall
        elo_b_surf = getattr(rb, elo_attr) or rb.elo_overall

        row = {
            "elo_diff": ra.elo_overall - rb.elo_overall,
            "elo_surf_diff": elo_a_surf - elo_b_surf,
            "form10_diff": _last_n_form(s, player_a_id, None) - _last_n_form(s, player_b_id, None),
            "surf_form_diff": _last_n_form(s, player_a_id, surface) - _last_n_form(s, player_b_id, surface),
            "rest_diff": _days_since_last(s, player_a_id, match_date) - _days_since_last(s, player_b_id, match_date),
            "h2h_diff": _h2h_diff(s, player_a_id, player_b_id),
            "matches_played_diff": (ra.matches_played or 0) - (rb.matches_played or 0),
        }
    return pd.DataFrame([row])
