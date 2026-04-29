"""Build features for an upcoming match using current DB state.

Mirrors the schema of `build_training_frame` but pulls each per-player
metric directly from the DB instead of running a full chronological pass.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from sqlalchemy import and_, desc, or_, select

from ..db import Match, Player, Ranking, Rating, init_db, session


SERVE_WINDOW = 20


def _latest_rating(s, player_id: int):
    return s.scalar(
        select(Rating).where(Rating.player_id == player_id).order_by(desc(Rating.date)).limit(1)
    )


def _last_n_form(s, player_id: int, surface: str | None, n: int = 10) -> float:
    q = select(Match).where(
        or_(Match.winner_id == player_id, Match.loser_id == player_id)
    ).order_by(desc(Match.date)).limit(n * 3)
    matches = list(s.scalars(q))
    if surface:
        matches = [m for m in matches if m.surface == surface]
    matches = matches[:n]
    if not matches:
        return 0.5
    return sum(1 for m in matches if m.winner_id == player_id) / len(matches)


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


def _serve_return_pcts(s, player_id: int, surface: str | None) -> dict:
    """Pulls last SERVE_WINDOW matches for player and aggregates serve/return %."""
    q = select(Match).where(
        or_(Match.winner_id == player_id, Match.loser_id == player_id)
    ).order_by(desc(Match.date)).limit(SERVE_WINDOW * 3)
    rows = list(s.scalars(q))
    if surface:
        rows = [m for m in rows if m.surface == surface]
    rows = rows[:SERVE_WINDOW]

    sw = st = rw = rt = 0
    bp_saved = bp_faced = bp_converted = bp_chances = 0
    aces = svpt_ace = dfs = svpt_df = 0

    for m in rows:
        if m.winner_id == player_id:
            own_svpt, own_won = m.w_svpt, (m.w_1stWon or 0) + (m.w_2ndWon or 0)
            opp_svpt, opp_won = m.l_svpt, (m.l_1stWon or 0) + (m.l_2ndWon or 0)
            own_ace, own_df = m.w_ace, m.w_df
            own_bp_saved, own_bp_faced = m.w_bpSaved, m.w_bpFaced
            opp_bp_faced, opp_bp_saved = m.l_bpFaced, m.l_bpSaved
        else:
            own_svpt, own_won = m.l_svpt, (m.l_1stWon or 0) + (m.l_2ndWon or 0)
            opp_svpt, opp_won = m.w_svpt, (m.w_1stWon or 0) + (m.w_2ndWon or 0)
            own_ace, own_df = m.l_ace, m.l_df
            own_bp_saved, own_bp_faced = m.l_bpSaved, m.l_bpFaced
            opp_bp_faced, opp_bp_saved = m.w_bpFaced, m.w_bpSaved

        if own_svpt and opp_svpt:
            sw += own_won; st += own_svpt
            rw += (opp_svpt - opp_won); rt += opp_svpt
            if own_ace is not None: aces += own_ace; svpt_ace += own_svpt
            if own_df is not None: dfs += own_df; svpt_df += own_svpt
        if own_bp_faced:
            bp_saved += own_bp_saved or 0; bp_faced += own_bp_faced
        if opp_bp_faced:
            bp_converted += (opp_bp_faced or 0) - (opp_bp_saved or 0)
            bp_chances += opp_bp_faced

    return {
        "serve_pts_won": (sw / st) if st else None,
        "return_pts_won": (rw / rt) if rt else None,
        "bp_save_pct": (bp_saved / bp_faced) if bp_faced else None,
        "bp_convert_pct": (bp_converted / bp_chances) if bp_chances else None,
        "ace_rate": (aces / svpt_ace) if svpt_ace else None,
        "df_rate": (dfs / svpt_df) if svpt_df else None,
    }


def _rank_trajectory(s, player_id: int, ref_date: date) -> int | None:
    """Rank improvement vs ~12 weeks ago (positive = improving)."""
    now_r = s.scalar(
        select(Ranking.rank).where(Ranking.player_id == player_id, Ranking.date <= ref_date)
        .order_by(desc(Ranking.date)).limit(1)
    )
    old_r = s.scalar(
        select(Ranking.rank).where(
            Ranking.player_id == player_id, Ranking.date <= ref_date - timedelta(days=84)
        ).order_by(desc(Ranking.date)).limit(1)
    )
    if now_r is None or old_r is None:
        return None
    return old_r - now_r


def _diff(a, b):
    if a is None or b is None:
        return None
    return a - b


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

        sr_a = _serve_return_pcts(s, player_a_id, None)
        sr_b = _serve_return_pcts(s, player_b_id, None)
        sr_a_surf = _serve_return_pcts(s, player_a_id, surface)
        sr_b_surf = _serve_return_pcts(s, player_b_id, surface)

        pa = s.get(Player, player_a_id)
        pb = s.get(Player, player_b_id)
        height_diff = (pa.height_cm - pb.height_cm) if (pa and pb and pa.height_cm and pb.height_cm) else None

        traj_a = _rank_trajectory(s, player_a_id, match_date)
        traj_b = _rank_trajectory(s, player_b_id, match_date)

        row = {
            "elo_diff": ra.elo_overall - rb.elo_overall,
            "elo_surf_diff": elo_a_surf - elo_b_surf,
            "form10_diff": _last_n_form(s, player_a_id, None) - _last_n_form(s, player_b_id, None),
            "surf_form_diff": _last_n_form(s, player_a_id, surface) - _last_n_form(s, player_b_id, surface),
            "rest_diff": _days_since_last(s, player_a_id, match_date) - _days_since_last(s, player_b_id, match_date),
            "h2h_diff": _h2h_diff(s, player_a_id, player_b_id),
            "matches_played_diff": (ra.matches_played or 0) - (rb.matches_played or 0),
            "serve_pts_won_diff": _diff(sr_a["serve_pts_won"], sr_b["serve_pts_won"]),
            "return_pts_won_diff": _diff(sr_a["return_pts_won"], sr_b["return_pts_won"]),
            "surf_serve_diff": _diff(sr_a_surf["serve_pts_won"], sr_b_surf["serve_pts_won"]),
            "surf_return_diff": _diff(sr_a_surf["return_pts_won"], sr_b_surf["return_pts_won"]),
            "bp_save_pct_diff": _diff(sr_a["bp_save_pct"], sr_b["bp_save_pct"]),
            "bp_convert_pct_diff": _diff(sr_a["bp_convert_pct"], sr_b["bp_convert_pct"]),
            "ace_rate_diff": _diff(sr_a["ace_rate"], sr_b["ace_rate"]),
            "df_rate_diff": _diff(sr_a["df_rate"], sr_b["df_rate"]),
            "height_diff_cm": height_diff,
            "rank_traj_diff": _diff(traj_a, traj_b),
        }
    return pd.DataFrame([row])
