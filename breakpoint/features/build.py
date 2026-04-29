"""Construct per-match feature rows for training.

Walks every match in chronological order, maintaining per-player rolling
state for: Elo (overall + per-surface), form, surface form, head-to-head,
serve/return points won, break-point save/conversion, ace and double-fault
rate, and per-surface serve. Pulls static attributes (height) from the
players table; pulls ranking-trajectory deltas from the rankings table.

Each emitted row is computed from state *before* the match's outcome is
applied — we update state only after appending the row, so no leakage.

Each row randomizes which player is "a" so the model can't shortcut on
position. The label is 1 if player_a was the winner, 0 otherwise.
"""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import select

from ..db import Match, Player, Ranking, init_db, session


SERVE_DEQUE_LEN = 20  # rolling window for serve/return aggregates


def _safe_div(num: float, denom: float) -> float | None:
    return (num / denom) if denom else None


def _avg_pct(buf: deque) -> float | None:
    """buf holds (won, total) tuples. Returns weighted percentage or None if empty."""
    if not buf:
        return None
    won = sum(w for w, _ in buf)
    total = sum(t for _, t in buf)
    return _safe_div(won, total)


def _last_n_winrate(history: deque, n: int = 10) -> float:
    if not history:
        return 0.5
    sub = list(history)[-n:]
    return sum(sub) / len(sub)


def _build_player_index(s) -> dict[int, dict]:
    return {
        p.id: {"height": p.height_cm}
        for p in s.scalars(select(Player))
    }


def _build_ranking_index(s) -> dict[int, list[tuple[date, int]]]:
    """player_id → sorted [(date, rank)]. Sorted ascending by date for bisect."""
    idx: dict[int, list[tuple[date, int]]] = defaultdict(list)
    for r in s.scalars(select(Ranking).order_by(Ranking.date)):
        if r.rank is not None:
            idx[r.player_id].append((r.date, int(r.rank)))
    return idx


def _rank_at(rank_history: list[tuple[date, int]], target: date) -> int | None:
    """Most recent rank on or before target. Linear scan from the right is fine
    because we walk matches chronologically and call this with non-decreasing
    target dates per (player, build pass)."""
    if not rank_history:
        return None
    # Binary search: largest i with date <= target
    lo, hi = 0, len(rank_history)
    while lo < hi:
        mid = (lo + hi) // 2
        if rank_history[mid][0] <= target:
            lo = mid + 1
        else:
            hi = mid
    if lo == 0:
        return None
    return rank_history[lo - 1][1]


def build_training_frame(engine=None, tour: str | None = None,
                         min_year: int = 2005, surfaces=("Hard", "Clay", "Grass")) -> pd.DataFrame:
    from .elo import PlayerElo, _k, _expected

    engine = engine or init_db()
    rows: list[dict] = []

    with session(engine) as s:
        q = select(Match).order_by(Match.date, Match.id)
        if tour:
            q = q.where(Match.tour == tour)
        matches = list(s.scalars(q))
        player_index = _build_player_index(s)
        rank_index = _build_ranking_index(s)

    elo: dict[int, PlayerElo] = defaultdict(PlayerElo)
    form: dict[int, deque] = defaultdict(lambda: deque(maxlen=20))
    surf_form: dict[tuple[int, str], deque] = defaultdict(lambda: deque(maxlen=10))
    last_match: dict[int, date] = {}
    h2h: dict[tuple[int, int], list[int]] = defaultdict(list)

    serve_pts: dict[int, deque] = defaultdict(lambda: deque(maxlen=SERVE_DEQUE_LEN))
    return_pts: dict[int, deque] = defaultdict(lambda: deque(maxlen=SERVE_DEQUE_LEN))
    serve_pts_surf: dict[tuple[int, str], deque] = defaultdict(lambda: deque(maxlen=SERVE_DEQUE_LEN))
    return_pts_surf: dict[tuple[int, str], deque] = defaultdict(lambda: deque(maxlen=SERVE_DEQUE_LEN))
    bp_saved: dict[int, deque] = defaultdict(lambda: deque(maxlen=SERVE_DEQUE_LEN))
    bp_converted: dict[int, deque] = defaultdict(lambda: deque(maxlen=SERVE_DEQUE_LEN))
    aces: dict[int, deque] = defaultdict(lambda: deque(maxlen=SERVE_DEQUE_LEN))
    dfs: dict[int, deque] = defaultdict(lambda: deque(maxlen=SERVE_DEQUE_LEN))

    for m in matches:
        if m.surface not in surfaces or m.winner_id is None or m.loser_id is None:
            continue

        w, l = m.winner_id, m.loser_id
        rw, rl = elo[w], elo[l]
        surf = m.surface

        if m.date.year >= min_year:
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

            # Serve/return rolling aggregates
            sp_a = _avg_pct(serve_pts[a])
            sp_b = _avg_pct(serve_pts[b])
            rp_a = _avg_pct(return_pts[a])
            rp_b = _avg_pct(return_pts[b])
            sps_a = _avg_pct(serve_pts_surf[(a, surf)])
            sps_b = _avg_pct(serve_pts_surf[(b, surf)])
            rps_a = _avg_pct(return_pts_surf[(a, surf)])
            rps_b = _avg_pct(return_pts_surf[(b, surf)])
            bps_a = _avg_pct(bp_saved[a])
            bps_b = _avg_pct(bp_saved[b])
            bpc_a = _avg_pct(bp_converted[a])
            bpc_b = _avg_pct(bp_converted[b])
            ace_a = _avg_pct(aces[a])
            ace_b = _avg_pct(aces[b])
            df_a = _avg_pct(dfs[a])
            df_b = _avg_pct(dfs[b])

            def _diff(x, y):
                if x is None or y is None:
                    return None
                return x - y

            # Physical
            ha = (player_index.get(a) or {}).get("height")
            hb = (player_index.get(b) or {}).get("height")
            height_diff = (ha - hb) if (ha and hb) else None

            # Ranking trajectory: rank now vs ~12 weeks ago
            target_now = m.date - timedelta(days=1)
            target_old = m.date - timedelta(days=84)
            ra_now = _rank_at(rank_index.get(a, []), target_now)
            ra_old = _rank_at(rank_index.get(a, []), target_old)
            rb_now = _rank_at(rank_index.get(b, []), target_now)
            rb_old = _rank_at(rank_index.get(b, []), target_old)
            traj_a = (ra_old - ra_now) if (ra_now and ra_old) else None  # positive = improving
            traj_b = (rb_old - rb_now) if (rb_now and rb_old) else None
            rank_traj_diff = _diff(traj_a, traj_b)

            rows.append({
                "match_id": m.id,
                "date": m.date,
                "tour": m.tour,
                "surface": surf,
                "elo_diff": ra.overall - rb.overall,
                "elo_surf_diff": elo_a_surf - elo_b_surf,
                "form10_diff": _last_n_winrate(form[a], 10) - _last_n_winrate(form[b], 10),
                "surf_form_diff": _last_n_winrate(surf_form[(a, surf)], 10) - _last_n_winrate(surf_form[(b, surf)], 10),
                "rest_diff": days_since_a - days_since_b,
                "h2h_diff": h2h_diff,
                "matches_played_diff": ra.n_overall - rb.n_overall,
                "serve_pts_won_diff": _diff(sp_a, sp_b),
                "return_pts_won_diff": _diff(rp_a, rp_b),
                "surf_serve_diff": _diff(sps_a, sps_b),
                "surf_return_diff": _diff(rps_a, rps_b),
                "bp_save_pct_diff": _diff(bps_a, bps_b),
                "bp_convert_pct_diff": _diff(bpc_a, bpc_b),
                "ace_rate_diff": _diff(ace_a, ace_b),
                "df_rate_diff": _diff(df_a, df_b),
                "height_diff_cm": height_diff,
                "rank_traj_diff": rank_traj_diff,
                "label": label,
            })

        # ---- Update running state with this match's outcome ----
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
        surf_form[(w, surf)].append(1); surf_form[(l, surf)].append(0)
        last_match[w] = m.date; last_match[l] = m.date
        key = (min(w, l), max(w, l))
        h2h[key].append(1 if w == key[0] else 0)

        # Serve/return state — only update when stats present
        if m.w_svpt and m.l_svpt:
            w_serve_won = (m.w_1stWon or 0) + (m.w_2ndWon or 0)
            l_serve_won = (m.l_1stWon or 0) + (m.l_2ndWon or 0)
            serve_pts[w].append((w_serve_won, m.w_svpt))
            serve_pts[l].append((l_serve_won, m.l_svpt))
            return_pts[w].append((m.l_svpt - l_serve_won, m.l_svpt))
            return_pts[l].append((m.w_svpt - w_serve_won, m.w_svpt))
            serve_pts_surf[(w, surf)].append((w_serve_won, m.w_svpt))
            serve_pts_surf[(l, surf)].append((l_serve_won, m.l_svpt))
            return_pts_surf[(w, surf)].append((m.l_svpt - l_serve_won, m.l_svpt))
            return_pts_surf[(l, surf)].append((m.w_svpt - w_serve_won, m.w_svpt))
            if m.w_ace is not None:
                aces[w].append((m.w_ace, m.w_svpt))
            if m.w_df is not None:
                dfs[w].append((m.w_df, m.w_svpt))
            if m.l_ace is not None:
                aces[l].append((m.l_ace, m.l_svpt))
            if m.l_df is not None:
                dfs[l].append((m.l_df, m.l_svpt))
        if m.w_bpFaced:
            bp_saved[w].append((m.w_bpSaved or 0, m.w_bpFaced))
            bp_converted[l].append(((m.w_bpFaced or 0) - (m.w_bpSaved or 0), m.w_bpFaced))
        if m.l_bpFaced:
            bp_saved[l].append((m.l_bpSaved or 0, m.l_bpFaced))
            bp_converted[w].append(((m.l_bpFaced or 0) - (m.l_bpSaved or 0), m.l_bpFaced))

    return pd.DataFrame(rows)
