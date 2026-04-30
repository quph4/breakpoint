"""Export DB state to JSON files the dashboard reads.

We ship JSON to `dashboard/public/data/` so Vite copies them as static assets.
This is the single source of truth the website renders.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import select, func

from .betting.ledger import STARTING_BANKROLL, current_bankroll
from .db import Bet, Match, Player, Prediction, Rating, init_db, session

OUT = Path(__file__).resolve().parent.parent / "dashboard" / "public" / "data"


def _json_default(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    raise TypeError(f"unserializable: {type(o)}")


def _write(name: str, payload) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / name, "w", encoding="utf-8") as f:
        json.dump(payload, f, default=_json_default, indent=2)


def export_summary(engine=None) -> dict:
    engine = engine or init_db()
    with session(engine) as s:
        total = s.scalar(select(func.count(Bet.id))) or 0
        won = s.scalar(select(func.count(Bet.id)).where(Bet.status == "won")) or 0
        lost = s.scalar(select(func.count(Bet.id)).where(Bet.status == "lost")) or 0
        open_ = s.scalar(select(func.count(Bet.id)).where(Bet.status == "open")) or 0
        staked = s.scalar(select(func.sum(Bet.stake)).where(Bet.status.in_(["won", "lost"]))) or 0
        pnl = s.scalar(select(func.sum(Bet.pnl)).where(Bet.status.in_(["won", "lost"]))) or 0

    settled = won + lost
    payload = {
        "updated_at": datetime.utcnow().isoformat(),
        "starting_bankroll": STARTING_BANKROLL,
        "bankroll": round(current_bankroll(engine), 2),
        "total_bets": total,
        "open": open_,
        "won": won,
        "lost": lost,
        "win_rate": round(won / settled, 4) if settled else None,
        "roi": round(pnl / staked, 4) if staked else None,
        "total_pnl": round(pnl, 2),
    }
    _write("summary.json", payload)
    return payload


def export_bets(engine=None, limit_settled: int = 500) -> None:
    engine = engine or init_db()
    with session(engine) as s:
        rows = []
        for b in s.scalars(select(Bet).order_by(Bet.placed_at.desc())):
            pick = s.get(Player, b.pick_player_id)
            opp = s.get(Player, b.opponent_id)
            try:
                rationale = json.loads(b.rationale) if b.rationale else []
            except (TypeError, ValueError):
                rationale = []
            rows.append({
                "id": b.id,
                "placed_at": b.placed_at,
                "match_date": b.match_date,
                "pick": pick.name if pick else None,
                "opponent": opp.name if opp else None,
                "tour": pick.tour if pick else None,
                "surface": b.surface,
                "tourney": b.tourney_name,
                "stake": b.stake, "odds": b.odds,
                "model_p": b.model_p, "edge": b.edge,
                "status": b.status, "pnl": b.pnl,
                "rationale": rationale,
            })
    open_bets = [r for r in rows if r["status"] == "open"]
    settled = [r for r in rows if r["status"] != "open"][:limit_settled]
    _write("open_bets.json", open_bets)
    _write("settled_bets.json", settled)


def export_players(engine=None, top_n: int = 500) -> None:
    """Latest snapshot per player, top-N by overall Elo, both tours."""
    engine = engine or init_db()
    with session(engine) as s:
        # subquery: max(date) per player_id
        latest = {}
        for r in s.scalars(select(Rating).order_by(Rating.date)):
            latest[r.player_id] = r

        out = []
        for pid, r in latest.items():
            p = s.get(Player, pid)
            if not p:
                continue
            out.append({
                "id": pid, "name": p.name, "tour": p.tour, "country": p.country, "hand": p.hand,
                "elo_overall": round(r.elo_overall, 1) if r.elo_overall else None,
                "elo_hard": round(r.elo_hard, 1) if r.elo_hard else None,
                "elo_clay": round(r.elo_clay, 1) if r.elo_clay else None,
                "elo_grass": round(r.elo_grass, 1) if r.elo_grass else None,
                "matches_played": r.matches_played,
            })

    out.sort(key=lambda x: x["elo_overall"] or 0, reverse=True)
    _write("players.json", out[:top_n])


def export_pnl_curve(engine=None) -> None:
    engine = engine or init_db()
    with session(engine) as s:
        bets = list(s.scalars(
            select(Bet).where(Bet.status.in_(["won", "lost"])).order_by(Bet.settled_at)
        ))
    points = []
    running = STARTING_BANKROLL
    for b in bets:
        running += b.pnl or 0
        points.append({"date": b.settled_at, "bankroll": round(running, 2)})
    _write("pnl_curve.json", points)


def export_audit() -> None:
    from .audit import compute_audit
    try:
        report = compute_audit()
    except FileNotFoundError:
        return
    _write("audit.json", report)


def export_market_audit() -> None:
    from pathlib import Path
    src = Path(__file__).resolve().parent.parent / "data" / "models" / "audit_market.json"
    if not src.exists():
        return
    import json
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "audit_market.json").write_text(src.read_text(), encoding="utf-8")


def export_train_stats() -> None:
    from pathlib import Path
    src = Path(__file__).resolve().parent.parent / "data" / "models" / "train_stats.json"
    if not src.exists():
        return
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "train_stats.json").write_text(src.read_text(), encoding="utf-8")


def export_all(engine=None) -> None:
    engine = engine or init_db()
    export_summary(engine)
    export_bets(engine)
    export_players(engine)
    export_pnl_curve(engine)
    export_audit()
    export_market_audit()
    export_train_stats()
