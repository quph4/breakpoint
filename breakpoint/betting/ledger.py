"""Fake-money ledger. Bankroll starts at $1000.

Sizing uses fractional Kelly (1/4) capped at 5% of bankroll. Full Kelly is too
aggressive — every working sports bettor uses a fraction. We only place a bet
when edge exceeds MIN_EDGE; tennis markets are tight so 3% is the floor.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import select

from ..db import Bet, Match, Player, Prediction, init_db, session

STARTING_BANKROLL = 1000.0
KELLY_FRACTION = 0.25
MAX_STAKE_FRAC = 0.05
MIN_EDGE = 0.03


def kelly_stake(p: float, decimal_odds: float, bankroll: float) -> float:
    """Fractional Kelly. Returns stake in $; 0 if no edge."""
    b = decimal_odds - 1.0
    if b <= 0 or p <= 0 or p >= 1:
        return 0.0
    q = 1 - p
    f = (b * p - q) / b
    if f <= 0:
        return 0.0
    f = min(f * KELLY_FRACTION, MAX_STAKE_FRAC)
    return round(bankroll * f, 2)


def current_bankroll(engine=None) -> float:
    engine = engine or init_db()
    with session(engine) as s:
        settled_pnl = sum(
            (b.pnl or 0.0)
            for b in s.scalars(select(Bet).where(Bet.status.in_(["won", "lost", "void"])))
        )
        open_stakes = sum(
            b.stake for b in s.scalars(select(Bet).where(Bet.status == "open"))
        )
    return STARTING_BANKROLL + settled_pnl - open_stakes


@dataclass
class PlacedBet:
    pick_player_id: int
    opponent_id: int
    stake: float
    odds: float
    edge: float
    model_p: float


def place_bet_from_prediction(pred: Prediction, engine=None) -> PlacedBet | None:
    """Picks the side with higher edge; skips if no side meets MIN_EDGE."""
    engine = engine or init_db()
    bankroll = current_bankroll(engine)

    sides = []
    if pred.odds_a and pred.edge_a is not None:
        sides.append((pred.player_a_id, pred.player_b_id, pred.p_a_wins, pred.odds_a, pred.edge_a))
    if pred.odds_b and pred.edge_b is not None:
        sides.append((pred.player_b_id, pred.player_a_id, 1 - pred.p_a_wins, pred.odds_b, pred.edge_b))
    if not sides:
        return None

    pick_id, opp_id, p, odds, edge = max(sides, key=lambda s: s[4])
    if edge < MIN_EDGE:
        return None

    stake = kelly_stake(p, odds, bankroll)
    if stake < 1.0:
        return None

    with session(engine) as s:
        bet = Bet(
            prediction_id=pred.id, match_date=pred.match_date,
            pick_player_id=pick_id, opponent_id=opp_id,
            surface=pred.surface, tourney_name=pred.tourney_name,
            stake=stake, odds=odds, model_p=p, edge=edge, status="open",
        )
        s.add(bet); s.commit()
    return PlacedBet(pick_id, opp_id, stake, odds, edge, p)


def settle_bets(engine=None) -> int:
    """Match open bets against historical results in `matches` table; mark won/lost."""
    engine = engine or init_db()
    settled = 0
    with session(engine) as s:
        for bet in list(s.scalars(select(Bet).where(Bet.status == "open"))):
            m = s.scalar(
                select(Match).where(
                    Match.date == bet.match_date,
                    ((Match.winner_id == bet.pick_player_id) & (Match.loser_id == bet.opponent_id)) |
                    ((Match.winner_id == bet.opponent_id) & (Match.loser_id == bet.pick_player_id))
                )
            )
            if not m:
                continue
            if m.winner_id == bet.pick_player_id:
                bet.status = "won"
                bet.pnl = round(bet.stake * (bet.odds - 1), 2)
            else:
                bet.status = "lost"
                bet.pnl = -bet.stake
            bet.settled_at = datetime.utcnow()
            settled += 1
        s.commit()
    return settled
