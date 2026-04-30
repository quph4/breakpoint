"""Fake-money ledger. Bankroll starts at $1000.

Sizing uses fractional Kelly (1/4) capped at 5% of bankroll. Full Kelly is too
aggressive — every working sports bettor uses a fraction. We only place a bet
when edge exceeds MIN_EDGE; tennis markets are tight so 3% is the floor.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import and_, or_, select

from ..db import Bet, Match, Player, Prediction, init_db, session

log = logging.getLogger(__name__)

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


def place_bet_from_prediction(pred: Prediction, engine=None,
                              rationale: list[str] | None = None) -> PlacedBet | None:
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
        # Idempotency: refuse to place a second bet on the same matchup
        # (same date, same pair) regardless of which fixture row drove it
        # or which side we previously picked. Guards against The Odds API
        # listing the same match under multiple event_ids and against
        # cache-loss replays after a failed run.
        dup = s.scalar(
            select(Bet).where(
                Bet.match_date == pred.match_date,
                Bet.status == "open",
                or_(
                    and_(Bet.pick_player_id == pick_id, Bet.opponent_id == opp_id),
                    and_(Bet.pick_player_id == opp_id, Bet.opponent_id == pick_id),
                ),
            )
        )
        if dup:
            return None

        bet = Bet(
            prediction_id=pred.id, match_date=pred.match_date,
            pick_player_id=pick_id, opponent_id=opp_id,
            surface=pred.surface, tourney_name=pred.tourney_name,
            stake=stake, odds=odds, model_p=p, edge=edge, status="open",
            rationale=json.dumps(rationale) if rationale else None,
        )
        s.add(bet); s.commit()
    return PlacedBet(pick_id, opp_id, stake, odds, edge, p)


def void_duplicate_bets(engine=None) -> int:
    """Group open bets by (date, unordered pair). For any group with more than
    one bet, keep the oldest and void the rest (status=void, pnl=0). Voided
    bets release their stake back to bankroll automatically because
    current_bankroll only deducts stakes from `status=open` rows.
    """
    engine = engine or init_db()
    voided = 0
    groups: dict[tuple, list[Bet]] = {}
    with session(engine) as s:
        opens = list(s.scalars(select(Bet).where(Bet.status == "open").order_by(Bet.placed_at)))
        for b in opens:
            key = (b.match_date, frozenset({b.pick_player_id, b.opponent_id}))
            groups.setdefault(key, []).append(b)
        for bets in groups.values():
            if len(bets) <= 1:
                continue
            for dup in bets[1:]:
                dup.status = "void"
                dup.pnl = 0
                dup.settled_at = datetime.utcnow()
                voided += 1
        s.commit()
    return voided


def _settle_against_match(bet: Bet, m: Match) -> None:
    if m.winner_id == bet.pick_player_id:
        bet.status = "won"
        bet.pnl = round(bet.stake * (bet.odds - 1), 2)
    else:
        bet.status = "lost"
        bet.pnl = -bet.stake
    bet.settled_at = datetime.utcnow()


def _settle_via_odds_api(s, open_bets: list) -> int:
    """Look up completed events on The Odds API /scores and settle any open
    bets we can identify by player names. Used when Sackmann hasn't published
    a same-day result yet (his repo updates weekly during the season).

    The /scores endpoint costs 2 quota per call; quota is the binding
    constraint on the free tier. So we only invoke it when we actually
    have something to settle: an open bet whose match_date is within the
    /scores window (today - 3 days to today). Older bets won't be in the
    /scores response anyway, future-dated bets haven't happened yet.
    """
    today = date.today()
    settleable = [
        b for b in open_bets
        if b.match_date and (today - b.match_date).days >= 0
                       and (today - b.match_date).days <= 3
    ]
    if not settleable:
        log.info("scores fallback: no settleable bets in /scores window, skipping API call")
        return 0
    open_bets = settleable

    # Late import to avoid pulling requests in CLI startup.
    from ..ingest.odds_api import active_tennis_sports, fetch_scores_for_sport
    from ..name_resolver import resolve

    sports = active_tennis_sports()
    if not sports:
        return 0

    # Index open bets by (sorted player_id pair, date) for fast lookup
    bet_index: dict[tuple[frozenset, "date"], list] = {}
    for b in open_bets:
        key = (frozenset({b.pick_player_id, b.opponent_id}), b.match_date)
        bet_index.setdefault(key, []).append(b)

    settled = 0
    log.info("scores fallback: %d open bets to try", len(open_bets))
    for sport in sports:
        sport_key = sport["key"]
        tour = "wta" if "wta" in sport_key.lower() else "atp"
        events = fetch_scores_for_sport(sport_key, days_from=3)
        log.info("scores [%s]: %d events", sport_key, len(events))
        for ev in events:
            home, away = ev.get("home_team"), ev.get("away_team")
            completed = ev.get("completed", False)
            commence = ev.get("commence_time", "")
            scores = ev.get("scores") or []
            log.info("  ev: %s vs %s | completed=%s | commence=%s | scores=%s",
                     home, away, completed, commence, scores)
            if not completed:
                continue
            if not (home and away and commence):
                continue
            try:
                ev_date = datetime.fromisoformat(commence.replace("Z", "+00:00")).date()
            except ValueError:
                continue
            home_id = resolve(home, tour)
            away_id = resolve(away, tour)
            log.info("    resolved: %s -> %s, %s -> %s", home, home_id, away, away_id)
            if not home_id or not away_id:
                continue

            home_score = next((sc.get("score") for sc in scores if sc.get("name") == home), None)
            away_score = next((sc.get("score") for sc in scores if sc.get("name") == away), None)
            try:
                home_n = int(home_score)
                away_n = int(away_score)
            except (TypeError, ValueError):
                continue
            if home_n == away_n:
                continue
            winner_id = home_id if home_n > away_n else away_id

            from datetime import timedelta as _td
            for ddelta in (0, -1, 1):
                key = (frozenset({home_id, away_id}), ev_date + _td(days=ddelta))
                for bet in bet_index.get(key, []):
                    if bet.status != "open":
                        continue
                    class _M: pass
                    m = _M()
                    m.winner_id = winner_id
                    _settle_against_match(bet, m)
                    log.info("    SETTLED bet %s (pick=%s) against winner=%s",
                             bet.id, bet.pick_player_id, winner_id)
                    settled += 1
    return settled


def settle_bets(engine=None) -> int:
    """Mark open bets won/lost.

    Primary path: look up the result in our matches table (Sackmann data).
    Fallback: when the matches table doesn't have it yet, hit The Odds API
    /scores endpoint to settle from live results.
    """
    engine = engine or init_db()
    settled = 0
    with session(engine) as s:
        open_bets = list(s.scalars(select(Bet).where(Bet.status == "open")))
        unsettled = []
        for bet in open_bets:
            m = s.scalar(
                select(Match).where(
                    Match.date == bet.match_date,
                    ((Match.winner_id == bet.pick_player_id) & (Match.loser_id == bet.opponent_id)) |
                    ((Match.winner_id == bet.opponent_id) & (Match.loser_id == bet.pick_player_id))
                )
            )
            if m:
                _settle_against_match(bet, m)
                settled += 1
            else:
                unsettled.append(bet)
        # Try the API fallback for anything still open
        settled += _settle_via_odds_api(s, unsettled)
        s.commit()
    return settled
