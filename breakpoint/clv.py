"""Closing Line Value tracking.

Each cron run, for every open bet whose match hasn't started, find the
matching Fixture row and snapshot its current odds onto the bet. The
LAST snapshot before the match falls out of `/odds` is our best
approximation of the closing line.

After settlement we can compare the odds we took against this closing
snapshot to compute CLV — the standard reference metric for whether a
bettor is sharper than the market. Sustained positive CLV is the
signal we'd need before considering real money.
"""
from __future__ import annotations

import logging
from datetime import date, datetime

from sqlalchemy import and_, or_, select

from .db import Bet, Fixture, init_db, session

log = logging.getLogger(__name__)


def update_closing_lines(engine=None) -> int:
    """Refresh closing_odds_* on every open bet whose match is today or future.

    Matches Bet to Fixture by (player_a_id, player_b_id, match_date). Both
    orderings of the pair are tried since the bet's pick_player_id might be
    either side. Updates closing_odds_pick / opp aligned to the bet's pick.
    Returns count of bets updated.
    """
    engine = engine or init_db()
    today = date.today()
    updated = 0

    with session(engine) as s:
        open_bets = list(s.scalars(select(Bet).where(Bet.status == "open")))
        for b in open_bets:
            if not b.match_date or b.match_date < today:
                continue
            if b.pick_player_id is None or b.opponent_id is None:
                continue

            fx = s.scalar(
                select(Fixture).where(
                    Fixture.date == b.match_date,
                    or_(
                        and_(Fixture.player_a_id == b.pick_player_id,
                             Fixture.player_b_id == b.opponent_id),
                        and_(Fixture.player_a_id == b.opponent_id,
                             Fixture.player_b_id == b.pick_player_id),
                    ),
                    Fixture.odds_a.is_not(None),
                    Fixture.odds_b.is_not(None),
                )
            )
            if not fx:
                continue

            if fx.player_a_id == b.pick_player_id:
                pick_o, opp_o = fx.odds_a, fx.odds_b
            else:
                pick_o, opp_o = fx.odds_b, fx.odds_a

            b.closing_odds_pick = pick_o
            b.closing_odds_opp = opp_o
            b.closing_fetched_at = datetime.utcnow()
            updated += 1
        s.commit()

    log.info("closing-line refresh: updated %d open bets", updated)
    return updated


def compute_clv(odds_taken: float, closing_pick: float, closing_opp: float) -> float | None:
    """CLV expressed as expected edge (%) given the de-vigged closing line.

    EV = p_close * odds_taken - 1
    where p_close is the de-vigged closing implied probability for the side
    we backed. Positive means we got a price better than the closing line's
    fair value — i.e. we beat the market.
    """
    if not odds_taken or not closing_pick or not closing_opp:
        return None
    inv_pick = 1.0 / closing_pick
    inv_opp = 1.0 / closing_opp
    margin = inv_pick + inv_opp
    if margin <= 0:
        return None
    p_close = inv_pick / margin
    return p_close * odds_taken - 1.0
