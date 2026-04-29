"""For every priced fixture: build features, run model, write Prediction, place Bet if edge ≥ threshold."""
from __future__ import annotations

import logging
from datetime import date, datetime

from sqlalchemy import select

from .betting.ledger import place_bet_from_prediction
from .db import Fixture, Prediction, init_db, session
from .features.live import build_live_row
from .models.baseline import load_latest, predict_proba

log = logging.getLogger(__name__)


def run(engine=None) -> dict:
    engine = engine or init_db()
    try:
        model = load_latest()
    except FileNotFoundError as e:
        log.error("%s", e)
        return {"predictions": 0, "bets": 0, "skipped": 0}

    today = date.today()
    n_pred = n_bet = n_skip = 0

    with session(engine) as s:
        fixtures = list(s.scalars(
            select(Fixture).where(
                Fixture.status == "scheduled",
                Fixture.date >= today,
                Fixture.player_a_id.is_not(None),
                Fixture.player_b_id.is_not(None),
                Fixture.odds_a.is_not(None),
                Fixture.odds_b.is_not(None),
                Fixture.surface.is_not(None),
            )
        ))
        log.info("predicting on %d priced fixtures", len(fixtures))

        for fx in fixtures:
            row = build_live_row(fx.player_a_id, fx.player_b_id, fx.surface, fx.date, engine)
            if row is None:
                n_skip += 1
                continue
            p_a = float(predict_proba(row, model)[0])

            # De-vig the market for a sanity-check edge calculation
            implied_a = 1 / fx.odds_a
            implied_b = 1 / fx.odds_b
            margin = implied_a + implied_b
            fair_a = implied_a / margin
            fair_b = implied_b / margin

            edge_a = p_a * fx.odds_a - 1
            edge_b = (1 - p_a) * fx.odds_b - 1

            pred = Prediction(
                model_version=model["version"],
                player_a_id=fx.player_a_id,
                player_b_id=fx.player_b_id,
                match_date=fx.date,
                surface=fx.surface,
                tourney_name=fx.tourney_name,
                p_a_wins=p_a,
                odds_a=fx.odds_a,
                odds_b=fx.odds_b,
                edge_a=edge_a,
                edge_b=edge_b,
            )
            s.add(pred)
            s.flush()  # need pred.id for bet
            n_pred += 1

            placed = place_bet_from_prediction(pred, engine)
            if placed:
                n_bet += 1
                fx.status = "bet_placed"
                log.info("BET %s vs %s @ %.2f stake $%.2f edge %.1f%% (model %.3f, market %.3f)",
                         placed.pick_player_id, placed.opponent_id, placed.odds, placed.stake,
                         placed.edge * 100, placed.model_p,
                         fair_a if placed.pick_player_id == fx.player_a_id else fair_b)
            else:
                fx.status = "predicted"
        s.commit()

    log.info("done: predictions=%d bets=%d skipped=%d", n_pred, n_bet, n_skip)
    return {"predictions": n_pred, "bets": n_bet, "skipped": n_skip}
