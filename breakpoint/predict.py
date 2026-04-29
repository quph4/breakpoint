"""For every priced fixture: build features, run model, write Prediction, place Bet if edge ≥ threshold."""
from __future__ import annotations

import logging
from datetime import date, datetime

from sqlalchemy import select

from .betting.ledger import place_bet_from_prediction
from .betting.rationale import make_rationale
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

    # Phase 1: write all predictions in one transaction.
    pred_ids: list[tuple[int, int]] = []  # (prediction_id, fixture_id)
    features_by_pred: dict[int, dict] = {}
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
            s.flush()
            pred_ids.append((pred.id, fx.id))
            features_by_pred[pred.id] = row.iloc[0].to_dict()
            n_pred += 1
        s.commit()

    # Phase 2: place bets in a separate session per prediction so SQLite never
    # holds two writers at once.
    with session(engine) as s:
        for pred_id, fx_id in pred_ids:
            pred = s.get(Prediction, pred_id)
            fx = s.get(Fixture, fx_id)
            features = features_by_pred.get(pred_id, {})
            pick_is_a = (pred.edge_a or -1) >= (pred.edge_b or -1)
            rationale = make_rationale(features, pick_is_a, pred.surface)
            placed = place_bet_from_prediction(pred, engine, rationale=rationale)
            if placed:
                n_bet += 1
                fx.status = "bet_placed"
                log.info("BET %s vs %s @ %.2f stake $%.2f edge %.1f%% (model %.3f)",
                         placed.pick_player_id, placed.opponent_id, placed.odds,
                         placed.stake, placed.edge * 100, placed.model_p)
            else:
                fx.status = "predicted"
        s.commit()

    log.info("done: predictions=%d bets=%d skipped=%d", n_pred, n_bet, n_skip)
    return {"predictions": n_pred, "bets": n_bet, "skipped": n_skip}
