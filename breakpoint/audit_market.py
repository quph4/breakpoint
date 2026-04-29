"""Compare model probabilities against de-vigged market probabilities.

For every test-set match where we also have closing odds (from tennis-
data.co.uk), de-vig the bookmaker line into a fair probability and
compare against the model's calibrated prediction.

This answers: when the model claims an edge, is it on average right
against the market closing line?

Approach:
  1. Load test predictions (saved by `models.baseline.train`).
  2. Look up Odds row per match (we need to re-run scoring against the
     match table, since test_predictions.json doesn't carry match_id).
  3. We rebuild the test set's match lineage on the fly by re-running
     the same `build_training_frame` pass and keeping match_ids.
  4. De-vig: fair_w = (1/odds_w) / (1/odds_w + 1/odds_l).
  5. Bucket the difference (model_p - market_p) and check:
       - calibration of model vs actual outcome
       - calibration of market vs actual outcome
       - distribution of model-market disagreement
       - profit-curve simulation: if we'd bet every match where edge ≥ X,
         what would historical ROI have been?
"""
from __future__ import annotations

import json
import logging
import pickle
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss
from sqlalchemy import select

from .db import Match, Odds, init_db, session
from .features.build import build_training_frame
from .models.baseline import FEATURES, MODEL_DIR, load_latest

log = logging.getLogger(__name__)


def _load_model():
    path = MODEL_DIR / "latest.pkl"
    if not path.exists():
        raise FileNotFoundError(f"No model at {path}")
    with open(path, "rb") as f:
        return pickle.load(f)


def _build_audit_frame(min_year: int = 2015, holdout_frac: float = 0.15,
                       engine=None) -> pd.DataFrame | None:
    """Re-derive the test split with match_ids preserved, then attach odds."""
    engine = engine or init_db()

    df = build_training_frame(engine=engine, min_year=min_year)
    if df.empty:
        log.warning("no training rows; cannot run market audit")
        return None
    df = df.sort_values("date").reset_index(drop=True)
    cutoff = int(len(df) * (1 - holdout_frac))
    test_df = df.iloc[cutoff:].copy()
    log.info("test split: %d rows from %s to %s", len(test_df),
             test_df["date"].min(), test_df["date"].max())

    # Pull odds for every match_id in test_df
    with session(engine) as s:
        rows = s.execute(
            select(Match.id, Match.winner_id, Match.loser_id,
                   Odds.b365_w, Odds.b365_l, Odds.ps_w, Odds.ps_l, Odds.avg_w, Odds.avg_l)
            .join(Odds, Odds.match_id == Match.id)
            .where(Match.id.in_(test_df["match_id"].tolist()))
        ).all()

    if not rows:
        log.warning("no Odds rows joined to test matches — run `ingest-historical-odds` first")
        return None

    odds_df = pd.DataFrame(rows, columns=[
        "match_id", "winner_id", "loser_id",
        "b365_w", "b365_l", "ps_w", "ps_l", "avg_w", "avg_l",
    ])
    log.info("joined odds for %d / %d test matches", len(odds_df), len(test_df))

    merged = test_df.merge(odds_df, on="match_id", how="inner")

    # Each test row has: features_for_(player_a vs player_b), label = 1 if a won.
    # We need to map odds_w/l (winner/loser) onto a/b. We need to know who the
    # winner_id was for each match — fetch and align.
    with session(engine) as s:
        winners = dict(s.execute(
            select(Match.id, Match.winner_id).where(Match.id.in_(merged["match_id"].tolist()))
        ).all())
    merged["match_winner_id"] = merged["match_id"].map(winners)

    # Need player_a_id/_b_id for each test row. The build pass randomized these.
    # Reconstruct by re-running with same hash logic — but it's simpler to just
    # check label: if label==1, player_a was the winner; if label==0, player_b was.
    # Either way, we can map odds w/l to a/b without identity resolution:
    #   if label==1: player_a is winner, so odds_a = odds_w, odds_b = odds_l
    #   if label==0: player_a is loser, so odds_a = odds_l, odds_b = odds_w
    merged["odds_a"] = np.where(merged["label"] == 1, merged["b365_w"], merged["b365_l"])
    merged["odds_b"] = np.where(merged["label"] == 1, merged["b365_l"], merged["b365_w"])

    # Drop rows missing B365 odds
    merged = merged.dropna(subset=["odds_a", "odds_b"])
    if merged.empty:
        return None

    # De-vig
    inv_a = 1 / merged["odds_a"]
    inv_b = 1 / merged["odds_b"]
    margin = inv_a + inv_b
    merged["market_p_a"] = inv_a / margin
    merged["market_overround"] = margin - 1.0

    return merged


def compute_market_audit(min_year: int = 2015, engine=None) -> dict | None:
    df = _build_audit_frame(min_year=min_year, engine=engine)
    if df is None:
        return None

    model = _load_model()
    raw = model["booster"].predict_proba(df[FEATURES])[:, 1]
    cal = model["calibrator"].predict(raw)
    df["model_p"] = cal

    y = df["label"].to_numpy()
    p_model = df["model_p"].to_numpy()
    p_market = df["market_p_a"].to_numpy()

    def metrics(p):
        p_clip = np.clip(p, 1e-6, 1 - 1e-6)
        return {
            "log_loss": float(log_loss(y, p_clip)),
            "brier": float(brier_score_loss(y, p)),
            "n": int(len(y)),
        }

    # Disagreement distribution
    disagreement = (p_model - p_market)
    disagreement_buckets = []
    edges = [-1.01, -0.20, -0.10, -0.05, -0.02, 0.02, 0.05, 0.10, 0.20, 1.01]
    for i in range(len(edges) - 1):
        lo, hi = edges[i], edges[i + 1]
        mask = (disagreement >= lo) & (disagreement < hi)
        if mask.sum() == 0:
            disagreement_buckets.append({"lo": lo, "hi": hi, "n": 0,
                                         "model_winrate": None, "market_winrate": None,
                                         "actual_rate": None})
            continue
        disagreement_buckets.append({
            "lo": lo, "hi": hi, "n": int(mask.sum()),
            "model_winrate": float(p_model[mask].mean()),
            "market_winrate": float(p_market[mask].mean()),
            "actual_rate": float(y[mask].mean()),
        })

    # Edge-threshold profit simulation: bet $1 (flat) every time edge ≥ T
    # on player_a side, payoff = odds_a - 1 if a wins else -1.
    # Player_b side too.
    profit_thresholds = []
    odds_a = df["odds_a"].to_numpy()
    odds_b = df["odds_b"].to_numpy()
    edge_a = p_model * odds_a - 1
    edge_b = (1 - p_model) * odds_b - 1
    for t in [0.0, 0.02, 0.03, 0.05, 0.08, 0.12, 0.20]:
        n_bets = 0; total_pnl = 0.0
        for i in range(len(df)):
            if edge_a[i] >= t:
                n_bets += 1
                total_pnl += (odds_a[i] - 1) if y[i] == 1 else -1
            if edge_b[i] >= t:
                n_bets += 1
                total_pnl += (odds_b[i] - 1) if y[i] == 0 else -1
        roi = total_pnl / n_bets if n_bets else None
        profit_thresholds.append({
            "edge_threshold": t,
            "bets": n_bets,
            "pnl_units": round(total_pnl, 2),
            "roi": round(roi, 4) if roi is not None else None,
        })

    return {
        "n": int(len(df)),
        "date_min": df["date"].min().date().isoformat() if hasattr(df["date"].min(), "date") else str(df["date"].min()),
        "date_max": df["date"].max().date().isoformat() if hasattr(df["date"].max(), "date") else str(df["date"].max()),
        "mean_overround": round(float(df["market_overround"].mean()), 4),
        "model": metrics(p_model),
        "market": metrics(p_market),
        "disagreement_distribution": disagreement_buckets,
        "profit_simulation": profit_thresholds,
    }


def write_market_audit() -> dict | None:
    report = compute_market_audit()
    if report is None:
        return None
    out = MODEL_DIR / "audit_market.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    log.info("market audit written: n=%d, model_brier=%.4f, market_brier=%.4f",
             report["n"], report["model"]["brier"], report["market"]["brier"])
    return report
