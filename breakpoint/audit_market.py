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

from .db import Match, Odds, Player, init_db, session
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

    # Sample 20 matches from the most-extreme-disagreement bucket so we can
    # eyeball them. If model_p, market_p, player names, and outcomes pass a
    # sniff test against historical sportsbook archives, the audit numbers
    # are real. If they look wrong, we've localized a bug.
    samples = _sample_extreme_disagreement(df, p_model, engine=engine)

    return {
        "n": int(len(df)),
        "date_min": df["date"].min().date().isoformat() if hasattr(df["date"].min(), "date") else str(df["date"].min()),
        "date_max": df["date"].max().date().isoformat() if hasattr(df["date"].max(), "date") else str(df["date"].max()),
        "mean_overround": round(float(df["market_overround"].mean()), 4),
        "model": metrics(p_model),
        "market": metrics(p_market),
        "disagreement_distribution": disagreement_buckets,
        "profit_simulation": profit_thresholds,
        "samples_extreme_disagreement": samples,
    }


def _sample_extreme_disagreement(df: pd.DataFrame, p_model: np.ndarray,
                                  threshold: float = 0.20, n_per_side: int = 10,
                                  engine=None) -> list[dict]:
    """Pull the most disagreeable matches both ways.

    Returns up to n_per_side rows where (model_p - market_p) >= +threshold AND
    up to n_per_side where the disagreement runs the other way. Each entry has
    enough context — names, date, surface, tournament, both probs, outcome,
    a few key features — that we can spot-check against external sources.
    """
    engine = engine or init_db()
    df = df.assign(
        model_p=p_model,
        disagreement=p_model - df["market_p_a"].to_numpy(),
    )
    pos = df[df["disagreement"] >= threshold].sample(min(n_per_side, sum(df["disagreement"] >= threshold)),
                                                       random_state=42) if (df["disagreement"] >= threshold).any() else df.head(0)
    neg = df[df["disagreement"] <= -threshold].sample(min(n_per_side, sum(df["disagreement"] <= -threshold)),
                                                       random_state=42) if (df["disagreement"] <= -threshold).any() else df.head(0)

    sample_rows = pd.concat([pos, neg]).reset_index(drop=True)
    if sample_rows.empty:
        return []

    # Resolve player_a and player_b names. Need to recover them from the match.
    # build_training_frame doesn't return player IDs, only match_id and label.
    with session(engine) as s:
        match_meta: dict[int, dict] = {}
        for m in s.scalars(select(Match).where(Match.id.in_(sample_rows["match_id"].tolist()))):
            match_meta[m.id] = {
                "tour": m.tour,
                "date": m.date.isoformat() if m.date else None,
                "tourney": m.tourney_name,
                "surface": m.surface,
                "round": m.round,
                "winner_id": m.winner_id,
                "loser_id": m.loser_id,
            }
        # Pull player names for everyone we care about
        all_pids = set()
        for meta in match_meta.values():
            if meta["winner_id"]: all_pids.add(meta["winner_id"])
            if meta["loser_id"]: all_pids.add(meta["loser_id"])
        names = dict(s.execute(select(Player.id, Player.name).where(Player.id.in_(all_pids))).all())

    out = []
    for _, row in sample_rows.iterrows():
        meta = match_meta.get(row["match_id"], {})
        # If label==1, player_a was the winner; else loser
        if row["label"] == 1:
            a_id, b_id = meta.get("winner_id"), meta.get("loser_id")
        else:
            a_id, b_id = meta.get("loser_id"), meta.get("winner_id")

        out.append({
            "match_id": int(row["match_id"]),
            "date": meta.get("date"),
            "tour": meta.get("tour"),
            "tourney": meta.get("tourney"),
            "round": meta.get("round"),
            "surface": meta.get("surface"),
            "player_a": names.get(a_id),
            "player_b": names.get(b_id),
            "model_p_a_wins": round(float(row["model_p"]), 4),
            "market_p_a_wins_devigged": round(float(row["market_p_a"]), 4),
            "disagreement": round(float(row["disagreement"]), 4),
            "actual_a_won": int(row["label"]),
            "odds_a": round(float(row["odds_a"]), 3),
            "odds_b": round(float(row["odds_b"]), 3),
            "elo_diff": round(float(row.get("elo_diff", 0)), 1),
            "elo_surf_diff": round(float(row.get("elo_surf_diff", 0)), 1),
            "h2h_diff": round(float(row.get("h2h_diff", 0)), 3),
        })
    # Sort by absolute disagreement, biggest first
    out.sort(key=lambda r: abs(r["disagreement"]), reverse=True)
    return out


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
