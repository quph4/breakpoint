"""Diagnostic: count P(label=1) bucketed by feature value across the full
training frame. Tells us whether the labels in `build_training_frame` are
correctly aligned with the features.

If P(label=1 | elo_diff < -150) is roughly 50%, the labels aren't aligned
with features at all — the model is learning something else (and we have
a build bug). If it's ~5%, labels are correctly anti-aligned with strongly
negative features and the model's confident predictions on negative-feature
rows are coming from elsewhere (calibrator, model overfit, or actual signal).
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from .features.build import build_training_frame
from .models.baseline import MODEL_DIR

log = logging.getLogger(__name__)


ELO_BINS = [(-1e9, -300), (-300, -200), (-200, -100), (-100, 0),
            (0, 100), (100, 200), (200, 300), (300, 1e9)]
SMALL_BINS = [(-1e9, -0.2), (-0.2, -0.1), (-0.1, 0), (0, 0.1), (0.1, 0.2), (0.2, 1e9)]


def _bin_label_rate(df: pd.DataFrame, col: str, bins: list[tuple[float, float]]) -> list[dict]:
    out = []
    for lo, hi in bins:
        mask = (df[col] >= lo) & (df[col] < hi)
        n = int(mask.sum())
        if n == 0:
            out.append({"lo": lo, "hi": hi, "n": 0, "label_rate": None})
        else:
            out.append({
                "lo": lo if lo > -1e8 else None,
                "hi": hi if hi < 1e8 else None,
                "n": n,
                "label_rate": round(float(df.loc[mask, "label"].mean()), 4),
            })
    return out


def compute_stats(min_year: int = 2015) -> dict:
    df = build_training_frame(min_year=min_year)
    if df.empty:
        return {"error": "empty training frame"}

    overall_label_rate = round(float(df["label"].mean()), 4)
    n = len(df)

    # 1. Bucket by single features
    by_elo_diff = _bin_label_rate(df, "elo_diff", ELO_BINS)
    by_elo_surf_diff = _bin_label_rate(df, "elo_surf_diff", ELO_BINS)
    by_form10_diff = _bin_label_rate(df, "form10_diff", SMALL_BINS)
    by_h2h_diff = _bin_label_rate(df, "h2h_diff", SMALL_BINS)

    # 2. Joint condition: ALL major features negative for player_a
    cond_all_neg = (
        (df["elo_diff"] < -150) &
        (df["elo_surf_diff"] < -150) &
        (df["form10_diff"] < 0) &
        (df["h2h_diff"] <= 0)
    )
    cond_all_pos = (
        (df["elo_diff"] > 150) &
        (df["elo_surf_diff"] > 150) &
        (df["form10_diff"] > 0) &
        (df["h2h_diff"] >= 0)
    )

    joint = {
        "all_features_negative": {
            "n": int(cond_all_neg.sum()),
            "label_rate": (round(float(df.loc[cond_all_neg, "label"].mean()), 4)
                           if cond_all_neg.any() else None),
        },
        "all_features_positive": {
            "n": int(cond_all_pos.sum()),
            "label_rate": (round(float(df.loc[cond_all_pos, "label"].mean()), 4)
                           if cond_all_pos.any() else None),
        },
    }

    # 3. Sanity: distribution of `label` overall and within each tour
    by_tour = {}
    for tour in sorted(df["tour"].dropna().unique()):
        sub = df[df["tour"] == tour]
        by_tour[tour] = {
            "n": int(len(sub)),
            "label_rate": round(float(sub["label"].mean()), 4),
        }

    return {
        "n_rows": n,
        "overall_label_rate": overall_label_rate,
        "by_elo_diff": by_elo_diff,
        "by_elo_surf_diff": by_elo_surf_diff,
        "by_form10_diff": by_form10_diff,
        "by_h2h_diff": by_h2h_diff,
        "joint_extreme": joint,
        "by_tour": by_tour,
    }


def write_stats() -> dict:
    report = compute_stats()
    out = MODEL_DIR / "train_stats.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    log.info("train_stats: n=%d, overall_label_rate=%.4f",
             report.get("n_rows", 0), report.get("overall_label_rate", 0))
    return report
