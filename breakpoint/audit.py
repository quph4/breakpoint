"""Calibration audit on the held-out test split.

Reads the test predictions saved by `models.baseline.train`, builds:
  - 10-bucket reliability diagram (predicted vs actual frequency)
  - AUC / Brier / log-loss per slice (overall, per surface, per tour)
  - Confidence histogram (how often the model picks each probability range)
  - Bias summary: average over- or under-confidence

Caller writes the result to dashboard JSON via `export.py`.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

log = logging.getLogger(__name__)

MODEL_DIR = Path(__file__).resolve().parent.parent / "data" / "models"
N_BUCKETS = 10


def _load_predictions() -> pd.DataFrame:
    path = MODEL_DIR / "test_predictions.json"
    if not path.exists():
        raise FileNotFoundError(
            f"No test predictions at {path}. Train the model first."
        )
    df = pd.read_json(path)
    df["date"] = pd.to_datetime(df["date"])
    return df


def _safe_metrics(y: np.ndarray, p: np.ndarray) -> dict:
    if len(y) < 2 or len(set(y)) < 2:
        return {"n": int(len(y)), "auc": None, "log_loss": None, "brier": None}
    p_clip = np.clip(p, 1e-6, 1 - 1e-6)
    return {
        "n": int(len(y)),
        "auc": float(roc_auc_score(y, p)),
        "log_loss": float(log_loss(y, p_clip)),
        "brier": float(brier_score_loss(y, p)),
    }


def _reliability_buckets(y: np.ndarray, p: np.ndarray, n: int = N_BUCKETS) -> list[dict]:
    edges = np.linspace(0, 1, n + 1)
    out = []
    for i in range(n):
        lo, hi = edges[i], edges[i + 1]
        mask = (p >= lo) & (p < hi if i < n - 1 else p <= hi)
        bucket_p = p[mask]
        bucket_y = y[mask]
        out.append({
            "bucket_lo": float(lo),
            "bucket_hi": float(hi),
            "n": int(mask.sum()),
            "mean_predicted": float(bucket_p.mean()) if len(bucket_p) else None,
            "actual_rate": float(bucket_y.mean()) if len(bucket_y) else None,
        })
    return out


def compute_audit() -> dict:
    df = _load_predictions()
    y = df["label"].to_numpy()
    p_raw = df["raw_prob"].to_numpy()
    p_cal = df["cal_prob"].to_numpy()

    report: dict = {
        "n_test": int(len(df)),
        "date_min": df["date"].min().date().isoformat(),
        "date_max": df["date"].max().date().isoformat(),
        "overall": {
            "raw": _safe_metrics(y, p_raw),
            "calibrated": _safe_metrics(y, p_cal),
        },
        "reliability_calibrated": _reliability_buckets(y, p_cal),
        "reliability_raw": _reliability_buckets(y, p_raw),
        "by_surface": {},
        "by_tour": {},
        "confidence_histogram": [],
    }

    # Slices
    for surface in sorted(df["surface"].dropna().unique()):
        sub = df[df["surface"] == surface]
        report["by_surface"][surface] = _safe_metrics(
            sub["label"].to_numpy(), sub["cal_prob"].to_numpy()
        )
    for tour in sorted(df["tour"].dropna().unique()):
        sub = df[df["tour"] == tour]
        report["by_tour"][tour] = _safe_metrics(
            sub["label"].to_numpy(), sub["cal_prob"].to_numpy()
        )

    # Confidence histogram: how often does the model pick each prob range
    # (max(p, 1-p) — confidence in the favored side)
    conf = np.maximum(p_cal, 1 - p_cal)
    edges = [0.5, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.01]
    for i in range(len(edges) - 1):
        lo, hi = edges[i], edges[i + 1]
        mask = (conf >= lo) & (conf < hi)
        report["confidence_histogram"].append({
            "lo": lo, "hi": min(hi, 1.0), "n": int(mask.sum()),
            "fraction": float(mask.mean()),
        })

    # Bias summary — bucket-weighted gap between predicted and actual
    cal_buckets = report["reliability_calibrated"]
    weighted_gap = sum(
        (b["mean_predicted"] - b["actual_rate"]) * b["n"]
        for b in cal_buckets
        if b["n"] > 0 and b["mean_predicted"] is not None and b["actual_rate"] is not None
    ) / max(sum(b["n"] for b in cal_buckets), 1)
    report["bias"] = {
        "weighted_gap": round(weighted_gap, 4),
        "interpretation": (
            "overconfident" if weighted_gap > 0.02 else
            "underconfident" if weighted_gap < -0.02 else
            "well-calibrated"
        ),
    }

    return report


def write_audit(out_path: Path | None = None) -> dict:
    report = compute_audit()
    out_path = out_path or (MODEL_DIR / "audit.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2))
    log.info("audit written to %s (n=%d, bias=%s)",
             out_path, report["n_test"], report["bias"]["interpretation"])
    return report
