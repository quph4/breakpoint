"""LightGBM binary classifier with isotonic calibration.

Calibration is non-optional for betting — raw GBM probabilities are systematically
overconfident. We split the training set into a fit fold and a calibration fold,
train the booster on fit, then fit isotonic regression on calibration-fold predictions.
"""
from __future__ import annotations

import json
import pickle
from dataclasses import dataclass, asdict
from datetime import date, datetime
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

FEATURES = [
    "elo_diff", "elo_surf_diff", "form10_diff", "surf_form_diff",
    "rest_diff", "h2h_diff", "matches_played_diff",
]
MODEL_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "models"


@dataclass
class TrainReport:
    version: str
    rows_train: int
    rows_test: int
    auc: float
    log_loss: float
    brier: float
    feature_importance: dict


def _split_by_date(df: pd.DataFrame, holdout_frac: float = 0.15):
    df = df.sort_values("date").reset_index(drop=True)
    cutoff = int(len(df) * (1 - holdout_frac))
    return df.iloc[:cutoff], df.iloc[cutoff:]


def train(df: pd.DataFrame, version: str | None = None) -> TrainReport:
    version = version or datetime.utcnow().strftime("%Y%m%d-%H%M")
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    train_df, test_df = _split_by_date(df, holdout_frac=0.15)
    fit_df, calib_df = _split_by_date(train_df, holdout_frac=0.15)

    X_fit, y_fit = fit_df[FEATURES], fit_df["label"]
    X_calib, y_calib = calib_df[FEATURES], calib_df["label"]
    X_test, y_test = test_df[FEATURES], test_df["label"]

    booster = lgb.LGBMClassifier(
        n_estimators=600, learning_rate=0.03, num_leaves=63,
        min_child_samples=200, subsample=0.9, colsample_bytree=0.9,
        reg_lambda=1.0, random_state=42, verbose=-1,
    )
    booster.fit(X_fit, y_fit, eval_set=[(X_calib, y_calib)], callbacks=[lgb.early_stopping(40, verbose=False)])

    raw_calib = booster.predict_proba(X_calib)[:, 1]
    iso = IsotonicRegression(out_of_bounds="clip").fit(raw_calib, y_calib)

    raw_test = booster.predict_proba(X_test)[:, 1]
    cal_test = iso.predict(raw_test)

    # Persist held-out predictions for calibration audit
    test_export = test_df.assign(
        raw_prob=raw_test, cal_prob=cal_test
    )[["date", "tour", "surface", "label", "raw_prob", "cal_prob"]]
    test_export["date"] = test_export["date"].astype(str)
    test_export.to_json(MODEL_DIR / "test_predictions.json", orient="records")

    report = TrainReport(
        version=version,
        rows_train=len(train_df),
        rows_test=len(test_df),
        auc=float(roc_auc_score(y_test, cal_test)),
        log_loss=float(log_loss(y_test, np.clip(cal_test, 1e-6, 1 - 1e-6))),
        brier=float(brier_score_loss(y_test, cal_test)),
        feature_importance={f: int(i) for f, i in zip(FEATURES, booster.feature_importances_)},
    )

    with open(MODEL_DIR / f"model_{version}.pkl", "wb") as f:
        pickle.dump({"booster": booster, "calibrator": iso, "features": FEATURES, "version": version}, f)
    with open(MODEL_DIR / "latest.json", "w") as f:
        json.dump({"version": version, **asdict(report)}, f, indent=2)
    with open(MODEL_DIR / "latest.pkl", "wb") as f:
        pickle.dump({"booster": booster, "calibrator": iso, "features": FEATURES, "version": version}, f)

    return report


def load_latest():
    path = MODEL_DIR / "latest.pkl"
    if not path.exists():
        raise FileNotFoundError(f"No trained model at {path}. Run `breakpoint train` first.")
    with open(path, "rb") as f:
        return pickle.load(f)


def predict_proba(features_df: pd.DataFrame, model: dict | None = None) -> np.ndarray:
    model = model or load_latest()
    raw = model["booster"].predict_proba(features_df[model["features"]])[:, 1]
    return model["calibrator"].predict(raw)
