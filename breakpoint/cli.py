"""Breakpoint CLI — `breakpoint <command>`."""
from __future__ import annotations

import logging
from datetime import date

import click

from . import __version__


@click.group()
@click.version_option(__version__)
@click.option("-v", "--verbose", is_flag=True, help="DEBUG logging")
def cli(verbose: bool):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@cli.command("init-db")
def init_db_cmd():
    """Create the SQLite schema."""
    from .db import init_db
    init_db()
    click.echo("DB initialized.")


@cli.command()
@click.option("--tours", default="atp,wta", help="comma-sep: atp,wta")
@click.option("--start-year", type=int, default=2005)
@click.option("--end-year", type=int, default=None)
def ingest(tours: str, start_year: int, end_year: int | None):
    """Pull Sackmann match CSVs into the DB."""
    from .ingest.sackmann import ingest_all
    ingest_all(tuple(t.strip() for t in tours.split(",")), start_year, end_year)


@cli.command()
@click.option("--tour", default=None, help="atp | wta | both (default both)")
def elo(tour: str | None):
    """Recompute Elo ratings from match history."""
    from .features.elo import compute_all
    df = compute_all(tour=tour)
    click.echo(f"Computed Elo for {len(df)} players.")


@cli.command()
@click.option("--tour", default=None)
@click.option("--min-year", type=int, default=2005)
def train(tour: str | None, min_year: int):
    """Build features + train LightGBM model."""
    from .features.build import build_training_frame
    from .models.baseline import train as train_model

    df = build_training_frame(tour=tour, min_year=min_year)
    click.echo(f"Built {len(df)} training rows.")
    report = train_model(df)
    click.echo(
        f"Model {report.version}: AUC={report.auc:.4f} "
        f"LogLoss={report.log_loss:.4f} Brier={report.brier:.4f}"
    )


@cli.command("sync")
def sync_cmd():
    """Pull active tennis events + odds from The Odds API → Fixture table."""
    from .ingest.odds_api import sync_fixtures_and_odds
    r = sync_fixtures_and_odds()
    click.echo(
        f"Events seen: {r['events_seen']} | "
        f"Inserted: {r['fixtures_upserted']} | "
        f"Priced: {r['priced']} | "
        f"Unresolved names: {r['unresolved']}"
    )


@cli.command("predict")
def predict_cmd():
    """Predict + place bets on priced fixtures."""
    from .predict import run
    r = run()
    click.echo(f"Predictions: {r['predictions']} | Bets placed: {r['bets']} | Skipped: {r['skipped']}")


@cli.command("update-clv")
def update_clv_cmd():
    """Refresh closing-line snapshots on open bets."""
    from .clv import update_closing_lines
    n = update_closing_lines()
    click.echo(f"Refreshed closing odds on {n} open bets.")


@cli.command()
def settle():
    """Settle open bets against historical results."""
    from .betting.ledger import settle_bets
    n = settle_bets()
    click.echo(f"Settled {n} bets.")


@cli.command("void-duplicates")
def void_duplicates_cmd():
    """Void any duplicate open bets on the same matchup, keeping the oldest."""
    from .betting.ledger import void_duplicate_bets
    n = void_duplicate_bets()
    click.echo(f"Voided {n} duplicate bets.")


@cli.command("mark-bet")
@click.argument("bet_id", type=int)
@click.argument("outcome", type=click.Choice(["won", "lost", "void"]))
def mark_bet_cmd(bet_id: int, outcome: str):
    """Manually settle a bet by id. Use when automated settle can't (e.g. Sackmann lag)."""
    from datetime import datetime
    from .db import Bet, init_db, session
    engine = init_db()
    pnl: float | None = None
    with session(engine) as s:
        bet = s.get(Bet, bet_id)
        if not bet:
            click.echo(f"No bet with id={bet_id}"); return
        if bet.status != "open":
            click.echo(f"Bet {bet_id} already {bet.status}; refusing"); return
        if outcome == "won":
            pnl = round(bet.stake * (bet.odds - 1), 2)
        elif outcome == "lost":
            pnl = -bet.stake
        else:
            pnl = 0
        bet.pnl = pnl
        bet.status = outcome
        bet.settled_at = datetime.utcnow()
        s.commit()
    click.echo(f"Marked bet {bet_id} as {outcome} (pnl={pnl:+.2f})")


@cli.command()
def audit():
    """Build calibration audit JSON from the latest test split."""
    from .audit import write_audit
    r = write_audit()
    click.echo(
        f"n_test={r['n_test']} | "
        f"AUC={r['overall']['calibrated']['auc']:.4f} | "
        f"Brier={r['overall']['calibrated']['brier']:.4f} | "
        f"bias_gap={r['bias']['weighted_gap']:+.4f} ({r['bias']['interpretation']})"
    )


@cli.command("ingest-historical-odds")
@click.option("--start-year", type=int, default=2015)
@click.option("--end-year", type=int, default=None)
@click.option("--refresh", is_flag=True, default=False, help="Re-process all years, ignoring the already-ingested skip")
def ingest_historical_odds_cmd(start_year: int, end_year: int | None, refresh: bool):
    """Pull tennis-data.co.uk closing odds (ATP+WTA) into the Odds table."""
    from .ingest.tennisdata import ingest_all
    n = ingest_all(start_year=start_year, end_year=end_year, refresh=refresh)
    click.echo(f"Inserted {n} odds rows.")


@cli.command("train-stats")
def train_stats_cmd():
    """Bucket training data label rate by feature value to detect label-feature misalignment."""
    from .train_stats import write_stats
    r = write_stats()
    if "error" in r:
        click.echo(r["error"]); return
    click.echo(f"n_rows={r['n_rows']:,} | overall_label_rate={r['overall_label_rate']:.4f}")
    click.echo("by elo_diff:")
    for b in r["by_elo_diff"]:
        if b["n"] == 0: continue
        click.echo(f"  [{b['lo']}, {b['hi']}): n={b['n']:,} label_rate={b['label_rate']}")
    j = r["joint_extreme"]
    click.echo(f"all-features-negative: n={j['all_features_negative']['n']:,} label_rate={j['all_features_negative']['label_rate']}")
    click.echo(f"all-features-positive: n={j['all_features_positive']['n']:,} label_rate={j['all_features_positive']['label_rate']}")


@cli.command("audit-market")
def audit_market_cmd():
    """Compare model predictions to closing-odds market on the test set."""
    from .audit_market import write_market_audit
    r = write_market_audit()
    if r is None:
        click.echo("No market audit produced — ingest closing odds first.")
        return
    click.echo(
        f"n={r['n']} | model_brier={r['model']['brier']:.4f} | "
        f"market_brier={r['market']['brier']:.4f} | "
        f"overround={r['mean_overround']*100:.2f}%"
    )


@cli.command()
def export():
    """Write JSON snapshots into dashboard/public/data/."""
    from .export import export_all
    export_all()
    click.echo("Exported JSON to dashboard/public/data/.")


@cli.command()
def status():
    """Print current bankroll + bet counts."""
    from .export import export_summary
    s = export_summary()
    roi = "—" if s["roi"] is None else f"{s['roi'] * 100:+.2f}%"
    click.echo(
        f"Bankroll: ${s['bankroll']:.2f} | "
        f"Bets: {s['total_bets']} ({s['open']} open, {s['won']}W / {s['lost']}L) | "
        f"PnL: ${s['total_pnl']:+.2f} | "
        f"ROI: {roi}"
    )


if __name__ == "__main__":
    cli()
