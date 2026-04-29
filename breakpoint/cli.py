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


@cli.command()
def settle():
    """Settle open bets against historical results."""
    from .betting.ledger import settle_bets
    n = settle_bets()
    click.echo(f"Settled {n} bets.")


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
