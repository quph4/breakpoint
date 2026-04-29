"""SQLAlchemy schema. Single SQLite file; one row per match, ratings recomputed nightly."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from sqlalchemy import (
    Column, Date, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint, create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "breakpoint.db"


class Base(DeclarativeBase):
    pass


class Player(Base):
    __tablename__ = "players"
    id = Column(Integer, primary_key=True)  # Sackmann player_id
    name = Column(String, nullable=False, index=True)
    tour = Column(String, nullable=False)  # 'atp' | 'wta'
    country = Column(String)
    hand = Column(String)  # R / L / U
    height_cm = Column(Integer)
    dob = Column(Date)


class Match(Base):
    __tablename__ = "matches"
    id = Column(Integer, primary_key=True, autoincrement=True)
    tour = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    tourney_name = Column(String)
    tourney_level = Column(String)  # G, M, A, F, D, C, S, etc.
    surface = Column(String, index=True)  # Hard, Clay, Grass, Carpet
    round = Column(String)
    best_of = Column(Integer)
    winner_id = Column(Integer, ForeignKey("players.id"), index=True)
    loser_id = Column(Integer, ForeignKey("players.id"), index=True)
    score = Column(String)
    minutes = Column(Integer)
    # Serve stats — winner
    w_ace = Column(Integer); w_df = Column(Integer); w_svpt = Column(Integer)
    w_1stIn = Column(Integer); w_1stWon = Column(Integer); w_2ndWon = Column(Integer)
    w_SvGms = Column(Integer); w_bpSaved = Column(Integer); w_bpFaced = Column(Integer)
    # Serve stats — loser
    l_ace = Column(Integer); l_df = Column(Integer); l_svpt = Column(Integer)
    l_1stIn = Column(Integer); l_1stWon = Column(Integer); l_2ndWon = Column(Integer)
    l_SvGms = Column(Integer); l_bpSaved = Column(Integer); l_bpFaced = Column(Integer)
    winner_rank = Column(Integer); loser_rank = Column(Integer)

    __table_args__ = (
        UniqueConstraint("tour", "date", "winner_id", "loser_id", "tourney_name", name="uq_match"),
    )


class Rating(Base):
    """Snapshot of Elo per player per date. One row per player per match-day they played."""
    __tablename__ = "ratings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("players.id"), index=True)
    date = Column(Date, nullable=False, index=True)
    elo_overall = Column(Float)
    elo_hard = Column(Float)
    elo_clay = Column(Float)
    elo_grass = Column(Float)
    matches_played = Column(Integer, default=0)


class Ranking(Base):
    """Weekly singles ranking snapshot. Source: Sackmann atp_rankings_*.csv / wta_rankings_*.csv."""
    __tablename__ = "rankings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("players.id"), index=True)
    tour = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    rank = Column(Integer)
    points = Column(Integer)
    __table_args__ = (UniqueConstraint("player_id", "date", name="uq_ranking_pid_date"),)


class Fixture(Base):
    """Upcoming match — pulled from Sofascore, enriched with odds, fed to the model."""
    __tablename__ = "fixtures"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String, nullable=False)  # 'sofascore'
    source_id = Column(String, nullable=False, index=True)
    tour = Column(String)  # 'atp' | 'wta' | None when ambiguous
    date = Column(Date, index=True)
    start_ts = Column(DateTime)
    tourney_name = Column(String)
    round = Column(String)
    surface = Column(String)
    indoor = Column(Integer)  # 0/1
    player_a_name = Column(String, nullable=False)
    player_b_name = Column(String, nullable=False)
    player_a_id = Column(Integer, ForeignKey("players.id"))  # resolved
    player_b_id = Column(Integer, ForeignKey("players.id"))
    odds_a = Column(Float)
    odds_b = Column(Float)
    odds_book = Column(String)  # which book the odds came from
    odds_fetched_at = Column(DateTime)
    status = Column(String, default="scheduled", index=True)  # scheduled | predicted | bet_placed | finished | skipped
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("source", "source_id", name="uq_fixture_source"),)


class Odds(Base):
    """Closing odds joined from tennis-data.co.uk. Decimal format."""
    __tablename__ = "odds"
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(Integer, ForeignKey("matches.id"), unique=True, index=True)
    b365_w = Column(Float); b365_l = Column(Float)
    ps_w = Column(Float); ps_l = Column(Float)  # Pinnacle
    avg_w = Column(Float); avg_l = Column(Float)


class Prediction(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    model_version = Column(String, nullable=False)
    player_a_id = Column(Integer, ForeignKey("players.id"))
    player_b_id = Column(Integer, ForeignKey("players.id"))
    match_date = Column(Date, index=True)
    surface = Column(String)
    tourney_name = Column(String)
    p_a_wins = Column(Float, nullable=False)
    odds_a = Column(Float)  # market decimal odds at prediction time
    odds_b = Column(Float)
    edge_a = Column(Float)  # p_a_wins * odds_a - 1
    edge_b = Column(Float)


class Bet(Base):
    """Fake-money ledger. One row per placed bet."""
    __tablename__ = "bets"
    id = Column(Integer, primary_key=True, autoincrement=True)
    placed_at = Column(DateTime, default=datetime.utcnow)
    prediction_id = Column(Integer, ForeignKey("predictions.id"))
    match_date = Column(Date, index=True)
    pick_player_id = Column(Integer, ForeignKey("players.id"))
    opponent_id = Column(Integer, ForeignKey("players.id"))
    surface = Column(String)
    tourney_name = Column(String)
    stake = Column(Float, nullable=False)
    odds = Column(Float, nullable=False)
    model_p = Column(Float, nullable=False)
    edge = Column(Float, nullable=False)
    status = Column(String, default="open", index=True)  # open | won | lost | void
    pnl = Column(Float)  # +stake*(odds-1) on win, -stake on loss, 0 on void
    settled_at = Column(DateTime)
    rationale = Column(String)  # JSON-encoded list of short reason strings


def get_engine(path: Path | str | None = None):
    p = Path(path) if path else DB_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{p}", future=True)


def init_db(engine=None):
    engine = engine or get_engine()
    Base.metadata.create_all(engine)
    return engine


def session(engine=None) -> Session:
    return Session(engine or get_engine(), future=True)
