# breakpoint

ML-driven fake-money tennis betting bot. Trains a calibrated LightGBM model on
[Jeff Sackmann's ATP/WTA match archive](https://github.com/JeffSackmann/tennis_atp),
hunts for value vs market odds, places paper bets, and renders the running ledger
on a static dashboard at [quph4.github.io/breakpoint](https://quph4.github.io/breakpoint).

No real money. Real model, real backtest, real ego damage when the model is wrong.

## How it works

```
┌─ GitHub Actions (cron, every 6h) ──────────────────────────────┐
│  ingest → elo → train → predict → settle → export JSON         │
│         │                                                       │
│         ▼                                                       │
│   data/breakpoint.db (cached between runs)                      │
│         │                                                       │
│         ▼                                                       │
│   dashboard/public/data/*.json  ── committed to repo            │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
                        GitHub Pages → quph4.github.io/breakpoint
```

## Local quickstart

```bash
# Backend
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -e .

breakpoint init-db
breakpoint ingest --start-year 2015          # ~10 min, pulls Sackmann CSVs
breakpoint elo                               # surface-aware Elo for every player
breakpoint train --min-year 2015             # LightGBM + isotonic calibration
breakpoint settle                            # mark any open bets vs results
breakpoint export                            # write JSON for the dashboard
breakpoint status                            # current bankroll + P&L

# Dashboard
cd dashboard
npm install
npm run dev
```

## Project layout

```
breakpoint/
├── breakpoint/              Python package
│   ├── db.py                SQLAlchemy schema
│   ├── ingest/sackmann.py   pull ATP/WTA CSVs from GitHub
│   ├── features/
│   │   ├── elo.py           surface-aware Elo (overall + Hard/Clay/Grass)
│   │   └── build.py         build training rows from match history
│   ├── models/baseline.py   LightGBM + isotonic calibration
│   ├── betting/ledger.py    fake-money ledger + Kelly sizing
│   ├── export.py            DB → JSON for the dashboard
│   └── cli.py               `breakpoint <command>`
├── dashboard/               Vite + React + Tailwind + Recharts
├── data/                    SQLite + trained models (gitignored)
└── .github/workflows/       cron runner + Pages deploy
```

## Model

LightGBM binary classifier on these features (all symmetric A−B differences):

- `elo_diff` — overall Elo
- `elo_surf_diff` — Elo on this surface
- `form10_diff` — last-10 win rate
- `surf_form_diff` — last-10 win rate on this surface
- `rest_diff` — days since last match
- `h2h_diff` — historical head-to-head balance
- `matches_played_diff` — career match volume

Isotonic calibration on a held-out fold — raw GBM probabilities are
overconfident and unsafe to bet on directly.

## Sizing

Quarter-Kelly capped at 5% of bankroll. Bets only placed when calibrated edge
exceeds 3%. Starting bankroll: $1000 fake.

## Deploy

1. Push this repo to `github.com/quph4/breakpoint` (public).
2. Repo Settings → Pages → Source: **GitHub Actions**.
3. The first push triggers the workflow; subsequent runs hit the cron schedule.

## Data licensing

[Jeff Sackmann's match archives](https://github.com/JeffSackmann/tennis_atp) are
CC-BY-NC. This project is non-commercial. If that ever changes, the data has to
go too.
