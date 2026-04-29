# Scout intelligence pack

Snapshot gathered 2026-04-29 by seven parallel research agents. Used as the
human-facing context layer the model leans on when sizing fake bets.

## Files

| File | Beat | Confidence |
|---|---|---|
| `01_players.json` | Top-50 ATP + top-50 WTA player profiles | **Mixed** — demographics solid, all 2026 W-L numbers projected from cutoff |
| `02_schedule.json` | Tournaments April 29 – May 18 with seeds + withdrawals | **High** — Madrid and Rome 2026 confirmed via web sources |
| `03_surface_form.json` | Surface-split form for top 30 ATP/WTA | **Low** — all 2026 numbers projected; career percentages reliable |
| `04_h2h.json` | H2H records for relevant Madrid/Rome pairings | **Mixed** — top rivalries verified through April 2026, others estimated |
| `05_injuries.json` | Active injuries, withdrawals, fitness flags | **High for cited cases** — Alcaraz wrist, Swiatek virus, Rune Achilles all confirmed reporting |
| `06_odds.json` | Outright + match odds, line movement | **Mixed** — Bet365/BetMGM cited, Pinnacle figures model-derived |
| `07_conditions.json` | Court speed, altitude, weather, ball brands | **Mixed** — venue specs confirmed; weather rows are climatological norms, replace with live forecast before betting |

## Reliability notes

Every agent disclosed where its confidence drops. Inputs that say
"projected", "estimated", or "PROJECTED" in the JSON should not be treated
as fact — they're priors, not posteriors. The bot's pricing pipeline must
weight these accordingly when (eventually) wired in.

## Refresh policy

This pack is a one-time snapshot. The eventual live-data pipeline (Sofascore
fixtures + Odds API odds + a dedicated injury-news scraper) replaces these
files. Until then, treat anything in here as April-29-snapshot context only.
