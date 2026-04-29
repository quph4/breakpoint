// Fetch helpers — JSON files live at /breakpoint/data/*.json under the GitHub Pages base.
const BASE = `${import.meta.env.BASE_URL}data`;

export type Summary = {
  updated_at: string;
  starting_bankroll: number;
  bankroll: number;
  total_bets: number;
  open: number;
  won: number;
  lost: number;
  win_rate: number | null;
  roi: number | null;
  total_pnl: number;
};

export type Bet = {
  id: number;
  placed_at: string;
  match_date: string;
  pick: string;
  opponent: string;
  tour: "atp" | "wta";
  surface: string;
  tourney: string;
  stake: number;
  odds: number;
  model_p: number;
  edge: number;
  status: "open" | "won" | "lost" | "void";
  pnl: number | null;
};

export type Player = {
  id: number;
  name: string;
  tour: "atp" | "wta";
  country: string | null;
  hand: string | null;
  elo_overall: number | null;
  elo_hard: number | null;
  elo_clay: number | null;
  elo_grass: number | null;
  matches_played: number;
};

export type PnlPoint = { date: string; bankroll: number };

async function getJson<T>(name: string, fallback: T): Promise<T> {
  try {
    const r = await fetch(`${BASE}/${name}`, { cache: "no-store" });
    if (!r.ok) return fallback;
    return (await r.json()) as T;
  } catch {
    return fallback;
  }
}

export const fetchSummary = () =>
  getJson<Summary>("summary.json", {
    updated_at: new Date().toISOString(),
    starting_bankroll: 1000,
    bankroll: 1000,
    total_bets: 0, open: 0, won: 0, lost: 0,
    win_rate: null, roi: null, total_pnl: 0,
  });

export const fetchOpenBets = () => getJson<Bet[]>("open_bets.json", []);
export const fetchSettledBets = () => getJson<Bet[]>("settled_bets.json", []);
export const fetchPlayers = () => getJson<Player[]>("players.json", []);
export const fetchPnlCurve = () => getJson<PnlPoint[]>("pnl_curve.json", []);
