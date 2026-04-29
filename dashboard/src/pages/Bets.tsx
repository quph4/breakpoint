import { useEffect, useMemo, useState } from "react";
import { fetchOpenBets, fetchSettledBets, type Bet } from "../lib/data";

const surfaceColor: Record<string, string> = {
  Hard: "bg-blue-100 text-blue-800 border-blue-200",
  Clay: "bg-orange-100 text-orange-900 border-orange-200",
  Grass: "bg-green-100 text-green-900 border-green-200",
};

function StatusPill({ b }: { b: Bet }) {
  if (b.status === "won") return <span className="pill bg-court/10 text-court border-court/30">won {b.pnl ? `+$${b.pnl.toFixed(2)}` : ""}</span>;
  if (b.status === "lost") return <span className="pill bg-clay/10 text-clay border-clay/30">lost −${b.stake.toFixed(2)}</span>;
  if (b.status === "void") return <span className="pill bg-ink/5 text-ink/60 border-ink/20">void</span>;
  return <span className="pill bg-ace/10 text-ink border-ace/40">open</span>;
}

function Row({ b }: { b: Bet }) {
  return (
    <tr className="border-b border-ink/5 hover:bg-ink/[0.02]">
      <td className="py-2 pr-4 text-sm text-ink/70">{b.match_date}</td>
      <td className="py-2 pr-4">
        <div className="font-medium">{b.pick} <span className="text-ink/40">vs</span> {b.opponent}</div>
        <div className="text-xs text-ink/50">{b.tourney} · {b.tour?.toUpperCase()}</div>
      </td>
      <td className="py-2 pr-4">
        <span className={`pill ${surfaceColor[b.surface] ?? "bg-ink/5"}`}>{b.surface}</span>
      </td>
      <td className="py-2 pr-4 text-right tabular-nums">{b.odds.toFixed(2)}</td>
      <td className="py-2 pr-4 text-right tabular-nums">{(b.model_p * 100).toFixed(1)}%</td>
      <td className="py-2 pr-4 text-right tabular-nums text-court">+{(b.edge * 100).toFixed(1)}%</td>
      <td className="py-2 pr-4 text-right tabular-nums">${b.stake.toFixed(2)}</td>
      <td className="py-2 pr-4"><StatusPill b={b} /></td>
    </tr>
  );
}

export default function Bets() {
  const [tab, setTab] = useState<"open" | "settled">("open");
  const [open, setOpen] = useState<Bet[]>([]);
  const [settled, setSettled] = useState<Bet[]>([]);

  useEffect(() => {
    fetchOpenBets().then(setOpen);
    fetchSettledBets().then(setSettled);
  }, []);

  const rows = tab === "open" ? open : settled;
  const totals = useMemo(() => {
    const staked = settled.reduce((a, b) => a + b.stake, 0);
    const pnl = settled.reduce((a, b) => a + (b.pnl ?? 0), 0);
    return { staked, pnl };
  }, [settled]);

  return (
    <div className="space-y-6">
      <div className="flex items-baseline justify-between">
        <h1 className="text-4xl">The bets.</h1>
        <div className="flex gap-1">
          <button
            className={`px-3 py-1.5 rounded-md text-sm ${tab === "open" ? "bg-ink text-paper" : "text-ink/70"}`}
            onClick={() => setTab("open")}
          >
            Open ({open.length})
          </button>
          <button
            className={`px-3 py-1.5 rounded-md text-sm ${tab === "settled" ? "bg-ink text-paper" : "text-ink/70"}`}
            onClick={() => setTab("settled")}
          >
            Settled ({settled.length})
          </button>
        </div>
      </div>

      {tab === "settled" && settled.length > 0 && (
        <div className="text-sm text-ink/60">
          Settled stakes: ${totals.staked.toFixed(2)} · Net P&amp;L:{" "}
          <span className={totals.pnl >= 0 ? "text-court" : "text-clay"}>
            {totals.pnl >= 0 ? "+" : "−"}${Math.abs(totals.pnl).toFixed(2)}
          </span>
        </div>
      )}

      <div className="card overflow-x-auto">
        {rows.length === 0 ? (
          <p className="text-ink/40 italic">Nothing here yet.</p>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-left text-xs uppercase tracking-wider text-ink/50">
              <tr>
                <th className="py-2 pr-4">Date</th>
                <th className="py-2 pr-4">Match</th>
                <th className="py-2 pr-4">Surface</th>
                <th className="py-2 pr-4 text-right">Odds</th>
                <th className="py-2 pr-4 text-right">Model P</th>
                <th className="py-2 pr-4 text-right">Edge</th>
                <th className="py-2 pr-4 text-right">Stake</th>
                <th className="py-2 pr-4">Status</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((b) => <Row key={b.id} b={b} />)}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
