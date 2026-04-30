import { useEffect, useState } from "react";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { fetchPnlCurve, fetchSummary, type PnlPoint, type Summary } from "../lib/data";

function fmtPct(v: number | null, sign = false) {
  if (v == null) return "—";
  const s = (v * 100).toFixed(2) + "%";
  return sign && v > 0 ? "+" + s : s;
}

function fmtMoney(v: number, sign = false) {
  const s = `$${Math.abs(v).toFixed(2)}`;
  return v < 0 ? "−" + s : sign && v > 0 ? "+" + s : s;
}

export default function Home() {
  const [s, setS] = useState<Summary | null>(null);
  const [curve, setCurve] = useState<PnlPoint[]>([]);

  useEffect(() => {
    fetchSummary().then(setS);
    fetchPnlCurve().then(setCurve);
  }, []);

  if (!s) return <div className="text-ink/50">Loading…</div>;

  const profitColor = s.total_pnl >= 0 ? "text-court" : "text-clay";
  const updated = new Date(s.updated_at).toLocaleString();

  return (
    <div className="space-y-8">
      <section>
        <h1 className="text-4xl mb-2">The ledger.</h1>
        <p className="text-ink/60 text-sm">
          Started with {fmtMoney(s.starting_bankroll)}. Updated {updated}.
        </p>
      </section>

      <section className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <div className="card">
          <div className="stat-label">Bankroll</div>
          <div className={`stat ${profitColor}`}>{fmtMoney(s.bankroll)}</div>
        </div>
        <div className="card">
          <div className="stat-label">Total P&amp;L</div>
          <div className={`stat ${profitColor}`}>{fmtMoney(s.total_pnl, true)}</div>
        </div>
        <div className="card">
          <div className="stat-label">ROI</div>
          <div className={`stat ${profitColor}`}>{fmtPct(s.roi, true)}</div>
        </div>
        <div className="card">
          <div className="stat-label">Win rate</div>
          <div className="stat">{fmtPct(s.win_rate)}</div>
        </div>
        <div className="card">
          <div className="stat-label">Avg CLV</div>
          <div className={`stat ${(s.avg_clv ?? 0) >= 0 ? "text-court" : "text-clay"}`}>
            {fmtPct(s.avg_clv ?? null, true)}
          </div>
          <div className="text-xs text-ink/50 mt-1">
            n={s.n_clv_bets ?? 0} settled bets w/ closing line
          </div>
        </div>
      </section>

      <section className="card">
        <div className="flex justify-between items-baseline mb-4">
          <h2 className="text-2xl">Bankroll curve</h2>
          <div className="text-sm text-ink/60">
            {s.total_bets} bets · {s.won}W / {s.lost}L · {s.open} open
          </div>
        </div>
        {curve.length > 1 ? (
          <div className="h-64">
            <ResponsiveContainer>
              <LineChart data={curve}>
                <XAxis dataKey="date" tick={{ fontSize: 11 }} stroke="#0e0f1255" />
                <YAxis domain={["auto", "auto"]} tick={{ fontSize: 11 }} stroke="#0e0f1255" />
                <Tooltip />
                <Line
                  type="monotone"
                  dataKey="bankroll"
                  stroke="#3d6b4e"
                  strokeWidth={2}
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <p className="text-ink/40 italic">No settled bets yet — curve renders once the bot starts running.</p>
        )}
      </section>
    </div>
  );
}
