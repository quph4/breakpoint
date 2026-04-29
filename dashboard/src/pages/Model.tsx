import { useEffect, useState } from "react";
import {
  Bar, BarChart, CartesianGrid, ReferenceLine,
  ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis, ZAxis,
} from "recharts";
import { fetchAudit, type Audit } from "../lib/data";

const fmt = (n: number | null, d = 4) => (n == null ? "—" : n.toFixed(d));

function MetricCard({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="card">
      <div className="stat-label">{label}</div>
      <div className="stat">{value}</div>
      {hint && <div className="text-xs text-ink/50 mt-1">{hint}</div>}
    </div>
  );
}

export default function Model() {
  const [a, setA] = useState<Audit | null>(null);

  useEffect(() => { fetchAudit().then(setA); }, []);

  if (a === null) {
    return (
      <div className="card">
        <h1 className="text-2xl mb-2">Model audit</h1>
        <p className="text-ink/50 italic">
          No audit yet. Run <code className="font-mono bg-ink/5 px-1.5 py-0.5 rounded">breakpoint train</code>{" "}
          then <code className="font-mono bg-ink/5 px-1.5 py-0.5 rounded">breakpoint audit</code>.
        </p>
      </div>
    );
  }

  // Reliability scatter — predicted (x) vs actual (y), with ideal y=x.
  const reliabilityData = a.reliability_calibrated
    .filter((b) => b.mean_predicted != null && b.actual_rate != null && b.n > 0)
    .map((b) => ({
      predicted: b.mean_predicted,
      actual: b.actual_rate,
      n: b.n,
    }));

  const biasColor =
    a.bias.interpretation === "well-calibrated" ? "text-court" :
    a.bias.interpretation === "overconfident" ? "text-clay" : "text-ace";

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-4xl mb-2">The model.</h1>
        <p className="text-sm text-ink/60">
          Held-out test set: <span className="font-mono">{a.n_test.toLocaleString()}</span> matches
          from <span className="font-mono">{a.date_min}</span> to <span className="font-mono">{a.date_max}</span>.
        </p>
      </div>

      <section className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard
          label="AUC"
          value={fmt(a.overall.calibrated.auc)}
          hint="ranking accuracy"
        />
        <MetricCard
          label="Brier"
          value={fmt(a.overall.calibrated.brier)}
          hint="lower is better"
        />
        <MetricCard
          label="Log loss"
          value={fmt(a.overall.calibrated.log_loss)}
        />
        <div className="card">
          <div className="stat-label">Calibration bias</div>
          <div className={`stat ${biasColor}`}>
            {a.bias.weighted_gap > 0 ? "+" : ""}{(a.bias.weighted_gap * 100).toFixed(2)}%
          </div>
          <div className="text-xs text-ink/60 mt-1">{a.bias.interpretation}</div>
        </div>
      </section>

      <section className="card">
        <h2 className="text-2xl mb-1">Reliability diagram</h2>
        <p className="text-sm text-ink/60 mb-4">
          Each bucket plots the model's mean predicted probability against the actual win rate.
          On the diagonal = perfect calibration. Above = underconfident. Below = overconfident.
        </p>
        <div className="h-80">
          <ResponsiveContainer>
            <ScatterChart margin={{ top: 10, right: 20, bottom: 30, left: 10 }}>
              <CartesianGrid stroke="#0e0f1210" />
              <XAxis
                type="number" dataKey="predicted" domain={[0, 1]}
                tickFormatter={(v) => (v * 100).toFixed(0) + "%"}
                label={{ value: "Mean predicted probability", position: "insideBottom", offset: -10, fontSize: 12 }}
                stroke="#0e0f1255"
              />
              <YAxis
                type="number" dataKey="actual" domain={[0, 1]}
                tickFormatter={(v) => (v * 100).toFixed(0) + "%"}
                label={{ value: "Actual win rate", angle: -90, position: "insideLeft", fontSize: 12 }}
                stroke="#0e0f1255"
              />
              <ZAxis type="number" dataKey="n" range={[40, 400]} />
              <Tooltip
                formatter={(v: number, name: string) => {
                  if (name === "predicted" || name === "actual") return (v * 100).toFixed(1) + "%";
                  return v;
                }}
              />
              <ReferenceLine
                segment={[{ x: 0, y: 0 }, { x: 1, y: 1 }]}
                stroke="#0e0f1255" strokeDasharray="4 4"
              />
              <Scatter data={reliabilityData} fill="#3d6b4e" />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="card">
          <h2 className="text-2xl mb-3">Performance by surface</h2>
          <table className="w-full text-sm">
            <thead className="text-left text-xs uppercase tracking-wider text-ink/50">
              <tr>
                <th className="py-2 pr-4">Surface</th>
                <th className="py-2 pr-4 text-right">N</th>
                <th className="py-2 pr-4 text-right">AUC</th>
                <th className="py-2 pr-4 text-right">Brier</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(a.by_surface).map(([surf, m]) => (
                <tr key={surf} className="border-b border-ink/5">
                  <td className="py-2 pr-4 font-medium">{surf}</td>
                  <td className="py-2 pr-4 text-right tabular-nums">{m.n.toLocaleString()}</td>
                  <td className="py-2 pr-4 text-right tabular-nums">{fmt(m.auc)}</td>
                  <td className="py-2 pr-4 text-right tabular-nums">{fmt(m.brier)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="card">
          <h2 className="text-2xl mb-3">Performance by tour</h2>
          <table className="w-full text-sm">
            <thead className="text-left text-xs uppercase tracking-wider text-ink/50">
              <tr>
                <th className="py-2 pr-4">Tour</th>
                <th className="py-2 pr-4 text-right">N</th>
                <th className="py-2 pr-4 text-right">AUC</th>
                <th className="py-2 pr-4 text-right">Brier</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(a.by_tour).map(([tour, m]) => (
                <tr key={tour} className="border-b border-ink/5">
                  <td className="py-2 pr-4 font-medium uppercase">{tour}</td>
                  <td className="py-2 pr-4 text-right tabular-nums">{m.n.toLocaleString()}</td>
                  <td className="py-2 pr-4 text-right tabular-nums">{fmt(m.auc)}</td>
                  <td className="py-2 pr-4 text-right tabular-nums">{fmt(m.brier)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="card">
        <h2 className="text-2xl mb-1">Confidence histogram</h2>
        <p className="text-sm text-ink/60 mb-4">
          How often the model picks each confidence range (max(p, 1−p) on the favored side).
        </p>
        <div className="h-56">
          <ResponsiveContainer>
            <BarChart data={a.confidence_histogram.map((b) => ({
              label: `${(b.lo * 100).toFixed(0)}–${(b.hi * 100).toFixed(0)}%`,
              n: b.n,
            }))}>
              <CartesianGrid stroke="#0e0f1210" />
              <XAxis dataKey="label" tick={{ fontSize: 11 }} stroke="#0e0f1255" />
              <YAxis tick={{ fontSize: 11 }} stroke="#0e0f1255" />
              <Tooltip />
              <Bar dataKey="n" fill="#b25c2c" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </section>
    </div>
  );
}
