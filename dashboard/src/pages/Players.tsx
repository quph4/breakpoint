import { useEffect, useMemo, useState } from "react";
import { fetchPlayers, type Player } from "../lib/data";

type Tour = "all" | "atp" | "wta";
type SortKey = "elo_overall" | "elo_hard" | "elo_clay" | "elo_grass";

export default function Players() {
  const [players, setPlayers] = useState<Player[]>([]);
  const [tour, setTour] = useState<Tour>("all");
  const [sort, setSort] = useState<SortKey>("elo_overall");
  const [q, setQ] = useState("");

  useEffect(() => { fetchPlayers().then(setPlayers); }, []);

  const filtered = useMemo(() => {
    return players
      .filter((p) => tour === "all" || p.tour === tour)
      .filter((p) => q === "" || p.name.toLowerCase().includes(q.toLowerCase()))
      .sort((a, b) => (b[sort] ?? 0) - (a[sort] ?? 0));
  }, [players, tour, sort, q]);

  return (
    <div className="space-y-6">
      <h1 className="text-4xl">The players.</h1>

      <div className="flex flex-wrap gap-3 items-center">
        <input
          type="text"
          placeholder="Search name…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          className="px-3 py-1.5 border border-ink/20 rounded-md bg-white text-sm focus:outline-none focus:border-ink/50"
        />
        <div className="flex gap-1">
          {(["all", "atp", "wta"] as Tour[]).map((t) => (
            <button
              key={t}
              onClick={() => setTour(t)}
              className={`px-3 py-1.5 rounded-md text-sm ${tour === t ? "bg-ink text-paper" : "text-ink/70"}`}
            >
              {t.toUpperCase()}
            </button>
          ))}
        </div>
        <select
          value={sort}
          onChange={(e) => setSort(e.target.value as SortKey)}
          className="px-3 py-1.5 border border-ink/20 rounded-md bg-white text-sm"
        >
          <option value="elo_overall">Sort: Overall Elo</option>
          <option value="elo_hard">Sort: Hard Elo</option>
          <option value="elo_clay">Sort: Clay Elo</option>
          <option value="elo_grass">Sort: Grass Elo</option>
        </select>
        <span className="text-sm text-ink/50 ml-auto">{filtered.length} shown</span>
      </div>

      <div className="card overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="text-left text-xs uppercase tracking-wider text-ink/50">
            <tr>
              <th className="py-2 pr-4 w-10">#</th>
              <th className="py-2 pr-4">Player</th>
              <th className="py-2 pr-4">Tour</th>
              <th className="py-2 pr-4">Country</th>
              <th className="py-2 pr-4 text-right">Overall</th>
              <th className="py-2 pr-4 text-right">Hard</th>
              <th className="py-2 pr-4 text-right">Clay</th>
              <th className="py-2 pr-4 text-right">Grass</th>
              <th className="py-2 pr-4 text-right">Matches</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((p, i) => (
              <tr key={p.id} className="border-b border-ink/5 hover:bg-ink/[0.02]">
                <td className="py-2 pr-4 text-ink/40 tabular-nums">{i + 1}</td>
                <td className="py-2 pr-4 font-medium">{p.name}</td>
                <td className="py-2 pr-4 text-xs uppercase tracking-wider text-ink/60">{p.tour}</td>
                <td className="py-2 pr-4 text-ink/70">{p.country ?? "—"}</td>
                <td className="py-2 pr-4 text-right tabular-nums font-semibold">{p.elo_overall?.toFixed(0) ?? "—"}</td>
                <td className="py-2 pr-4 text-right tabular-nums">{p.elo_hard?.toFixed(0) ?? "—"}</td>
                <td className="py-2 pr-4 text-right tabular-nums">{p.elo_clay?.toFixed(0) ?? "—"}</td>
                <td className="py-2 pr-4 text-right tabular-nums">{p.elo_grass?.toFixed(0) ?? "—"}</td>
                <td className="py-2 pr-4 text-right tabular-nums text-ink/60">{p.matches_played}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {filtered.length === 0 && (
          <p className="text-ink/40 italic mt-2">No players. Run <code className="font-mono">breakpoint ingest</code> then <code className="font-mono">breakpoint elo</code>.</p>
        )}
      </div>
    </div>
  );
}
