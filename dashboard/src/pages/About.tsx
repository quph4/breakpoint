export default function About() {
  return (
    <div className="prose max-w-none space-y-6">
      <h1 className="text-4xl">About breakpoint.</h1>

      <div className="card space-y-3">
        <h2 className="text-2xl">What it is</h2>
        <p className="text-ink/80">
          A fake-money tennis betting bot. A LightGBM model — calibrated with isotonic regression —
          predicts win probability for ATP and WTA matches, compares against bookmaker odds, and
          places paper bets when it finds an edge.
        </p>
      </div>

      <div className="card space-y-3">
        <h2 className="text-2xl">The data</h2>
        <ul className="list-disc list-inside text-ink/80 space-y-1">
          <li><a className="underline" href="https://github.com/JeffSackmann/tennis_atp">Jeff Sackmann's ATP/WTA repos</a> — match history back to 1968, weekly updates.</li>
          <li><a className="underline" href="http://www.tennis-data.co.uk/alldata.php">tennis-data.co.uk</a> — closing odds (Bet365, Pinnacle) for backtesting.</li>
          <li>Live odds via <a className="underline" href="https://the-odds-api.com">The Odds API</a> free tier.</li>
        </ul>
      </div>

      <div className="card space-y-3">
        <h2 className="text-2xl">The model features</h2>
        <ul className="list-disc list-inside text-ink/80 space-y-1">
          <li>Surface-aware Elo (overall + Hard/Clay/Grass)</li>
          <li>Recent form (last 10 matches, overall + on this surface)</li>
          <li>Head-to-head record</li>
          <li>Days since last match (fatigue proxy)</li>
          <li>Career match volume</li>
        </ul>
      </div>

      <div className="card space-y-3">
        <h2 className="text-2xl">Sizing</h2>
        <p className="text-ink/80">
          Quarter-Kelly stake capped at 5% of bankroll. Bets only placed when calibrated edge
          exceeds 3%. Starting bankroll: $1000 fake.
        </p>
      </div>

      <p className="text-xs text-ink/40 italic">
        Sackmann data is CC-BY-NC. This project is non-commercial. No actual money changes hands.
      </p>
    </div>
  );
}
