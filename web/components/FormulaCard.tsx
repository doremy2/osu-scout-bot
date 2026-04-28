export function FormulaCard() {
  return (
    <aside className="info-stack" aria-label="Ranking explanation">
      <section className="panel side-card">
        <p className="eyebrow">Overview</p>
        <h2>What are osu! Tournament Power Rankings?</h2>
        <p>
          This project helps players, captains, and analysts understand trends
          in competitive play. It does not define absolute skill.
        </p>
        <p className="disclaimer">
          Rankings are based on recent tournament results, strength of
          opponents, and activity. They reflect current form, not permanent
          skill.
        </p>
      </section>

      <section className="panel side-card">
        <p className="eyebrow">Performance Factors</p>
        <ul className="factor-list">
          <li>Recent tournament results</li>
          <li>Strength of opponents</li>
          <li>Tournament level</li>
          <li>Consistency</li>
          <li>Activity</li>
        </ul>
      </section>

      <section className="panel side-card">
        <p className="eyebrow">Tier Distribution</p>
        <div className="formula-list">
          <div className="formula-row">
            <span>Tier 1</span>
            <span className="tier-text tier-text-1">Top 5%</span>
          </div>
          <div className="formula-row">
            <span>Tier 2</span>
            <span className="tier-text tier-text-2">Next 15%</span>
          </div>
          <div className="formula-row">
            <span>Tier 3</span>
            <span className="tier-text tier-text-3">Remaining 80%</span>
          </div>
        </div>
      </section>

      <section className="panel side-card">
        <p className="eyebrow">Methodology Note</p>
        <p>
          No single number can fully represent a player. osu! performance
          varies by map pool, team context, role, teammate synergy, and
          playstyle. Use this as a scouting reference, not a final judgment.
        </p>
      </section>
    </aside>
  );
}
