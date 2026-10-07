"use client";

export default function ScoutError({ error, reset }: { error: Error; reset: () => void }) {
  return (
    <div className="sc-card sc-error-card">
      <h1>Couldn’t load this page</h1>
      <p>
        The scouting site gets its data from a local Python service. Make sure it is running:
      </p>
      <pre className="sc-code">python -m scout serve</pre>
      <p className="sc-dim">{error.message}</p>
      <button className="sc-btn" onClick={reset}>Try again</button>
    </div>
  );
}
