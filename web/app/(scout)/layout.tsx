import Link from "next/link";
import type { ReactNode } from "react";
import "../scout.css";

export default function ScoutLayout({ children }: { children: ReactNode }) {
  return (
    <div className="sc-root">
      <header className="sc-topbar">
        <div className="sc-wrap sc-topbar-in">
          <Link href="/" className="sc-brand"><span className="sc-brand-dot" />osu! <b>scout</b></Link>
          <nav className="sc-topnav" aria-label="Main">
            <Link href="/tournaments">Tournaments</Link>
            <Link href="/legacy" className="sc-hide-sm">OWC rankings</Link>
            <Link href="/import" className="sc-btn sc-btn-primary sc-btn-sm">Import tournament</Link>
          </nav>
        </div>
      </header>
      <main className="sc-wrap sc-main">{children}</main>
      <footer className="sc-footer sc-wrap">
        Ratings are calculated by the scout analytics engine from osu! multiplayer lobby data. Not affiliated with osu!.
      </footer>
    </div>
  );
}
