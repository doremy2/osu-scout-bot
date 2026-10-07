import Link from "next/link";
import type { ReactNode } from "react";
import { importsPolicy, scoutGet } from "@/lib/scout";
import type { TournamentListItem } from "@/lib/scoutTypes";
import { ImportButton } from "@/components/scout/ui";
import "../scout.css";

async function siteMeta(): Promise<string> {
  try {
    const t = await scoutGet<TournamentListItem[]>("/tournaments");
    const matches = t.reduce((n, x) => n + x.matches, 0);
    return `${t.length} tournaments · ${matches.toLocaleString("en-US")} matches`;
  } catch {
    return "osu! tournament analytics";
  }
}

export default async function ScoutLayout({ children }: { children: ReactNode }) {
  const [meta, imports] = await Promise.all([siteMeta(), importsPolicy()]);
  return (
    <div className="sc-root">
      <header className="sc-topbar">
        <div className="sc-wrap sc-head1">
          <Link href="/" className="sc-brand">osu!<b>scout</b><span className="sc-brand-dot" /><span className="sc-beta-tag">beta</span></Link>
          <span className="sc-head-meta">{meta}</span>
          <form className="sc-head-search" action="/tournaments" role="search">
            <input name="q" type="search" placeholder="Search tournaments…" aria-label="Search tournaments" />
          </form>
        </div>
        <div className="sc-wrap sc-head2">
          <nav className="sc-topnav" aria-label="Main">
            <Link href="/">Home</Link>
            <Link href="/tournaments">Tournaments</Link>
            <Link href="/methodology">Methodology</Link>
            <ImportButton policy={imports} variant="nav">Import</ImportButton>
          </nav>
        </div>
      </header>
      <main className="sc-wrap sc-main">{children}</main>
      <footer className="sc-footer sc-wrap">
        BETA · RATINGS ARE STILL BEING TUNED{imports === "off" ? " · IMPORTS CURRENTLY RUN LOCALLY" : <> · <Link href="/import" style={{ color: "var(--sc-accent)" }}>ADD A TOURNAMENT</Link></>}<br />
        RATINGS ARE CALCULATED BY THE SCOUT ANALYTICS ENGINE FROM OSU! MULTIPLAYER LOBBY DATA · NOT AFFILIATED WITH OSU!
      </footer>
    </div>
  );
}
