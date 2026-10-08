import type { ReactNode } from "react";
import { PlayerSearch } from "@/components/scout/PlayerSearch";
import { TournamentTabs } from "@/components/scout/TournamentTabs";
import { scoutGet } from "@/lib/scout";
import { fmtDate, fmtInt } from "@/lib/scoutFormat";
import type { Overview } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

export async function generateMetadata({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const o = await scoutGet<Overview>(`/tournaments/${encodeURIComponent(slug)}`);
  return { title: o.tournament.name };
}

export default async function TournamentLayout({ children, params }: { children: ReactNode; params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const { tournament: t, summary: s } = await scoutGet<Overview>(`/tournaments/${encodeURIComponent(slug)}`);
  return (
    <>
      <section className="sc-thead">
        <div>
          <div className="sc-thead-tags">
            <span className="sc-tag">{t.acronym || t.slug}</span>
            <span className={`sc-tag sc-tag-${t.format}`}>{t.format_label}</span>
            {t.client === "lazer" && <span className="sc-tag sc-tag-lazer">Lazer</span>}
          </div>
          <h1>{t.name}</h1>
          <p className="sc-thead-stats">
            <span><b>{fmtInt(s.matches)}</b>MATCHES</span>
            {t.has_teams && <span><b>{fmtInt(s.teams)}</b>TEAMS</span>}
            <span><b>{fmtInt(s.players)}</b>PLAYERS</span>
            <span><b>{fmtInt(s.games)}</b>MAPS</span>
            {t.start_date && <span>{fmtDate(t.start_date)}{t.end_date && t.end_date !== t.start_date ? ` – ${fmtDate(t.end_date)}` : ""}</span>}
          </p>
        </div>
        <PlayerSearch slug={t.slug} />
      </section>
      <TournamentTabs slug={t.slug} hasTeams={t.has_teams} />
      <div className="sc-tcontent">{children}</div>
    </>
  );
}
