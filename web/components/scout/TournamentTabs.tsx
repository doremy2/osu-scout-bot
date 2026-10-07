"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { tournamentHref } from "@/lib/scoutFormat";

export function TournamentTabs({ slug, hasTeams }: { slug: string; hasTeams: boolean }) {
  const pathname = usePathname();
  const base = tournamentHref(slug);
  const tabs = [
    { label: "Overview", href: base, exact: true },
    { label: "Players", href: `${base}/players` },
    ...(hasTeams ? [{ label: "Teams", href: `${base}/teams` }] : []),
    { label: "Leaderboards", href: `${base}/leaderboards` },
    { label: "Matches", href: `${base}/matches` },
    { label: "Awards", href: `${base}/awards` }
  ];
  return (
    <nav className="sc-tabs" aria-label="Tournament sections">
      {tabs.map((t) => {
        const on = t.exact ? pathname === t.href : pathname === t.href || pathname.startsWith(`${t.href}/`);
        return <Link key={t.href} href={t.href} className={on ? "on" : ""} aria-current={on ? "page" : undefined}>{t.label}</Link>;
      })}
    </nav>
  );
}
