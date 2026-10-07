"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { tournamentHref } from "@/lib/scoutFormat";

export function TournamentTabs({ slug, hasTeams }: { slug: string; hasTeams: boolean }) {
  const pathname = usePathname();
  const base = tournamentHref(slug);
  const tabs: { label: string; href: string; exact?: boolean; also?: string }[] = [
    { label: "Overview", href: base, exact: true },
    ...(hasTeams ? [{ label: "Teams", href: `${base}/teams` }] : []),
    { label: "Leaderboards", href: `${base}/leaderboards`, also: `${base}/players` },
    { label: "Matches", href: `${base}/matches` },
    { label: "Draft", href: `${base}/draft` },
    { label: "Awards", href: `${base}/awards` },
    { label: "Method", href: `/methodology?t=${encodeURIComponent(slug)}` }
  ];
  return (
    <nav className="sc-tabs" aria-label="Tournament sections">
      {tabs.map((t) => {
        const hit = (h: string) => pathname === h || pathname.startsWith(`${h}/`);
        const on = t.exact ? pathname === t.href : hit(t.href) || (t.also ? hit(t.also) : false);
        return <Link key={t.href} href={t.href} className={on ? "on" : ""} aria-current={on ? "page" : undefined}>{t.label}</Link>;
      })}
    </nav>
  );
}
