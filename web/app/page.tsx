import { LeaderboardShell } from "@/components/LeaderboardShell";
import { fetchLeaderboard } from "@/lib/api";
import type { LeaderboardRow, Tier } from "@/lib/types";

export const dynamic = "force-dynamic";

type PageProps = {
  searchParams: Promise<{
    tier?: string;
    country?: string;
    limit?: string;
    year?: string;
  }>;
};

function countryName(countryCode: string | null): string {
  if (!countryCode) return "Unknown country";
  try {
    return new Intl.DisplayNames(["en"], { type: "region" }).of(countryCode.toUpperCase()) || countryCode;
  } catch {
    return countryCode.toUpperCase();
  }
}

function cleanTier(value: string | undefined): Tier | "" {
  return value === "Tier 1" || value === "Tier 2" || value === "Tier 3" ? value : "";
}

function cleanCountry(value: string | undefined): string {
  return value ? value.slice(0, 2).toUpperCase() : "";
}

function cleanLimit(value: string | undefined): number {
  const parsed = Number(value);
  return [20, 50, 100, 250].includes(parsed) ? parsed : 100;
}

function cleanYear(value: string | undefined): string {
  return value === "2026" || value === "2025" || value === "2024" ? value : "2026";
}

function cleanExplanation(value: string | null | undefined): string {
  return (value || "")
    .replace(/,?\s*provisional\.?/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function publicWebsiteRow(row: LeaderboardRow): LeaderboardRow {
  return {
    ...row,
    explanation: cleanExplanation(row.explanation)
  };
}

export default async function Home({ searchParams }: PageProps) {
  const params = await searchParams;
  const tier = cleanTier(params.tier);
  const country = cleanCountry(params.country);
  const limit = cleanLimit(params.limit);
  const year = cleanYear(params.year);

  let initialRows: LeaderboardRow[] = [];
  let allRows: LeaderboardRow[] = [];
  try {
    [initialRows, allRows] = await Promise.all([
      fetchLeaderboard({ limit, tier: tier || undefined, country: country || undefined }),
      fetchLeaderboard({ limit: 10000 })
    ]);
  } catch {
    // Backend not reachable — render empty shell so the page still loads
  }
  const websiteInitialRows = initialRows.map(publicWebsiteRow);
  const websiteAllRows = allRows.map(publicWebsiteRow);
  const countriesByCode = new Map<string, string>();
  for (const row of websiteAllRows) {
    if (!row.country_code) continue;
    const code = row.country_code.toUpperCase();
    countriesByCode.set(code, countryName(code));
  }
  const countryOptions = Array.from(countriesByCode, ([code, name]) => ({ code, name })).sort((a, b) =>
    a.name.localeCompare(b.name)
  );

  return (
    <LeaderboardShell
      initialRows={websiteInitialRows}
      initialAllRows={websiteAllRows}
      initialCountryOptions={countryOptions}
      initialTier={tier}
      initialCountry={country}
      initialLimit={limit}
      selectedYear={year}
    />
  );
}
