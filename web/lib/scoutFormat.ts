// Presentation helpers only. No rating maths lives in the frontend.

export const MOD_ORDER = ["NM", "HD", "HR", "DT", "EZ", "FL", "HT", "FM", "LM", "TB"];

/** Colour tier for a rating. The rating scale is fixed by the backend (7.0 = field average,
 *  +1.5 per standard deviation), so these are absolute and not tied to any tournament. */
export function ratingTier(rating: number): "legend" | "elite" | "strong" | "solid" | "average" | "low" {
  if (rating >= 9.5) return "legend";
  if (rating >= 8.5) return "elite";
  if (rating >= 7.75) return "strong";
  if (rating >= 7.0) return "solid";
  if (rating >= 6.0) return "average";
  return "low";
}

export function fmtRating(rating: number | null | undefined): string {
  return rating == null ? "–" : rating.toFixed(2);
}

export function fmtInt(value: number | null | undefined): string {
  return value == null ? "–" : value.toLocaleString("en-US");
}

export function fmtPct(value: number | null | undefined, digits = 2): string {
  return value == null ? "–" : `${(value * 100).toFixed(digits)}%`;
}

export function fmtDate(value: string | null | undefined): string {
  if (!value) return "";
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? value : d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
}

export function fmtRecord(rec: [number, number] | null | undefined): string {
  return rec ? `${rec[0]}-${rec[1]}` : "–";
}

export function flagSrc(country: string | null | undefined): string | null {
  return country ? `https://flagcdn.com/w40/${country.toLowerCase()}.png` : null;
}

export function coverSrc(beatmapsetId: number | null | undefined): string | null {
  return beatmapsetId ? `https://assets.ppy.sh/beatmaps/${beatmapsetId}/covers/list.jpg` : null;
}

export function tournamentHref(slug: string, ...rest: string[]): string {
  return ["/tournaments", encodeURIComponent(slug), ...rest.map(encodeURIComponent)].join("/");
}
