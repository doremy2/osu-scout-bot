import type { LeaderboardRow, PlayerPower, TournamentCatalog, TournamentEntry } from "./types";

const DEFAULT_BACKEND_API_BASE = "http://127.0.0.1:8000";
const DEFAULT_BROWSER_API_BASE = "/api";

export function apiBaseUrl(): string {
  const configuredBrowserBase = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (typeof window !== "undefined") {
    if (configuredBrowserBase) {
      const normalizedBase = configuredBrowserBase.replace(/\/$/, "");
      const isHttpsPage = window.location.protocol === "https:";
      const isLocalHttpBackend =
        normalizedBase.startsWith("http://localhost") ||
        normalizedBase.startsWith("http://127.0.0.1");
      if (!(isHttpsPage && isLocalHttpBackend)) return normalizedBase;
    }
    return DEFAULT_BROWSER_API_BASE;
  }

  const configuredServerBase = process.env.API_BASE_URL;
  if (configuredServerBase) return configuredServerBase.replace(/\/$/, "");
  return DEFAULT_BACKEND_API_BASE;
}

export type LeaderboardQuery = {
  tier?: string;
  country?: string;
  provisional?: boolean;
  limit?: number;
  offset?: number;
};

async function getJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${apiBaseUrl()}${path}`, {
    ...init,
    cache: "no-store"
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

export async function fetchLeaderboard(query: LeaderboardQuery = {}): Promise<LeaderboardRow[]> {
  const params = new URLSearchParams();
  if (query.tier) params.set("tier", query.tier);
  if (query.country) params.set("country", query.country);
  if (query.provisional !== undefined) params.set("provisional", String(query.provisional));
  params.set("limit", String(query.limit ?? 100));
  params.set("offset", String(query.offset ?? 0));
  const suffix = params.toString();
  return getJson<LeaderboardRow[]>(`/leaderboard${suffix ? `?${suffix}` : ""}`);
}

export async function fetchPlayerPower(username: string): Promise<PlayerPower> {
  return getJson<PlayerPower>(`/player/${encodeURIComponent(username)}/power`);
}

export type TournamentQuery = {
  year?: number;
  game_mode?: string;
  classification?: string;
  import_status?: string;
  limit?: number;
};

export async function fetchTournamentCatalog(query: TournamentQuery = {}): Promise<TournamentCatalog> {
  const params = new URLSearchParams();
  if (query.year) params.set("year", String(query.year));
  if (query.game_mode) params.set("game_mode", query.game_mode);
  if (query.classification) params.set("classification", query.classification);
  if (query.import_status) params.set("import_status", query.import_status);
  params.set("limit", String(query.limit ?? 2000));
  const suffix = params.toString();
  return getJson<TournamentCatalog>(`/tournaments/catalog${suffix ? `?${suffix}` : ""}`);
}

export async function fetchTournamentDetail(slug: string): Promise<TournamentEntry> {
  return getJson<TournamentEntry>(`/tournaments/catalog/${encodeURIComponent(slug)}`);
}
