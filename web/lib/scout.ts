import { notFound } from "next/navigation";

// Server-side calls go straight to the Python service; browser code uses the /api/scout proxy
// (app/api/scout/[...path]/route.ts). Neither ever sees the osu! credentials.
//
// SCOUT_API_URL is read at request time (never at build time): on Vercel it is the internal URL of the
// `scout_api` service binding, locally it defaults to the dev server.
export function scoutApiUrl(path: string): URL {
  const raw = process.env.SCOUT_API_URL || "http://127.0.0.1:8001";
  const base = raw.endsWith("/") ? raw : `${raw}/`;
  return new URL(`api${path.startsWith("/") ? path : `/${path}`}`, base);
}

/** Who may start imports: "open" (local), "public" (anyone, within limits), "token" (invite code) or "off" (read-only). */
export type ImportsPolicy = "open" | "public" | "token" | "off";

export async function importsPolicy(): Promise<ImportsPolicy> {
  try {
    const res = await fetch(scoutApiUrl("/health"), { cache: "no-store" });
    const body = await res.json();
    return ["off", "token", "public"].includes(body.imports) ? body.imports : "open";
  } catch {
    return "open";
  }
}

export class ScoutApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

/** Server components: fetch JSON from the scout service. 404 -> Next's notFound(). */
export async function scoutGet<T>(path: string): Promise<T> {
  let response: Response;
  try {
    response = await fetch(scoutApiUrl(path), { cache: "no-store" });
  } catch {
    throw new ScoutApiError(
      503,
      "Can't reach the scout API. Start it with:  python -m scout serve   (or  python -m scout dev)"
    );
  }
  if (response.status === 404) notFound();
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    throw new ScoutApiError(response.status, body?.detail ?? response.statusText);
  }
  return response.json() as Promise<T>;
}

/** Client components: same API through the Next proxy. */
export async function scoutClient<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`/api/scout${path}`, { cache: "no-store", ...init });
  const body = await response.json().catch(() => null);
  if (!response.ok) throw new ScoutApiError(response.status, body?.detail ?? response.statusText);
  return body as T;
}
