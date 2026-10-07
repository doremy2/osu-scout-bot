import { notFound } from "next/navigation";

// Server-side calls go straight to the Python service; browser code uses the /api/scout proxy
// (see next.config.mjs). Neither ever sees the osu! credentials.
const SERVER_BASE = (process.env.SCOUT_API_URL || "http://127.0.0.1:8001").replace(/\/$/, "");

export class ScoutApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

/** Server components: fetch JSON from the scout service. 404 -> Next's notFound(). */
export async function scoutGet<T>(path: string): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${SERVER_BASE}/api${path}`, { cache: "no-store" });
  } catch {
    throw new ScoutApiError(
      503,
      "Can't reach the scout API on 127.0.0.1:8001. Start it with:  python -m scout serve   (or  python -m scout dev)"
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
