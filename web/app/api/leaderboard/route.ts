const BACKEND_API_BASE = (process.env.API_BASE_URL || "http://127.0.0.1:8000").replace(/\/$/, "");

export async function GET(request: Request) {
  const url = new URL(request.url);
  const upstream = await fetch(`${BACKEND_API_BASE}/leaderboard${url.search}`, {
    cache: "no-store"
  });
  const body = await upstream.text();

  return new Response(body, {
    status: upstream.status,
    headers: {
      "content-type": upstream.headers.get("content-type") || "application/json"
    }
  });
}
