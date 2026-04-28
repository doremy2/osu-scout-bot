const BACKEND_API_BASE = (process.env.API_BASE_URL || "http://127.0.0.1:8000").replace(/\/$/, "");

type RouteContext = {
  params: Promise<{ username: string }>;
};

export async function GET(request: Request, context: RouteContext) {
  const { username } = await context.params;
  const url = new URL(request.url);
  const upstream = await fetch(
    `${BACKEND_API_BASE}/player/${encodeURIComponent(username)}/power${url.search}`,
    {
      cache: "no-store"
    }
  );
  const body = await upstream.text();

  return new Response(body, {
    status: upstream.status,
    headers: {
      "content-type": upstream.headers.get("content-type") || "application/json"
    }
  });
}
