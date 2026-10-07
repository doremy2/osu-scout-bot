const SCOUT_API_URL = (process.env.SCOUT_API_URL || "http://127.0.0.1:8001").replace(/\/$/, "");

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  async rewrites() {
    // The browser only ever talks to /api/scout/*; Next forwards it to the local Python
    // service (scout.server). SQLite and the osu! credentials stay on the Python side.
    return [{ source: "/api/scout/:path*", destination: `${SCOUT_API_URL}/api/:path*` }];
  }
};

export default nextConfig;
