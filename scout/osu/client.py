"""Minimal osu! API v2 client: client-credentials auth, throttling, full match fetch."""
from __future__ import annotations

import time

import requests

API = "https://osu.ppy.sh/api/v2"
TOKEN_URL = "https://osu.ppy.sh/oauth/token"


class OsuApiError(RuntimeError):
    pass


class MatchNotFound(OsuApiError):
    pass


class OsuClient:
    def __init__(self, client_id: str, client_secret: str, min_interval: float = 1.0,
                 session: requests.Session | None = None):
        if not client_id or not client_secret:
            raise OsuApiError(
                "Missing OSU_CLIENT_ID / OSU_CLIENT_SECRET. Create an OAuth app at "
                "https://osu.ppy.sh/home/account/edit#oauth and put them in .env"
            )
        self.client_id = client_id
        self.client_secret = client_secret
        self.min_interval = min_interval
        self.http = session or requests.Session()
        self._token: str | None = None
        self._token_expiry = 0.0
        self._last_call = 0.0

    # --- auth -----------------------------------------------------------
    def _auth_header(self) -> dict[str, str]:
        if not self._token or time.time() > self._token_expiry - 60:
            r = self.http.post(TOKEN_URL, data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
                "scope": "public",
            }, timeout=30)
            if r.status_code != 200:
                raise OsuApiError(f"osu! auth failed ({r.status_code}): {r.text[:200]}")
            data = r.json()
            self._token = data["access_token"]
            self._token_expiry = time.time() + int(data.get("expires_in", 86400))
        return {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}

    # --- transport ------------------------------------------------------
    def get(self, path: str, params: dict | None = None, retries: int = 4) -> dict:
        for attempt in range(retries):
            wait = self.min_interval - (time.time() - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.time()
            r = self.http.get(f"{API}{path}", params=params, headers=self._auth_header(), timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                raise MatchNotFound(path)
            if r.status_code == 401:
                self._token = None
                continue
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(min(60, 2 ** attempt * 5))
                continue
            raise OsuApiError(f"GET {path} -> {r.status_code}: {r.text[:200]}")
        raise OsuApiError(f"GET {path} failed after {retries} attempts")

    # --- endpoints ------------------------------------------------------
    def get_match_full(self, match_id: int) -> dict:
        """The match endpoint returns at most 100 events per call. Page forward with
        `after=<last event id>` until we reach latest_event_id, then merge into one payload."""
        first = self.get(f"/matches/{match_id}", params={"after": 0, "limit": 100})
        events = list(first.get("events", []))
        users = {u["id"]: u for u in first.get("users", [])}
        latest = first.get("latest_event_id")
        guard = 0
        while events and latest and events[-1]["id"] < latest and guard < 200:
            guard += 1
            page = self.get(f"/matches/{match_id}", params={"after": events[-1]["id"], "limit": 100})
            new = page.get("events", [])
            if not new:
                break
            events.extend(new)
            users.update({u["id"]: u for u in page.get("users", [])})
        first["events"] = events
        first["users"] = list(users.values())
        return first
