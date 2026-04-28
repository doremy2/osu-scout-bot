# Stage export cache

Save browser-exported Stage tournament data in this folder, then run:

```powershell
python -m scripts.index_stage_tournaments --cache-dir data\cache\stage --years 2026 2025 2024 --limit 50
```

Accepted file types:

- `.json`: preferred, if you can save an API response or app state payload.
- `.har`: browser DevTools network export. This is useful when Stage loads tournament data through XHR/fetch.
- `.html` / `.htm`: saved page HTML. This works if the tournament data is embedded in the page.

Suggested browser workflow:

1. Open `https://otr.stagec.net/tournaments` in your normal browser.
2. If Cloudflare appears, complete it manually.
3. Open DevTools, go to `Network`, refresh the page, then export as HAR.
4. Save the file here, for example `stage_tournaments_2026_2025.har`.
5. Run the command above.

Stage is used only as discovery/enrichment. Do not import Stage-only tournaments
until they have been reviewed or cross-referenced against forum/wiki/sheet/bracket
sources.
