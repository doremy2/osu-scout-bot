# 4WC / 3WC Verification and 4WC Import Report

Generated: `2026-04-27T03:13:45+00:00`

## FDC Mismatch Review

- Wrote detailed review to `data/reports/fdc_2025_score_mismatch_review.md`.
- 10 of 11 flagged rows have an imported team-oriented row that matches the source score; these appear to be harmless orientation/derivation flags.
- 1 row has a negative source score (`1--1`) and should stay flagged for manual source review. No manual patch was made.

## 4WC / 3WC Verification

| Tournament | Mode | Team size | Links | Spreadsheet | Bracket | Production safe | Matches | Map scores | Missing timestamps | Duplicate matches | Broken players | Broken maps |
|---|---|---:|---:|---|---|---|---:|---:|---:|---:|---:|---:|
| 4WC 2025 | osu | 4v4 | 61 | yes | yes | true | 122 | 3683 | 0 | 0 | 0 | 0 |
| 3WC 2025 | osu | 3v3 | 31 | yes | yes | true | 62 | 1672 | 0 | 0 | 0 | 0 |

## Import Decision

- Imported exactly one tournament: `4WC 2025`.
- Reason: both candidates were valid standard osu! packages, and 4WC was queue rank 1 with more match links.
- `3WC 2025` was not imported and remains the only standard verified queue item.
- Ranking formula was not changed.

## 4WC Import Stats

- Matches written: 122
- Map scores written: 3683
- Players: 253
- Mappool rows: 123
- Score mismatches flagged for later review: 14
- Skipped non-mappool beatmaps: 7

## Current Queue After Import

- #1 3WC 2025 (osu, verified, 31 links)

## Top 50 Movement

Largest movements among current top 50:

- Azer: #103 -> #26 (up 77), score 53.10 (+10.52)
- BATBALL: #110 -> #33 (up 77), score 51.63 (+10.47)
- ESCRUPULILLO: #54 -> #45 (up 9), score 49.17 (+1.22)
- -IZZY: #30 -> #34 (down 4), score 51.43 (+0.00)
- tekkito: #31 -> #35 (down 4), score 51.26 (+0.00)
- A L E P H: #42 -> #46 (down 4), score 48.92 (+0.00)
- Kamensh1k: #43 -> #47 (down 4), score 48.88 (+0.00)
- mcy4: #44 -> #48 (down 4), score 48.78 (+0.00)
- fedoragoose: #45 -> #49 (down 4), score 48.78 (+0.00)
- NeliNyan: #29 -> #32 (down 3), score 52.02 (+0.00)
- Amasetic: #33 -> #36 (down 3), score 51.17 (+0.00)
- Hakui Koyori: #34 -> #37 (down 3), score 51.10 (+0.00)
- Crystal: #35 -> #38 (down 3), score 50.88 (+0.00)
- Kurumiw: #36 -> #39 (down 3), score 50.69 (+0.00)
- WindowLife: #37 -> #40 (down 3), score 50.60 (+0.00)

New or returning top-50 entries:
- Azer: 103 -> #26 (53.10)
- rng_: unranked -> #31 (52.41)
- BATBALL: 110 -> #33 (51.63)
- ESCRUPULILLO: 54 -> #45 (49.17)

Dropped from top 50:
- sorinica17: #46 -> 53
- JackPaX: #48 -> 52
- hexi: #49 -> 54
- xootynator: #50 -> 55

## Current Top 10 After Import

- #1 mrekk: 67.89 power, 72.65 recent form, 82.27 consistency
- #2 MALISZEWSKI: 67.86 power, 81.34 recent form, 92.31 consistency
- #3 liliel: 65.14 power, 69.52 recent form, 97.93 consistency
- #4 lifeline: 64.43 power, 73.00 recent form, 72.01 consistency
- #5 Raikouhou: 64.32 power, 75.69 recent form, 98.84 consistency
- #6 ASecretBox: 63.09 power, 68.92 recent form, 95.08 consistency
- #7 enri: 62.27 power, 90.11 recent form, 25.00 consistency
- #8 misha awa: 62.24 power, 77.34 recent form, 97.53 consistency
- #9 rektygon: 61.88 power, 75.46 recent form, 85.06 consistency
- #10 Riot: 61.34 power, 81.50 recent form, 66.42 consistency
