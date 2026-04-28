# FDC 2025 Score Mismatch Review

These rows were flagged by the extractor because the API-derived red/blue score did not exactly match the source table score. The imported package stores team-oriented rows for both sides, so a reversed row can still be valid if the opposite orientation matches the source score.

| Match | Expected source score | Imported team score rows | Skipped beatmaps | Assessment |
|---|---:|---|---:|---|
| [117201047](https://osu.ppy.sh/community/matches/117201047) | [7, 0] | bunny party 0-7 oddloop; oddloop 7-0 bunny party | 2 | harmless orientation/derivation flag; imported team row matches source score |
| [117118038](https://osu.ppy.sh/community/matches/117118038) | [2, 6] | Suklaapallit 2-6 bunny party; bunny party 6-2 Suklaapallit | 1 | harmless orientation/derivation flag; imported team row matches source score |
| [117120290](https://osu.ppy.sh/community/matches/117120290) | [6, 3] | Melon Boys 6-3 NO CLUE!; NO CLUE! 3-6 Melon Boys | 1 | harmless orientation/derivation flag; imported team row matches source score |
| [117104340](https://osu.ppy.sh/community/matches/117104340) | [1, 6] | Karjalanpiirakka 1-6 NO CLUE!; NO CLUE! 6-1 Karjalanpiirakka | 2 | harmless orientation/derivation flag; imported team row matches source score |
| [117047568](https://osu.ppy.sh/community/matches/117047568) | [6, 1] | mä rakastan sua 1-6 oddloop; oddloop 6-1 mä rakastan sua | 0 | harmless orientation/derivation flag; imported team row matches source score |
| [117037811](https://osu.ppy.sh/community/matches/117037811) | [6, 3] | Galaxy Destroyers 3-6 ballers will ball; ballers will ball 6-3 Galaxy Destroyers | 2 | harmless orientation/derivation flag; imported team row matches source score |
| [117038110](https://osu.ppy.sh/community/matches/117038110) | [5, 6] | NO CLUE! 5-6 might miss a match; might miss a match 6-5 NO CLUE! | 2 | harmless orientation/derivation flag; imported team row matches source score |
| [117038819](https://osu.ppy.sh/community/matches/117038819) | [6, 1] | public vessa 1-6 terence; terence 6-1 public vessa | 0 | harmless orientation/derivation flag; imported team row matches source score |
| [116942008](https://osu.ppy.sh/community/matches/116942008) | [5, 1] | NO CLUE! 1-5 oddloop; oddloop 5-1 NO CLUE! | 2 | harmless orientation/derivation flag; imported team row matches source score |
| [116943225](https://osu.ppy.sh/community/matches/116943225) | [2, 5] | No title 2-5 Schizo rizzzlers; Schizo rizzzlers 5-2 No title | 0 | harmless orientation/derivation flag; imported team row matches source score |
| [116834912](https://osu.ppy.sh/community/matches/116834912) | [1, -1] | No title -1-1 mä rakastan sua; mä rakastan sua 1--1 No title | 0 | source row contains a negative score; review source manually before any patch |
