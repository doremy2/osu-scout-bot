#bot.py
print("BOT STARTING...")

import asyncio
import os

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

from analysis import (
    SLOT_GROUPS,
    compare_players,
    get_full_slot_summary,
    get_overall_summary,
)

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = os.getenv("DISCORD_GUILD_ID")


class ScoutBot(commands.Bot):
    async def setup_hook(self) -> None:
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            synced = await self.tree.sync(guild=guild)
            names = ", ".join(cmd.name for cmd in synced)
            print(f"Synced {len(synced)} guild command(s): {names}")
        else:
            synced = await self.tree.sync()
            names = ", ".join(cmd.name for cmd in synced)
            print(f"Synced {len(synced)} global command(s): {names}")


intents = discord.Intents.default()
bot = ScoutBot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    # Backfill match_history from legacy data on first startup
    try:
        from storage import backfill_match_history_from_legacy
        count = await asyncio.to_thread(backfill_match_history_from_legacy)
        if count:
            print(f"Backfilled {count} rows into match_history")
    except Exception as exc:
        print(f"Match history backfill skipped: {exc}")


# ---------- formatting helpers ----------

def _format_score(value):
    return f"{value:,}" if isinstance(value, (int, float)) else str(value)


def _format_sr_label(stats: dict) -> str:
    """Build a compact star rating label like '(6.5★ eff)' or '(4.6★→6.9★)'."""
    base = stats.get("avg_star_rating")
    eff = stats.get("effective_sr")
    if isinstance(eff, (int, float)) and isinstance(base, (int, float)):
        if abs(eff - base) > 0.05:
            return f"{base}→{eff}★"
        return f"{eff}★"
    if isinstance(base, (int, float)):
        return f"{base}★"
    return ""


def _format_se_label(stats: dict) -> str:
    """Format Star Efficiency like 'SE:128k'."""
    se = stats.get("star_efficiency")
    if not isinstance(se, (int, float)):
        return ""
    if se >= 1000:
        return f"SE:{se // 1000}k"
    return f"SE:{se}"


def _format_stat_line(label: str, stats: dict) -> str:
    if stats["matches"] == 0:
        return f"{label}: N/A"
    sr_str = _format_sr_label(stats)
    se_str = _format_se_label(stats)
    extras = " | ".join(x for x in [sr_str, se_str] if x)
    extras_part = f" | {extras}" if extras else ""
    if stats["winrate"] != "N/A":
        return (
            f"{label}: {_format_score(stats['avg_score'])} | "
            f"{stats['winrate']}% WR{extras_part} | {stats['matches']}x"
        )
    # leaderboard mode: no winrate
    return (
        f"{label}: {_format_score(stats['avg_score'])}{extras_part} | {stats['matches']}x"
    )


def _format_slot_group(slot_stats: dict, mod: str) -> str:
    lines = [_format_stat_line(slot, slot_stats[slot]) for slot in SLOT_GROUPS[mod]]
    return "\n".join(lines)


def _format_wr_display(value):
    return f"{value}%" if isinstance(value, (int, float)) else "N/A"


def _format_performance_score_display(value):
    return str(value) if isinstance(value, (int, float)) else "N/A"


def _format_key_pick_line(row: dict, player1: str, player2: str) -> str:
    eff_sr = row.get("effective_sr")
    sr_str = f" ({eff_sr}★)" if isinstance(eff_sr, (int, float)) else ""
    return (
        f"{row['slot']}{sr_str}: {player1} | "
        f"{_format_score(row['player1_score'])} vs {_format_score(row['player2_score'])}"
    )


def _format_slot_winrate_line(row: dict, player1: str, player2: str) -> str:
    return (
        f"{row['slot']}: {player1} vs {player2} | "
        f"{_format_wr_display(row['player1_winrate'])} vs {_format_wr_display(row['player2_winrate'])}"
    )


def _format_comfort_picks(picks: list[dict]) -> str:
    if not picks:
        return "N/A"

    lines = []
    for pick in picks:
        base_sr = pick.get("avg_star_rating")
        eff_sr = pick.get("effective_sr")
        se = pick.get("star_efficiency")
        if isinstance(eff_sr, (int, float)) and isinstance(base_sr, (int, float)) and abs(eff_sr - base_sr) > 0.05:
            star = f"{base_sr}→{eff_sr}★"
        elif isinstance(eff_sr, (int, float)):
            star = f"{eff_sr}★"
        elif isinstance(base_sr, (int, float)):
            star = f"{base_sr}★"
        else:
            star = "N/A★"
        se_str = f" | SE:{se // 1000}k" if isinstance(se, (int, float)) and se >= 1000 else ""
        lines.append(
            f"{pick['slot']}: {_format_score(pick['avg_score'])} | "
            f"{pick['avg_accuracy']}% | {star}{se_str}"
        )

    return "\n".join(lines)


def _has_meaningful_winrates(rows: list[dict]) -> bool:
    return any(
        row["player1_winrate"] != "N/A" or row["player2_winrate"] != "N/A"
        for row in rows
    )


def _summary_is_leaderboard_mode(summary: dict) -> bool:
    """A player's summary is leaderboard-mode if neither map nor match WR
    produced a real number."""
    return (
        not isinstance(summary.get("map_winrate"), (int, float))
        and not isinstance(summary.get("match_winrate"), (int, float))
    )


def _format_recent_match_history(history: list[dict], player: str) -> str:
    """Render true match-level history (full series), not individual maps."""
    if not history:
        return "No match data yet"

    lines = []
    for row in history:
        opponent = (
            row.get("opponent_team_name")
            or row.get("opponent_team")
            or row.get("opponent")
            or "Unknown"
        )
        p_score = row.get("player_score")
        o_score = row.get("opponent_score")
        if isinstance(p_score, int) and isinstance(o_score, int):
            score_str = f"{p_score}-{o_score}"
        else:
            score_str = "?"

        link = row.get("match_link")
        base = f"{player} vs {opponent} | {score_str}"
        lines.append(f"{base} | {link}" if link else base)

    return "\n".join(lines)


def _format_overview_block(summary: dict) -> str:
    return (
        f"Maps Played: {summary['total_maps_played']}\n"
        f"Map WR: {_format_wr_display(summary['map_winrate'])}\n"
        f"Match WR: {_format_wr_display(summary['match_winrate'])}\n"
        f"Performance Score: {_format_performance_score_display(summary['avg_performance_score'])}"
    )


def _format_ratings_block(summary: dict) -> str:
    ratings = summary.get("ratings") or {}
    return (
        f"ROMAI: {ratings.get('romai', 'N/A')}\n"
        f"Duel: {ratings.get('elitebotix_duel', 'N/A')}\n"
        f"Skillissue: {ratings.get('skillissue', 'N/A')}"
    )


# ---------- /scout ----------

@bot.tree.command(name="scout", description="Scout an osu tournament player")
@app_commands.describe(username="The osu username to scout")
async def scout(interaction: discord.Interaction, username: str):
    await interaction.response.defer(thinking=True)
    summary = await asyncio.to_thread(get_overall_summary, username)

    if summary is None:
        await interaction.followup.send(
            f"No data found for **{username}**.",
            ephemeral=True,
        )
        return

    recent_maps = summary["recent_maps"]
    recent_match_history = summary["recent_match_history"]
    strengths = summary["strengths"]
    weaknesses = summary["weaknesses"]
    slot_stats = summary["slot_stats_90"]

    leaderboard_mode = _summary_is_leaderboard_mode(summary)

    # Recent matches = full series (BO9/BO11/BO13) with final score and link.
    # This is intentionally separate from "recent maps" below.
    recent_matches_text = _format_recent_match_history(recent_match_history, username)

    # Recent maps = individual map rows the player played inside any match.
    # Leaderboard-only rows (OWC CSV) have no opponent or win/loss, so render
    # them as "{slot} | {map name} | {score}". When real result data exists
    # (match_scores-derived rows), prefix with a W / L marker.
    def _fmt_recent_map(m: dict) -> str:
        slot = (m.get("slot") or "").strip()
        score = _format_score(m.get("score"))
        map_name = (m.get("map_name") or "").strip()
        if len(map_name) > 34:
            map_name = map_name[:31] + "..."
        sr = m.get("star_rating")
        sr_str = f" ({sr}★)" if isinstance(sr, (int, float)) else ""
        result = (m.get("result") or "").lower()
        if result == "win":
            head = "W"
        elif result == "loss":
            head = "L"
        else:
            head = None
        if slot:
            core = f"{slot}{sr_str} | {map_name or '-'} | {score}"
        else:
            core = f"{map_name or '-'}{sr_str} | {score}"
        return f"{head} {core}" if head else core

    recent_maps_text = "\n".join(_fmt_recent_map(m) for m in recent_maps) or "No recent maps"

    # Top slots (dynamic, driven by whatever slots the player has data in)
    slot_lines = []
    for slot, stats in slot_stats.items():
        if stats["matches"] == 0 or stats["avg_score"] == "N/A":
            continue
        slot_lines.append((slot, stats["avg_score"], stats["winrate"]))

    slot_lines.sort(key=lambda x: x[1], reverse=True)
    slot_text = "\n".join(
        (
            f"{slot}: {_format_score(score)} ({wr}%)"
            if isinstance(wr, (int, float))
            else f"{slot}: {_format_score(score)}"
        )
        for slot, score, wr in slot_lines[:8]
    ) or "No data"

    # Color: use real winrate if we have one, otherwise neutral
    overall_wr = summary["overall_winrate"]
    if not leaderboard_mode and isinstance(overall_wr, (int, float)):
        if overall_wr >= 70:
            color = discord.Color.green()
        elif overall_wr >= 40:
            color = discord.Color.orange()
        else:
            color = discord.Color.red()
    else:
        color = discord.Color.blurple()

    embed = discord.Embed(
        title=f"Scouting Report: {username}",
        color=color,
    )

    # Top-line overview (matches /compare layout)
    embed.add_field(
        name="Overview",
        value=_format_overview_block(summary),
        inline=False,
    )

    embed.add_field(
        name="Ratings",
        value=_format_ratings_block(summary),
        inline=False,
    )

    embed.add_field(name="Recent Matches", value=recent_matches_text, inline=False)
    embed.add_field(name="Recent Maps", value=recent_maps_text, inline=False)
    embed.add_field(name="Top Slots (Last 90d)", value=slot_text, inline=False)

    # Strengths / weaknesses: rank by Star Efficiency (score / effective SR)
    # so that scoring 900k on a 7★ DT map ranks higher than 900k on a 4★ NM.
    slot_entries = []
    for slot, stats in slot_stats.items():
        if stats["matches"] == 0 or stats["avg_score"] == "N/A":
            continue
        se = stats.get("star_efficiency")
        eff = stats.get("effective_sr")
        sr_label = _format_sr_label(stats)
        se_label = _format_se_label(stats)
        # Use star efficiency for sorting if available, else raw score
        sort_key = se if isinstance(se, (int, float)) else stats["avg_score"]
        detail = f" | {sr_label}" if sr_label else ""
        detail += f" | {se_label}" if se_label else ""
        slot_entries.append((slot, stats["avg_score"], stats["matches"], detail, sort_key))
    slot_entries.sort(key=lambda x: -x[4])
    top_2 = slot_entries[:2]
    bot_2 = slot_entries[-2:] if len(slot_entries) > 2 else list(reversed(slot_entries[:2]))
    strengths_text = "\n".join(
        f"{slot}: avg {score:,}{detail} ({n}x)" for slot, score, n, detail, _ in top_2
    ) or "N/A"
    weaknesses_text = "\n".join(
        f"{slot}: avg {score:,}{detail} ({n}x)" for slot, score, n, detail, _ in bot_2
    ) or "N/A"

    embed.add_field(name="Strengths", value=strengths_text, inline=True)
    embed.add_field(name="Weaknesses", value=weaknesses_text, inline=True)

    # Draft advice: use the ban_pick engine for real data-driven suggestions.
    try:
        from ban_pick import generate_draft_advice as _gen_advice
        # Reshape slot_stats into the format ban_pick expects
        bp_stats = {
            slot: {
                "played": s.get("matches", 0),
                "wins": s.get("wins", 0),
                "losses": s.get("losses", 0),
                "winrate": s.get("winrate") if isinstance(s.get("winrate"), (int, float)) else None,
                "avg_score": s.get("avg_score") if s.get("avg_score") != "N/A" else None,
                "avg_accuracy": s.get("accuracy") if s.get("accuracy") != "N/A" else None,
                "avg_star_rating": s.get("avg_star_rating") if isinstance(s.get("avg_star_rating"), (int, float)) else None,
                "effective_sr": s.get("effective_sr"),
                "star_efficiency": s.get("star_efficiency"),
                "scores": [],
            }
            for slot, s in slot_stats.items()
            if s.get("matches", 0) > 0
        }
        advice = _gen_advice(username, bp_stats)
        advice_lines = []
        if advice.comfort_picks:
            top_comfort = advice.comfort_picks[0]
            advice_lines.append(f"Comfort: **{top_comfort.slot}** ({top_comfort.played} maps)")
        if advice.risky_slots:
            top_risky = advice.risky_slots[0]
            advice_lines.append(f"Avoid: **{top_risky.slot}** ({top_risky.reason.split(': ', 1)[-1]})")
        if advice.suggested_picks:
            top_pick = advice.suggested_picks[0]
            advice_lines.append(f"Best pick: **{top_pick.slot}** `[{top_pick.confidence_label}]`")
        if not advice_lines:
            advice_lines.append("Not enough data for suggestions yet")
        embed.add_field(name="Draft Advice", value="\n".join(advice_lines), inline=False)
    except Exception:
        # Fallback to simple heuristic if ban_pick engine fails
        embed.add_field(
            name="Draft Advice",
            value="Use `/draft` for detailed ban/pick analysis",
            inline=False,
        )

    mode_label = "Leaderboard mode" if leaderboard_mode else "Result mode"
    embed.set_footer(text=mode_label)

    await interaction.followup.send(embed=embed)



# ---------- /slots ----------

@bot.tree.command(name="slots", description="Show full slot performance for a player")
@app_commands.describe(username="The osu username to inspect")
async def slots(interaction: discord.Interaction, username: str):
    slot_stats = get_full_slot_summary(username)

    if slot_stats is None:
        await interaction.response.send_message(
            f"No data found for **{username}**.",
            ephemeral=True,
        )
        return

    rows = [
        (slot, stats)
        for slot, stats in slot_stats.items()
        if stats["matches"] > 0
    ]

    if not rows:
        await interaction.response.send_message(
            f"No slot data found for **{username}**.",
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title=f"Slot Performance: {username}",
        description="Recent imported data",
        color=discord.Color.blurple(),
    )

    chunk_size = 8
    lines = [
        _format_stat_line(slot, stats)
        for slot, stats in rows
    ]

    for index in range(0, len(lines), chunk_size):
        chunk = "\n".join(lines[index:index + chunk_size])
        embed.add_field(
            name=f"Slots {index // chunk_size + 1}",
            value=chunk,
            inline=False,
        )

    await interaction.response.send_message(embed=embed)



# ---------- /compare ----------

@bot.tree.command(name="compare", description="Compare two osu tournament players")
@app_commands.describe(player1="First player", player2="Second player")
async def compare(interaction: discord.Interaction, player1: str, player2: str):
    if player1.lower() == player2.lower():
        await interaction.response.send_message(
            "Choose two different players.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True)
    result = await asyncio.to_thread(compare_players, player1, player2)

    if result is None:
        await interaction.followup.send(
            "Could not compare those players. Check your data.",
            ephemeral=True,
        )
        return

    p1 = result["player1"]
    p2 = result["player2"]

    key_pick_rows = result["key_picks"][:3]
    key_picks_text = (
        "\n".join(_format_key_pick_line(row, player1, player2) for row in key_pick_rows)
        if key_pick_rows
        else f"No clear score-gap picks for {player1}"
    )

    slot_winrates = result["slot_winrates"]
    has_meaningful_winrates = _has_meaningful_winrates(slot_winrates)
    slot_winrates_text = "\n".join(
        _format_slot_winrate_line(row, player1, player2)
        for row in slot_winrates
    )

    comfort_1 = _format_comfort_picks(result["comfort_picks"][player1])
    comfort_2 = _format_comfort_picks(result["comfort_picks"][player2])

    bans = result["recommended_bans"]
    bans_text = (
        "\n".join(
            f"{row['slot']}: ban vs {player2} | "
            f"{_format_score(row['player1_score'])} vs {_format_score(row['player2_score'])}"
            for row in bans
        )
        if bans
        else f"No clear bans needed vs {player2}"
    )

    embed = discord.Embed(
        title=f"Compare: {player1} vs {player2}",
        description="Drafting snapshot from the last 90 days",
        color=discord.Color.purple(),
    )

    embed.add_field(name=player1, value=_format_overview_block(p1), inline=True)
    embed.add_field(name=player2, value=_format_overview_block(p2), inline=True)
    embed.add_field(name=f"{player1} Ratings", value=_format_ratings_block(p1), inline=True)
    embed.add_field(name=f"{player2} Ratings", value=_format_ratings_block(p2), inline=True)

    embed.add_field(name="Key Picks", value=key_picks_text, inline=False)

    if has_meaningful_winrates:
        embed.add_field(name="Slot Winrates", value=slot_winrates_text, inline=False)

    embed.add_field(name=f"{player1} Comfort", value=comfort_1, inline=True)
    embed.add_field(name=f"{player2} Comfort", value=comfort_2, inline=True)
    embed.add_field(name="Recommended Bans", value=bans_text, inline=False)

    mode_label = "Leaderboard mode" if not has_meaningful_winrates else "Result mode"
    embed.set_footer(text=f"{mode_label} | Perspective: {player1} is treated as the drafting player.")

    await interaction.followup.send(embed=embed)

# ---------- /recent ----------

@bot.tree.command(name="recent", description="Show recent match history for a player")
@app_commands.describe(
    username="The osu username to look up",
    limit="How many matches to show (default 10, max 25)",
)
async def recent(interaction: discord.Interaction, username: str, limit: int = 10):
    await interaction.response.defer(thinking=True)

    from storage import fetch_recent_match_history

    limit = max(1, min(limit, 25))
    history = await asyncio.to_thread(fetch_recent_match_history, username, limit=limit)

    if not history:
        await interaction.followup.send(
            f"No recent match data found for **{username}**.",
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title=f"Recent Matches: {username}",
        color=discord.Color.teal(),
    )

    lines = []
    for row in history:
        # Date
        date_raw = row.get("match_date") or ""
        if len(date_raw) >= 10:
            parts = date_raw[:10].split("-")
            date_str = f"{parts[2]}/{parts[1]}" if len(parts) == 3 else date_raw[:10]
        elif date_raw:
            date_str = date_raw
        else:
            date_str = "??"

        # Tournament + stage
        tourney = row.get("tournament_name") or ""
        stage = row.get("stage") or ""
        tourney_str = f"{tourney}" if tourney else ""
        if stage:
            tourney_str += f" ({stage})" if tourney_str else stage

        # Teams / players
        team = row.get("team_name") or username
        opponent = row.get("opponent_team_name") or row.get("opponent_name") or "Unknown"

        # Scoreline
        p_score = row.get("player_score")
        o_score = row.get("opponent_score")
        if isinstance(p_score, int) and isinstance(o_score, int):
            score_str = f"{p_score}-{o_score}"
        else:
            score_str = "?"

        # Result emoji
        result = (row.get("result") or "").lower()
        if result == "win":
            r_icon = "🟢"
        elif result == "loss":
            r_icon = "🔴"
        else:
            r_icon = "⚪"

        # Match link
        link = row.get("match_link")
        link_str = f" [link]({link})" if link else ""

        # Data quality indicator
        dq = row.get("data_quality", "")
        dq_str = ""
        if dq == "verified":
            dq_str = " ✓"
        elif dq == "inferred":
            dq_str = " ~"
        elif dq == "sample":
            dq_str = " ?"

        line = f"{r_icon} `{date_str}` {team} vs {opponent} | {score_str}{link_str}{dq_str}"
        if tourney_str:
            line += f"\n> {tourney_str}"
        lines.append(line)

    # Discord embed fields max 1024 chars, split if needed
    text = "\n".join(lines)
    if len(text) <= 1024:
        embed.add_field(name="Matches", value=text, inline=False)
    else:
        # Split into chunks
        chunk = []
        chunk_len = 0
        field_num = 1
        for line in lines:
            if chunk_len + len(line) + 1 > 1000 and chunk:
                embed.add_field(name=f"Matches ({field_num})", value="\n".join(chunk), inline=False)
                field_num += 1
                chunk = []
                chunk_len = 0
            chunk.append(line)
            chunk_len += len(line) + 1
        if chunk:
            embed.add_field(name=f"Matches ({field_num})", value="\n".join(chunk), inline=False)

    # Footer with provenance info
    sources = set(row.get("source", "?") for row in history)
    source_str = ", ".join(sorted(sources))
    embed.set_footer(text=f"Sources: {source_str} | ✓=verified ~=inferred ?=sample")

    await interaction.followup.send(embed=embed)


# ---------- /draft ----------

@bot.tree.command(name="draft", description="Get data-driven ban/pick advice for a matchup")
@app_commands.describe(
    player="Player or team to scout",
    opponent="Opponent player or team (optional — omit for self-scout)",
)
async def draft(interaction: discord.Interaction, player: str, opponent: str | None = None):
    await interaction.response.defer(thinking=True)

    try:
        from database import fetch_player_slot_stats as v2_slot_stats
        from ban_pick import generate_draft_advice
    except ImportError:
        await interaction.followup.send("Draft engine not available.", ephemeral=True)
        return

    own_stats = await asyncio.to_thread(v2_slot_stats, username=player)
    opp_stats = None
    if opponent:
        opp_stats = await asyncio.to_thread(v2_slot_stats, username=opponent)

    # Fall back to legacy slot stats if v2 has no data or all slots are "?"
    def _v2_slots_are_useless(stats: dict) -> bool:
        if not stats:
            return True
        return all(k == "?" for k in stats.keys())

    if _v2_slots_are_useless(own_stats):
        from analysis import build_slot_stats, get_matches_last_n_days, get_all_slots
        recent = get_matches_last_n_days(player, 90)
        own_stats = build_slot_stats(recent, slots=get_all_slots(recent))
        # Reshape to match v2 format
        own_stats = {
            slot: {
                "played": s.get("matches", 0),
                "wins": s.get("wins", 0),
                "losses": s.get("losses", 0),
                "winrate": s.get("winrate"),
                "avg_score": s.get("avg_score"),
                "avg_accuracy": s.get("accuracy"),
                "avg_star_rating": s.get("avg_star_rating"),
                "effective_sr": s.get("effective_sr"),
                "star_efficiency": s.get("star_efficiency"),
                "scores": [],
            }
            for slot, s in own_stats.items()
            if s.get("matches", 0) > 0
        }
    if opponent and _v2_slots_are_useless(opp_stats):
        from analysis import build_slot_stats, get_matches_last_n_days, get_all_slots
        recent = get_matches_last_n_days(opponent, 90)
        opp_stats = build_slot_stats(recent, slots=get_all_slots(recent))
        opp_stats = {
            slot: {
                "played": s.get("matches", 0),
                "wins": s.get("wins", 0),
                "losses": s.get("losses", 0),
                "winrate": s.get("winrate"),
                "avg_score": s.get("avg_score"),
                "avg_accuracy": s.get("accuracy"),
                "avg_star_rating": s.get("avg_star_rating"),
                "effective_sr": s.get("effective_sr"),
                "star_efficiency": s.get("star_efficiency"),
                "scores": [],
            }
            for slot, s in opp_stats.items()
            if s.get("matches", 0) > 0
        }

    if not own_stats:
        await interaction.followup.send(f"No slot data found for **{player}**.", ephemeral=True)
        return

    advice = await asyncio.to_thread(
        generate_draft_advice, player, own_stats, opp_stats,
        ban_count=2, pick_count=3,
    )

    title = f"Draft Advice: {player}" + (f" vs {opponent}" if opponent else "")
    embed = discord.Embed(title=title, color=discord.Color.gold())

    # Bans
    if advice.suggested_bans:
        ban_text = "\n".join(
            f"**{s.slot}** — {s.reason} `[{s.confidence_label}]`"
            for s in advice.suggested_bans
        )
        embed.add_field(name="Suggested Bans", value=ban_text, inline=False)

    # Picks
    if advice.suggested_picks:
        pick_text = "\n".join(
            f"**{s.slot}** — {s.reason} `[{s.confidence_label}]`"
            for s in advice.suggested_picks
        )
        embed.add_field(name="Suggested Picks", value=pick_text, inline=False)

    # Comfort
    if advice.comfort_picks:
        comfort_text = "\n".join(
            f"**{s.slot}** — {s.reason} `[{s.confidence_label}]`"
            for s in advice.comfort_picks
        )
        embed.add_field(name="Comfort Picks", value=comfort_text, inline=False)

    # Risky
    if advice.risky_slots:
        risky_text = "\n".join(
            f"**{s.slot}** — {s.reason} `[{s.confidence_label}]`"
            for s in advice.risky_slots
        )
        embed.add_field(name="Risky / Avoid", value=risky_text, inline=False)

    # Slot rankings
    if advice.slot_rankings:
        def _rank_line(r):
            base_sr = r.get("avg_star_rating")
            eff = r.get("effective_sr")
            if isinstance(eff, (int, float)) and isinstance(base_sr, (int, float)) and abs(eff - base_sr) > 0.05:
                sr_str = f" | {base_sr}→{eff}★"
            elif isinstance(eff, (int, float)):
                sr_str = f" | {eff}★"
            elif isinstance(base_sr, (int, float)):
                sr_str = f" | {base_sr}★"
            else:
                sr_str = ""
            se = r.get("star_efficiency")
            se_str = f" | SE:{se // 1000}k" if isinstance(se, (int, float)) and se >= 1000 else ""
            wr = r.get("winrate")
            wr_str = f" | {wr}% WR" if isinstance(wr, (int, float)) else ""
            return f"{r['slot']}: {_format_score(r['avg_score'])}{sr_str}{se_str}{wr_str} | {r['played']} maps `[{r['confidence']}]`"
        rank_text = "\n".join(_rank_line(r) for r in advice.slot_rankings[:8])
        embed.add_field(name="Slot Rankings", value=rank_text, inline=False)

    embed.set_footer(text="Confidence: high = 8+ maps + consistent | low = few maps or high variance")
    await interaction.followup.send(embed=embed)


if not TOKEN:
    raise ValueError("DISCORD_TOKEN is missing")

bot.run(TOKEN)
