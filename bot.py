#bot.py
print("BOT STARTING...")

import os

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

from analysis import (
    SLOT_GROUPS,
    compare_players,
    get_full_slot_summary,
    get_overall_summary,
)

load_dotenv()

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


# ---------- formatting helpers ----------

def _format_score(value):
    return f"{value:,}" if isinstance(value, (int, float)) else str(value)


def _format_stat_line(label: str, stats: dict) -> str:
    if stats["matches"] == 0:
        return f"{label}: N/A"
    if stats["winrate"] != "N/A":
        return (
            f"{label}: {_format_score(stats['avg_score'])} | "
            f"{stats['winrate']}% WR | {stats['matches']}x"
        )
    # leaderboard mode: no winrate
    return (
        f"{label}: {_format_score(stats['avg_score'])} | {stats['matches']}x"
    )


def _format_slot_group(slot_stats: dict, mod: str) -> str:
    lines = [_format_stat_line(slot, slot_stats[slot]) for slot in SLOT_GROUPS[mod]]
    return "\n".join(lines)


def _format_wr_display(value):
    return f"{value}%" if isinstance(value, (int, float)) else "N/A"


def _format_performance_score_display(value):
    return str(value) if isinstance(value, (int, float)) else "N/A"


def _format_key_pick_line(row: dict, player1: str, player2: str) -> str:
    return (
        f"{row['slot']}: {player1} | "
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
        star = (
            f"{pick['avg_star_rating']}★"
            if pick["avg_star_rating"] != "N/A"
            else "N/A★"
        )
        lines.append(
            f"{pick['slot']}: {_format_score(pick['avg_score'])} | "
            f"{pick['avg_accuracy']}% | {star}"
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


# ---------- /scout ----------

@bot.tree.command(name="scout", description="Scout an osu tournament player")
@app_commands.describe(username="The osu username to scout")
async def scout(interaction: discord.Interaction, username: str):
    summary = get_overall_summary(username)

    if summary is None:
        await interaction.response.send_message(
            f"No data found for **{username}**.",
            ephemeral=True,
        )
        return

    ratings = summary["ratings"]
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
    recent_maps_text = "\n".join(
        f"{(m.get('result') or '?')[0].upper()} vs {m.get('opponent') or 'Unknown'} | {m['slot']} | {_format_score(m['score'])}"
        for m in recent_maps
    ) or "No recent maps"

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
        value=(
            f"ROMAI: {ratings['romai']}\n"
            f"Duel: {ratings['elitebotix_duel']}\n"
            f"Skillissue: {ratings['skillissue']}"
        ),
        inline=False,
    )

    embed.add_field(name="Recent Matches", value=recent_matches_text, inline=False)
    embed.add_field(name="Recent Maps", value=recent_maps_text, inline=False)
    embed.add_field(name="Top Slots (Last 90d)", value=slot_text, inline=False)

    # Strengths / weaknesses / draft advice only make sense when we actually
    # have win/loss data. Otherwise the "Ban Unknown / Avoid Unknown" output
    # is meaningless, so hide it in leaderboard mode.
    if not leaderboard_mode:
        strengths_text = "\n".join(
            f"{mod}: {wr}%" for mod, wr in strengths if wr != "N/A"
        ) or "N/A"
        weaknesses_text = "\n".join(
            f"{mod}: {wr}%" for mod, wr in weaknesses if wr != "N/A"
        ) or "N/A"

        embed.add_field(name="Strengths", value=strengths_text, inline=True)
        embed.add_field(name="Weaknesses", value=weaknesses_text, inline=True)

        weakest_mod = weaknesses[0][0] if weaknesses else "Unknown"
        best_mod = strengths[0][0] if strengths else "Unknown"

        embed.add_field(
            name="Draft Advice",
            value=(
                f"Ban {weakest_mod}\n"
                f"Avoid {best_mod}\n"
                f"Force uncomfortable picks"
            ),
            inline=False,
        )

    mode_label = "Leaderboard mode" if leaderboard_mode else "Result mode"
    embed.set_footer(text=mode_label)

    await interaction.response.send_message(embed=embed)



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

    result = compare_players(player1, player2)

    if result is None:
        await interaction.response.send_message(
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

    embed.add_field(name="Key Picks", value=key_picks_text, inline=False)

    if has_meaningful_winrates:
        embed.add_field(name="Slot Winrates", value=slot_winrates_text, inline=False)

    embed.add_field(name=f"{player1} Comfort", value=comfort_1, inline=True)
    embed.add_field(name=f"{player2} Comfort", value=comfort_2, inline=True)
    embed.add_field(name="Recommended Bans", value=bans_text, inline=False)

    mode_label = "Leaderboard mode" if not has_meaningful_winrates else "Result mode"
    embed.set_footer(text=f"{mode_label} | Perspective: {player1} is treated as the drafting player.")

    await interaction.response.send_message(embed=embed)

if not TOKEN:
    raise ValueError("DISCORD_TOKEN is missing")

bot.run(TOKEN)
