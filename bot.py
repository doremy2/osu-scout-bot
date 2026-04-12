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


def _format_score(value):
    return f"{value:,}" if isinstance(value, (int, float)) else str(value)


def _format_stat_line(label: str, stats: dict) -> str:
    if stats["matches"] == 0:
        return f"{label}: N/A"
    return (
        f"{label}: {_format_score(stats['avg_score'])} | "
        f"{stats['winrate']}% WR | {stats['matches']}x"
    )


def _format_slot_group(slot_stats: dict, mod: str) -> str:
    lines = [_format_stat_line(slot, slot_stats[slot]) for slot in SLOT_GROUPS[mod]]
    return "\n".join(lines)


def _format_mod_compare_line(row: dict, player1: str, player2: str) -> str:
    p1 = row["player1"]
    p2 = row["player2"]

    p1_wr = p1["winrate"] if p1["winrate"] != "N/A" else "-"
    p2_wr = p2["winrate"] if p2["winrate"] != "N/A" else "-"

    if row["winner"] == "Tie":
        return f"{row['label']}: Close | {p1_wr}% vs {p2_wr}%"

    return (
        f"{row['label']}: {row['winner']} "
        f"({row['advantage']}, {row['confidence']}) | "
        f"{p1_wr}% vs {p2_wr}%"
    )


def _format_slot_compare_line(row: dict, player1: str, player2: str) -> str:
    p1 = row["player1"]
    p2 = row["player2"]

    p1_score = _format_score(p1["avg_score"]) if p1["matches"] else "-"
    p2_score = _format_score(p2["avg_score"]) if p2["matches"] else "-"

    if row["winner"] == "Tie":
        return f"{row['label']}: Close | {p1_score} vs {p2_score}"

    return (
        f"{row['label']}: {row['winner']} "
        f"({row['advantage']}, {row['confidence']}) | "
        f"{p1_score} vs {p2_score}"
    )


def _format_comfort_picks(picks: list[dict]) -> str:
    if not picks:
        return "N/A"

    return "\n".join(
        f"{pick['slot']}: {_format_score(pick['avg_score'])} | {pick['winrate']}% | {pick['matches']}x"
        for pick in picks
    )


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
    recent_matches = summary["recent_matches"]
    strengths = summary["strengths"]
    weaknesses = summary["weaknesses"]
    slot_stats = summary["slot_stats_90"]

    recent_text = "\n".join(
        f"{m['result'][0].upper()} vs {m['opponent']} | {m['slot']} | {_format_score(m['score'])}"
        for m in recent_matches
    ) or "No recent matches"

    strengths_text = "\n".join(f"{mod}: {wr}%" for mod, wr in strengths) or "N/A"
    weaknesses_text = "\n".join(f"{mod}: {wr}%" for mod, wr in weaknesses) or "N/A"

    slot_lines = []
    for slot, stats in slot_stats.items():
        if stats["avg_score"] != "N/A":
            slot_lines.append((slot, stats["avg_score"], stats["winrate"]))

    slot_lines.sort(key=lambda x: x[1], reverse=True)
    slot_text = "\n".join(
        f"{slot}: {_format_score(score)} ({wr}%)" for slot, score, wr in slot_lines[:8]
    ) or "No data"

    weakest_mod = weaknesses[0][0] if weaknesses else "Unknown"
    best_mod = strengths[0][0] if strengths else "Unknown"

    if summary["overall_winrate"] >= 70:
        color = discord.Color.green()
    elif summary["overall_winrate"] >= 40:
        color = discord.Color.orange()
    else:
        color = discord.Color.red()

    embed = discord.Embed(title=f"Scouting Report: {username}", color=color)

    embed.add_field(
        name="Ratings",
        value=(
            f"ROMAI: {ratings['romai']}\n"
            f"Duel: {ratings['elitebotix_duel']}\n"
            f"Skillissue: {ratings['skillissue']}"
        ),
        inline=False,
    )

    embed.add_field(name="Recent Matches", value=recent_text, inline=False)
    embed.add_field(name="Last 90 Days (Top Slots)", value=slot_text, inline=False)
    embed.add_field(name="Strengths", value=strengths_text, inline=True)
    embed.add_field(name="Weaknesses", value=weaknesses_text, inline=True)

    embed.add_field(
        name="Draft Advice",
        value=(
            f"Ban {weakest_mod}\n"
            f"Avoid {best_mod}\n"
            f"Force uncomfortable picks"
        ),
        inline=False,
    )

    embed.set_footer(
        text=f"WR: {summary['overall_winrate']}% | Consistency: {summary['consistency']}"
    )

    await interaction.response.send_message(embed=embed)


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

    embed = discord.Embed(
        title=f"Slot Performance: {username}",
        description="Last 90 days",
        color=discord.Color.blurple(),
    )

    for mod in ["NM", "HD", "HR", "DT", "FM"]:
        embed.add_field(
            name=mod,
            value=_format_slot_group(slot_stats, mod),
            inline=False,
        )

    await interaction.response.send_message(embed=embed)


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

    mod_lines = [
        _format_mod_compare_line(row, player1, player2)
        for row in result["mod_comparisons"]
    ]

    top_slot_rows = [
        row for row in result["slot_comparisons"] if row["winner"] != "Tie"
    ][:6]

    slot_lines = [
        _format_slot_compare_line(row, player1, player2) for row in top_slot_rows
    ] or ["No decisive slot edges"]

    bans_vs_1 = ", ".join(result["recommended_bans"][player1]) or "N/A"
    bans_vs_2 = ", ".join(result["recommended_bans"][player2]) or "N/A"

    comfort_1 = _format_comfort_picks(result["comfort_picks"][player1])
    comfort_2 = _format_comfort_picks(result["comfort_picks"][player2])

    embed = discord.Embed(
        title=f"Compare: {player1} vs {player2}",
        description="Drafting snapshot from the last 90 days",
        color=discord.Color.purple(),
    )

    embed.add_field(
        name=player1,
        value=(
            f"WR: {p1['overall_winrate']}%\n"
            f"Consistency: {p1['consistency']}\n"
            f"Matches: {p1['total_matches']}"
        ),
        inline=True,
    )

    embed.add_field(
        name=player2,
        value=(
            f"WR: {p2['overall_winrate']}%\n"
            f"Consistency: {p2['consistency']}\n"
            f"Matches: {p2['total_matches']}"
        ),
        inline=True,
    )

    embed.add_field(name="Mod Edge", value="\n".join(mod_lines), inline=False)
    embed.add_field(name="Top Slot Edge", value="\n".join(slot_lines), inline=False)
    embed.add_field(name=f"{player1} Comfort", value=comfort_1, inline=True)
    embed.add_field(name=f"{player2} Comfort", value=comfort_2, inline=True)
    embed.add_field(
        name="Recommended Bans",
        value=(
            f"Ban vs {player1}: {bans_vs_1}\n"
            f"Ban vs {player2}: {bans_vs_2}"
        ),
        inline=False,
    )

    await interaction.response.send_message(embed=embed)


if not TOKEN:
    raise ValueError("DISCORD_TOKEN is missing")

bot.run(TOKEN)