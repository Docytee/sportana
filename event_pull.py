"""
event_pull.py
─────────────────────────────────────────────────────────────
Fetches goal events and xG data from Understat for recently
completed matches and upserts them into the Supabase
`match_event` and `team_match_stat` tables.

Runs after matches are complete (recommended: 23:00 UTC daily).

Environment variables required:
  SUPABASE_URL         – your project URL
  SUPABASE_SERVICE_KEY – service_role key
  UNDERSTAT_LEAGUE     – e.g. "EPL", "La_liga", "Bundesliga"
                         "Serie_A", "Ligue_1", "RFPL"
  TARGET_SEASON        – e.g. "2026" (Understat uses single year
                         for the start of the season)
"""

import os
import sys
import asyncio
import logging
from datetime import date, timedelta, datetime

from dotenv import load_dotenv
from supabase import create_client, Client
from understat import Understat
import aiohttp

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
load_dotenv()

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

# Understat league strings (case-sensitive):
# EPL, La_liga, Bundesliga, Serie_A, Ligue_1, RFPL
UNDERSTAT_LEAGUE = os.getenv("UNDERSTAT_LEAGUE", "EPL")

# Understat uses the calendar year the season STARTS in
# e.g. for 2024/25 season → "2024"
TARGET_SEASON = os.getenv("TARGET_SEASON_UNDERSTAT", "2026")

# We pull events for yesterday (matches are complete by then)
TARGET_DATE = date.today() - timedelta(days=1)


# ── Supabase client ───────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ── Supabase helpers ──────────────────────────────────────────
def get_fixture_id(sb: Client, home: str, away: str, match_date: date) -> int | None:
    """Look up our internal fixture_id by team names and date."""
    result = (
        sb.table("fixture")
        .select("fixture_id")
        .eq("match_date", str(match_date))
        .ilike("home_team_name", f"%{home}%")
        .ilike("away_team_name", f"%{away}%")
        .limit(1)
        .execute()
    )
    if result.data:
        return result.data[0]["fixture_id"]
    return None


def update_fixture_score(sb: Client, fixture_id: int, home_goals: int,
                          away_goals: int) -> None:
    """Update the fixture row with final score and result."""
    if home_goals > away_goals:
        result = "H"
    elif away_goals > home_goals:
        result = "A"
    else:
        result = "D"

    sb.table("fixture").update({
        "home_score":       home_goals,
        "away_score":       away_goals,
        "full_time_result": result,
        "status":           "completed",
        "updated_at":       datetime.utcnow().isoformat(),
    }).eq("fixture_id", fixture_id).execute()


def upsert_stat(sb: Client, fixture_id: int, team_name: str,
                is_home: bool, stat_row: dict) -> None:
    """Upsert aggregated match stats for one team."""
    sb.table("team_match_stat").upsert(
        {
            "fixture_id":      fixture_id,
            "team_name":       team_name,
            "is_home":         is_home,
            "xg":              stat_row.get("xg"),
            "shots":           stat_row.get("shots"),
            "shots_on_target": stat_row.get("shots_on_target"),
        },
        on_conflict="fixture_id,team_name",
    ).execute()


def upsert_event(sb: Client, fixture_id: int, team_name: str,
                 player: str, minute: int, event_type: str,
                 is_own_goal: bool, is_penalty: bool) -> None:
    """Upsert a single match event (goal, etc.)."""
    sb.table("match_event").upsert(
        {
            "fixture_id":  fixture_id,
            "team_name":   team_name,
            "event_type":  event_type,
            "player_name": player,
            "minute":      minute,
            "is_own_goal": is_own_goal,
            "is_penalty":  is_penalty,
        },
        on_conflict="fixture_id,team_name,minute,player_name",
    ).execute()


# ── Name normalisation ────────────────────────────────────────
# Understat uses different team name spellings vs FBref.
# Extend this dict as you discover mismatches in your logs.
TEAM_NAME_MAP = {
    "Manchester United":    "Manchester Utd",
    "Manchester City":      "Manchester City",
    "Tottenham":            "Tottenham",
    "Wolverhampton":        "Wolves",
    "Nottingham Forest":    "Nott'ham Forest",
    "Newcastle United":     "Newcastle Utd",
    "West Ham":             "West Ham",
    "Brighton":             "Brighton",
    "Leicester":            "Leicester City",
    "Leeds":                "Leeds United",
    "Atletico Madrid":      "Atlético Madrid",
    "Celta Vigo":           "Celta de Vigo",
    "Betis":                "Real Betis",
    "Alaves":               "Alavés",
    "Paris Saint Germain":  "Paris S-G",
    "Marseille":            "Marseille",
}

def normalise(name: str) -> str:
    return TEAM_NAME_MAP.get(name, name)


# ── Core async logic ──────────────────────────────────────────
async def fetch_and_store():
    log.info("Starting event pull for %s | League: %s | Date: %s",
             TARGET_SEASON, UNDERSTAT_LEAGUE, TARGET_DATE)

    sb = get_supabase()

    async with aiohttp.ClientSession() as session:
        understat = Understat(session)

        # Fetch all results for the league/season
        log.info("Fetching results from Understat…")
        try:
            results = await understat.get_league_results(
                UNDERSTAT_LEAGUE,
                int(TARGET_SEASON)
            )
        except Exception as exc:
            log.error("Failed to fetch league results: %s", exc)
            sys.exit(1)

        log.info("Total results fetched: %d", len(results))

        # Filter to our target date
        todays_results = [
            r for r in results
            if r.get("datetime", "").startswith(str(TARGET_DATE))
        ]

        if not todays_results:
            log.info("No completed matches found for %s.", TARGET_DATE)
            return

        log.info("Matches on %s: %d", TARGET_DATE, len(todays_results))

        for match in todays_results:
            home_raw  = match.get("h", {}).get("title", "")
            away_raw  = match.get("a", {}).get("title", "")
            home_name = normalise(home_raw)
            away_name = normalise(away_raw)
            match_id  = match.get("id")

            home_goals = int(match.get("goals", {}).get("h", 0))
            away_goals = int(match.get("goals", {}).get("a", 0))
            home_xg    = float(match.get("xG", {}).get("h", 0.0))
            away_xg    = float(match.get("xG", {}).get("a", 0.0))

            log.info("Processing: %s %d-%d %s",
                     home_name, home_goals, away_goals, away_name)

            # Find the fixture in our database
            fixture_id = get_fixture_id(sb, home_name, away_name, TARGET_DATE)
            if not fixture_id:
                # Try with raw names as fallback
                fixture_id = get_fixture_id(sb, home_raw, away_raw, TARGET_DATE)

            if not fixture_id:
                log.warning("  ✗ No matching fixture found in DB for: %s vs %s on %s",
                            home_name, away_name, TARGET_DATE)
                log.warning("    Add to TEAM_NAME_MAP if names don't match.")
                continue

            # Update fixture with final score
            update_fixture_score(sb, fixture_id, home_goals, away_goals)
            log.info("  ✓ Score updated: %d-%d", home_goals, away_goals)

            # Upsert aggregated xG stats
            upsert_stat(sb, fixture_id, home_name, True,
                        {"xg": home_xg, "shots": None, "shots_on_target": None})
            upsert_stat(sb, fixture_id, away_name, False,
                        {"xg": away_xg, "shots": None, "shots_on_target": None})

            # Fetch shot-by-shot data for this specific match
            # This gives us exact goal minutes
            try:
                match_shots = await understat.get_match_shots(match_id)
            except Exception as exc:
                log.warning("  ✗ Could not fetch shots for match %s: %s",
                            match_id, exc)
                continue

            home_shots = match_shots.get("h", [])
            away_shots = match_shots.get("a", [])

            goal_count = 0
            for shot in home_shots + away_shots:
                # Only process goals
                if shot.get("result") != "Goal":
                    continue

                is_home    = shot in home_shots
                team_name  = home_name if is_home else away_name
                player     = shot.get("player", "Unknown")
                minute     = int(shot.get("minute", 0))
                is_own     = shot.get("result") == "OwnGoal"
                situation  = shot.get("situation", "")
                is_penalty = situation == "Penalty"

                upsert_event(
                    sb,
                    fixture_id=fixture_id,
                    team_name=team_name,
                    player=player,
                    minute=minute,
                    event_type="Goal",
                    is_own_goal=is_own,
                    is_penalty=is_penalty,
                )
                goal_count += 1

            log.info("  ✓ %d goal event(s) stored for this match.", goal_count)

    log.info("Event pull complete.")


# ── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(fetch_and_store())
