"""
daily_pull.py
─────────────────────────────────────────────────────────────
Pulls today's fixtures for a configured league using soccerdata
and upserts them into the Supabase `fixture` and `team` tables.

Environment variables required (set in GitHub Actions secrets):
  SUPABASE_URL        – your project URL
  SUPABASE_SERVICE_KEY – service_role key (NOT the anon key)
  TARGET_LEAGUE       – e.g. "ENG-Premier League"  (optional, defaults below)
  TARGET_SEASON       – e.g. "2324"                (optional, defaults to current)
"""

import os
import sys
import logging
from datetime import date, datetime

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
import soccerdata as sd

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

# soccerdata league string format: "COUNTRY-League Name"
# Full list: https://soccerdata.readthedocs.io/en/latest/datasources/fbref.html
TARGET_LEAGUE  = os.getenv("TARGET_LEAGUE", "ENG-Premier League")
TARGET_SEASON  = os.getenv("TARGET_SEASON", "2526")   # "2324" = 2023/24 season
TODAY          = date.today()

# ── Supabase client ───────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ── Helpers ───────────────────────────────────────────────────
def upsert_team(sb: Client, team_name: str, league: str) -> None:
    """Insert team if it doesn't exist yet (upsert on name)."""
    sb.table("team").upsert(
        {"name": team_name, "league": league},
        on_conflict="name",
    ).execute()


def upsert_fixture(sb: Client, row: dict) -> None:
    """Upsert a single fixture row (keyed on match_id)."""
    sb.table("fixture").upsert(
        row,
        on_conflict="match_id",
    ).execute()


# ── Core pull logic ───────────────────────────────────────────
def pull_fixtures() -> None:
    log.info("Starting daily pull for %s | Season %s | Date %s",
             TARGET_LEAGUE, TARGET_SEASON, TODAY)

    sb = get_supabase()

    # Initialise the FBref data source (no account needed)
    fbref = sd.FBref(leagues=TARGET_LEAGUE, seasons=TARGET_SEASON)

    # schedule() returns a DataFrame with all fixtures for the season
    log.info("Fetching schedule from FBref…")
    try:
        schedule: pd.DataFrame = fbref.read_schedule()
    except Exception as exc:
        log.error("Failed to fetch schedule: %s", exc)
        sys.exit(1)

    # ── Normalise column names (soccerdata may change them) ──
    schedule.columns = [c.lower().replace(" ", "_") for c in schedule.columns]
    log.info("Schedule fetched. Columns: %s", list(schedule.columns))
    log.info("Total fixtures in dataset: %d", len(schedule))

    # ── Filter to today's matches ────────────────────────────
    # soccerdata stores the date in a 'date' column as datetime/date objects
    if "date" not in schedule.columns:
        log.error("'date' column not found in schedule. Available: %s",
                  list(schedule.columns))
        sys.exit(1)

    schedule["date"] = pd.to_datetime(schedule["date"]).dt.date
    todays = schedule[schedule["date"] == TODAY].copy()

    if todays.empty:
        log.info("No fixtures today (%s). Nothing to insert.", TODAY)
        return

    log.info("Found %d fixture(s) today.", len(todays))

    # ── Insert teams & fixtures ──────────────────────────────
    inserted = 0
    skipped  = 0

    for _, match in todays.iterrows():
        try:
            # Column name mapping — adjust if soccerdata changes its schema
            home_name = str(match.get("home_team", match.get("home", "Unknown")))
            away_name = str(match.get("away_team", match.get("away", "Unknown")))
            kick_off  = match.get("time", None)

            # Build a stable match_id from the data
            match_id = (
                match.get("game_id") or          # FBref internal ID when available
                f"{TARGET_LEAGUE}_{TODAY}_{home_name}_{away_name}"
                .replace(" ", "_").lower()
            )

            # Upsert both teams
            upsert_team(sb, home_name, TARGET_LEAGUE)
            upsert_team(sb, away_name, TARGET_LEAGUE)

            # Build full_time_result if scores are present
            home_score = match.get("home_goals", match.get("score_home", None))
            away_score = match.get("away_goals", match.get("score_away", None))
            result     = None
            if pd.notna(home_score) and pd.notna(away_score):
                h, a = int(home_score), int(away_score)
                result = "H" if h > a else ("A" if a > h else "D")
            else:
                h, a = None, None

            # Determine status
            status = "completed" if result else "scheduled"

            fixture_row = {
                "match_id":        str(match_id),
                "home_team_name":  home_name,
                "away_team_name":  away_name,
                "match_date":      str(TODAY),
                "kick_off_time":   str(kick_off) if pd.notna(kick_off) else None,
                "league":          TARGET_LEAGUE,
                "season":          TARGET_SEASON,
                "matchweek":       int(match["round"]) if "round" in match and pd.notna(match["round"]) else None,
                "venue":           str(match["venue"]) if "venue" in match and pd.notna(match["venue"]) else None,
                "full_time_result": result,
                "home_score":      h,
                "away_score":      a,
                "status":          status,
                "updated_at":      datetime.utcnow().isoformat(),
            }

            upsert_fixture(sb, fixture_row)
            log.info("  ✓ %s vs %s  [%s]", home_name, away_name, status)
            inserted += 1

        except Exception as exc:
            log.warning("  ✗ Skipped a row due to error: %s", exc)
            skipped += 1

    log.info("Done. Inserted/updated: %d | Skipped: %d", inserted, skipped)


# ── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    pull_fixtures()
