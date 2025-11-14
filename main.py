from datetime import datetime, timedelta
from trackman.auth import get_access_token
from trackman.discovery import get_game_sessions
import os
import time
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load credentials
load_dotenv()

CLIENT_ID = os.getenv("TRACKMAN_CLIENT_ID")
CLIENT_SECRET = os.getenv("TRACKMAN_CLIENT_SECRET")

# Load PostgreSQL URL
DATABASE_URL = os.getenv("DATABASE_URL")

print("DATABASE_URL:", DATABASE_URL)  # Debug line (can delete later)

# Create database engine
engine = create_engine(DATABASE_URL)



# -----------------------------------------
# Fetch sessions for a given year
# -----------------------------------------
def get_sessions_year(access_token, year=2024):
    """Fetch all TrackMan sessions for a given year."""
    all_sessions = []
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)

    while start < end:
        stop = min(start + timedelta(days=29), end)
        utc_from = start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        utc_to = stop.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        print(f" Querying {utc_from} → {utc_to}")
        try:
            sessions = get_game_sessions(access_token, utc_from, utc_to, session_type="All")
            print(f"  Found {len(sessions)} sessions")
            all_sessions.extend(sessions)
        except Exception as e:
            print(f" Error on date range {utc_from} → {utc_to}: {e}")

        time.sleep(2)
        start = stop + timedelta(days=1)

    return all_sessions


# -----------------------------------------
# Main ETL logic
# -----------------------------------------
def main():
    # Authenticate with the TrackMan API
    tokens = get_access_token(CLIENT_ID, CLIENT_SECRET)
    access_token = tokens["access_token"]
    print("Authenticated with TrackMan API")

    # Fetch sessions from TrackMan
    sessions = get_sessions_year(access_token, year=2024)
    print(f"Total sessions found: {len(sessions)}")

    # Flatten JSON into DataFrame
    df = pd.json_normalize(sessions, sep="_")
    df = df.rename(columns={
        "sessionId": "session_id",
        "gameDateUtc": "game_date_utc",
        "gameDateLocal": "game_date_local",
        "location_field_name": "field_name",
        "location_venue_name": "venue_name",
        "homeTeam_name": "home_team",
        "awayTeam_name": "away_team",
        "league_name": "league",
        "level_name": "level"
    })[
        [
            "session_id",
            "game_date_utc",
            "game_date_local",
            "field_name",
            "venue_name",
            "league",
            "level",
            "home_team",
            "away_team"
        ]
    ]

    # Write DataFrame to Railway PostgreSQL
    df.to_sql("sessions", engine, if_exists="replace", index=False)
    print(f"Loaded {len(df)} rows into 'sessions' table in Railway PostgreSQL")


# -----------------------------------------
if __name__ == "__main__":
    main()