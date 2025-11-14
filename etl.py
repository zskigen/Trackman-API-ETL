import os
import time
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv

# TrackMan API
from trackman.auth import get_access_token
from trackman.discovery import get_game_sessions
from trackman.data import get_game_plays, get_game_balls


# --------------------------------------------------------
# Load environment variables
# --------------------------------------------------------
load_dotenv()
CLIENT_ID = os.getenv("TRACKMAN_CLIENT_ID")
CLIENT_SECRET = os.getenv("TRACKMAN_CLIENT_SECRET")
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)


# --------------------------------------------------------
# Fetch all sessions for a given year
# --------------------------------------------------------
def fetch_sessions(access_token, year=2024):
    all_sessions = []
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)

    while start < end:
        stop = min(start + timedelta(days=29), end)
        utc_from = start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        utc_to = stop.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        print(f"[Sessions] {utc_from} → {utc_to}")
        try:
            sessions = get_game_sessions(access_token, utc_from, utc_to, session_type="All")
            print(f"  Found {len(sessions)} sessions.")
            all_sessions.extend(sessions)
        except Exception as e:
            print(f"  ERROR: {e}")

        start = stop + timedelta(days=1)
        time.sleep(1)

    return all_sessions


# --------------------------------------------------------
# Fetch plays for a list of session_ids
# --------------------------------------------------------
def fetch_all_plays(access_token, session_ids):
    all_rows = []

    for sid in session_ids:
        print(f"[Plays] Fetching session {sid}...")
        try:
            plays = get_game_plays(access_token, sid)
            for p in plays:
                p["session_id"] = sid
            all_rows.extend(plays)
        except Exception as e:
            print(f"  ERROR fetching plays for {sid}: {e}")

        time.sleep(0.3)

    return all_rows


# --------------------------------------------------------
# Fetch balls for a list of session_ids
# --------------------------------------------------------
def fetch_all_balls(access_token, session_ids):
    all_rows = []

    for sid in session_ids:
        print(f"[Balls] Fetching session {sid}...")
        try:
            balls = get_game_balls(access_token, sid)
            for b in balls:
                b["session_id"] = sid
            all_rows.extend(balls)
        except Exception as e:
            print(f"  ERROR fetching balls for {sid}: {e}")

        time.sleep(0.3)

    return all_rows


# --------------------------------------------------------
# Main ETL workflow
# --------------------------------------------------------
def main():

    # Authenticate
    tokens = get_access_token(CLIENT_ID, CLIENT_SECRET)
    access_token = tokens["access_token"]
    print("\nAuthenticated with TrackMan\n")

    # ---- 1. Sessions ----
    sessions = fetch_sessions(access_token, year=2024)
    sessions_df = pd.json_normalize(sessions, sep="_")

    # Clean + rename
    sessions_df = sessions_df.rename(columns={
        "sessionId": "session_id",
        "gameDateUtc": "game_date_utc",
        "gameDateLocal": "game_date_local",
    })

    # Deduplicate sessions
    sessions_df = sessions_df.drop_duplicates(subset=["session_id"])

    sessions_df = sessions_df[["session_id", "game_date_utc", "game_date_local"]]
    sessions_df.to_sql("sessions", engine, if_exists="replace", index=False)

    session_ids = sessions_df["session_id"].tolist()
    print(f"Loaded {len(session_ids)} sessions into DB.\n")

    # ---- 2. Plays ----
    plays = fetch_all_plays(access_token, session_ids)
    plays_df = pd.json_normalize(plays, sep="_")
    plays_df.to_sql("plays", engine, if_exists="replace", index=False)
    print(f"Loaded {len(plays_df)} plays into DB.\n")

    # ---- 3. Balls ----
    balls = fetch_all_balls(access_token, session_ids)
    balls_df = pd.json_normalize(balls, sep="_")
    balls_df.to_sql("balls", engine, if_exists="replace", index=False)
    print(f"Loaded {len(balls_df)} balls into DB.\n")

    print("---------------------------------------------------")
    print(" ETL complete — sessions, plays, balls all loaded.")
    print("---------------------------------------------------")


if __name__ == "__main__":
    main()
