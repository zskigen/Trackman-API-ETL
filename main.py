from datetime import datetime, timedelta
from trackman.auth import get_access_token
from trackman.discovery import get_game_sessions
from trackman.data import get_game_balls
import os
import time
from dotenv import load_dotenv

load_dotenv()  # will look for .env in project root

CLIENT_ID = os.getenv("TRACKMAN_CLIENT_ID")
CLIENT_SECRET = os.getenv("TRACKMAN_CLIENT_SECRET")



def get_sessions_year(access_token, year=2024):
    all_sessions = []
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)

    while start < end:
        stop = min(start + timedelta(days=29), end)  # 30 days inclusive
        utc_from = start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        utc_to = stop.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        print(f" Querying {utc_from} → {utc_to}")
        try:
            sessions = get_game_sessions(access_token, utc_from, utc_to, session_type="All")
            print(f"  Found {len(sessions)} sessions")
            all_sessions.extend(sessions)
        except Exception as e:
            print(f" Error on range {utc_from} → {utc_to}: {e}")

        #  Pause to avoid 429s
        time.sleep(2)

        start = stop + timedelta(days=1)

    return all_sessions


def main():
    tokens = get_access_token(CLIENT_ID, CLIENT_SECRET)
    access_token = tokens["access_token"]
    print("Got token")

    sessions = get_sessions_year(access_token, year=2024)

    print(f"Total sessions found in 2024: {len(sessions)}")
    for s in sessions[:5]:
        print(s["sessionId"], s.get("gameDateUtc"), s.get("location", {}))

    # Flatten JSON into a dataframe
    import pandas as pd

    sessions_df = pd.json_normalize(sessions, sep="_")
    sessions_df = sessions_df.rename(columns={
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

    sessions_df.to_csv("sessions_2024.csv", index=False)
    print("Saved sessions_2024.csv with shape:", sessions_df.shape)


if __name__ == "__main__":
    main()

