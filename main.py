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

        print(f"🔎 Querying {utc_from} → {utc_to}")
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
    # Step 1: Authenticate
    tokens = get_access_token(CLIENT_ID, CLIENT_SECRET)
    access_token = tokens["access_token"]
    print(" Got token")

    # Step 2: Collect sessions for 2024 in 30-day chunks
    sessions = get_sessions_year(access_token, year=2024)

    print(f" Total sessions found in 2024: {len(sessions)}")
    for s in sessions[:5]:  # preview first 5
        print(s["sessionId"], s.get("gameDateUtc"), s.get("location", {}))

    # Step 3: If any sessions exist, fetch balls
    if sessions:
        session_id = sessions[0]["sessionId"]
        balls = get_game_balls(access_token, session_id)
        print(f" First session {session_id} balls:", balls[:2])

if __name__ == "__main__":
    main()
