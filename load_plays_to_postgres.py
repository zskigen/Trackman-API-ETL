import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from trackman.auth import get_access_token
from trackman.data import get_game_plays

load_dotenv()

CLIENT_ID = os.getenv("TRACKMAN_CLIENT_ID")
CLIENT_SECRET = os.getenv("TRACKMAN_CLIENT_SECRET")

PGUSER = os.getenv("PGUSER")
PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT")
PGDATABASE = os.getenv("PGDATABASE")

engine = create_engine(f"postgresql://{PGUSER}@{PGHOST}:{PGPORT}/{PGDATABASE}")

tokens = get_access_token(CLIENT_ID, CLIENT_SECRET)
access_token = tokens["access_token"]

sessions_df = pd.read_sql("SELECT session_id FROM sessions", engine)
print(f"Found {len(sessions_df)} sessions to process.")

all_plays = []
for session_id in sessions_df["session_id"].head(10):  # test first
    print(f"Fetching plays for {session_id}...")
    try:
        plays = get_game_plays(access_token, session_id)
        for p in plays:
            p["session_id"] = session_id
        all_plays.extend(plays)
    except Exception as e:
        print(f"Error fetching {session_id}: {e}")

if all_plays:
    plays_df = pd.json_normalize(all_plays, sep="_")
    plays_df.to_sql("plays", engine, if_exists="replace", index=False)
    print(f"Loaded {len(plays_df)} rows into 'plays' table.")
else:
    print("No play data found.")
