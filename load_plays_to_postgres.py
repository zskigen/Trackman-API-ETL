import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from trackman.auth import get_access_token
from trackman.data import get_game_plays

# Load environment variables
load_dotenv()

# --- TrackMan credentials ---
CLIENT_ID = os.getenv("TRACKMAN_CLIENT_ID")
CLIENT_SECRET = os.getenv("TRACKMAN_CLIENT_SECRET")

# --- Connect to Railway Postgres ---
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# --- Step 1: Authenticate ---
tokens = get_access_token(CLIENT_ID, CLIENT_SECRET)
access_token = tokens["access_token"]

# --- Step 2: Load sessions ---
sessions_df = pd.read_sql("SELECT session_id FROM sessions", engine)
print(f"Found {len(sessions_df)} sessions to process.")

# --- Step 3: Fetch plays ---
all_plays = []
for session_id in sessions_df["session_id"].head(10):  # limit for testing
    print(f"Fetching plays for {session_id}...")
    try:
        plays = get_game_plays(access_token, session_id)
        for p in plays:
            p["session_id"] = session_id
        all_plays.extend(plays)
    except Exception as e:
        print(f"Error fetching {session_id}: {e}")

# --- Step 4: Write to DB ---
if all_plays:
    plays_df = pd.json_normalize(all_plays, sep="_")
    plays_df.to_sql("plays", engine, if_exists="replace", index=False)
    print(f"Loaded {len(plays_df)} rows into 'plays' table.")
else:
    print("No play data found.")
