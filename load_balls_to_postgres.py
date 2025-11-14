import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from trackman.auth import get_access_token
from trackman.data import get_game_balls

# Load environment variables
load_dotenv()

# --- TrackMan credentials ---
CLIENT_ID = os.getenv("TRACKMAN_CLIENT_ID")
CLIENT_SECRET = os.getenv("TRACKMAN_CLIENT_SECRET")

# --- Connect to Railway Postgres ---
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# --- Step 1: Authenticate with TrackMan ---
tokens = get_access_token(CLIENT_ID, CLIENT_SECRET)
access_token = tokens["access_token"]

# --- Step 2: Load sessions from database ---
sessions_df = pd.read_sql("SELECT session_id FROM sessions", engine)
print(f"Found {len(sessions_df)} sessions to process.")

# --- Step 3: Fetch balls for each session ---
all_balls = []
for session_id in sessions_df["session_id"].head(10):  # limit for testing
    print(f"Fetching balls for session {session_id}...")
    try:
        balls = get_game_balls(access_token, session_id)
        for b in balls:
            b["session_id"] = session_id
        all_balls.extend(balls)
    except Exception as e:
        print(f"Error fetching {session_id}: {e}")

# --- Step 4: Write to database ---
if all_balls:
    balls_df = pd.json_normalize(all_balls, sep="_")
    balls_df.to_sql("balls", engine, if_exists="replace", index=False)
    print(f"Loaded {len(balls_df)} rows into 'balls' table.")
else:
    print("No ball data found.")
