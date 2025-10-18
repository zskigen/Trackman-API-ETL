import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

# Load environment variables
load_dotenv()

# TrackMan creds (already in your .env)
CLIENT_ID = os.getenv("TRACKMAN_CLIENT_ID")
CLIENT_SECRET = os.getenv("TRACKMAN_CLIENT_SECRET")

# Postgres connection settings
PGUSER = os.getenv("PGUSER")
PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT")
PGDATABASE = os.getenv("PGDATABASE")

# Build SQLAlchemy connection string
engine = create_engine(f"postgresql://{PGUSER}@{PGHOST}:{PGPORT}/{PGDATABASE}")

# Load your flattened CSV
df = pd.read_csv("sessions_2024.csv")
df.to_sql("sessions", engine, if_exists="replace", index=False)

print("Data loaded successfully into the 'sessions' table.")
