import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# -----------------------------------
# Load environment variables
# -----------------------------------
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

# -----------------------------------
# SQL query Matt wants
# -----------------------------------
QUERY = """
SELECT
    p."taggerBehavior_pitchNo"           AS pitch_no,
    to_timestamp(p."localDateTime", 'MM/DD/YYYY HH24:MI:SS') AS date,
    p."taggerBehavior_pAofinning"        AS p_aof_inning,
    p."taggerBehavior_pitchofPA"         AS pitchof_pa,
    p."pitcher_name"                     AS pitcher,
    p."pitcher_id"                       AS pitcher_id,
    p."pitcher_throws"                   AS pitcher_throws,
    p."pitcher_team"                     AS pitcher_team,
    p."pitchTag_taggedPitchType"         AS tagged_pitch_type,

    b."pitch_release_relSpeed"           AS rel_speed,
    b."pitch_release_relHeight"          AS rel_height,
    b."pitch_release_relSide"            AS rel_side,
    b."pitch_release_vertRelAngle"       AS vert_rel_angle,
    b."pitch_release_horzRelAngle"       AS horz_rel_angle,
    b."pitch_release_spinRate"           AS spin_rate,
    b."pitch_movement_spinAxis"          AS spin_axis,
    b."pitch_release_extension"          AS extension,
    b."pitch_movement_inducedVertBreak"  AS induced_vert_break,
    b."pitch_movement_vertBreak"         AS vert_break,
    b."pitch_movement_horzBreak"         AS horz_break

FROM plays p
JOIN balls b
  ON p."playID" = b."playId"

WHERE to_timestamp(p."localDateTime", 'MM/DD/YYYY HH24:MI:SS') >= '2024-01-01'

ORDER BY
    to_timestamp(p."localDateTime", 'MM/DD/YYYY HH24:MI:SS'),
    p."taggerBehavior_pitchNo";
"""

# -----------------------------------
# Run query + export to CSV
# -----------------------------------
df = pd.read_sql(QUERY, engine)
df.to_csv("pitch_data.csv", index=False)

print(f"Exported {len(df)} rows to pitch_data.csv")
