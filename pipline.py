import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Extract data from CSV
file_input = "~/assessment_finaledition/spotify_songs.csv"
df = pd.read_csv(file_input)

# cleaning missing values
df["track_name"].fillna("Unknown_track", inplace=True)
df["track_artist"].fillna("Unknown_artist", inplace=True)
df["track_album_name"].fillna("Unknown_album", inplace=True)
# Extract only the first 4 characters of the 'track_album_release_date' string
#df['track_album_release_date'] = df['track_album_release_date'].astype(str).str[:4]
df['track_album_release_date'] = df['track_album_release_date'].apply(lambda x: str(x)[:4])

# Convert 'duration_ms' to minutes and cast to int64
#df['duration_min'] = (df['duration_ms'] / (1000 * 60)).astype('int64')
# Drop the original 'duration_ms' column
#df.drop('duration_ms', axis=1, inplace=True)

df = df.assign(duration_min=(df['duration_ms'] / (1000 * 60)).astype('int64')).drop('duration_ms', axis=1)


# Snowflake account credentials and connection details
user = "EMADAM"
password = "Emad646261."
account = "MRVLRCM-BA89847"
database = "python_assessment"
schema = "python_assessment"

# Create a connection to Snowflake
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    database=database,
    schema=schema
)

# Write the data from the DataFrame to Snowflake
write_pandas(conn, df, "spotify_2", auto_create_table=True)

# Close the connection
conn.close()