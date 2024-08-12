from transform_data import *
from sqlalchemy import create_engine

# Get track features and merge with playlist data
tracks_id = list(df_playlist["track_id"])
tracks_id = ",".join(tracks_id)
df_features = get_features(token, tracks_id)["audio_features"]
df_features = pd.DataFrame(df_features)
df_features = df_features[["acousticness", "danceability", "energy", "instrumentalness", "liveness", "loudness", "valence", "mode", "tempo", "id"]]
result_df = pd.merge(df_playlist, df_features, left_on='track_id', right_on='id')
result_df = result_df.drop('id', axis=1)

# Loading data to sqlite3
def load(result_df: pd.DataFrame, df_artists: pd.DataFrame) -> None:
    disk_engine = create_engine('sqlite:///Spotify_top100.db')
    
    # Load playlist data
    result_df.to_sql('Spotify_Top_100', disk_engine, if_exists='replace', index=False)
    
    # Load artist data
    df_artists.to_sql('Artist_Details', disk_engine, if_exists='replace', index=False)

# Execute the load function
load(result_df, df_artists)
