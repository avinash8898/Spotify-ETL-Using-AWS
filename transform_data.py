import pandas as pd
import numpy as np
from extract_data import *

# Fetch token and playlist information
token = get_token()
playlist_info = search_for_playlist(token, "billboard hot 100")
playlist_name = playlist_info["playlists"]["items"][0]["name"]
playlist_description = playlist_info["playlists"]["items"][0]["description"]
playlist_id = playlist_info["playlists"]["items"][0]["id"]

print(f"Playlist Name: {playlist_name}")
print(f"Playlist Description: {playlist_description}")
print(f"Playlist ID: {playlist_id}")

playlist_json = get_playlist(token, playlist_id)

# Define columns for the Playlist
playlist_columns = [
    "top",
    "track_name",
    "track_popularity",
    "duration_ms",
    "artist_id",
    "feats",
    "explicit",
    "album",
    "type",
    "release_date",
    "track_id",
    "popularity_rank"
]

# Define columns for the Artist_Details
artist_columns = [
    "artist_id",
    "artist_name",
    "artist_popularity",
    "artist_followers",
    "artist_genres"
]

# Initialize DataFrames
df_playlist = pd.DataFrame(columns=playlist_columns)
df_artists = pd.DataFrame(columns=artist_columns)

# Extract track and artist information
track_info = playlist_json["tracks"]["items"]
feats = []

for i in range(len(track_info)):
    track = track_info[i]["track"]
    artist_id = track["artists"][0]["id"]
    
    # Extract track data
    track_data = {
        "top": i + 1,
        "track_name": track["name"],
        "track_popularity": track["popularity"],
        "duration_ms": track["duration_ms"],
        "artist_id": artist_id,
        "feats": "",
        "explicit": track["explicit"],
        "album": track["album"]["name"],
        "type": track["album"]["album_type"],
        "release_date": track["album"]["release_date"],
        "track_id": track["id"],
        "popularity_rank": 0  # Placeholder for popularity rank
    }
    
    # Extract features if there are multiple artists
    if len(track["artists"]) > 1:
        feats = [track["artists"][j + 1]["name"] for j in range(len(track["artists"]) - 1)]
        track_data["feats"] = "|".join(feats)
    else:
        track_data["feats"] = np.nan
    
    # Append track data to playlist DataFrame
    df_playlist = pd.concat([df_playlist, pd.DataFrame([track_data])], ignore_index=True)
    
    # Extract artist data
    artist_info = get_artist(token, artist_id)
    artist_data = {
        "artist_id": artist_id,
        "artist_name": track["artists"][0]["name"],
        "artist_popularity": artist_info["popularity"],
        "artist_followers": artist_info["followers"]["total"],
        "artist_genres": "|".join(artist_info["genres"]) if artist_info["genres"] else np.nan
    }
    
    # Append artist data to artists DataFrame, avoiding duplicates
    if artist_id not in df_artists["artist_id"].values:
        df_artists = pd.concat([df_artists, pd.DataFrame([artist_data])], ignore_index=True)

# Assign popularity rank
df_playlist["popularity_rank"] = df_playlist["track_popularity"].rank(ascending=False, method='min').astype(int)

# Print the DataFrames to verify the results
# print("Playlist DataFrame")
# print(df_playlist)
# print("\nArtist Details DataFrame")
# print(df_artists)
