import json
import os
import base64
import boto3
import pandas as pd
from io import StringIO
from requests import post, get

CLIENT_ID = "9e8bed9fbd0b43d99581a394a9b579c1"
CLIENT_SECRET = "e0ee4fbe0ca440a1b4ade78255512131"

client_id = CLIENT_ID
client_secret = CLIENT_SECRET

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

artist_columns = [
    "artist_id",
    "artist_name",
    "artist_popularity",
    "artist_followers",
    "artist_genres"
]

def get_token():
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token

def get_auth_header(token):
    return {"Authorization": "Bearer " + token}

def search_for_playlist(token, playlist_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={playlist_name}&type=playlist&limit=1"
    query_url = url + query
    result = get(query_url, headers=headers)
    json_result = json.loads(result.content)
    return json_result

def get_playlist(token, playlist_id):
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    headers = get_auth_header(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)
    return json_result

def get_features(token, songs_id):
    url = f"https://api.spotify.com/v1/audio-features?ids={songs_id}"
    headers = get_auth_header(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)
    return json_result

def get_artist(token, artist_id):
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = get_auth_header(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)
    return json_result

def lambda_handler(event, context):
    try:
        # Get the Spotify token
        token = get_token()

        # Example: Search for a playlist
        playlist_name = event.get('playlist_name', 'billboard hot 100')
        search_result = search_for_playlist(token, playlist_name)
        playlist_id = search_result['playlists']['items'][0]['id']

        # Get playlist details
        playlist_details = get_playlist(token, playlist_id)
        
        # Extract and transform playlist data
        tracks = playlist_details['tracks']['items']
        df_playlist = pd.DataFrame([{
            "top": idx + 1,
            "track_name": item['track']['name'],
            "track_popularity": item['track']['popularity'],
            "duration_ms": item['track']['duration_ms'],
            "artist_id": item['track']['artists'][0]['id'],
            "feats": ", ".join([artist['name'] for artist in item['track']['artists']]),
            "explicit": item['track']['explicit'],
            "album": item['track']['album']['name'],
            "type": item['track']['type'],
            "release_date": item['track']['album']['release_date'],
            "track_id": item['track']['id'],
            "popularity_rank": idx + 1
        } for idx, item in enumerate(tracks)])
        
        # Get track features and merge with playlist data
        tracks_id = list(df_playlist["track_id"])
        tracks_id = ",".join(tracks_id)
        df_features = get_features(token, tracks_id)["audio_features"]
        df_features = pd.DataFrame(df_features)
        df_features = df_features[["acousticness", "danceability", "energy", "instrumentalness", "liveness", "loudness", "valence", "mode", "tempo", "id"]]
        df_playlist = pd.merge(df_playlist, df_features, left_on='track_id', right_on='id')
        df_playlist = df_playlist.drop('id', axis=1)

        # Get artist details
        artist_ids = df_playlist['artist_id'].unique()
        artist_details = [get_artist(token, artist_id) for artist_id in artist_ids]
        df_artists = pd.DataFrame([{
            "artist_id": artist['id'],
            "artist_name": artist['name'],
            "artist_popularity": artist['popularity'],
            "artist_followers": artist['followers']['total'],
            "artist_genres": ", ".join(artist['genres'])
        } for artist in artist_details])

        # Merge artist details with playlist data
        df_final = pd.merge(df_playlist, df_artists, on='artist_id')

        # Convert final DataFrame to CSV and upload to S3
        s3 = boto3.client('s3')
        bucket_name = 'spotify-data-bucket3'
        
        # Upload merged data
        csv_buffer = StringIO()
        df_final.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=f'{playlist_name}_data.csv', Body=csv_buffer.getvalue())

        return {
            'statusCode': 200,
            'body': json.dumps('Data extracted and stored in S3 as a single CSV file!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
