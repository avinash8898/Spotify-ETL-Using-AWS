import json
from requests import post,get
from Spotify_API import *

def search_for_playlist(token,playlist_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={playlist_name}&type=playlist&limit=1" 
    query_url = url + query
    result = get(query_url,headers=headers)
    json_result = json.loads(result.content)
    return json_result
    
def get_playlist(token,playlist_id):
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    headers = get_auth_header(token)
    result = get(url,headers=headers)
    json_result = json.loads(result.content)
    return json_result

def get_artist(token,artist_id):
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = get_auth_header(token)
    result = get(url,headers=headers)
    json_result = json.loads(result.content)
    return json_result

def get_features(token,songs_id):
    url = f"https://api.spotify.com/v1/audio-features?ids={songs_id}"
    headers = get_auth_header(token)
    result = get(url,headers=headers)
    json_result = json.loads(result.content)
    return json_result