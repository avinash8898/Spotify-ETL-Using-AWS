import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
from botocore.exceptions import NoCredentialsError

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the Data Catalog database and table name
database_name = "spotify_db"
table_name = "spotify_data_bucket3"

# Define the S3 paths for temporary output
playlist_temp_output_path = "s3://spotify-data-bucket3-transformed/temp_playlist_data"
artist_temp_output_path = "s3://spotify-data-bucket3-transformed/temp_artist_data"

# Define the final S3 paths and filenames
playlist_final_path = "s3://spotify-data-bucket3-transformed/playlist_data.csv"
artist_final_path = "s3://spotify-data-bucket3-transformed/artist_data.csv"

# Debugging: Ensure paths are correct
assert playlist_temp_output_path, "Playlist temp output path is empty"
assert artist_temp_output_path, "Artist temp output path is empty"

# Load the data from the Data Catalog
data = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name)

# Define columns for playlist and artist
playlist_columns = [
    "top", "track_name", "track_popularity", "duration_ms", "artist_id",
    "feats", "explicit", "album", "type", "release_date", "track_id",
    "popularity_rank", "acousticness", "danceability", "energy", "instrumentalness",
    "liveness", "loudness", "valence", "mode", "tempo"
]

artist_columns = [
    "artist_id", "artist_name", "artist_popularity", "artist_followers", "artist_genres"
]

# Define the custom transform function
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    df_playlist = dfc.select_fields(playlist_columns).toDF()
    df_artist = dfc.select_fields(artist_columns).toDF().dropDuplicates(["artist_id"])

    # Coalesce to 1 partition to produce a single output file
    df_playlist = df_playlist.coalesce(1)
    df_artist = df_artist.coalesce(1)
    
    dyf_playlist = DynamicFrame.fromDF(df_playlist, glueContext, "dyf_playlist")
    dyf_artist = DynamicFrame.fromDF(df_artist, glueContext, "dyf_artist")
    
    return DynamicFrameCollection({"dyf_playlist": dyf_playlist, "dyf_artist": dyf_artist}, glueContext)

# Apply the transform
transformed_dfc = MyTransform(glueContext, data)

# Get the individual DynamicFrames from the collection
dyf_playlist = transformed_dfc.select("dyf_playlist")
dyf_artist = transformed_dfc.select("dyf_artist")

# Convert DynamicFrames to DataFrames
df_playlist = dyf_playlist.toDF()
df_artist = dyf_artist.toDF()

# Debugging: Print the paths to ensure they are correct
print(f"Playlist temp output path: {playlist_temp_output_path}")
print(f"Artist temp output path: {artist_temp_output_path}")

# Write the playlist data to temporary S3 location in CSV format
df_playlist.write.mode('overwrite').csv(playlist_temp_output_path, header=True)

# Write the artist data to temporary S3 location in CSV format
df_artist.write.mode('overwrite').csv(artist_temp_output_path, header=True)

# Commit the job
job.commit()

# Rename the output files
s3 = boto3.client('s3')

def rename_s3_object(source_bucket, source_key, dest_bucket, dest_key):
    try:
        # Copy the object
        s3.copy_object(Bucket=dest_bucket, CopySource={'Bucket': source_bucket, 'Key': source_key}, Key=dest_key)
        # Delete the original object
        s3.delete_object(Bucket=source_bucket, Key=source_key)
    except NoCredentialsError:
        print("Credentials not available")

# Extract bucket names and keys
def get_bucket_and_key(path):
    parts = path.replace("s3://", "").split('/', 1)
    return parts[0], parts[1] if len(parts) > 1 else ""

playlist_temp_bucket, playlist_temp_key_prefix = get_bucket_and_key(playlist_temp_output_path)
artist_temp_bucket, artist_temp_key_prefix = get_bucket_and_key(artist_temp_output_path)

# Get the temporary file keys
response_playlist = s3.list_objects_v2(Bucket=playlist_temp_bucket, Prefix=playlist_temp_key_prefix)
response_artist = s3.list_objects_v2(Bucket=artist_temp_bucket, Prefix=artist_temp_key_prefix)

playlist_temp_file_key = None
artist_temp_file_key = None

if 'Contents' in response_playlist:
    for obj in response_playlist['Contents']:
        if obj['Key'].endswith('.csv'):
            playlist_temp_file_key = obj['Key']
            break

if 'Contents' in response_artist:
    for obj in response_artist['Contents']:
        if obj['Key'].endswith('.csv'):
            artist_temp_file_key = obj['Key']
            break

# Rename the files
if playlist_temp_file_key:
    rename_s3_object(playlist_temp_bucket, playlist_temp_file_key, playlist_temp_bucket, 'playlist_data.csv')

if artist_temp_file_key:
    rename_s3_object(artist_temp_bucket, artist_temp_file_key, artist_temp_bucket, 'artist_data.csv')