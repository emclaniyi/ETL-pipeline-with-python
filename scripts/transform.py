import re
import csv

from common.tables import ArtistsRawAll, TracksRawAll, YearRawAll, GenreRawAll
from common.base import session
from sqlalchemy import text

# settings
base_path = "C:\\Users\\EM\PycharmProjects\\ETL-pipeline-with-python"


# raw_path = f"{base_path}/raw/data/data.csv"


def transform_case(string):
    return string.lower()


def clean_text(string_input):
    new_text = re.sub("['\"\[\]()*$-]", "", string_input)
    return new_text


def truncate_table(table):
    session.execute(
        text(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;')
    )
    session.commit()


# transform tracks
def transform_tracks_data():
    with open(f"{base_path}/raw/data/tracks.csv", mode="r", encoding="utf8") as csv_file:
        # Read the new CSV snapshot ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our track objects
        track_raw_object = []
        for row in reader:
            # Apply transformations and save as track object
            track_raw_object.append(
                TracksRawAll(
                    valence=row['valence'],
                    year=row['year'],
                    acousticness=row['acousticness'],
                    artists=clean_text(transform_case(row["artists"])),
                    danceability=row['danceability'],
                    duration_ms=row['duration_ms'],
                    energy=row['energy'],
                    explicit=row['explicit'],
                    instrumentalness=row['instrumentalness'],
                    key=row['key'],
                    liveness=row['liveness'],
                    loudness=row['loudness'],
                    mode=row['mode'],
                    name=row['name'],
                    popularity=row['popularity'],
                    #release_date=row['release_date'],
                    speechiness=row['speechiness'],
                    tempo=row['tempo']
                )
            )
        # Save all new processed objects and commit
        session.bulk_save_objects(track_raw_object)
        session.commit()


# transform artists
def transform_artists_data():
    with open(f"{base_path}/raw/data/data_by_artist.csv", mode="r", encoding="utf8") as csv_file:
        # Read the new CSV snapshot ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our track objects
        artist_raw_object = []
        for row in reader:
            # Apply transformations and save as track object
            artist_raw_object.append(
                ArtistsRawAll(
                    mode=row['mode'],
                    count=row['count'],
                    acousticness=row['acousticness'],
                    artists=clean_text(transform_case(row["artists"])),
                    danceability=row['danceability'],
                    duration_ms=row['duration_ms'],
                    energy=row['energy'],
                    instrumentalness=row['instrumentalness'],
                    liveness=row['liveness'],
                    loudness=row['loudness'],
                    speechiness=row['speechiness'],
                    tempo=['tempo'],
                    valence=row['valence'],
                    popularity=row['popularity'],
                    key=row['key']
                )
            )
        # Save all new processed objects and commit
        session.bulk_save_objects(artist_raw_object)
        session.commit()


# transform genre
def transform_genres_data():
    with open(f"{base_path}/raw/data/data_by_genres.csv", mode="r", encoding="utf8") as csv_file:
        # Read the new CSV snapshot ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our track objects
        genre_raw_object = []
        for row in reader:
            # Apply transformations and save as track object
            genre_raw_object.append(
                GenreRawAll(
                    mode=row['mode'],
                    genres=clean_text(transform_case(row["genres"])),
                    acousticness=row['acousticness'],
                    danceability=row['danceability'],
                    duration_ms=row['duration_ms'],
                    energy=row['energy'],
                    instrumentalness=row['instrumentalness'],
                    liveness=row['liveness'],
                    loudness=row['loudness'],
                    speechiness=row['speechiness'],
                    tempo=['tempo'],
                    valence=row['valence'],
                    popularity=row['popularity'],
                    key=row['key']
                )
            )
        # Save all new processed objects and commit
        session.bulk_save_objects(genre_raw_object)
        session.commit()


# transform year
def transform_year_data():
    with open(f"{base_path}/raw/data/data_by_year.csv", mode="r", encoding="utf8") as csv_file:
        # Read the new CSV snapshot ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our track objects
        year_raw_object = []
        for row in reader:
            # Apply transformations and save as track object
            year_raw_object.append(
                YearRawAll(
                    mode=row['mode'],
                    year=row['year'],
                    acousticness=row['acousticness'],
                    danceability=row['danceability'],
                    duration_ms=row['duration_ms'],
                    energy=row['energy'],
                    instrumentalness=row['instrumentalness'],
                    liveness=row['liveness'],
                    loudness=row['loudness'],
                    speechiness=row['speechiness'],
                    tempo=['tempo'],
                    valence=row['valence'],
                    popularity=row['popularity'],
                    key=row['key']
                )
            )
        # Save all new processed objects and commit
        session.bulk_save_objects(year_raw_object)
        session.commit()


def main():
    print("[Transform] Start")
    print("[Transform] remove old data from table")
    truncate_table('track')
    truncate_table('artists')
    truncate_table('genre')
    truncate_table('year')
    print("[Transform] run transformation")
    transform_tracks_data()
    transform_artists_data()
    transform_genres_data()
    transform_year_data()
    print("[Transform] End")
