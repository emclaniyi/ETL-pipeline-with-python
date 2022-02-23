import re
import csv

from common.tables import Artists, Year, Genre, Track
from common.base import session
from sqlalchemy import text

# settings
base_path = "C:\\Users\\User\\PycharmProjects\\ETL-pipeline-with-python"
# raw_path = f"{base_path}/raw/data/data.csv"


def transform_case(string):
    return string.lower()


def clean_text(string_input):
    new_text = re.sub("['\"\[\]()*$]", "", string_input)
    return new_text


def truncate_table(table):
    session.execute(
        text(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;')
    )
    session.commit()


# transform tracks
def transform_tracks_data():
    with open(f"{base_path}/raw/data/data.csv", mode="r", encoding="utf8") as csv_file:
        # Read the new CSV snapshot ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our track objects
        track_object = []
        for row in reader:
            # Apply transformations and save as track object
            track_object.append(
                Track(
                    artists=clean_text(transform_case(row["artists"])),
                )
            )
        # Save all new processed objects and commit
        session.bulk_save_objects(track_object)
        session.commit()


# transform artists
def transform_artists_data():
    with open(f"{base_path}/raw/data/data_by_artist.csv", mode="r", encoding="utf8") as csv_file:
        # Read the new CSV snapshot ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our track objects
        artist_object = []
        for row in reader:
            # Apply transformations and save as track object
            artist_object.append(
                Artists(
                    artists=clean_text(transform_case(row["artists"])),
                )
            )
        # Save all new processed objects and commit
        session.bulk_save_objects(artist_object)
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
    print("[Transform] End")
