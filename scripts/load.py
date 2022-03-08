from sqlalchemy import cast, delete, Integer, Date, Float
from sqlalchemy.dialects.postgresql import insert
from common.base import session
from common.tables import Artists, Genre, Tracks, Year, ArtistsRawAll, TracksRawAll, GenreRawAll, YearRawAll


def insert_tracks():
    # select track id
    clean_track_id = session.query(Tracks.track_id)

    # select columns and cast appropriate when needed
    tracks_to_insert = session.query(
        cast(TracksRawAll.valence, Float),
        cast(TracksRawAll.year, Integer),
        cast(TracksRawAll.acousticness, Float),
        TracksRawAll.artists,
        cast(TracksRawAll.danceability, Float),
        cast(TracksRawAll.duration_ms, Float),
        cast(TracksRawAll.energy, Float),
        cast(TracksRawAll.explicit, Integer),
        TracksRawAll.id,
        cast(TracksRawAll.instrumentalness, Float),
        cast(TracksRawAll.key, Integer),
        cast(TracksRawAll.liveness, Float),
        cast(TracksRawAll.loudness, Float),
        cast(TracksRawAll.mode, Integer),
        cast(TracksRawAll.popularity, Integer),
        cast(TracksRawAll.release_date, Integer),
        cast(TracksRawAll.speechiness, Float),
        cast(TracksRawAll.tempo, Float)
    ).filter(~TracksRawAll.track_id.in_(clean_track_id))

    # print number of transactions to insert
    print("Tracks to insert: ", tracks_to_insert.count())

    columns = ['valence', 'year', 'acousticness', 'artists', 'danceability', 'duration_ms', 'energy', 'explicit', 'id',
               'instrumentalness', 'key', 'liveness', 'loudness', 'mode', 'name', 'popularity', 'release_date',
               'speechiness', 'tempo']

    stmt = insert(Tracks).from_select(columns, tracks_to_insert)
    session.execute(stmt)
    session.commit()


def delete_tracks():
    """
        Delete operation: delete any row not present in the last snapshot
    """
    raw_track_id = session.query(TracksRawAll.track_id)

    tracks_to_delete = session.query(Tracks).filter(~Tracks.track_id.in_(raw_track_id))

    # print number of transactions to delete
    print("Tracks to delete: ", tracks_to_delete.count())

    tracks_to_delete.delete(synchronize_session=False)
    session.commit()


def insert_genres():
    # select track id
    clean_genre_id = session.query(Genre.genre_id)

    # select columns and cast appropriate when needed
    genres_to_insert = session.query(
        cast(GenreRawAll.mode, Integer),
        GenreRawAll.genres,
        cast(GenreRawAll.acousticness, Float),
        cast(GenreRawAll.danceability, Float),
        cast(GenreRawAll.duration_ms, Float),
        cast(GenreRawAll.energy, Float),
        cast(GenreRawAll.instrumentalness, Float),
        cast(GenreRawAll.liveness, Float),
        cast(GenreRawAll.loudness, Float),
        cast(GenreRawAll.speechiness, Float),
        cast(GenreRawAll.tempo, Float),
        cast(GenreRawAll.valence, Float),
        cast(GenreRawAll.popularity, Integer),
        cast(GenreRawAll.key, Integer),
    ).filter(~GenreRawAll.genre_id.in_(clean_genre_id))

    # print number of transactions to insert
    print("Tracks to insert: ", genres_to_insert.count())

    columns = ['mode', 'genres', 'acousticness', 'danceability', 'duration_ms', 'energy', 'instrumentalness',
               'liveness',
               'loudness', 'speechiness', 'tempo', 'valence', 'popularity', 'key',
               ]

    stmt = insert(Genre).from_select(columns, genres_to_insert)
    session.execute(stmt)
    session.commit()


def delete_genres():
    """
        Delete operation: delete any row not present in the last snapshot
    """
    raw_genre_id = session.query(GenreRawAll.genre_id)

    genres_to_delete = session.query(Genre).filter(~Genre.genre_id.in_(raw_genre_id))

    # print number of transactions to delete
    print("Genres to delete: ", genres_to_delete.count())

    genres_to_delete.delete(synchronize_session=False)
    session.commit()


def main():
    print("[Load] Start")
    print("[Load] Inserting new rows")
    insert_tracks()
    insert_genres()
    print("[Load] Deleting rows not available in the new transformed data")
    delete_tracks()
    delete_genres()
    print("[Load] End")
