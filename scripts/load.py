from sqlalchemy import cast, Integer, Float, String
from sqlalchemy.dialects.postgresql import insert, NUMERIC
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
        cast(TracksRawAll.id, String),
        cast(TracksRawAll.instrumentalness, Float),
        cast(TracksRawAll.key, Integer),
        cast(TracksRawAll.liveness, Float),
        cast(TracksRawAll.loudness, Float),
        cast(TracksRawAll.mode, Integer),
        TracksRawAll.name,
        cast(TracksRawAll.popularity, Float),
        cast(TracksRawAll.speechiness, Float),
        cast(TracksRawAll.tempo, Float)
    ).filter(~TracksRawAll.track_id.in_(clean_track_id))

    # print number of transactions to insert
    print("Tracks to insert: ", tracks_to_insert.count())

    columns = ['valence', 'year', 'acousticness', 'artists', 'danceability', 'duration_ms', 'energy', 'explicit', 'id',
               'instrumentalness', 'key', 'liveness', 'loudness', 'mode', 'name', 'popularity',
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
        cast(GenreRawAll.popularity, Float),
        cast(GenreRawAll.key, Integer),
    ).filter(~GenreRawAll.genre_id.in_(clean_genre_id))

    # print number of transactions to insert
    print("Genres to insert: ", genres_to_insert.count())

    columns = ['mode', 'genres', 'acousticness', 'danceability', 'duration_ms', 'energy', 'instrumentalness',
               'liveness', 'loudness', 'speechiness', 'tempo', 'valence', 'popularity', 'key']

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


def insert_artists():
    # select track id
    clean_artist_id = session.query(Artists.artist_id)

    # select columns and cast appropriate when needed
    artists_to_insert = session.query(
        cast(ArtistsRawAll.mode, Integer),
        cast(ArtistsRawAll.count, Integer),
        cast(ArtistsRawAll.acousticness, Float),
        ArtistsRawAll.artists,
        cast(ArtistsRawAll.danceability, Float),
        cast(ArtistsRawAll.duration_ms, Float),
        cast(ArtistsRawAll.energy, Float),
        cast(ArtistsRawAll.instrumentalness, Float),
        cast(ArtistsRawAll.liveness, Float),
        cast(ArtistsRawAll.loudness, Float),
        cast(ArtistsRawAll.speechiness, Float),
        cast(ArtistsRawAll.tempo, Float),
        cast(ArtistsRawAll.valence, Float),
        cast(ArtistsRawAll.popularity, Float),
        cast(ArtistsRawAll.key, Integer),
    ).filter(~ArtistsRawAll.artist_id.in_(clean_artist_id))

    # print number of transactions to insert
    print("Artists to insert: ", artists_to_insert.count())

    columns = ['mode', 'count', 'acousticness', 'artists', 'danceability', 'duration_ms', 'energy', 'instrumentalness',
               'liveness', 'loudness', 'speechiness', 'tempo', 'valence', 'popularity', 'key']

    stmt = insert(Artists).from_select(columns, artists_to_insert)
    session.execute(stmt)
    session.commit()


def delete_artists():
    """
        Delete operation: delete any row not present in the last snapshot
    """
    raw_artist_id = session.query(ArtistsRawAll.artist_id)

    artists_to_delete = session.query(Artists).filter(~Artists.artist_id.in_(raw_artist_id))

    # print number of transactions to delete
    print("Artists to delete: ", artists_to_delete.count())

    artists_to_delete.delete(synchronize_session=False)
    session.commit()


def insert_year():
    # select track id
    clean_year_id = session.query(Year.year_id)

    # select columns and cast appropriate when needed
    year_to_insert = session.query(
        cast(YearRawAll.mode, Integer),
        cast(YearRawAll.year, Integer),
        cast(YearRawAll.acousticness, Float),
        cast(YearRawAll.danceability, Float),
        cast(YearRawAll.duration_ms, Float),
        cast(YearRawAll.energy, Float),
        cast(YearRawAll.instrumentalness, Float),
        cast(YearRawAll.liveness, Float),
        cast(YearRawAll.loudness, Float),
        cast(YearRawAll.speechiness, Float),
        cast(YearRawAll.tempo, Float),
        cast(YearRawAll.valence, Float),
        cast(YearRawAll.popularity, Float),
        cast(YearRawAll.key, Integer),
    ).filter(~YearRawAll.year_id.in_(clean_year_id))

    # print number of transactions to insert
    print("Year to insert: ", year_to_insert.count())

    columns = ['mode', 'year', 'acousticness', 'danceability', 'duration_ms', 'energy', 'instrumentalness',
               'liveness', 'loudness', 'speechiness', 'tempo', 'valence', 'popularity', 'key']

    stmt = insert(Year).from_select(columns, year_to_insert)
    session.execute(stmt)
    session.commit()


def delete_year():
    """
        Delete operation: delete any row not present in the last snapshot
    """
    raw_year_id = session.query(YearRawAll.year_id)

    year_to_delete = session.query(Year).filter(~Year.year_id.in_(raw_year_id))

    # print number of transactions to delete
    print("Year to delete: ", year_to_delete.count())

    year_to_delete.delete(synchronize_session=False)
    session.commit()


def main():
    print("[Load] Start")
    print("[Load] Inserting new rows")
    insert_tracks()
    insert_artists()
    insert_year()
    insert_genres()
    print("[Load] Deleting rows not available in the new transformed data")
    delete_tracks()
    delete_artists()
    delete_year()
    delete_genres()
    print("[Load] End")
