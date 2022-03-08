from sqlalchemy import Column, Integer, String, Float, Identity
import common.base as b


class ArtistsRawAll(b.Base):
    __tablename__ = "artists_raw_all"
    artist_id = Column(Integer, Identity(start=42, cycle=True), primary_key=True)
    mode = Column(String(55))
    count = Column(String(55))
    acousticness = Column(String(55))
    artists = Column(String(500))
    danceability = Column(String(55))
    duration_ms = Column(String(55))
    energy = Column(String(55))
    instrumentalness = Column(String(55))
    liveness = Column(String(55))
    loudness = Column(String(55))
    speechiness = Column(String(55))
    tempo = Column(String(55))
    valence = Column(String(55))
    popularity = Column(String(55))
    key = Column(String(55))


class Artists(b.Base):
    __tablename__ = "artists"
    artist_id = Column(Integer, Identity(start=42, cycle=True), primary_key=True)
    mode = Column(Integer)
    count = Column(Integer)
    acousticness = Column(Float)
    artists = Column(String(500))
    danceability = Column(Float)
    duration_ms = Column(Float)
    energy = Column(Float)
    instrumentalness = Column(Float)
    liveness = Column(Float)
    loudness = Column(Float)
    speechiness = Column(Float)
    tempo = Column(Float)
    valence = Column(Float)
    popularity = Column(Float)
    key = Column(Integer)


class YearRawAll(b.Base):
    __tablename__ = "year_raw_all"
    year_id = Column(Integer, Identity(start=42, cycle=True), primary_key=True)
    mode = Column(String(55))
    year = Column(String(55))
    acousticness = Column(String(55))
    danceability = Column(String(55))
    duration_ms = Column(String(55))
    energy = Column(String(55))
    instrumentalness = Column(String(55))
    liveness = Column(String(55))
    loudness = Column(String(55))
    speechiness = Column(String(55))
    tempo = Column(String(55))
    valence = Column(String(55))
    popularity = Column(String(55))
    key = Column(String(55))


class Year(b.Base):
    __tablename__ = "year"
    year_id = Column(Integer, Identity(start=42, cycle=True), primary_key=True)
    mode = Column(Integer)
    year = Column(Integer)
    acousticness = Column(Float)
    danceability = Column(Float)
    duration_ms = Column(Float)
    energy = Column(Float)
    instrumentalness = Column(Float)
    liveness = Column(Float)
    loudness = Column(Float)
    speechiness = Column(Float)
    tempo = Column(Float)
    valence = Column(Float)
    popularity = Column(Float)
    key = Column(Integer)


class GenreRawAll(b.Base):
    __tablename__ = "genre_raw_all"
    genre_id = Column(Integer, Identity(start=42, cycle=True), primary_key=True)
    mode = Column(String(55))
    genres = Column(String(500))
    acousticness = Column(String(55))
    danceability = Column(String(55))
    duration_ms = Column(String(55))
    energy = Column(String(55))
    instrumentalness = Column(String(55))
    liveness = Column(String(55))
    loudness = Column(String(55))
    speechiness = Column(String(55))
    tempo = Column(String(55))
    valence = Column(String(55))
    popularity = Column(String(55))
    key = Column(String(55))


class Genre(b.Base):
    __tablename__ = "genre"
    genre_id = Column(Integer, Identity(start=42, cycle=True), primary_key=True)
    mode = Column(Integer)
    genres = Column(String(500))
    acousticness = Column(Float)
    danceability = Column(Float)
    duration_ms = Column(Float)
    energy = Column(Float)
    instrumentalness = Column(Float)
    liveness = Column(Float)
    loudness = Column(Float)
    speechiness = Column(Float)
    tempo = Column(Float)
    valence = Column(Float)
    popularity = Column(Float)
    key = Column(Integer)


class TracksRawAll(b.Base):
    __tablename__ = "track_raw_all"
    track_id = Column(Integer, Identity(start=42, cycle=True), primary_key=True)
    valence = Column(String(55))
    year = Column(String(55))
    acousticness = Column(String(55))
    artists = Column(String(5000))
    danceability = Column(String(55))
    duration_ms = Column(String(55))
    energy = Column(String(55))
    explicit = Column(String(55))
    id = Column(String(255))
    instrumentalness = Column(String(55))
    key = Column(String(55))
    liveness = Column(String(55))
    loudness = Column(String(55))
    mode = Column(String(55))
    name = Column(String(1000))
    popularity = Column(String(55))
    #release_date = Column(String(55))
    speechiness = Column(String(55))
    tempo = Column(String(55))


class Tracks(b.Base):
    __tablename__ = "track"
    track_id = Column(Integer, Identity(start=42, cycle=True), primary_key=True)
    valence = Column(Float)
    year = Column(Integer)
    acousticness = Column(Float)
    artists = Column(String(5000))
    danceability = Column(Float)
    duration_ms = Column(Float)
    energy = Column(Float)
    explicit = Column(Integer)
    id = Column(String(255))
    instrumentalness = Column(Float)
    key = Column(Integer)
    liveness = Column(Float)
    loudness = Column(Float)
    mode = Column(Integer)
    name = Column(String(1000))
    popularity = Column(Float)
    speechiness = Column(Float)
    tempo = Column(Float)
