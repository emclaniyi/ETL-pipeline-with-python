from sqlalchemy import Column, Integer, String, Float
from common.base import Base


class Artists(Base):
    __tablename__ = "artists"
    id = Column(Integer, primary_key=True, autoincrement=True)
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


class Year(Base):
    __tablename__ = "year"
    id = Column(Integer, primary_key=True, autoincrement=True)
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


class Genre(Base):
    __tablename__ = "genre"
    id = Column(Integer, primary_key=True, autoincrement=True)
    mode = Column(Integer)
    genre = Column(String(500))
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


class Track(Base):
    __tablename__ = "track"
    valence = Column(Float)
    year = Column(Integer)
    acousticness = Column(Float)
    artists = Column(String(5000))
    danceability = Column(Float)
    duration_ms = Column(Float)
    energy = Column(Float)
    explicit = Column(Integer)
    id = Column(Integer, primary_key=True)
    instrumentalness = Column(Float)
    key = Column(Integer)
    liveness = Column(Float)
    loudness = Column(Float)
    mode = Column(Integer)
    name = Column(String(1000))
    popularity = Column(Integer)
    release_date = Column(Integer)
    speechiness = Column(Float)
    tempo = Column(Float)