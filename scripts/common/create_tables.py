from base import Base, engine
from tables import Artists, Tracks, Year, Genre, ArtistsRawAll, GenreRawAll, TracksRawAll, YearRawAll

for table in Base.metadata.tables:
    print(table)


if __name__ == "__main__":
    Base.metadata.create_all(engine)

