from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Session

# create engine
engine = create_engine("postgresql://postgres:pharezpic123@localhost:5432/spotify_data")

session = Session(engine)

Base = declarative_base()
