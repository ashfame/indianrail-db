from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, distinct, func, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import sessionmaker
from logbook import debug, warn


rawdb = create_engine('sqlite:///raw.sqlite3', echo=False)
stagdb = create_engine('sqlite:///staging.sqlite3', echo=False)
proddb = create_engine('sqlite:///prod.sqlite3', echo=False)
Base1 = declarative_base(rawdb)
Base2 = declarative_base(stagdb)
Base3 = declarative_base(proddb)


def Field(type=None, **kwargs):
    type = type or String(512)
    return Column(type, **kwargs)


# Raw Database
class Raw(Base1):
    __tablename__ = 'raw'
    id = Column(Integer, primary_key=True)
    url = Column(String(100), index=True)
    html = Column(Text)
    crawled_on = Column(DateTime)

    def __init__(self, url, html):
        self.url = url
        self.html = html
        self.crawled_on = datetime.now()


# Staging Database
class TempTrain(Base2):
    __tablename__ = 'trains'
    id = Field(Integer, primary_key=True)
    url = Field(index=True)
    number = Field(index=True)
    name = Field()
    type = Field()
    zone = Field()
    from_station = Field()
    dep = Field()
    to_station = Field()
    arr = Field()
    duration = Field()
    halts = Field()
    days = Field()
    classes = Field()
    distance = Field()
    date_from = Field()
    date_to = Field()
    return_train = Field()

class TempSchedule(Base2):
    __tablename__ = 'schedule'
    id = Field(Integer, primary_key=True)
    train_number = Field()
    stop_number = Field()
    code = Field(index=True)
    station_name = Field()
    arrives = Field()
    departs = Field()
    halt = Field()
    pf = Field()
    day = Field()
    km = Field()
    speed = Field()
    elev = Field()
    zone = Field()
    address = Field()

# Production Database
class Station(Base3):
    __tablename__ = 'stations'
    id = Field(Integer, primary_key=True)
    code = Field(nullable=False, index=True, unique=True)
    name = Field(nullable=False)
    address = Field()
    state = Field()
    zone = Field()

class Train(Base3):
    __tablename__ = 'trains'
    id = Field(Integer, primary_key=True)
    number = Field(nullable=False, index=True, unique=True)
    name = Field(nullable=False)
    type = Field()
    zone = Field()
    from_station_code = Field()
    from_station_name = Field()
    to_station_code = Field()
    to_station_name = Field()
    departure = Field()
    arrival = Field()
    duration = Field()
    number_of_halts = Field()
    distance = Field()
    departure_days = Field()
    sunday = Field()
    monday = Field()
    tuesday = Field()
    wednesday = Field()
    thursday = Field()
    friday = Field()
    saturday = Field()
    classes = Field()
    first_ac = Field()
    second_ac = Field()
    third_ac = Field()
    first_class = Field()
    sleeper = Field()
    chair_car = Field()
    second_sitting = Field()
    return_train = Field()
    date_from = Field()
    date_to = Field()


class Schedule(Base3):
    __tablename__ = 'schedule'
    id = Field(Integer, primary_key=True)
    train_number = Field(nullable=False)
    stop_number = Field()
    station_code = Field(nullable=False)
    station_name = Field(nullable=False)
    arrival = Field()
    departure = Field()
    halt = Field()
    day = Field(Integer)
    distance_travelled = Field(Integer)


Base1.metadata.create_all()
Base2.metadata.create_all()
Base3.metadata.create_all()
db = sessionmaker()()

def exists(model, **kwargs):
    return bool(db.query(model).filter_by(**kwargs).first())

def row2dict(row):
    d = {}
    for columnName in row.__table__.columns.keys():
        d[columnName] = getattr(row, columnName)
    return d

