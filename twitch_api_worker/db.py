import os
import datetime
from sqlobject import StringCol, BLOBCol, TimestampCol, IntCol, SQLObject, ForeignKey, sqlhub, connectionForURI
from sqlobject.sqlbuilder import AND
import logging


class Stream(SQLObject):
    """ Streams table definition """
    stream_id = StringCol()
    game_id = StringCol()
    viewer_count = IntCol()
    user_name = StringCol()
    user_id = StringCol()
    language = StringCol()
    created_on = TimestampCol()


class GamesCache(SQLObject):
    game_id = StringCol(alternateID=True, unique=True, length=15)
    title = StringCol(default=None)


class Games(SQLObject):
    game_id = StringCol(alternateID=True, length=15)
    viewer_count = IntCol()
    streams_count = IntCol()
    viewers_10_min_ago = IntCol(default=None)
    viewers_60_min_ago = IntCol(default=None)
    streams_10_min_ago = IntCol(default=None)
    streams_60_min_ago = IntCol(default=None)
    distribution = StringCol()
    updated_on = TimestampCol()


class WorkerDb:
    """ Handles data storage and retrival """
    TABLES = {
        "games": Games,
        "games_cache": GamesCache,
        "streams": Stream
    }

    def __init__(self):
        passwd = os.environ.get("TWITCH_WORKER_DB_PASSWORD", None)
        user = os.environ.get("TWITCH_WORKER_DB_USER", None)
        host = os.environ.get("TWITCH_WORKER_DB_HOST", None)
        database = os.environ.get("TWITCH_WORKER_DB_DATABASE", None)
        port = os.environ.get("TWITCH_WORKER_DB_PORT", None)
        uri = 'mysql://{}:{}@{}:{}/{}'.format(user,
                                              passwd, host, port, database)
        uri = 'sqlite:/home/lectral/db3.sqlite'
        sqlhub.processConnection = connectionForURI(uri)
        self.already_cached = []
        self.already_stored = []

    def insert_stream(self, data):
        """ Stores dictionary in the database """
        if data['stream_id'] in self.already_stored:
            return
        data_to_store = data
        self.already_stored.append(data['stream_id'])
        data_to_store['created_on'] = datetime.datetime.now()
        data_to_store['user_name'] = data_to_store['user_name']
        Stream(**data_to_store)

    def retrive_by_time(self, date_from, date_to):
        streams = Stream.select(
            AND(Stream.q.created_on >= date_from,
                Stream.q.created_on <= date_to
                ))
        return list(streams)

    def create_or_update_game(self, game_id, data):
        games = Games.select(Games.q.game_id == game_id)
        if(games.count() == 0):
            data['updated_on'] = datetime.datetime.now()
            Games(**data)
        else:
            data['updated_on'] = datetime.datetime.now()
            games[0].set(**data)

    def mark_for_cache(self, game_id):
        if game_id in self.already_cached:
            logging.debug(
                "{} - already cached in this session. Skipping".format(game_id))
            return
        gamescache = GamesCache.select(GamesCache.q.game_id == game_id)
        if(gamescache.count() == 0):
            GamesCache(game_id=game_id
                       )
            self.already_cached.append(game_id)
            logging.debug("{} - added to be cached.".format(game_id))
        else:
            self.already_cached.append(game_id)
            logging.debug("{} - already cached.".format(game_id))

    def update_game_cache(self, game_id, title):
        gamescache = GamesCache.select(GamesCache.q.game_id == game_id)
        if(gamescache.count() != 0):
            gamescache[0].set(
                title=title
            )
            self.already_cached.append(game_id)
            logging.debug("{} - title cached.".format(game_id))

    def get_games_cache(self):
        return list(GamesCache.select(GamesCache.q.title == None))

    @staticmethod
    def create_tables():
        """ Creates tables if they do not exist """
        Stream.createTable(ifNotExists=True)
        GamesCache.createTable(ifNotExists=True)
        Games.createTable(ifNotExists=True)
