import logging
import os
import datetime
from sqlobject import StringCol, BLOBCol, TimestampCol, IntCol, SQLObject, ForeignKey, sqlhub, connectionForURI, DateTimeCol, JSONCol, mysql, dberrors
from sqlobject.sqlbuilder import AND, OR


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

class StreamSamples(SQLObject):
    started = DateTimeCol(default=None)
    completed = DateTimeCol(default=None)

class Games(SQLObject):
    game_id = StringCol(alternateID=True, length=15)
    viewer_count = IntCol()
    streams_count = IntCol()
    stream_sample_id = IntCol()
    distribution = StringCol()
    graphs = JSONCol()
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

        # uri = 'sqlite:/home/lectral/db3.sqlite'
        MySQLConnection = mysql.builder()
        sqlhub.processConnection = MySQLConnection(host=host,user=user,db=database, port=int(port),password=passwd, charset='utf8')
        self.already_cached = []
        self.already_stored = []
        self.last_started_sample = None

    def insert_stream(self, data):
        """ Stores dictionary in the database """
        if data['stream_id'] in self.already_stored:
            return
        data_to_store = data
        self.already_stored.append(data['stream_id'])
        data_to_store['created_on'] = datetime.datetime.now()
        data_to_store['user_name'] = str(data_to_store['user_name'])
        try:
            Stream(**data_to_store)
        except dberrors.OperationalError:
            logging.warning("Failed to insert into db: {}".format(data_to_store)) 

    def begin_sample(self):
        ssample = StreamSamples(started=datetime.datetime.now())
        self.last_started_sample = ssample.id

    def clean_invalid_samples(self):
        invalid_samples = StreamSamples.select(StreamSamples.q.completed == None)
        for sample in invalid_samples:
            logging.info("Cleaning invalid sample {}".format(sample.id))
            sample.destroySelf();

    def clean_no_longer_streamed_games(self, sample_id):
        invalid_games = Games.select(OR(Games.q.stream_sample_id != sample_id, Games.q.stream_sample_id == None) )
        logging.info("Cleaning no longer streamed games: {} entries".format(invalid_games.count()))
        for game in invalid_games:
            game.viewer_count = 0
            game.streams_count = 0
            game.graphs = [] 
            game.distribution = ""
            game.updated_on = datetime.datetime.now()

    def complete_sample(self):
        ssample = StreamSamples.get(self.last_started_sample)
        ssample.completed = datetime.datetime.now() 

    def retrive_by_time(self, date_from, date_to):
        streams = Stream.select(
            AND(Stream.q.created_on >= date_from,
                Stream.q.created_on <= date_to
                ))
        return list(streams)
    
    def return_range(self, ago, rng):
        ls = list(StreamSamples.select().limit(100).orderBy("-completed"))
        try:
            sample_to = ls[ago]
        except IndexError:
            return []
        sample_from = ls[ago+rng]
        end = ls[ago].completed
        begin = ls[ago+rng].started
        return [begin, end, sample_from.id, sample_to.id]

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
            try:
                gamescache[0].set(
                    title=title
                 )
            except: 
                logging.warning("{} - could not be cached".format(game_id))
            self.already_cached.append(game_id)
            logging.debug("{} - title cached.".format(game_id))

    def get_games_cache(self):
        query = GamesCache.select(GamesCache.q.title == None)
        return list(query)

    @staticmethod
    def create_tables():
        """ Creates tables if they do not exist """
        Stream.createTable(ifNotExists=True)
        StreamSamples.createTable(ifNotExists=True)
        GamesCache.createTable(ifNotExists=True)
        Games.createTable(ifNotExists=True)
