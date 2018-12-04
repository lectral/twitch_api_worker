""" This modules contains Workers """
import time
import logging
import datetime
from twitch_api_worker.twitch import TwitchGamesApi
from twitch_api_worker.data_compilers import StreamDataCompiler, \
    GameDataResultAdapter, TwitchStreamResultAdapter


class Worker:
    """ Base class for all workers """

    def __init__(self, name):
        self.name = name
        self.failed = False

    def work(self):
        """ Override this method in children """
        raise NotImplementedError


class CrawlerWorker(Worker):
    """ This worker gathers all the current polish streams from TwitchAPI
    and puts them in the stream table
    """

    def __init__(self, api, db):
        super().__init__("CrawlerWorker")
        self.api = api
        self.db = db

    def work(self):
        """ Begin work. All Workers should have this method """
        self.db.create_tables()
        for result in self.api:
            time.sleep(1)  # delay to make sure we don't trigger twitch ddos
            for stream in result:
                to_store = TwitchStreamResultAdapter.adapt(stream)
                self.db.insert_stream(to_store)
        if self.api.failed:
            logging.error("Twitch api failed")
            self.failed = True


class GamesCacheWorker(Worker):
    """ This workers is responsible to with filling title field in
    games_cache table
    """

    def __init__(self, db):
        super().__init__("GamesCache")
        self.db = db

    def work(self):
        """ Begin work. All Workers should have this method """
        all_games = self.db.get_games_cache()
        chunked_arr = self.chunk_list(all_games, 100)
        for game_chunk in chunked_arr:
            query_string = "?"
            for game in game_chunk:
                query_string += "id={}&".format(game.game_id)
            from_twitch = TwitchGamesApi.get(query_string)
            for twitch_data in from_twitch['data']:
                self.db.update_game_cache(
                    twitch_data['id'], twitch_data['name'])

    def chunk_list(self, arr, chunk_size):
        chunk_size = max(1, chunk_size)
        return(arr[i:i + chunk_size] for i in range(0, len(arr), chunk_size))


class AggregateDataWorker(Worker):
    """  Aggregates data about streams into games table

    Args:
        db(WorkerDb) : instance of the WorkerDb

    This adjustment is neccessery if twitch api had slowdown.
    If last results are older then that it will zero the games stats table
    """
    WORKER_ADJUSTMENT = 5  # minutes

    def __init__(self, db):
        super().__init__("AggregateData")
        self.db = db

    def work(self):
        """ Begin work. All Workers should have this method """
        now = self.retrive_ago(self.WORKER_ADJUSTMENT)
        min_10 = self.retrive_ago(10 + self.WORKER_ADJUSTMENT)
        min_60 = self.retrive_ago(60 + self.WORKER_ADJUSTMENT)
        c_now = StreamDataCompiler()
        c_10 = StreamDataCompiler()
        c_60 = StreamDataCompiler()
        for stream in now:
            c_now.parse_data_unit(stream)
        for stream in min_10:
            c_10.parse_data_unit(stream)
        for stream in min_60:
            c_60.parse_data_unit(stream)

        for game_id, stream in c_now.data().items():
            self.db.mark_for_cache(game_id)
            self.db.create_or_update_game(
                game_id=game_id,
                data=GameDataResultAdapter.adapt(
                    stream,
                    c_10.get(game_id),
                    c_60.get(game_id)
                ))

    def retrive_ago(self, minutes):
        """ Retrives data that where stored #minutes ago """
        ago = self.minutes_ago(minutes)
        to = ago + datetime.timedelta(minutes=self.WORKER_ADJUSTMENT)
        return self.db.retrive_by_time(ago, to)
    
    @staticmethod
    def now():
        """ Return now() time """
        return datetime.datetime.now()

    def minutes_ago(self, minutes):
        """ Calculate what time was #minutes ago
        Args:
            minutes (int) : minutes ago

        Returns:
           datatime: datatime object with time #minutes ago
        """
        return self.now() - datetime.timedelta(minutes=minutes)
