""" This modules contains Workers """
import time
import logging
import datetime
import json
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
        self.db.clean_invalid_samples()
        self.db.begin_sample()
        for result in self.api:
            time.sleep(1)  # delay to make sure we don't trigger twitch ddos
            for stream in result:
                to_store = TwitchStreamResultAdapter.adapt(stream)
                self.db.insert_stream(to_store)
        if self.api.failed:
            logging.error("Twitch api failed")
            self.failed = True
        self.db.complete_sample()


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

    If last results are older then that it will zero the games stats table
    """

    def __init__(self, db):
        super().__init__("AggregateData")
        self.db = db

    def work(self):
        """ Begin work. All Workers should have this method """
        number_of_samples = 12 
        graphs = {}
        for i in range(0,number_of_samples):
            sample = self.db.return_range(i,0)
            if sample == []:
                break;
            sample_from = sample[0]
            sample_to = sample[1]
            logging.info("Sample [{}] : {} to {}".format(i,sample_from, sample_to))
            data = self.db.retrive_by_time(sample_from, sample_to)
            compiler = StreamDataCompiler()
            for stream in data:
                compiler.parse_data_unit(stream)
            
            for game_id, stream in compiler.data().items():
                if not game_id:
                    continue

                if not game_id in graphs:
                    graphs[game_id] = {} 
                    graphs[game_id]['game_id'] = stream.game_id
                    graphs[game_id]['viewer_count'] = stream.viewer_count
                    graphs[game_id]['streams_count'] = stream.stream_count
                    graphs[game_id]['distribution'] = stream.distribution()
                    graphs[game_id]['graphs'] = []
                graphs[game_id]['graphs'].append( { 
                            "date" : sample_to.strftime("%Y-%m-%d %H:%M:%S %z"),
                            "interval" : 10,
                            "viewer_count" : stream.viewer_count,
                            "streams_count" : stream.stream_count
                            }
                        )

        for game_id, stream in graphs.items():
             self.db.mark_for_cache(game_id)
             self.db.create_or_update_game(
                game_id=game_id,
                data=stream,
                )
    
