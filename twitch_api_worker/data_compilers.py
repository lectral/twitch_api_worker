""" Data compilers are used to process stream data """
import logging


class StreamDataCompiler:
    """ Proccess data for each stream and returns array of GameDataResults 
    which hold stream stats connected to #game_id
    
    """
    def __init__(self):
        self.__data = {}
        self.game_ids = []
        self.current_gid = None

    def data(self):
        """ Returns compiled data """
        return self.__data

    def get(self, game_id):
        """ Get data for specyfic game_id
        Args:
            game_id (str) : twitch id of the game
        Returns:
            GameDataResult : result containg game stats
        """
        if game_id in self.__data:
            return self.__data[game_id]
        else:
            return GameDataResult(game_id)

    def parse_data_unit(self, data):
        """ Parses single stream """
        self.current_gid = data.game_id
        self.game_ids.append(data.game_id)
        if data.game_id in self.__data:
            self.__data[data.game_id].add_stream(
                data.user_id, data.user_name, data.viewer_count)
        else:
            self.__data[data.game_id] = self.__stream_data(data)

    def __stream_data(self, new_data):
        stream = GameDataResult(new_data.game_id)
        stream.add_stream(new_data.user_id, new_data.user_name,new_data.viewer_count)
        return stream


class GameDataResult:
    """ Stores stream stats for single game"""
    def __init__(self, game_id):
        self.game_id = game_id
        self.viewer_count = 0
        self.streamers = []

    def add_viewers(self, viewers):
        pass

    @property
    def stream_count(self):
        return len(self.streamers)

    def distribution(self):
        """ Calculates what percentage of viewers each streamer have
        
        Example:
            50 mike 40 cindy 10 bob => 50|40|10
        """
        dist = ""
        for stream in self.streamers:
            try:
                percent = (stream['viewer_count'] / self.viewer_count) * 100
            except ZeroDivisionError:
                percent = 0
            dist += "{}[{}]|".format(stream['name'],round(percent, 2))
        return dist[:-1]

    def add_stream(self, uid, name, viewer_count):
        self.viewer_count += viewer_count

        self.streamers.append({
            "streamer": uid,
            "name" : name,
            "viewer_count": viewer_count
        })


class TwitchStreamResultAdapter:
    """ Convert data retrivied from twitch to format understood by database"""
    @staticmethod
    def adapt(data):
        return {
            "stream_id": data['id'],
            "game_id": data['game_id'],
            "viewer_count": data['viewer_count'],
            "user_name": data['user_name'],
            "user_id": data['user_id'],
            "language": data['language']
        }


class GameDataResultAdapter:
    """ Convert data produced by DataCompiler to database format"""
    @staticmethod
    def adapt(now, m10, m60):
        return {
            'game_id': now.game_id,
            'viewer_count': now.viewer_count,
            'streams_count': now.stream_count,
            'viewers_10_min_ago': m10.viewer_count,
            'viewers_60_min_ago': m60.viewer_count,
            'streams_10_min_ago': m10.stream_count,
            'streams_60_min_ago': m60.stream_count,
            'distribution': now.distribution()
        }
