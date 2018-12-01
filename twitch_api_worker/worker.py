import json
import time


class Worker:
    def __init__(self, api, db):
        self.api = api
        self.db = db
        self.failed = False

    def work(self):
        self.db.create_tables()
        for result in self.api:
            time.sleep(1)
            for stream in result:
                to_store = self.data_to_dict(stream)
                self.db.store(to_store)
        if(self.api.failed):
            logging.error("Twitch api failed")
            self.failed = True

    def data_to_dict(self, data):
        return {
            "stream_id": data['id'],
            "game_id": data['game_id'],
            "viewer_count": data['viewer_count'],
            "user_name": data['user_name'],
            "user_id": data['user_id']
        }
