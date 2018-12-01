import requests
import logging
from twitch_api_worker.worker import Worker
from twitch_api_worker.db import WorkerDb
from twitch_api_worker.twitch import TwitchStreamsBrowser
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

if __name__ == "__main__":
    twitch = TwitchStreamsBrowser("pl")
    db = WorkerDb()
    worker = Worker(twitch, db)
    worker.work()
    if worker.failed:
        logging.error("Run [FAILED]")
        exit(1)
    logging.info("Run [OK]")
