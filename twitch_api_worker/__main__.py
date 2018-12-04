import requests
import logging
from twitch_api_worker.worker import CrawlerWorker
from twitch_api_worker.worker import GamesCacheWorker
from twitch_api_worker.worker import AggregateDataWorker

from twitch_api_worker.db import WorkerDb

from twitch_api_worker.twitch import TwitchStreamsBrowser

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

if __name__ == "__main__":
    twitch = TwitchStreamsBrowser("pl")
    worker_db = WorkerDb()
    workers = [
        CrawlerWorker(twitch, worker_db),
        AggregateDataWorker(worker_db),
        GamesCacheWorker(worker_db)
    ]
    worker_db.create_tables()
    for worker in workers:
        logging.info("{} : started".format(worker.name))
        worker.work()
        if worker.failed:
            logging.error("{} : failed".format(worker.name))
            logging.error("Run [FAILED]")
            exit(1)
        logging.info("{} : completed".format(worker.name))

    logging.info("Run [OK]")
