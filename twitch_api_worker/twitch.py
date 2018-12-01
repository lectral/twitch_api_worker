""" Twitch Api Handler """
import requests
import os
from requests.exceptions import HTTPError
import logging


class TwitchStreamsBrowser:
    """ This class implements API handling for the worker.
    Worker expects this methods and properties:
        __iter__
        __next__
        failed
    """
    URL = "https://api.twitch.tv/helix/streams?first=100"
    HEADERS = {
        'Client-ID': os.environ.get('TWITCH_API_CLIENT_ID', None)
    }

    def __init__(self, language):
        self.requests_made = 0
        self.iterator = None
        self.language = language
        self.failed = False
        if not self.HEADERS['Client-ID']:
            raise RuntimeError(
                "Client-ID enviromental variable needs to be defined")

    def __iter__(self):
        return self

    def __next__(self):
        results = self.__next_results()
        if results:
            self.requests_made += 1
            return results
        raise StopIteration

    """ Returns number of requests made """
    def requests_made():
        return self.requests_made

    def __next_results(self):
        resonse_body = {}
        try:
            response_body = self.__make_request()
        except HTTPError as exception:
            logging.error("  -> Twitch api failed: {}".format(exception))
            self.failed = True
            return None
        logging.info(
            "  -> Payload contains {} data entries".format(
                len(response_body['data'])
            )
        )
        if self.__payload_has_no_data(response_body):
            return None
        self.iterator = response_body['pagination']['cursor']
        return response_body['data']

    def __payload_has_no_data(self, body):
        if not body['data']:
            logging.info(
                "  -> This payload contained no data. Crawling complated.")
            return True
        return False

    def __make_request(self):
        logging.info("GET {}".format(self.__url()))
        request = requests.get(self.__url(), headers=self.HEADERS)
        if request.status_code == requests.codes.ok:
            return request.json()
        request.raise_for_status()

    def __url(self):
        if self.iterator:
            return "{}&language={}&after={}".format(
                self.URL, self.language, self.iterator)
        return "{}&language={}".format(self.URL, self.language)
