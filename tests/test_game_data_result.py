import unittest
from twitch_api_worker.data_compilers  import GameDataResult


class GameDataResultTestCase(unittest.TestCase):
    def test_init(self):
        gresult = GameDataResult("123456")
        self.assertEqual(gresult.game_id,"123456")
    
    def test_distribution(self):
        gresult = GameDataResult("123456")
        gresult.add_stream("345", 60)
        gresult.add_stream("123", 40)
        self.assertEqual(gresult.distribution(),"60.0|40.0")

    def test_stream_count(self):
        gresult = GameDataResult("123456")
        gresult.add_stream("345", 60)
        gresult.add_stream("123", 40)
        gresult.add_stream("123", 40)
        self.assertEqual(gresult.stream_count,3)

    def test_viewer_count(self):
        gresult = GameDataResult("123456")
        gresult.add_stream("345", 40)
        gresult.add_stream("345", 60)
        self.assertEqual(gresult.viewer_count,100)


class StreamMock():
    def __init__(self):
       pass 
