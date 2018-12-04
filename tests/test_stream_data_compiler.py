import unittest
from twitch_api_worker.data_compilers  import StreamDataCompiler 


class StreamDataCompilerTestCase(unittest.TestCase):
    def test_init(self):
        compiler = StreamDataCompiler()
        self.assertEqual(compiler.game_ids,[])
    
    def test_parse_data_unit_1(self):
        compiler = StreamDataCompiler()
        mock1 = DataUnitMock("1234",10,"123")
        compiler.parse_data_unit(mock1) 
        self.assertEqual(compiler.game_ids,["1234"])
        
    def test_parse_data_unit_2(self):
        compiler = StreamDataCompiler()
        mock1 = DataUnitMock("1234",10,"123")
        mock2 = DataUnitMock("1234",20,"331")
        compiler.parse_data_unit(mock1) 
        compiler.parse_data_unit(mock2) 
        self.assertEqual(compiler.get("1234").viewer_count,30)

class GameDataResultMock():
    def __init__(self, game_id,viewer_count, user_id):
        self.game_id = game_id
        self.viewer_count = viewer_count
        self.user_id = user_id

class DataUnitMock:
    def __init__(self, game_id, viewer_count, user_id):
        self.game_id = game_id
        self.viewer_count = viewer_count
        self.user_id = user_id 

        

