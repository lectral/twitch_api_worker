import datetime
import sqlite3
import json
from sqlobject import StringCol, TimestampCol, IntCol,SQLObject, ForeignKey, sqlhub, connectionForURI
 
class Stream(SQLObject):
    stream_id = StringCol()
    game_id = StringCol()
    viewer_count = IntCol()
    user_name = StringCol()
    user_id = StringCol()
    created_on = TimestampCol()

class WorkerDb:
    def __init__(self):
        sqlhub.processConnection = connectionForURI(
                'sqlite:/home/lectral/db.sqlite'
                ) 
   
    def store(self, data):
        data_to_store = data
        data_to_store['created_on'] = datetime.datetime.now()
        Stream(**data_to_store)
    
    def create_tables(self):
        Stream.createTable(ifNotExists=True)

    def show(self):
        pass

