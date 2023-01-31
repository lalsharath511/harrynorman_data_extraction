from settings import *
from pymongo import MongoClient

class HarryPipeline:
   
    def process_item(self,features):
        conn = MongoClient("localhost",27017)
        db =conn[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        collection.insert_one(features)
        return 