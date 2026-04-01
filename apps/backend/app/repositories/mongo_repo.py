# MongoDB repository
from pymongo import MongoClient
from app.core.config import settings

class MongoRepo:
    def __init__(self):
        self.client = MongoClient(settings.MONGO_URI)
        self.db = self.client['tourism']

    def get_connections(self):
        return list(self.db.connections.find())
