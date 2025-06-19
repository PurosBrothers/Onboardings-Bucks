from pymongo import MongoClient
from src.config.mongodb_config import MongoDBConfig

class MongoDBManager:
    def __init__(self, config: MongoDBConfig):
        self.config = config
        self.client = MongoClient(config.target_uri)
        self.db = self.client[config.db_name]
        self.collection = self.db[config.get_collection_name()]
        

    def product_exists(self, code: str, uid) -> bool:
        return self.collection.find_one({"UID": uid, "code": code}) is not None

    def save_product(self, doc: dict):
        self.collection.insert_one(doc)

    def delete_all_products(self, uid) -> int:
        result = self.collection.delete_many({"UID": uid})
        return result.deleted_count
    
    def delete_all_providers(self, uid) -> int:
        result = self.collection.delete_many({"UID": uid})
        return result.deleted_count

    def close(self):
        self.client.close()

