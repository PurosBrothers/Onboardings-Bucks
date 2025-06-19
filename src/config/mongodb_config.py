import os
from bson import ObjectId

class MongoDBConfig:
    def __init__(self, env_prefix: str = "DEV"):
        self.aws_access_key_id = os.getenv(f"{env_prefix}_AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv(f"{env_prefix}_AWS_SECRET_ACCESS_KEY")
        self.cluster_url = os.getenv(f"{env_prefix}_CLUSTER_URL")
        self.db_name = os.getenv(f"{env_prefix}_DB")
        self.app_name = os.getenv(f"{env_prefix}_APP_NAME")
        self.collection_name = "providers"
        self.uid_user = os.getenv("UID_USER")  

    @property
    def target_uri(self) -> str:
        return (
            f"mongodb+srv://{self.aws_access_key_id}:{self.aws_secret_access_key}"
            f"@{self.cluster_url}?authSource=%24external&authMechanism=MONGODB-AWS"
            f"&retryWrites=true&w=majority&appName={self.app_name}"
        )

    @property
    def uid_filter(self):
        return ObjectId(self.uid_user)

    def get_collection_name(self) -> str:
        return self.collection_name

    def set_collection_name(self, collection_name: str):
        if not collection_name:
            raise ValueError("Collection name cannot be empty.")
        self.collection_name = collection_name
        return self.collection_name
