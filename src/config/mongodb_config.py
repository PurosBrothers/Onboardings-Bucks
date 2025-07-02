from bson import ObjectId
import os
from omegaconf import OmegaConf

class MongoDBConfig:
    def __init__(self, env_prefix: str = "DEV"):
        """
        Recibe el string de ambiente (env_prefix: DEV, STAGING, PROD) y busca en src/conf/conf.yaml.
        Los nombres de los atributos y métodos se mantienen igual.
        """
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        config_path = os.path.join(root, "conf", "conf.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            config = OmegaConf.create(f.read())
        mongo_cfg = dict(config.mongodb[env_prefix])
        self.aws_access_key_id = mongo_cfg.get("aws_access_key_id")
        self.aws_secret_access_key = mongo_cfg.get("aws_secret_access_key")
        self.cluster_url = mongo_cfg.get("cluster_url")
        self.db_name = mongo_cfg.get("db_name")
        self.app_name = mongo_cfg.get("app_name")
        self.collection_name = mongo_cfg.get("collection_name", "providers")
        self.uid_user = mongo_cfg.get("uid_user")

    @property
    def target_uri(self) -> str:
        return (
            f"mongodb+srv://{self.aws_access_key_id}:{self.aws_secret_access_key}"
            f"@{self.cluster_url}?authSource=%24external&authMechanism=MONGODB-AWS"
            f"&retryWrites=true&w=majority&appName={self.app_name}"
        )

    @property
    def uid_filter(self):
        return ObjectId(self.uid_user) if self.uid_user else None

    def get_collection_name(self) -> str:
        return self.collection_name

    def set_collection_name(self, collection_name: str):
        if not collection_name:
            raise ValueError("Collection name cannot be empty.")
        self.collection_name = collection_name
        return self.collection_name
