import motor.motor_asyncio
from typing import Sequence, Type
from beanie import init_beanie, Document
from src.config.mongodb_config import MongoDBConfig 
from omegaconf import OmegaConf
import os

# Leer el ambiente desde src/conf/conf.yaml
root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
config_path = os.path.join(root, "conf", "conf.yaml")
with open(config_path, "r", encoding="utf-8") as f:
    config = OmegaConf.create(f.read())
AMBIENTE = config.ambiente

async def init_db(document_models: Sequence[Type[Document]]):
    # Configuración de MongoDB
    config_mongo = MongoDBConfig(env_prefix=AMBIENTE)

    # Crear cliente de Motor
    client = motor.motor_asyncio.AsyncIOMotorClient(config_mongo.target_uri)

    # Inicializar Beanie con la base de datos y los modelos
    await init_beanie(
        database=client[config_mongo.db_name],
        document_models=document_models,  
    )
