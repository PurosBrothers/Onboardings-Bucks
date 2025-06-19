import motor.motor_asyncio
from typing import Sequence, Type
from beanie import init_beanie, Document
from src.config.mongodb_config import MongoDBConfig 

async def init_db(document_models: Sequence[Type[Document]]):
    # Configuración de MongoDB
    config = MongoDBConfig(env_prefix="DEV")

    # Crear cliente de Motor
    client = motor.motor_asyncio.AsyncIOMotorClient(config.target_uri)

    # Inicializar Beanie con la base de datos y los modelos
    await init_beanie(
        database=client[config.db_name],
        document_models=document_models,  
    )
