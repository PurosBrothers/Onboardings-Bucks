#!/usr/bin/env python3
"""
Script para limpiar y recrear completamente un usuario de pruebas y sus módulos relacionados en una base de datos MongoDB.

Este script realiza las siguientes acciones:
1. Carga las variables de entorno necesarias desde el archivo .env, buscando en varias ubicaciones posibles.
2. Verifica que todas las variables de entorno requeridas para la conexión y el usuario estén presentes.
3. Se conecta a la base de datos MongoDB utilizando credenciales y parámetros de entorno.
4. Elimina todos los datos existentes del usuario objetivo (identificado por email) en las colecciones relevantes (users, modules, integrations).
5. Crea un nuevo usuario de pruebas con los datos especificados en el .env.
6. Crea los módulos de costos y gastos asociados a ese usuario.
7. Crea la integración correspondiente para el usuario.
8. Actualiza el archivo .env para reflejar el nuevo UID del usuario creado.
9. Incluye mensajes de depuración detallados para facilitar el diagnóstico de problemas de conexión o configuración.

El script está diseñado para entornos de desarrollo y pruebas, permitiendo reiniciar el estado de un usuario y sus módulos de manera segura y automatizada.
"""

import os
import json
import sys
from urllib.parse import quote_plus
from datetime import datetime
from argon2 import PasswordHasher
from src.utils.mongodb_manager import MongoDBManager
from src.config.mongodb_config import MongoDBConfig
from bson import ObjectId
from dotenv import load_dotenv, set_key
from src.usuario.user_manager import UserManager
from pathlib import Path
import asyncio

# =============================
# CARGA DE VARIABLES DE ENTORNO
# =============================
load_dotenv()

# Debug: Verificar carga de .env
print("🔍 Debugging .env loading in surtifloraUser.py:")
print(f"  - Current working directory: {os.getcwd()}")
print(f"  - .env file in current directory: {Path('.env').exists()}")
print(f"  - .env file in parent directory: {Path('../.env').exists()}")

# Intentar cargar .env desde varias ubicaciones posibles
env_locations = ['.env', '../.env', '../../.env']
for env_path in env_locations:
    if Path(env_path).exists():
        print(f"  - Loading .env from: {env_path}")
        load_dotenv(env_path)
        break
else:
    print("  - No .env file found in any expected location")

print("🔍 End of .env loading debug in surtifloraUser.py\n")

# =============================
# CONFIGURACIÓN DE CONEXIÓN A MONGODB
# =============================
target_config = {
    "aws_access_key_id": os.getenv("DEV_AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("DEV_AWS_SECRET_ACCESS_KEY"),
    "cluster_url": os.getenv("DEV_CLUSTER_URL"),
    "db_name": os.getenv("DEV_DB"),
    "app_name": os.getenv("DEV_APP_NAME")
}

mongodb_config = MongoDBConfig(env_prefix="DEV")

# Debug: Mostrar parámetros de conexión (sin datos sensibles)
print("🔍 Debugging MongoDB connection parameters:")
print(f"  - AWS Access Key ID: {'✓ Set' if target_config['aws_access_key_id'] else '✗ Missing'}")
print(f"  - AWS Secret Access Key: {'✓ Set' if target_config['aws_secret_access_key'] else '✗ Missing'}")
print(f"  - Cluster URL: {target_config['cluster_url'] or '✗ Missing'}")
print(f"  - Database Name: {target_config['db_name'] or '✗ Missing'}")
print(f"  - App Name: {target_config['app_name'] or '✗ Missing'}")

# Validar parámetros requeridos
missing_params = [key for key, value in target_config.items() if not value]
if missing_params:
    print(f"\n❌ Missing required environment variables: {', '.join(missing_params)}")
    print("Please check your .env file and ensure all DEV_* variables are set.")
    sys.exit(1)

# Consecutivo para el usuario
NUM_CONSECUTIVO = int(os.getenv("NUM_CONSECUTIVO", "1"))

# Construcción de URI de conexión
TARGET_URI = f"mongodb+srv://{target_config['aws_access_key_id']}:{target_config['aws_secret_access_key']}@{target_config['cluster_url']}?authSource=%24external&authMechanism=MONGODB-AWS&retryWrites=true&w=majority&appName={target_config['app_name']}"

print(f"  - Connection URI: mongodb+srv://***:***@{target_config['cluster_url']}?authSource=%24external&authMechanism=MONGODB-AWS&retryWrites=true&w=majority&appName={target_config['app_name']}")
print("🔍 End of MongoDB connection debugging\n")

# =============================
# PARÁMETROS DEL USUARIO DE PRUEBA
# =============================
target_email = os.getenv("TEST_USER_EMAIL")
password = os.getenv("TEST_PASSWORD_PLAIN")
name = os.getenv("TEST_USER_NAME")
lastname = os.getenv("TEST_USER_LASTNAME")
phone = os.getenv("TEST_USER_PHONE")

expense_code = os.getenv("TEST_EXPENSE_CODE")
cost_code = os.getenv("TEST_COST_CODE")

# Debug: Mostrar configuración del usuario
print("🔍 Debugging User Configuration:")
print(f"  - Email: {target_email or '✗ Missing'}")
print(f"  - Password: {'✓ Set' if password else '✗ Missing'}")
print(f"  - Name: {name or '✗ Missing'}")
print(f"  - Lastname: {lastname or '✗ Missing'}")
print(f"  - Phone: {phone or '✗ Missing'}")
print(f"  - Expense Code: {expense_code or '✗ Missing'}")
print(f"  - Cost Code: {cost_code or '✗ Missing'}")
print(f"  - Num Consecutivo: {NUM_CONSECUTIVO}")

# Validar parámetros requeridos del usuario
def check_user_env():
    missing_user_params = []
    if not target_email:
        missing_user_params.append("TEST_USER_EMAIL")
    if not password:
        missing_user_params.append("TEST_PASSWORD_PLAIN")
    if not name:
        missing_user_params.append("TEST_USER_NAME")
    if not lastname:
        missing_user_params.append("TEST_USER_LASTNAME")
    if not phone:
        missing_user_params.append("TEST_USER_PHONE")
    if not expense_code:
        missing_user_params.append("TEST_EXPENSE_CODE")
    if not cost_code:
        missing_user_params.append("TEST_COST_CODE")
    if missing_user_params:
        print(f"\n❌ Missing required user environment variables: {', '.join(missing_user_params)}")
        print("Please check your .env file and ensure all TEST_* variables are set.")
        sys.exit(1)

check_user_env()
print("🔍 End of User Configuration debugging\n")

# =============================
# FUNCIONES PRINCIPALES
# =============================
def clean_user_data(mongo_manager: MongoDBManager, user_id=None):
    """
    Elimina datos de módulos e integraciones del usuario, pero NO elimina el usuario si ya existe.
    Si no se proporciona user_id, busca por email.
    Retorna el UID del usuario si existe, o None si no existe.
    """
    print("Iniciando limpieza de módulos e integraciones...")
    users_collection = mongo_manager.db["users"]
    uid = None
    if user_id is None:
        user = users_collection.find_one({"email": target_email})
        if user:
            uid = user["_id"]
            users_collection.delete_one({"_id": uid})
            print(f"Usuario ya existe con UID: {uid}")
        else:
            print("No se encontró usuario existente para limpiar módulos/integraciones")
            return None
    else:
        try:
            uid = ObjectId(user_id)
        except Exception:
            uid = user_id
        users_collection.delete_one({"_id": uid})
    # Limpiar solo módulos e integraciones
    collections_to_clean = ["modules", "integrations"]
    total_deleted = 0
    for collection_name in collections_to_clean:
        try:
            collection = mongo_manager.db[collection_name]
            result = collection.delete_many({"UID": uid})
            deleted_count = result.deleted_count
            total_deleted += deleted_count
            if deleted_count > 0:
                print(f"  {collection_name}: {deleted_count} documentos eliminados")
            else:
                print(f"  {collection_name}: sin datos para eliminar")
        except Exception as e:
            print(f"  Error limpiando {collection_name}: {str(e)}")
    print(f"Total de documentos eliminados en módulos/integraciones: {total_deleted}")
    return uid


def create_modules(mongo_manager: MongoDBManager, user_id, cost_module_code: str, expense_module_code: str):
    """
    Elimina los módulos existentes del usuario y crea los módulos de costos y gastos.
    """
    print("📦 Creando módulos...")
    
    modules_collection = mongo_manager.db["modules"]
    current_time = datetime.now()

    # Convertir user_id a ObjectId si es necesario
    try:
        uid = ObjectId(user_id)
    except Exception:
        uid = user_id

    # Eliminar módulos existentes para evitar duplicados
    delete_result = modules_collection.delete_many({"UID": uid})
    print(f"Módulos eliminados para UID {uid}: {delete_result.deleted_count}")

    # Definir nuevos módulos
    costos_module = {
        "UID": uid,
        "name": "costos",
        "code": cost_module_code,
        "createdAt": current_time,
        "updatedAt": current_time,
        "__v": 0
    }

    gastos_module = {
        "UID": uid,
        "name": "gastos",
        "code": expense_module_code,
        "createdAt": current_time,
        "updatedAt": current_time,
        "__v": 0
    }

    # Insertar nuevos módulos
    result_costos = modules_collection.insert_one(costos_module)
    result_gastos = modules_collection.insert_one(gastos_module)

    print(f"  ✓ Módulo de costos creado con ID: {result_costos.inserted_id}")
    print(f"  ✓ Módulo de gastos creado con ID: {result_gastos.inserted_id}")

    return {
        "costos_id": result_costos.inserted_id,
        "gastos_id": result_gastos.inserted_id
    }


def create_integration(mongo_manager: MongoDBManager, user_id):
    """
    Crea la integración de usuario con el módulo de costos y gastos.
    """
    integration_collection = mongo_manager.db["integrations"]
    current_time = datetime.now()

    # Convertir user_id a ObjectId si es necesario
    try:
        uid = ObjectId(user_id)
    except Exception:
        uid = user_id

    delete_result = integration_collection.delete_many({"UID": uid})
    print(f"Integraciones eliminados para UID {uid}: {delete_result.deleted_count}")

    # Definir la integración
    integration = {
        "UID": uid,
        "name":"Siigo OnPremise",
        "onPremise": True,
        "apiKey": None,
        "apiSecret": None,
    }

    # Insertar la integración
    result = integration_collection.insert_one(integration)
    print(f"  ✓ Integración creada con ID: {result.inserted_id}")

    return result.inserted_id


async def setup_user() -> str:
    """
    Configura el usuario: si ya existe, lo elimina y lo crea de nuevo.
    Siempre borra y recrea módulos e integraciones.
    """
    try:
        print("🔍 Attempting to connect to MongoDB...")
        print(f"🔍 Using URI: mongodb+srv://***:***@{target_config['cluster_url']}?authSource=%24external&authMechanism=MONGODB-AWS&retryWrites=true&w=majority&appName={target_config['app_name']}")
        mongo_manager = MongoDBManager(mongodb_config)
        db = mongo_manager.db
        users_collection = db["users"]
        print("🔍 MongoClient created successfully")
        print("🔍 Attempting to ping MongoDB...")
        db.command('ping')
        print("✓ MongoDB connection successful")
        print(f"\nConfigurando usuario: {target_email}")
        print("=" * 50)
        # PASO 1: Limpiar usuario, módulos e integraciones, obtener UID si existe
        uid = clean_user_data(mongo_manager)
        # Ahora, siempre crea el usuario desde cero
        print("Creando usuario desde cero...")
        user_service = UserManager(
            name=name,
            lastname=lastname,
            email=target_email,
            phone=phone,
            password_plain=password,
            num_consecutivo=NUM_CONSECUTIVO
        )
        new_user = await user_service.create_user()
        uid = str(new_user.id)
        print(f"Usuario creado con ID: {uid}")
        # PASO 2: Crear módulos
        print("Creando módulos para usuario...")
        create_modules(mongo_manager, uid, cost_code, expense_code)
        # PASO 3: Crear integración
        print("Configurando integración...")
        create_integration(mongo_manager, uid)
        print("\n" + "=" * 50)
        print("Configuración completada exitosamente")
        print("=" * 50)
        print(f"Usuario: {target_email}")
        print(f"UID: {uid}")
        print(f"Numero consecutivo: {NUM_CONSECUTIVO}")
        mongo_manager.close()
        return str(uid)
    except Exception as e:
        print(f"Error de conexión MongoDB: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        print("\nFull error details:")
        import traceback
        traceback.print_exc()
        print("\nPor favor verifica:")
        print("1. Credenciales AWS en archivo .env")
        print("2. Conectividad de red")
        print("3. Estado del cluster MongoDB")
        print("4. Permisos IAM")
        sys.exit(1)


async def main():
    """
    Función principal para ejecutar la configuración del usuario.
    """
    try:
        uid = await setup_user()
        print(f"✅ Usuario configurado correctamente con UID: {uid}")
    except Exception as e:
        print(f"❌ Error en la configuración del usuario: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    # Ejecutar la función async correctamente
    asyncio.run(main())