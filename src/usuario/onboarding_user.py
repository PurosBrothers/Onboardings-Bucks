#!/usr/bin/env python3
"""
Script para limpiar completamente y recrear usuario y módulos relacionados en la base de datos.
"""
import os
import json
import sys
from urllib.parse import quote_plus
from datetime import datetime
from argon2 import PasswordHasher
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv, set_key
from src.user_manager import UserManager
from pathlib import Path
import asyncio

# Cargar variables de entorno
load_dotenv()

# Debug: Check if .env is loaded in surtifloraUser.py
print("🔍 Debugging .env loading in surtifloraUser.py:")
print(f"  - Current working directory: {os.getcwd()}")
print(f"  - .env file in current directory: {Path('.env').exists()}")
print(f"  - .env file in parent directory: {Path('../.env').exists()}")

# Try to load .env from multiple possible locations
env_locations = ['.env', '../.env', '../../.env']
for env_path in env_locations:
    if Path(env_path).exists():
        print(f"  - Loading .env from: {env_path}")
        load_dotenv(env_path)
        break
else:
    print("  - No .env file found in any expected location")

print("🔍 End of .env loading debug in surtifloraUser.py\n")


# Configuración de conexión a MongoDB dev
target_config = {
    "aws_access_key_id": os.getenv("DEV_AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("DEV_AWS_SECRET_ACCESS_KEY"),
    "cluster_url": os.getenv("DEV_CLUSTER_URL"),
    "db_name": os.getenv("DEV_DB"),
    "app_name": os.getenv("DEV_APP_NAME")
}

# Debug: Print connection parameters (without sensitive data)
print("🔍 Debugging MongoDB connection parameters:")
print(f"  - AWS Access Key ID: {'✓ Set' if target_config['aws_access_key_id'] else '✗ Missing'}")
print(f"  - AWS Secret Access Key: {'✓ Set' if target_config['aws_secret_access_key'] else '✗ Missing'}")
print(f"  - Cluster URL: {target_config['cluster_url'] or '✗ Missing'}")
print(f"  - Database Name: {target_config['db_name'] or '✗ Missing'}")
print(f"  - App Name: {target_config['app_name'] or '✗ Missing'}")

# Check for missing required parameters
missing_params = [key for key, value in target_config.items() if not value]
if missing_params:
    print(f"\n❌ Missing required environment variables: {', '.join(missing_params)}")
    print("Please check your .env file and ensure all DEV_* variables are set.")
    sys.exit(1)

# Get NUM_CONSECUTIVO with a fallback value of 1
NUM_CONSECUTIVO = int(os.getenv("NUM_CONSECUTIVO", "1"))  # Número consecutivo proporcionado en el .env, convertido a int

# URI de conexión
TARGET_URI = f"mongodb+srv://{target_config['aws_access_key_id']}:{target_config['aws_secret_access_key']}@{target_config['cluster_url']}?authSource=%24external&authMechanism=MONGODB-AWS&retryWrites=true&w=majority&appName={target_config['app_name']}"

print(f"  - Connection URI: mongodb+srv://***:***@{target_config['cluster_url']}?authSource=%24external&authMechanism=MONGODB-AWS&retryWrites=true&w=majority&appName={target_config['app_name']}")
print("🔍 End of MongoDB connection debugging\n")

# Email a buscar
target_email = os.getenv("TEST_USER_EMAIL")
password = os.getenv("TEST_PASSWORD_PLAIN")
name = os.getenv("TEST_USER_NAME")
lastname = os.getenv("TEST_USER_LASTNAME")
phone = os.getenv("TEST_USER_PHONE")

expense_code = os.getenv("TEST_EXPENSE_CODE")
cost_code = os.getenv("TEST_COST_CODE")

# Debug: Print user configuration parameters
print("🔍 Debugging User Configuration:")
print(f"  - Email: {target_email or '✗ Missing'}")
print(f"  - Password: {'✓ Set' if password else '✗ Missing'}")
print(f"  - Name: {name or '✗ Missing'}")
print(f"  - Lastname: {lastname or '✗ Missing'}")
print(f"  - Phone: {phone or '✗ Missing'}")
print(f"  - Expense Code: {expense_code or '✗ Missing'}")
print(f"  - Cost Code: {cost_code or '✗ Missing'}")
print(f"  - Num Consecutivo: {NUM_CONSECUTIVO}")

# Check for missing required user parameters
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

print("🔍 End of User Configuration debugging\n")

def clean_user_data(db, user_id=None):
    """Eliminar completamente todos los datos del usuario de todas las colecciones"""
    print("🧹 Iniciando limpieza completa de datos...")
    
    # Si no se proporciona user_id, buscar por email
    if user_id is None:
        users_collection = db["users"]
        user = users_collection.find_one({"email": target_email})
        if user:
            user_id = user["_id"]
        else:
            print("No se encontró usuario existente para limpiar")
            return
    
    # Convertir a ObjectId si es necesario
    try:
        uid = ObjectId(user_id)
    except Exception:
        uid = user_id
    
    print(f"Eliminando todos los datos para UID: {uid}")
    
    # Lista de colecciones que pueden contener datos del usuario
    collections_to_clean = [
        "users",
        "modules", 
        "integrations",
    ]
    
    total_deleted = 0
    
    for collection_name in collections_to_clean:
        try:
            collection = db[collection_name]
            
            if collection_name == "users":
                # Para usuarios, eliminar por _id
                result = collection.delete_many({"_id": uid})
            else:
                # Para otras colecciones, eliminar por UID
                result = collection.delete_many({"UID": uid})
            
            deleted_count = result.deleted_count
            total_deleted += deleted_count
            
            if deleted_count > 0:
                print(f"  ✓ {collection_name}: {deleted_count} documentos eliminados")
            else:
                print(f"  - {collection_name}: sin datos para eliminar")
                
        except Exception as e:
            print(f"  ⚠️ Error limpiando {collection_name}: {str(e)}")
    
    print(f"🗑️ Total de documentos eliminados: {total_deleted}")
    return total_deleted

def create_modules(db, user_id, cost_module_code: str, expense_module_code: str):
    """Eliminar los módulos existentes del usuario y luego crear los módulos de costos y gastos."""
    print("📦 Creando módulos...")
    
    modules_collection = db["modules"]
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

def create_integration(db, user_id):
    """Crear la integración de usuario con el módulo de costos y gastos."""
    integration_collection = db["integrations"]
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
    """Set up user from scratch after cleaning all existing data."""
    try:
        print("🔍 Attempting to connect to MongoDB...")
        print(f"🔍 Using URI: mongodb+srv://***:***@{target_config['cluster_url']}?authSource=%24external&authMechanism=MONGODB-AWS&retryWrites=true&w=majority&appName={target_config['app_name']}")
        
        # Test connection to MongoDB first
        client = MongoClient(TARGET_URI)
        print("🔍 MongoClient created successfully")
        
        db = client.admin
        print("🔍 Attempting to ping MongoDB...")
        db.command('ping')
        print("✓ MongoDB connection successful")

        db = client[target_config["db_name"]]
        users_collection = db["users"]
        modules_collection = db["modules"]

        print(f"\n🎯 Configurando usuario: {target_email}")
        print("=" * 50)

        # PASO 1: Limpiar datos existentes
        clean_user_data(db)

        # PASO 2: Crear nuevo usuario
        print("\n🆕 Creando usuario desde cero...")
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
        print(f"  ✓ Usuario creado con ID: {uid}")

        # PASO 3: Crear módulos
        # Check if modules exist
        module_count = modules_collection.count_documents({"UID": ObjectId(uid)})
        if module_count > 0:
            print("Módulos ya existen para este usuario")
        else:
            print("Creando módulos para usuario existente...")
            create_modules(db, uid, cost_code, expense_code)
        
        # PASO 4: Crear integración
        print("\n🔗 Configurando integración...")
        integration_result = create_integration(db, uid)

        # PASO 5: Actualizar archivo .env
        update_uid_in_env(uid)
        #VERIFICAR SI YA EXISTE EN EL .ENV, SI SI ENTONCES BORRA ESA LINEA EN EL ARCHIVO LA REEMPLAZA

        print(f"  ✓ Variable UID_USER={uid} añadida al archivo .env")

        print("\n" + "=" * 50)
        print("✅ CONFIGURACIÓN COMPLETADA EXITOSAMENTE")
        print("=" * 50)
        print(f"👤 Usuario: {target_email}")
        print(f"🆔 UID: {uid}")
        print(f"📝 Numero consecutivo: {NUM_CONSECUTIVO}")
        
        client.close()
        return uid

    except Exception as e:
        print(f"❌ Error de conexión MongoDB: {str(e)}")
        print(f"❌ Error type: {type(e).__name__}")
        print("\n🔍 Full error details:")
        import traceback
        traceback.print_exc()
        print("\nPor favor verifica:")
        print("1. Credenciales AWS en archivo .env")
        print("2. Conectividad de red")
        print("3. Estado del cluster MongoDB")
        print("4. Permisos IAM")
        sys.exit(1)


def update_uid_in_env(uid: str, env_file_path: str = ".env"):
    """
    Actualiza o añade la variable UID_USER en el archivo .env.
    Si ya existe, la elimina y la reemplaza con el nuevo valor.
    """
    env_path = Path(env_file_path)
    
    if not env_path.exists():
        print(f"  ⚠️ Archivo {env_file_path} no existe, se creará")
        # Si no existe el archivo, simplemente crear con la nueva variable
        with open(env_path, 'w') as f:
            f.write(f"UID_USER={uid}\n")
        print(f"  ✓ Variable UID_USER={uid} añadida al nuevo archivo .env")
        return
    
    # Leer todas las líneas del archivo
    with open(env_path, 'r') as f:
        lines = f.readlines()
    
    # Filtrar las líneas para eliminar cualquier línea que contenga UID_USER
    filtered_lines = []
    uid_user_found = False
    
    for line in lines:
        # Verificar si la línea contiene UID_USER (ignorando espacios y comentarios)
        stripped_line = line.strip()
        if stripped_line.startswith('UID_USER=') or stripped_line.startswith('#UID_USER='):
            uid_user_found = True
            print(f"  🗑️ Eliminando línea existente: {stripped_line}")
            continue  # Saltar esta línea (eliminarla)
        filtered_lines.append(line)
    
    # Añadir la nueva variable UID_USER
    filtered_lines.append(f"UID_USER={uid}\n")
    
    # Escribir el archivo actualizado
    with open(env_path, 'w') as f:
        f.writelines(filtered_lines)
    
    if uid_user_found:
        print(f"  ✓ Variable UID_USER reemplazada con nuevo valor: {uid}")
    else:
        print(f"  ✓ Variable UID_USER={uid} añadida al archivo .env")

async def main():
    """Main function to run the setup"""
    try:
        uid = await setup_user()
        print(f"✅ Usuario configurado correctamente con UID: {uid}")
    except Exception as e:
        print(f"❌ Error en la configuración del usuario: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Ejecutar la función async correctamente
    asyncio.run(main())