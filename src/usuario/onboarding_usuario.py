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
Script refactorizado para usar configuración desde diccionario en lugar de .env
Configuración del usuario de pruebas usando datos del YAML via Hydra.
"""

import sys
from urllib.parse import quote_plus
from datetime import datetime
from argon2 import PasswordHasher
from src.utils.mongodb_manager import MongoDBManager
from bson import ObjectId
from src.usuario.user_manager import UserManager
from pathlib import Path
import asyncio

# =============================
# CONFIGURACIÓN DE CONEXIÓN A MONGODB
# =============================
def obtener_config_ambiente(diccionario_config: dict, ambiente: str):
    """
    Obtiene la configuración de conexión desde el diccionario de configuración.
    
    Args:
        diccionario_config: Diccionario con toda la configuración del YAML
        ambiente: Ambiente objetivo (DEV, STAGING, PROD)
    
    Returns:
        dict: Configuración de conexión a MongoDB
    """
    config_mongodb = diccionario_config['mongodb'][ambiente]
    
    return {
        "aws_access_key_id": config_mongodb['aws_access_key_id'],
        "aws_secret_access_key": config_mongodb['aws_secret_access_key'],
        "cluster_url": config_mongodb['cluster_url'],
        "db_name": config_mongodb['db_name'],
        "app_name": config_mongodb['app_name']
    }

def obtener_config_mongodb(diccionario_config: dict, ambiente: str):
    """
    Crea la configuración de MongoDB usando los datos del diccionario.
    Como MongoDBConfig espera un env_prefix, temporalmente seteamos las variables de entorno.
    
    Args:
        diccionario_config: Diccionario con toda la configuración del YAML
        ambiente: Ambiente objetivo (DEV, STAGING, PROD)
    
    Returns:
        MongoDBConfig: Configuración de MongoDB
    """
    import os
    from src.config.mongodb_config import MongoDBConfig
    
    # Obtener los datos de MongoDB del diccionario
    datos_mongodb = diccionario_config['mongodb'][ambiente]
    
    # Temporarily set environment variables para que MongoDBConfig funcione
    # Guardamos los valores originales para restaurarlos después
    entorno_original = {}
    variables_entorno = {
        f"{ambiente}_AWS_ACCESS_KEY_ID": datos_mongodb['aws_access_key_id'],
        f"{ambiente}_AWS_SECRET_ACCESS_KEY": datos_mongodb['aws_secret_access_key'],
        f"{ambiente}_CLUSTER_URL": datos_mongodb['cluster_url'],
        f"{ambiente}_DB": datos_mongodb['db_name'],
        f"{ambiente}_APP_NAME": datos_mongodb['app_name']
    }
    
    # Setear las variables de entorno temporalmente
    for clave, valor in variables_entorno.items():
        entorno_original[clave] = os.environ.get(clave)  # Guardar valor original
        os.environ[clave] = str(valor)
    
    try:
        # Crear MongoDBConfig con el env_prefix como espera
        config_mongodb = MongoDBConfig(env_prefix=ambiente)
        return config_mongodb
    finally:
        # Restaurar las variables de entorno originales
        for clave, valor_original in entorno_original.items():
            if valor_original is None:
                os.environ.pop(clave, None)
            else:
                os.environ[clave] = valor_original

# =============================
# FUNCIONES DE VALIDACIÓN
# =============================
def validar_config_usuario(config_usuario: dict):
    """
    Valida que todos los parámetros requeridos del usuario estén presentes.
    
    Args:
        config_usuario: Diccionario con la configuración del usuario
    """
    campos_requeridos = [
        'email', 'password', 'name', 'lastname', 
        'phone', 'expense_code', 'cost_code'
    ]
    
    campos_faltantes = []
    for campo in campos_requeridos:
        if not config_usuario.get(campo):
            campos_faltantes.append(campo)
    
    if campos_faltantes:
        print(f"\n❌ Faltan campos requeridos en la configuración del usuario: {', '.join(campos_faltantes)}")
        print("Por favor verifica tu archivo de configuración YAML.")
        sys.exit(1)

def debug_config_usuario(config_usuario: dict):
    """
    Muestra la configuración del usuario para debugging.
    
    Args:
        config_usuario: Diccionario con la configuración del usuario
    """
    print("🔍 Debugging configuración del usuario desde YAML:")
    print(f"  - Email: {config_usuario.get('email', '✗ Faltante')}")
    print(f"  - Password: {'✓ Configurado' if config_usuario.get('password') else '✗ Faltante'}")
    print(f"  - Name: {config_usuario.get('name', '✗ Faltante')}")
    print(f"  - Lastname: {config_usuario.get('lastname', '✗ Faltante')}")
    print(f"  - Phone: {config_usuario.get('phone', '✗ Faltante')}")
    print(f"  - Expense Code: {config_usuario.get('expense_code', '✗ Faltante')}")
    print(f"  - Cost Code: {config_usuario.get('cost_code', '✗ Faltante')}")
    print("🔍 Fin del debugging de configuración del usuario\n")

# =============================
# FUNCIONES PRINCIPALES (Sin cambios en la lógica)
# =============================
def limpiar_asociaciones_usuario(mongo_manager: MongoDBManager, email_usuario: str, id_usuario=None):
    """
    Elimina datos de módulos e integraciones del usuario, pero NO elimina el usuario si ya existe.
    """
    print("Iniciando limpieza de módulos e integraciones...")
    coleccion_usuarios = mongo_manager.db["users"]
    uid = None
    
    if id_usuario is None:
        usuario = coleccion_usuarios.find_one({"email": email_usuario})
        if usuario:
            uid = usuario["_id"]
            coleccion_usuarios.delete_one({"_id": uid})
            print(f"Usuario ya existe con UID: {uid}")
        else:
            print("No se encontró usuario existente para limpiar módulos/integraciones")
            return None
    else:
        try:
            uid = ObjectId(id_usuario)
        except Exception:
            uid = id_usuario
        coleccion_usuarios.delete_one({"_id": uid})
    
    # Limpiar solo módulos e integraciones
    colecciones_a_limpiar = ["modules", "integrations"]
    total_eliminados = 0
    
    for nombre_coleccion in colecciones_a_limpiar:
        try:
            coleccion = mongo_manager.db[nombre_coleccion]
            resultado = coleccion.delete_many({"UID": uid})
            cantidad_eliminada = resultado.deleted_count
            total_eliminados += cantidad_eliminada
            
            if cantidad_eliminada > 0:
                print(f"  {nombre_coleccion}: {cantidad_eliminada} documentos eliminados")
            else:
                print(f"  {nombre_coleccion}: sin datos para eliminar")
        except Exception as e:
            print(f"  Error limpiando {nombre_coleccion}: {str(e)}")
    
    print(f"Total de documentos eliminados en módulos/integraciones: {total_eliminados}")
    return uid

def crear_modules(mongo_manager: MongoDBManager, id_usuario, codigo_modulo_costos: str, codigo_modulo_gastos: str):
    """
    Elimina los módulos existentes del usuario y crea los módulos de costos y gastos.
    """
    print("📦 Creando módulos...")
    
    coleccion_modulos = mongo_manager.db["modules"]
    tiempo_actual = datetime.now()

    # Convertir id_usuario a ObjectId si es necesario
    try:
        uid = ObjectId(id_usuario)
    except Exception:
        uid = id_usuario

    # Eliminar módulos existentes para evitar duplicados
    resultado_eliminacion = coleccion_modulos.delete_many({"UID": uid})
    print(f"Módulos eliminados para UID {uid}: {resultado_eliminacion.deleted_count}")

    # Definir nuevos módulos
    modulo_costos = {
        "UID": uid,
        "name": "costos",
        "code": codigo_modulo_costos,
        "createdAt": tiempo_actual,
        "updatedAt": tiempo_actual,
        "__v": 0
    }

    modulo_gastos = {
        "UID": uid,
        "name": "gastos",
        "code": codigo_modulo_gastos,
        "createdAt": tiempo_actual,
        "updatedAt": tiempo_actual,
        "__v": 0
    }

    # Insertar nuevos módulos
    resultado_costos = coleccion_modulos.insert_one(modulo_costos)
    resultado_gastos = coleccion_modulos.insert_one(modulo_gastos)

    print(f"  ✓ Módulo de costos creado con ID: {resultado_costos.inserted_id}")
    print(f"  ✓ Módulo de gastos creado con ID: {resultado_gastos.inserted_id}")

    return {
        "costos_id": resultado_costos.inserted_id,
        "gastos_id": resultado_gastos.inserted_id
    }

def crear_integration(mongo_manager: MongoDBManager, id_usuario):
    """
    Crea la integración de usuario con el módulo de costos y gastos.
    """
    coleccion_integraciones = mongo_manager.db["integrations"]
    tiempo_actual = datetime.now()

    # Convertir id_usuario a ObjectId si es necesario
    try:
        uid = ObjectId(id_usuario)
    except Exception:
        uid = id_usuario

    resultado_eliminacion = coleccion_integraciones.delete_many({"UID": uid})
    print(f"Integraciones eliminados para UID {uid}: {resultado_eliminacion.deleted_count}")

    # Definir la integración
    integracion = {
        "UID": uid,
        "name": "Siigo OnPremise",
        "onPremise": True,
        "apiKey": None,
        "apiSecret": None,
        "__v": 0,
    }

    # Insertar la integración
    resultado = coleccion_integraciones.insert_one(integracion)
    print(f"  ✓ Integración creada con ID: {resultado.inserted_id}")

    return resultado.inserted_id

# =============================
# FUNCIÓN PRINCIPAL REFACTORIZADA
# =============================
async def setup_usuario(diccionario_config: dict, ambiente: str) -> str:
    """
    Configura el usuario usando la configuración del diccionario en lugar del .env
    
    Args:
        diccionario_config: Diccionario con toda la configuración del YAML
        ambiente: Ambiente objetivo (DEV, STAGING, PROD)
    
    Returns:
        str: UID del usuario configurado
    """
    try:
        # Extraer configuraciones
        config_usuario = diccionario_config['user']
        
        # Validar configuración del usuario
        validar_config_usuario(config_usuario)
        debug_config_usuario(config_usuario)
        
        # Obtener configuración de MongoDB
        config_objetivo = obtener_config_ambiente(diccionario_config, ambiente)
        config_mongodb = obtener_config_mongodb(diccionario_config, ambiente)
        
        print("🔍 Intentando conectar a MongoDB...")
        print(f"🔍 Usando URI: mongodb+srv://***:***@{config_objetivo['cluster_url']}?authSource=%24external&authMechanism=MONGODB-AWS&retryWrites=true&w=majority&appName={config_objetivo['app_name']}")
        
        gestor_mongo = MongoDBManager(config_mongodb)
        bd = gestor_mongo.db
        coleccion_usuarios = bd["users"]
        
        print("🔍 MongoClient creado exitosamente")
        print("🔍 Intentando hacer ping a MongoDB...")
        bd.command('ping')
        print("✓ Conexión a MongoDB exitosa")
        
        print(f"\nConfigurando usuario: {config_usuario['email']}")
        print("=" * 50)
        
        # PASO 1: Buscar usuario existente
        usuario = coleccion_usuarios.find_one({"email": config_usuario['email']})
        
        if usuario:
            uid = str(usuario["_id"])
            print(f"Usuario ya existe con UID: {uid}")
            
            # Verificar si existen módulos y/o integraciones
            coleccion_modulos = bd["modules"]
            coleccion_integraciones = bd["integrations"]
            
            mod_costos = coleccion_modulos.find_one({"UID": usuario["_id"], "name": "costos"})
            mod_gastos = coleccion_modulos.find_one({"UID": usuario["_id"], "name": "gastos"})
            integracion = coleccion_integraciones.find_one({"UID": usuario["_id"]})
            
            if not mod_costos or not mod_gastos:
                print("Creando módulos que faltan...")
                crear_modules(gestor_mongo, uid, config_usuario['cost_code'], config_usuario['expense_code'])
            else:
                print("Módulos ya existen, no se hace nada.")
            
            if not integracion:
                print("Creando integración que falta...")
                crear_integration(gestor_mongo, uid)
            else:
                print("Integración ya existe, no se hace nada.")
        else:
            print("Usuario no existe, creando usuario desde cero...")
            
            servicio_usuario = UserManager(
                name=config_usuario['name'],
                lastname=config_usuario['lastname'],
                email=config_usuario['email'],
                phone=config_usuario['phone'],
                password_plain=config_usuario['password'],
                num_consecutivo=1  # Valor por defecto o podrías agregarlo al YAML
            )
            
            nuevo_usuario = await servicio_usuario.create_user()
            uid = str(nuevo_usuario.id)
            print(f"Usuario creado con ID: {uid}")
            
            crear_modules(gestor_mongo, uid, config_usuario['cost_code'], config_usuario['expense_code'])
            crear_integration(gestor_mongo, uid)
        
        print("\n" + "=" * 50)
        print("Configuración completada exitosamente")
        print("=" * 50)
        print(f"Usuario: {config_usuario['email']}")
        print(f"UID: {uid}")
        print(f"Ambiente: {ambiente}")
        
        gestor_mongo.close()
        return str(uid)
        
    except Exception as e:
        print(f"Error de conexión MongoDB: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        print("\nFull error details:")
        import traceback
        traceback.print_exc()
        print("\nPor favor verifica:")
        print("1. Credenciales AWS en archivo YAML")
        print("2. Conectividad de red")
        print("3. Estado del cluster MongoDB")
        print("4. Permisos IAM")
        sys.exit(1)