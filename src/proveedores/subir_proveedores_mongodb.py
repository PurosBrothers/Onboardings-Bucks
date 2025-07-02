"""
Script para subir proveedores y transacciones a MongoDB..
"""

import os
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
from config.mongodb_config import MongoDBConfig
from utils.mongodb_manager import MongoDBManager
from dotenv import load_dotenv
import logging
from urllib.parse import quote_plus
import datetime
import json
import random
import string

# =============================
# CONFIGURACIÓN Y CONSTANTES
# =============================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("onboarding_surtiflora.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
# Configuración de conexión a MongoDB Staging
# target_config = {
#     "aws_access_key_id": os.getenv("STAGING_AWS_ACCESS_KEY_ID"),
#     "aws_secret_access_key": os.getenv("STAGING_AWS_SECRET_ACCESS_KEY"),
#     "cluster_url": os.getenv("DEV_CLUSTER_URL"),
#     "db_name": os.getenv("DEV_DB"),
#     "app_name": os.getenv("DEV_APP_NAME")
# }

# Configuración de conexión a MongoDB dev
# config = MongoDBConfig(env_prefix=AMBIENTE)
# TARGET_URI = config.target_uri
# COLLECTION_NAME = config.get_collection_name()
# UID_FILTER = config.uid_filter

# Ajuste: Carpeta donde están los archivos procesados
CARPETA_CSV = os.path.join(".", "results")
CSV_FALLIDOS = "fallidos.csv"
JSON_REPORTE = "reporte_onboarding.json"

def generar_id_proveedor(cadena_fecha_entrada, base_para_unicidad_cadena=""):
    """
    Genera un ID único para el proveedor basado en la fecha y un sufijo aleatorio.
    Args:
        cadena_fecha_entrada (str): Fecha de la transacción o del archivo.
        base_para_unicidad_cadena (str): Cadena base para unicidad (opcional).
    Returns:
        str: ID generado.
    """
    fecha_formateada_yyyymmdd = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
    if cadena_fecha_entrada and isinstance(cadena_fecha_entrada, str):
        fecha_parseada = None
        try:
            fecha_parseada = datetime.datetime.strptime(cadena_fecha_entrada, '%d/%m/%Y')
        except ValueError:
            try:
                fecha_parseada = pd.to_datetime(cadena_fecha_entrada, errors='coerce')
                if pd.isna(fecha_parseada):
                    fecha_parseada = None
            except Exception:
                pass
        if fecha_parseada:
            fecha_formateada_yyyymmdd = fecha_parseada.strftime('%Y%m%d')
        else:
            logger.warning(f"No se pudo parsear la fecha '{cadena_fecha_entrada}' para el ID. Usando fecha actual.")
    else:
        logger.info(f"Fecha no proporcionada o inválida para el ID. Usando fecha actual.")

    sufijo_aleatorio = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    id_final = f"{fecha_formateada_yyyymmdd}_{sufijo_aleatorio}"
    return id_final

def limpiar_campo(valor):
    """
    Limpia un campo eliminando espacios en blanco.
    Args:
        valor: Valor a limpiar.
    Returns:
        Valor limpio.
    """
    if isinstance(valor, str):
        return valor.strip()
    return valor

def limpiar_nit(nit):
    """
    Limpia el NIT dejando solo los dígitos.
    Args:
        nit (str): NIT a limpiar.
    Returns:
        str: NIT limpio.
    """
    if isinstance(nit, str):
        return ''.join(filter(str.isdigit, nit))
    return nit

def extraer_datos_transaccion(fila):
    """
    Extrae los datos de transacción relevantes de una fila del DataFrame.
    Args:
        fila (pd.Series): Fila del DataFrame.
    Returns:
        dict: Diccionario con los datos de la transacción.
    """
    transaccion = {}
    campos_transaccion = {
        'COMPROBANTE': 'comprobante',
        'FECHA': 'fecha',
        'DETALLE': 'detalle',
        'DEBITOS': 'debitos',
        'CREDITOS': 'creditos',
        'SALDO ACUMULADO': 'saldo_acumulado',
        'INV-CRUC-BASE': 'inv_cruc_base',
        'CENTRO COSTO': 'centro_costo'
    }
    for campo_original, campo_mongodb in campos_transaccion.items():
        if campo_original in fila and pd.notna(fila[campo_original]) and str(fila[campo_original]).strip():
            transaccion[campo_mongodb] = limpiar_campo(fila[campo_original])
    return transaccion if transaccion else None

# =============================
# PROCESAMIENTO DE ARCHIVOS CSV
# =============================

def leer_y_procesar_csvs():
    """
    Lee los archivos CSV procesados y retorna una lista de proveedores y transacciones.
    Returns:
        tuple: (proveedores, registros_fallidos, estadisticas)
    """
    proveedores = []
    registros_fallidos = []
    estadisticas = {
        "fecha_proceso": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "archivos_procesados": 0,
        "registros_procesados": 0,
        "registros_fallidos": 0,
        "errores": []
    }

    archivos = [f for f in os.listdir(CARPETA_CSV) if f.endswith('_Procesado.csv')]
    for archivo in archivos:
        registros_unicos_por_archivo = set()
        ruta_archivo = os.path.join(CARPETA_CSV, archivo)
        logger.info(f"Procesando archivo: {ruta_archivo}")
        try:
            marco_datos = pd.read_csv(ruta_archivo, encoding='utf-8', low_memory=False, dtype=str)
            estadisticas["archivos_procesados"] += 1

            columnas_requeridas = ['CUENTA', 'DESCRIPCION', 'NIT', 'NOMBRE', 'FECHA', 'DIG.VER.', 'CENTRO COSTO', 'SALDO ACUMULADO']
            columnas_faltantes = [col for col in columnas_requeridas if col not in marco_datos.columns]
            if columnas_faltantes:
                mensaje_error = f"Archivo {archivo} no tiene las columnas requeridas: {', '.join(columnas_faltantes)}"
                logger.error(mensaje_error)
                estadisticas["errores"].append(mensaje_error)
                continue

            # Agrupar por NIT para recolectar múltiples códigos PUC
            for nit, grupo in marco_datos.groupby('NIT'):
                estadisticas["registros_procesados"] += len(grupo)
                try:
                    if pd.isna(nit) or not nit:
                        logger.warning(f"Grupo con NIT nulo en {archivo}.")
                        registros_fallidos.append({
                            "archivo": archivo, "nit": "N/A", "error": "NIT nulo o inválido"
                        })
                        estadisticas["registros_fallidos"] += len(grupo)
                        continue

                    cuentas = []
                    transacciones = []
                    descripcion = None
                    nombre = None
                    fecha_csv = None
                    saldo_acumulado = None
                    tipo = None
                    tipoid = None
                    campos_adicionales = {}

                    for idx, fila in grupo.iterrows():
                        cuenta = limpiar_campo(fila.get('CUENTA'))
                        descripcion_temp = limpiar_campo(fila.get('DESCRIPCION'))
                        nombre_temp = limpiar_campo(fila.get('NOMBRE'))
                        fecha_csv_temp = limpiar_campo(fila.get('FECHA'))
                        centro_costo = limpiar_campo(fila.iloc[12]) if pd.notna(fila.iloc[12]) else ""
                        saldo_acumulado_temp = limpiar_campo(fila.get('SALDO ACUMULADO')) if pd.notna(fila.get('SALDO ACUMULADO')) else ""

                        if isinstance(nit, str) and len(nit) == 9 and (nit[0] in ['8', '9']):
                            tipo = 'Company'
                            tipoid = '31'
                        else:
                            tipo = 'Person'
                            tipoid = '13'

                        if not cuenta or not nit or not nombre_temp:
                            logger.warning(f"Fila {idx+2} en {archivo} no tiene CUENTA, NIT o NOMBRE indispensables.")
                            registros_fallidos.append({
                                "archivo": archivo, "fila": idx + 2, "cuenta": cuenta, "nit": nit, "nombre": nombre_temp,
                                "error": "Campos indispensables (CUENTA, NIT, NOMBRE) faltantes"
                            })
                            estadisticas["registros_fallidos"] += 1
                            continue

                        # Verificar duplicados por fila
                        clave_registro_entrada = f"{archivo}_{idx}"
                        if clave_registro_entrada in registros_unicos_por_archivo:
                            logger.info(f"Registro duplicado en archivo {archivo}, fila {idx+2} - Omitiendo")
                            continue
                        registros_unicos_por_archivo.add(clave_registro_entrada)

                        if cuenta not in cuentas:
                            cuentas.append(cuenta)
                        if not descripcion:
                            descripcion = descripcion_temp
                        if not nombre:
                            nombre = nombre_temp
                        if not fecha_csv:
                            fecha_csv = fecha_csv_temp
                        if not saldo_acumulado:
                            saldo_acumulado = saldo_acumulado_temp

                        for campo_col_original in marco_datos.columns:
                            if campo_col_original not in columnas_requeridas and pd.notna(fila[campo_col_original]):
                                valor = limpiar_campo(fila[campo_col_original])
                                if valor or isinstance(valor, (int, float)):
                                    nombre_campo_db = campo_col_original.replace(' ', '_').replace('.', '_').replace('-', '_').lower()
                                    campos_adicionales[nombre_campo_db] = valor

                        transaccion = extraer_datos_transaccion(fila)
                        if transaccion:
                            transacciones.append(transaccion)

                    proveedor = {
                        "nit": nit,
                        "descripcion": descripcion,
                        "name": nombre,
                        "cuentas": cuentas,  # Lista de códigos PUC
                        "tipo": tipo,
                        "tipoid": tipoid,
                        "saldo_acumulado": saldo_acumulado,
                        "campos_adicionales": campos_adicionales,
                        "transacciones": transacciones,
                        "fecha_csv": fecha_csv
                    }
                    proveedores.append(proveedor)
                except Exception as error_fila:
                    mensaje_error = f"Error al procesar grupo con NIT {nit} en archivo {archivo}: {str(error_fila)}"
                    logger.error(mensaje_error, exc_info=True)
                    registros_fallidos.append({
                        "archivo": archivo, "nit": nit, "error": str(error_fila)
                    })
                    estadisticas["registros_fallidos"] += len(grupo)
                    continue
        except pd.errors.EmptyDataError:
            mensaje_error = f"Archivo {archivo} está vacío o no es un CSV válido."
            logger.warning(mensaje_error)
            estadisticas["errores"].append(mensaje_error)
            continue
        except Exception as error_archivo:
            mensaje_error = f"Error al procesar archivo {archivo}: {str(error_archivo)}"
            logger.error(mensaje_error, exc_info=True)
            estadisticas["errores"].append(mensaje_error)
            continue

    return proveedores, registros_fallidos, estadisticas

# =============================
# SUBIDA DE DATOS A MONGODB
# =============================

def subir_proveedores_a_mongodb(proveedores, uid, config, TARGET_URI, COLLECTION_NAME):
    """
    Sube la lista de proveedores procesados a MongoDB.
    Args:
        proveedores (list): Lista de proveedores procesados.
        uid (ObjectId): UID del cliente/proyecto.
        config: Configuración de MongoDB.
        TARGET_URI: URI de conexión.
        COLLECTION_NAME: Nombre de la colección.
    Returns:
        dict: Estadísticas de la operación (creados/actualizados).
    """
    client = MongoClient(TARGET_URI)
    db = client[config.db_name]
    collection = db[COLLECTION_NAME]
    existing_providers_cursor = collection.find({"UID": uid}, {"nit": 1, "_id": 1})
    provider_nit_map = {p.get("nit"): p["_id"] for p in existing_providers_cursor if p.get("nit")}
    stats = {
        "proveedores_actualizados": 0,
        "proveedores_creados": 0
    }

    for proveedor in proveedores:
        nit = proveedor["nit"]
        cuentas = proveedor["cuentas"]
        descripcion = proveedor["descripcion"]
        nombre = proveedor["name"]
        tipo = proveedor["tipo"]
        tipoid = proveedor["tipoid"]
        saldo_acumulado = proveedor["saldo_acumulado"]
        transacciones = proveedor["transacciones"]

        proveedor_mongo_id = provider_nit_map.get(nit)
        if proveedor_mongo_id:
            update_data = {
                "$set": {
                    "descripcion": descripcion,
                    "name": nombre,
                    "UID": uid,
                    "defaultPUC": {"code": cuentas[0] if cuentas else None},
                    "personType": tipo,
                    "idType": tipoid,
                    "saldo_acumulado": saldo_acumulado,
                    "ultima_actualizacion": datetime.datetime.now(datetime.timezone.utc)
                },
                "$addToSet": {
                    "PUC": {"$each": cuentas}  # Agregar todos los códigos PUC
                }
            }
            if transacciones:
                update_data.setdefault("$push", {})["transacciones"] = {"$each": transacciones}
            logger.debug(f"Intentando actualizar NIT {nit} con datos: {update_data}")
            pre_update_doc = collection.find_one({"_id": proveedor_mongo_id})
            logger.debug(f"Documento antes de la actualización NIT {nit}: {pre_update_doc}")
            try:
                result = collection.update_one({"_id": proveedor_mongo_id}, update_data)
                if result.modified_count > 0:
                    logger.info(f"Actualizado proveedor con NIT: {nit} (Cuentas asociadas: {cuentas})")
                    stats["proveedores_actualizados"] += 1
                else:
                    logger.warning(f"No se modificó el proveedor con NIT: {nit}. Datos ya actualizados o error en la operación.")
                    post_update_doc = collection.find_one({"_id": proveedor_mongo_id})
                    logger.debug(f"Documento después de la actualización NIT {nit}: {post_update_doc}")
                    if pre_update_doc == post_update_doc:
                        logger.debug(f"Los datos en update_data son idénticos al documento existente para NIT {nit}.")
                    else:
                        logger.error(f"Actualización fallida para NIT {nit} sin cambios detectados. Posible problema de permisos o validador.")
            except Exception as update_error:
                logger.error(f"Error al actualizar NIT {nit}: {str(update_error)}")
                stats["proveedores_actualizados"] -= 1
        else:
            nuevo_id_proveedor = generar_id_proveedor(proveedor["fecha_csv"], nit)
            nuevo_proveedor_doc = {
                "id": nit,
                "UID": uid,
                "descripcion": descripcion,
                "name": nombre,
                "PUC": cuentas,
                "defaultPUC": {"code": cuentas[0] if cuentas else None},
                "personType": tipo,
                "idType": tipoid,
                "saldo_acumulado": saldo_acumulado,
                "ultima_actualizacion": datetime.datetime.now(datetime.timezone.utc)
            }
            if transacciones:
                nuevo_proveedor_doc["transacciones"] = transacciones
            try:
                collection.insert_one(nuevo_proveedor_doc)
                logger.info(f"Creado nuevo proveedor con NIT: {nit}, nuevo ID asignado: {nuevo_id_proveedor}")
                provider_nit_map[nit] = nuevo_id_proveedor
                stats["proveedores_creados"] += 1
            except Exception as insert_error:
                logger.error(f"Error al crear proveedor con NIT {nit}: {str(insert_error)}")
                stats["proveedores_creados"] -= 1

    client.close()
    return stats

def delete_existing_providers(uid, config):
    """
    Elimina todos los proveedores existentes en la colección de MongoDB para el UID dado.
    """
    db_manager = MongoDBManager(config)
    deleted_count = db_manager.delete_all_providers(uid)
    logger.info(f"Se eliminaron {deleted_count} proveedores existentes con UID: {uid}")
    db_manager.close()

# =============================
# MÉTODO PRINCIPAL DE SUBIDA
# =============================

def subir_main(uid, ambiente):
    """
    Orquesta el proceso completo de onboarding:
    - Elimina proveedores existentes.
    - Procesa los archivos CSV.
    - Sube los proveedores a MongoDB.
    - Muestra un resumen del proceso.
    Args:
        uid (str): UID del cliente/proyecto.
        ambiente (str): Ambiente de ejecución.
    """
    # Configuración de conexión a MongoDB según ambiente
    config = MongoDBConfig(env_prefix=ambiente)
    TARGET_URI = config.target_uri
    COLLECTION_NAME = config.get_collection_name()
    UID_FILTER = config.uid_filter

    if not isinstance(uid, ObjectId):
        try:
            uid = ObjectId(uid)
        except Exception:
            logger.error("El UID proporcionado no es válido. Debe ser un ObjectId de MongoDB.")
            return
    delete_existing_providers(uid, config)
    logger.info("=" * 60)
    logger.info("Iniciando proceso de onboarding de datos")
    logger.info("=" * 60)

    variables_requeridas_dev = [
        "DEV_AWS_ACCESS_KEY_ID", 
        "DEV_AWS_SECRET_ACCESS_KEY", 
        "DEV_CLUSTER_URL", 
        "DEV_DB", 
        "DEV_APP_NAME"
    ]
    variables_faltantes = [var for var in variables_requeridas_dev if not os.getenv(var)]

    if variables_faltantes:
        logger.error(f"Faltan variables de entorno: {', '.join(variables_faltantes)}")
        logger.error("Proceso cancelado. Asegúrese de que el archivo .env está configurado correctamente.")
        return

    proveedores, registros_fallidos, stats_csv = leer_y_procesar_csvs()
    stats_mongo = subir_proveedores_a_mongodb(proveedores, uid, config, TARGET_URI, COLLECTION_NAME)

    logger.info("=" * 60)
    logger.info("RESUMEN DEL PROCESO")
    logger.info("=" * 60)
    logger.info(f"Archivos procesados: {stats_csv.get('archivos_procesados', 0)}")
    logger.info(f"Registros procesados: {stats_csv.get('registros_procesados', 0)}")
    logger.info(f"Proveedores actualizados: {stats_mongo.get('proveedores_actualizados', 0)}")
    logger.info(f"Proveedores creados: {stats_mongo.get('proveedores_creados', 0)}")
    logger.info(f"Registros fallidos: {stats_csv.get('registros_fallidos', 0)}")

    if stats_csv.get('errores'):
        logger.info(f"Errores encontrados durante el proceso: {len(stats_csv['errores'])}")
    logger.info("=" * 60)
    logger.info(f"Proceso completado.")
    if os.path.exists(JSON_REPORTE):
        logger.info(f"Reporte guardado en: {JSON_REPORTE}")
    if stats_csv.get('registros_fallidos', 0) > 0 and os.path.exists(CSV_FALLIDOS):
        logger.info(f"Registros fallidos guardados en: {CSV_FALLIDOS}")
    logger.info("=" * 60)

# =============================
# MAIN
# =============================
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Uso: python subir_proveedores_mongodb.py <UID_USER> <ambiente>")
        sys.exit(1)
    try:
        uid = ObjectId(sys.argv[1])
    except Exception:
        print("El UID proporcionado no es válido. Debe ser un ObjectId de MongoDB.")
        sys.exit(1)
    ambiente = sys.argv[2]
    subir_main(uid, ambiente)
