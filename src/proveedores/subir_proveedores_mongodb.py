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
config = MongoDBConfig(env_prefix="DEV")
TARGET_URI = config.target_uri
COLLECTION_NAME = config.get_collection_name()
UID_FILTER = config.uid_filter

# Ajuste: Carpeta donde están los archivos procesados
CSV_FOLDER = os.path.join(".", "results")
FAILED_CSV = "fallidos.csv"
REPORT_JSON = "reporte_onboarding.json"

def generar_id_proveedor(fecha_str_input, base_para_unicidad_str=""):
    fecha_formateada_yyyymmdd = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
    if fecha_str_input and isinstance(fecha_str_input, str):
        parsed_date = None
        try:
            parsed_date = datetime.datetime.strptime(fecha_str_input, '%d/%m/%Y')
        except ValueError:
            try:
                parsed_date = pd.to_datetime(fecha_str_input, errors='coerce')
                if pd.isna(parsed_date):
                    parsed_date = None
            except Exception:
                pass
        if parsed_date:
            fecha_formateada_yyyymmdd = parsed_date.strftime('%Y%m%d')
        else:
            logger.warning(f"No se pudo parsear la fecha '{fecha_str_input}' para el ID. Usando fecha actual.")
    else:
        logger.info(f"Fecha no proporcionada o inválida para el ID. Usando fecha actual.")

    aleatorio_sufijo = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    id_final = f"{fecha_formateada_yyyymmdd}_{aleatorio_sufijo}"
    return id_final

def limpiar_campo(valor):
    if isinstance(valor, str):
        return valor.strip()
    return valor

def limpiar_nit(nit):
    if isinstance(nit, str):
        return ''.join(filter(str.isdigit, nit))
    return nit

def extraer_datos_transaccion(row):
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
        if campo_original in row and pd.notna(row[campo_original]) and str(row[campo_original]).strip():
            transaccion[campo_mongodb] = limpiar_campo(row[campo_original])
    return transaccion if transaccion else None

def procesar_archivos_csv():
    db_manager = None
    registros_fallidos = []
    stats = {
        "fecha_proceso": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "archivos_procesados": 0,
        "registros_procesados": 0,
        "proveedores_actualizados": 0,
        "proveedores_creados": 0,
        "registros_fallidos": 0,
        "errores": []
    }

    try:
        client = MongoClient(TARGET_URI)
        db = client[config.db_name]
        collection = db[COLLECTION_NAME]
        
        existing_providers_cursor = collection.find({}, {"nit": 1, "_id": 1})
        provider_nit_map = {p.get("nit"): p["_id"] for p in existing_providers_cursor if p.get("nit")}
        logger.info(f"Se encontraron {len(provider_nit_map)} proveedores existentes mapeados por NIT en la base de datos")
        
        
        # Solo considerar archivos procesados (_Procesado.csv)
        archivos = [f for f in os.listdir(CSV_FOLDER) if f.endswith('_Procesado.csv')]
        
        for archivo in archivos:
            registros_unicos_por_archivo = set()
            ruta_archivo = os.path.join(CSV_FOLDER, archivo)
            logger.info(f"Procesando archivo: {ruta_archivo}")
            try:
                df = pd.read_csv(ruta_archivo, encoding='utf-8', low_memory=False, dtype=str)
                stats["archivos_procesados"] += 1
                
                columnas_requeridas = ['CUENTA', 'DESCRIPCION', 'NIT', 'NOMBRE', 'FECHA','DIG.VER.',"CENTRO COSTO","SALDO ACUMULADO"]
                columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
                
                if columnas_faltantes:
                    mensaje_error = f"Archivo {archivo} no tiene las columnas requeridas: {', '.join(columnas_faltantes)}"
                    logger.error(mensaje_error)
                    stats["errores"].append(mensaje_error)
                    continue
                
                for idx, row in df.iterrows():
                    stats["registros_procesados"] += 1
                    try:
                        cuenta = limpiar_campo(row.get('CUENTA'))
                        descripcion = limpiar_campo(row.get('DESCRIPCION'))
                        nit = limpiar_nit(row.get('NIT'))
                        nombre = limpiar_campo(row.get('NOMBRE'))
                        fecha_csv = limpiar_campo(row.get('FECHA'))
                        tipo='' #puede ser Company o Person
                        tipoid='' #Puede ser 31 o 13
                        centro_costo = limpiar_campo(row.get("CENTRO COSTO"))
                        saldo_acumulado = limpiar_campo(row.get("SALDO ACUMULADO"))

                        if isinstance(nit, str) and len(nit) == 9 and (nit[0] == '8' or nit[0] == '9'):
                            tipo='Company'
                            tipoid='31'
                        else:
                            tipo='Person'
                            tipoid='13'    

                        registro_key_input = f"{nit}_{cuenta}"
                        
                        if not cuenta or not nit or not nombre:
                            logger.warning(f"Fila {idx+2} en {archivo} no tiene CUENTA, NIT o NOMBRE indispensables.")
                            registros_fallidos.append({
                                "archivo": archivo, "fila": idx + 2, "cuenta": cuenta, "nit": nit, "nombre": nombre,
                                "error": "Campos indispensables (CUENTA, NIT, NOMBRE) faltantes"
                            })
                            stats["registros_fallidos"] += 1
                            continue
                        
                        if registro_key_input in registros_unicos_por_archivo:
                            logger.info(f"Registro CSV duplicado (NIT {nit}, CUENTA {cuenta}) en archivo {archivo}, fila {idx+2} - Omitiendo")
                            continue
                        registros_unicos_por_archivo.add(registro_key_input)
                        
                        campos_adicionales = {}
                        for campo_col_original in df.columns:
                            if campo_col_original not in columnas_requeridas and pd.notna(row[campo_col_original]):
                                valor = limpiar_campo(row[campo_col_original])
                                if valor or isinstance(valor, (int, float)):
                                    nombre_campo_db = campo_col_original.replace(' ', '_').replace('.', '_').replace('-', '_').lower()
                                    campos_adicionales[nombre_campo_db] = valor
                        
                        transaccion = extraer_datos_transaccion(row)
                        
                        proveedor_mongo_id = provider_nit_map.get(nit)
                        
                        if proveedor_mongo_id:
                            update_data = {
                                "$set": {
                                    "descripcion": descripcion,
                                    "name": nombre,
                                    "UID": UID_FILTER,
                                    "defaultPUC": {"code": cuenta},
                                    "personType": tipo,
                                    "idType": tipoid,
                                   # "centro_costo": centro_costo,
                                    "saldo_acumulado": saldo_acumulado,
                                    "ultima_actualizacion": datetime.datetime.now(datetime.timezone.utc)
                                },
                                "$addToSet": {"PUC": cuenta}
                            }
                            if transaccion:
                                update_data.setdefault("$push", {})["transacciones"] = transaccion
                            collection.update_one({"_id": proveedor_mongo_id}, update_data)
                            logger.info(f"Actualizado proveedor con NIT: {nit} (CUENTA asociada en fila: {cuenta})")
                            stats["proveedores_actualizados"] += 1
                        else:
                            nuevo_id_proveedor = generar_id_proveedor(fecha_csv, nit)
                            nuevo_proveedor_doc = {
                                "id": nit,
                                "UID": UID_FILTER,
                                "descripcion": descripcion,
                                "name": nombre,
                                "PUC": [cuenta],
                                "defaultPUC": {"code": cuenta},
                                "personType": tipo,
                                "idType": tipoid,
                                "centro_costo": centro_costo,
                            }
                            if transaccion:
                                nuevo_proveedor_doc["transacciones"] = [transaccion]
                            result = collection.insert_one(nuevo_proveedor_doc)
                            logger.info(f"Creado nuevo proveedor con NIT: {nit}, nuevo ID asignado: {nuevo_id_proveedor}, _id MongoDB: {result.inserted_id}")
                            provider_nit_map[nit] = result.inserted_id
                            stats["proveedores_creados"] += 1
                    except Exception as row_error:
                        mensaje_error = f"Error al procesar fila {idx+2} en archivo {archivo}: {str(row_error)}"
                        logger.error(mensaje_error, exc_info=True)
                        registros_fallidos.append({
                             "archivo": archivo, "fila": idx + 2, "cuenta": row.get('CUENTA', 'N/A'), "nit": row.get('NIT', 'N/A'), 
                             "nombre": row.get('NOMBRE', 'N/A'), "error": str(row_error)
                        })
                        stats["registros_fallidos"] += 1
                        continue
            except pd.errors.EmptyDataError:
                mensaje_error = f"Archivo {archivo} está vacío o no es un CSV válido."
                logger.warning(mensaje_error)
                stats["errores"].append(mensaje_error)
                continue
            except Exception as file_error:
                mensaje_error = f"Error al procesar archivo {archivo}: {str(file_error)}"
                logger.error(mensaje_error, exc_info=True)
                stats["errores"].append(mensaje_error)
                continue
    finally:
        if registros_fallidos:
            df_fallidos = pd.DataFrame(registros_fallidos)
            df_fallidos.to_csv(FAILED_CSV, index=False, encoding='utf-8')
            logger.info(f"Se guardaron {len(registros_fallidos)} registros fallidos en {FAILED_CSV}")
        stats["fecha_finalizacion_proceso"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(REPORT_JSON, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)
            logger.info(f"Reporte de estadísticas guardado en {REPORT_JSON}")
        except Exception as report_error:
            logger.error(f"No se pudo guardar el reporte JSON: {report_error}", exc_info=True)

        if db_manager:
            db_manager.close()
            logger.info("Conexión a MongoDB cerrada")
    return stats

def surtiflora_main():
    delete_existing_providers()
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

    stats_resultado = procesar_archivos_csv()

    logger.info("=" * 60)
    logger.info("RESUMEN DEL PROCESO")
    logger.info("=" * 60)
    logger.info(f"Archivos procesados: {stats_resultado.get('archivos_procesados', 0)}")
    logger.info(f"Registros procesados: {stats_resultado.get('registros_procesados', 0)}")
    logger.info(f"Proveedores actualizados: {stats_resultado.get('proveedores_actualizados', 0)}")
    logger.info(f"Proveedores creados: {stats_resultado.get('proveedores_creados', 0)}")
    logger.info(f"Registros fallidos: {stats_resultado.get('registros_fallidos', 0)}")

    if stats_resultado.get('errores'):
        logger.info(f"Errores encontrados durante el proceso: {len(stats_resultado['errores'])})")
    logger.info("=" * 60)
    logger.info(f"Proceso completado.")
    if os.path.exists(REPORT_JSON):
        logger.info(f"Reporte guardado en: {REPORT_JSON}")
    if stats_resultado.get('registros_fallidos', 0) > 0 and os.path.exists(FAILED_CSV):
         logger.info(f"Registros fallidos guardados en: {FAILED_CSV}")
    logger.info("=" * 60)

def delete_existing_providers():
    db_manager = MongoDBManager(config)
    deleted_count = db_manager.delete_all_providers(UID_FILTER)
    logger.info(f"Se eliminaron {deleted_count} proveedores existentes con UID: {UID_FILTER}")
    db_manager.close()


if __name__ == "__main__":
    delete_existing_providers()
    surtiflora_main()
