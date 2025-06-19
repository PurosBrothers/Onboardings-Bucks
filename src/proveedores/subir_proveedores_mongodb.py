import os
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
from config.mongodb_config import MongoDBConfig
from utils.mongodb_manager import MongoDBManager
from dotenv import load_dotenv
import logging
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

CARPETA_CSV = os.path.join(".", "results")
ARCHIVO_FALLIDOS = "fallidos.csv"
REPORTE_JSON = "reporte_onboarding.json"

# =============================
# FUNCIONES AUXILIARES
# =============================
def generar_id_proveedor(fecha_str_input, base_para_unicidad=""):
    """
    Genera un ID único de proveedor basado en la fecha y un sufijo aleatorio.
    """
    fecha_formateada = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
    if fecha_str_input and isinstance(fecha_str_input, str):
        try:
            fecha_parseada = datetime.datetime.strptime(fecha_str_input, '%d/%m/%Y')
            fecha_formateada = fecha_parseada.strftime('%Y%m%d')
        except Exception:
            try:
                fecha_parseada = pd.to_datetime(fecha_str_input, errors='coerce')
                if not pd.isna(fecha_parseada):
                    fecha_formateada = fecha_parseada.strftime('%Y%m%d')
            except Exception:
                logger.warning(f"No se pudo parsear la fecha '{fecha_str_input}' para el ID del proveedor. Se usará la fecha actual.")
    sufijo_aleatorio = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{fecha_formateada}_{sufijo_aleatorio}"

def limpiar_campo(valor):
    if isinstance(valor, str):
        return valor.strip()
    return valor

def limpiar_nit(nit):
    if isinstance(nit, str):
        return ''.join(filter(str.isdigit, nit))
    return nit

def extraer_datos_transaccion(row):
    """
    Extrae los datos de transacción de una fila, mapeando a claves en inglés para MongoDB.
    """
    transaccion = {}
    mapa_campos = {
        'COMPROBANTE': 'voucher',
        'FECHA': 'date',
        'DETALLE': 'detail',
        'DEBITOS': 'debits',
        'CREDITOS': 'credits',
        'SALDO ACUMULADO': 'accumulated_balance',
        'INV-CRUC-BASE': 'inv_cruc_base',
        'CENTRO COSTO': 'cost_center'
    }
    for original, clave_mongo in mapa_campos.items():
        if original in row and pd.notna(row[original]) and str(row[original]).strip():
            transaccion[clave_mongo] = limpiar_campo(row[original])
    return transaccion if transaccion else None

# =============================
# LIMPIEZA DE DATOS PREVIOS
# =============================
def limpiar_datos_previos_uid(uid):
    """
    Elimina todos los documentos de la colección que tengan el UID especificado.
    Args:
        uid (ObjectId): UID cuyos datos se quieren eliminar
    Returns:
        int: Número de documentos eliminados
    """
    try:
        logger.info(f"Iniciando limpieza de datos previos para UID: {uid}")
        
        config = MongoDBConfig(env_prefix="DEV")
        TARGET_URI = config.target_uri
        COLLECTION_NAME = config.get_collection_name()
        
        client = MongoClient(TARGET_URI)
        db = client[config.db_name]
        collection = db[COLLECTION_NAME]
        
        # Contar documentos existentes antes de eliminar
        documentos_existentes = collection.count_documents({"UID": uid})
        logger.info(f"Documentos encontrados con UID {uid}: {documentos_existentes}")
        
        if documentos_existentes > 0:
            # Eliminar todos los documentos con ese UID
            resultado = collection.delete_many({"UID": uid})
            documentos_eliminados = resultado.deleted_count
            
            logger.info(f"Limpieza completada. Documentos eliminados: {documentos_eliminados}")
            
            # Verificar que se eliminaron correctamente
            verificacion = collection.count_documents({"UID": uid})
            if verificacion == 0:
                logger.info(f"Verificación exitosa: No quedan documentos con UID {uid}")
            else:
                logger.warning(f"Advertencia: Aún quedan {verificacion} documentos con UID {uid}")
        else:
            logger.info(f"No se encontraron documentos previos con UID {uid}")
            documentos_eliminados = 0
        
        client.close()
        return documentos_eliminados
        
    except Exception as e:
        logger.error(f"Error durante la limpieza de datos previos: {e}")
        raise e

# =============================
# PROCESAMIENTO DEL CSV
# =============================
def procesar_archivos_csv(uid):
    """
    Procesa todos los archivos CSV procesados y retorna una lista de documentos de proveedores listos para MongoDB.
    Args:
        uid (ObjectId): UID a asociar a cada proveedor
    Returns:
        List[dict]: Lista de documentos de proveedores
    """
    proveedores_dict = {}  # 🔧 CAMBIO: Usar diccionario para deduplicar globalmente
    registros_fallidos = []
    archivos = [f for f in os.listdir(CARPETA_CSV) if f.endswith('_Procesado.csv')]
    
    logger.info(f"Archivos CSV encontrados: {len(archivos)}")
    
    for archivo in archivos:
        ruta_archivo = os.path.join(CARPETA_CSV, archivo)
        logger.info(f"Procesando archivo: {archivo}")
        
        try:
            df = pd.read_csv(ruta_archivo, encoding='utf-8', low_memory=False, dtype=str)
            columnas_requeridas = ['CUENTA', 'DESCRIPCION', 'NIT', 'NOMBRE', 'FECHA', 'DIG.VER.', 'CENTRO COSTO', 'SALDO ACUMULADO']
            columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
            
            if columnas_faltantes:
                logger.error(f"El archivo {archivo} no tiene las columnas requeridas: {', '.join(columnas_faltantes)}")
                continue
                
            for idx, row in df.iterrows():
                try:
                    cuenta = limpiar_campo(row.get('CUENTA'))
                    descripcion = limpiar_campo(row.get('DESCRIPCION'))
                    nit = limpiar_nit(row.get('NIT'))
                    nombre = limpiar_campo(row.get('NOMBRE'))
                    fecha_csv = limpiar_campo(row.get('FECHA'))
                    centro_costo = limpiar_campo(row.get('CENTRO COSTO'))
                    saldo_acumulado = limpiar_campo(row.get('SALDO ACUMULADO'))
                    
                    if not cuenta or not nit or not nombre:
                        registros_fallidos.append({
                            "archivo": archivo, "fila": idx + 2, "cuenta": cuenta, "nit": nit, "nombre": nombre,
                            "error": "Faltan campos requeridos (cuenta, nit, nombre)"
                        })
                        continue
                    
                    # 🔧 CAMBIO: Usar solo NIT como clave única (no NIT + cuenta)
                    clave_proveedor = nit
                    
                    tipo_persona = 'Company' if isinstance(nit, str) and len(nit) == 9 and (nit[0] == '8' or nit[0] == '9') else 'Person'
                    tipo_id = '31' if tipo_persona == 'Company' else '13'
                    
                    # Mapear campos adicionales a inglés
                    campos_extra = {}
                    for col in df.columns:
                        if col not in columnas_requeridas and pd.notna(row[col]):
                            valor = limpiar_campo(row[col])
                            if valor or isinstance(valor, (int, float)):
                                nombre_campo = col.replace(' ', '_').replace('.', '_').replace('-', '_').lower()
                                campos_extra[nombre_campo] = valor
                    
                    transaccion = extraer_datos_transaccion(row)
                    
                    # 🔧 CAMBIO: Si el proveedor ya existe, consolidar datos
                    if clave_proveedor in proveedores_dict:
                        proveedor_existente = proveedores_dict[clave_proveedor]
                        
                        # Consolidar PUC (agregar cuenta si no existe)
                        if cuenta not in proveedor_existente["PUC"]:
                            proveedor_existente["PUC"].append(cuenta)
                        
                        # Agregar transacción si existe
                        if transaccion:
                            if "transactions" not in proveedor_existente:
                                proveedor_existente["transactions"] = []
                            proveedor_existente["transactions"].append(transaccion)
                        
                        # Actualizar campos extra
                        proveedor_existente.update(campos_extra)
                        
                        logger.debug(f"Proveedor {nit} consolidado desde {archivo}")
                        
                    else:
                        # 🔧 CAMBIO: Crear nuevo proveedor
                        proveedor_doc = {
                            "id": nit,
                            "UID": uid,
                            "description": descripcion,
                            "name": nombre,
                            "PUC": [cuenta],
                            "defaultPUC": {"code": cuenta},
                            "personType": tipo_persona,
                            "idType": tipo_id,
                            "cost_center": centro_costo,
                            "accumulated_balance": saldo_acumulado,
                        }
                        
                        proveedor_doc.update(campos_extra)
                        
                        if transaccion:
                            proveedor_doc["transactions"] = [transaccion]
                        
                        proveedores_dict[clave_proveedor] = proveedor_doc
                        logger.debug(f"Proveedor {nit} creado desde {archivo}")
                        
                except Exception as row_error:
                    registros_fallidos.append({
                        "archivo": archivo, "fila": idx + 2, "cuenta": row.get('CUENTA', 'N/A'), "nit": row.get('NIT', 'N/A'),
                        "nombre": row.get('NOMBRE', 'N/A'), "error": str(row_error)
                    })
                    continue
                    
        except Exception as file_error:
            logger.error(f"Error procesando el archivo {archivo}: {file_error}")
            continue
    
    # 🔧 CAMBIO: Convertir diccionario a lista
    proveedores = list(proveedores_dict.values())
    
    if registros_fallidos:
        pd.DataFrame(registros_fallidos).to_csv(ARCHIVO_FALLIDOS, index=False, encoding='utf-8')
        logger.info(f"Se guardaron {len(registros_fallidos)} registros fallidos en {ARCHIVO_FALLIDOS}")
    
    logger.info(f"Total de proveedores únicos procesados: {len(proveedores)}")
    return proveedores
    """
    Procesa todos los archivos CSV procesados y retorna una lista de documentos de proveedores listos para MongoDB.
    Args:
        uid (ObjectId): UID a asociar a cada proveedor
    Returns:
        List[dict]: Lista de documentos de proveedores
    """
    proveedores = []
    registros_fallidos = []
    archivos = [f for f in os.listdir(CARPETA_CSV) if f.endswith('_Procesado.csv')]
    
    logger.info(f"Archivos CSV encontrados: {len(archivos)}")
    
    for archivo in archivos:
        ruta_archivo = os.path.join(CARPETA_CSV, archivo)
        logger.info(f"Procesando archivo: {archivo}")
        
        try:
            df = pd.read_csv(ruta_archivo, encoding='utf-8', low_memory=False, dtype=str)
            columnas_requeridas = ['CUENTA', 'DESCRIPCION', 'NIT', 'NOMBRE', 'FECHA', 'DIG.VER.', 'CENTRO COSTO', 'SALDO ACUMULADO']
            columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
            
            if columnas_faltantes:
                logger.error(f"El archivo {archivo} no tiene las columnas requeridas: {', '.join(columnas_faltantes)}")
                continue
                
            claves_unicas = set()
            for idx, row in df.iterrows():
                try:
                    cuenta = limpiar_campo(row.get('CUENTA'))
                    descripcion = limpiar_campo(row.get('DESCRIPCION'))
                    nit = limpiar_nit(row.get('NIT'))
                    nombre = limpiar_campo(row.get('NOMBRE'))
                    fecha_csv = limpiar_campo(row.get('FECHA'))
                    centro_costo = limpiar_campo(row.get('CENTRO COSTO'))
                    saldo_acumulado = limpiar_campo(row.get('SALDO ACUMULADO'))
                    
                    tipo_persona = 'Company' if isinstance(nit, str) and len(nit) == 9 and (nit[0] == '8' or nit[0] == '9') else 'Person'
                    tipo_id = '31' if tipo_persona == 'Company' else '13'
                    clave_registro = f"{nit}_{cuenta}"
                    
                    if not cuenta or not nit or not nombre:
                        registros_fallidos.append({
                            "archivo": archivo, "fila": idx + 2, "cuenta": cuenta, "nit": nit, "nombre": nombre,
                            "error": "Faltan campos requeridos (cuenta, nit, nombre)"
                        })
                        continue
                        
                    if clave_registro in claves_unicas:
                        continue
                        
                    claves_unicas.add(clave_registro)
                    
                    # Mapear campos adicionales a inglés
                    campos_extra = {}
                    for col in df.columns:
                        if col not in columnas_requeridas and pd.notna(row[col]):
                            valor = limpiar_campo(row[col])
                            if valor or isinstance(valor, (int, float)):
                                nombre_campo = col.replace(' ', '_').replace('.', '_').replace('-', '_').lower()
                                campos_extra[nombre_campo] = valor
                                
                    transaccion = extraer_datos_transaccion(row)
                    
                    proveedor_doc = {
                        "id": nit,
                        "UID": uid,
                        "description": descripcion,
                        "name": nombre,
                        "PUC": [cuenta],
                        "defaultPUC": {"code": cuenta},
                        "personType": tipo_persona,
                        "idType": tipo_id,
                        "cost_center": centro_costo,
                        "accumulated_balance": saldo_acumulado,
                    }
                    
                    proveedor_doc.update(campos_extra)
                    
                    if transaccion:
                        proveedor_doc["transactions"] = [transaccion]
                        
                    proveedores.append(proveedor_doc)
                    
                except Exception as row_error:
                    registros_fallidos.append({
                        "archivo": archivo, "fila": idx + 2, "cuenta": row.get('CUENTA', 'N/A'), "nit": row.get('NIT', 'N/A'),
                        "nombre": row.get('NOMBRE', 'N/A'), "error": str(row_error)
                    })
                    continue
                    
        except Exception as file_error:
            logger.error(f"Error procesando el archivo {archivo}: {file_error}")
            continue
            
    if registros_fallidos:
        pd.DataFrame(registros_fallidos).to_csv(ARCHIVO_FALLIDOS, index=False, encoding='utf-8')
        logger.info(f"Se guardaron {len(registros_fallidos)} registros fallidos en {ARCHIVO_FALLIDOS}")
        
    logger.info(f"Total de proveedores procesados: {len(proveedores)}")
    return proveedores

# =============================
# SUBIDA A MONGODB
# =============================
def subir_proveedores_a_mongodb(uid, proveedores):
    """
    Sube la lista de documentos de proveedores a MongoDB usando UPSERT.
    Args:
        uid (ObjectId): UID a asociar
        proveedores (List[dict]): Lista de documentos de proveedores
    """
    config = MongoDBConfig(env_prefix="DEV")
    TARGET_URI = config.target_uri
    COLLECTION_NAME = config.get_collection_name()
    
    logger.info(f"Conectando a MongoDB...")
    logger.info(f"URI: {TARGET_URI[:50]}...")  # Solo mostrar parte de la URI por seguridad
    logger.info(f"Base de datos: {config.db_name}")
    logger.info(f"Coleccion: {COLLECTION_NAME}")
    
    try:
        client = MongoClient(TARGET_URI)
        
        # Verificar conexión
        client.admin.command('ping')
        logger.info("Conexion a MongoDB exitosa")
        
        db = client[config.db_name]
        collection = db[COLLECTION_NAME]
        
        logger.info(f"Procesando {len(proveedores)} proveedores con UPSERT...")
        
        creados = 0
        actualizados = 0
        errores = 0
        errores_detalle = []
        
        for i, doc in enumerate(proveedores):
            try:
                # Verificar que el documento tenga los campos requeridos
                if not doc.get('id') or not doc.get('UID'):
                    error_msg = f"Documento {i+1} falta campos requeridos: id={doc.get('id')}, UID={doc.get('UID')}"
                    logger.error(error_msg)
                    errores_detalle.append(error_msg)
                    errores += 1
                    continue
                
                # Usar replace_one con upsert=True
                filtro = {"UID": uid, "id": doc["id"]}
                
                logger.debug(f"Procesando proveedor {i+1}/{len(proveedores)}: {doc['id']}")
                
                resultado = collection.replace_one(
                    filtro, 
                    doc, 
                    upsert=True
                )
                
                if resultado.upserted_id:
                    creados += 1
                    logger.debug(f"Proveedor {doc['id']} CREADO")
                elif resultado.modified_count > 0:
                    actualizados += 1
                    logger.debug(f"Proveedor {doc['id']} ACTUALIZADO")
                else:
                    logger.debug(f"Proveedor {doc['id']} SIN CAMBIOS")
                
                # Log de progreso cada 100 registros
                if (i + 1) % 100 == 0:
                    logger.info(f"Progreso: {i+1}/{len(proveedores)} procesados")
                    
            except Exception as e:
                error_msg = f"Error procesando proveedor {doc.get('id', 'N/A')}: {str(e)}"
                logger.error(error_msg)
                errores_detalle.append(error_msg)
                errores += 1
        
        # Verificar que realmente se insertaron/actualizaron
        total_documentos = collection.count_documents({"UID": uid})
        logger.info(f"Verificacion: Total documentos con UID {uid} en base de datos: {total_documentos}")
        
        client.close()
        
        # Logs sin emojis para evitar errores de encoding
        logger.info(f"RESULTADOS:")
        logger.info(f"  - Proveedores creados: {creados}")
        logger.info(f"  - Proveedores actualizados: {actualizados}")
        logger.info(f"  - Total exitosos: {creados + actualizados}")
        logger.info(f"  - Errores: {errores}")
        
        if errores > 0:
            logger.warning(f"ERRORES DETALLADOS:")
            for error in errores_detalle[:5]:  # Mostrar solo los primeros 5 errores
                logger.warning(f"  - {error}")
            if len(errores_detalle) > 5:
                logger.warning(f"  - ... y {len(errores_detalle) - 5} errores mas")
        
        # Validar que el proceso fue realmente exitoso
        if creados + actualizados == 0 and len(proveedores) > 0:
            raise Exception(f"FALLO CRITICO: Se intentaron procesar {len(proveedores)} proveedores pero ninguno fue insertado/actualizado")
        
        return actualizados, creados
        
    except Exception as e:
        logger.error(f"ERROR CRITICO en subir_proveedores_a_mongodb: {str(e)}")
        logger.error(f"Tipo de error: {type(e).__name__}")
        
        # Intentar cerrar conexión si existe
        try:
            if 'client' in locals():
                client.close()
        except:
            pass
            
        raise e
# =============================
# FUNCIÓN PRINCIPAL MEJORADA
# =============================
def subir_main(uid, limpiar_antes=True):
    """
    Función principal para procesar los CSV y subir proveedores a MongoDB.
    Args:
        uid (ObjectId|str): UID a asociar a todos los proveedores (se convertirá a ObjectId si es necesario)
        limpiar_antes (bool): Si True, elimina datos previos antes de insertar
    """
    # Asegurar que uid es un ObjectId
    if not isinstance(uid, ObjectId):
        try:
            uid = ObjectId(uid)
        except Exception:
            logger.error("El UID proporcionado no es válido. Debe ser un ObjectId de MongoDB.")
            raise ValueError("El UID proporcionado no es válido. Debe ser un ObjectId de MongoDB.")
    logger.info("=" * 80)
    logger.info("INICIANDO PROCESO DE ONBOARDING DE PROVEEDORES")
    logger.info(f"UID: {uid}")
    logger.info(f"Limpiar datos previos: {'SÍ' if limpiar_antes else 'NO'}")
    logger.info("=" * 80)
    try:
        if limpiar_antes:
            documentos_eliminados = limpiar_datos_previos_uid(uid)
            logger.info(f"Resumen limpieza: {documentos_eliminados} documentos eliminados")
        logger.info("PASO 2: Procesando archivos CSV...")
        proveedores = procesar_archivos_csv(uid)
        if not proveedores:
            logger.warning("No se encontraron proveedores para procesar")
            return
        logger.info("PASO 3: Subiendo datos a MongoDB...")
        actualizados, creados = subir_proveedores_a_mongodb(uid, proveedores)
        logger.info("=" * 80)
        logger.info("PROCESO COMPLETADO")
        logger.info(f"RESUMEN:")
        logger.info(f"   • Proveedores actualizados: {actualizados}")
        logger.info(f"   • Proveedores creados: {creados}")
        logger.info(f"   • Total procesados: {actualizados + creados}")
        logger.info("=" * 80)
    except Exception as e:
        logger.error("=" * 80)
        logger.error("ERROR EN EL PROCESO DE ONBOARDING")
        logger.error(f"Error: {e}")
        logger.error("=" * 80)
        raise e

# =============================
# MAIN
# =============================
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: python subir_proveedores_mongodb.py <UID_USER> [--no-cleanup]")
        print("  <UID_USER>: ObjectId del usuario")
        print("  --no-cleanup: (Opcional) No eliminar datos previos")
        sys.exit(1)
    try:
        uid = ObjectId(sys.argv[1])
    except Exception:
        print("El UID proporcionado no es válido. Debe ser un ObjectId de MongoDB.")
        sys.exit(1)
    limpiar_antes = True
    if len(sys.argv) > 2 and '--no-cleanup' in sys.argv[2:]:
        limpiar_antes = False
    subir_main(uid, limpiar_antes)