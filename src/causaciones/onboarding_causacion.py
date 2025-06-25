#!/usr/bin/env python3
"""
Script para procesar el archivo de modelo de causación y almacenar los datos en MongoDB en la colección client_pucs.
"""

# =============================
# Imports y configuración global
# =============================
import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from bson import ObjectId
import openpyxl
from config.mongodb_config import MongoDBConfig
from utils.mongodb_manager import MongoDBManager
from config.beanie_config import init_db

AMBIENTE = "STAGING"



# =============================
# Configuración de logging
# =============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================
# Utilidades de Excel
# =============================
def obtener_encabezados_excel(libro, fila_encabezados=5):
    """
    Obtiene los encabezados del archivo Excel a partir de la fila especificada.
    """
    hoja = libro["Hoja1"]
    fila = list(hoja.iter_rows(min_row=fila_encabezados, max_row=fila_encabezados, values_only=True))[0]
    encabezados = [str(h).strip() if h is not None else "" for h in fila]
    logger.info(f"Encabezados encontrados: {encabezados}")
    return encabezados

# =============================
# Índices y limpieza en MongoDB
# =============================
def crear_indices_client_pucs(uid_usuario: str):
    """
    Crea los índices necesarios para la colección client_pucs.
    """
    try:
        config = MongoDBConfig(env_prefix=AMBIENTE)
        config.set_collection_name("client_pucs")
        gestor = MongoDBManager(config)
        gestor.collection.create_index(
            [("UID", 1), ("cuenta_contable", 1)],
            unique=True,
            name="uid_cuenta_contable_unique"
        )
        logger.info("Índice compuesto único creado: UID + cuenta_contable")
        gestor.close()
    except Exception as e:
        logger.error(f"Error creando índices: {str(e)}")
        logger.warning("Continuando sin crear índices...")

def eliminar_client_pucs_existentes(uid_usuario: str):
    """
    Elimina todos los documentos existentes para el usuario en la colección client_pucs.
    """
    try:
        config = MongoDBConfig(env_prefix=AMBIENTE)
        config.set_collection_name("client_pucs")
        gestor = MongoDBManager(config)
        resultado = gestor.collection.delete_many({"UID": ObjectId(uid_usuario)})
        logger.info(f"Eliminados {resultado.deleted_count} documentos existentes para el usuario {uid_usuario}")
        gestor.close()
    except Exception as e:
        logger.error(f"Error eliminando documentos existentes: {str(e)}")
        sys.exit(1)

# =============================
# Procesamiento y subida de datos
# =============================
def procesar_archivo_excel(uid_usuario: str, ruta_xlsx: str):
    """
    Procesa el archivo Excel y almacena los datos en MongoDB.
    """
    try:
        if not os.path.exists(ruta_xlsx):
            logger.error(f"Archivo Excel no encontrado: {ruta_xlsx}")
            sys.exit(1)
        logger.info("Eliminando documentos existentes...")
        eliminar_client_pucs_existentes(uid_usuario)
        logger.info("Creando índices...")
        crear_indices_client_pucs(uid_usuario)
        config = MongoDBConfig(env_prefix=AMBIENTE)
        config.set_collection_name("client_pucs")
        gestor = MongoDBManager(config)
        logger.info(f"Cargando archivo Excel: {ruta_xlsx}")
        libro = openpyxl.load_workbook(ruta_xlsx, data_only=True)
        if "Hoja1" not in libro.sheetnames:
            logger.error(f"Hoja 'Hoja1' no encontrada en el archivo. Hojas disponibles: {libro.sheetnames}")
            sys.exit(1)
        encabezados = obtener_encabezados_excel(libro)
        hoja = libro["Hoja1"]
        filas_procesadas = 0
        filas_omitidas = 0
        for idx, fila in enumerate(hoja.iter_rows(min_row=6, values_only=True), start=6):
            idx_cuenta = encabezados.index("CUENTA CONTABLE   (OBLIGATORIO)")
            cuenta_contable = str(fila[idx_cuenta]).strip() if fila[idx_cuenta] is not None else ""
            idx_centro = encabezados.index("CENTRO DE COSTO")
            centro_costo = str(fila[idx_centro]).strip() if fila[idx_centro] is not None else ""
            idx_nit = encabezados.index("NIT")
            nit = str(fila[idx_nit]).strip() if fila[idx_nit] is not None else ""
            idx_subcentro = encabezados.index("SUBCENTRO DE COSTO")
            subcentro_costo = str(fila[idx_subcentro]).strip() if fila[idx_subcentro] is not None else ""
            if not cuenta_contable:
                continue
            doc_existente = gestor.collection.find_one({
                "UID": ObjectId(uid_usuario),
                "cuenta_contable": cuenta_contable
            })
            if doc_existente:
                logger.warning(f"Cuenta contable '{cuenta_contable}' ya existe, saltando...")
                filas_omitidas += 1
                continue
            puc = cuenta_contable[:6]
            if buscar_embedding(puc):
                documento = {
                    "UID": ObjectId(uid_usuario),
                    "cuenta_contable": cuenta_contable,
                    "description": "",
                    "code_field": "",
                    "createdAt": datetime.now(),
                    "updatedAt": datetime.now()
                }
            else:
                documento = {
                    "UID": ObjectId(uid_usuario),
                    "cuenta_contable": cuenta_contable,
                    "description": "uniquePuc",
                    "code_field": "",
                    "createdAt": datetime.now(),
                    "updatedAt": datetime.now()
                }
            gestor.collection.insert_one(documento)
            crear_centro_costo_por_puc(uid_usuario, nit, cuenta_contable, centro_costo, subcentro_costo)
            filas_procesadas += 1
            logger.info(f"Documento creado: {documento}")
            if filas_procesadas % 100 == 0:
                logger.info(f"Procesadas {filas_procesadas} filas...")
        logger.info(f"Proceso completado. Se procesaron {filas_procesadas} filas en total.")
        if filas_omitidas > 0:
            logger.info(f"Se saltaron {filas_omitidas} filas con cuentas contables duplicadas.")
        gestor.close()
    except Exception as e:
        logger.error(f"Error procesando archivo Excel: {str(e)}")
        sys.exit(1)

# =============================
# Utilidades de búsqueda y limpieza
# =============================
def buscar_embedding(puc: str):
    """
    Busca si existe un embedding para el PUC dado en la colección puc_embeddings.
    """
    try:
        config = MongoDBConfig(env_prefix="DEV")
        config.set_collection_name("puc_embeddings")
        gestor_embedding = MongoDBManager(config)
        existe_puc = gestor_embedding.collection.find_one({"code": puc})
        return bool(existe_puc)
    except Exception as e:
        logger.error(f"Error buscando embedding para el PUC {puc}: {str(e)}")
        return False

def limpiar_nit(nit):
    """
    Limpia el NIT/cédula de caracteres no numéricos y espacios.
    """
    if isinstance(nit, str):
        return ''.join(filter(str.isdigit, nit))
    return nit

# =============================
# Creación de centro de costo por PUC
# =============================
def crear_centro_costo_por_puc(uid: str, nit: str, cuenta: str, centro: str, subcentro: str = None):
    """
    Crea un documento de centro de costo por PUC en la colección correspondiente.
    """
    logger.info(f"Iniciando creación de centro de costo para PUC: UID={uid}, NIT={nit}, Cuenta={cuenta}, CentroCosto={centro}, SubCentro={subcentro}")
    config = MongoDBConfig(env_prefix=AMBIENTE)
    config.set_collection_name("cost_center_per_puc")
    gestor = MongoDBManager(config)
    nit_limpio = limpiar_nit(nit)
    centro_costo_obj = {"center": centro}
    if subcentro:
        centro_costo_obj["subcenter"] = subcentro
    doc_existente = gestor.collection.find_one({
        "UID": ObjectId(uid),
        "id_supplier": nit_limpio,
        "account_code": cuenta,
        "cost_center": centro_costo_obj,
    })
    if doc_existente:
        logger.warning(f"Ya existe un centro de costo para la cuenta '{cuenta}' y NIT '{nit_limpio}', se omite...")
        return
    documento = {
        "uid": ObjectId(uid),
        "id_supplier": nit_limpio,
        "account_code": cuenta,
        "cost_center": centro_costo_obj
    }
    resultado = gestor.collection.insert_one(documento)
    logger.info(f"Centro de costo por PUC creado con _id: {resultado.inserted_id} para NIT: {nit_limpio}, Cuenta: {cuenta}, CentroCosto: {centro_costo_obj}")

# =============================
# Función principal
# =============================
def main(uid_usuario=None, ruta_xlsx=None):
    """
    Orquesta el proceso completo de onboarding de causación.
    Recibe el UID y la ruta del archivo Excel como argumentos.
    Si no se proporcionan, los toma de la línea de comandos o usa la ruta por defecto.
    """
    logger.info("Iniciando procesamiento del archivo de causación...")
    if uid_usuario is None:
        if len(sys.argv) < 2:
            print("Uso: python onboarding_causacion.py <UID> [ruta_xlsx]")
            sys.exit(1)
        try:
            uid_usuario = ObjectId(sys.argv[1])
        except Exception:
            print("El UID proporcionado no es un ObjectId válido.")
            sys.exit(1)
    else:
        if not isinstance(uid_usuario, ObjectId):
            try:
                uid_usuario = ObjectId(uid_usuario)
            except Exception:
                print("El UID proporcionado no es un ObjectId válido.")
                sys.exit(1)
    if ruta_xlsx is None:
        if len(sys.argv) > 2:
            ruta_xlsx = sys.argv[2]
        else:
            app_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            ruta_xlsx = os.path.abspath(os.path.join(app_root, "data", "modelos_causacion", "SurtifloraModeloCausacionAbril2025.xlsx"))
    procesar_archivo_excel(str(uid_usuario), ruta_xlsx)
    logger.info("Proceso completado exitosamente.")

if __name__ == "__main__":
    main()
