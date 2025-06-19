import logging
import os
import sys
import gc
from bson import ObjectId
from dotenv import load_dotenv
import openpyxl
import pymongo_auth_aws
from pymongo import MongoClient
from pathlib import Path

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Añadir la raíz del proyecto al path de Python
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Cargar variables de entorno
load_dotenv()

# Definir paths
APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
XLSX_PATH = os.path.abspath(os.path.join(APP_ROOT, "data", "modelos_causacion", "SurtifloraModeloCausacionAbril2025.xlsx"))
PUC_USER_PATH = os.path.abspath(os.path.join(APP_ROOT, "data", "pucs", "codigos_puc_surtiflora.xlsx"))

# Obtener UID del usuario desde variables de entorno
UID_FILTER = ObjectId(os.getenv("UID_USER"))

# Obtener variables de entorno para la conexión a MongoDB
aws_key = os.getenv("DEV_AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("DEV_AWS_SECRET_ACCESS_KEY")
cluster = os.getenv("DEV_CLUSTER_URL")
db_name = os.getenv("DEV_DB")
app_name = os.getenv("DEV_APP_NAME")

def get_pucs_user():
    """
    Obtiene los PUCs del usuario desde el archivo Excel de modelo de causación.
    """
    if not os.path.exists(PUC_USER_PATH):
        logging.error(f"El archivo de PUCs del usuario no existe: {PUC_USER_PATH}")
        return {}
    
    wb = openpyxl.load_workbook(PUC_USER_PATH, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    
    # Crear diccionario para almacenar los PUCs
    pucs = {}
    
    for row in rows[1:]:  # Saltar la fila de encabezado
        if not row or not row[0]:
            continue
        
        codigo_puc = str(row[0]).strip()
        descripcion_puc = str(row[1]).strip() if len(row) > 1 else ""
        
        if codigo_puc and descripcion_puc:
            # Agregar al diccionario solo si ambos campos están presentes
            pucs[codigo_puc] = descripcion_puc
            logging.info(f"PUC agregado al diccionario local: {codigo_puc} - {descripcion_puc}")
    
    return pucs

def upload_pucs(pucs: dict):
    """
    Sube los PUCs al usuario especificado en MongoDB utilizando las variables de entorno.
    Si ya existen PUCs para ese usuario, solo añade los nuevos sin duplicar.
    """
    try:
        logging.info(f"Subiendo {len(pucs)} PUCs al usuario {UID_FILTER}...")
        uri = f"mongodb+srv://{aws_key}:{aws_secret}@{cluster}/?authMechanism=MONGODB-AWS&authSource=$external"
        client = MongoClient(uri)
        db = client[db_name]
        collection = db["client_puc"]
        nuevos_pucs = set(pucs.keys())
        # Buscar documento existente
        doc_existente = collection.find_one({"uid": UID_FILTER})
        if doc_existente and "pucs" in doc_existente:
            pucs_existentes = set(doc_existente["pucs"])
            pucs_final = list(pucs_existentes | nuevos_pucs)
            nuevos_agregados = nuevos_pucs - pucs_existentes
            if nuevos_agregados:
                logging.info(f"PUCs nuevos a agregar: {nuevos_agregados}")
            else:
                logging.info("No hay PUCs nuevos para agregar.")
        else:
            pucs_final = list(nuevos_pucs)
            logging.info("No existían PUCs previos, se insertan todos los nuevos.")
        documento = {
            "uid": UID_FILTER,
            "pucs": pucs_final
        }
        collection.update_one(
            {"uid": UID_FILTER},
            {"$set": documento},
            upsert=True
        )
        logging.info(f"PUCs subidos correctamente para el usuario {UID_FILTER}.")
        client.close()
    except Exception as e:
        logging.error(f"Error al subir PUCs: {e}")

if __name__ == "__main__":
    pucs_user = get_pucs_user()
    if pucs_user:
        logging.info(f"PUCs del usuario cargados correctamente: {len(pucs_user)} PUCs encontrados.")
        upload_pucs(pucs_user)
    gc.collect()