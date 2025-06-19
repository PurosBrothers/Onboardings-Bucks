import difflib
import os
import sys
import logging
import gc
import openpyxl
import pdfplumber
import pymongo_auth_aws
from typing import Optional
from bson import ObjectId
from pathlib import Path

# Add the project root directory to Python path
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dotenv import load_dotenv
from src.config.mongodb_config import MongoDBConfig
from src.utils.mongodb_manager import MongoDBManager
from src.causaciones.renombrar_zips import get_zip_files, extract_zip, process_zip_files
from src.causaciones.renombrar_excels import rename_excel_files

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Carga variables de entorno desde .env
load_dotenv()

aws_key = os.getenv("DEV_AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("DEV_AWS_SECRET_ACCESS_KEY")
cluster = os.getenv("DEV_CLUSTER_URL")
db_name = os.getenv("DEV_DB")
app_name = os.getenv("DEV_APP_NAME")

uri = (
    f"mongodb+srv://{aws_key}:{aws_secret}"
    f"@{cluster}"
    "?authSource=%24external"
    "&authMechanism=MONGODB-AWS"
    "&retryWrites=true&w=majority"
    f"&appName={app_name}"
)

UID_FILTER = ObjectId(os.getenv("UID_USER"))

XLSX_PATH = os.path.join("data", "modelos_causacion", "SurtifloraModeloCausacionAbril2025.xlsx")
ZIP_PATH = os.path.join("data", "facturas")

def limpiar_nit(nit_raw: str) -> str:
    """
    Limpia el NIT/cédula de caracteres no numéricos y espacios.
    """
    return ''.join(filter(str.isdigit, nit_raw or ''))

def extraer_id_factura(descripcion_archivo: str):
    descripcion_archivo = descripcion_archivo.strip()
    if not descripcion_archivo:
        return 0
    partes = descripcion_archivo.split()
    for parte in partes:
        if any(char.isdigit() for char in parte):
            return parte

def extraer_descripcion_dian(id_factura: str):
    zip_files = get_zip_files(ZIP_PATH)
    zip_encontrado = buscar_zip_similar(zip_files, id_factura)
    if not zip_encontrado:
        logging.warning(f"[DIAN] No se encontró zip para id_factura: {id_factura}")
        return None
    zip_path = os.path.join(ZIP_PATH, zip_encontrado)
    extracted_files = extract_zip(zip_path, ZIP_PATH)
    descripcion_dian = None
    for file in extracted_files:
        if file.endswith(".pdf"):
            pdf_path = os.path.join(ZIP_PATH, file)
            descripcion_dian = get_dian_description_from_pdf(pdf_path)
            break
    
    # Borrar los archivos extraídos
    for file in extracted_files:
        file_path = os.path.join(ZIP_PATH, file)
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            logging.warning(f"[LIMPIEZA] No se pudo eliminar {file_path}: {e}")

    return descripcion_dian

def get_dian_description_from_pdf(pdf_path: str) -> Optional[str]:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            descriptions = []
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    header_row = None
                    description_col_idx = None
                    for row in table:
                        for idx, cell in enumerate(row):
                            if cell and ("Descripción" in cell or "Descripcion" in cell):
                                header_row = row
                                description_col_idx = idx
                                break
                        if header_row:
                            break
                    if header_row and description_col_idx is not None:
                        for row in table[table.index(header_row) + 1:]:
                            if row and len(row) > description_col_idx:
                                cell = row[description_col_idx]
                                if cell and isinstance(cell, str) and cell.strip():
                                    descriptions.append(cell.strip())
            if descriptions:
                return descriptions[0] if len(descriptions) == 1 else ' | '.join(descriptions)
            else:
                logging.warning(f"[PDF] Sin descripciones en {os.path.basename(pdf_path)}.")
                return None
    except Exception as e:
        logging.error(f"[PDF] Error procesando {os.path.basename(pdf_path)}: {e}")
        return None

def buscar_zip_similar(lista_zips, id_factura):
    if not id_factura or not lista_zips:
        return None

    id_factura = str(id_factura).lower().strip()
    lista_zips = [z for z in lista_zips if z]

    for zip_name in lista_zips:
        zip_base = os.path.splitext(zip_name)[0].lower().strip()
        if id_factura in zip_base or zip_base in id_factura:
            return zip_name

    zip_bases = [os.path.splitext(z)[0].lower().strip() for z in lista_zips]
    zip_similar = difflib.get_close_matches(id_factura, zip_bases, n=1)
    if zip_similar:
        idx = zip_bases.index(zip_similar[0])
        return lista_zips[idx]

    return None

def upload_to_mongodb():
    config = MongoDBConfig(env_prefix="DEV")
    config.collection_name = "invoices"
    manager = MongoDBManager(config)
    created_count = 0
    invoice_index = 1
    facturas_procesadas = set()

    wb = openpyxl.load_workbook(XLSX_PATH, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    header_found = False
    for idx, row in enumerate(rows):
        if row and str(row[0]).strip() == "TIPO DE FACTURA":
            logging.info("[INICIO] Encabezado XLSX encontrado, procesando facturas...")
            start_idx = idx + 1
            header_found = True
            break

    if not header_found:
        logging.error("[ERROR] No se encontró el encabezado en el archivo XLSX.")
        manager.close()
        return

    for row in rows[start_idx:]:
        if not row or not row[0]:
            continue
        tipo_factura = str(row[0]).strip()
        
        if tipo_factura != "Arrendamiento":
            logging.info(f"[SKIP] Tipo de factura '{tipo_factura}' no es 'Arrendamiento', saltando...")
            continue
        
        id_proveedor = limpiar_nit(str(row[16])).strip()
        descripcion_archivo = str(row[18]).strip()
        id_factura_original = extraer_id_factura(descripcion_archivo)
        
        if id_factura_original in facturas_procesadas:
            logging.info(f"[SKIP] Factura duplicada en Excel: {id_factura_original}")
            invoice_index += 1
            continue
        facturas_procesadas.add(id_factura_original)
        
        zip_files = get_zip_files(ZIP_PATH)
        zip_encontrado = buscar_zip_similar(zip_files, id_factura_original)
        if zip_encontrado:
            id_factura = os.path.splitext(zip_encontrado)[0]
            descripcion_dian = extraer_descripcion_dian(id_factura)
        else:
            id_factura = id_factura_original
            descripcion_dian = extraer_descripcion_dian(id_factura)
        
        # Verificar si ya existe un documento con la misma clave única
        if manager.collection.find_one({
            "UID": UID_FILTER,
            "invoice_id": id_factura
        }):
            logging.info(f"[SKIP] Factura ya existe en MongoDB: {id_factura}")
            invoice_index += 1
            continue

        invoice_data = {
            "UID": UID_FILTER,
            "provider_id": id_proveedor,
            "file_description": descripcion_archivo,
            "invoice_id": id_factura,
            "invoice_type": tipo_factura,
            "dian_description": descripcion_dian,
        }

        try:
            manager.collection.insert_one(invoice_data)
            created_count += 1
            logging.info(f"[OK] Factura {invoice_index} creada: proveedor={id_proveedor}, factura={id_factura}")
        except Exception as e:
            logging.error(f"[ERROR] Error al insertar factura {id_factura}: {str(e)}")
        invoice_index += 1

    manager.close()
    logging.info(f"[RESUMEN] Se crearon {created_count} facturas nuevas en la base de datos.")
    
def main():
    logging.info("[SISTEMA] Iniciando procesamiento de archivos ZIP...")
    process_zip_files()
    logging.info("[SISTEMA] Renombrando archivos Excel...")
    rename_excel_files()
    logging.info("[SISTEMA] Carga de facturas de proveedor iniciada...")
    upload_to_mongodb()
    logging.info("[SISTEMA] Proceso completado.")

if __name__ == "__main__":
    try:
        main()
        gc.collect()  # Forzar la recolección de basura
        logging.info("[SISTEMA] Todas las tareas se completaron con éxito. ✨")
    except Exception as e:
        logging.error(f"[ERROR] Ocurrió un error: {str(e)}")
        raise