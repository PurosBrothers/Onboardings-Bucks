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
from src.causaciones.renombrar_zips import obtener_archivos_zip, extraer_zip, procesar_archivos_zip
from src.causaciones.renombrar_excels import renombrar_archivos_excel

# =============================
# Configuración de rutas y logging
# =============================
RUTA_XLSX = os.path.join("data", "modelos_causacion", "SurtifloraModeloCausacionAbril2025.xlsx")
RUTA_ZIPS = os.path.join("data", "facturas")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# =============================
# Funciones auxiliares
# =============================
def limpiar_nit(nit_raw: str) -> str:
    """
    Limpia el NIT/cédula de caracteres no numéricos y espacios.
    """
    return ''.join(filter(str.isdigit, nit_raw or ''))

def extraer_id_factura(descripcion_archivo: str):
    """
    Extrae el identificador de factura de una descripción de archivo.
    """
    descripcion_archivo = descripcion_archivo.strip()
    if not descripcion_archivo:
        return 0
    partes = descripcion_archivo.split()
    for parte in partes:
        if any(char.isdigit() for char in parte):
            return parte

def buscar_zip_similar(lista_zips, id_factura):
    """
    Busca el archivo zip más similar al id_factura.
    """
    if not id_factura or not lista_zips:
        return None
    id_factura = str(id_factura).lower().strip()
    lista_zips = [z for z in lista_zips if z]
    for nombre_zip in lista_zips:
        base_zip = os.path.splitext(nombre_zip)[0].lower().strip()
        if id_factura in base_zip or base_zip in id_factura:
            return nombre_zip
    bases_zip = [os.path.splitext(z)[0].lower().strip() for z in lista_zips]
    zip_similar = difflib.get_close_matches(id_factura, bases_zip, n=1)
    if zip_similar:
        idx = bases_zip.index(zip_similar[0])
        return lista_zips[idx]
    return None

def obtener_descripcion_dian_desde_pdf(ruta_pdf: str) -> Optional[str]:
    """
    Extrae la descripción DIAN de un archivo PDF.
    """
    try:
        with pdfplumber.open(ruta_pdf) as pdf:
            descripciones = []
            for pagina in pdf.pages:
                tablas = pagina.extract_tables()
                for tabla in tablas:
                    fila_encabezado = None
                    indice_col_descripcion = None
                    for fila in tabla:
                        for idx, celda in enumerate(fila):
                            if celda and ("Descripción" in celda or "Descripcion" in celda):
                                fila_encabezado = fila
                                indice_col_descripcion = idx
                                break
                        if fila_encabezado:
                            break
                    if fila_encabezado and indice_col_descripcion is not None:
                        for fila in tabla[tabla.index(fila_encabezado) + 1:]:
                            if fila and len(fila) > indice_col_descripcion:
                                celda = fila[indice_col_descripcion]
                                if celda and isinstance(celda, str) and celda.strip():
                                    descripciones.append(celda.strip())
            if descripciones:
                return descripciones[0] if len(descripciones) == 1 else ' | '.join(descripciones)
            else:
                logging.warning(f"[PDF] Sin descripciones en {os.path.basename(ruta_pdf)}.")
                return None
    except Exception as e:
        logging.error(f"[PDF] Error procesando {os.path.basename(ruta_pdf)}: {e}")
        return None

def extraer_descripcion_dian(id_factura: str):
    """
    Extrae la descripción DIAN de un archivo zip relacionado con la factura.
    """
    archivos_zip = obtener_archivos_zip(RUTA_ZIPS)
    zip_encontrado = buscar_zip_similar(archivos_zip, id_factura)
    if not zip_encontrado:
        logging.warning(f"[DIAN] No se encontró zip para id_factura: {id_factura}")
        return None
    ruta_zip = os.path.join(RUTA_ZIPS, zip_encontrado)
    archivos_extraidos = extraer_zip(ruta_zip, RUTA_ZIPS)
    descripcion_dian = None
    for archivo in archivos_extraidos:
        if archivo.endswith(".pdf"):
            ruta_pdf = os.path.join(RUTA_ZIPS, archivo)
            descripcion_dian = obtener_descripcion_dian_desde_pdf(ruta_pdf)
            break
    # Borrar los archivos extraídos
    for archivo in archivos_extraidos:
        ruta_archivo = os.path.join(RUTA_ZIPS, archivo)
        try:
            if os.path.exists(ruta_archivo):
                os.remove(ruta_archivo)
        except Exception as e:
            logging.warning(f"[LIMPIEZA] No se pudo eliminar {ruta_archivo}: {e}")
    return descripcion_dian

# =============================
# Procesamiento y subida a MongoDB
# =============================
def procesar_y_subir_facturas(uid, ambiente):
    """
    Procesa el archivo Excel y sube las facturas de arrendamiento a MongoDB para el UID dado.
    """
    configuracion = MongoDBConfig(env_prefix=ambiente)
    configuracion.collection_name = "invoices"
    gestor = MongoDBManager(configuracion)
    contador_creadas = 0
    indice_factura = 1
    facturas_procesadas = set()
    libro_trabajo = openpyxl.load_workbook(RUTA_XLSX, data_only=True)
    hoja_trabajo = libro_trabajo.active
    filas = list(hoja_trabajo.iter_rows(values_only=True))
    encabezado_encontrado = False
    for idx, fila in enumerate(filas):
        if fila and str(fila[0]).strip() == "TIPO DE FACTURA":
            logging.info("[INICIO] Encabezado XLSX encontrado, procesando facturas...")
            indice_inicio = idx + 1
            encabezado_encontrado = True
            break
    if not encabezado_encontrado:
        logging.error("[ERROR] No se encontró el encabezado en el archivo XLSX.")
        gestor.close()
        return
    for fila in filas[indice_inicio:]:
        if not fila or not fila[0]:
            continue
        tipo_factura = str(fila[0]).strip()
        if "Servicio" not in tipo_factura and "Arrendamiento" not in tipo_factura:
            logging.info(f"[SKIP] Tipo de factura '{tipo_factura}' no es 'Servicio - Gasto' ni 'Arrendamiento', saltando...")
            continue
        id_proveedor = limpiar_nit(str(fila[16])).strip()
        descripcion_archivo = str(fila[18]).strip()
        id_factura_original = extraer_id_factura(descripcion_archivo)
        if id_factura_original in facturas_procesadas:
            logging.info(f"[SKIP] Factura duplicada en Excel: {id_factura_original}")
            indice_factura += 1
            continue
        facturas_procesadas.add(id_factura_original)
        id_factura = str(fila[86]).strip()
        descripcion_dian = extraer_descripcion_dian(id_factura)
        # Verificar si ya existe un documento con la misma clave única
        if gestor.collection.find_one({
            "UID": uid,
            "invoiceId": id_factura
        }):
            logging.info(f"[SKIP] Factura ya existe en MongoDB: {id_factura}")
            indice_factura += 1
            continue
        invoice_data = {
            "UID": uid,
            "supplierId": id_proveedor,
            "file_description": descripcion_archivo,
            "invoiceId": id_factura,
            "invoice_type": tipo_factura,
            "dian_description": descripcion_dian,
            "module": id_proveedor,
            "entity": id_factura 
        }
        try:
            gestor.collection.insert_one(invoice_data)
            contador_creadas += 1
            logging.info(f"[OK] Factura {indice_factura} creada: proveedor={id_proveedor}, factura={id_factura}")
        except Exception as e:
            logging.error(f"[ERROR] Error al insertar factura {id_factura}: {str(e)}")
        indice_factura += 1
    gestor.close()
    logging.info(f"[RESUMEN] Se crearon {contador_creadas} facturas nuevas en la base de datos.")

# =============================
# Función principal
# =============================
def main(uid=None, ambiente="STAGING"):
    """
    Orquesta el procesamiento de archivos ZIP, renombrado de excels y subida de facturas de arrendamiento.
    Recibe el UID como argumento (ObjectId o str convertible a ObjectId).
    Si no se proporciona, lo toma de la línea de comandos.
    """
    # Aquí puedes usar ambiente para la configuración si es necesario
    # Convertir uid a ObjectId si es necesario
    if uid is None:
        if len(sys.argv) < 3:
            print("Uso: python facturas_arrendamiento_por_proveedor.py <UID> <ambiente>")
            return
        try:
            uid = ObjectId(sys.argv[1])
        except Exception:
            print("El UID proporcionado no es un ObjectId válido.")
            return
        ambiente = sys.argv[2]
    else:
        if not isinstance(uid, ObjectId):
            try:
                uid = ObjectId(uid)
            except Exception:
                print("El UID proporcionado no es un ObjectId válido.")
                return
    logging.info("[SISTEMA] Iniciando procesamiento de archivos ZIP...")
    procesar_archivos_zip()
    logging.info("[SISTEMA] Renombrando archivos Excel...")
    renombrar_archivos_excel()
    logging.info("[SISTEMA] Carga de facturas de proveedor iniciada...")
    #procesar_y_subir_facturas(uid, ambiente)
    logging.info("[SISTEMA] Proceso completado.")

if __name__ == "__main__":
    try:
        main()
        gc.collect()  # Forzar la recolección de basura
        logging.info("[SISTEMA] Todas las tareas se completaron con éxito.")
    except Exception as e:
        logging.error(f"[SISTEMA] Error: {str(e)}")
        raise