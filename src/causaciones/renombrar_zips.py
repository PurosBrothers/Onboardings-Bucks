import os
import logging
import zipfile
from pdfquery import PDFQuery
import shutil
import gc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

RUTA_ZIPS = os.path.join("data", "facturas")
RUTA_EXTRACCION = os.path.join("data", "facturas", "targetdir")


def obtener_archivos_zip(ruta):
    # Obtiene la lista de archivos zip en el directorio especificado.
    return [f for f in os.listdir(ruta) if f.endswith('.zip')]


def extraer_zip(ruta_zip, extraer_a):
    # Extrae el contenido de un archivo zip al directorio especificado.
    with zipfile.ZipFile(ruta_zip, "r") as zip_ref:
        zip_ref.extractall(extraer_a)
        logger.info(f"Extracted {os.path.basename(ruta_zip)} to {extraer_a}.")
        return zip_ref.namelist()


def obtener_numero_factura_pdf(ruta_pdf):
    # Extrae el número de factura de un PDF usando pdfquery.
    try:
        pdf = PDFQuery(ruta_pdf)
        pdf.load()
        logger.info(f"PDF {os.path.basename(ruta_pdf)} loaded successfully.")
        elemento = pdf.pq('LTTextLineHorizontal:contains("Número de Factura")')
        if elemento:
            numero_factura = elemento.text()
            logger.info(f"Invoice number found: {numero_factura}")
            return numero_factura.replace("Número de Factura: ", "").strip()
        else:
            logger.warning(f"No invoice number found in {os.path.basename(ruta_pdf)}.")
            return None
    except Exception as e:
        logger.error(f"Error loading PDF {os.path.basename(ruta_pdf)}: {e}")
        return None
    finally:
        del pdf
        gc.collect()


def renombrar_archivo_zip(ruta_original, nuevo_nombre):
    # Renombra el archivo zip con el nuevo nombre proporcionado.
    nueva_ruta = os.path.join(os.path.dirname(ruta_original), f"{nuevo_nombre}.zip")
    try:
        os.rename(ruta_original, nueva_ruta)
        logger.info(f"Renamed {os.path.basename(ruta_original)} to {os.path.basename(nueva_ruta)}.")
    except Exception as e:
        logger.error(f"Error renaming zip file {os.path.basename(ruta_original)}: {e}")


def procesar_archivos_zip():
    archivos_zip = obtener_archivos_zip(RUTA_ZIPS)

    if not archivos_zip:
        logger.info("No zip files found in the directory.")
        return

    logger.info(f"Found {len(archivos_zip)} zip files in the directory.")

    for archivo_zip in archivos_zip:
        ruta_zip = os.path.join(RUTA_ZIPS, archivo_zip)
        archivos_extraidos = extraer_zip(ruta_zip, RUTA_EXTRACCION)
        nuevo_nombre = None

        for archivo in archivos_extraidos:
            logger.info(f"Extracted file: {archivo}")
            if archivo.endswith('.pdf'):
                ruta_pdf = os.path.join(RUTA_EXTRACCION, archivo)
                nuevo_nombre = obtener_numero_factura_pdf(ruta_pdf)
                if nuevo_nombre:
                    break  # Solo necesitamos el primer número de factura encontrado

        if nuevo_nombre:
            renombrar_archivo_zip(ruta_zip, nuevo_nombre)
        else:
            logger.warning(f"No valid invoice number found for {archivo_zip}. File not renamed.")
            
        # Limpiar archivos extraídos después de renombrar el zip
        limpiar_archivos_extraidos()

def limpiar_archivos_extraidos():
    # Elimina la carpeta de archivos extraídos y todo su contenido.
    try:
        if os.path.exists(RUTA_EXTRACCION):
            shutil.rmtree(RUTA_EXTRACCION)
            logger.info(f"Removed extracted directory: {RUTA_EXTRACCION}")
    except Exception as e:
        logger.error(f"Error removing extracted directory {RUTA_EXTRACCION}: {e}")


if __name__ == "__main__":
    try:
        procesar_archivos_zip()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    else:
        logger.info("All zip files extracted and renamed successfully.")
