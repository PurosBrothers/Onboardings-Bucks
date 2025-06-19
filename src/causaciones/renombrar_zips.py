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

ZIP_PATH = os.path.join("data", "facturas")
EXTRACTED_PATH = os.path.join("data", "facturas", "targetdir")


def get_zip_files(path):
    # Obtiene la lista de archivos zip en el directorio especificado.
    return [f for f in os.listdir(path) if f.endswith('.zip')]


def extract_zip(zip_path, extract_to):
    # Extrae el contenido de un archivo zip al directorio especificado.
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
        logger.info(f"Extracted {os.path.basename(zip_path)} to {extract_to}.")
        return zip_ref.namelist()


def get_invoice_number_from_pdf(pdf_path):
    # Extrae el número de factura de un PDF usando pdfquery.
    try:
        pdf = PDFQuery(pdf_path)
        pdf.load()
        logger.info(f"PDF {os.path.basename(pdf_path)} loaded successfully.")
        element = pdf.pq('LTTextLineHorizontal:contains("Número de Factura")')
        if element:
            invoice_number = element.text()
            logger.info(f"Invoice number found: {invoice_number}")
            return invoice_number.replace("Número de Factura: ", "").strip()
        else:
            logger.warning(f"No invoice number found in {os.path.basename(pdf_path)}.")
            return None
    except Exception as e:
        logger.error(f"Error loading PDF {os.path.basename(pdf_path)}: {e}")
        return None
    finally:
        del pdf
        gc.collect()


def rename_zip_file(original_path, new_name):
    # Renombra el archivo zip con el nuevo nombre proporcionado.
    new_path = os.path.join(os.path.dirname(original_path), f"{new_name}.zip")
    try:
        os.rename(original_path, new_path)
        logger.info(f"Renamed {os.path.basename(original_path)} to {os.path.basename(new_path)}.")
    except Exception as e:
        logger.error(f"Error renaming zip file {os.path.basename(original_path)}: {e}")


def process_zip_files():
    zip_files = get_zip_files(ZIP_PATH)

    if not zip_files:
        logger.info("No zip files found in the directory.")
        return

    logger.info(f"Found {len(zip_files)} zip files in the directory.")

    for zip_file in zip_files:
        zip_path = os.path.join(ZIP_PATH, zip_file)
        extracted_files = extract_zip(zip_path, EXTRACTED_PATH)
        new_name = None

        for file in extracted_files:
            logger.info(f"Extracted file: {file}")
            if file.endswith('.pdf'):
                pdf_path = os.path.join(EXTRACTED_PATH, file)
                new_name = get_invoice_number_from_pdf(pdf_path)
                if new_name:
                    break  # Solo necesitamos el primer número de factura encontrado

        if new_name:
            rename_zip_file(zip_path, new_name)
        else:
            logger.warning(f"No valid invoice number found for {zip_file}. File not renamed.")
            
        # Limpiar archivos extraídos después de renombrar el zip
        limpiar_archivos_extraidos()

def limpiar_archivos_extraidos():
    # Elimina la carpeta de archivos extraídos y todo su contenido.
    try:
        if os.path.exists(EXTRACTED_PATH):
            shutil.rmtree(EXTRACTED_PATH)
            logger.info(f"Removed extracted directory: {EXTRACTED_PATH}")
    except Exception as e:
        logger.error(f"Error removing extracted directory {EXTRACTED_PATH}: {e}")


if __name__ == "__main__":
    try:
        process_zip_files()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    else:
        logger.info("All zip files extracted and renamed successfully.")
