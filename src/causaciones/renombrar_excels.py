import logging
import os
import unicodedata

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)   

logger = logging.getLogger(__name__)

EXCEL_PATH = 'data/modelos_causacion'

def get_excel_files(path):
    try:
        files = [f for f in os.listdir(path) if f.endswith('.xlsx')]
        logger.info(f"Archivos Excel encontrados: {files}")
        return files
    except Exception as e:
        logger.error(f"Error al obtener archivos Excel: {e}")
        return []

def limpiar_y_camelcase(nombre):
    base, ext = os.path.splitext(nombre)
    # Verifica si el nombre ya está limpio (sin espacios, tildes ni guiones)
    base_normalizada = unicodedata.normalize('NFKD', base).encode('ascii', 'ignore').decode('ascii')
    if base == base_normalizada and ' ' not in base and '-' not in base:
        return nombre  # No cambia el nombre si ya está limpio
    palabras = [p for p in base_normalizada.replace('-', ' ').split() if p]
    palabras = [p.capitalize() for p in palabras]
    return ''.join(palabras) + ext

def rename_excel_files():
    excel_files = get_excel_files(EXCEL_PATH)
    
    if not excel_files:
        logger.warning("No se encontraron archivos Excel para renombrar.")
        return
    
    for file in excel_files:
        logger.info(f"Renombrando archivo: {file}")
        new_name = limpiar_y_camelcase(file)
        if new_name != file:
            old_path = os.path.join(EXCEL_PATH, file)
            new_path = os.path.join(EXCEL_PATH, new_name)
            try:
                os.rename(old_path, new_path)
                logger.info(f"Archivo renombrado de {file} a {new_name}")
            except Exception as e:
                logger.error(f"Error al renombrar {file} a {new_name}: {e}")

if __name__ == "__main__":
    rename_excel_files()