import logging
import os
import unicodedata

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)   

logger = logging.getLogger(__name__)

RUTA_EXCELS = 'data/modelos_causacion'

def obtener_archivos_excel(ruta):
    try:
        archivos = [f for f in os.listdir(ruta) if f.endswith('.xlsx')]
        logger.info(f"Archivos Excel encontrados: {archivos}")
        return archivos
    except Exception as e:
        logger.error(f"Error al obtener archivos Excel: {e}")
        return []

def limpiar_y_camelcase(nombre):
    base, extension = os.path.splitext(nombre)
    # Verifica si el nombre ya está limpio (sin espacios, tildes ni guiones)
    base_normalizada = unicodedata.normalize('NFKD', base).encode('ascii', 'ignore').decode('ascii')
    if base == base_normalizada and ' ' not in base and '-' not in base:
        return nombre  # No cambia el nombre si ya está limpio
    palabras = [p for p in base_normalizada.replace('-', ' ').split() if p]
    palabras = [p.capitalize() for p in palabras]
    return ''.join(palabras) + extension

def renombrar_archivos_excel():
    archivos_excel = obtener_archivos_excel(RUTA_EXCELS)
    
    if not archivos_excel:
        logger.warning("No se encontraron archivos Excel para renombrar.")
        return
    
    for archivo in archivos_excel:
        logger.info(f"Renombrando archivo: {archivo}")
        nuevo_nombre = limpiar_y_camelcase(archivo)
        if nuevo_nombre != archivo:
            ruta_antigua = os.path.join(RUTA_EXCELS, archivo)
            ruta_nueva = os.path.join(RUTA_EXCELS, nuevo_nombre)
            try:
                os.rename(ruta_antigua, ruta_nueva)
                logger.info(f"Archivo renombrado de {archivo} a {nuevo_nombre}")
            except Exception as e:
                logger.error(f"Error al renombrar {archivo} a {nuevo_nombre}: {e}")

if __name__ == "__main__":
    renombrar_archivos_excel()