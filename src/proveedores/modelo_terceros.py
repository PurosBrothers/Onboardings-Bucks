import os
import logging
import pandas as pd
from bson import ObjectId
import sys

from src.utils.mongodb_manager import MongoDBManager
from src.config.mongodb_config import MongoDBConfig

# =============================
# Configuración de logging
# =============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================
# Configuración de conexión a MongoDB
# =============================
COLLECTION_NAME = "providers"
ruta_archivo = os.path.join('data', 'modelos_terceros', 'Surtiflora-Modelo_de_terceros.csv')

# =============================
# Funciones auxiliares de limpieza
# =============================
def limpiar_nit(nit_raw: str) -> str:
    """
    Limpia el NIT/cédula de caracteres no numéricos y espacios.
    """
    return ''.join(filter(str.isdigit, nit_raw or ''))


def limpiar_campo(valor: str) -> str:
    """
    Limpia el valor de un campo eliminando espacios extra y normalizando.
    """
    if valor is None:
        return ''
    return valor.strip()


# =============================
# Procesamiento del archivo CSV
# =============================
def procesar_csv_terceros(ruta_archivo: str):
    """
    Procesa el archivo CSV de terceros y retorna una lista de diccionarios con los datos limpios.
    """
    try:
        df = pd.read_csv(ruta_archivo, encoding='utf-8', low_memory=False, dtype=str)
    except Exception as e:
        logger.error(f"Error al leer el archivo CSV: {ruta_archivo}: {e}")
        return None, f"Error al leer el archivo CSV: {e}"

    # Identificar columnas relevantes
    columnas = {
        'nit': 'IDENTIFICACIÓN  (OBLIGATORIO)',
        'resp_fiscal': None,
        'act_economica': None,
        'codigo_ciudad': None,
        'razon_social': None,
        'sucursal': None
    }
    for col in df.columns:
        if 'RESPONSABILIDAD FISCAL' in col.upper():
            columnas['resp_fiscal'] = col
        if 'ACTIVIDAD ECONÓMICA' in col.upper() or 'CODIGO ACTIVIDAD' in col.upper():
            columnas['act_economica'] = col
        if 'CIUDAD' in col.upper():
            columnas['codigo_ciudad'] = col
        if 'RAZÓN SOCIAL' in col.upper():
            columnas['razon_social'] = col
        if 'SUCURSAL' in col.upper():
            columnas['sucursal'] = col

    # Validar columnas requeridas
    if not columnas['resp_fiscal'] or not columnas['act_economica'] or not columnas['codigo_ciudad']:
        logger.error(f"No se encontraron todas las columnas necesarias en el archivo {ruta_archivo}")
        return None, "Faltan columnas requeridas en el archivo CSV."

    logger.info(f"Columnas detectadas: {columnas}")
    datos = []
    for idx, row in df.iterrows():
        nit_raw = row.get(columnas['nit'])
        if pd.isna(nit_raw) or not nit_raw:
            logger.warning(f"Fila {idx+2} sin NIT válido, se omite.")
            continue
        nit = limpiar_nit(nit_raw)
        registro = {
            'nit': nit,
            'fiscalResponsability': limpiar_campo(row.get(columnas['resp_fiscal'], '')),
            'activity': limpiar_campo(row.get(columnas['act_economica'], '')),
            'city': limpiar_campo(row.get(columnas['codigo_ciudad'], '')),
            'businessName': limpiar_campo(row.get(columnas['razon_social'], '')),
            'branchOffice': limpiar_campo(row.get(columnas['sucursal'], ''))
        }
        datos.append(registro)
    logger.info(f"Total de registros procesados del CSV: {len(datos)}")
    return datos, None


# =============================
# Actualización de proveedores en MongoDB
# =============================
def actualizar_proveedores(datos, uid):
    """
    Actualiza los proveedores en MongoDB con la información procesada del CSV y el UID proporcionado.
    Busca el proveedor comparando el NIT del Excel con el campo 'id' de la base de datos.
    """
    client = None
    stats = {
        'proveedores_actualizados_fiscal': 0,
        'registros_procesados_fiscal': 0,
        'registros_fallidos_fiscal': 0,
        'errores': []
    }
    mongo_manager = None
    try:
        mongodb_config = MongoDBConfig(env_prefix="DEV")
        mongodb_config.set_collection_name(COLLECTION_NAME)
        mongo_manager = MongoDBManager(mongodb_config)
        collection = mongo_manager.collection
        query = {'UID': uid}
        existing_providers = list(collection.find(query))
        # Mapeo: NIT (del Excel) -> id (de la base de datos)
        provider_map_by_id = {p.get('id'): p['_id'] for p in existing_providers if p.get('id')}
        for idx, registro in enumerate(datos):
            stats['registros_procesados_fiscal'] += 1
            nit = registro['nit']
            if not nit:
                stats['registros_fallidos_fiscal'] += 1
                continue
            mongo_id = provider_map_by_id.get(nit)
            if mongo_id:
                update_data = {'$set': {}}
                for campo in ['fiscalResponsability', 'activity', 'city', 'businessName', 'branchOffice']:
                    if registro[campo]:
                        update_data['$set'][campo] = registro[campo]
                if update_data['$set']:
                    result = collection.update_one({'_id': mongo_id}, update_data)
                    if result.modified_count > 0:
                        logger.info(f"Proveedor NIT {nit} actualizado correctamente.")
                        stats['proveedores_actualizados_fiscal'] += 1
                    else:
                        logger.warning(f"Proveedor NIT {nit} no requirió actualización.")
                else:
                    logger.warning(f"No hay datos para actualizar para NIT {nit}.")
            else:
                logger.warning(f"Proveedor no encontrado para NIT {nit}.")
                stats['registros_fallidos_fiscal'] += 1
    except Exception as e:
        logger.error(f"Error general al actualizar proveedores: {e}", exc_info=True)
        stats['errores'].append(str(e))
    finally:
        if client:
            client.close()
            logger.info("Conexión a MongoDB cerrada.")
    logger.info(f"Finalizado. Proveedores actualizados: {stats['proveedores_actualizados_fiscal']}")
    return stats


# =============================
# Función principal
# =============================
def main(uid=None):
    """
    Orquesta el procesamiento del CSV y la actualización de proveedores en MongoDB.
    Recibe el UID como argumento (ObjectId o str convertible a ObjectId).
    Si no se proporciona, lo toma de la línea de comandos.
    """
    if uid is None:
        if len(sys.argv) < 2:
            print("Uso: python modelo_terceros.py <UID>")
            return
        try:
            uid = ObjectId(sys.argv[1])
        except Exception:
            print("El UID proporcionado no es un ObjectId válido.")
            return
    else:
        if not isinstance(uid, ObjectId):
            try:
                uid = ObjectId(uid)
            except Exception:
                print("El UID proporcionado no es un ObjectId válido.")
                return
    logger.info(f"Iniciando procesamiento de archivo: {ruta_archivo}")
    datos, error = procesar_csv_terceros(ruta_archivo)
    if error:
        logger.error(error)
        print(f"Error: {error}")
        return
    stats = actualizar_proveedores(datos, uid)
    print("--- Resultado de la actualización ---")
    print(f"Proveedores procesados: {stats['registros_procesados_fiscal']}")
    print(f"Proveedores actualizados: {stats['proveedores_actualizados_fiscal']}")
    print(f"Registros fallidos: {stats['registros_fallidos_fiscal']}")
    if stats['errores']:
        print("Errores:")
        for err in stats['errores']:
            print(f" - {err}")


if __name__ == '__main__':
    main()
