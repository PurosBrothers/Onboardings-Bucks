import datetime
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
NOMBRE_COLECCION = "providers"
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
        marco_datos = pd.read_csv(ruta_archivo, encoding='utf-8', low_memory=False, dtype=str)
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
    for col in marco_datos.columns:
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
    for idx, fila in marco_datos.iterrows():
        nit_raw = fila.get(columnas['nit'])
        if pd.isna(nit_raw) or not nit_raw:
            logger.warning(f"Fila {idx+2} sin NIT válido, se omite.")
            continue
        nit = limpiar_nit(nit_raw)
        registro = {
            'nit': nit,
            'fiscalResponsability': limpiar_campo(fila.get(columnas['resp_fiscal'], '')),
            'activity': limpiar_campo(fila.get(columnas['act_economica'], '')),
            'city': limpiar_campo(fila.get(columnas['codigo_ciudad'], '')),
            'businessName': limpiar_campo(fila.get(columnas['razon_social'], '')),
            'branchOffice': limpiar_campo(fila.get(columnas['sucursal'], ''))
        }
        datos.append(registro)
    logger.info(f"Total de registros procesados del CSV: {len(datos)}")
    return datos, None


# =============================
# Actualización de proveedores en MongoDB
# =============================
def actualizar_proveedores(datos, uid, ambiente):
    """
    Actualiza los proveedores en MongoDB con la información procesada del CSV y el UID proporcionado.
    Busca el proveedor comparando el NIT del Excel con el campo 'id' de la base de datos.
    """
    cliente = None
    estadisticas = {
        'proveedores_actualizados_fiscal': 0,
        'registros_procesados_fiscal': 0,
        'registros_fallidos_fiscal': 0,
        'errores': []
    }
    gestor_mongo = None
    try:
        configuracion_mongodb = MongoDBConfig(env_prefix=ambiente)
        configuracion_mongodb.set_collection_name(NOMBRE_COLECCION)
        gestor_mongo = MongoDBManager(configuracion_mongodb)
        coleccion = gestor_mongo.collection
        consulta = {'UID': uid}
        proveedores_existentes = list(coleccion.find(consulta))
        # Mapeo: NIT (del Excel) -> id (de la base de datos)
        mapa_proveedor_por_id = {p.get('id'): p['_id'] for p in proveedores_existentes if p.get('id')}
        for idx, registro in enumerate(datos):
            estadisticas['registros_procesados_fiscal'] += 1
            nit = registro['nit']
            if not nit:
                estadisticas['registros_fallidos_fiscal'] += 1
                continue
            id_mongo = mapa_proveedor_por_id.get(nit)
            if id_mongo:
                datos_actualizacion = {
                    '$set': {
                        'ultima_actualizacion': datetime.datetime.now(datetime.timezone.utc)
                    }
                }
                for campo in ['fiscalResponsability', 'activity', 'city', 'businessName', 'branchOffice']:
                    if registro[campo]:
                        datos_actualizacion['$set'][campo] = registro[campo]
                if datos_actualizacion['$set']:
                    logger.debug(f"Intentando actualizar NIT {nit} con datos: {datos_actualizacion}")
                    doc_antes_actualizacion = coleccion.find_one({'_id': id_mongo})
                    logger.debug(f"Documento antes de la actualización NIT {nit}: {doc_antes_actualizacion}")
                    try:
                        resultado = coleccion.update_one({'_id': id_mongo}, datos_actualizacion)
                        if resultado.modified_count > 0:
                            logger.info(f"Proveedor NIT {nit} actualizado correctamente.")
                            estadisticas['proveedores_actualizados_fiscal'] += 1
                        else:
                            logger.warning(f"Proveedor NIT {nit} no requirió actualización.")
                            doc_despues_actualizacion = coleccion.find_one({'_id': id_mongo})
                            logger.debug(f"Documento después de la actualización NIT {nit}: {doc_despues_actualizacion}")
                    except Exception as error_actualizacion:
                        logger.error(f"Error al actualizar NIT {nit}: {str(error_actualizacion)}")
                        estadisticas['proveedores_actualizados_fiscal'] -= 1
                else:
                    logger.warning(f"No hay datos para actualizar para NIT {nit}.")
            else:
                logger.warning(f"Proveedor no encontrado para NIT {nit}.")
                estadisticas['registros_fallidos_fiscal'] += 1
    except Exception as e:
        logger.error(f"Error general al actualizar proveedores: {e}", exc_info=True)
        estadisticas['errores'].append(str(e))
    finally:
        if cliente:
            cliente.close()
            logger.info("Conexión a MongoDB cerrada.")
    logger.info(f"Finalizado. Proveedores actualizados: {estadisticas['proveedores_actualizados_fiscal']}")
    return estadisticas


# =============================
# Función principal
# =============================
def main(uid=None, ambiente=None):
    """
    Orquesta el procesamiento del CSV y la actualización de proveedores en MongoDB.
    Recibe el UID como argumento (ObjectId o str convertible a ObjectId).
    Si no se proporciona, lo toma de la línea de comandos.
    """
    # Convertir uid a ObjectId si es necesario
    if uid is None:
        if len(sys.argv) < 3:
            print("Uso: python modelo_terceros.py <UID> <ambiente>")
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
    logger.info(f"Iniciando procesamiento de archivo: {ruta_archivo}")
    datos, error = procesar_csv_terceros(ruta_archivo)
    if error:
        logger.error(error)
        print(f"Error: {error}")
        return
    estadisticas = actualizar_proveedores(datos, uid, ambiente)
    print("--- Resultado de la actualización ---")
    print(f"Proveedores procesados: {estadisticas['registros_procesados_fiscal']}")
    print(f"Proveedores actualizados: {estadisticas['proveedores_actualizados_fiscal']}")
    print(f"Registros fallidos: {estadisticas['registros_fallidos_fiscal']}")
    if estadisticas['errores']:
        print("Errores:")
        for err in estadisticas['errores']:
            print(f" - {err}")


if __name__ == '__main__':
    main()
