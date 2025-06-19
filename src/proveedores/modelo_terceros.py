import os
import logging
import pandas as pd
from pymongo import MongoClient
from bson import ObjectId

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración de conexión (entorno de desarrollo)
target_config = {
    "aws_access_key_id": os.getenv("DEV_AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("DEV_AWS_SECRET_ACCESS_KEY"),
    "cluster_url": os.getenv("DEV_CLUSTER_URL"),
    "db_name": os.getenv("DEV_DB"),
    "app_name": os.getenv("DEV_APP_NAME")
}

# Identificador de usuario específico (campo en la colección: 'UID')
UID_FILTER = ObjectId(os.getenv("UID_USER"))

# URI de conexión a MongoDB Atlas con autenticación AWS
TARGET_URI = (
    f"mongodb+srv://{target_config['aws_access_key_id']}:{target_config['aws_secret_access_key']}@"
    f"{target_config['cluster_url']}?authSource=%24external&authMechanism=MONGODB-AWS"
    f"&retryWrites=true&w=majority&appName={target_config['app_name']}"
)

# Colección de trabajo
COLLECTION_NAME = "providers"


# Funciones auxiliares
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


def actualizar_responsabilidad_fiscal_actividad():
    """
    Lee el archivo Surtiflora-Modelo_de_terceros.csv y actualiza los proveedores
    con la información de responsabilidad fiscal y actividad económica.
    """
    client = None
    stats = {
        'proveedores_actualizados_fiscal': 0,
        'registros_procesados_fiscal': 0,
        'registros_fallidos_fiscal': 0,
        'errores': []
    }

    try:
        client = MongoClient(TARGET_URI)
        db = client[target_config['db_name']]
        collection = db[COLLECTION_NAME]

        # Ruta al archivo de modelo de terceros
        ruta_archivo = os.path.join('data', 'modelos', 'Surtiflora-Modelo_de_terceros.csv')
        logger.info(f"Procesando archivo de responsabilidad fiscal: {ruta_archivo}")

        try:
            # Leer el CSV con todas las columnas como tipo string
            df = pd.read_csv(ruta_archivo, encoding='utf-8', low_memory=False, dtype=str)

            # Detectar columnas necesarias
            columnas_resp_fiscal = [col for col in df.columns if 'RESPONSABILIDAD FISCAL' in col.upper()]
            columnas_act_economica = [col for col in df.columns if 'ACTIVIDAD ECONÓMICA' in col.upper() or 'CODIGO ACTIVIDAD' in col.upper()]
            columnas_codigo_ciudad = [col for col in df.columns if 'CIUDAD' in col.upper()]
            columnas_razon_social = [col for col in df.columns if 'RAZÓN SOCIAL' in col.upper()]
            columnas_sucursal = [col for col in df.columns if 'SUCURSAL  (OBLIGATORIO)' in col.upper()]
            #RAZÓN SOCIAL
            #SUCURSAL  (OBLIGATORIO)


            if not columnas_resp_fiscal or not columnas_act_economica or not columnas_codigo_ciudad:
                mensaje_error = f"No se encontraron las columnas necesarias en el archivo {ruta_archivo}"
                logger.error(mensaje_error)
                stats['errores'].append(mensaje_error)
                logger.error(f"Columnas disponibles: {', '.join(df.columns)}")
                return stats

            columna_resp_fiscal = columnas_resp_fiscal[0]
            columna_act_economica = columnas_act_economica[0]
            columna_codigo_ciudad = columnas_codigo_ciudad[0]
            columna_razon_social = columnas_razon_social[0]
            columna_sucursal = columnas_sucursal[0]
            #RAZÓN SOCIAL
            #SUCURSAL  (OBLIGATORIO)


            logger.info(f"Usando columna '{columna_resp_fiscal}' para responsabilidad fiscal")
            logger.info(f"Usando columna '{columna_act_economica}' para actividad económica")
            logger.info(f"Usando columna '{columna_codigo_ciudad}' para código de ciudad")

            # Construir query para filtrar por usuario si aplica
            query = {'UID': UID_FILTER} 
            existing_providers = list(collection.find(query))
            logger.info(f"Se encontraron {len(existing_providers)} proveedores en la base de datos con filtro {query}")

            # Mapas de búsqueda
            provider_map_by_id = {p.get('id'): p['_id'] for p in existing_providers if p.get('id')}
            provider_map_by_nit = {p.get('nit'): p['_id'] for p in existing_providers if p.get('nit')}

            # Procesar cada fila del CSV
            for idx, row in df.iterrows():
                stats['registros_procesados_fiscal'] += 1
                try:
                    nit_raw = row.get('IDENTIFICACIÓN  (OBLIGATORIO)')
                    if pd.isna(nit_raw) or not nit_raw:
                        logger.warning(f"Fila {idx+2} sin NIT válido")
                        stats['registros_fallidos_fiscal'] += 1
                        continue

                    nit = limpiar_nit(nit_raw)
                    responsabilidad_fiscal = limpiar_campo(row.get(columna_resp_fiscal, ''))
                    actividad_economica = limpiar_campo(row.get(columna_act_economica, ''))
                    codigo_ciudad = limpiar_campo(row.get(columna_codigo_ciudad, ''))
                    razon_social = limpiar_campo(row.get(columna_razon_social, ''))
                    sucursal = limpiar_campo(row.get(columna_sucursal, ''))
                    #RAZÓN SOCIAL
                    #SUCURSAL  (OBLIGATORIO)

                    if not responsabilidad_fiscal and not actividad_economica or not codigo_ciudad:
                        logger.warning(f"Fila {idx+2} sin datos fiscales o actividad económica para NIT {nit}")
                        stats['registros_fallidos_fiscal'] += 1
                        continue

                    # Buscar ID en los mapas
                    mongo_id = provider_map_by_id.get(nit) or provider_map_by_nit.get(nit)
                    if not mongo_id:
                        filtro_id = {'$or': [{'id': nit}, {'nit': nit}]}
                        # combinar con filtro de usuario si corresponde
                        filtro_final = {'$and': [query, filtro_id]} if query else filtro_id
                        proveedor = collection.find_one(filtro_final)
                        if proveedor:
                            mongo_id = proveedor['_id']

                    if mongo_id:
                        update_data = {'$set': {}}
                        if responsabilidad_fiscal:
                            update_data['$set']['fiscalResponsability'] = responsabilidad_fiscal
                        if actividad_economica:
                            update_data['$set']['activity'] = actividad_economica
                        if codigo_ciudad:
                            update_data['$set']['city'] = codigo_ciudad
                        if razon_social:
                            update_data['$set']['businessName'] = razon_social
                        if sucursal:
                            update_data['$set']['branchOffice'] = sucursal                 
                            #RAZÓN SOCIAL
                            #SUCURSAL  (OBLIGATORIO)
                            
                        if update_data['$set']:
                            result = collection.update_one({'_id': mongo_id}, update_data)
                            if result.modified_count > 0:
                                logger.info(f"Actualizado proveedor NIT {nit}")
                                stats['proveedores_actualizados_fiscal'] += 1
                            else:
                                logger.warning(f"No modificado documento para NIT {nit}")
                        else:
                            logger.warning(f"No hay datos para actualizar para NIT {nit}")
                    else:
                        logger.warning(f"Proveedor no encontrado para NIT {nit}")
                        stats['registros_fallidos_fiscal'] += 1

                except Exception as row_error:
                    mensaje_error = f"Error fila {idx+2}: {str(row_error)}"
                    logger.error(mensaje_error, exc_info=True)
                    stats['registros_fallidos_fiscal'] += 1
                    stats['errores'].append(mensaje_error)
                    continue

        except pd.errors.EmptyDataError:
            mensaje_error = f"Archivo {ruta_archivo} está vacío o inválido."
            logger.warning(mensaje_error)
            stats['errores'].append(mensaje_error)
        except Exception as file_error:
            mensaje_error = f"Error al procesar archivo {ruta_archivo}: {str(file_error)}"
            logger.error(mensaje_error, exc_info=True)
            stats['errores'].append(mensaje_error)

    finally:
        if client:
            client.close()
            logger.info("Conexión a MongoDB cerrada")

    logger.info(f"Finalizado. Proveedores actualizados: {stats['proveedores_actualizados_fiscal']}")
    return stats


def main():
    stats = actualizar_responsabilidad_fiscal_actividad()
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
