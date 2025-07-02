import os
from dotenv import load_dotenv
import csv
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timezone
import sys

from src.utils.mongodb_manager import MongoDBManager
from src.config.mongodb_config import MongoDBConfig

"""
Script para limpiar y cargar productos en la base de datos MongoDB a partir de un archivo CSV.

Este script realiza las siguientes acciones:
1. Carga las variables de entorno necesarias para conectarse a la base de datos MongoDB (por predeterminado en entorno de desarrollo, está comentada la configuración para conectarse STAGING si se requiere).
2. Elimina todos los productos existentes asociados al usuario especificado por UID_USER en la colección "products".
3. Lee un archivo CSV de productos, omitiendo las primeras 5 líneas, y procesa cada fila para extraer información relevante como nombre, descripción, precio, línea, grupo, unidad de medida y NIT de proveedores.
4. Convierte los precios a formato numérico, limpia los NITs y genera un código único para cada producto.
5. Inserta cada producto como un documento en la colección "products" de MongoDB, evitando duplicados por código.

"""

# =============================
# CONFIGURACIÓN Y CONSTANTES
# =============================
load_dotenv()

RUTA_CSV = os.path.join("data", "productos", "SurtifloraListaProductos.csv")

# =============================
# FUNCIONES AUXILIARES
# =============================
def convertir_precio(cadena_valor):
    """
    Convierte una cadena con separadores de miles (coma) y punto decimal en un float.
    Ejemplo: "25,000.00000" -> 25000.0
    """
    limpiado = cadena_valor.replace(',', '')
    try:
        return float(limpiado)
    except ValueError:
        return 0.0

def limpiar_nit(nit_raw: str) -> str:
    """
    Limpia el NIT/cédula de caracteres no numéricos y espacios.
    """
    return ''.join(filter(str.isdigit, nit_raw or ''))

# =============================
# LECTURA Y PROCESAMIENTO DEL CSV
# =============================
def leer_productos_desde_csv(ruta_csv=RUTA_CSV, uid=None):
    """
    Lee los productos desde el archivo CSV y los retorna como una lista de diccionarios.
    Args:
        ruta_csv (str): Ruta al archivo CSV
        uid (ObjectId): UID del usuario para asociar los productos (obligatorio)
    Returns:
        List[dict]: Lista de productos listos para insertar en MongoDB
    """
    if uid is None:
        raise ValueError("El parámetro 'uid' es obligatorio y debe ser un ObjectId válido.")
    productos = []
    indice_producto = 1
    with open(ruta_csv, newline="", encoding="utf-8") as f:
        lector = csv.reader(f)
        # Saltar las primeras 5 líneas (encabezados)
        for _ in range(5):
            next(lector, None)
        for fila in lector:
            nombre = (fila[3] or "").strip()
            descripcion = (fila[4] or "").strip()
            precio1 = convertir_precio(fila[6] if len(fila) > 6 else "0")
            linea = (fila[0] or "").strip()
            grupo = (fila[1] or "").strip()
            unidadMedida = (fila[79] or "").strip() if len(fila) > 79 else ""
            nit_proveedor = [limpiar_nit(fila[i]) for i in range(50, 54) if len(fila) > i and fila[i].strip()]
            codigo = f"Surtiflora{indice_producto:09d}"
            # Construir la lista de precios dinámicamente
            lista_precios = [{
                "position": 1,
                "name": "Lista general",
                "value": precio1
            }]
            doc = {
                "UID": uid,
                "code": codigo,
                "active": True,
                "available_quantity": 0,
                "inventory_type": 1,
                "description": descripcion,
                "name": nombre or f"Producto {indice_producto}",
                "prices": [{
                    "currency_code": "COP",
                    "price_list": lista_precios
                }],
                "reference": codigo,
                "stock_control": True,
                "tax_classification": "Taxed",
                "tax_consumption_value": 0,
                "tax_included": True,
                "type": "Product",
                "unit": {"code": "94", "name": "Unidad"},
                "unit_label": "Unidad",
                "warehouses": [{"id": 1, "name": "Principal", "quantity": 0}],
                "createdAt": datetime.now(timezone.utc),
                "updatedAt": datetime.now(timezone.utc),
                "line" : linea,
                "group": grupo,
                "unitMeasure": unidadMedida,
                "nit_providers": nit_proveedor,
            }
            productos.append(doc)
            indice_producto += 1
    return productos

# =============================
# OPERACIONES EN MONGODB
# =============================
def eliminar_productos_existentes(gestor_mongo: MongoDBManager, uid: ObjectId):
    """
    Elimina todos los productos con el UID especificado de la colección products.
    """
    coleccion = gestor_mongo.db["products"]
    resultado = coleccion.delete_many({"UID": uid})
    print(f"🗑️  Deleted {resultado.deleted_count} existing products for UID: {uid}".encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))

def subir_productos_a_mongodb(gestor_mongo: MongoDBManager, uid):
    """
    Lee los productos del CSV y los sube a MongoDB, evitando duplicados por code y UID.
    """
    productos = leer_productos_desde_csv(uid=uid)
    coleccion = gestor_mongo.db["products"]
    contador_creados = 0
    for doc in productos:
        if coleccion.find_one({"UID": doc["UID"], "code": doc["code"]}):
            continue
        coleccion.insert_one(doc)
        print(f"✅ Created: {doc['name']} ({doc['code']}) with price {doc['prices'][0]['price_list'][0]['value']}".encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))
        contador_creados += 1
    print(f"🎉 Done. Created {contador_creados} new products.".encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))

def cargar_productos_desde_csv_a_mongodb(uid, ambiente):
    """
    Método general para eliminar productos existentes y cargar los nuevos desde el CSV a MongoDB.
    """
    configuracion_mongodb = MongoDBConfig(env_prefix=ambiente)
    gestor_mongo = MongoDBManager(configuracion_mongodb)
    try:
        # Convertir uid a ObjectId si es necesario
        if not isinstance(uid, ObjectId):
            try:
                uid = ObjectId(uid)
            except Exception:
                raise ValueError("El UID proporcionado no es válido. Debe ser un ObjectId de MongoDB.")
        eliminar_productos_existentes(gestor_mongo, uid)
        subir_productos_a_mongodb(gestor_mongo, uid)
    finally:
        gestor_mongo.close()

# =============================
# MAIN
# =============================
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Uso: python subir_productos_mongodb.py <UID_USER> <ambiente>")
        sys.exit(1)
    try:
        uid = ObjectId(sys.argv[1])
    except Exception:
        print("El UID proporcionado no es válido. Debe ser un ObjectId de MongoDB.")
        sys.exit(1)
    ambiente = sys.argv[2]
    cargar_productos_desde_csv_a_mongodb(uid, ambiente)
