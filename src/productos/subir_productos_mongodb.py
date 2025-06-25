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

AMBIENTE = "STAGING"

# =============================
# CONFIGURACIÓN Y CONSTANTES
# =============================
load_dotenv()

CSV_PATH = os.path.join("data", "productos", "SurtifloraListaProductos.csv")

# =============================
# FUNCIONES AUXILIARES
# =============================
def parse_price(value_str):
    """
    Convierte una cadena con separadores de miles (coma) y punto decimal en un float.
    Ejemplo: "25,000.00000" -> 25000.0
    """
    cleaned = value_str.replace(',', '')
    try:
        return float(cleaned)
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
def leer_productos_desde_csv(csv_path=CSV_PATH, uid=None):
    """
    Lee los productos desde el archivo CSV y los retorna como una lista de diccionarios.
    Args:
        csv_path (str): Ruta al archivo CSV
        uid (ObjectId): UID del usuario para asociar los productos (obligatorio)
    Returns:
        List[dict]: Lista de productos listos para insertar en MongoDB
    """
    if uid is None:
        raise ValueError("El parámetro 'uid' es obligatorio y debe ser un ObjectId válido.")
    productos = []
    product_index = 1
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        # Saltar las primeras 5 líneas (encabezados)
        for _ in range(5):
            next(reader, None)
        for row in reader:
            name = (row[3] or "").strip()
            description = (row[4] or "").strip()
            price1 = parse_price(row[6] if len(row) > 6 else "0")
            linea = (row[0] or "").strip()
            grupo = (row[1] or "").strip()
            unidadMedida = (row[79] or "").strip() if len(row) > 79 else ""
            nit_proveedor = [limpiar_nit(row[i]) for i in range(50, 54) if len(row) > i and row[i].strip()]
            code = f"Surtiflora{product_index:09d}"
            # Construir la lista de precios dinámicamente
            price_list = [{
                "position": 1,
                "name": "Lista general",
                "value": price1
            }]
            doc = {
                "UID": uid,
                "code": code,
                "active": True,
                "available_quantity": 0,
                "inventory_type": 1,
                "description": description,
                "name": name or f"Producto {product_index}",
                "prices": [{
                    "currency_code": "COP",
                    "price_list": price_list
                }],
                "reference": code,
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
            product_index += 1
    return productos

# =============================
# OPERACIONES EN MONGODB
# =============================
def delete_existing_products(mongo_manager: MongoDBManager, uid: ObjectId):
    """
    Elimina todos los productos con el UID especificado de la colección products.
    """
    collection = mongo_manager.db["products"]
    result = collection.delete_many({"UID": uid})
    print(f"🗑️  Deleted {result.deleted_count} existing products for UID: {uid}".encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))

def subir_productos_a_mongodb(mongo_manager: MongoDBManager, uid):
    """
    Lee los productos del CSV y los sube a MongoDB, evitando duplicados por code y UID.
    """
    productos = leer_productos_desde_csv(uid=uid)
    collection = mongo_manager.db["products"]
    created_count = 0
    for doc in productos:
        if collection.find_one({"UID": doc["UID"], "code": doc["code"]}):
            continue
        collection.insert_one(doc)
        print(f"✅ Created: {doc['name']} ({doc['code']}) with price {doc['prices'][0]['price_list'][0]['value']}".encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))
        created_count += 1
    print(f"🎉 Done. Created {created_count} new products.".encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))

def cargar_productos_desde_csv_a_mongodb(uid):
    """
    Método general para eliminar productos existentes y cargar los nuevos desde el CSV a MongoDB.
    """
    mongodb_config = MongoDBConfig(env_prefix=AMBIENTE)
    mongo_manager = MongoDBManager(mongodb_config)
    try:
        delete_existing_products(mongo_manager, uid)
        subir_productos_a_mongodb(mongo_manager, uid)
    finally:
        mongo_manager.close()

# =============================
# MAIN
# =============================
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: python subir_productos_mongodb.py <UID_USER>")
        sys.exit(1)
    try:
        uid = ObjectId(sys.argv[1])
    except Exception:
        print("El UID proporcionado no es válido. Debe ser un ObjectId de MongoDB.")
        sys.exit(1)
    cargar_productos_desde_csv_a_mongodb(uid)
