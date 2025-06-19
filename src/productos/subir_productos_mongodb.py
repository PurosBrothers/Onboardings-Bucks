import os
from dotenv import load_dotenv
import csv
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timezone
import sys  # Import the sys module

# Carga variables de entorno desde .env
load_dotenv()

CSV_PATH = os.path.join("data", "productos", "SurtifloraListaProductos.csv")

# Configuración MongoDB (Staging)
# aws_key = os.getenv("STAGING_AWS_ACCESS_KEY_ID")
# aws_secret = os.getenv("STAGING_AWS_SECRET_ACCESS_KEY")
# cluster = os.getenv("STAGING_CLUSTER_URL")
# db_name = os.getenv("STAGING_DB")
# app_name = os.getenv("STAGING_APP_NAME")

# Configuración MongoDB (Dev)
aws_key = os.getenv("DEV_AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("DEV_AWS_SECRET_ACCESS_KEY")
cluster = os.getenv("DEV_CLUSTER_URL")
db_name = os.getenv("DEV_DB")
app_name = os.getenv("DEV_APP_NAME")

uri = (
    f"mongodb+srv://{aws_key}:{aws_secret}"
    f"@{cluster}"
    "?authSource=%24external"
    "&authMechanism=MONGODB-AWS"
    "&retryWrites=true&w=majority"
    f"&appName={app_name}"
)


UID_FILTER = ObjectId(os.getenv("UID_USER"))

def parse_price(value_str):
    """
    Convierte una cadena con separadores de miles (coma) y punto decimal
    en un float.  Ej: "25,000.00000" -> 25000.0
    """
    # Elimina las comas utilizadas como separador de miles
    cleaned = value_str.replace(',', '')
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def create_products_from_csv():
    client = MongoClient(uri)
    db = client[db_name]
    products = db["products"]

    created_count = 0
    product_index = 1

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        # Saltar las primeras 5 líneas
        for _ in range(5):
            next(reader, None)

        for row in reader:
            # columnas: D->row[3], E->row[4], G->row[6]
            name = (row[3] or "").strip()
            description = (row[4] or "").strip()
            price1 = parse_price(row[6] if len(row) > 6 else "0")
            linea = (row[0] or "").strip()
            grupo = (row[1] or "").strip()
            unidadMedida = (row[79] or "").strip()
            nit_proveedor = [limpiar_nit(row[i]) for i in range(50, 54) if row[i].strip()]

            code = f"Surtiflora{product_index:09d}"

            # Verificar la existencia
            if products.find_one({"UID": UID_FILTER, "code": code}):
                product_index += 1
                continue

            # Construir la lista de precios dinámicamente
            price_list = [{
                "position": 1,
                "name": "Lista general",
                "value": price1
            }]

            doc = {
                "UID": UID_FILTER,
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

            products.insert_one(doc)
            # Imprime en la salida estándar, lo que aún puede tener problemas de codificación.
            # Utiliza la declaración de impresión corregida a continuación.
            print(f"✅ Created: {doc['name']} ({code}) with price {price1}".encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))
            created_count += 1
            product_index += 1

    client.close()
    # Utiliza sys.stdout.encoding para manejar la codificación correctamente.
    print(f"🎉 Done. Created {created_count} new products.".encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))


def delete_existing_products(uid: ObjectId):
    """
    Delete all products with the specified UID from the products collection.

    Args:
        uid (ObjectId): The UID filter to match products for deletion
    """
    client = MongoClient(uri)
    db = client[db_name]
    products = db["products"]

    result = products.delete_many({"UID": uid})
    print(f"🗑️  Deleted {result.deleted_count} existing products for UID: {uid}".encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))
    client.close()
    
def limpiar_nit(nit_raw: str) -> str:
    """
    Limpia el NIT/cédula de caracteres no numéricos y espacios.
    """
    return ''.join(filter(str.isdigit, nit_raw or ''))


if __name__ == "__main__":
    delete_existing_products(UID_FILTER)
    create_products_from_csv()
