import csv
import os
import sys
from datetime import datetime, timezone
from typing import Callable
from bson import ObjectId

from utils.parse import parse_price

class CSVManager:
    def __init__(
        self,
        csv_path: str,
        product_exists: Callable[[str], bool],
        save_product: Callable[[dict], None],
        uid: ObjectId,  
    ):
        self.csv_path = csv_path
        self.product_exists = product_exists
        self.save_product = save_product
        self.uid = uid  
        self.created_count = 0
        self.product_index = 1

    def _generate_code(self) -> str:
        return f"Surtiflora{self.product_index:09d}"

    def _build_product_document(self, code: str, name: str, description: str, price1: float) -> dict:
        now = datetime.now(timezone.utc)
        return {
            "UID": self.uid, 
            "code": code,
            "active": True,
            "available_quantity": 0,
            "inventory_type": 1,
            "description": description,
            "name": name or f"Producto {self.product_index}",
            "prices": [{
                "currency_code": "COP",
                "price_list": [{"position": 1, "name": "Lista general", "value": price1}]
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
            "createdAt": now,
            "updatedAt": now,
        }

    def run(self):
        if not os.path.exists(self.csv_path):
            print(f"❌ Archivo CSV no encontrado: {self.csv_path}")
            return

        with open(self.csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for _ in range(5): 
                next(reader, None)

            for row in reader:
                name = (row[3] or "").strip()
                description = (row[4] or "").strip()
                price1 = parse_price(row[6] if len(row) > 6 else "0")

                code = self._generate_code()

                if self.product_exists(code):
                    self.product_index += 1
                    continue

                doc = self._build_product_document(code, name, description, price1)
                self.save_product(doc)

                print(
                    f"✅ Created: {doc['name']} ({code}) with price {price1}".encode(
                        sys.stdout.encoding, errors='replace'
                    ).decode(sys.stdout.encoding)
                )

                self.created_count += 1
                self.product_index += 1

        print(
            f"🎉 Done. Created {self.created_count} new products.".encode(
                sys.stdout.encoding, errors='replace'
            ).decode(sys.stdout.encoding)
        )
