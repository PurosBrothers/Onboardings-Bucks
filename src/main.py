"""
Script principal de automatización para el procesamiento y carga de datos en el sistema Bucks OnBoardings.

Este script ejecuta de forma secuencial y automatizada los siguientes procesos:
1. Configuración y recreación del usuario de pruebas en la base de datos, incluyendo módulos e integraciones asociadas.
2. Limpieza y carga de productos en la base de datos a partir de archivos CSV.
3. Procesamiento y limpieza de archivos del Libro Auxiliar de proveedores.
4. Ejecución del proceso de onboarding de proveedores.
5. Actualización de la responsabilidad fiscal y actividad económica de terceros.
6. Procesamiento de facturas de arrendamiento por proveedor.
7. Carga y subida de PUCs del usuario.
8. Procesamiento del modelo de causación a partir de un archivo Excel.
"""

import os
import sys
import traceback
from pathlib import Path
import asyncio
from dotenv import load_dotenv, set_key

# =============================
# Carga y verificación de variables de entorno
# =============================
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print("🔍 Debugging .env file loading:")
env_file_path = Path(project_root) / ".env"
print(f"  - .env file path: {env_file_path}")
print(f"  - .env file exists: {env_file_path.exists()}")
load_dotenv(env_file_path)
print("  - .env file loaded")

print("🔍 Checking environment variables:")
test_vars = [
    "TEST_USER_EMAIL", "TEST_PASSWORD_PLAIN", "TEST_USER_NAME", "TEST_USER_LASTNAME", "TEST_USER_PHONE",
    "TEST_EXPENSE_CODE", "TEST_COST_CODE", "DEV_AWS_ACCESS_KEY_ID", "DEV_AWS_SECRET_ACCESS_KEY",
    "DEV_CLUSTER_URL", "DEV_DB", "DEV_APP_NAME"
]
UID = ""
for var in test_vars:
    value = os.getenv(var)
    print(f"  - {var}: {'✓ Set' if value else '✗ Missing'}")
print("🔍 End of .env debugging\n")
set_key(".env", "NUM_CONSECUTIVO", "1")

# =============================
# Ejecución secuencial de procesos de onboarding
# =============================
def ejecutar_onboarding_completo():
    """
    Ejecuta todos los procesos de onboarding y carga de datos en orden lógico.
    Si ocurre un error en cualquier paso, el proceso se detiene y muestra el error.
    """
    try:
        # === PASO 1: Configuración del usuario de pruebas ===
        print("\n" + "="*80)
        print("[1/8] Configuración del usuario de pruebas...")
        print("="*80)
        from usuario.onboarding_user import setup_user
        try:
            UID = asyncio.run(setup_user())
            if not UID:
                raise ValueError("No se obtuvo UID del usuario de pruebas.")
            else:
                print(f"Usuario {UID} configurado correctamente.")
                set_key(str(env_file_path), "UID_USER", UID)
                print(f"UID_USER actualizado en el .env: {UID}")
        except Exception as e:
            print(f"\nError en la configuración del usuario: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

        """        # === PASO 2: Carga de productos a la base de datos ===
        print("\n" + "="*80)
        print("[2/8] Carga de productos a la base de datos...")
        print("="*80)
        from productos.subir_productos_mongodb import cargar_productos_desde_csv_a_mongodb
        try:
            cargar_productos_desde_csv_a_mongodb(UID)
            print("Productos cargados correctamente en la base de datos.")
        except Exception as e:
            print(f"\nError en la carga de productos: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

        # === PASO 3: Procesamiento del Libro Auxiliar de proveedores ===
        print("\n" + "="*80)
        print("[3/8] Procesamiento del Libro Auxiliar de proveedores...")
        print("="*80)
        from proveedores.limpiar_excels_proveedores import limpiar_y_procesar_proveedores
        try:
            input_dir = os.path.join("data", "proveedores")
            output_dir = os.path.join(".", "results")
            limpiar_y_procesar_proveedores(input_dir, output_dir)
            print("Libro Auxiliar procesado correctamente.")
        except Exception as e:
            print(f"\nError en el procesamiento del Libro Auxiliar: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

        # === PASO 4: Onboarding de proveedores ===
        print("\n" + "="*80)
        print("[4/8] Onboarding de proveedores...")
        print("="*80)
        from proveedores.subir_proveedores_mongodb import subir_main as onboarding_proveedores
        try:
            onboarding_proveedores(UID)
            print("Onboarding de proveedores ejecutado correctamente.")
        except Exception as e:
            print(f"\nError en el onboarding de proveedores: {str(e)}")
            traceback.print_exc()
            sys.exit(1)
        
        # === PASO 5: Actualización de responsabilidad fiscal y actividad económica ===
        print("\n" + "="*80)
        print("[5/8] Actualización de responsabilidad fiscal y actividad económica...")
        print("="*80)
        from proveedores.modelo_terceros import main as actualizar_responsabilidad_fiscal
        try:
            actualizar_responsabilidad_fiscal(UID)
            print("Actualización de responsabilidad fiscal y actividad económica completada.")
        except Exception as e:
            print(f"\nError en la actualización de responsabilidad fiscal: {str(e)}")
            traceback.print_exc()
            sys.exit(1)
        
        """
        
        # === PASO 6: Procesamiento de facturas de arrendamiento ===
        print("\n" + "="*80)
        print("[6/8] Procesamiento de facturas de arrendamiento...")
        print("="*80)
        from causaciones.facturas_por_proveedor import main as procesamiento_facturas
        try:
            procesamiento_facturas(UID)
            print("Procesamiento de facturas de arrendamiento completado.")
        except Exception as e:
            print(f"\nError en el procesamiento de facturas de arrendamiento: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

        # === PASO 7: Procesamiento y subida de PUCs del usuario ===
        # === PASO 8: Procesamiento del modelo de causación ===
        print("\n" + "="*80)
        print("[7/8] Procesamiento y subida de PUCs del usuario...")
        print("="*80)
        
        print("\n" + "="*80)
        print("[8/8] Procesamiento del modelo de causación...")
        print("="*80)
        from causaciones.onboarding_causacion import main as procesamiento_causacion
        try:
            app_root = os.path.abspath(os.path.dirname(__file__))
            xlsx_path = os.path.abspath(os.path.join(app_root, "..", "data", "modelos_causacion", "SurtifloraModeloCausacionAbril2025.xlsx"))
            procesamiento_causacion(UID, xlsx_path)
            print("Procesamiento del modelo de causación completado.")
        except Exception as e:
            print(f"\nError en el procesamiento del modelo de causación: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

        # === FINALIZACIÓN ===
        print("\n" + "="*80)
        print("¡Todos los procesos se completaron exitosamente! ✨")
        print("="*80)

    except Exception as e:
        print(f"\nError general en el proceso de onboarding: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    ejecutar_onboarding_completo()