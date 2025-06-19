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
import asyncio  # Importamos asyncio para manejar funciones asíncronas

# Add the project root directory to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dotenv import load_dotenv, set_key

# Debug: Check if .env file exists and load it
print("🔍 Debugging .env file loading:")
env_file_path = Path(project_root) / ".env"
print(f"  - .env file path: {env_file_path}")
print(f"  - .env file exists: {env_file_path.exists()}")

# Load .env file
load_dotenv(env_file_path)
print("  - .env file loaded")

# Debug: Check some key environment variables
print("🔍 Checking environment variables:")
test_vars = [
    "TEST_USER_EMAIL",
    "TEST_PASSWORD_PLAIN", 
    "TEST_USER_NAME",
    "TEST_USER_LASTNAME",
    "TEST_USER_PHONE",
    "TEST_EXPENSE_CODE",
    "TEST_COST_CODE",
    "DEV_AWS_ACCESS_KEY_ID",
    "DEV_AWS_SECRET_ACCESS_KEY",
    "DEV_CLUSTER_URL",
    "DEV_DB",
    "DEV_APP_NAME"
]

UID = ""

for var in test_vars:
    value = os.getenv(var)
    print(f"  - {var}: {'✓ Set' if value else '✗ Missing'}")

print("🔍 End of .env debugging\n")

# Set the NUM_CONSECUTIVO environment variable before importing setup_user
set_key(".env", "NUM_CONSECUTIVO", "1")  # El número consecutivo que el usuario nos da, lo mandamos al .env

def run_all_tasks():
    """
    Run all data processing tasks in sequence.
    
    Raises:
        SystemExit: If any task fails
    """
    try:
        # PASO 1: CONFIGURAR USUARIO CORREO, CONTRASEÑA, UID Y TODO
        print("\n" + "="*80)
        print("Running User Setup...")
        print("="*80)
        
        # Importar y ejecutar el script de usuario
        from usuario.onboarding_user import setup_user

        try:
            # Ejecutar la función setup_user correctamente con asyncio
            UID = asyncio.run(setup_user())
            if not UID:
                raise ValueError("Failed to setup user - no UID returned")
            else:
                print(f"Usuario {UID} configurado correctamente.")
                set_key(str(env_file_path), "UID_USER", UID)
                print(f"UID_USER actualizado en el .env: {UID}")
        except Exception as e:
            print(f"\nError en la configuración del usuario: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)

        # PASO 2: GENERAR PRODUCTOS EN DB, LEE DE LOS EXCELS Y LOS CREA EN LA DB
        """ print("\n" + "="*80)
        print("Running Products DB generation...")
        print("="*80)
        
        from productos.subir_productos_mongodb import cargar_productos_desde_csv_a_mongodb
        from bson import ObjectId
        
        try:
            cargar_productos_desde_csv_a_mongodb(UID)
            print("Productos en DB generados correctamente.")
        except Exception as e:
            print(f"\nError en generación de productos: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)

        # PASO 3: PROCESAR LIBRO AUXILIAR DE LOS PROVEEDORES
        print("\n" + "="*80)
        print("Running Libro Auxiliar processing...")
        print("="*80)

        # Importar y ejecutar el script de procesamiento del libro auxiliar
        from proveedores.limpiar_excels_proveedores import limpiar_y_procesar_proveedores
        
        try:
            input_dir = os.path.join("data", "proveedores")
            output_dir = os.path.join(".", "results")
            print(f"input_dir: {input_dir}")
            print(f"output_dir: {output_dir}")
            limpiar_y_procesar_proveedores(input_dir, output_dir)
            print("Libro Auxiliar procesado correctamente.")
        except Exception as e:
            print(f"\nError en procesamiento del Libro Auxiliar: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)"""

        # PASO 4: SUBE LOS PROVEEDORES A LA DB
        print("\n" + "="*80)
        print("Running Provider Onboarding process...")
        print("="*80)
        
        # Importar y ejecutar el script de onboarding de proveedores
        from proveedores.subir_proveedores_mongodb import subir_main
        
        try:
            subir_main(UID)
            print("El script de onboarding de proveedores ejecutado correctamente.")
        except Exception as e:
            print(f"\nError en el proceso de onboarding de proveedores: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)

        # PASO 5: ACTUALIZAR RESPONSABILIDAD FISCAL EN LOS PROVEEDORES
        print("\n" + "="*80)
        print("Running Actualizar responsabilidad Fiscal process...")
        print("="*80)
        
        from proveedores.modelo_terceros import main as actualizar_responsabilidad_fiscal_actividad
        
        try:
            # Ejecutar la función de actualización de responsabilidad fiscal y actividad
            actualizar_responsabilidad_fiscal_actividad(UID)
            print("Actualización de responsabilidad fiscal y actividad económica completada.")
        except Exception as e:
            print(f"\nError en la actualización de responsabilidad fiscal: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)

        # PASO 6: FACTURAS DE ARRENDAMIENTO 
        print("\n" + "="*80)
        print("Running Facturas de Arrendamiento process...")
        print("="*80)
        
        from causaciones.facturas_arrendamiento_por_proveedor import main as facturas_main
        
        try:
            facturas_main(UID)
            print("Proceso de facturas de arrendamiento por proveedor completado.")
        except Exception as e:
            print(f"\nError en el proceso de facturas de arrendamiento: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)

        # Paso 7 SUBE LOS CODIGOS PUCS DEL USUARIO
        
        print("\n" + "="*80)
        print("Running Modelo Causación process...")
        print("="*80)
        
        # Importar y ejecutar el script de procesamiento del modelo de causación
        from causaciones.onboarding_causacion import main as procesar_causacion
        
        try:
            
            app_root = os.path.abspath(os.path.dirname(__file__))
            xlsx_path = os.path.abspath(os.path.join(app_root, "..", "data", "modelos_causacion", "SurtifloraModeloCausacionAbril2025.xlsx"))
            
            procesar_causacion(UID, xlsx_path)
            print("Proceso de modelo de causación completado.")
        except Exception as e:
            print(f"\nError en el proceso de modelo de causación: {str(e)}")
            sys.exit(1)
        
        # FINALIZACIÓN
        print("\n" + "="*80)
        print("All tasks completed successfully! ✨")
        print("="*80)

    except Exception as e:
        print(f"\nError general en run_all_tasks: {str(e)}")
        print("\nFull stack trace:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_all_tasks()