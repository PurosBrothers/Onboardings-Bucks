"""
Main script to run all data processing tasks.
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
        # PASO 1: CONFIGURAR USUARIO
        print("\n" + "="*80)
        print("Running User Setup...")
        print("="*80)
        
        # Importar y ejecutar el script de usuario
        from surtifloraUser import setup_user

        try:
            # Ejecutar la función setup_user correctamente con asyncio
            uid = asyncio.run(setup_user())
            if not uid:
                raise ValueError("Failed to setup user - no UID returned")
            else:
                print(f"Usuario {uid} configurado correctamente.")
        except Exception as e:
            print(f"\nError en la configuración del usuario: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)

        # PASO 2: GENERAR PRODUCTOS EN DB
        print("\n" + "="*80)
        print("Running Products DB generation...")
        print("="*80)

        # Importar y ejecutar el script de generación de productos en db
        from surtifloraProductosDBGenerate import delete_existing_products, create_products_from_csv
        from bson import ObjectId
        
        try:
            uid = ObjectId(os.getenv("UID_USER"))
            delete_existing_products(uid)
            create_products_from_csv()
            print("Productos en DB generados correctamente.")
        except Exception as e:
            print(f"\nError en generación de productos: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)

        # PASO 3: PROCESAR LIBRO AUXILIAR
        print("\n" + "="*80)
        print("Running Libro Auxiliar processing...")
        print("="*80)

        # Importar y ejecutar el script de procesamiento del libro auxiliar
        from limpiar_excels import process_all_files
        
        try:
            input_dir = os.path.join("data", "proveedores")
            output_dir = os.path.join(".", "results")
            print(f"input_dir: {input_dir}")
            print(f"output_dir: {output_dir}")
            process_all_files(input_dir, output_dir)
            print("Libro Auxiliar procesado correctamente.")
        except Exception as e:
            print(f"\nError en procesamiento del Libro Auxiliar: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)

        # PASO 4: ONBOARDING DE PROVEEDORES
        print("\n" + "="*80)
        print("Running Provider Onboarding process...")
        print("="*80)
        
        # Importar y ejecutar el script de onboarding de proveedores
        from surtiflora import surtiflora_main
        
        try:
            surtiflora_main()
            print("El script de onboarding de proveedores ejecutado correctamente.")
        except Exception as e:
            print(f"\nError en el proceso de onboarding de proveedores: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)

        # PASO 5: ACTUALIZAR RESPONSABILIDAD FISCAL
        print("\n" + "="*80)
        print("Running Actualizar responsabilidad Fiscal process...")
        print("="*80)
        
        from modelo_terceros import actualizar_responsabilidad_fiscal_actividad
        
        try:
            # Ejecutar la función de actualización de responsabilidad fiscal y actividad
            actualizar_responsabilidad_fiscal_actividad()
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
            facturas_main()
            print("Proceso de facturas de arrendamiento por proveedor completado.")
        except Exception as e:
            print(f"\nError en el proceso de facturas de arrendamiento: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)

        # PASO 7: CARGAR Y SUBIR PUCS
        """print("\n" + "="*80)
        print("Running PUCs upload process...")
        print("="*80)
        
        from pucs_user import get_pucs_user, upload_pucs
        
        try:
            pucs_user = get_pucs_user()
            if pucs_user:
                print(f"PUCs del usuario cargados correctamente: {len(pucs_user)} PUCs encontrados.")
                upload_pucs(pucs_user)
                print("PUCs subidos correctamente.")
            else:
                print("No se encontraron PUCs del usuario.")
        except Exception as e:
            print(f"\nError al cargar o subir PUCs: {str(e)}")
            print("\nFull stack trace:")
            traceback.print_exc()
            sys.exit(1)"""

        # Paso 8 -- mismo del 7 . Favor revisar
        print("\n" + "="*80)
        print("Running Modelo Causación process...")
        print("="*80)
        
        # Importar y ejecutar el script de procesamiento del modelo de causación
        from surtifloraCausacion import process_excel_file
        
        try:
            # Obtener UID_USER y construir la ruta del archivo
            uid_user = os.getenv("UID_USER")
            if not uid_user:
                raise ValueError("Variable de entorno UID_USER no encontrada en el archivo .env")
            
            app_root = os.path.abspath(os.path.dirname(__file__))
            xlsx_path = os.path.abspath(os.path.join(app_root, "..", "data", "modelos_causacion", "SurtifloraModeloCausacionAbril2025.xlsx"))
            
            process_excel_file(uid_user, xlsx_path)
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