import hydra
from omegaconf import DictConfig
import os
import sys
import traceback
from pathlib import Path
import asyncio

@hydra.main(config_path="./config", config_name="config", version_base=None)
def ejecutar_onboarding_completo(cfg: DictConfig):
    """
    Ejecuta todos los procesos de onboarding y carga de datos en orden lógico.
    Si ocurre un error en cualquier paso, el proceso se detiene y muestra el error.
    """
    try:
        # === Acceder a configuraciones globales y rutas ===
        project_root = hydra.utils.get_original_cwd()  # Directorio raíz del proyecto
        global_config = cfg.global
        paths = cfg.paths

        # === PASO 1: Configuración del usuario de pruebas ===
        print("\n" + "="*80)
        print("[1/8] Configuración del usuario de pruebas...")
        print("="*80)
        from usuario.onboarding_user import setup_user
        try:
            UID = asyncio.run(setup_user(global_config))  # Pasar configuraciones globales
            if not UID:
                raise ValueError("No se obtuvo UID del usuario de pruebas.")
            else:
                print(f"Usuario {UID} configurado correctamente.")
                # Actualizar UID en la configuración (no en .env)
                cfg.global_.uid_user = UID
                print(f"UID_USER actualizado en la configuración: {UID}")
        except Exception as e:
            print(f"\nError en la configuración del usuario: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

        # === PASO 2: Carga de productos a la base de datos ===
        print("\n" + "="*80)
        print("[2/8] Carga de productos a la base de datos...")
        print("="*80)
        from productos.subir_productos_mongodb import cargar_productos_desde_csv_a_mongodb
        try:
            cargar_productos_desde_csv_a_mongodb(UID, paths.csv_productos)
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
            output_dir = os.path.join(project_root, "results")
            limpiar_y_procesar_proveedores(paths.dir_proveedores, output_dir)
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
            actualizar_responsabilidad_fiscal(UID, paths.modelo_terceros)
            print("Actualización de responsabilidad fiscal y actividad económica completada.")
        except Exception as e:
            print(f"\nError en la actualización de responsabilidad fiscal: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

        # === PASO 6: Procesamiento de facturas de arrendamiento ===
        print("\n" + "="*80)
        print("[6/8] Procesamiento de facturas de arrendamiento...")
        print("="*80)
        from causaciones.facturas_arrendamiento_por_proveedor import main as procesamiento_facturas
        try:
            procesamiento_facturas(UID, paths.zip_facturas)
            print("Procesamiento de facturas de arrendamiento completado.")
        except Exception as e:
            print(f"\nError en el procesamiento de facturas de arrendamiento: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

        # === PASO 7: Procesamiento y subida de PUCs del usuario ===
        print("\n" + "="*80)
        print("[7/8] Procesamiento y subida de PUCs del usuario...")
        print("="*80)
        # Nota: No hay lógica para PUCs en el script original. Agregar cuando se proporcione.

        # === PASO 8: Procesamiento del modelo de causación ===
        print("\n" + "="*80)
        print("[8/8] Procesamiento del modelo de causación...")
        print("="*80)
        from causaciones.onboarding_causacion import main as procesamiento_causacion
        try:
            procesamiento_causacion(UID, paths.xlsx_causacion)
            print("Procesamiento del modelo de causación completado.")
        except Exception as e:
            print(f"\nError en el procesamiento del modelo de causación: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

        # === FINALIZACIÓN ===
        print("\n" + "="*80)
        print("¡Todos los procesos se completaron exitosamente! ✨")
        print("="*80)

        # Guardar log en la carpeta de salida de Hydra
        output_dir = os.path.join(project_root, "outputs")
        with open(os.path.join(output_dir, "onboarding_log.txt"), "w") as f:
            f.write(f"Procesamiento completado para consecutivo {global_config.num_consecutivo}")

    except Exception as e:
        print(f"\nError general en el proceso de onboarding: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    ejecutar_onboarding_completo()