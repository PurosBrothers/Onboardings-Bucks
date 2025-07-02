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
import asyncio
from pathlib import Path
import hydra
from omegaconf import DictConfig

# =============================
# Configuración del path del proyecto
# =============================
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# =============================
# Función principal con Hydra
# =============================
@hydra.main(version_base=None, config_path="conf", config_name="conf")
def main_onboarding(cfg: DictConfig) -> None:
    """
    Función principal para ejecutar el onboarding completo usando configuración Hydra.
    
    Args:
        cfg: Configuración cargada desde YAML por Hydra
    """
    # Convertir DictConfig a diccionario normal para facilitar el manejo
    config_dict = {
        'user': dict(cfg.user),
        'mongodb': dict(cfg.mongodb)
    }
    
    # Configuración del ambiente (puede venir como parámetro o por defecto)
    ambiente = getattr(cfg, 'ambiente', 'DEV')
    
    print("🔍 Configuración cargada desde YAML:")
    print(f"  - Ambiente: {ambiente}")
    print(f"  - Usuario: {config_dict['user']['email']}")
    print(f"  - DB: {config_dict['mongodb'][ambiente]['db_name']}")
    print("🔍 Fin de debug de configuración\n")
    
    def ejecutar_onboarding_completo():
        """
        Ejecuta todos los procesos de onboarding y carga de datos en orden lógico.
        """
        # === PASO 1: Configuración del usuario de pruebas ===
        print("\n" + "="*80)
        print("[1/7] Configuración del usuario de pruebas...")
        print("="*80)
        
        from usuario.onboarding_usuario import setup_usuario
        
        try:
            # Pasar el diccionario de configuración y ambiente a setup_user
            UID = asyncio.run(setup_usuario(config_dict, ambiente))
            
            if not UID:
                raise ValueError("No se obtuvo UID del usuario de pruebas.")
            else:
                print(f"Usuario {UID} configurado correctamente.")
                

        except Exception as e:
            print(f"\nError en la configuración del usuario: {str(e)}")
            traceback.print_exc()
            sys.exit(1)
        """""
        # === PASO 2: Carga de productos a la base de datos ===
        print("\n" + "="*80)
        print("[2/7] Carga de productos a la base de datos...")
        print("="*80)
        from productos.subir_productos_mongodb import cargar_productos_desde_csv_a_mongodb
        try:
            cargar_productos_desde_csv_a_mongodb(UID, ambiente)
            print("Productos cargados correctamente en la base de datos.")
        except Exception as e:
            print(f"\nError en la carga de productos: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

        # === PASO 3: Procesamiento del Libro Auxiliar de proveedores ===
        print("\n" + "="*80)
        print("[3/7] Procesamiento del Libro Auxiliar de proveedores...")
        print("="*80)
        from proveedores.limpiar_excels_proveedores import limpiar_y_procesar_proveedores
        try:
            input_dir = os.path.join("data", "proveedores")
            output_dir = os.path.join(".", "results")
            limpiar_y_procesar_proveedores(input_dir, output_dir, ambiente)
            print("Libro Auxiliar procesado correctamente.")
        except Exception as e:
            print(f"\nError en el procesamiento del Libro Auxiliar: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

        # === PASO 4: Onboarding de proveedores ===
        print("\n" + "="*80)
        print("[4/7] Onboarding de proveedores...")
        print("="*80)
        from proveedores.subir_proveedores_mongodb import subir_main as onboarding_proveedores
        try:
            onboarding_proveedores(UID, ambiente)
            print("Onboarding de proveedores ejecutado correctamente.")
        except Exception as e:
            print(f"\nError en el onboarding de proveedores: {str(e)}")
            traceback.print_exc()
            sys.exit(1)
        
        # === PASO 5: Actualización de responsabilidad fiscal y actividad económica ===
        print("\n" + "="*80)
        print("[5/7] Actualización de responsabilidad fiscal y actividad económica...")
        print("="*80)
        from proveedores.actualizar_proveedores_de_modelo_terceros import main as actualizar_responsabilidad_fiscal
        try:
            actualizar_responsabilidad_fiscal(UID, ambiente)
            print("Actualización de responsabilidad fiscal y actividad económica completada.")
        except Exception as e:
            print(f"\nError en la actualización de responsabilidad fiscal: {str(e)}")
            traceback.print_exc()
            sys.exit(1)
            
        # === PASO 6: Procesamiento de facturas de arrendamiento ===
        print("\n" + "="*80)
        print("[6/7] Procesamiento de facturas de arrendamiento...")
        print("="*80)
        from causaciones.subir_facturas_mongodb import main as procesamiento_facturas
        from causaciones.renombrar_excels import renombrar_archivos_excel
        try:
            #procesamiento_facturas(UID, AMBIENTE)
            renombrar_archivos_excel()
            print("Procesamiento de facturas de arrendamiento completado.")
        except Exception as e:
            print(f"\nError en el procesamiento de facturas de arrendamiento: {str(e)}")
            traceback.print_exc()
            sys.exit(1) 
        """""
        # === PASO 7: Procesamiento del modelo de causación y subida de PUCs del usuario ===
        print("\n" + "="*80)
        print("[7/7] Procesamiento del modelo de causación y subida de PUCs del usuario...")
        print("="*80)
        from causaciones.onboarding_causacion import main as procesamiento_causacion
        try:
            app_root = os.path.abspath(os.path.dirname(__file__))
            xlsx_path = os.path.abspath(os.path.join(app_root, "..", "data", "modelos_causacion", "SurtifloraModeloCausacionAbril2025.xlsx"))
            procesamiento_causacion(UID, xlsx_path, ambiente)
            print("Procesamiento del modelo de causación y subida de PUCs completado.")
        except Exception as e:
            print(f"\nError en el procesamiento del modelo de causación y subida de PUCs: {str(e)}")
            traceback.print_exc()
            sys.exit(1)
        
        # === FINALIZACIÓN ===
        print("\n" + "="*80)
        print("¡Todos los procesos se completaron exitosamente! ✨")
        print("="*80)

    # Llamar a la función de onboarding completo
    try:
        ejecutar_onboarding_completo()
    except Exception as e:
        print(f"\nError general en el proceso de onboarding: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main_onboarding()