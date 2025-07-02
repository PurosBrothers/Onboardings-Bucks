import sys
import pandas as pd
import re
from typing import Tuple, List, Dict
import numpy as np
import os
from collections import Counter

"""
Script profesional para limpiar y procesar archivos de proveedores (Libro Auxiliar).
- Todos los métodos y logs están en español.
- Funciones separadas y bien documentadas.
- Un método general orquesta todo el flujo.
"""

# =============================
# CONFIGURACIÓN Y CONSTANTES
# =============================
CONTEOS_ESPERADOS = {
    "Surtiflora-LibroAuxiliar_2022.csv": {
        "total_rows": 7301,
        "puc_5_rows": 3999,
        "puc_6_rows": 3302,
        "description": "Surtiflora-LibroAuxiliar_2022"
    },
    "Surtiflora-LibroAuxiliar_2023.csv": {
        "total_rows": 13748,
        "puc_5_rows": 4370,
        "puc_6_rows": 9378,
        "description": "Surtiflora-LibroAuxiliar_2023"
    },
    "Surtiflora-LibroAuxiliar_2024.csv": {
        "total_rows": 7083,
        "puc_5_rows": 5664,
        "puc_6_rows": 1419,
        "description": "Surtiflora-LibroAuxiliar_2024"
    }
}

# =============================
# FUNCIONES AUXILIARES
# =============================
def analizar_densidad_filas(marco_datos: pd.DataFrame, umbral: float = 0.3) -> List[int]:
    """
    Analiza la densidad de datos por fila y retorna los índices válidos.
    
    Args:
        marco_datos (pd.DataFrame): El DataFrame de entrada
        umbral (float): Proporción mínima de valores no nulos requerida (por defecto: 0.3)
    
    Returns:
        List[int]: Lista de índices de filas que cumplen con el umbral de densidad
    """
    # Calcular la proporción de valores no nulos para cada fila
    densidades = marco_datos.notna().sum(axis=1) / marco_datos.shape[1]
    
    # Obtener índices donde la densidad está por encima del umbral
    indices_validos = densidades[densidades >= umbral].index.tolist()
    
    print(f"Se encontraron {len(indices_validos)} filas con densidad de datos mayor al {umbral*100}%")
    
    # Imprimir distribución de densidad
    print("\nDistribución de densidad:")
    print(densidades.describe())
    
    return indices_validos

def analizar_datos(marco_datos: pd.DataFrame) -> None:
    """
    Muestra un resumen de las primeras filas y la distribución de cuentas PUC.
    
    Args:
        marco_datos (pd.DataFrame): DataFrame a analizar
    """
    print("\nPrimeras filas:")
    print(marco_datos.iloc[:5, [1, 2]].to_string())  # Mostrar columnas de PUC y descripción
    
    # Contar tipos de cuenta (comenzando con 5 o 6)
    cuentas = marco_datos.iloc[:, 1].astype(str).str[:1]  # Obtener el primer dígito del código PUC
    conteo = cuentas[cuentas.isin(['5', '6'])].value_counts()
    
    print("\nDistribución de cuentas:")
    for digito, cantidad in conteo.items():
        print(f"Comienza con {digito}: {cantidad} filas")

def extraer_nit_nombre(fila: pd.Series) -> Tuple[str, str]:
    """
    Extrae el NIT y el nombre de una fila, usando múltiples columnas para validación.
    
    Args:
        fila (pd.Series): Fila que contiene información de NIT y nombre
        
    Returns:
        Tuple[str, str]: NIT y nombre
    """
    try:
        # Obtener valores de las columnas relevantes
        nit_nombre = str(fila.iloc[3]) if pd.notna(fila.iloc[3]) else ""
        nit_formateado = str(fila.iloc[4]) if pd.notna(fila.iloc[4]) else ""
        nombre_alt = str(fila.iloc[7]) if pd.notna(fila.iloc[7]) else ""
        
        print(f"\nDepuración extracción NIT/Nombre:")
        print(f"Columna 4 (NIT + Nombre): {nit_nombre}")
        print(f"Columna 5 (NIT formateado): {nit_formateado}")
        print(f"Columna 8 (Nombre alt): {nombre_alt}")
        
        # Extraer NIT (solo dígitos) de la columna 4
        nit = ''.join(c for c in nit_nombre if c.isdigit())
        
        # Validar NIT contra la columna 5 (eliminando comas)
        nit_validacion = ''.join(c for c in nit_formateado if c.isdigit())
        if nit_validacion and nit != nit_validacion:
            print(f"Advertencia: NIT diferente entre columnas - Col4: {nit}, Col5: {nit_validacion}")
            nit = nit_validacion  # Usar la versión formateada si es diferente
            
        # Obtener nombre de la columna 8
        nombre = nombre_alt.strip()
        
        return nit, nombre
        
    except Exception as e:
        print(f"Error en extraer_nit_nombre: {str(e)}")
        print(f"Datos de la fila: {fila.to_dict()}")
        return "", ""

def encontrar_fila_encabezado(marco_datos: pd.DataFrame, umbral_sin_nombre: float = 0.5) -> int:
    """
    Busca la primera fila que debe usarse como encabezados verificando la proporción de columnas sin nombre.
    
    Args:
        marco_datos (pd.DataFrame): El DataFrame de entrada
        umbral_sin_nombre (float): Proporción máxima de columnas sin nombre permitida
        
    Returns:
        int: Índice de la fila a usar como encabezado
    """
    for idx in range(len(marco_datos)):
        # Convertir fila a nombres de columna y verificar proporción de sin nombre
        columnas_prueba = marco_datos.iloc[idx].tolist()
        
        # Contar valores vacíos, nan, o sin nombre
        sin_nombre = sum(1 for col in columnas_prueba 
                          if pd.isna(col)
                          or not str(col).strip()
                          or str(col).strip().startswith('Unnamed:'))
        
        # Contar valores significativos (no solo cualquier cadena no vacía)
        valores_significativos = sum(1 for col in columnas_prueba 
                              if pd.notna(col)
                              and str(col).strip()
                              and any(palabra in str(col).lower() for palabra in 
                                    ['cuenta', 'nit', 'saldo', 'fecha', 'descripcion', 'debito', 'credito', 'comprobante']))
        
        proporcion_sin_nombre = sin_nombre / len(columnas_prueba)
        
        # Queremos al menos 3 nombres de columna significativos y menos que el umbral de sin nombre
        if valores_significativos >= 3 and proporcion_sin_nombre <= umbral_sin_nombre:
            print(f"\nEncabezado encontrado en la fila {idx}")
            print("Encabezados:", [str(col).strip() for col in columnas_prueba if pd.notna(col) and str(col).strip()])
            return idx
            
        # Información de depuración
        if idx < 10:  # Imprimir primeras 10 filas para depuración
            print(f"\nAnálisis de fila {idx}:")
            print(f"Valores significativos: {valores_significativos}")
            print(f"Proporción sin nombre: {proporcion_sin_nombre:.2f}")
            print("Valores:", [str(col).strip() for col in columnas_prueba if pd.notna(col) and str(col).strip()])
            
    return 7  # Por defecto a la fila 6 si no se encuentra una buena fila de encabezado

def validar_conteo_filas(nombre_archivo: str, total: int, puc_5: int, puc_6: int) -> None:
    """
    Valida los conteos de filas contra los valores esperados para un archivo dado.
    
    Args:
        nombre_archivo (str): Nombre del archivo siendo procesado
        total (int): Número total de filas procesadas
        puc_5 (int): Número de filas con PUC comenzando con 5
        puc_6 (int): Número de filas con PUC comenzando con 6
        
    Raises:
        ValueError: Si la validación falla o no se encuentran reglas para el archivo
    """
    base = os.path.basename(nombre_archivo)
    if base not in CONTEOS_ESPERADOS:
        raise ValueError(f"No hay reglas de validación para {base}")
        
    esperado = CONTEOS_ESPERADOS[base]
    errores = []
    
    if total != esperado["total_rows"]:
        errores.append(f"Se esperaban {esperado['total_rows']} filas, pero hay {total}")
    if puc_5 != esperado["puc_5_rows"]:
        errores.append(f"Se esperaban {esperado['puc_5_rows']} filas de PUC 5, pero hay {puc_5}")
    if puc_6 != esperado["puc_6_rows"]:
        errores.append(f"Se esperaban {esperado['puc_6_rows']} filas de PUC 6, pero hay {puc_6}")
        
    if errores:
        raise ValueError(f"Validación fallida para {esperado['description']}:\n" + "\n".join(errores))
        
    print(f"\nValidación de conteo de filas exitosa para {esperado['description']} ✓")

# =============================
# PROCESAMIENTO PRINCIPAL DE ARCHIVO
# =============================
def procesar_libro_auxiliar(archivo_entrada: str, archivo_salida: str, conteos_esperados: Dict[str, int] | None = None) -> None:
    """
    Procesa un archivo de libro auxiliar y genera un CSV limpio.
    
    Args:
        archivo_entrada (str): Ruta al archivo CSV de entrada
        archivo_salida (str): Ruta para guardar el archivo CSV procesado
        conteos_esperados (Dict[str, int] | None): Diccionario opcional con conteos de filas esperados
            que contiene claves: "total_rows", "puc_5_rows", "puc_6_rows"
    """
    # Asegurar que las rutas de archivo usen separadores compatibles con Windows
    archivo_entrada = os.path.normpath(archivo_entrada)
    archivo_salida = os.path.normpath(archivo_salida)
    
    # Probar diferentes codificaciones para Windows
    codificaciones = ['latin1', 'utf-8', 'cp1252']
    marco_datos = None
    codificacion_exitosa = None
    
    for codificacion in codificaciones:
        try:
            print(f"\nIntentando leer el archivo con codificación: {codificacion}")
            # Leer el archivo CSV sin encabezados primero
            marco_datos = pd.read_csv(archivo_entrada, engine='python', encoding=codificacion, header=None)
            codificacion_exitosa = codificacion
            print(f"Lectura exitosa con codificación: {codificacion}")
            break
        except Exception as e:
            print(f"Error con codificación {codificacion}: {str(e)}")
            continue
    
    if marco_datos is None:
        raise ValueError("No se pudo leer el archivo con ninguna de las codificaciones intentadas")
    
    print(f"\nAnálisis inicial: {len(marco_datos)} filas")
    
    # Encontrar la fila del encabezado
    fila_encabezado = encontrar_fila_encabezado(marco_datos)
    
    # Volver a leer el CSV con la fila de encabezado correcta
    try:
        marco_datos = pd.read_csv(archivo_entrada, engine='python', encoding=codificacion_exitosa, skiprows=fila_encabezado)
    except Exception as e:
        print(f"Error al releer con encabezados: {str(e)}")
        # Probar enfoque alternativo - leer sin encabezados y establecer manualmente
        marco_datos = pd.read_csv(archivo_entrada, engine='python', encoding=codificacion_exitosa, header=None, skiprows=fila_encabezado)
        # Usar la primera fila como encabezado (este enfoque puede necesitar ajustes)
        marco_datos.columns = [f"Col_{i}" if pd.isna(x) or not str(x).strip() else str(x).strip() for i, x in enumerate(marco_datos.iloc[0])]
        marco_datos = marco_datos.iloc[1:].reset_index(drop=True)
    
    print(f"\nColumnas detectadas: {marco_datos.columns.tolist()}")
    
    # Obtener índices válidos basados en la densidad de datos
    indices_validos = analizar_densidad_filas(marco_datos)
    
    # Filtrar DataFrame para mantener solo filas válidas
    marco_datos_filtrado = marco_datos.iloc[indices_validos].copy()
    
    # Analizar datos filtrados
    print("\nAnálisis de datos filtrados:")
    analizar_datos(marco_datos_filtrado)
    
    # Crear lista para datos de salida
    datos_salida: List[Dict] = []
    
    # Procesar cada fila válida
    for _, fila in marco_datos_filtrado.iterrows():
        try:
            # Obtener código PUC y descripción directamente de las columnas
            # Manejar posibles errores de conversión de cadena
            codigo_puc = ""
            if pd.notna(fila.iloc[1]):
                try:
                    codigo_puc = str(int(float(fila.iloc[1])))
                except:
                    codigo_puc = str(fila.iloc[1]).strip()
            descripcion = str(fila.iloc[2]) if pd.notna(fila.iloc[2]) else ""
            # Solo procesar filas donde el número de cuenta comienza con 5 o 6
            if codigo_puc and (codigo_puc.startswith('5') or codigo_puc.startswith('6')):
                nit, nombre = extraer_nit_nombre(fila)
                # Omitir filas sin NIT o NOMBRE (probablemente totales)
                if not nit or not nombre:
                    continue
                fila_dict = {
                    'CUENTA': codigo_puc,
                    'DESCRIPCION': descripcion,
                    'NIT': nit,
                    'NOMBRE': nombre
                }
                # Agregar columnas restantes
                for indice_col, nombre_col in enumerate(marco_datos.columns[2:], start=2):
                    fila_dict[nombre_col] = fila.iloc[indice_col]
                datos_salida.append(fila_dict)
        except Exception as e:
            print(f"Error procesando fila {fila.name}: {str(e)}")
            print(f"Datos: {fila.iloc[0:5].to_dict()}")  # Imprimir solo las primeras columnas para reducir tamaño de salida
            continue
    
    # Crear DataFrame final y guardar en CSV
    if not datos_salida:
        raise ValueError("No se pudo procesar ninguna fila válida")
        
    marco_datos_final = pd.DataFrame(datos_salida)
    
    # Eliminar columnas que están completamente vacías o en NA
    columnas_no_vacias = marco_datos_final.columns[marco_datos_final.notna().any()]
    marco_datos_final = marco_datos_final[columnas_no_vacias]
    
    # Formatear CUENTA como entero (eliminando decimales) - con manejo de errores
    try:
        marco_datos_final['CUENTA'] = marco_datos_final['CUENTA'].astype(float).astype(int).astype(str)
    except Exception as e:
        print(f"Error convirtiendo CUENTA: {str(e)}. Se conserva el formato original.")
    
    # Limpiar nombres de columnas
    nuevas_columnas = []
    ya_vistas = set()
    for col in marco_datos_final.columns:
        limpio = str(col).strip()
        if limpio in ya_vistas:
            i = 1
            while f"{limpio}_{i}" in ya_vistas:
                i += 1
            limpio = f"{limpio}_{i}"
        ya_vistas.add(limpio)
        nuevas_columnas.append(limpio)
    marco_datos_final.columns = nuevas_columnas
    
    # Contar códigos PUC que comienzan con 5 y 6
    puc_5 = marco_datos_final['CUENTA'].str.startswith('5').sum()
    puc_6 = marco_datos_final['CUENTA'].str.startswith('6').sum()
    total = len(marco_datos_final)
    
    print(f"\nDistribución final de PUC:")
    print(f"Comienza con 5: {puc_5} filas")
    print(f"Comienza con 6: {puc_6} filas")
    print(f"Total filas: {total}")
    
    # Validar conteos de filas si se proporcionaron conteos esperados
    if conteos_esperados:
        errores = []
        if total != conteos_esperados["total_rows"]:
            errores.append(f"Se esperaban {conteos_esperados['total_rows']} filas, pero hay {total}")
        if puc_5 != conteos_esperados["puc_5_rows"]:
            errores.append(f"Se esperaban {conteos_esperados['puc_5_rows']} filas de PUC 5, pero hay {puc_5}")
        if puc_6 != conteos_esperados["puc_6_rows"]:
            errores.append(f"Se esperaban {conteos_esperados['puc_6_rows']} filas de PUC 6, pero hay {puc_6}")
        
        if errores:
            print("\nADVERTENCIA: Validación de conteo de filas fallida:")
            for error in errores:
                print(f"  - {error}")
            print("Se continuará con el procesamiento a pesar de las diferencias.")
        else:
            print("\nValidación de conteo de filas exitosa ✓")
    
    print(f"\nDatos finales procesados: {len(marco_datos_final)} filas, columnas: {list(marco_datos_final.columns)}")
    print("\nMuestra de datos procesados:")
    print(marco_datos_final.head())
    
    # Usar try-except para guardar en CSV
    try:
        marco_datos_final.to_csv(archivo_salida, index=False, encoding='utf-8-sig')
        print(f"\nArchivo procesado guardado como {archivo_salida} con {len(marco_datos_final)} filas y {len(columnas_no_vacias)} columnas.")
    except Exception as e:
        print(f"\nError al guardar el archivo CSV: {str(e)}")

# =============================
# PROCESAMIENTO DE TODOS LOS ARCHIVOS
# =============================
def procesar_todos_los_archivos(directorio_entrada: str, directorio_salida: str) -> None:
    """
    Procesa todos los archivos CSV en el directorio de entrada y guarda los resultados en el de salida.
    
    Args:
        directorio_entrada (str): Directorio que contiene archivos CSV de entrada
        directorio_salida (str): Directorio donde se guardarán los archivos CSV de salida

    Raises:
        ValueError: Si algún archivo falla en la validación
    """
    # Asegurar que el directorio de salida exista
    os.makedirs(directorio_salida, exist_ok=True)

    # Encontrar todos los archivos CSV en el directorio de entrada
    archivos_csv = [f for f in os.listdir(directorio_entrada) if f.endswith('.csv')]

    if not archivos_csv:
        print(f"No se encontraron archivos CSV en {directorio_entrada}")
        return

    print(f"\nSe encontraron {len(archivos_csv)} archivos CSV para procesar:")
    for archivo in archivos_csv:
        print(f"- {archivo}")

    # Mantener un registro de los resultados del procesamiento
    resultados = []

    # Procesar cada archivo
    for archivo_csv in archivos_csv:
        archivo_entrada = os.path.join(directorio_entrada, archivo_csv)
        archivo_salida = os.path.join(directorio_salida, archivo_csv.replace('.csv', '_Procesado.csv'))

        print(f"\n{'='*80}")
        print(f"Procesando {archivo_csv}...")
        print(f"{'='*80}")

        try:
            # Obtener conteos esperados si están disponibles
            esperado = CONTEOS_ESPERADOS.get(archivo_csv)
            if not esperado:
                raise ValueError(f"No hay reglas de validación para {archivo_csv}")

            procesar_libro_auxiliar(archivo_entrada, archivo_salida, esperado)
            resultados.append({
                "archivo": archivo_csv,
                "estado": "✅ Éxito",
                "total": esperado["total_rows"],
                "puc_5": esperado["puc_5_rows"],
                "puc_6": esperado["puc_6_rows"]
            })
            print(f"Procesado correctamente {archivo_csv}")

        except Exception as e:
            resultados.append({
                "archivo": archivo_csv,
                "estado": "❌ Fallo",
                "error": str(e)
            })
            print(f"Error procesando {archivo_csv}: {str(e)}")
            sys.exit(1)

    # Imprimir resumen
    print("\n" + "="*100)
    print("RESUMEN DE PROCESAMIENTO")
    print("="*100)
    print(f"Total de archivos procesados: {len(resultados)}")

    for res in resultados:
        print(f"\nArchivo: {res['archivo']}")
        print(f"Estado: {res['estado']}")
        if res['estado'] == "✅ Éxito":
            print(f"Total filas: {res['total']}")
            print(f"PUC 5 filas: {res['puc_5']}")
            print(f"PUC 6 filas: {res['puc_6']}")
        else:
            print(f"Error: {res['error']}")

# =============================
# MÉTODO GENERAL PARA LLAMAR TODO
# =============================
def limpiar_y_procesar_proveedores(directorio_entrada, directorio_salida, ambiente):
    """
    Método general para limpiar y procesar todos los archivos de proveedores.
    Args:
        directorio_entrada (str): Directorio de entrada con los archivos CSV
        directorio_salida (str): Directorio de salida para los archivos procesados
        ambiente (str): Ambiente de ejecución
    """
    # Si en el futuro se requiere usar ambiente, se puede pasar a funciones internas
    procesar_todos_los_archivos(directorio_entrada, directorio_salida)

# =============================
# MAIN
# =============================
if __name__ == "__main__":
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ambiente = os.environ.get("ambiente", "STAGING")
    limpiar_y_procesar_proveedores(directorio_actual, directorio_actual, ambiente)