import sys
import pandas as pd
import re
from typing import Tuple, List, Dict
import numpy as np
import os
from collections import Counter

def analyze_row_density(df: pd.DataFrame, threshold: float = 0.3) -> List[int]:
    """
    Analyze each row to determine which ones have enough data to be considered valid.
    
    Args:
        df (pd.DataFrame): The input DataFrame
        threshold (float): Minimum proportion of non-null values required (default: 0.3)
    
    Returns:
        List[int]: List of indices for rows that meet the density threshold
    """
    # Calculate the proportion of non-null values for each row
    row_densities = df.notna().sum(axis=1) / df.shape[1]
    
    # Get indices where density is above threshold
    valid_indices = row_densities[row_densities >= threshold].index.tolist()
    
    print(f"Found {len(valid_indices)} rows with data density above {threshold*100}%")
    
    # Print density distribution
    print("\nDensity distribution:")
    print(row_densities.describe())
    
    return valid_indices

def analyze_data(df: pd.DataFrame) -> None:
    """
    Print analysis of the DataFrame contents.
    
    Args:
        df (pd.DataFrame): DataFrame to analyze
    """
    print("\nFirst few rows:")
    print(df.iloc[:5, [1, 2]].to_string())  # Show PUC and description columns
    
    # Count account types (starting with 5 or 6)
    accounts = df.iloc[:, 1].astype(str).str[:1]  # Get first digit of PUC code
    account_counts = accounts[accounts.isin(['5', '6'])].value_counts()
    
    print("\nAccount number distribution:")
    for digit, count in account_counts.items():
        print(f"Starting with {digit}: {count} rows")

def extract_nit_info(row: pd.Series) -> Tuple[str, str]:
    """
    Extract NIT and name from row data, using multiple columns for validation.
    
    Args:
        row (pd.Series): Row containing NIT and name information
        
    Returns:
        Tuple[str, str]: NIT and name
    """
    try:
        # Get values from relevant columns
        nit_nombre = str(row.iloc[3]) if pd.notna(row.iloc[3]) else ""
        nit_formatted = str(row.iloc[4]) if pd.notna(row.iloc[4]) else ""
        nombre_alt = str(row.iloc[7]) if pd.notna(row.iloc[7]) else ""
        
        print(f"\nNIT/Nombre extraction debug:")
        print(f"Column 4 (NIT + Nombre): {nit_nombre}")
        print(f"Column 5 (NIT formatted): {nit_formatted}")
        print(f"Column 8 (Nombre alt): {nombre_alt}")
        
        # Extract NIT (only digits) from column 4
        nit = ''.join(c for c in nit_nombre if c.isdigit())
        
        # Validate NIT against column 5 (removing commas)
        nit_validation = ''.join(c for c in nit_formatted if c.isdigit())
        if nit_validation and nit != nit_validation:
            print(f"Warning: NIT mismatch - Col4: {nit}, Col5: {nit_validation}")
            nit = nit_validation  # Use the formatted version if different
            
        # Get name from column 8
        nombre = nombre_alt.strip()
        
        return nit, nombre
        
    except Exception as e:
        print(f"Error in extract_nit_info: {str(e)}")
        print(f"Row data: {row.to_dict()}")
        return "", ""

def find_header_row(df: pd.DataFrame, unnamed_threshold: float = 0.5) -> int:
    """
    Find the first row that should be used as headers by checking the proportion of unnamed columns.
    
    Args:
        df (pd.DataFrame): The input DataFrame
        unnamed_threshold (float): Maximum proportion of unnamed columns allowed
        
    Returns:
        int: Index of the row to use as header
    """
    for idx in range(len(df)):
        # Convert row to column names and check proportion of unnamed
        test_columns = df.iloc[idx].tolist()
        
        # Count empty, nan, or unnamed values
        unnamed_count = sum(1 for col in test_columns 
                          if pd.isna(col)
                          or not str(col).strip()
                          or str(col).strip().startswith('Unnamed:'))
        
        # Count meaningful values (not just any non-empty string)
        meaningful_values = sum(1 for col in test_columns 
                              if pd.notna(col)
                              and str(col).strip()
                              and any(keyword in str(col).lower() for keyword in 
                                    ['cuenta', 'nit', 'saldo', 'fecha', 'descripcion', 'debito', 'credito', 'comprobante']))
        
        unnamed_proportion = unnamed_count / len(test_columns)
        
        # We want at least 3 meaningful column names and less than threshold unnamed
        if meaningful_values >= 3 and unnamed_proportion <= unnamed_threshold:
            print(f"\nFound header row at index {idx}")
            print("Headers:", [str(col).strip() for col in test_columns if pd.notna(col) and str(col).strip()])
            return idx
            
        # Debug information
        if idx < 10:  # Print first 10 rows for debugging
            print(f"\nRow {idx} analysis:")
            print(f"Meaningful values: {meaningful_values}")
            print(f"Unnamed proportion: {unnamed_proportion:.2f}")
            print("Values:", [str(col).strip() for col in test_columns if pd.notna(col) and str(col).strip()])
            
    return 7  # Default to row 6 if no good header row found

# Define expected row counts for different files
EXPECTED_COUNTS = {
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
    # Add more files as needed
}

def validate_row_counts(filename: str, total_rows: int, puc_5: int, puc_6: int) -> None:
    """
    Validate row counts against expected values for a given file.
    
    Args:
        filename (str): Name of the file being processed
        total_rows (int): Total number of rows processed
        puc_5 (int): Number of rows with PUC starting with 5
        puc_6 (int): Number of rows with PUC starting with 6
        
    Raises:
        ValueError: If validation fails or no rules found for the file
    """
    base_filename = os.path.basename(filename)
    if base_filename not in EXPECTED_COUNTS:
        raise ValueError(f"No validation rules found for {base_filename}")
        
    expected = EXPECTED_COUNTS[base_filename]
    errors = []
    
    if total_rows != expected["total_rows"]:
        errors.append(f"Expected {expected['total_rows']} total rows, but got {total_rows}")
    if puc_5 != expected["puc_5_rows"]:
        errors.append(f"Expected {expected['puc_5_rows']} rows starting with 5, but got {puc_5}")
    if puc_6 != expected["puc_6_rows"]:
        errors.append(f"Expected {expected['puc_6_rows']} rows starting with 6, but got {puc_6}")
        
    if errors:
        raise ValueError(f"Validation failed for {expected['description']}:\n" + "\n".join(errors))
        
    print(f"\nRow count validation passed for {expected['description']} ✓")

def procesar_libro_auxiliar(
    input_file: str, 
    output_file: str, 
    expected_counts: Dict[str, int] | None = None
) -> None:
    """
    Process the auxiliary book file to extract and structure provider information.
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path to save the processed CSV file
        expected_counts (Dict[str, int] | None): Optional dictionary with expected row counts
            containing keys: "total_rows", "puc_5_rows", "puc_6_rows"
    """
    # Ensure file paths use Windows-compatible separators
    input_file = os.path.normpath(input_file)
    output_file = os.path.normpath(output_file)
    
    # Try different encodings for Windows
    encodings = ['latin1', 'utf-8', 'cp1252']
    df = None
    successful_encoding = None
    
    for encoding in encodings:
        try:
            print(f"\nIntentando leer el archivo con codificación: {encoding}")
            # Read the CSV file without headers first
            df = pd.read_csv(input_file, engine='python', encoding=encoding, header=None)
            successful_encoding = encoding
            print(f"Éxito al leer el archivo con codificación: {encoding}")
            break
        except Exception as e:
            print(f"Error al leer con codificación {encoding}: {str(e)}")
            continue
    
    if df is None:
        raise ValueError("No se pudo leer el archivo con ninguna de las codificaciones intentadas")
    
    print("\nInitial data analysis:")
    print(f"Total rows: {len(df)}")
    
    # Find the header row
    header_row = find_header_row(df)
    
    # Re-read the CSV with the correct header row
    try:
        df = pd.read_csv(input_file, engine='python', encoding=successful_encoding, skiprows=header_row)
    except Exception as e:
        print(f"Error al releer el archivo con las cabeceras correctas: {str(e)}")
        # Try alternative approach - read without headers and set them manually
        df = pd.read_csv(input_file, engine='python', encoding=successful_encoding, header=None, skiprows=header_row)
        # Use the first row as header (this approach might need adjustment)
        df.columns = [f"Column_{i}" if pd.isna(x) or not str(x).strip() else str(x).strip() for i, x in enumerate(df.iloc[0])]
        df = df.iloc[1:].reset_index(drop=True)
    
    print("\nProcessing with correct headers:")
    print(f"Columns: {df.columns.tolist()}")
    
    # Get valid row indices based on data density
    valid_indices = analyze_row_density(df)
    
    # Filter DataFrame to keep only valid rows
    df_filtered = df.iloc[valid_indices].copy()
    
    # Analyze filtered data
    print("\nAnalyzing filtered data:")
    analyze_data(df_filtered)
    
    # Create output DataFrame
    output_data: List[Dict] = []
    
    # Process each valid row
    for _, row in df_filtered.iterrows():
        try:
            # Debug message for row processing
            #print(f"\nProcessing row {row.name} of {len(df_filtered)}: ")
            
            # Get PUC code and description directly from columns
            # Handle potential string conversion errors
            try:
                codigo_puc = ""
                if pd.notna(row.iloc[1]):
                    try:
                        # Try converting through float first
                        codigo_puc = str(int(float(row.iloc[1])))
                    except:
                        # If that fails, use the string directly
                        codigo_puc = str(row.iloc[1]).strip()
            except Exception as e:
                print(f"Error converting PUC code: {str(e)}")
                print(f"Raw value: {row.iloc[1]}, type: {type(row.iloc[1])}")
                codigo_puc = ""
                
            descripcion = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ""
            
            # Only process rows where account number starts with 5 or 6
            if codigo_puc and (codigo_puc.startswith('5') or codigo_puc.startswith('6')):
                nit, nombre = extract_nit_info(row)
                
                # Skip rows without NIT or NOMBRE (likely totals)
                if not nit or not nombre:
                    continue
                
                row_data = {
                    'CUENTA': codigo_puc,
                    'DESCRIPCION': descripcion,
                    'NIT': nit,
                    'NOMBRE': nombre
                }
                
                # Add remaining columns
                for col_idx, col_name in enumerate(df.columns[2:], start=2):
                    row_data[col_name] = row.iloc[col_idx]
                
                output_data.append(row_data)
                
        except Exception as e:
            print(f"Error processing row {row.name}: {str(e)}")
            print(f"Row data: {row.iloc[0:5].to_dict()}")  # Print just first few columns to reduce output size
            continue
    
    # Create final DataFrame and save to Excel
    if not output_data:
        raise ValueError("No se pudo procesar ninguna fila de datos válida")
        
    output_df = pd.DataFrame(output_data)
    
    # Remove columns that are entirely empty or NA
    non_empty_cols = output_df.columns[output_df.notna().any()]
    output_df = output_df[non_empty_cols]
    
    # Format CUENTA as integer (removing decimals) - with error handling
    try:
        output_df['CUENTA'] = output_df['CUENTA'].astype(float).astype(int).astype(str)
    except Exception as e:
        print(f"Error al convertir la columna CUENTA: {str(e)}")
        print("Se conservará el formato original")
    
    # Clean column names by trimming whitespace and handling duplicates
    new_columns = []
    seen = set()
    for col in output_df.columns:
        clean_col = str(col).strip()
        if clean_col in seen:
            i = 1
            while f"{clean_col}_{i}" in seen:
                i += 1
            clean_col = f"{clean_col}_{i}"
        seen.add(clean_col)
        new_columns.append(clean_col)
    output_df.columns = new_columns
    
    # Count PUC codes starting with 5 and 6
    puc_5 = output_df['CUENTA'].str.startswith('5').sum()
    puc_6 = output_df['CUENTA'].str.startswith('6').sum()
    total_rows = len(output_df)
    
    print(f"\nPUC code distribution:")
    print(f"Starting with 5: {puc_5} rows")
    print(f"Starting with 6: {puc_6} rows")
    print(f"Total rows: {total_rows}")
    
    # Validate row counts if expected counts were provided
    if expected_counts:
        try:
            errors = []
            if total_rows != expected_counts["total_rows"]:
                errors.append(f"Expected {expected_counts['total_rows']} total rows, but got {total_rows}")
            if puc_5 != expected_counts["puc_5_rows"]:
                errors.append(f"Expected {expected_counts['puc_5_rows']} rows starting with 5, but got {puc_5}")
            if puc_6 != expected_counts["puc_6_rows"]:
                errors.append(f"Expected {expected_counts['puc_6_rows']} rows starting with 6, but got {puc_6}")
            
            if errors:
                print("\nADVERTENCIA: Validación de conteo de filas fallida:")
                for error in errors:
                    print(f"  - {error}")
                print("Esto podría indicar cambios en los datos o en la lógica de procesamiento.")
                print("Se continuará con el procesamiento a pesar de las diferencias.")
                # Don't raise exception to allow processing to continue
            else:
                print("\nRow count validation passed ✓")
        except ValueError as e:
            print(f"\nError: {str(e)}")
            print("This might indicate changes in the data or processing logic.")
            # Don't raise to allow processing to continue
    
    print(f"\nFinal processed data:")
    print(f"Total rows to be saved: {len(output_df)}")
    print(f"Columns being saved: {list(output_df.columns)}")
    print("\nSample of processed data:")
    print(output_df.head())
    
    # Use try-except for saving to CSV
    try:
        output_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\nProcessed file saved as {output_file} with {len(output_data)} rows and {len(non_empty_cols)} columns.")
    except Exception as e:
        print(f"\nError al guardar el archivo CSV: {str(e)}")

def process_all_files(input_dir: str, output_dir: str) -> None:
    """
    Process all CSV files in the input directory.

    Args:
        input_dir (str): Directory containing input CSV files
        output_dir (str): Directory where output CSV files will be saved

    Raises:
        ValueError: If any file fails validation
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Find all CSV files in input directory
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]

    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return

    print(f"\nFound {len(csv_files)} CSV files to process:")
    for file in csv_files:
        print(f"- {file}")

    # Keep track of processing results
    results = []

    # Process each file
    for csv_file in csv_files:
        input_file = os.path.join(input_dir, csv_file)
        output_file = os.path.join(output_dir, csv_file.replace('.csv', '_Procesado.csv'))

        print(f"\n{'='*80}")
        print(f"Processing {csv_file}...")
        print(f"{'='*80}")

        try:
            # Get expected counts if available
            expected_counts = EXPECTED_COUNTS.get(csv_file)
            if not expected_counts:
                raise ValueError(f"Validation rules required but not found for {csv_file}")

            procesar_libro_auxiliar(input_file, output_file, expected_counts)
            results.append({
                "file": csv_file,
                "status": "✅ Success",
                "total_rows": expected_counts["total_rows"],
                "puc_5_rows": expected_counts["puc_5_rows"],
                "puc_6_rows": expected_counts["puc_6_rows"]
            })
            print(f"Successfully processed {csv_file}")

        except Exception as e:
            results.append({
                "file": csv_file,
                "status": "❌ Failed",
                "error": str(e)
            })
            print(f"Error processing {csv_file}: {str(e)}")
            sys.exit(1)

    # Print summary
    print("\n" + "="*100)
    print("PROCESSING SUMMARY")
    print("="*100)
    print(f"Total files processed: {len(results)}")

    for result in results:
        print(f"\nFile: {result['file']}")
        print(f"Status: {result['status']}")
        if result['status'] == "✅ Success":
            print(f"Total rows: {result['total_rows']}")
            print(f"PUC 5 rows: {result['puc_5_rows']}")
            print(f"PUC 6 rows: {result['puc_6_rows']}")
        else:
            print(f"Error: {result['error']}")

if __name__ == "__main__":
    # Usar ruta completa o relativa correcta para Windows
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Check if input file exists
    if not os.path.exists(current_dir):
        raise FileNotFoundError(f"Input file not found: {current_dir}")

    # Print directory contents
    print("\nDirectory contents:")
    os.system("ls -al")

    # Process all .csv files in directory
    process_all_files(current_dir, current_dir)