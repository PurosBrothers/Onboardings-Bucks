# Onboardings-Bucks

## Archivos requeridos para el flujo completo

Debemos asegurarnos de tener los siguientes archivos y carpetas antes de ejecutar el flujo de onboarding, debajo de cada archivo está la especificación de que información debe tener:

### Archivos Excel (XLSX)
Modelos de Causación
- `data/modelos_causacion/<modelo de causacion.xlsx>` 
    Suele ser más de un archivo
    **Debe contener:**
    - PUCs, Centros de costo, subcentros, valores, descripciones
    - NIT o identificación del tercero

### Archivos CSV
Producto
- `data/productos/<lista de productos>.csv`
    **Debe contener:**  
    - Nombre del producto
    - Descripción
    - Precio
    - Línea, grupo, unidad de medida
    - NIT de proveedores
Proveedor
- `data/modelos_terceros/<modelo de terceros>.csv`
    **Debe contener:**  
    - Responsabiilidad fiscal
    - Actividad económica
    - Ciudad (Opcional, pero preferible tenerla)
    - Razón Social
    - Sucursal
- `data/proveedores/<libro auxiliar>.csv` 
    Suele ser más de un archivo
    **Debe contener:** 
    - NIT del proveedor
    - Nombre del proveedor
    - Código PUC (cuenta contable)
    - Fecha, valor, descripción de la transacción
    - Otros campos relevantes según el formato contable

### Archivos ZIP
- Carpeta `data/facturas/` con los archivos ZIP de facturas (cada uno debe contener MÍNIMO los PDFs).
    **Cada ZIP debe contener:**  
    - Archivos PDF de las facturas
    - (Opcional) Archivos XML u otros soportes relacionados


### Otros
- Carpeta `results/` (se genera automáticamente para archivos procesados del Libro Auxiliar).

Si falta alguno de estos archivos, el flujo completo no funcionará correctamente.