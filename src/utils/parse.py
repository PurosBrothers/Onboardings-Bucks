def parse_price(value_str):
    """
    Convierte una cadena con separadores de miles (coma) y punto decimal
    en un float.  Ej: "25,000.00000" -> 25000.0
    """
    cleaned = value_str.replace(',', '')
    try:
        return float(cleaned)
    except ValueError:
        return 0.0