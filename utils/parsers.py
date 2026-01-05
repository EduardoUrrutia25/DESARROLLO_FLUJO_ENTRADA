from decimal import Decimal, ConversionSyntax, InvalidOperation
from datetime import datetime
from uuid import UUID
from typing import List, Optional


def to_bool(value: str) -> bool:
    """Convierte de forma segura un string a booleano."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return value.strip().lower() in ['true', '1', 't', 'y', 'yes']


def parse_datetime_safely(date_string: str) -> datetime:
    """
    Intenta convertir un string a datetime, manejando tanto formatos de solo fecha
    como de fecha y hora. Si falla, devuelve una fecha mínima por defecto.
    """
    if not date_string or date_string.strip().lower() in ['00:00.0', 'null']:
        return datetime(1900, 1, 1)
    
    # Quita espacios en blanco al inicio y al final
    clean_date_string = date_string.strip()

    try:
        # 1. Intenta parsear el formato más completo primero (con fecha y hora)
        # El formato '%f' maneja milisegundos.
        return datetime.strptime(clean_date_string, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        try:
            # 2. Si falla, intenta parsear solo la parte de la fecha
            return datetime.strptime(clean_date_string, '%Y-%m-%d')
        except ValueError:
            # 3. Si ambos fallan, devuelve la fecha por defecto
            return datetime(1900, 1, 1)
        

def to_decimal_safely(value: str) -> Optional[Decimal]: 
    """Si el valor es inválido, devuelve None."""
    if not value or value.strip().lower() == 'null':
        return None
    try:
        return Decimal(value.strip())
    except InvalidOperation:
        return None
    
    
#def to_decimal_safely(value: Optional[str]) -> Optional[Decimal]: 
#    """
#    Si el valor es inválido o 'null', devuelve None.
#    Si el valor es un string vacío, devuelve 0.
#    """
#    
#    # 1. Manejar None o el string 'null'
#    if value is None or value.strip().lower() == 'null':
#        return None
#        
#    # 2. Manejar string vacío (como '' o ' ')
#    if not value.strip():
#        return Decimal('0')
#        
#    # 3. Intentar convertir el resto (ej. '123.45')
#    try:
#        return Decimal(value.strip())
#    except InvalidOperation: # Para valores inválidos como 'abc'
#        return None
    
    
def to_str_safely(value: str) -> Optional[str]:
    """Si el valor es 'NULL' o vacío, devuelve None. Sino, el string."""
    if not value or value.strip().lower() == 'null':
        return ''
    return value.strip()


def to_str_or_empty(value: str) -> str:
    """Si el valor es 'NULL' o vacío, devuelve una cadena vacía ('')."""
    if not value or value.strip().lower() == 'null':
        return ''
    return value.strip()


def to_str_or_none(value: str) -> Optional[str]:
    """Si el valor es 'NULL' o vacío, devuelve None."""
    if not value or value.strip().lower() == 'null':
        return None
    return value.strip()

    
def to_int_safely(value: str) -> Optional[int]:
    """
    Intenta convertir un string a int. Si está vacío, es 'NULL'
    o tiene un formato inválido, devuelve 0.
    """
    if not value or value.strip().lower() == 'null':
        return None
    try:
        return int(float(value.strip()))
    except (ValueError, TypeError):
        return None
    

def to_uuid_safely(value: str) -> Optional[UUID]:
    """
    Intenta convertir un string a un objeto UUID.
    Si el string es inválido, vacío o 'NULL', devuelve None.
    """
    if not value or value.strip().lower() == 'null':
        return None
    try:
        # El constructor de UUID valida el formato del string
        return UUID(value.strip())
    except ValueError:
        # Se produce si el string no es un UUID válido
        print(f"ADVERTENCIA: El valor '{value}' no es un UUID válido. Se guardará como NULL.")
        return None