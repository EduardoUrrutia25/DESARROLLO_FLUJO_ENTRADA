import asyncio
import pyodbc
from config import ALL_DB_CONFIGS
from typing import Optional, List, Any

#async def execute_query(org: str, query: str, params: dict) -> Optional[List[Any]]:
async def execute_query(org: str, query: str, params: dict) -> Any:
    """
    Ejecuta una consulta de solo lectura de forma asíncrona en un hilo separado.
    """
    if org not in ALL_DB_CONFIGS:
        raise ValueError(f"Identificador de BD '{org}' no válido.")
    
    db_config = ALL_DB_CONFIGS[org]
    
    # pyodbc es síncrono, por lo que lo ejecutamos en un hilo
    # para no bloquear el bucle de eventos de asyncio
    def _run_sync_query():
        try:
            with pyodbc.connect(**db_config) as conn:
                cursor = conn.cursor()
                # Ajusta la consulta para pyodbc (usa '?' en lugar de ':key')
                query_params = list(params.values())
                cursor.execute(query, query_params)
                row = cursor.fetchone()
                return row
        except Exception as e:
            print(f"Error en consulta async: {e}")
            return None

    row = await asyncio.to_thread(_run_sync_query)
    
    return row