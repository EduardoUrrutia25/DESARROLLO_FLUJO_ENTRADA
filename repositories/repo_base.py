import pyodbc
from typing import Sequence
#from config import DB_CONFIG
from config import ALL_DB_CONFIGS
from dtos import BaseHoldDTO

class BaseRepository:
    """
    Repositorio base que maneja la conexión y las transacciones.
    """
    _table_name: str = ""
    _columns: list[str] = []
    _decimal_map: dict[str, tuple[int, int]] = {} # Define map como atributo de clase

    #def __init__(self):
    def __init__(self, db_identifier: str):

        #print(f"\n*** ¡ÉXITO! El __init__ DE BASE_REPOSITORY se está ejecutando con el ID: {db_identifier} ***\n")
        
        if db_identifier not in ALL_DB_CONFIGS:
            raise ValueError(f"El identificador de base de datos '{db_identifier}' no es válido.")
        
        # 2. Selecciona la configuración correcta
        db_config = ALL_DB_CONFIGS[db_identifier]
        print(f"--- Repositorio conectando a: {db_config['database']} ---")

        #try:
            #self.conn = pyodbc.connect(**DB_CONFIG)
            #self.cursor = self.conn.cursor()
        #except Exception as e:
            #print(f"Error al conectar a la base de datos: {e}")
            #raise

        try:
            self.conn = pyodbc.connect(**db_config)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"Error al conectar a la base de datos '{db_config['database']}': {e}")
            raise

    def save_batch(self, dtos: Sequence[BaseHoldDTO]):
        if not dtos:
            return
        
        #query = f"""
        #    INSERT INTO {self._table_name} ({', '.join(self._columns)})
        #    VALUES ({', '.join(['?' for _ in self._columns])})
        #"""
        #params = [[getattr(dto, col) for col in self._columns] for dto in dtos]

        query = f"""
            INSERT INTO {self._table_name} ({', '.join(self._columns)})
            VALUES ({', '.join(['?' for _ in self._columns])})
        """
        params = [[getattr(dto, col) for col in self._columns] for dto in dtos]

        param_types = []

        for col_name in self._columns:
            if col_name in self._decimal_map:
                # Si la columna está en el mapa, define su tipo explícitamente
                precision, scale = self._decimal_map[col_name]
                param_types.append((pyodbc.SQL_DECIMAL, precision, scale))
            else:
                # Si no, deja que pyodbc adivine (para strings, ints, etc.)
                param_types.append(None)

        try:
            # 2b. Asignar los tipos ANTES de ejecutar
            self.cursor.setinputsizes(param_types)
            
            # 2c. Ejecutar la inserción
            self.cursor.fast_executemany = True
            self.cursor.executemany(query, params)
            self.conn.commit()
            print(f"Inserción de {len(params)} registros en '{self._table_name}' completada.")
        except pyodbc.DatabaseError as e:
            print(f"Error en la base de datos. Realizando rollback... {e}")
            self.conn.rollback()
            raise
        finally:
            self.cursor.close()
            self.conn.close()

#        try:
#            self.cursor.fast_executemany = True
#            self.cursor.executemany(query, params)
#            self.conn.commit()
#            print(f"Inserción de {len(params)} registros en '{self._table_name}' completada.")
#        except pyodbc.DatabaseError as e:
#            print(f"Error en la base de datos. Realizando rollback... {e}")
#            self.conn.rollback()
#            raise
#        finally:
#            self.cursor.close()
#            self.conn.close()