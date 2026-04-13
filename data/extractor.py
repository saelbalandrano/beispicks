import os
from typing import List, Dict, Any
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

class SupabaseExtractor:
    """
    Clase encargada de conectar con la base de datos de Supabase
    y extraer datos saltando los límites de paginación de la API.
    """

    def __init__(self):
        # Cargar variables del entorno (.env)
        load_dotenv()
        
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        
        if not url or not key:
            raise ValueError("Error: SUPABASE_URL o SUPABASE_KEY no están configuradas correctamente en el archivo .env")
            
        # Inicializar el cliente
        self.client: Client = create_client(url, key)

    def get_full_table(self, table_name: str, batch_size: int = 1000) -> pd.DataFrame:
        """
        Descarga una tabla completa sorteando los límites de paginación.
        Supabase por defecto limita a 1000 filas por consulta.
        
        :param table_name: Nombre de la tabla a extraer.
        :param batch_size: Cantidad de registros por lote. Máximo sugerido 1000.
        :return: DataFrame de Pandas con la data recolectada.
        """
        all_records: List[Dict[str, Any]] = []
        start = 0
        
        print(f"Iniciando extracción de la tabla: '{table_name}'...")
        
        while True:
            # Los rangos en la API de Supabase/PostgREST son inclusivos [start, end]
            end = start + batch_size - 1
            
            try:
                response = self.client.table(table_name) \
                    .select("*") \
                    .range(start, end) \
                    .execute()
                
                data = response.data
                
                # Si no hay data o la lista está vacía, terminamos el ciclo
                if not data:
                    break
                    
                all_records.extend(data)
                
                # Si la cantidad de registros retornados es menor al batch_size,
                # significa que hemos llegado al final de la tabla.
                if len(data) < batch_size:
                    break
                    
                # Avanzamos a la siguiente "página"
                start += batch_size
                
            except Exception as e:
                print(f"Error extrayendo datos en el rango {start}-{end}: {e}")
                break
                
        print(f"Extracción finalizada. {len(all_records)} registros obtenidos de '{table_name}'.")
        return pd.DataFrame(all_records)

# --- Ejemplo de uso si ejecutas directamente este archivo ---
if __name__ == "__main__":
    try:
        extractor = SupabaseExtractor()
        # print("Conectado! Intentemos extraer una tabla.")
        # df = extractor.get_full_table("nombre_de_tu_tabla")
        # print(df.head())
    except ValueError as e:
        print(e)
