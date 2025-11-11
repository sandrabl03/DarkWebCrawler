#!/usr/bin/env python3
"""
Carga seeds_with_terms.json en MongoDB, colección 'seeds' en la bbdd 'darkweb_tfg', 
utilizando una clase SeedLoader para encapsular la lógica y la conexión.
"""

import os
import json
import sys
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, BulkWriteError

# --- CONFIGURACIÓN ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DBNAME = os.getenv("DBNAME", "darkweb_tfg")
SEEDS_COLL = os.getenv("SEEDS_COLL", "seeds")
SEEDS_FILE = os.getenv("SEEDS_FILE", "../output_ahmia/seeds_with_terms.json")

# ---------------- CLASE SEED LOADER ----------------
class SeedLoader:
    """
    Controlador para manejar la conexión a MongoDB y la carga de semillas 
    desde un archivo JSON usando operaciones bulk (upsert).
    """
    def __init__(self, uri=MONGO_URI, dbname=DBNAME, coll_name=SEEDS_COLL, file_path=SEEDS_FILE):
        """Inicializa la conexión a MongoDB."""
        self.file_path = file_path
        self.client = None
        self.coll = None
        
        print(f"[INFO] Intentando conectar a MongoDB en {uri}...")
        try:
            self.client = MongoClient(uri)
            self.client.admin.command('ping')
            self.coll = self.client[dbname][coll_name]
            print(f"[INFO] Conexión a MongoDB exitosa. Colección: {dbname}.{coll_name}")
        except ConnectionFailure as e:
            print(f"[ERROR] No se pudo conectar a MongoDB. Asegúrate de que el servicio esté activo.")
            print(f"Detalle del error: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"[ERROR] Error al inicializar SeedLoader: {e}")
            sys.exit(1)

    def _load_seeds_from_file(self):
        """Carga el array de semillas desde el archivo JSON."""
        if not os.path.exists(self.file_path):
            print(f"[ERROR] No encontrado {self.file_path}")
            print("[HINT] Asegúrate de que el archivo de semillas esté en la ruta correcta.")
            sys.exit(1)

        print(f"[INFO] Cargando datos de {self.file_path}...")
        try:
            with open(self.file_path, encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"[ERROR] El archivo {self.file_path} no es un JSON válido.")
            sys.exit(1)
            
    def _prepare_bulk_operations(self, seeds):
        """Itera sobre las semillas y prepara las operaciones UpdateOne para la carga bulk."""
        ops = []
        for s in seeds:
            url = s.get("url")
            if not url:
                print(f"[WARN] Semilla sin URL válida encontrada, omitiendo: {s}")
                continue
            
            filter_query = {"url": url}
            
            doc_operations = {
                # Campos que solo se escriben si el documento es NUEVO ($setOnInsert)
                "$setOnInsert": {
                    "url": url,
                    "host": s.get("host"),
                    "created_at": datetime.utcnow(),
                    "last_scraped": None,         
                    "scrape_attempts": 0,
                    "depth": 0 # Inicializamos la profundidad en 0 para las seeds de Ahmia
                },
                # Campos que se actualizan siempre
                "$set": {
                    "detected": s.get("detected", []),
                    "status": "pending",        
                    "updated_at": datetime.utcnow()
                }
            }
            
            ops.append(UpdateOne(filter_query, doc_operations, upsert=True))
        
        return ops

    def execute_load(self):
        """Función principal para cargar las semillas."""
        seeds = self._load_seeds_from_file()
        ops = self._prepare_bulk_operations(seeds)

        if not ops:
            print("[INFO] No se prepararon operaciones. El archivo de semillas podría estar vacío.")
            return

        print(f"[INFO] Preparadas {len(ops)} operaciones de carga/actualización.")
        try:
            res = self.coll.bulk_write(ops, ordered=False)
            
            print("\n--- Resultado de Bulk Write ---")
            print(f"Documentos Insertados: {res.upserted_count}")
            # Documentos Matched: Total de documentos que coincidieron (actualizados o no)
            # Documentos Actualizados: Documentos que existían y cuyos campos $set fueron modificados.
            print(f"Documentos Actualizados (matched): {res.matched_count}") 
            print("------------------------------\n")
            
        except BulkWriteError as e:
            print(f"[ERROR] Falló la operación bulk_write (parcial): {e.details}")
        except Exception as e:
            print(f"[ERROR] Falló la operación bulk_write (general): {e}")

    def close(self):
        """Cierra la conexión a MongoDB."""
        if self.client:
            self.client.close()
            print(f"[DONE] Proceso finalizado. Colección: {DBNAME}.{SEEDS_COLL}")


# ---------------- PUNTO DE ENTRADA ----------------
if __name__ == '__main__':
    try:
        # 1. Crea la instancia, estableciendo la conexión
        loader = SeedLoader() 
        # 2. Ejecuta la carga
        loader.execute_load()
        
    except SystemExit:
        # Se captura el SystemExit generado si hay errores críticos (ej. no JSON, no file)
        pass 
    except Exception as e:
        print(f"[ERROR] Error inesperado en el cargador: {e}")
    finally:
        # Asegura el cierre de la conexión al finalizar o en caso de error
        if 'loader' in locals():
            loader.close()