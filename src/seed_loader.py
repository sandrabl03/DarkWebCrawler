#!/usr/bin/env python3
"""
Script de entrada para cargar seeds desde seeds_with_terms.json en MongoDB, 
utilizando la funcionalidad de carga masiva de la clase MongoController.
"""
import os
import sys
from pymongo.errors import ConnectionFailure
# Importamos MongoController para centralizar la lógica de DB
from mongo_controller import MongoController 

# --- CONFIGURACIÓN ---
# Usamos la misma variable de entorno por consistencia con el script original
SEEDS_FILE = os.getenv("SEEDS_FILE", "../output_ahmia/seeds_with_terms.json")

def main():
    """Función principal para ejecutar la carga."""
    mc = None
    try:
        # 1. Crea la instancia de MongoController, estableciendo la conexión
        mc = MongoController()
        
        print(f"[INFO] Intentando cargar semillas desde: {SEEDS_FILE}")
        
        # 2. Ejecuta la carga masiva usando el método centralizado
        loaded_count = mc.load_seeds_bulk(SEEDS_FILE)
        
        if loaded_count > 0:
            print(f"[DONE] Proceso de carga finalizado. {loaded_count} documentos procesados.")
        elif loaded_count == 0:
            print("[WARN] La carga finalizó sin documentos procesados.")
            
    except ConnectionFailure as e:
        # Capturamos el error de conexión si ocurre durante la inicialización
        print(f"[ERROR] No se pudo conectar a MongoDB. Asegúrate de que el servicio esté activo.")
        print(f"Detalle del error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Error inesperado en el cargador: {e}")
    finally:
        # 3. Asegura el cierre de la conexión
        if mc:
            mc.close()
            print("[INFO] Conexión a MongoDB cerrada.")


if __name__ == '__main__':
    main()