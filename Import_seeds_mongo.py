#!/usr/bin/env python3
"""
Carga seeds_with_terms.json que hemos generado en Ahmia_controller.py en MongoDB, 
colección `seeds` en la bbdd darkweb_tfg.
"""
"""
Estos son todos los imports necesarios para realizar la busqueda en ahmia con este codigo.
- os para interactuar con el sistema.
- json es para poder cargar nuestro archivo de sinonimos json y guardar los diversos json de output.
- datatime se usa para poder manejar marcas de tiempo o timestamps en los diversos documentos de MongoDB.
- pymongo importa dos:
    - MongoClient: para poder conectarse a MongoDB
    - UpdateOne: crear operaciones de actualizacion e insercion.

"""
import os
import json
from datetime import datetime
from pymongo import MongoClient, UpdateOne

"""
Aqui comienza la configuracion de las diversas variables.
"""
# --- CONFIGURACIÓN ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DBNAME = os.getenv("DBNAME", "darkweb_tfg")
SEEDS_COLL = os.getenv("SEEDS_COLL", "seeds")
SEEDS_FILE = os.getenv("SEEDS_FILE", "output_ahmia/seeds_with_terms.json")

# --- CONEXIÓN ---
"""
En esta seccion nos encargamos de establecer la conexión con la base de datos MongoDB.
"""
try:
    client = MongoClient(MONGO_URI)
    # Ping para verificar la conexión inmediatamente
    client.admin.command('ping')
    db = client[DBNAME]
    coll = db[SEEDS_COLL]
except Exception as e:
    print(f"[ERROR] No se pudo conectar a MongoDB en {MONGO_URI}. Asegúrate de que el servicio esté activo.")
    print(f"Detalle del error: {e}")
    raise SystemExit(1)

# --- CARGA DE DATOS ---
"""
Como ya tenemos conexion con la base de datos nos podemos poner ya con la carga de datos.
Por lo tanto, podemos cargar el json ya con todas las semillas al mongo.
"""
if not os.path.exists(SEEDS_FILE):
    print(f"[ERROR] No encontrado {SEEDS_FILE}")
    print("[HINT] Asegúrate de que el archivo de semillas de backup esté en la ruta correcta.")
    raise SystemExit(1)

print(f"[INFO] Cargando datos de {SEEDS_FILE}...")
with open(SEEDS_FILE, encoding='utf-8') as f:
    seeds = json.load(f)

# --- PREPARACIÓN DE OPERACIONES BULK ---
"""
Aqui vamos a iterar sobre las semillas y preparar todas las operaciones necesarias para la actualizacion y la insercion que posteriormente vamos a ejecutar
"""
ops = [] # Aqui almacenaremos las diversas operaciones
for s in seeds:
    # Intenta obtener la URL. Asumimos el formato 'http://host/'
    url = s.get("url")
    if not url:
        print(f"[WARN] Semilla sin URL válida encontrada: {s}")
        continue
    
    # 1. Definir el filtro de búsqueda, es decir el documento se identifica por la url q sera unica
    filter_query = {"url": url}
    
    # 2. Definir las operaciones de SET y SETONINSERT, es decir, las operaciones de modificacion del documento
    doc_operations = {
        # Campos que solo se escriben si el documento es NUEVO (insert)
        "$setOnInsert": {
            "url": url,
            "host": s.get("host"),
            "detected": s.get("detected", []),
            "created_at": datetime.utcnow(),
            "last_scraped": None,         
            "scrape_attempts": 0          
        },
        # Campos que se actualizan siempre
        "$set": {
            "status": "pending",        
            "updated_at": datetime.utcnow()
        }
    }
    
    # Añadir a la lista de operaciones bulk, crea un objeto UpdateOne con: 
    # - filter_query: El documento a encontrar.
    # - doc_operations: Las modificaciones a aplicar.
    # - upsert=True: Crucial. Indica a MongoDB que si no encuentra el documento, debe insertarlo (con $setOnInsert).
    ops.append(UpdateOne(filter_query, doc_operations, upsert=True))

# --- EJECUCIÓN BULK ---
"""
En esta seccion nos encargamos de ejecutar todas las operaciones preparadas anteriormente.
"""
if ops:
    print(f"[INFO] Preparadas {len(ops)} operaciones de carga/actualización.")
    try:
        res = coll.bulk_write(ops)
        print("\n--- Resultado de Bulk Write ---")
        print(f"Documentos Insertados: {res.upserted_count}")
        print(f"Documentos Actualizados: {res.matched_count - res.upserted_count}")
        print("------------------------------\n")
        
    except Exception as e:
        print(f"[ERROR] Falló la operación bulk_write: {e}")
        
else:
    print("[INFO] No se prepararon operaciones. El archivo de semillas podría estar vacío.")

# Nos indica que ya termino
print(f"[DONE] Proceso finalizado. Colección: {DBNAME}.{SEEDS_COLL}")
# Cierra la conexion para poder liberar los recursos.
client.close()