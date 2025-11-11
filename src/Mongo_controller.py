import os
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse

from pymongo import MongoClient, ReturnDocument

# --- CONFIGURACIÓN (Variables de Módulo) ---
# Se definen aquí y son accesibles para cualquier archivo que importe este módulo
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DBNAME = os.getenv("DBNAME", "darkweb_tfg")
SEEDS_COLL = os.getenv("SEEDS_COLL", "seeds")
STATS_COLL = os.getenv("STATS_COLL", "crawler_stats") 
RESET_INPROGRESS_OLDER_MIN = int(os.getenv("RESET_INPROGRESS_OLDER_MIN", "60"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [MONGO] %(message)s")

class Mongo_controller:
    """
    Controlador para toda la interacción con la base de datos MongoDB.
    Gestiona la cola de semillas (seeds) y las estadísticas de crawling.
    """
    def __init__(self):
        # 1. Configuración de la conexión
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[DBNAME]
        self.seeds_col = self.db[SEEDS_COLL]
        self.stats_col = self.db[STATS_COLL]
        logging.info("Conexión a MongoDB establecida.")

    def close(self):
        """Cierra la conexión de MongoDB."""
        self.client.close()
        logging.info("Conexión a MongoDB cerrada.")

    # --- Métodos de Estadísticas (Counter) ---
    
    def get_and_inc_processed_count(self):
        """Incrementa el contador global en MongoDB de forma atómica y devuelve el nuevo valor."""
        doc = self.stats_col.find_one_and_update(
            {"_id": "processed_pages_counter"},
            {"$inc": {"count": 1}, "$set": {"updated_at": datetime.utcnow()}},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        return doc['count']

    def get_current_processed_count(self):
        """Obtiene el valor actual del contador de páginas procesadas."""
        doc = self.stats_col.find_one({"_id": "processed_pages_counter"})
        return doc.get('count', 0) if doc else 0

    # --- Métodos de Semillas (Seeds) ---
    
    def reset_stale_inprogress(self):
        """Pone a pending las seeds 'in_progress' que fueron empezadas hace mucho."""
        # Usa la variable de MÓDULO definida arriba
        threshold = datetime.utcnow() - timedelta(minutes=RESET_INPROGRESS_OLDER_MIN)
        res = self.seeds_col.update_many(
            {"status": "in_progress", "last_try": {"$lt": threshold}},
            {"$set": {"status": "pending"}}
        )
        if res.modified_count:
            logging.info("Reset %d stale in_progress -> pending", res.modified_count)

    def pop_next_seed(self):
        """Obtiene una seed pendiente y la marca in_progress."""
        doc = self.seeds_col.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "in_progress", "last_try": datetime.utcnow()}, "$inc": {"attempts": 1}},
            sort=[("depth", 1), ("priority", -1), ("created_at", 1)],
            return_document=ReturnDocument.AFTER
        )
        return doc

    def mark_done(self, url, update_fields=None, discard_reason=None):
        """Marca la URL como 'ingested' o 'discarded'. Preserva el registro."""
        upd = {"$set": {"updated_at": datetime.utcnow()}}
        
        if discard_reason:
            upd["$set"]["status"] = "discarded"
            upd["$set"]["discard_reason"] = discard_reason
            logging.info("URL %s MARCADA como 'discarded' (Razón: %s).", url, discard_reason)
        else:
            upd["$set"]["status"] = "ingested"
        
        if update_fields:
            upd["$set"].update(update_fields)
            
        self.seeds_col.update_one({"url": url}, upd)

    def mark_failed(self, url, reason=None):
        """Marca la URL como fallida (alcanzó el máx. de reintentos)."""
        self.seeds_col.update_one(
            {"url": url}, 
            {"$set": {"status": "failed_perm", "failed_reason": reason, "updated_at": datetime.utcnow()}}
        )
        logging.info("Semilla %s MARCADA como 'failed_perm' (Razón: %s).", url, reason)

    def revert_to_pending(self, url):
        """Revierte una seed de 'in_progress' a 'pending' para reintento."""
        self.seeds_col.update_one(
            {"url": url}, 
            {"$set": {"status": "pending", "updated_at": datetime.utcnow()}}
        )

    def ensure_seed(self, url, detected=None, origin=None, depth=0):
        """Inserta una nueva seed si no existe, incluyendo la profundidad."""
        host = urlparse(url).hostname or url
        now = datetime.utcnow()
        doc = {
            "host": host,
            "url": url,
            "detected": detected or [], 
            "status": "pending",
            "attempts": 0,
            "priority": 0,
            "created_at": now,
            "updated_at": now,
            "depth": depth 
        }
        if origin:
            self.seeds_col.update_one({"url": url}, {"$addToSet": {"origins": origin}, "$setOnInsert": doc}, upsert=True)
        else:
            try:
                self.seeds_col.update_one({"url": url}, {"$setOnInsert": doc}, upsert=True)
            except Exception as e:
                logging.debug("ensure_seed upsert error: %s", e)

if __name__ == '__main__':
    # Esto es solo para probar el controlador de forma aislada, no se ejecuta en el flujo normal
    mc = Mongo_controller()
    print(f"Contador actual: {mc.get_current_processed_count()}")
    mc.reset_stale_inprogress()
    mc.close()