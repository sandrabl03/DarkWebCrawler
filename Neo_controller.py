# neo_controller.py

import os
import logging
import requests
from requests.exceptions import RequestException

# --- CONFIGURACIÓN (Variables de Módulo) ---
# Lee la URL y el secreto de las variables de entorno o usa valores por defecto
NEO_INGEST_URL = os.getenv("NEO_INGEST_URL", "http://127.0.0.1:9000/ingest_page")
NEO_INGEST_SECRET = os.getenv("NEO_INGEST_SECRET", "changeme")

# Configuración de logging específica para este módulo
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [NEO] %(message)s")

class NeoController:
    """
    Controlador para la interacción con el servicio de ingesta de Neo4j (API POST).
    Encapsula la configuración y el método para enviar el payload de datos.
    """
    def __init__(self):
        """
        Inicializa el controlador con la URL y el secreto de la API de ingesta.
        """
        self.ingest_url = NEO_INGEST_URL
        self.secret = NEO_INGEST_SECRET
        logging.info("NeoController inicializado. URL de ingesta: %s", self.ingest_url)

    def post_page_payload(self, payload):
        """
        Envía el payload JSON (página, enlaces, términos) al servicio de ingesta.

        Args:
            payload (dict): El diccionario de datos a enviar.

        Returns:
            requests.Response o None: El objeto respuesta si tiene éxito, o None si falla.
        """
        headers = {"X-API-KEY": self.secret, "Content-Type": "application/json"}
        try:
            # Realiza la llamada POST a la API de ingesta
            resp = requests.post(self.ingest_url, json=payload, headers=headers, timeout=30)
            
            # Devuelve el objeto Response, incluso si es un error HTTP (ej. 400)
            return resp
            
        except RequestException as e:
            # Captura errores de red, DNS o timeout
            logging.exception("Error POST a neo_ingest (red/timeout): %s", e)
            return None

if __name__ == '__main__':
    # Bloque de prueba simple para verificar la inicialización del controlador
    # (No debe ejecutarse en el flujo normal de TorController)
    test_controller = NeoController()
    logging.info("Prueba de NeoController completada. Listo para la ingesta.")