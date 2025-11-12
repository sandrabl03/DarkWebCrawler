#!/usr/bin/env python3
import os
import logging
import requests
import time
from requests.exceptions import RequestException
# Importar la clase del servidor para poder iniciarla en el main (opcional, pero útil para pruebas)
from persistence.neo_ingest_server import NeoIngestServer 

# ----------------- CONFIGURACIÓN GLOBAL -----------------
# Neo Controller (Cliente) Config
NEO_INGEST_URL = os.getenv("NEO_INGEST_URL", "http://127.0.0.1:9000/ingest_page")
NEO_INGEST_SECRET = os.getenv("NEO_INGEST_SECRET", "changeme")

# Configuración de Logging
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s %(levelname)s [NEO] %(message)s")

# --------------------------------------------------------
#               2. NeoController (Cliente)
# --------------------------------------------------------

class NeoController:
    """
    Controlador para la interacción cliente con el servicio de ingesta de Neo4j (API POST).
    Esta clase es utilizada por otros módulos (como TorController) para enviar datos.
    """
    def __init__(self):
        """
        Inicializa el controlador con la URL y el secreto de la API de ingesta.
        """
        self.ingest_url = NEO_INGEST_URL
        self.secret = NEO_INGEST_SECRET
        logging.info("NeoController (cliente) inicializado.")

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
            
            # Si el estado es 2xx, el POST fue exitoso.
            if resp.status_code >= 200 and resp.status_code < 300:
                logging.info("Ingesta de %s exitosa. Status: %d", payload["page"].get("url", "N/A"), resp.status_code)
            else:
                logging.warning("Ingesta de %s fallida. Status: %d. Respuesta: %s", payload["page"].get("url", "N/A"), resp.status_code, resp.text)
            
            return resp
            
        except RequestException:
            logging.exception("Error POST a neo_ingest (red/timeout). Revisar si el servidor está activo.", exc_info=False) 
            return None

# --------------------------------------------------------
#                   3. EJECUCIÓN PRINCIPAL
# --------------------------------------------------------

if __name__ == '__main__':
    # 1. Iniciar el servidor Flask en un hilo separado (Necesario para la prueba)
    # En un entorno de producción real, el servidor se ejecutaría de forma independiente.
    logging.info("Iniciando NeoIngestServer en hilo para la prueba de funcionalidad...")
    server = NeoIngestServer()
    server.start()
    
    # 2. Dar tiempo al servidor para que inicie (crucial para asegurar que el puerto esté abierto)
    logging.info("Esperando 3 segundos para que el servidor Flask se inicialice...")
    time.sleep(3) 

    # 3. Crear el controlador (cliente)
    neo_controller_instance = NeoController()
    
    # --- PRUEBA DE FUNCIONALIDAD (Ejemplo de uso) ---
    logging.info("Iniciando simulación de uso por TorController...")
    
    test_payload = {
        "page": {"url": "http://testonion.onion/1", "title": "Test Page Title", "crawl_date": time.time() * 1000},
        "links": [{"src_url": "http://testonion.onion/1", "dst_url": "http://linkto.onion", "anchor": "test link", "depth": 1, "crawl_date": time.time() * 1000}],
        "matched_terms": [
            {"page_url": "http://testonion.onion/1", "root": "test_root", "synonym": "test_syn1", "source": "title", "crawl_date": time.time() * 1000}
        ]
    }
    
    response = neo_controller_instance.post_page_payload(test_payload)
    
    if response and response.status_code == 200:
        logging.info("Prueba de ingesta exitosa. Neo4j debería tener un nuevo nodo.")
    else:
        logging.error("Prueba de ingesta fallida. Revisa los logs y la conexión a Neo4j.")

    # Si se ejecuta como script principal, mantenemos el hilo vivo
    logging.info("Simulación terminada. Servidor sigue en funcionamiento. Presiona Ctrl+C para salir.")
    while True:
        time.sleep(1)