#!/usr/bin/env python3
import os
import logging
import requests
import threading
import time
from requests.exceptions import RequestException
from flask import Flask, request, jsonify, abort
from neo4j import GraphDatabase, exceptions as neo4j_exceptions
from urllib.parse import urlparse

# ----------------- CONFIGURACIÓN GLOBAL -----------------
# Neo Controller (Cliente) Config
NEO_INGEST_URL = os.getenv("NEO_INGEST_URL", "http://127.0.0.1:9000/ingest_page")
NEO_INGEST_SECRET = os.getenv("NEO_INGEST_SECRET", "changeme")

# Neo Ingest Server (Servidor Flask) Config
NEO_URI = os.getenv("NEO_URI", "bolt://127.0.0.1:7687")
NEO_USER = os.getenv("NEO_USER", "neo4j")
NEO_PASS = os.getenv("NEO_PASS", "test1234")
API_SECRET = os.getenv("NEO_INGEST_SECRET", "changeme") # Usa la misma variable de entorno
PORT = int(os.getenv("NEO_INGEST_PORT", "9000"))

# Configuración de Logging
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s %(levelname)s [NEO] %(message)s")

# Variables Globales (Compartidas entre clases)
driver = None
app = Flask(__name__)

# --------------------------------------------------------
#               1. NeoIngestServer (Servidor)
# --------------------------------------------------------

class NeoIngestServer(threading.Thread):
    """
    Encapsula el servidor Flask y la lógica de base de datos Neo4j.
    Hereda de threading.Thread para ejecutarse en segundo plano.
    """
    def __init__(self, host="0.0.0.0", port=PORT):
        super().__init__()
        self.host = host
        self.port = port
        self.daemon = True # Permite que el hilo termine cuando el programa principal lo haga
        self._initialize_driver()
        self._setup_flask_routes()

    def _initialize_driver(self):
        """Inicializa el driver de Neo4j y verifica la conexión."""
        global driver
        try:
            driver = GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PASS))
            driver.verify_connectivity()
            logging.info("Conexión a Neo4j establecida correctamente.")
            self._ensure_constraints()
        except neo4j_exceptions.AuthError:
            logging.error("Error de autenticación: Verifica NEO_USER y NEO_PASS.")
            driver = None
        except neo4j_exceptions.ServiceUnavailable:
            logging.error("Neo4j no está disponible en %s.", NEO_URI)
            driver = None
        except Exception as e:
            logging.error("Error al inicializar Neo4j driver: %s", e)
            driver = None

    def _ensure_constraints(self):
        """Asegura que los constraints de unicidad existan en Neo4j."""
        if not driver:
            logging.error("No se puede asegurar constraints: Driver no inicializado.")
            return
            
        try:
            with driver.session() as s:
                s.run("CREATE CONSTRAINT page_url_unique IF NOT EXISTS FOR (p:Page) REQUIRE p.url IS UNIQUE")
                s.run("CREATE CONSTRAINT term_name_unique IF NOT EXISTS FOR (t:Term) REQUIRE t.name IS UNIQUE")
                s.run("CREATE CONSTRAINT synonym_name_unique IF NOT EXISTS FOR (s:Synonym) REQUIRE s.name IS UNIQUE")
            logging.info("Constraints creadas/aseguradas.")
        except Exception as e:
            logging.error("Error al crear constraints: %s", e)

    def _upsert_page_and_relations(self, payload):
        """Ejecuta las consultas Cypher para persistir la página, enlaces y términos."""
        if not driver:
            raise Exception("Neo4j driver no está activo. Imposible guardar datos.")

        page = payload.get("page", {})
        links = payload.get("links", []) or []
        matched = payload.get("matched_terms", []) or []
        
        url = page.get("url", "")
        host = urlparse(url).hostname or url

        # Lógica Cypher (MISMA LÓGICA DE TU CÓDIGO ANTERIOR)
        with driver.session() as s:
            # 1. MERGE Page node 
            s.run("""
            MERGE (p:Page {url: $url})
            ON CREATE SET p.title = $title, p.text = $text, p.html_file = $html_file,
                          p.html_file_path = $html_file_path, p.html_file_url = $html_file_url,
                          p.host = $host, p.has_html_content = true,
                          p.first_seen = coalesce($crawl_date,timestamp())
            ON MATCH SET p.title = CASE WHEN $title <> '' THEN $title ELSE p.title END,
                         p.text = CASE WHEN $text <> '' THEN $text ELSE p.text END,
                         p.updated_at = coalesce($crawl_date,timestamp())
            """, {
                "url": url, "host": host, "title": page.get("title",""), "text": page.get("text",""),
                "html_file": page.get("html_file",""), "html_file_path": page.get("html_file_path",""),
                "html_file_url": page.get("html_file_url",""), "crawl_date": page.get("crawl_date")
            })
            
            # 2. UNWIND links -> create LINKS_TO 
            if links:
                s.run("""
                UNWIND $rows AS r
                MERGE (a:Page {url: r.src_url})
                MERGE (b:Page {url: r.dst_url})
                ON CREATE SET b:Seed, b.first_seen_as_link = coalesce(r.crawl_date, timestamp())
                MERGE (a)-[rel:LINKS_TO {anchor: r.anchor, depth: r.depth, crawl_date: r.crawl_date}]->(b)
                ON CREATE SET rel.first_seen = coalesce(r.crawl_date, timestamp())
                """, {"rows": links})

            # 3. UNWIND matched_terms -> CREACIÓN DE TÉRMINOS Y RELACIONES MENTIONS
            if matched:
                s.run("""
                UNWIND $rows AS r
                MERGE (t:Term {name: r.root})
                MERGE (s:Synonym {name: r.synonym})
                MERGE (s)-[:IS_SYNONYM_OF]->(t)
                MERGE (p:Page {url: r.page_url})
                MERGE (p)-[m:MENTIONS {source: r.source, root: r.root}]->(s)
                ON CREATE SET m.first_seen = coalesce(r.crawl_date, timestamp()), m.count = 1
                ON MATCH SET m.count = m.count + 1 
                """, {"rows": matched})

    def _setup_flask_routes(self):
        """Define las rutas del servidor Flask."""
        
        @app.route("/ingest_page", methods=["POST"])
        def ingest_page():
            key = request.headers.get("X-API-KEY")
            if key != API_SECRET:
                # El servidor verifica la clave API
                abort(403, description="Invalid API key")
            payload = request.get_json(silent=True)
            if not payload or "page" not in payload:
                abort(400, description="Invalid payload")
            try:
                # Llama al método de persistencia
                self._upsert_page_and_relations(payload)
            except Exception as e:
                logging.exception("Neo upsert failed for URL: %s", payload["page"].get("url","")) 
                return jsonify({"status":"error", "detail": str(e)}), 500
            
            return jsonify({"status":"ok", "ingested_page": payload["page"].get("url","")})

        @app.route("/health", methods=["GET"])
        def health():
            """Ruta de chequeo de salud."""
            if not driver:
                return jsonify({"status":"error", "detail": "Neo4j Driver no está conectado."}), 503
            return jsonify({"status":"ok"})

    def run(self):
        """Método principal del Thread, inicia el servidor Flask."""
        logging.info(f"Starting Flask Neo ingest server in background on {self.host}:{self.port}")
        # flask run() inicia el servidor
        # El parámetro `debug=False` es crucial en entornos de producción. 
        # `use_reloader=False` previene que Flask se inicie dos veces.
        try:
            app.run(host=self.host, port=self.port, threaded=True, debug=False, use_reloader=False)
        except Exception as e:
            logging.error("Failed to start Flask server: %s", e)

# --------------------------------------------------------
#               2. NeoController (Cliente)
# --------------------------------------------------------

class NeoController:
    """
    Controlador para la interacción cliente con el servicio de ingesta de Neo4j (API POST).
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
            
        except RequestException as e:
            logging.exception("Error POST a neo_ingest (red/timeout).", exc_info=False) # exc_info=False para logs más limpios
            return None

# --------------------------------------------------------
#                   3. EJECUCIÓN PRINCIPAL
# --------------------------------------------------------

if __name__ == '__main__':
    # 1. Iniciar el servidor Flask en un hilo separado (Ejecución paralela)
    server = NeoIngestServer()
    server.start()
    
    # 2. Dar tiempo al servidor para que inicie (opcional, pero útil)
    logging.info("Esperando 3 segundos para que el servidor Flask se inicialice...")
    time.sleep(3) 

    # 3. Crear el controlador que será usado por TorController
    neo_controller_instance = NeoController()
    
    # --- PRUEBA DE FUNCIONALIDAD (Ejemplo de uso) ---
    logging.info("Iniciando simulación de uso por TorController...")
    
    test_payload = {
        "page": {"url": "http://testonion.onion/1", "title": "Test Page Title", "crawl_date": time.time() * 1000},
        "links": [{"src_url": "http://testonion.onion/1", "dst_url": "http://linkto.onion", "anchor": "test link"}],
        "matched_terms": [
            {"page_url": "http://testonion.onion/1", "root": "test_root", "synonym": "test_syn1", "source": "title"}
        ]
    }
    
    response = neo_controller_instance.post_page_payload(test_payload)
    
    if response and response.status_code == 200:
        logging.info("Prueba de ingesta exitosa. Neo4j debería tener un nuevo nodo.")
    else:
        logging.error("Prueba de ingesta fallida. Revisa los logs y la conexión a Neo4j.")

    # En un entorno real, aquí es donde TorController.py importaría e iniciaría NeoController.
    # Como es un script de prueba, simplemente lo dejamos correr.
    # Puedes usar Ctrl+C para detener ambos, el hilo principal y el servidor Flask (gracias a `self.daemon = True`).
    
    # Si quisieras que el script principal no termine inmediatamente (como lo haría TorController), 
    # podrías usar un bucle infinito aquí para mantener el proceso principal vivo:
    # while True:
    #     time.sleep(1)