#!/usr/bin/env python3
# Este script inicia el servidor web Flask que recibe peticiones POST desde TorController
# y maneja la lógica de upsert en la base de datos Neo4j.

import os, logging
from flask import Flask, request, jsonify, abort
from neo4j import GraphDatabase, exceptions as neo4j_exceptions
from urllib.parse import urlparse

# ----------------- CONFIGURACIÓN -----------------
# Variables de Neo4j Driver
NEO_URI = os.getenv("NEO_URI", "bolt://127.0.0.1:7687")
NEO_USER = os.getenv("NEO_USER", "neo4j")
NEO_PASS = os.getenv("NEO_PASS", "test1234")

# Variables del Servidor Flask
API_SECRET = os.getenv("NEO_INGEST_SECRET", "changeme")
PORT = int(os.getenv("NEO_INGEST_PORT", "9000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
app = Flask(__name__)

# ----------------- INICIALIZACIÓN DEL DRIVER -----------------
try:
    # Intenta conectar el driver de Neo4j
    driver = GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PASS))
    driver.verify_connectivity()
    logging.info("Conexión a Neo4j establecida correctamente.")
except neo4j_exceptions.AuthError:
    logging.error("Error de autenticación: Verifica NEO_USER y NEO_PASS.")
    driver = None
except neo4j_exceptions.ServiceUnavailable:
    logging.error("Neo4j no está disponible en %s.", NEO_URI)
    driver = None
except Exception as e:
    logging.error("Error al inicializar Neo4j driver: %s", e)
    driver = None


# ----------------- UTIL: ensure constraints -----------------
def ensure_constraints():
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


# ----------------- NEO UPSERT WORK -----------------
def upsert_page_and_relations(payload):
    """Ejecuta las consultas Cypher para persistir la página, enlaces y términos."""
    if not driver:
        # Esto debería haber sido capturado en la inicialización, pero es una buena guardia
        raise Exception("Neo4j driver no está activo. Imposible guardar datos.")

    page = payload.get("page", {})
    links = payload.get("links", []) or []
    matched = payload.get("matched_terms", []) or []
    
    url = page.get("url", "")
    host = urlparse(url).hostname or url

    with driver.session() as s:
        # 1. MERGE Page node 
        s.run("""
        MERGE (p:Page {url: $url})
        ON CREATE SET p.title = $title,
                      p.text = $text,
                      p.html_file = $html_file,
                      p.html_file_path = $html_file_path,
                      p.html_file_url = $html_file_url,
                      p.host = $host,
                      p.has_html_content = true,
                      p.first_seen = coalesce($crawl_date,timestamp())
        ON MATCH SET p.title = CASE WHEN $title <> '' THEN $title ELSE p.title END,
                     p.text = CASE WHEN $text <> '' THEN $text ELSE p.text END,
                     p.updated_at = coalesce($crawl_date,timestamp())
        """, {
            "url": url,
            "host": host,
            "title": page.get("title",""),
            "text": page.get("text",""),
            "html_file": page.get("html_file",""),
            "html_file_path": page.get("html_file_path",""),
            "html_file_url": page.get("html_file_url",""),
            "crawl_date": page.get("crawl_date")
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

# ----------------- RUTAS FLASK -----------------
@app.route("/ingest_page", methods=["POST"])
def ingest_page():
    """Ruta para recibir el payload de ingesta del crawler."""
    key = request.headers.get("X-API-KEY")
    if key != API_SECRET:
        abort(403, description="Invalid API key")
    payload = request.get_json(silent=True)
    if not payload or "page" not in payload:
        abort(400, description="Invalid payload")
    try:
        upsert_page_and_relations(payload)
    except Exception as e:
        logging.exception("Neo upsert failed for URL: %s", payload["page"].get("url","")) 
        return jsonify({"status":"error", "detail": str(e)}), 500
    
    return jsonify({"status":"ok", "ingested_page": payload["page"].get("url","")})

@app.route("/health", methods=["GET"])
def health():
    """Ruta de chequeo de salud para verificar si el driver de Neo4j está conectado."""
    if not driver:
        return jsonify({"status":"error", "detail": "Neo4j Driver no está conectado."}), 503
    return jsonify({"status":"ok"})

# ----------------- MAIN SERVER START -----------------
if __name__ == "__main__":
    if driver:
        logging.info("Asegurando constraints en Neo4j...")
        ensure_constraints()
        
    logging.info(f"Starting Flask Neo ingest server on 0.0.0.0:{PORT}")
    # Esta línea es la que hace que el servidor escuche en el puerto 9000
    app.run(host="0.0.0.0", port=PORT, threaded=True)
