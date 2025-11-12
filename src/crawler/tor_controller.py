#!/usr/bin/env python3

import os
import time
import re
import hashlib
import logging
import signal
import sys
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from requests.exceptions import RequestException, ConnectionError, Timeout
from bs4 import BeautifulSoup

# Importamos los controladores de los otros python.
from persistence.mongo_controller import MongoController, RESET_INPROGRESS_OLDER_MIN 
from persistence.neo_controller import NeoController
from persistence.neo_ingest_server import NeoIngestServer 

# Logging simple (el resto de módulos usan el suyo propio)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [TOR] %(message)s")


# ---------------- CLASE TOR CONTROLLER ----------------
class TorController:
    """
    Motor principal del crawler. Encapsula la lógica de fetching, filtrado,
    extracción de enlaces y coordinación con MongoController y NeoController.
    """
    def __init__(self):
        # --- Configuración ---
        self.tor_timeout = float(os.getenv("TOR_TIMEOUT", "60.0"))
        self.proxies = {'http': 'socks5h://127.0.0.1:9050', 'https': 'socks5h://127.0.0.1:9050'}
        self.user_agents = [os.getenv("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36")]
        self.min_text_chars = int(os.getenv("MIN_TEXT_CHARS", "1200"))
        
        # --- Límites ---
        self.max_pages_to_fetch = sys.maxsize
        self.max_depth = 3
        
        self.sleep = float(os.getenv("SLEEP", "3.5"))
        self.max_attempts = int(os.getenv("MAX_ATTEMPTS", "4"))
        self.onion_re = re.compile(r'\b([a-z2-7]{16,56}\.onion)\b', re.IGNORECASE)
        
        self.running = True
        
        # --- Inicialización de Controladores ---
        self.mongo_db = MongoController()

        # 1. INICIAR EL SERVIDOR DE INGESTA DE NEO4J EN UN HILO
        self.neo_server = NeoIngestServer()
        self.neo_server.start()
        
        # 2. ESPERAR UN MOMENTO para que el servidor Flask se levante
        logging.info("Iniciando servidor Neo4j en segundo plano. Esperando 3 segundos...")
        time.sleep(3)
        
        # 3. Inicializar el CLIENTE NeoController
        self.neo_db = NeoController()
        
        # Manejo de señal
        signal.signal(signal.SIGINT, self.handle_sigint)
        
        logging.info("TorController inicializado. Conexiones a DB y Servidor Neo activo.")

    def handle_sigint(self, signum, frame):
        """Maneja la señal SIGINT para una parada ordenada."""
        logging.info("SIGINT recibido: preparando parada ordenada...")
        self.running = False
        
    # --- MÉTODOS DE CRAWLING ---
    
    def fetch_via_tor(self, url): 
        """Realiza la petición HTTP a través de Tor."""
        headers = {'User-Agent': self.user_agents[0]}
        try:
            r = requests.get(url, headers=headers, proxies=self.proxies, timeout=self.tor_timeout)
            r.raise_for_status() 
            return r
        except (Timeout, ConnectionError, RequestException) as e:
            logging.debug("fetch error %s para %s", type(e).__name__, url)
        except Exception as e:
            logging.debug("fetch error Inesperado %s : %s", url, e)
        return None

    def sanitize_html(self, raw_html):
        """Sanitiza el HTML para almacenamiento seguro y análisis de texto."""
        try:
            soup = BeautifulSoup(raw_html, "lxml")
        except Exception:
            soup = BeautifulSoup(raw_html, "html.parser")

        # 1. Eliminar tags peligrosos (scripts, estilos, contenido incrustado, forms)
        for tag_name in ['script', 'style', 'noscript', 'iframe', 'form', 'object', 'embed']:
            for t in soup.find_all(tag_name):
                t.decompose()

        # 2. Neutralizar imágenes: reemplazarlas por un placeholder textual
        for img in soup.find_all('img'):
            alt = img.get('alt', '[imagen]')
            img.replace_with(f" [IMG: {alt}] ")

        # 3. Neutralizar enlaces: reemplazarlos por su texto interno (no clicable)
        for a in soup.find_all('a'):
            link_content = a.contents 
            if link_content:
                # Reemplazar el tag <a> por su contenido (texto)
                a.replace_with(*link_content) 
            else:
                a.decompose()

        # 4. Eliminar meta refresh
        for meta in soup.find_all('meta'):
            if meta.get('http-equiv','').lower() == 'refresh':
                meta.decompose()

        # 5. Eliminar atributos peligrosos/innecesarios
        event_attrs = re.compile(r'^on', re.IGNORECASE)
        attrs_to_remove = ('style', 'src', 'srcset', 'href', 'data') 
        
        for tag in soup.find_all(True):
            remove_attrs = []
            for attr in list(tag.attrs.keys()):
                attr_lower = attr.lower()
                if event_attrs.match(attr) or attr_lower in attrs_to_remove:
                    remove_attrs.append(attr)
            
            for a in remove_attrs:
                try:
                    del tag.attrs[a]
                except Exception:
                    pass

        # 6. Ensamblaje del HTML limpio con CSP de seguridad
        safe_body = soup.body or soup
        safe_html = f"""<!doctype html>
            <html>
            <head>
            <meta charset="utf-8"/>
            <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'self' 'unsafe-inline';">
            <title>Sanitized snapshot</title>
            </head>
            <body>
            <p><em>Snapshot sanitized — no scripts, iframes, forms, src/href removed.</em></p>
            {str(safe_body)}
            </body>
            </html>
            """
        return safe_html


    # --- MÉTODO PRINCIPAL ----
    
    def start_crawling(self):
        """Bucle principal del crawler."""
        
        logging.info("Resetting stale in_progress seeds older than %d minutes...", RESET_INPROGRESS_OLDER_MIN)
        self.mongo_db.reset_stale_inprogress()

        while self.running:
            
            # 1. Chequeo del límite 
            current_count = self.mongo_db.get_current_processed_count() 
            if current_count >= self.max_pages_to_fetch:
                 logging.warning(f"Límite de {self.max_pages_to_fetch} páginas alcanzado ({current_count}). DETENIENDO CRAWLER.")
                 break 

            seed = self.mongo_db.pop_next_seed()
            if not seed:
                logging.info("No pending seeds. Esperando %s s...", self.sleep)
                time.sleep(self.sleep)
                continue

            url = seed.get("url")
            attempts = seed.get("attempts", 0)
            current_page_depth = seed.get("depth", 0)
            current_seed_detected = seed.get("detected")
            
            logging.info("Procesando seed: %s (attempts=%d, depth=%d)", url, attempts, current_page_depth)

            # Si ya ha fallado demasiadas veces
            if attempts > self.max_attempts:
                self.mongo_db.mark_failed(url, "max_attempts_reached")
                time.sleep(self.sleep)
                continue

            # ---------------- FASE DE FETCH Y FILTRADO ----------------
            r = self.fetch_via_tor(url)
            
            # Manejo de fallos en la petición
            if not r or not r.text:
                logging.info("Falló fetch para %s", url)
                self.mongo_db.revert_to_pending(url) 
                time.sleep(self.sleep)
                continue

            html = r.text
            raw_soup = BeautifulSoup(html, "lxml")
            # Extraer texto y normalizar múltiples espacios
            text = " ".join(raw_soup.get_text(" ").split()) 
            
            # FILTRADO 1: Contenido pequeño
            if not text or len(text) < self.min_text_chars:
                logging.info("Contenido pequeño, MARCANDO como 'discarded': %s (%d chars)", url, len(text))
                self.mongo_db.mark_done(url, discard_reason="too_short_content")
                
                new_count = self.mongo_db.get_and_inc_processed_count()
                if new_count >= self.max_pages_to_fetch: break 
                time.sleep(self.sleep)
                continue

            # ---------------- EXTRACCIÓN Y PREPARACIÓN ----------------
            safe_html = self.sanitize_html(html)
            fname = hashlib.sha1(url.encode('utf-8')).hexdigest()[:16] + ".html"
            
            gridfs_ref = None
            crawled = datetime.utcnow().isoformat()
            try:
                file_id = self.mongo_db.save_html_to_gridfs(
                    filename=fname, 
                    content=safe_html, 
                    metadata={"source_url": url, "crawl_date": crawled, "sha1": fname[:-5]}
                )
                gridfs_ref = str(file_id) 
                logging.info("HTML guardado en GridFS para %s. ID: %s", url, gridfs_ref)
            except Exception as e:
                logging.warning("Error saving html %s : %s", url, e)
                self.mongo_db.revert_to_pending(url)
                continue
            
            
            # Extracción de título más segura
            title = ""
            try:
                if raw_soup.title and raw_soup.title.string:
                    title = raw_soup.title.string.strip()
            except Exception:
                title = ""

            # Extracción de enlaces y propagación de seeds
            links_list = []
            new_depth = current_page_depth + 1
            
            for a in raw_soup.find_all("a", href=True): 
                href = a['href']
                full_link = urljoin(url, href).split('#')[0]
                
                if self.onion_re.search(full_link):
                    parsed_link = urlparse(full_link)
                    # Asegura que el link destino siempre termine en / si es solo el host
                    link = "http://" + (parsed_link.hostname or full_link) + "/"
                    anchor_text = a.get_text(strip=True) or (a.get('title') or "[enlace]")

                    if link == url:
                        logging.debug("Self-link detected and skipped: %s", url)
                        continue

                    if new_depth <= self.max_depth:
                        self.mongo_db.ensure_seed(link, detected=current_seed_detected, origin={"parent": url, "anchor": anchor_text[:200]}, depth=new_depth)
                    
                    links_list.append({
                        "src_url": url,
                        "dst_url": link,
                        "anchor": anchor_text[:200],
                        "depth": current_page_depth,
                        "dst_html_ref": gridfs_ref,
                        "crawl_date": crawled
                    })

            # Construcción del payload de términos
            matched_terms = []
            if current_seed_detected:
                for d in current_seed_detected: 
                    root = d.get("root", "")
                    syns = d.get("synonyms", []) 
                    for syn in syns:
                        matched_terms.append({
                            "page_url": url,
                            "root": root,
                            "synonym": syn, 
                            "source": "ahmia",
                            "crawl_date": crawled
                        })
                        
            page_node = { 
                "url": url,
                "title": title,
                "text": text[:10000],
                "crawl_date": crawled,
                "http_content_type": r.headers.get("Content-Type", ""),
                "html_file_id": gridfs_ref, # Solo la referencia a GridFS
            }
        
            payload = {
                "page": page_node,
                "links": links_list,
                "matched_terms": matched_terms
            }

            # ---------------- POST a Neo4j ----------------
            # FILTRADO 2: Sin términos coincidentes
            if not matched_terms:
                logging.warning("DIAGNÓSTICO: 'matched_terms' está vacío para %s. Saltando Neo4j, MARCANDO como 'discarded'.", url)
                self.mongo_db.mark_done(url, discard_reason="no_matching_terms_propagated")
                
                new_count = self.mongo_db.get_and_inc_processed_count()
                if new_count >= self.max_pages_to_fetch: break
                time.sleep(self.sleep)
                continue
                
            # ---------------- POST a Neo4j ----------------
            # FILTRADO 2: Sin términos coincidentes
            if not matched_terms:
                logging.warning("DIAGNÓSTICO: 'matched_terms' está vacío para %s. Saltando Neo4j, MARCANDO como 'discarded'.", url)
                self.mongo_db.mark_done(url, discard_reason="no_matching_terms_propagated")
                
                new_count = self.mongo_db.get_and_inc_processed_count()
                if new_count >= self.max_pages_to_fetch: break
                time.sleep(self.sleep)
                continue
                
            # LLAMADA AL CONTROLADOR DE NEO (CLIENTE)
            resp = self.neo_db.post_page_payload(payload) 
            
            if resp is None or resp.status_code != 200:
                logging.warning("Neo ingest devolvió %s o falló -> reintentando más tarde", resp.status_code if resp else "No response")
                self.mongo_db.revert_to_pending(url)
                time.sleep(self.sleep)
                continue

            # Si la ingesta es 200 OK:
            logging.info("Ingestado en Neo: %s", url)
            self.mongo_db.mark_done(url, {
                "html_file_id": gridfs_ref, 
                "title": title,
                "updated_at": datetime.utcnow(),
                "last_scraped": datetime.utcnow()
            })
            
            new_count = self.mongo_db.get_and_inc_processed_count()

            if new_count >= self.max_pages_to_fetch: break 
            
            if new_count % 50 == 0:
                logging.info(f"--- Páginas completadas (Neo OK) hasta ahora: {new_count} ---")
            
            logging.info("Seed procesada y marcada ingested: %s (depth=%d)", url, current_page_depth)
            time.sleep(self.sleep)

        logging.info("Worker terminado. Processed=%d", self.mongo_db.get_current_processed_count())
        self.mongo_db.close()

# ---------------- PUNTO DE ENTRADA ----------------
if __name__ == "__main__":
    try:
        logging.info("Iniciando Tor worker.")
        # Creamos la instancia de la clase TorController, que inicia el servidor Neo4j en un hilo
        crawler = TorController() 
        # Ejecutamos el método principal (el bucle de crawling)
        crawler.start_crawling() 
    except Exception as e:
        logging.exception("Error inesperado en worker: %s", e)
    finally:
        logging.info("Worker finalizado.")