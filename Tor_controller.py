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
from Mongo_Controller import MongoController, RESET_INPROGRESS_OLDER_MIN 
from Neo_controller import NeoController 

# Logging simple (el resto de módulos usan el suyo propio)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [TOR] %(message)s")


# ---------------- CLASE TOR CONTROLLER ----------------
class TorController:
    """
    Motor principal del crawler. Encapsula la lógica de fetching, filtrado,
    extracción de enlaces y coordinación con MongoController y NeoController.
    """
    def __init__(self):
        # --- Configuración (Mover todas las variables globales aquí) ---
        self.out_html_dir = os.getenv("OUT_HTML_DIR", "raw/html_files")
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
        self.neo_db = NeoController()
        
        # Setup de directorios
        os.makedirs(self.out_html_dir, exist_ok=True)
        
        # Manejo de señal
        signal.signal(signal.SIGINT, self.handle_sigint)
        
        logging.info("TorController inicializado. Conexiones a DB activas.")

    def handle_sigint(self, signum, frame):
        """Maneja la señal SIGINT para una parada ordenada."""
        logging.info("SIGINT recibido: preparando parada ordenada...")
        self.running = False
        
    # --- MÉTODOS DE CRAWLING (Antiguas funciones fetch_via_tor y sanitize_html) ---
    
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
        """Sanitiza el HTML."""
        try:
            soup = BeautifulSoup(raw_html, "lxml")
        except Exception:
            soup = BeautifulSoup(raw_html, "html.parser")

        for t in soup(['script','style','noscript','iframe','form','object','embed']):
            t.decompose()
            
        event_attrs = re.compile(r'^on', re.IGNORECASE)
        global_attrs_to_remove = ('style', 'data', 'src', 'srcset') 
        
        for tag in soup.find_all(True):
            if tag.name != 'a': 
                for attr in list(tag.attrs.keys()):
                    if event_attrs.match(attr) or attr.lower() in global_attrs_to_remove:
                        del tag.attrs[attr]
            else:
                for attr in list(tag.attrs.keys()):
                    if event_attrs.match(attr):
                        del tag.attrs[attr]
        
        safe_body = soup.body or soup
        safe_html = f"<!doctype html><html><head><meta charset='utf-8'/></head><body>{str(safe_body)}</body></html>"
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
                if attempts < self.max_attempts:
                     self.mongo_db.revert_to_pending(url)
                else:
                     self.mongo_db.mark_failed(url, "fetch_failed_final")
                
                time.sleep(self.sleep)
                continue

            html = r.text
            text = BeautifulSoup(html, "lxml").get_text(" ")
            
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
            path = os.path.join(self.out_html_dir, fname)
            
            try:
                with open(path, "w", encoding="utf-8") as fh:
                    fh.write(safe_html)
            except Exception as e:
                logging.warning("Error saving html %s : %s", path, e)
                self.mongo_db.mark_failed(url, "save_error")
                continue
                
            crawled = datetime.utcnow().isoformat()
            try:
                raw_soup = BeautifulSoup(html, "lxml") 
                title = raw_soup.title.string.strip() if raw_soup.title and raw_soup.title.string else ""
            except:
                title = ""

            # Extracción de enlaces y propagación de seeds
            links_list = []
            new_depth = current_page_depth + 1
            
            for a in raw_soup.find_all("a", href=True): 
                href = a['href']
                full_link = urljoin(url, href).split('#')[0]
                
                if self.onion_re.search(full_link):
                    parsed_link = urlparse(full_link)
                    link = "http://" + (parsed_link.hostname or full_link) + "/"
                    anchor_text = a.get_text(strip=True) or (a.get('title') or "[enlace]")

                    if new_depth <= self.max_depth:
                        self.mongo_db.ensure_seed(link, detected=current_seed_detected, origin={"parent": url, "anchor": anchor_text[:200]}, depth=new_depth)
                    
                    links_list.append({
                        "src_url": url,
                        "dst_url": link,
                        "anchor": anchor_text[:200],
                        "depth": current_page_depth,
                        "dst_html": fname,
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
                "html_file": fname,
                "html_file_path": os.path.abspath(path),
                "html_file_url": f"http://127.0.0.1:8000/html_files/{fname}",
                "safe_html": safe_html
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
                
            # LLAMADA AL CONTROLADOR DE NEO
            resp = self.neo_db.post_page_payload(payload) 
            
            if resp is None or resp.status_code != 200:
                logging.warning("Neo ingest devolvió %s o falló -> reintentando más tarde", resp.status_code if resp else "No response")
                self.mongo_db.revert_to_pending(url)
                time.sleep(self.sleep)
                continue

            # Si la ingesta es 200 OK:
            logging.info("Ingestado en Neo: %s", url)
            self.mongo_db.mark_done(url, {
                "html_file": fname,
                "html_file_path": os.path.abspath(path),
                "html_file_url": page_node["html_file_url"],
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
        self.mongo_db.close() # CERRAR CONEXIÓN DE MONGO

# ---------------- PUNTO DE ENTRADA ----------------
if __name__ == "__main__":
    try:
        logging.info("Iniciando Tor worker.")
        # Creamos la instancia de la clase TorController
        crawler = TorController() 
        # Ejecutamos el método principal
        crawler.start_crawling() 
    except Exception as e:
        logging.exception("Error inesperado en worker: %s", e)
    finally:
        logging.info("Worker finalizado.")