#!/usr/bin/env python3
import re
import sys
import json
import time
import requests
import os
from bs4 import BeautifulSoup
from urllib.parse import urlencode, urljoin, urlparse, parse_qs, unquote

# --- Constantes ---
AHMIA_HOME = "https://ahmia.fi/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0'
}
WAIT = 5 # segundos entre búsquedas
ONION_RE = re.compile(r'\b([a-z2-7]{16,56}\.onion)\b', re.IGNORECASE)
OUTPUT_DIR_NAME = "output_ahmia"
OUTPUT_DIR = os.path.join("..", OUTPUT_DIR_NAME)
OUTPUT_SEEDS = os.path.join(OUTPUT_DIR, "seeds_with_terms.json")
OUTPUT_HOSTS = os.path.join(OUTPUT_DIR, "hosts_terms.json")
SYN_FILE = os.path.join("..", "docs", "synonyms.json")

class Result_processor:
    """
    Clase responsable de la gestión de datos: 
    cargar sinónimos, extraer hosts del HTML y generar archivos de salida.
    """
    def __init__(self, syn_file=SYN_FILE):
        self.syn_file = syn_file
        self.hosts_map = {}
        self.total_found = 0
        self.synmap = {}
        self.term_list = []
        self._load_synonyms()
        self._build_term_list()

    def _load_synonyms(self):
        """Carga el mapeo de sinónimos desde el archivo."""
        try:
            with open(self.syn_file, encoding='utf-8') as f:
                self.synmap = json.load(f)
        except FileNotFoundError:
            print(f"[ERROR] Archivo {self.syn_file} no encontrado. Crea: {{raiz: [sin1, sin2, ...]}}")
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"[ERROR] Error al decodificar JSON en {self.syn_file}.")
            sys.exit(1)
        print(f"[INFO] Sinónimos cargados desde {self.syn_file}")

    def _build_term_list(self):
        """Construye la lista de términos de búsqueda [(raiz, término)]."""
        for root, syns in self.synmap.items():
            self.term_list.append((root, root))
            for s in syns:
                self.term_list.append((root, s))
        print(f"[INFO] Total de términos a buscar: {len(self.term_list)}")

    def extract_onions_from_html(self, html):
        """Analiza y extrae .onion del HTML."""
        if not html:
            return set()
        
        onions = set()
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ")

        # 1. Matches directos en hrefs y texto
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = ONION_RE.search(href)
            if m:
                onions.add(m.group(1).lower())
        
        # 2. Matches en texto crudo
        for m in ONION_RE.finditer(text):
             onions.add(m.group(1).lower())
            
        # 3. Matches en parámetros de URL de redirección
        for a in soup.find_all("a", href=True):
            href = a["href"]
            try:
                parsed = urlparse(href)
                qs = parse_qs(parsed.query)
                if "redirect_url" in qs:
                    rv = unquote(qs["redirect_url"][0])
                    m = ONION_RE.search(rv)
                    if m:
                        onions.add(m.group(1).lower())
            except Exception:
                pass
                
        return onions

    def record_host(self, host, root, term):
        """Guarda la información de un host detectado."""
        self.total_found += 1
        self.hosts_map.setdefault(host, {}).setdefault(root, set()).add(term)

    def output_results(self, output_hosts=OUTPUT_HOSTS, output_seeds=OUTPUT_SEEDS):
        """Escribe los resultados a archivos JSON."""
        hosts_terms = {}
        seeds_list = []
        
        for host, roots_dict in self.hosts_map.items():
            arr = []
            for root, synset in roots_dict.items():
                arr.append({
                    "root": root,
                    "synonyms": sorted(list(synset)), 
                    "is_root": root in synset
                })
            
            hosts_terms[host] = arr
            seeds_list.append({
                "host": host,
                "url": f"http://{host}/",
                "detected": arr
            })
        
        try:
            with open(output_hosts, "w", encoding='utf-8') as f:
                json.dump(hosts_terms, f, indent=2, ensure_ascii=False)
            print(f"[INFO] Hosts y términos escritos en: {output_hosts}")
        except Exception as e:
            print(f"[ERROR] Error al escribir {output_hosts}: {e}")

        try:
            with open(output_seeds, "w", encoding='utf-8') as f:
                json.dump(seeds_list, f, indent=2, ensure_ascii=False)
            print(f"[INFO] Seeds escritos en: {output_seeds}")
        except Exception as e:
            print(f"[ERROR] Error al escribir {output_seeds}: {e}")
            
        print(f"[DONE] Hosts detectados: {len(hosts_terms)} | Total hits (raw): {self.total_found}")


class Ahmia_scraper:
    """Clase principal para manejar la sesión, la interacción HTTP y orquestar la búsqueda."""

    def __init__(self, processor, wait_time=WAIT):
        """Recibe el objeto ResultProcessor."""
        self.processor = processor
        self.wait_time = wait_time
        self.token_key = None
        self.token_val = None

    def _get_session_token(self):
        """Obtiene el token de sesión necesario para las búsquedas."""
        print("[INFO] Obteniendo token de sesión...")
        try:
            r = requests.get(AHMIA_HOME, headers=HEADERS, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"[ERROR] No se puede cargar la página principal de Ahmia: {e}")
            sys.exit(1)
            
        soup = BeautifulSoup(r.text, "lxml")
        form = soup.find("form", id="searchForm")
        hidden = form.find("input", {"type": "hidden"}) if form else None
        
        if not hidden or not hidden.get("name") or not hidden.get("value"):
            print("[ERROR] No se pudo extraer el token de sesión oculto.")
            sys.exit(1)
            
        self.token_key = hidden["name"]
        self.token_val = hidden["value"]
        print(f"[INFO] Token de sesión capturado: {self.token_key}={self.token_val}")

    def _fetch_search_page(self, query):
        """Busca y obtiene los resultados de una consulta."""
        if not self.token_key or not self.token_val:
            # Esto no debería pasar si run_search llama a _get_session_token
            self._get_session_token()

        params = {"q": query, self.token_key: self.token_val}
        url = urljoin(AHMIA_HOME, "search/") + "?" + urlencode(params)
        
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"[WARN] Error al buscar {url}: {e}")
            return None

    def run_search(self):
        """Ejecuta el proceso de búsqueda para todos los términos."""
        self._get_session_token()
        
        term_list = self.processor.term_list 
        
        for idx, (root, term) in enumerate(term_list, 1):
            print(f"\n[{idx}/{len(term_list)}] Buscando término='{term}' (raíz={root})")
            
            html = self._fetch_search_page(term)
            
            if not html:
                print(f"  [INFO] Búsqueda fallida para '{term}'. Saltando.")
            else:
                # Usa el ResultProcessor para analizar el HTML
                onions = self.processor.extract_onions_from_html(html)
                
                if not onions:
                    print(f"  [INFO] No se encontraron resultados .onion para '{term}'")
                else:
                    print(f"  [INFO] Encontrados {len(onions)} hosts para '{term}'")
                    for host in onions:
                        # Usa el ResultProcessor para almacenar el resultado
                        self.processor.record_host(host, root, term)
            
            # Espera entre búsquedas
            print(f"  [INFO] Espera '{self.wait_time}' segundos")
            time.sleep(self.wait_time)
            
        print("\n[INFO] Proceso de scraping finalizado. Generando salidas...")
        # Usa el ResultProcessor para generar los archivos
        self.processor.output_results()


def ensure_output_directory(directory_name):
    """Asegura que el directorio de salida exista. Esto es nuevo."""
    if not os.path.exists(directory_name):
        try:
            os.makedirs(directory_name)
            print(f"[INFO] Creado el directorio de salida: '{directory_name}'")
        except OSError as e:
            print(f"[ERROR] No se pudo crear el directorio '{directory_name}': {e}")
            sys.exit(1)

def main_oop():
    """Función principal para ejecutar la versión orientada a objetos."""
    # 1. Crear el objeto que maneja la carga y el procesamiento de datos
    processor = Result_processor(syn_file=SYN_FILE)
    
    # 2. Crear el objeto que hace las búsquedas, inyectándole el procesador
    # Esta es la parte clave de la colaboración entre clases.
    scraper = Ahmia_scraper(processor=processor, wait_time=WAIT)
    
    # 3. Iniciar el proceso
    scraper.run_search()


if __name__ == "__main__":
    main_oop()