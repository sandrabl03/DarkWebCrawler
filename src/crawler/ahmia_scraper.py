#!/usr/bin/env python3

import sys
import time
import requests
import os
from bs4 import BeautifulSoup
from urllib.parse import urlencode, urljoin

from crawler.result_processor import ResultProcessor, OUTPUT_DIR_NAME, SYN_FILE

# --- Constantes ---
AHMIA_HOME = "https://ahmia.fi/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0'
}
WAIT = 5 # segundos entre búsquedas


class AhmiaScraper:
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


# --- Funciones de Orquestación y Ejecución ---

def ensure_output_directory(directory_name):
    """Asegura que el directorio de salida exista."""
    if not os.path.exists(directory_name):
        try:
            os.makedirs(directory_name)
            print(f"[INFO] Creado el directorio de salida: '{directory_name}'")
        except OSError as e:
            print(f"[ERROR] No se pudo crear el directorio '{directory_name}': {e}")
            sys.exit(1)

def main_oop():
    """Función principal para ejecutar la versión orientada a objetos."""
    # Asegura que el directorio de salida exista
    ensure_output_directory(os.path.join("..", OUTPUT_DIR_NAME))
    
    # 1. Crear el objeto que maneja la carga y el procesamiento de datos
    processor = ResultProcessor(syn_file=SYN_FILE)
    
    # 2. Crear el objeto que hace las búsquedas, inyectándole el procesador
    scraper = AhmiaScraper(processor=processor, wait_time=WAIT)
    
    # 3. Iniciar el proceso
    scraper.run_search()


if __name__ == "__main__":
    main_oop()