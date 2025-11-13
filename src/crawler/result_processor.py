import re
import sys
import json
import os
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, unquote

# --- Constantes (ajustadas para la ruta) ---
ONION_RE = re.compile(r'\b([a-z2-7]{16,56}\.onion)\b', re.IGNORECASE)

OUTPUT_DIR_NAME = "output_ahmia"
OUTPUT_DIR = os.path.join(OUTPUT_DIR_NAME)
OUTPUT_SEEDS = os.path.join(OUTPUT_DIR, "seeds_with_terms.json")
OUTPUT_HOSTS = os.path.join(OUTPUT_DIR, "hosts_terms.json")
SYN_FILE = os.path.join("docs", "synonyms.json")


class ResultProcessor:
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