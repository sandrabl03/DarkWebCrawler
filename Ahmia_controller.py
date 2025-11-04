#!/usr/bin/env python3
import re
import sys
import json
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlencode, urljoin, urlparse, parse_qs, unquote

AHMIA_HOME = "https://ahmia.fi/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0'
}
WAIT = 5 # seconds between searches
ONION_RE = re.compile(r'\b([a-z2-7]{16,56}\.onion)\b', re.IGNORECASE)
OUTPUT_SEEDS = "seeds_with_terms.json"
OUTPUT_HOSTS = "hosts_terms.json"
SYN_FILE = "synonyms.json"

def get_session_token():
    """Fetch Ahmia front page and extract the hidden key=value token."""
    try:
        r = requests.get(AHMIA_HOME, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"[ERROR] Cannot load Ahmia front page: {e}")
        sys.exit(1)
    soup = BeautifulSoup(r.text, "lxml")
    form = soup.find("form", id="searchForm")
    if not form:
        print("[ERROR] Cannot find search form in Ahmia front page HTML.")
        sys.exit(1)
    hidden = form.find("input", {"type": "hidden"})
    if not hidden or not hidden.get("name") or not hidden.get("value"):
        print("[ERROR] Could not extract hidden session token.")
        sys.exit(1)
    token_key = hidden["name"]
    token_val = hidden["value"]
    print(f"[INFO] Session token captured: {token_key}={token_val}")
    return token_key, token_val

def fetch_search_page(token_key, token_val, query):
    """Fetch a search results from Ahmia using the current token."""
    params = {"q": query, token_key: token_val}
    url = urljoin(AHMIA_HOME, "search/") + "?" + urlencode(params)
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[WARN] Error fetching {url}: {e}")
        return None

def extract_onions_from_html(html):
    """Parse all .onion domains from the HTML text."""
    if not html:
        return set()
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ")
    onions = set()
    # Direct href matches
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = ONION_RE.search(href)
        if m:
            onions.add(m.group(1).lower())
    # Raw text matches
    for m in ONION_RE.finditer(text):
        onions.add(m.group(1).lower())
    # Redirect URL parameters
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

def main():
    """ Load synonyms mapping """
    try:
        with open(SYN_FILE, encoding='utf-8') as f:
            synmap = json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] File {SYN_FILE} not found. Create: {{root: [syn1, syn2, ...]}}")
        sys.exit(1)
    token_key, token_val = get_session_token()
    hosts_map = {}
    total_found = 0
    # Build list of (root, term)
    term_list = []
    for root, syns in synmap.items():
        term_list.append((root, root))
        for s in syns:
            term_list.append((root, s))
    print(f"[INFO] Total terms to search: {len(term_list)}")
    for idx, (root, term) in enumerate(term_list, 1):
        print(f"[{idx}/{len(term_list)}] Searching term='{term}' (root={root})")
        html = fetch_search_page(token_key, token_val, term)
        print(f"  [INFO] Wait '{WAIT}' seconds")
        time.sleep(WAIT)
        onions = extract_onions_from_html(html)
        if not onions:
            print(f"  [INFO] No results for '{term}'")
            continue
        for host in onions:
            total_found += 1
            hosts_map.setdefault(host, {}).setdefault(root, set()).add(term)
    # Prepare outputs
    hosts_terms = {}
    seeds_list = []
    for host, roots_dict in hosts_map.items():
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
    with open(OUTPUT_HOSTS, "w", encoding='utf-8') as f:
        json.dump(hosts_terms, f, indent=2, ensure_ascii=False)
    with open(OUTPUT_SEEDS, "w", encoding='utf-8') as f:
        json.dump(seeds_list, f, indent=2, ensure_ascii=False)
    print(f"[DONE] Hosts detected: {len(hosts_terms)} | Total hits (raw): {total_found}")
    print(f"Outputs written to: {OUTPUT_HOSTS}, {OUTPUT_SEEDS}")


if __name__ == "__main__":
    main()
