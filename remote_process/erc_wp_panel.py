
from paths import PATH_HARVEST, PATH_WP
import time, pandas as pd, requests, fitz, re, os, json, pdfplumber
# from bs4 import BeautifulSoup
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.firefox .service import Service as FirefoxService
# from webdriver_manager.firefox  import GeckoDriverManager
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.chrome.options import Options

def erc_wp_panel(year, url):

    fname = f"{PATH_WP}{year}/erc.pdf"
    try:
        r = requests.get(url, allow_redirects=True)
        open(fname, 'wb').write(r.content)

    except requests.RequestException as e:
        print(f"⚠ Téléchargement impossible : {e}")
        print("→ Basculement sur les données embarquées.")
    
    
    pages = []
    with pdfplumber.open(fname) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                pages.append(t)
    full_text = "\n".join(pages)
    
    # ─── Parsing ──────────────────────────────────────────────────────────────────
    
    DOMAINS = {
        "PE": "Physical Sciences and Engineering",
        "LS": "Life Sciences",
        "SH": "Social Sciences and Humanities",
    }
    
    # Sous-panel : "LS1_1 Logic and foundations"
    RE_SUBPANEL = re.compile(r"^((?:PE|LS|SH)\d{1,2}_\d{1,2})\s+(.+)$", re.MULTILINE)
    
    # Panel principal : nom + description limitée à 3 lignes max avant le premier sous-panel
    # On s'arrête dès qu'on rencontre un code panel/sous-panel OU après 3 lignes
    RE_PANEL = re.compile(
        r"^((?:PE|LS|SH)\d{1,2})\s+([^\n]+)\n"
        r"((?:(?!(?:PE|LS|SH)\d)[^\n]+\n?){1,3})",
        re.MULTILINE
    )
    
    def clean(s):
        # Couper dès qu'on rencontre un marqueur de pagination ou de section annexe
        s = re.sub(r"\s*\d+\s*[|\u2502]\s*[Pp](?:age|\.).*", "", s, flags=re.DOTALL)
        s = re.sub(r"\s*[Pp]age\s*[|\u2502]\s*\d+.*",         "", s, flags=re.DOTALL)
        s = re.sub(r"\s*\bAnnex\s+\d+\b.*",                   "", s, flags=re.IGNORECASE | re.DOTALL)
        s = re.sub(r"\s*\bERC policy\b.*",                      "", s, flags=re.IGNORECASE | re.DOTALL)
        s = re.sub(r"\s+", " ", s)
        return s.strip().rstrip(" ,;")
    
    results = []
    
    # Entrées domaine de haut niveau
    for code, name in DOMAINS.items():
        results.append({"panel_code": code, "panel_name": name})
    
    # Panels principaux avec description
    seen = set()
    for m in RE_PANEL.finditer(full_text):
        code = m.group(1).strip()
        if "_" in code or code in seen:
            continue
        entry = {"panel_code": code, "panel_name": clean(m.group(2))}
        desc = clean(m.group(3))
        if desc:
            entry["panel_description"] = desc
        results.append(entry)
        seen.add(code)
    
    # Sous-panels
    for m in RE_SUBPANEL.finditer(full_text):
        results.append({"panel_code": m.group(1).strip(), "panel_name": clean(m.group(2))})
    
    # Tri PE → LS → SH, numérique, sous-panels après leur panel
    def sort_key(e):
        c = e["panel_code"]
        p = {"PE": 0, "LS": 1, "SH": 2}.get(c[:2], 9)
        if "_" in c:
            a, b = c.rsplit("_", 1)
            return (p, int(re.search(r"\d+", a).group()), 1, int(b))
        elif re.search(r"\d", c):
            return (p, int(re.search(r"\d+", c).group()), 0, 0)
        return (p, -1, 0, 0)
    
    results.sort(key=sort_key)
    
    # ─── Export JSON ──────────────────────────────────────────────────────────────
    json_path = f"{PATH_HARVEST}erc_panels.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    
    print(f"✓ {len(results)} entrées exportées → {json_path}")