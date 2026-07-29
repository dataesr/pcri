
import csv
import io, sys
import time
import zipfile
from pathlib import Path
import requests


"""
Récupère, pour une liste de pays, les codes postaux GeoNames enrichis
avec la subdivision de niveau 2 (province/état/comté) même quand
`adminName2` est vide dans le fichier source (cas de la Belgique
notamment).

Stratégie :
1. Téléchargement du fichier ZIP officiel (pas d'appel API, pas de
   limite de débit) : http://download.geonames.org/export/zip/{CC}.zip
2. Pour chaque ligne où adminName2 est vide, appel ciblé de
   `countrySubdivisionJSON` (lat/lng -> nom de la subdivision niveau 2),
   avec mise en cache pour éviter les appels redondants.
3. Écriture d'un CSV par pays dans OUTPUT_DIR/{CC}.csv, plus un
   fichier combiné all_countries.csv.

Reprise possible : les pays déjà traités (fichier CSV existant) sont
sautés automatiquement.
"""
import requests
import csv
import io, sys
import time
import zipfile
from collections import deque
from pathlib import Path


# Garantit que la racine du projet (contenant paths.py) est trouvable,
# quel que soit le mode de lancement (direct, subprocess, IDE, notebook...)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from paths import PATH_HARVEST
# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
USERNAME = "zoefri"                 # ton compte GeoNames
OUTPUT_DIR = Path(f"{PATH_HARVEST}geoloc/by_countries")
CACHE_FILE = Path(f"{PATH_HARVEST}geoloc/by_countries/subdivision_cache.csv")
SLEEP_BETWEEN_API_CALLS = 1.0       # secondes, à ajuster selon ton quota
ROUND_DECIMALS = 3                  # arrondi lat/lng pour le cache (~100 m)
HOURLY_CREDIT_LIMIT = 950           # marge de sécurité sous la limite officielle (1000/h, compte gratuit)
QUOTA_RETRY_WAIT_SECONDS = 65 * 60  # pause si le serveur renvoie quand même l'erreur de quota

COUNTRIES = [
    'ZM', 'GU', 'TG', 'TV', 'KP', 'MU', 'BQ', 'EC', 'GD', 'CH', 'TN', 'IN',
    'AM', 'SR', 'MT', 'SE', 'PG', 'AW', 'LB', 'LV', 'RW', 'LY', 'KG', 'SM',
    'KZ', 'MG', 'CN', 'FK', 'KM', 'CU', 'IS', 'UY', 'LT', 'KE', 'NI', 'CW',
    'SZ', 'FM', 'LU', 'DE', 'SJ', 'MC', 'MA', 'HT', 'GT', 'AX', 'HK', 'TF',
    'HR', 'SA', 'NL', 'SH', 'VN', 'ES', 'PE', 'KR', 'PH', 'MM', 'SX', 'VE',
    'NA', 'NG', 'GN', 'BS', 'IR', 'GW', 'PL', 'LK', 'BD', 'UG', 'BW', 'BZ',
    'OM', 'BG', 'GA', 'MV', 'BJ', 'SN', 'IM', 'TD', 'MK', 'WS', 'CR', 'FJ',
    'BN', 'DK', 'TW', 'ML', 'TM', 'TT', 'ZW', 'BT', 'IE', 'PF', 'DO', 'MZ',
    'DZ', 'ET', 'MD', 'PS', 'AR', 'MX', 'RO', 'LA', 'TR', 'JO', 'MN', 'DJ',
    'SV', 'KI', 'GQ', 'CI', 'GY', 'PW', 'MR', 'JE', 'VC', 'SS', 'AT', 'VA',
    'NP', 'BE', 'EG', 'BB', 'CK', 'NO', 'GI', 'FR', 'AE', 'BO', 'TH', 'PR',
    'RE', 'MH', 'BF', 'BM', 'XK', 'RS', 'RU', 'AI', 'CD', 'PK', 'ER', 'AF',
    'KY', 'IL', 'GH', 'AU', 'CA', 'CZ', 'CO', 'VG', 'QA', 'LC', 'US', 'ZZ',
    'CM', 'GE', 'KW', 'HU', 'SG', 'UM', 'GM', 'UZ', 'SL', 'TJ', 'NE', 'GL',
    'KH', 'BY', 'PT', 'SI', 'ID', 'CF', 'HN', 'SC', 'PA', 'CV', 'CG', 'NC',
    'ZA', 'EE', 'MW', 'IT', 'GG', 'SY', 'SB', 'SD', 'TL', 'BA', 'MY', 'NZ',
    'BR', 'CL', 'LI', 'BI', 'FI', 'AZ', 'IQ', 'VU', 'CY', 'FO', 'JP', 'SK',
    'AL', 'YE', 'ST', 'PY', 'LS', 'AO', 'KN', 'WF', 'TZ', 'JM', 'MO', 'UA',
    'ME', 'SO', 'MF', 'BH', 'LR', 'GB', 'GR',
]

FIELDNAMES = [
    "country_code", "postal_code", "place_name",
    "admin_name1", "admin_code1",
    "admin_name2", "admin_code2",   # rempli si besoin via fallback
    "admin_name3", "admin_code3",
    "lat", "lng",
    "province_source",  # "original" ou "fallback_api"
]

# ---------------------------------------------------------------------
# Cache disque pour les lookups de subdivision (persistant entre runs)
# ---------------------------------------------------------------------
_subdivision_cache: dict[tuple[float, float], str] = {}


def load_cache():
    if CACHE_FILE.exists():
        with CACHE_FILE.open(newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                lat, lng, name = row
                _subdivision_cache[(float(lat), float(lng))] = name


def save_cache_entry(lat, lng, name):
    with CACHE_FILE.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([lat, lng, name])


# ---------------------------------------------------------------------
# Téléchargement du fichier ZIP officiel (pas d'appel API)
# ---------------------------------------------------------------------
def download_country_zip(country_code: str) -> list[dict]:
    url = f"http://download.geonames.org/export/zip/{country_code}.zip"
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        print(f"  [!] Pas de fichier ZIP pour {country_code} (HTTP {resp.status_code})", flush=True)
        return []

    rows = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        # le fichier utile porte le nom {CC}.txt (l'autre est readme.txt)
        txt_name = next(n for n in zf.namelist() if n.upper().endswith(".TXT") and "readme" not in n.lower())
        with zf.open(txt_name) as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"), delimiter="\t")
            for r in reader:
                if len(r) < 11:
                    continue
                rows.append({
                    "country_code": r[0],
                    "postal_code": r[1],
                    "place_name": r[2],
                    "admin_name1": r[3],
                    "admin_code1": r[4],
                    "admin_name2": r[5],
                    "admin_code2": r[6],
                    "admin_name3": r[7],
                    "admin_code3": r[8],
                    "lat": r[9],
                    "lng": r[10],
                })
    return rows


# ---------------------------------------------------------------------
# Auto-throttle proactif : reste sous la limite horaire de crédits
# ---------------------------------------------------------------------
_call_timestamps: deque[float] = deque()


def _throttle_for_hourly_quota():
    now = time.time()
    while _call_timestamps and now - _call_timestamps[0] > 3600:
        _call_timestamps.popleft()
    if len(_call_timestamps) >= HOURLY_CREDIT_LIMIT:
        wait = 3600 - (now - _call_timestamps[0]) + 5
        if wait > 0:
            print(f"    [i] Proche de la limite horaire ({HOURLY_CREDIT_LIMIT} crédits), "
                  f"pause de {wait / 60:.1f} min...", flush=True)
            time.sleep(wait)
    _call_timestamps.append(time.time())


# ---------------------------------------------------------------------
# Fallback API : subdivision de niveau 2 à partir des coordonnées
# ---------------------------------------------------------------------
def fetch_subdivision_level2(lat: float, lng: float) -> str | None:
    key = (round(lat, ROUND_DECIMALS), round(lng, ROUND_DECIMALS))
    if key in _subdivision_cache:
        return _subdivision_cache[key]

    url = "http://api.geonames.org/countrySubdivisionJSON"
    params = {"lat": lat, "lng": lng, "level": 2, "username": USERNAME}

    while True:
        _throttle_for_hourly_quota()
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"    [!] Erreur API subdivision ({lat},{lng}): {e}", flush=True)
            time.sleep(SLEEP_BETWEEN_API_CALLS)
            return None

        if isinstance(data, dict) and data.get("status"):
            message = data["status"].get("message", "")
            if "hourly limit" in message.lower() or "daily limit" in message.lower():
                # le quota a quand même été dépassé (ex: crédits déjà consommés
                # ailleurs, ou compteur local désynchronisé après redémarrage) :
                # on attend et on retente la MÊME requête, rien n'est perdu.
                print(f"    [!] Quota dépassé malgré le throttle : {message}", flush=True)
                print(f"    [i] Pause de {QUOTA_RETRY_WAIT_SECONDS / 60:.0f} min avant de reprendre...", flush=True)
                time.sleep(QUOTA_RETRY_WAIT_SECONDS)
                continue
            print(f"    [!] GeoNames API error: {message}", flush=True)
            time.sleep(SLEEP_BETWEEN_API_CALLS)
            return None

        break

    time.sleep(SLEEP_BETWEEN_API_CALLS)
    name = None
    if isinstance(data, list) and data:
        name = data[0].get("adminName1") or data[0].get("name")

    _subdivision_cache[key] = name
    save_cache_entry(*key, name or "")
    return name


# ---------------------------------------------------------------------
# Traitement d'un pays
# ---------------------------------------------------------------------
def process_country(country_code: str):
    out_path = OUTPUT_DIR / f"{country_code}.csv"
    if out_path.exists():
        print(f"[skip] {country_code} déjà traité", flush=True)
        return

    print(f"[...] {country_code}", flush=True)
    rows = download_country_zip(country_code)
    if not rows:
        return

    missing = sum(1 for r in rows if not r["admin_name2"])
    print(f"      {len(rows)} lignes, {missing} sans province -> fallback API", flush=True)

    tmp_path = out_path.with_suffix(".csv.tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for r in rows:
            r["province_source"] = "original"
            if not r["admin_name2"] and r["lat"] and r["lng"]:
                try:
                    lat, lng = float(r["lat"]), float(r["lng"])
                except ValueError:
                    lat = lng = None
                if lat is not None:
                    fallback_name = fetch_subdivision_level2(lat, lng)
                    if fallback_name:
                        r["admin_name2"] = fallback_name
                        r["province_source"] = "fallback_api"
            writer.writerow(r)

    # renommage atomique : le fichier final n'apparaît que si tout s'est bien passé,
    # donc une interruption en cours de route ne fera pas croire au script (au
    # prochain lancement) que ce pays est déjà traité
    tmp_path.replace(out_path)
    print(f"      -> {out_path}", flush=True)


# ---------------------------------------------------------------------
# Fusion finale
# ---------------------------------------------------------------------
def merge_all():
    combined_path = OUTPUT_DIR / "all_countries.csv"
    with combined_path.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for cc in COUNTRIES:
            p = OUTPUT_DIR / f"{cc}.csv"
            if not p.exists():
                continue
            with p.open(newline="", encoding="utf-8") as in_f:
                reader = csv.DictReader(in_f)
                for row in reader:
                    writer.writerow(row)
    print(f"Fichier combiné : {combined_path}", flush=True)


# ---------------------------------------------------------------------
if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)   # parents=True au cas où PATH_HARVEST/geoloc n'existe pas encore
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    load_cache()

    print(f"=== Démarrage : {len(COUNTRIES)} pays à traiter ===", flush=True)
    for i, cc in enumerate(COUNTRIES, start=1):
        try:
            print(f"[{i}/{len(COUNTRIES)}]", end=" ", flush=True)
            process_country(cc)
        except Exception as e:
            print(f"  [!] Échec {cc}: {e}", flush=True)

    merge_all()
    print("=== Terminé ===", flush=True)