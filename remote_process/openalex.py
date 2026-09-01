import math, time, pickle
import requests
from urllib.parse import quote
from retry import retry
from paths import PATH_HARVEST

@retry(delay=100, tries=3)
def get_all_from_openalex(url):
    data = []
    r = requests.get(url)
    res = r.json()

    if 'results' not in res:
        # affiche l'erreur réelle renvoyée par l'API pour comprendre le problème
        print(f"Erreur API OpenAlex (status {r.status_code}) pour l'URL: {url}")
        print(f"Réponse: {res}")
        # on lève une erreur volontaire seulement si ça vaut la peine de retenter
        # (ex: erreur 429/500 = transitoire), sinon on retourne vide direct
        if r.status_code in (429, 500, 502, 503):
            raise RuntimeError(f"Erreur transitoire OpenAlex: {res}")
        return data  # erreur définitive (ex: requête malformée) -> pas la peine de retry

    data = res['results']
    nb_res = res['meta']['count']
    nb_pages = math.ceil(nb_res / 200)
    for page in range(2, nb_pages + 2):
        url_paged = f'{url}&page={page}'
        res = requests.get(url_paged).json()
        if isinstance(res.get('results'), list):
            data += res['results']
    print(f'{len(data)} results found')
    return data


def parse_openalex_author(e, match):
    elt = {'match': match}
    for f in ['id', 'ids', 'orcid', 'display_name', 'display_name_alternatives', 'works_count',
              'affiliations', 'last_known_institutions', 'topics']:
        if e.get(f):
            elt[f] = e[f]
    return elt


def get_author_from_openalex(orcid, full_name, country_code):
    URL_AUTHORS = 'https://api.openalex.org/authors?per_page=200&filter='
    print(f"For {full_name}")
    if country_code:
        URL_AUTHORS += f'affiliations.institution.country_code:{country_code},'

    if isinstance(orcid, str) and orcid.strip():
        url_to_use = f'{URL_AUTHORS}orcid:{orcid}'
        res = get_all_from_openalex(url_to_use)
        print(f'{len(res)} results found with orcid {orcid}')
        if len(res) > 0:
            return [parse_openalex_author(e, 'orcid') for e in res]

    if isinstance(full_name, str) and full_name.strip():
        # encodage indispensable : espaces, apostrophes, accents...
        url_to_use = f'{URL_AUTHORS}display_name.search:{quote(full_name)}'
        res = get_all_from_openalex(url_to_use)
        print(f'{len(res)} results found with full_name {full_name}')
        if len(res) > 0:
            return [parse_openalex_author(e, 'full_name') for e in res]

    return []


def harvest_openalex(df, iso2):
    print(time.strftime("%H:%M:%S"))
    rlist = []
    n = 0
    for _, row in df.iterrows():
        n += 1
        if n % 100 == 0:
            print(f"{n}", end=',')

        country = row['iso2'] if iso2 else ''
        try:
            res = get_author_from_openalex(row['orcid_id'], row['contact2'], country)
            rlist.extend(res)
        except Exception as e:
            # une ligne en erreur ne doit plus faire planter tout le harvest
            print(f"⚠️ Échec pour '{row['contact2']}' (ligne {n}): {e}")
            continue

        if n % 2000 == 0:
            a = str(int(n / 1000))
            with open(f'{PATH_HARVEST}persons/persons_authors_{a}.pkl', 'wb') as f:
                pickle.dump(rlist, f)

    print(time.strftime("%H:%M:%S"))
    return rlist