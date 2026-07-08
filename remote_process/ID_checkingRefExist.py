from config_api import sirene_headers, paysage_headers, ror_headers, ods_headers
from ratelimit import limits, sleep_and_retry
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests, time, pandas as pd
from dotenv import load_dotenv
load_dotenv()


def check_id_in_paysage(df, var_id: str, pid):
    """
    True/False if id in paysage or not
    """
    df.loc[(df['source_id']=='ror')&(df[var_id].str.startswith('R')), var_id] = df.loc[(df['source_id']=='ror')&(df[var_id].str.startswith('R')), var_id].str[1:]
    df = pd.DataFrame(df).merge(pid, how='left', left_on=var_id, right_on='check_id')
    df.loc[(df['source_id']=='paysage')|(~df.resourceId.isna()), 'in_paysage'] = True
    df.loc[df['in_paysage']!=True, 'in_paysage'] = False
    return df


def check_id_by_source(source_id, id_list):
    """
    check if ID exist in source API, return code 200 if exist, 404 if not exist, 429 if too many requests, None if error
        
    """
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    # Session globale avec configuration des retries
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=8,
        status_forcelist=[500, 502, 503, 504],
        respect_retry_after_header=True
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))

    # Décorateur pour limiter les requêtes à 30/minute
    @sleep_and_retry
    @limits(calls=30, period=60)
    def make_limited_request(url, headers):
        return session.get(url, headers=headers, verify=False, timeout=10)

    def requestor(url, keyw, id):
        if keyw == 'rnsr':
            url2 = f'{url}"{id}"'
        else:
            url2 = f"{url}{id}"

        # Appliquer la limite à toutes les requêtes vers l'API SIRENE
        if keyw in ['siren', 'siret', 'identifiantAssociationUniteLegale']:
            response = make_limited_request(url2, head)
        else:
            # Pour les autres sources, pas de limite, mais un délai raisonnable
            time.sleep(0.5)
            response = session.get(url2, headers=head, verify=False, timeout=10)

        return {
            'checked_id': id,
            'source': keyw,
            'code': response.status_code,
            'error': None
        }

    base = [
        {'source': 'siren', 'url': 'https://api.insee.fr/api-sirene/3.11/siren/', 'h': sirene_headers},
        {'source': 'siret', 'url': 'https://api.insee.fr/api-sirene/3.11/siret/', 'h': sirene_headers},
        {'source': 'rna', 'url': 'https://api.insee.fr/api-sirene/3.11/siret?q=identifiantAssociationUniteLegale:', 'h': sirene_headers},
        {'source': 'rnsr', 'url': 'https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-repertoire-national-structures-recherche/records?select=numero_national_de_structure&where=', 'h': ods_headers},
        {'source': 'ror', 'url': 'https://api.ror.org/organizations?query=', 'h': ror_headers},
        {'source': 'grid', 'url': 'https://api.ror.org/organizations?query.advanced=external_ids.GRID.preferred:', 'h': ror_headers},
        {'source': 'paysage', 'url': 'https://api.paysage.dataesr.ovh/structures/', 'h': paysage_headers},
        {'source': 'uai', 'url': 'https://api.paysage.dataesr.ovh/structures/', 'h': paysage_headers},
        {'source': 'finess', 'url': 'https://public.opendatasoft.com/api/v2/catalog/datasets/finess-etablissements/records?select=', 'h': None}
    ]

    result = []
    for id in id_list:
        for entry in base:
            keyw = entry['source']
            if keyw == source_id:
                if keyw == 'rna':
                    keyw = 'identifiantAssociationUniteLegale'
                url = entry['url']
                head = entry['h']
                try:
                    response = requestor(url, keyw, id)
                    if response['code'] == 429:
                        # Si 429, attendre avant de continuer
                        time.sleep(5)
                        response = requestor(url, keyw, id)
                except Exception as e:
                    response = {
                        'checked_id': id,
                        'source': keyw,
                        'code': None,
                        'error': str(e)
                    }
                result.append(response)
                print(f"{len(result)}, {id}, {keyw}, {response.get('code')}, {response.get('error')}")
    return result