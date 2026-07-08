from config_path import PATH_DATA, PATH_REF, PATH_HARVEST
from functions_shared import extract_json_from_file
from config_api import ods_headers
from remote_process.rnsr import get_rnsr_by_id
import pandas as pd, numpy as np, requests, json, time


def load_inHouse(file_load):
    PATH_UNIT = f"{PATH_DATA}data_unit/"
    path_load = f"{PATH_UNIT}in_progress/"

    file_path = f"{path_load}{file_load}.json"
    try:
        list_dict = extract_json_from_file(file_path)
        print(list_dict[:2])  # Affiche les deux premiers éléments pour vérification
    except Exception as e:
        print(f"Erreur : {e}")
    df = pd.DataFrame(list_dict).replace('#', np.nan)
    df = df.mask(df=='')
    return df


def prepare_in_House():

    link = load_inHouse('pcrdt_mult')
    base = load_inHouse('pcrdt')
    ref = load_inHouse('referentiel')
    patch = json.load(open("data_files/matching_fix_patch.json"))   

    #link
    tmp = link.loc[link['p_key'].isin(['103', '104', '105'])]
    link = link.loc[link['id_ref']!='20134']
    link = link.loc[link['id_ref']!='0']
    link = pd.concat([link, tmp], ignore_index=True)
    link = link.drop_duplicates()
    

    ref = ref[['id_ref', 'orig_ref', 'num_nat_struct', 'ror', 'siren', 'siret', 'rna', 'paysage']].drop_duplicates()

    link = pd.merge(link, ref, how='left', on='id_ref')
    print(link['orig_ref'].value_counts())

    cols = ['num_nat_struct', 'paysage', 'siret', 'siren', 'ror']
    link[cols] = link[cols].apply(lambda x: x.fillna('').astype(str).str.replace(' ', ';'))
    link = link.mask(link=='')

    for p1, p2 in patch['num_nat_struct'].items():
        link.loc[link['paysage']==p1, 'num_nat_struct'] = np.nan

    rnsr = (link.loc[(link['orig_ref']=='rnsr')|(~link['num_nat_struct'].isnull()),
            ['p_key', 'orig_ref', 'num_nat_struct']]
            .sort_values(['p_key'])
            )

    paysage = (link.loc[(link['orig_ref']=='paysage') & (link['num_nat_struct'].isnull()),
            ['p_key', 'orig_ref', 'paysage', 'siret']]
            .sort_values(['p_key'])
            )

    sirene = (link.loc[(link['orig_ref'].isin(['sirene']))&(link['paysage'].isnull()),
            ['p_key', 'orig_ref', 'siret', 'siren']]
            .sort_values(['p_key'])
            )
    print(sirene[sirene['siret'].isnull()])

    ror = (link.loc[(link['orig_ref'].isin(['ror']))&(link['paysage'].isnull()),
            ['p_key', 'orig_ref', 'ror']]
            .sort_values(['p_key'])
            .assign(ror=link['ror'].str[1:])
            )

    ids = pd.concat([rnsr, paysage, sirene, ror], ignore_index=True)

    tmp = base[['statut', 'p_key', 'p_key_id', 'pays_dept', 'pays', 'pcrdt_pic']].drop_duplicates()
    
    for p1, p2 in patch['p_key_id'].items():
        tmp.loc[tmp['p_key']==p1, 'p_key_id'] = p2

    tmp.loc[tmp['p_key_id'].str.contains(r"[0-9]{9,}-[0-9]{1,}$"), 'p_key_id'] = tmp['p_key_id'] + "-" + tmp['pcrdt_pic']
    if len(tmp[~tmp['p_key_id'].str.contains(r"[0-9]{9,}-[0-9]{1,}")])>0:
        print(f'- WARNING ! fix p_key_id {set(tmp.loc[~tmp["p_key_id"].str.contains(r"[0-9]{9,}-[0-9]{1,}"), "p_key_id"])}')

    tmp = pd.merge(tmp, ids, how='inner', on='p_key')
    tmp = tmp.mask(tmp=='')
    return tmp.drop_duplicates()


def inHouse_unit(rnsr_exist=False):
    
    print("### load gilberinette and prepare")
    df = prepare_in_House()

    #### RNSR ######################

    print(f"### load ods rnsr start -> {time.strftime('%H:%M:%S')}")
    url = "https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-repertoire-national-structures-recherche/exports/json"
    response = requests.get(url, headers=ods_headers)
    rods = response.json()
    rods = pd.DataFrame(rods)
    rods.to_pickle(f"{PATH_HARVEST}rnsr.pkl")

    rods = rods[['numero_national_de_structure', 'etat', 'annee_de_fermeture']]
    
    r = df.loc[~df['num_nat_struct'].isnull(), ['p_key_id', 'orig_ref', 'num_nat_struct']]
    r['nns'] = r['num_nat_struct'].str.split(';')
    r = r.explode('nns').drop_duplicates().sort_values(['p_key_id', 'nns', 'orig_ref'], ascending=[True, True, False])
    r = pd.merge(r, rods, how='left', left_on='nns', right_on='numero_national_de_structure')

    # multi rnsr in paysage
    rp = r[r.num_nat_struct.str.contains(';')]
    # remove rp from r
    r = r[~r.set_index(['p_key_id', 'orig_ref', 'num_nat_struct', 'nns']).index.isin(
                rp.set_index(['p_key_id', 'orig_ref', 'num_nat_struct', 'nns']).index
            )
        ].reset_index(drop=True)

    # clean duplicated
    rp = rp.groupby(['p_key_id', 'num_nat_struct'], group_keys=False).apply(lambda x: x[x['etat'] != 'Inactive'])
    
    r = pd.concat([r, rp], ignore_index=True)

    r = r.drop_duplicates(subset=['p_key_id', 'nns'], keep='first')
    r['nb'] = r.groupby(['p_key_id', 'nns'])['orig_ref'].transform('nunique')
    if len(r[r.nb>1])>1:
        print(f"- Warning ! multi source for the same nns {r[r.nb>1]}")
    
    r = (r.assign(orig_ref='rnsr')
         .drop(columns=['num_nat_struct', 'numero_national_de_structure', 'nb'])
         .drop_duplicates())

    if rnsr_exist==False:
        rnsr = get_rnsr_by_id(list(r['nns'].unique()))
        rnsr = pd.DataFrame(rnsr)
        rnsr = rnsr.mask(rnsr=='')
        rnsr.to_pickle(f"{PATH_REF}rnsr.pkl")
    else:
        rnsr=pd.read_pickle(f"{PATH_REF}rnsr.pkl")
        # rnsr=rnsr.drop(columns=['annee_de_fermeture', 'etat'])

    rnsr2 = pd.merge(r, rnsr, how='left', left_on='nns', right_on='numero_national_de_structure')
    if any(rnsr2['numero_national_de_structure'].isnull()):
        print(f"{rnsr2[rnsr2['numero_national_de_structure'].isnull][['num_nat_struct', 'orig_ref', 'p_key_id']]}")
    else:
        print("- all RNSR exist")

    rnsr2 = rnsr2[['p_key_id', 'orig_ref', 'numero_national_de_structure',
                'annee_de_fermeture', 'adresse', 'code_postal', 'commune', 'libelle', 'sigle',
                 'label_numero', 'siret_des_tutelles', 'uai_des_tutelles', 'code_de_type_de_tutelle']]


    # rnsr2 = rnsr2.loc[rnsr2['etat']=='Inactive', 
    #     ['numero_national_de_structure', 'libelle', 'code_de_type_de_succession',
    #     'numero_de_structure_historique',
    #     'type_de_succession', 'etat', 'annee_de_fermeture']
    #     ]
    # rnsr2 = rnsr2.loc[rnsr2['annee_de_fermeture'].astype(int)<2021].drop_duplicates()

    # OTHER ###############################
    r = df.loc[df['num_nat_struct'].isnull(), ['p_key_id', 'orig_ref', 'paysage', 'siret', 'siren', 'ror']].drop_duplicates().fillna('')
    r['tmp'] = r['siret'].str.split(';').apply(lambda x: ';'.join([i[0:9] for i in x if x is not None]))
    r.loc[r['siren']=='', 'siren'] = r.loc[r['siren']=='', 'tmp']
    r = r.drop(columns='tmp')

    r = r.mask(r=='')
    
    rnsr2.to_pickle(f"{PATH_REF}rnsr_in_project.pkl")

    return rnsr2, r

