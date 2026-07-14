from paths import PATH_WORK
from functions_shared import convert_lambert_to_gps, chunkify, upper_word_in_text
from remote_process.grist import communesG, countryG
import pandas as pd, numpy as np, json, datetime


"""
script to create files containing the new winning references to be integrated into the application
- sirene
- ror
- pic

"""

def bulk_import_prepare(df, name_df: str):
    print('- data to be integrated into paysage')
    map_dict = json.load(open(f"data_files/ref_cols_import.json", 'r', encoding='utf-8'))
    sir = [dico for data, dico in map_dict.items() if data==name_df]
    cold = [o for o, n in sir[0].items()]
    diff = list(set(cold)-set(df.columns))
    for colonne in diff:
        df[colonne] = None
    df = df[cold].rename(columns=sir[0])

    df.drop_duplicates(inplace=True)

    for i, chunk in enumerate(chunkify(df, 200)):
        # Sauvegarder chaque morceau dans un fichier CSV
        chunk.to_csv(f'{PATH_WORK}bulk_import_{name_df}_{i+1}.txt', index=False, sep='\t', encoding='utf-8', na_rep='', quoting=3)
        print(f"Fichier decoupe_{i+1} sauvegardé.")

def bulk_import_sirene(df, res):

    print("- Preparing sirene for bulk import size sirene:", df.shape)
    laureat = (res
                .loc[res['source_id'].isin(['siren', 'siret', 'rna'])&(res['in_paysage']==False)]
                .groupby(['id', 'source_id'])['project']
                .sum()
                .reset_index()
            )
    laureat = laureat.loc[laureat['project']>0, 'id'].drop_duplicates().to_list()
    df = df.loc[(df['siren'].isin(laureat))|(df['siret'].isin(laureat))|(df['rna'].isin(laureat))].drop_duplicates()
    df = df[(df['cj']!='1000')&(df['diffus']=='O')]

    if not df.empty:
        status=[('C', 'F'), ('A', 'O')]
        for old, new in status:
            df.loc[df['etat_et'] == old, 'etat_et'] = new
            df.loc[df['etat_ul'] == old, 'etat_ul'] = new 

        if any(df['etat_et'].isin(['F','O'])==False):
            print(f"- Warning ! unexpected etat_et values: {df['etat_et'].unique()}")

        if any(df['date_debut'].isna() == True):
            print(f"- Warning ! date_debut should be NaN for all rows\n{df[df.date_debut.isna()]}")

        comG = pd.DataFrame(communesG['Commune'])[['COM_CODE', 'REG_CODE']]
        df = pd.merge(df, comG, how='left', left_on='com_code', right_on='COM_CODE')

        df.loc[df['iso3']=='FRA', 'localisation'] = 'A1'
        df.loc[df['iso3']!='FRA', 'localisation'] = 'A3'
        df.loc[df['REG_CODE']=='00', 'localisation'] = 'A2'

        country_g = pd.DataFrame(countryG['Pays'])[ ['LIBCOG', 'CODEISO2', 'CODEISO3','COG']]
        df = pd.merge(df, country_g[['CODEISO3', 'LIBCOG']], how='left', left_on='iso3', right_on='CODEISO3')

        for v in ['lat', 'long']:
            df.loc[(df[v]=='None')|(df[v]=='[ND]'), v] = np.nan
        # ids autdf
        df['gps'] = df.apply(lambda x: convert_lambert_to_gps(x['lat'], x['long']), axis=1)

        propre = ['paris', 'france', 'europe', 'marseille']
        df['nom'] = df['nom'].apply(lambda x: upper_word_in_text(x, propre))

    # bulk_import_prepare(df, 'sirene')
    return df


def bulk_import_ror(df, res):
    print("- Preparing sirene for bulk import size sirene:", df.shape)
    laureat = (res
                .loc[(res['source_id'].isin(['ror']))&(res['in_paysage']==False)]
                .groupby(['from_id_to_ref', 'source_id'])['project']
                .sum()
                .reset_index()
            )
    laureat = laureat.loc[laureat['project']>0, 'from_id_to_ref'].drop_duplicates().to_list()
    df = df.loc[(df['id_clean'].isin(laureat))].drop(columns='id_source').drop_duplicates()

    if not df.empty:

        status=[('inactive', 'F'), ('active', 'O')]
        for old, new in status:
            df.loc[df['status'] == old, 'status'] = new
        
        if any(df['status'].isin(['F','O'])==False):
            print(f"- Warning ! unexpected etat_et values: {df['status'].unique()}")

        df['date_debut'] = pd.to_datetime(df['year'], format='%Y', errors='coerce').dt.strftime('%Y-%m-%d')

        if any(df['date_debut'].isna() == True):
            print(f"- Warning ! date_debut should be NaN for {len(df[df.date_debut.isna()])} rows")

        df.loc[df['country_code']=='FRA', 'localisation'] = 'A1'
        df.loc[df['country_code']!='FRA', 'localisation'] = 'A3'

        if any(df['latitude'].isna()):
            df.loc[df['latitude'].isna(), 'gps'] = None
        else:
            df['gps'] = df.apply(lambda row: f"{row['latitude']:.3f},{row['longitude']:.3f}", axis=1)

        df['name_usual'] = df['name_usual'].str.replace(r'/', ' ')

        # df.loc[df['relation_type']=='parent', 'parent'] = df.loc[df['relation_type']=='parent', 'relation_id']

    return df

def bulk_import_pic(df):
    df = df.loc[(df['project']>0)&(df['country_code']=='FRA')]
    df = df.drop(columns='generalPic').drop_duplicates()

    if not df.empty:
        mapping={'DEPRECATED':'F', 
                'DECLARED': 'O', 
                'VALIDATED': 'O', 
                'SUSPENDED': 'F', 
                'SLEEPING': 'O',
                'BLOCKED':'O'}
        
        for k, v in mapping.items():
            df.loc[df['generalState']==k, 'status'] = v
        
        df[ 'localisation'] = 'A1'

    return df