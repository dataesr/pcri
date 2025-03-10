import pandas as pd, numpy as np, json
from config_path import PATH_WORK

def merge_ror(entities_tmp, ror, countries):
    print("### merge ROR")
    ccode=json.load(open("data_files/countryCode_match.json"))
    for k,v in ccode.items():
        ror.loc[ror.country_code==k, 'country_code'] = v
        ror.loc[ror.country_code==k, 'country_code'] = v
    ror = (ror
           .merge(countries[['countryCode', 'country_code_mapping']], 
                  how='left', left_on='country_code', right_on='countryCode')
            .drop(columns=['countryCode', 'country_code']))

    entities_tmp = (entities_tmp
                    .merge(ror.drop(columns='country_code_mapping'), 
                           how='left', left_on=['id_extend'], right_on=['id_source'])
                    .drop(columns='id_source')
                    .drop_duplicates())
    print(f"- End size entities_tmp+ror_info: {len(entities_tmp)}")
    if any(entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1):
        entities_tmp[entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1]
    return entities_tmp

def merge_paysage(entities_tmp, paysage, cat_filter):
    print("### merge PAYSAGE")            

    paysage = (paysage
            .rename(columns={'id':'id_extend',
                                'id_clean':'entities_id', 
                                'name_clean':'entities_name', 
                                'acronym_clean':'entities_acronym', 
                                'cj_name':'paysage_cj_name',
                            'siren':'paysage_siren'})
            .drop(columns=['acro_tmp'])
            .drop_duplicates()
            .merge(cat_filter, how='left', left_on='entities_id', right_on='id_clean')
            .drop(columns='id_clean'))

    paysage.loc[paysage.id_extend.str.len()==14, 'id_extend'] = paysage.id_extend.str[0:9]
    paysage = paysage.loc[~(paysage.entities_id.isin(['sJKd8','pG74N']))] # BioEnTech  792918765  

    entities_tmp=(entities_tmp
        .drop_duplicates()
        .merge(paysage, how='left', on='id_extend'))

    entities_tmp.loc[entities_tmp.entities_id.isnull(), 'entities_id'] = entities_tmp.id_clean
    entities_tmp.loc[entities_tmp.entities_name.isnull(), 'entities_name'] = entities_tmp.name_clean
    entities_tmp.loc[entities_tmp.entities_acronym.isnull(), 'entities_acronym'] = entities_tmp.acronym_clean

    entities_tmp = entities_tmp.drop(['id_clean','name_clean','acronym_clean'], axis=1).drop_duplicates()

    if ('legalName' in entities_tmp.columns) & (any(entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1)):
        print(f"- doublons PIC\n{entities_tmp[entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1][['generalPic', 'legalName','country_code_mapping', 'id']]}")
        
    print(f"- End size entities_tmp+paysage_info: {len(entities_tmp)}")
    return entities_tmp


def merge_sirene(entities_tmp, sirene):
    print("### merge SIRENE")
    sirene = sirene.drop_duplicates()
    print(f"- first size sirene : {len(sirene)}")

    sirene=sirene.loc[~sirene.siren.isin(['889664413'])]

    # si doublon siren/siret
    sirene['nb']=sirene.groupby(['siren', 'siret'], as_index=False)['siret'].transform('count')
    sirene=sirene.loc[~((sirene.nb>1)&(sirene.etat_ul=='C'))]
    sirene['nb']=sirene.groupby(['siren', 'siret'], as_index=False)['siret'].transform('count')
    sirene=sirene.sort_values(['siren', 'siret','date_debut'], ascending=False)
    sirene=sirene.groupby(['siren', 'siret']).first().reset_index()

    print(f"- size sirene : {len(sirene)}")


    sirene=sirene.rename(columns={'date_debut':"siret_closeDate"})
    sirene.loc[sirene.etat_ul=='A', 'siren_closeDate']=np.nan

    sirene=sirene.assign(ens=sirene[['ens1', 'ens2', 'ens3']].fillna('').agg(' '.join, axis=1).str.strip()).drop(columns=['ens1', 'ens2', 'ens3'])
    sirene=sirene.assign(nom_perso=sirene[['nom_pp', 'prenom']].fillna('').agg(' '.join, axis=1).str.strip()).drop(columns=['nom_pp', 'prenom'])

    sirene.mask(sirene=='', inplace=True)

    df = (entities_tmp.loc[~entities_tmp.id_extend.isnull(), ['id_extend']]
        .merge(sirene, how='inner', left_on='id_extend', right_on='siret')
        .drop_duplicates().assign(orig="siret"))

    s = sirene.loc[sirene.siege==True]
    df1 = (entities_tmp.loc[~entities_tmp.id_extend.isnull(), ['id_extend']]
        .merge(s, how='inner', left_on='id_extend', right_on='rna')
        .drop_duplicates()
        .assign(orig="rna"))
    df2 = (entities_tmp.loc[~entities_tmp.id_extend.isnull(), ['id_extend']]
        .merge(s, how='inner', left_on='id_extend', right_on='siren')
        .drop_duplicates().assign(orig="siren"))
    df = pd.concat([df, df1, df2], ignore_index=True).drop_duplicates()

    if any(df.loc[df.orig=='siret']):
        print(f"1 - A vérifier -> liste des noms à traiter:\n {df.loc[df.orig=='siret', ['ens', 'denom_us', 'nom_ul']]}\n#####")

    df=df.assign(nom=np.where((df.orig=='siret')&(df.denom_us.isnull()), df.ens, df.denom_us))
    df.loc[df.nom.isnull(), 'nom']=df['nom_ul']
    df.loc[df.nom.isnull(), 'nom']=df['nom_perso']

    if df.loc[df.nom.isnull()].empty:
        pass
    else:
        print(f"2 - compléter code pour récupérer une valeur pour nom manquant - {df.loc[df.nom.isnull()]}")
        
    df['nom']= df.nom.str.capitalize()
    df=df.assign(id_m=np.where(df.orig.isin(['siret', 'rna']), df.siren.fillna('')+' '+df.rna.fillna(''), df.rna))

    df=df[['id_extend', 'nom', 'sigle', 'siret_closeDate', 'id_m', 'siren', 'orig', 'siege']].drop_duplicates()

    entities_tmp = entities_tmp.merge(df, how='left', on='id_extend').drop_duplicates()
    entities_tmp.loc[~(entities_tmp.sigle.isnull())&(entities_tmp.entities_acronym.isnull()), 'entities_acronym'] = entities_tmp['sigle']
    entities_tmp.loc[~(entities_tmp.nom.isnull())&(entities_tmp.entities_name.isnull()), 'entities_name'] = entities_tmp['nom']
    entities_tmp.loc[(entities_tmp.entities_id.isnull())&(entities_tmp.orig=='siret'), 'entities_id'] = entities_tmp['id_extend']
    entities_tmp.loc[~(entities_tmp.siren.isnull())&(entities_tmp.entities_id.isnull()), 'entities_id'] = entities_tmp['siren']


    entities_tmp.drop(columns=['nom','sigle', 'orig'], inplace=True)

    if ('legalName' in entities_tmp.columns)&(any(entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1)):
        print(f"3 - si ++ lignes / pics :\n{entities_tmp[entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1][['generalPic', 'legalName', 'country_code_mapping', 'id']]}")

    print(f"- End size entities_tmp+sirene: {len(entities_tmp)}")
    return entities_tmp