from paths import PATH_HARVEST, PATH_WORK, PATH_CLEAN
from remote_process.grist import personsG, update_doc_grist
from functions_shared import last_file_into_folder_by_pat
from remote_process.paysage import get_paysageODS
import pandas as pd, numpy as np

last_file = last_file_into_folder_by_pat(f"{PATH_HARVEST}persons", 'perso_complete', 'pkl')
perso_complete = pd.read_pickle(last_file)
print(f"- size perso_complete: {len(perso_complete)}")

perso_init = pd.read_pickle(f"{PATH_CLEAN}persons_all.pkl")


update_doc_grist(personsG, 'persons')

def fix_identifiers(df):

    # corr = pd.read_excel(f'{PATH_WORK}verification_merge_orcid_idref.xlsx', sheet_name=None, dtype=str)

    # Accéder à chaque onglet par son nom
    corr_orcid = personsG['Orcid_fix']
    corr_idref = personsG['Idref_fix']
    corr_both = personsG['Both_ids']

    #orcid fixed
    corr_orcid = corr_orcid[corr_orcid['orcid_fix'].notna()]

    cols = ['project_id', 'generalPic', 'contact', 'country_code', 'orcid_fix']
    tmp = perso_complete.merge(corr_orcid[cols], how='left').drop_duplicates()

    mask = tmp['orcid_fix'].isnull()
    tmp.loc[mask, 'orcid_fix'] = tmp.loc[mask, 'orcid_id_final']

    #idref fixed
    corr_idref = corr_idref[corr_idref['idref_fix'].notna()]
        
    c = ['last_name', 'first_name', 'idref_source', 'idref_fix']
    tmp = tmp.merge(corr_idref[c], how='left')

    mask = tmp['idref_fix'].isnull()
    tmp.loc[mask, 'idref_fix'] = tmp.loc[mask, 'idref_id']

    mask = tmp['idref_fix'].str.contains('idref', na=False)
    tmp.loc[mask, 'idref_fix'] = tmp.loc[mask, 'idref_fix'].str.replace('idref', '').str.strip()

    #=======================================================
    #both
    corr_both = corr_both.mask(corr_both == '')

    ## refid_fix again

    cb = corr_both[corr_both['idref_fix'].notna()]
    
    cols = ['project_id', 'generalPic', 'contact', 'country_code']
    tmp = tmp.merge(
    cb.loc[cb['idref_fix'] == 'False', cols],
    on=cols,
    how="left",
    indicator=True,
    )

    tmp.loc[tmp["_merge"] == "both", "idref_fix"] = np.nan

    tmp = tmp.drop(columns=['_merge'])

    ## idref new
    cols = ['project_id', 'generalPic', 'contact', 'country_code', 'idref_fix']
    cb = cb.loc[(cb['idref_fix'] != 'False') & cb['idref_fix'].notna(), cols].rename(columns={'idref_fix': 'idref_fix_new'})

    tmp = tmp.merge(cb,
        on=['project_id', 'generalPic', 'contact', 'country_code'],
        how="left"
        )

    mask = tmp['idref_fix_new'].notna()
    tmp.loc[mask, 'idref_fix'] = tmp.loc[mask, 'idref_fix_new']



    #------------------------------------------------
    ## orcid_fix again

    cb = corr_both.copy()
    cb.loc[cb['orcid_fix'].isnull(), 'orcid_fix'] = cb.loc[cb['orcid_fix'].isnull(), 'idref_orcid_found']

    cols = ['project_id', 'generalPic', 'contact', 'country_code']
    tmp = tmp.merge(
    cb.loc[cb['orcid_fix'] == 'False', cols],
    on=cols,
    how="left",
    indicator=True,
    )

    tmp.loc[tmp["_merge"] == "both", "orcid_fix"] = np.nan
    tmp = tmp.drop(columns=['_merge'])

    ## orcid_new
    cols = ['project_id', 'generalPic', 'contact', 'country_code', 'orcid_fix']
    cb = cb.loc[cb['orcid_fix'] != 'False', cols].rename(columns={'orcid_fix': 'orcid_fix_new'})

    tmp = tmp.merge(cb,
        on=['project_id', 'generalPic', 'contact', 'country_code'],
        how="left"
        )

    mask = tmp['orcid_fix_new'].notna()
    tmp.loc[mask, 'orcid_fix'] = tmp.loc[mask, 'orcid_fix_new']



    erc = tmp.loc[(tmp['stage']=='successful') & (tmp['country_code']=='FRA') & (tmp['action_code'] == 'ERC'), ['project_id', 'call_year', 'destination_code', 'generalPic', 'role', 'first_name',
       'last_name', 'contact', 'gender', 'entities_id',
       'entities_name', 'orcid_fix', 'idref_fix']].drop_duplicates()

    dataset = "fr-esr-iuf-les-membres"
    iuf = get_paysageODS(dataset)
    # iuf = iuf.loc[iuf['annee'].astype(int) > 2019]

   
    erc.loc[erc['idref_fix'].isin(iuf['idref'].unique()), 'iuf_flag'] = True

    iuf.loc[iuf['idref'].isin(erc['idref_fix'].unique()), 'erc_flag'] = True    

    iuf = iuf.loc[(iuf['annee'].astype(int) > 2019) | (iuf['erc_flag']==True)]


    mask_all_before_2019 = iuf.groupby('idref')['annee'].transform(lambda x: (x.astype(int) < 2019).all())


    # Lignes concernées par la dédup (tous < 2019) -> on garde la ligne la plus récente par idref
    a_dedupliquer = (
        iuf[mask_all_before_2019]
        .sort_values('annee', ascending=False)
        .drop_duplicates(subset='idref', keep='first')
    )

    # Lignes non concernées (au moins une année >= 2019) -> on garde tout
    a_garder_tel_quel = iuf[~mask_all_before_2019]

    # Résultat final
    resultat = pd.concat([a_dedupliquer, a_garder_tel_quel], ignore_index=True)


    pi = perso_init.loc[(perso_init['stage']=='successful') & (perso_init['country_code']=='FRA') & (perso_init['action_code'] == 'ERC'), ['project_id', 'call_year', 'destination_code', 'generalPic', 'role', 'first_name',
       'last_name', 'contact', 'gender', 'entities_id', 'entities_name']].drop_duplicates()

    