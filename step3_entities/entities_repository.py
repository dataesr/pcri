
from config_path import PATH_CLEAN
from config_url import grist_url
from remote_process.grist import load_to_grist
from remote_process.sirene import get_sirene
from remote_process.ror import get_ror
from remote_process.paysage import * 
from remote_process.ID_getSourceRef import get_source_ID, fix_source_from_siren_to_ror
from step3_entities.references import ref_source_2d_select, ref_source_load
from step3_entities.entities_select import *
from step3_entities.first_update import *
from remote_process.ID_checkingRefExist import *
from step3_entities.entities_get_pic import *
from step3_entities.ID_checkingResult import *
from step3_entities.bulk_import import bulk_import_sirene, bulk_import_prepare, bulk_import_ror, bulk_import_pic
from step3_entities.merge_referentiels import merge_id_to_ref, merge_paysage, merge_ror, merge_sirene
from step3_entities.groupes import *
from step3_entities.ID_pic_group import *
from step3_entities.entities_cleaning import add_countries_info, entities_check_null
from step3_entities.categories import legal_category, category_paysage_ref, category_paysage_by_struct, naf_etab_sirene, cat_entreprise, category_woven, category_agreg, cordis_type



def entities_repository_select_maj(frameworks:list, countries, load_url, UPDATE_PAYSAGE:bool=False):
    """
    prepare new ref and convert IDs to paysage IDs and update paysage repository
    1. load new repository with pic=IDs
    2. keep H20+HE
    3. manage multi IDs
    4. fix IDs affiliated with siren in place of ROR
    5. create files to import into Paysage application
    """
    
    ref_source = ref_source_load('ref')
    ref, genPic_to_new = ref_source_2d_select(ref_source, frameworks)

    ref['id_extend'] = ref['id'].str.split(r'[; ]')
    ref_id = ref.explode(['id_extend']).reset_index(drop=True)
    ref_id['id_extend'] = ref_id['id_extend'].str.strip()
    ref_id.loc[(ref_id['id_extend'].str.startswith('R0', na=False)), 'id_extend'] = ref_id.loc[(ref_id['id_extend'].str.startswith('R0', na=False)), 'id_extend'].str[1:]
    ref_id = get_source_ID(ref_id.drop(columns='source_id'), 'id_extend')
    ref_id = fix_source_from_siren_to_ror(ref_id, 'id_extend')
    print(f"- size ref_id {len(ref_id)}")


    #referentiels
    # traiter ror avant pour être sûr de paysager le last ID
    ror = get_ror(ref_id, 'id_extend', countries, load_url)

    # replace id_extend with from_id_to_ref (ROR parents/successor)
    ref_id = pd.merge(ref_id, 
                    ror[['id_source', 'id_clean']]
                    .rename(columns={'id_source':'id_extend', 'id_clean': 'from_id_to_ref'}), 
                    how='left', 
                    on='id_extend')
    ref_id.loc[ref_id['from_id_to_ref'].isnull(), 'from_id_to_ref'] = ref_id.loc[ref_id['from_id_to_ref'].isnull(), 'id_extend']
    print(f"- size ref_id {len(ref_id)}")

    ### to execute only if needed -> create files with paysage models 
    if UPDATE_PAYSAGE==True:
        ref_with_paysage = merge_id_to_ref(ref_id, 'from_id_to_ref')
        sirene = get_sirene(ref_with_paysage, 'from_id_to_ref')
        sirene = bulk_import_sirene(sirene, ref_with_paysage)
        if len(sirene)>0:
            bulk_import_prepare(sirene, 'sirene')

        ror = bulk_import_ror(ror, ref_with_paysage)
        if len(ror)>0:
            bulk_import_prepare(ror, 'ror')

    ref_id = ref_id.rename(columns={'id':'id_first'})
    load_to_grist(ref_id, grist_url, 'pcri', 'identification', 'from_pic_to_id')
    load_to_grist(genPic_to_new, grist_url, 'pcri', 'identification', 'from_oldpic_to_new')

    return ref_id, genPic_to_new


def maj_ref_by_pic(entities_info, countries, genPic_to_new, ref_id):
    # adapter les données brutes d'entities pour les structures non liées à un référentiel -> toute la base est traitée
    pic = get_pic(entities_info, genPic_to_new, countries)
    print(f"- size pic {len(pic)}")
    pic2 = (pd.merge(pic, ref_id.loc[ref_id['id_first'].notnull(), 
                        ['generalPic', 'country_code_source']]
                        .drop_duplicates(), 
                        how='outer', 
                        on=['generalPic', 'country_code_source'], 
                        indicator=True)
                        .query("_merge=='left_only'")
                        .drop(columns='_merge')
        )

    pic2 = bulk_import_pic(pic2)
    print(f"- size pic to import {len(pic2)}")
    if len(pic2)>0:
        bulk_import_prepare(pic2, 'pic')
    return pic


def paysage_repository(PAYSAGE_GET_INFO:bool=False):
    """
    create paysage repository with all winning entities  fr+foreign in paysage application
    load all IDS and categories + legal categories (INSEE) + mires
    functions on api.process.paysage
    
    """
    if PAYSAGE_GET_INFO==True:
        paysage_getRefInfo()
        paysage_category = IDpaysage_category()
        paysage_cj = legal_category()
        # get_paysage_cat_entreprise(paysage_siret)
        paysage_mires = get_mires()
    else:
        paysage_mires = pd.read_pickle(f"{PATH_HARVEST}operateurs_mires.pkl")
        paysage_category = pd.read_pickle(f"{PATH_HARVEST}paysage_category.pkl")
        paysage_cj = legal_category()


    ### prepare category paysage for struct paysage or other
    cat = category_paysage_ref()
    cat_filter = category_paysage_by_struct(paysage_category, paysage_mires, cat)
    return paysage_cj, cat, cat_filter


def merge_repositories(df, paysage_cj, cat, cat_filter):
    """
    whole repository with winning and evaluated entities from sirenen, ror and paysage
    """

    # PAYSAGE
    paysage = pd.read_pickle(f"{PATH_HARVEST}paysage_df.pkl")
    df = merge_paysage(df, paysage, cat_filter)
    print(f"- after merge paysage ref to entities_tmp : {df.columns}")
    # reporting.append({'stage_process':'process_paysage', 'entities_size':len(entities_tmp)})

    # ROR
    ### si besoin de charger ror pickle
    ror = pd.read_pickle(f"{PATH_REF}ror.pkl")
    df = merge_ror(df, ror, cat, paysage_cj)
    print(f"- after merge ROR ref to entities_tmp : {df.columns}")
    # reporting.append({'stage_process':'process_ror', 'entities_size':len(entities_tmp)})

    # SIRENE
    ### si besoin de charger paysage pickle
    sirene = pd.read_pickle(f"{PATH_REF}sirene.pkl")
    sirene = naf_etab_sirene(sirene)
    df = merge_sirene(df, sirene, cat, paysage_cj)
    print(f"- after merge SIRENE ref to entities_tmp : {df.columns}")
    # reporting.append({'stage_process':'process_sirene', 'entities_size':len(df)})

    return df.drop_duplicates()


def entities_groupe(df, framework:str=None):
    print("### groupe")

    paysage_siret = pd.read_pickle(f"{PATH_HARVEST}paysage_siret.pkl")
    paysage_siret = paysage_siret.mask(paysage_siret=='')
    paysage_siret.loc[paysage_siret['siren_end_date'].notna(), 'endyear'] = paysage_siret.loc[paysage_siret['siren_end_date'].notna(), 'siren_end_date'].str[:4].astype('Int64')
    print(f"-1 size paysage_siret before cleaning: {len(paysage_siret)}")

    paysage_siret = paysage_siret[paysage_siret['id_clean'].isin(df['entities_id'].unique())]
    print(f"-2 size paysage_siret after selecting IDs HE: {len(paysage_siret)}")

    paysage_siret.sort_values(['id_clean', 'siren_main', 'active', 'endyear'], ascending=[True, True, False, False], na_position='first', inplace=True)
    paysage_siret = paysage_siret.drop_duplicates(subset=['id_clean', 'siren_main'], keep='first')

    tmp = paysage_siret[paysage_siret.groupby('id_clean')['siren_main'].transform('nunique') > 1]
    if len(tmp)>0:
        print("-3 ATTENTION ! several siren per id_clean check endyear and activity")
        paysage_siret = paysage_siret.groupby('id_clean', as_index=False).agg(lambda x: ';'.join(x.dropna().unique().astype(str))).drop_duplicates()
    
    
    paysage_siret = paysage_siret.mask(paysage_siret=='')
    print(f"-4 size paysage_siret after group: {len(paysage_siret)}")

    print(f"- size entities_tmp before add groupe: {len(df)}")
    df = (pd.merge(df, 
                    paysage_siret[['id_clean', 'siren', 'siren_main']].rename(columns={'siren':'siren_all'}).drop_duplicates(), 
                    how='left', left_on='entities_id', right_on='id_clean')
            .drop(columns='id_clean'))


    # create var siren_all -> paysage_siren + siren
    df.loc[df['source_id'].isin(['siren', 'siret']), 'siren_main'] = df.loc[df['source_id'].isin(['siren', 'siret']), 'entities_id'].str[:9]
    df.loc[(df['siren_all'].isnull()) & (df['siren_main'].notnull()), 'siren_all'] = df.loc[(df['siren_all'].isnull()) & (df['siren_main'].notnull()), 'siren_main'] 
    df.loc[df['siren_all'].notnull(), 'siren_all'] = df.loc[df['siren_all'].notnull(), 'siren_all'].apply(lambda x: ';'.join(set(x.split(';'))))


    # # groupe
    print("### ADD GROUPE")
    filename = f"{framework}_groupe.pkl" if framework != None else "groupe.pkl"
    groupe = pd.read_pickle(f"{PATH_REF}{filename}")
    print(f"taille de entities_tmp avant groupe:{len(df)}")
    df = merge_groupe(df, groupe)
    # # reporting.append({'stage_process':'process_groupe', 'entities_size':len(entities_tmp)})
    return df


def entities_categories(df):
    # catégorie entreprise
    print("### CAT ENTREPRISE")
    df = cat_entreprise(df)

    # # traitement catégorie
    df = cordis_type(df)
    df = category_woven(df)
    df = category_agreg(df)
    return df


def entities_finalize(df, countries,  framework:str=None):
    print("### entities finalize")
    # # add countries infos and rename and remove useless columns
    entities_info = add_countries_info(df, countries)


    entities_info = (entities_info.drop(columns=[
                    'source_id_source', 
                    'category_name', 'vat',
                    'legalRegNumber',
                    'link_to_ref', 'id_extend',
                    'paysage_category_priority',
                    'siren_main'])
                .drop_duplicates()
            .rename(columns={'businessName':'entities_acronym_source',
                             'legalName':'entities_name_source'})
    )


    entities_check_null(entities_info)

    #check entities with pic_id
    print("### check enties fr avec id commençant par pic")
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    print(entities_info[(entities_info['country_code']=='FRA')&(entities_info['entities_id'].str.contains('pic'))][['entities_id', 'entities_name']])
    # reporting.append({'stage_process':'process_entities_info', 'entities_size':len(entities_info)})


    file_name = f"{framework}_entities_info.pkl" if framework != None else "entities_info_current2.pkl"
    with open(f"{PATH_CLEAN}{file_name}", 'wb') as file:
        pd.to_pickle(entities_info, file)

    return entities_info