# import numpy as np, pandas as pd
# from config_path import PATH_CLEAN
# from step2_participations.nuts import *


def participations_nuts(df):
    import pandas as pd, numpy as np
    from config_path import PATH_REF
    # gestion code nuts
    nuts = pd.read_pickle(f'{PATH_REF}nuts_complet.pkl')
    temp=df[['participation_nuts', 'nutsCode']].drop_duplicates()

    temp = temp.assign(n1=temp.participation_nuts.str.split(';'), n2=temp.nutsCode.str.split(';'))
    temp['n1'] = [ [] if (x is np.nan)|(x is None) else x for x in temp['n1'] ]
    temp['n2'] = [ [] if (x is np.nan)|(x is None) else x for x in temp['n2'] ]
    temp['n3'] = temp.apply(lambda x: list(set(x['n1'] + x['n2'])), axis=1)
    temp = temp.explode('n3').drop(columns=['n1', 'n2']).drop_duplicates()
 
    temp=temp.merge(nuts, how='left', left_on='n3', right_on='nuts_code').drop(columns=['nuts_code'])
 
    temp=(temp
        .groupby(['participation_nuts', 'nutsCode'])
        .agg(lambda x: ';'.join(x.dropna()))
        .reset_index()
        .drop_duplicates())
    
    df = (df.merge(temp, how='left', on=['participation_nuts', 'nutsCode'])
            .drop(columns=['participation_nuts', 'nutsCode'])
            .rename(columns={'n3':'participation_nuts'}))
    print(f"size part_step after add nuts: {len(df)}, sans code_nuts: {len(df.loc[(~df.participation_nuts.isnull())&(df.region_1_name.isnull())])}")
    return df


def entities_with_lien(entities_info, lien, genPic_to_new):
    from config_path import PATH_CLEAN
    print("### LIEN + entities_info -> pour calculations")
    print(f"- ETAT avant lien ->\ngeneralPic de lien={lien.generalPic.nunique()},\ngeneralPic de entities_info={entities_info.generalPic.nunique()}")

    lien = lien.merge(genPic_to_new, how='left', on=['generalPic', 'country_code_mapping'])
    lien = lien.rename(columns={'generalPic':'pic_old', 'pic_new':'generalPic'})
    lien.loc[lien.generalPic.isnull(), 'generalPic'] = lien.loc[lien.generalPic.isnull(), 'pic_old']

    ent_tmp = (entities_info[
            ['generalPic', 'flag_entreprise',
            'cordis_is_sme', 'cordis_type_entity_code', 'cordis_type_entity_name_fr', 
            'cordis_type_entity_name_en', 'cordis_type_entity_acro', 'nutsCode',
            'country_code', 'country_code_mapping', 'extra_joint_organization']]
            .drop_duplicates())
    ent_tmp['n_pic_cc'] = ent_tmp.groupby(['generalPic', 'country_code_mapping'])['country_code'].transform('count')

    part_step = (lien.merge(ent_tmp,
                how='left', on=['generalPic', 'country_code_mapping'])
                .drop(columns={'n_app', 'n_part', 'participant_pic',  'nuts_participant', 'nuts_applicants'})
                # .rename(columns={ 'nutsCode':'entities_nuts', 'nuts_code':'participation_nuts'})
                .drop_duplicates())

    part_step = participations_nuts(part_step)

    if len(part_step)==len(lien):
        print(f'1- part_step ({len(part_step)}) = lien')
    else:
        print(f"2- lien={len(lien)}, part_step={len(part_step)}")
    
    part_step.to_pickle(f"{PATH_CLEAN}participation_complete.pkl")
    return part_step

def proj_no_coord(projects):
    return projects[(projects.thema_code.isin(['ACCELERATOR']))|(projects.destination_code.isin(['PF','COST']))|((projects.thema_code=='ERC'))].project_id.to_list()

def participations_complete(part_prop, part_proj, proj_no_coord):
    from config_path import PATH_CLEAN
    import numpy as np, pandas as pd
    print("### PARTICIPATIONS final")
    participation = pd.concat([part_prop, part_proj], ignore_index=True)

    print(f"- control role: {participation.role.unique()}")
    participation['coordination_number']=np.where(participation['role']=='coordinator', 1, 0)
    participation.loc[participation.project_id.isin(proj_no_coord), 'coordination_number'] = 0
    participation = participation.assign(with_coord=True)
    participation.loc[participation.project_id.isin(proj_no_coord), 'with_coord'] = False

    participation = (participation
            .assign(is_ejo=np.where(participation.extra_joint_organization.isnull(), 'Sans', 'Avec')))
 
    participation.rename(columns={'partnerType':'participates_as'}, inplace=True)
    participation['participation_linked'] = participation['project_id']+"-"+participation['orderNumber']
    

    print(f"- size participation: {len(participation)}")

    file_name = f"{PATH_CLEAN}participation_current.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(participation, file)
    return participation
    
def ent(participation, entities_info, projects):
    import  pandas as pd
    print("### ENTITIES preparation")
    part=participation[
        ['stage', 'project_id','generalPic', 'role', 'participates_as', 'erc_role', 
         'with_coord', 'is_ejo', 'country_code', 'participation_nuts', 'country_code_mapping',
         'region_1_name', 'region_2_name', 'regional_unit_name','participation_linked',
        'coordination_number', 'calculated_fund', 'beneficiary_subv', 'fund_ent_erc']].assign(number_involved=1)

    def ent_stage(df, stage_value:str):
        import numpy as np
        df=(df[df.stage==stage_value]
            .merge(entities_info.drop(columns='country_code_mapping'), 
                   how='left', on=['generalPic','country_code']))
        
        if any(df.id.str.contains(';', na=False)):
            print(f"- Attention multi id pour une participation, calculs sur les chiffres\n {df.loc[df.id.str.contains(';', na=False), 'id'].drop_duplicates()}")
            df['nb'] = df.id.str.split(';').str.len()
            for i in ['coordination_number', 'calculated_fund', 'beneficiary_subv', 'fund_ent_erc', 'number_involved']:
                df[i] = np.where(df['nb']>1, df[i]/df['nb'], df[i])
        return df
    
    entities_eval = ent_stage(part, 'evaluated')
    entities_signed = ent_stage(part, 'successful')
    entities_part = pd.concat([entities_eval, entities_signed], ignore_index=True)

    # mask=(entities_part.entities_id.str.contains('^gent', na=False))&(~entities_part.entities_acronym_source.isnull())
    # r=(entities_part.loc[~entities_part.entities_name.isnull(), ['generalPic', 'entities_id','entities_name', 'entities_acronym']]
    # .drop_duplicates())

    # entities_part['entities_acronym'] = entities_part[['entities_acronym']].fillna('')
    # r['entities_name'] = r.apply(lambda x: x['entities_acronym'] if x["entities_name"].isnull() else x['entities_name'], axis=1)
    # r['entities_name'] = r.apply(lambda x: x['entities_name'] if x["entities_acronym"].upper() in x["entities_name"].upper() else x['entities_name']+' '+x["entities_acronym"].lower(),axis=1)
    # entities_part = (entities_part.drop(columns=['entities_name', 'entities_acronym'])
    #                 .merge(r.drop(columns='entities_acronym'), how='left', on=['generalPic', 'entities_id'])
    # )

    entities_part=(entities_part
                .drop(columns=
                ['generalPic','generalState', 'street', 'postalCode','postalBox',
                'webPage','naceCode','gps_loc', 'city', 'countryCode','isNonProfit',  
                'cat_an','isPublicBody', 'isInternationalOrganisation', 'isResearchOrganisation', 
                'isHigherEducation','legalType', 'vat', 'legalRegNumber', 
                'naceCode', 'gps_loc', 'id', 'id_m', 'siret_closeDate','siren']))

    entities_part=(entities_part
        .groupby(list(entities_part.columns.difference(['coordination_number', 'number_involved', 'calculated_fund', 'beneficiary_subv', 'fund_ent_erc'])), dropna=False, as_index=False).sum()
        .drop_duplicates()
        )

    entities_part = entities_part.map(lambda x: x.strip() if isinstance(x, str) else x)

    print(f"1 - part={'{:,.1f}'.format(part.loc[part.stage=='evaluated', 'calculated_fund'].sum())},participation={'{:,.1f}'.format(participation.loc[participation.stage=='evaluated', 'calculated_fund'].sum())}")
    print(f"2 - part={'{:,.1f}'.format(part.loc[part.stage=='successful', 'calculated_fund'].sum())},participation={'{:,.1f}'.format(participation.loc[participation.stage=='successful', 'calculated_fund'].sum())}")
    print(f"3 - comparaison nb couple genpic + country (doit être égal) {len(part[['generalPic','country_code']].drop_duplicates())},{len(entities_info[['generalPic','country_code']].drop_duplicates())}")

    proj=(projects[
        ['project_id', 'call_id', 'stage', 'status_code', 'framework', 'ecorda_date',
        'call_year','topic_code', 'topic_name', 'action_code', 'action_name', 'action_code2', 
        'action_name2', 'panel_code', 'panel_name', 'panel_regroupement_code', 'panel_regroupement_name',
        'pilier_code', 'pilier_name_en', 'pilier_name_fr', 'programme_code', 'programme_name_en',
        'programme_name_fr', 'thema_code', 'thema_name_fr', 'thema_name_en', 'destination_code',
        'destination_lib', 'destination_name_en', 'destination_detail_code', 
        'destination_detail_name_en', 'free_keywords', 'abstract', 'acronym']]
        .drop_duplicates()
        )

    temp = (entities_part
            .merge(proj, how='inner', on=['project_id', 'stage'])
            .sort_values(['destination_name_en'], ascending=True))
        
    temp = temp.reindex(sorted(temp.columns), axis=1)
    print(f"size de entities_participation : {len(temp)}")
    return temp