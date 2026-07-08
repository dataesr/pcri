import numpy as np, pandas as pd
from config_path import PATH_CLEAN, PATH_REF
from functions_shared import capitalize_if_all_upper
# from step2_participations.nuts import *


def participations_nuts(df):
    # gestion code nuts
    print("## participations nuts")
    nuts = pd.read_pickle(f'{PATH_REF}nuts_complet.pkl')
    temp=df[['participation_nuts', 'nutsCode']].drop_duplicates()
#501171 507105
    temp=temp.fillna('')
    temp = temp.assign(n1=temp.participation_nuts.str.split(';'), n2=temp.nutsCode.str.split(';'))
    # temp['n1'] = [ [] if (x is np.nan)|(x is None) else x for x in temp['n1'] ]
    # temp['n2'] = [ [] if (x is np.nan)|(x is None) else x for x in temp['n2'] ]
    # temp['n3'] = temp.apply(lambda x: list(set(x['n1'] + x['n2'])), axis=1)
    temp['n3'] = temp.apply(lambda x: x['n1']+x['n2'], axis=1)
    temp = temp.explode('n3').drop(columns=['n1', 'n2']).drop_duplicates().loc[temp.n3!='']
 
    temp=temp.merge(nuts, how='left', left_on='n3', right_on='nuts_code').drop(columns=['nuts_code'])
 
    temp=(temp
        .groupby(['participation_nuts', 'nutsCode'])
        .agg(lambda x: ';'.join(x.dropna()))
        .reset_index()
        .drop_duplicates())
    
    df = (df.merge(temp, how='left', on=['participation_nuts', 'nutsCode'])
            .drop(columns=['participation_nuts', 'nutsCode'])
            .rename(columns={'n3':'participation_nuts'}))
    
    print(f"- size part_step after add nuts: {len(df)}, sans code_nuts: {len(df.loc[(~df.participation_nuts.isnull())&(df.region_1_name.isnull())])}")
    return df


def entities_with_lien(entities_info, df):
    print("### LIEN + entities_info -> pour calculations")
    print(f"- ETAT avant lien ->\ngeneralPic de lien={df.generalPic.nunique()},\ngeneralPic de entities_info={entities_info.generalPic.nunique()}")


    ent_tmp = (entities_info[
            ['generalPic', 'entreprise_flag',
            'cordis_is_sme', 'cordis_type_entity_code', 'cordis_type_entity_name_fr', 
            'cordis_type_entity_name_en', 'cordis_type_entity_acro', 'nutsCode',
            'country_code', 'country_code_source', 'extra_joint_organization']]
            .drop_duplicates())
    ent_tmp['n_pic_cc'] = ent_tmp.groupby(['generalPic', 'country_code_source'])['country_code'].transform('count')

    part_step = (df.merge(ent_tmp,
                how='left', on=['generalPic', 'country_code_source'])
                )

    print(f"- size participations merge with entities: {len(part_step)}\n- columns: {part_step.columns}")

    part_step = participations_nuts(part_step)

    print(f"- size participations merge with nuts: {len(part_step)}\n- columns: {part_step.columns}")

    if len(part_step)==len(df):
        print(f'1- part_step ({len(part_step)}) = lien')
    else:
        print(f"2- lien={len(df)}, part_step={len(part_step)}")
    return part_step

def proj_no_coord(projects):
    return projects[(projects.thema_code.isin(['ACCELERATOR']))|(projects.destination_code.isin(['PF','COST']))|((projects.action_code.isin(['ERC', 'RPR'])))].project_id.to_list()


def participations_calc(lien, proj, entities_info):
    from step4_calculations.participations import entities_with_lien 
    '''Traitement des subventions proposals -> création calculated_applicant_subv'''
    print("\n### CALCULS participations")  

    cols=['project_id', 'generalPic', 'applicant_orderNumber',
       'applicant_participant_pic', 'inProposal', 'applicant_country_code_source', 'applicant_role',
       'applicant_partnerType', 'applicant_erc_role', 'app_fund', 'participation_nuts']
    app = lien.loc[lien.inProposal==True, cols].assign(stage='evaluated')
    rename_cols = {col: col.removeprefix('applicant_') for col in app.columns}
    app = app.merge(proj, how='left', on='project_id', indicator=True).rename(columns=rename_cols)

    # ERC
    app.loc[app._merge=='both', 'fund_ent_erc'] = app.loc[app._merge=='both'].app_fund
    tmp = app.loc[(app._merge=='both')&(app.destination_code!='SyG')]
    app_tmp = tmp[['project_id', 'app_fund']].groupby(['project_id'])['app_fund'].sum().reset_index()

    tmp = tmp.drop(columns='app_fund').merge(app_tmp, how='left', on='project_id')
    tmp.loc[tmp.erc_role!='pi', 'app_fund'] = 0

    app=pd.concat([app[~app.project_id.isin(tmp.project_id.unique())], tmp], ignore_index=True)

    print("PARTICIPANT CALC")
    cols=['project_id', 'generalPic', 'orderNumber', 'participant_pic', 'inProject', 
          'country_code_source', 'role', 'partnerType', 'erc_role', 'part_fund', 'beneficiary_fund',
          'participation_nuts']
    part=lien.loc[lien.inProject==True, cols].assign(stage='successful')
    # ERC
    part.loc[part.project_id.isin(proj.project_id.unique()), 'fund_ent_erc'] = part.loc[part.project_id.isin(proj.project_id.unique())].part_fund
    part.loc[part.project_id.isin(proj.project_id.unique()), 'part_fund'] = part.loc[part.project_id.isin(proj.project_id.unique())].beneficiary_fund


    merged=pd.concat([app, part], ignore_index=True).drop(columns=['_merge', 'inProposal', 'inProject', 'participant_pic', 'destination_code'])

    print("## entities_withh_lien")
    part_step = entities_with_lien(entities_info, merged)


    part_step['calculated_fund'] = (np.where(part_step.stage=='evaluated', 
                                part_step['app_fund']/part_step['n_pic_cc'], part_step['part_fund']/part_step['n_pic_cc']))

    part_step['fund_ent_erc'] = part_step['fund_ent_erc']/part_step['n_pic_cc']
    
    x=part_step[part_step.stage=='successful']
    if len(part) != len(x):
        print(f"2- ATTENTION ! pas le même nbre de lignes-> part_step: {len(x)}, first_part_step: {len(part)}")    

    if '{:,.1f}'.format(x['beneficiary_fund'].sum())=='{:,.1f}'.format(part['beneficiary_fund'].sum()):
        print("3- Etape part_step/part1 -> beneficiary_fund OK")
    else:
        print(f"4- ATTENTION ! Revoir le calcul de beneficiary_fund:{'{:,.1f}'.format(x['beneficiary_fund'].sum())}, euContribution:{'{:,.1f}'.format(part['beneficiary_fund'].sum())}")
        
    if '{:,.1f}'.format(x['calculated_fund'].sum())=='{:,.1f}'.format(part['part_fund'].sum()):
        print("5- Etape part_step/part1 -> calculated_fund OK")
    else:
        print(f"-- ATTENTION ! Revoir le calcul de calculated_other_subv:{'{:,.1f}'.format(x['calculated_fund'].sum())}, netEuContribution:{'{:,.1f}'.format(part['part_fund'].sum())}")

    x=part_step[part_step.stage=='evaluated']
    if '{:,.1f}'.format(app['app_fund'].sum()) == '{:,.1f}'.format(x['calculated_fund'].sum()):
        print("2- requests grants = subventions proposals OK")
    else:
        print(f"3- ATTENTION ! Ecart subventions proposals -> subv_orig:{'{:,.1f}'.format(app['app_fund'].sum())}, après fusion:{'{:,.1f}'.format(x['calculated_fund'].sum())}")

    return part_step



def rnsr_add(df):
        
    rnsr = pd.read_pickle(f"{PATH_REF}rnsr_in_project.pkl")

    add_rnsr = rnsr.groupby('p_key_id')[['numero_national_de_structure', 'libelle']].agg(lambda x: ';'.join(x)).reset_index()
    add_rnsr['structure_name'] = add_rnsr['libelle'].apply(capitalize_if_all_upper)
    add_rnsr[['part1', 'part2', 'generalPic_from_pkey']] = add_rnsr['p_key_id'].str.split('-', expand=True)
    add_rnsr['participation_linked'] = add_rnsr['part1'] + '-' + add_rnsr['part2']
    add_rnsr['generalPic'] = add_rnsr['generalPic_from_pkey']
    add_rnsr = add_rnsr.drop(columns=['part1', 'part2', 'generalPic_from_pkey', 'p_key_id'])

    return df.merge(add_rnsr, how='left', on=['participation_linked', 'generalPic']).drop(columns='libelle')


def participations_finalize(part_step, proj_no_coord):

    print("### PARTICIPATIONS final")
    # participation = pd.concat([part_prop, part_proj], ignore_index=True)

    print(f"- control role: {part_step.role.unique()}")
    # part_step['coordination_number']=np.where(part_step['role'].str.lower()=='coordinator', 1, 0)
    part_step.loc[part_step.project_id.isin(proj_no_coord), 'coordination_number'] = 0
    part_step.loc[~part_step.project_id.isin(proj_no_coord), 'coordination_number'] = 1
    # part_step = part_step.assign(with_coord=True)
    part_step[ 'with_coord'] = np.where(part_step['coordination_number']==1, True, False)

    part_step.loc[part_step.role.isin(['co-pi', 'pi']), 'role'] = part_step.loc[part_step.role.isin(['co-pi', 'pi'])].role.str.upper()
    part_step.loc[part_step.role.isin(['coordinator', 'partner']), 'role'] = part_step.loc[part_step.role.isin(['coordinator', 'partner'])].role.str.capitalize()
    part_step.loc[part_step.erc_role.isin(['pi']), 'erc_role'] = part_step.loc[part_step.erc_role.isin(['pi'])].erc_role.str.upper()

    part_step = (part_step
            .assign(is_ejo=np.where(part_step.extra_joint_organization.isnull(), 'Sans', 'Avec')))
 
    part_step.rename(columns={'partnerType':'participates_as'}, inplace=True)
    part_step['participation_linked'] = part_step['project_id']+"-"+part_step['orderNumber']
    
    part_step =  rnsr_add(part_step)
    
    print(f"- size participation: {len(part_step)}")

    file_name = f"{PATH_CLEAN}participation_current.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(part_step, file)
    return part_step
    

def ent(participation, entities_info, projects):
    import  pandas as pd
    print("### ENTITIES preparation")
    part=(participation[
        ['stage', 'project_id','generalPic', 'role', 'participates_as', 'erc_role', 
        'with_coord', 'is_ejo', 'country_code', 'participation_nuts', 'country_code_mapping',
        'region_1_name', 'region_2_name', 'regional_unit_name','participation_linked', 
        'numero_national_de_structure', 'structure_name',
        'coordination_number', 'calculated_fund', 'beneficiary_fund', 'fund_ent_erc']]
        .assign(number_involved=1))

    print(f"0 - subv={'{:,.1f}'.format(part.loc[(part.country_code=='FRA')&(part.stage=='successful'), 'calculated_fund'].sum())}")

    def ent_stage(df, stage_value:str):
        import numpy as np
        df=(df[df.stage==stage_value]
            .merge(entities_info.drop(columns='country_code'), 
                   how='left', on=['generalPic','country_code_mapping']))
        
        print(f"- subv {stage_value}={'{:,.1f}'.format(df.loc[(df.country_code=='FRA')&(df.stage==stage_value), 'calculated_fund'].sum())}")

        if any(df.id.str.contains(';', na=False)):
            print(f"- Attention multi id pour une participation, calculs sur les chiffres\n {df.loc[df.id.str.contains(';', na=False), 'id'].drop_duplicates()}")
            df['entities_num'] = np.where(df.id.str.contains(';', na=False), df.id.str.split(';').str.len(), 1)
            for i in ['coordination_number', 'calculated_fund', 'beneficiary_fund', 'fund_ent_erc', 'number_involved']:
                df[i] = df[i]/df['entities_num']
        return df
    
    entities_eval = ent_stage(part, 'evaluated')
    print(f"2 - subv={'{:,.1f}'.format(entities_eval.loc[(entities_eval.country_code=='FRA')&(entities_eval.stage=='evaluated'), 'calculated_fund'].sum())}")
    entities_signed = ent_stage(part, 'successful')
    print(f"3 - subv={'{:,.1f}'.format(entities_signed.loc[(entities_signed.country_code=='FRA')&(entities_signed.stage=='successful'), 'calculated_fund'].sum())}")
    entities_part = pd.concat([entities_eval, entities_signed], ignore_index=True)

    mask = (
        entities_part['id_secondaire'].notna() &
        (
            entities_part['numero_national_de_structure'].isna() |
            ~entities_part.apply(lambda x: str(x['id_secondaire']) in str(x['numero_national_de_structure']).split(';'), axis=1)
        )
        )

    entities_part.loc[mask & entities_part['numero_national_de_structure'].notna(), 'numero_national_de_structure'] = (
        entities_part['numero_national_de_structure'] + ';' + entities_part['id_secondaire']
    )

    entities_part.loc[mask & entities_part['numero_national_de_structure'].isna(), 'numero_national_de_structure'] = entities_part['id_secondaire']

    entities_part=(entities_part
                .drop(columns=
                ['generalState', 'street', 'postalCode','postalBox', 'cj_code', 'cj_name', 
                'webPage','naceCode','gps_loc', 'city', 'isNonProfit', 'id', 'id_secondaire',
                'isPublicBody', 'isInternationalOrganisation', 'isResearchOrganisation', 
                'isHigherEducation','legalType', 'naceCode', 'gps_loc'])
                )
    print(f"4 - entities_part subv drop columns={'{:,.1f}'.format(entities_part.loc[(entities_part.country_code=='FRA')&(entities_part.stage=='successful'), 'calculated_fund'].sum())}")

    entities_part=(entities_part
        .groupby(list(entities_part.columns.difference(['coordination_number', 'number_involved', 'calculated_fund', 'beneficiary_fund', 'fund_ent_erc'])), dropna=False, as_index=False).sum()
        .drop_duplicates()
        )

    print(f"5 - entities_part subv groupby and sum={'{:,.1f}'.format(entities_part.loc[(entities_part.country_code=='FRA')&(entities_part.stage=='successful'), 'calculated_fund'].sum())}")

    entities_part = entities_part.map(lambda x: x.strip() if isinstance(x, str) else x)

    print(f"6 - part={'{:,.1f}'.format(entities_part.loc[entities_part.stage=='evaluated', 'calculated_fund'].sum())},participation={'{:,.1f}'.format(participation.loc[participation.stage=='evaluated', 'calculated_fund'].sum())}")
    print(f"7 - part={'{:,.1f}'.format(entities_part.loc[entities_part.stage=='successful', 'calculated_fund'].sum())},participation={'{:,.1f}'.format(participation.loc[participation.stage=='successful', 'calculated_fund'].sum())}")
    print(f"8 - comparaison nb couple genpic + country (doit être égal) {len(entities_part[['generalPic','country_code']].drop_duplicates())},{len(entities_info[['generalPic','country_code']].drop_duplicates())}")

    proj=(projects
        .drop(columns=['call_deadline', 'destination_name_fr', 'duration', 'eic_panels',
                        'end_date', 'eu_reqrec_grant', 'isSeo', 'lastUpdateDate',
                        'nationalContribution', 'number_involved', 'otherContribution',
                        'panel_description', 'project_webpage', 'signature_date', 'start_date',
                        'submission_date', 'title', 'totalGrant', 'total_cost',
                        'typeOfActionCode', 'url'])
        .drop_duplicates()
        )

    # merge inner ; ATTENTION perte de participations baisse des subv
    temp = (entities_part
            .merge(proj, how='inner', on=['project_id', 'stage'])
            .sort_values(['destination_name_en'], ascending=True))
        
    temp = temp.reindex(sorted(temp.columns), axis=1)
    print(f"-size de entities_participation : {len(temp)}\n- {temp.loc[(temp.country_code=='FRA')&(temp.stage=='successful'), 'calculated_fund'].sum()}")
    return temp