import numpy as np

def participants_calcul(part_step, part):

    print("\n### CALCULS participants")

    '''SUBVENTIONS : mise à zéro les EUsubventions au niveau des non-beneficiaires'''
    # part1['euContribution'] = np.where(part1['partnerType'] != 'beneficiary', 0.0, part1['euContribution'])
    # part1['netEuContribution'] = np.where((part1['euContribution'] != 0.0) & (part1['netEuContribution'] == 0.0), part1['euContribution'], part1['netEuContribution'])

    # print(f"calculated_subv, verif des volumes après traitement-> benef_part1:{'{:,.1f}'.format(part1['euContribution'].sum())}, other_part1:{'{:,.1f}'.format(part1['netEuContribution'].sum())}")

    subv_pt = (part_step.loc[part_step.inProject==True, ['project_id', 'generalPic', 'calculated_pic', 'orderNumber', 'propNlien']]
            .drop_duplicates()
            .merge(part[['project_id', 'generalPic', 'participant_pic', 'orderNumber', 'role', 'partnerType', 'erc_role', 'euContribution', 'netEuContribution']], 
                how='inner',
                left_on=['project_id', 'generalPic', 'calculated_pic', 'orderNumber'],
                right_on=['project_id', 'generalPic', 'participant_pic', 'orderNumber']))

    subv_pt['beneficiary_subv'] = np.where(subv_pt['propNlien']>1, subv_pt['euContribution']/subv_pt['propNlien'], subv_pt['euContribution'])
    subv_pt['calculated_fund'] = np.where(subv_pt['propNlien']>1, subv_pt['netEuContribution']/subv_pt['propNlien'], subv_pt['netEuContribution'])
    subv_pt.drop(['propNlien', 'calculated_pic' ], axis=1, inplace=True)

    if len(subv_pt)!=len(part):
        print(f"1- ATTENTION ! participations perdues entre {len(part)} de part1 et {len(subv_pt)} de subv_pt")

    part_step_first_len=len(part_step)

    part_proj = (part_step.merge(subv_pt, how='inner')[
                ['project_id',  'generalPic', 'orderNumber', 'participant_pic','cordis_is_sme', 
                'flag_entreprise', 'groupe_id', 'groupe_name', 'groupe_acronym', 'participation_linked',
                'cordis_type_entity_code', 'cordis_type_entity_name_fr', 'cordis_type_entity_name_en', 'cordis_type_entity_acro',
                'participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name', 
                'country_code', 'country_code_mapping', 'extra_joint_organization', 'role', 'partnerType', 'erc_role', 
                'calculated_fund', 'beneficiary_subv']]
                .rename(columns={'participant_pic':'pic'})
                .assign(stage='successful'))    

    if part_step_first_len != len(part_step):
        print(f"2- ATTENTION ! pas le même nbre de lignes-> part_step: {len(part_step)}, first_part_step: {part_step_first_len}")    

    if '{:,.1f}'.format(part_proj['beneficiary_subv'].sum())=='{:,.1f}'.format(part['euContribution'].sum()):
        print("3- Etape part_step/part1 -> beneficiary_subv OK")
    else:
        print(f"4- ATTENTION ! Revoir le calcul de beneficiary_subv:{'{:,.1f}'.format(part_proj['beneficiary_subv'].sum())}, euContribution:{'{:,.1f}'.format(part['euContribution'].sum())}")
        
    if '{:,.1f}'.format(part_proj['calculated_fund'].sum())=='{:,.1f}'.format(part['netEuContribution'].sum()):
        print("5- Etape part_step/part1 -> calculated_fund OK")
    else:
        print(f"-- ATTENTION ! Revoir le calcul de calculated_other_subv:{'{:,.1f}'.format(part_proj['calculated_fund'].sum())}, netEuContribution:{'{:,.1f}'.format(part['netEuContribution'].sum())}")
        
    print(f"- size part_prop: {len(part_proj)}")
    return part_proj