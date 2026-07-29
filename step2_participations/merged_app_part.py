# def merged_partApp(app1, part):
#     """
#     merge app1 and part to create a table with all the possible links between applicants and participants, 
#     with the most complete information possible on each of them.
#     The merge is done in several steps to try to keep the most complete information possible on each link,
#     and to be able to identify the links that are only in app1 or only in part, and to be able to identify the links that are in both but with different information (e.g. orderNumber, participant_pic).   
#     The final table will be used to create the table of participations with the most complete information possible on each participation, and to be able to identify the participations that are only in app1 or only in part, and to be able to identify the participations that are in both but with different information (e.g. orderNumber, participant_pic).   
#     """

#     import pandas as pd, numpy as np
#     print("\n### create LIEN")

#     part2 = part[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'n_part']].drop_duplicates()
#     app2 = app1[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'n_app']].drop_duplicates()
#     print(f'app2: {len(app2)} part2:{len(part2)}')
#     cols_part = part2.columns
#     cols_app = app2.columns

#     lien = pd.DataFrame(columns=['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'n_app',
#        'base_only', 'n_part', 'orderNumber_p', 'participant_pic_p'])

#     '''proposal uniquement'''
#     lien1 = (app2
#             .merge(part2[['project_id']], how='outer', on='project_id', indicator=True)
#             .query('_merge == "left_only"')
#             .drop('_merge', axis=1)
#             .assign(base_only='prop_only'))
#     app2 = (app2.merge(lien1[['project_id']], how='outer', on='project_id', indicator=True)
#             .query('_merge == "left_only"')
#             .drop('_merge', axis=1))
#     print(f'- applicant uniquement lien1: {len(lien1)} reste à croiser -> app2: {len(app2)}')
#     lien = pd.concat([lien, lien1], ignore_index=True)

#     '''jointure parfaite'''
#     lien2 = part2.merge(app2, how='inner')
#     print(f'jointure parfaite -> lien2: {len(lien2)}')
#     lien = pd.concat([lien, lien2], ignore_index=True)

#     part3 = (part2.merge(lien2[cols_part], how='outer', indicator=True)
#              .query('_merge == "left_only"')
#              .drop('_merge', axis=1))
#     app3 = (app2.merge(lien2[cols_app], how='outer', indicator=True)
#             .query('_merge == "left_only"')
#             .drop('_merge', axis=1))
#     print(f'reste à croiser -> app3: {len(app3)} part3: {len(part3)}')

#     '''jointure sans orderNumber'''
#     lien3 = part3.merge(app3, how='inner', on=['project_id', 'generalPic', 'participant_pic'], suffixes=('', '_p'))
#     print(f'jointure sans ordernumber -> lien3: {len(lien3)}')
#     if len(lien3)>0:
#         lien = pd.concat([lien, lien3], ignore_index=True)

#     part4 = part3.merge(lien3[cols_part], how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
#     df_app = lien3[['project_id', 'generalPic', 'participant_pic', 'orderNumber_p']].rename(columns=({'orderNumber_p':'orderNumber'}))
#     app4 = (app3.merge(df_app, how='outer', indicator=True)
#             .query('_merge == "left_only"')
#             .drop('_merge', axis=1))
#     print(f'reste à croiser -> app4: {len(app4)} part4: {len(part4)}')


#     '''jointure sans participant_pic'''
#     lien4 = part4.merge(app4, how='inner', on=['project_id', 'generalPic', 'orderNumber'], suffixes=('', '_p'))
#     print(f'jointure sans participant_pic -> lien4: {len(lien4)}')
#     if len(lien4)>0:
#         lien = pd.concat([lien, lien4], ignore_index=True)

#     part5 = (part4.merge(lien4[cols_part], how='outer', indicator=True)
#              .query('_merge == "left_only"')
#              .drop('_merge', axis=1))
#     df_app = lien4[['project_id', 'generalPic', 'participant_pic_p', 'orderNumber']].rename(columns=({'participant_pic_p':'participant_pic'}))
#     app5 = (app4.merge(df_app, how='outer', indicator=True)
#             .query('_merge == "left_only"')
#             .drop('_merge', axis=1))
#     print(f'reste à croiser -> app5: {len(app5)} part5: {len(part5)}')

#     '''jointure juste generalPic'''
#     lien5 = part5.merge(app5, how='inner', on=['project_id', 'generalPic'], suffixes=('', '_p'))
#     print(f'jointure seulement avec generalpic -> lien5: {len(lien5)}')
#     if len(lien5)>0:
#         lien = pd.concat([lien, lien5], ignore_index=True)

#     part6 = (part5.merge(lien5[cols_part], how='outer', indicator=True)
#                 .query('_merge == "left_only"')
#                 .drop('_merge', axis=1))
#     df_app = lien5[['project_id', 'generalPic', 'participant_pic_p', 'orderNumber_p']].rename(columns=({'orderNumber_p':'orderNumber', 'participant_pic_p':'participant_pic'}))
#     app6 = (app5.merge(df_app, how='outer', indicator=True)
#             .query('_merge == "left_only"')
#             .drop('_merge', axis=1))
#     print(f'reste à croiser -> app6: {len(app6)} part6: {len(part6)}')
    
#     '''jointure juste participant_pic'''
#     lien6 = (part6
#             .merge(app6, how='inner', on=['project_id', 'participant_pic'], suffixes=('', '_p'))
#             .assign(base_only='a_joindre'))
#     print(f'jointure avec seulement participant_pic -> lien6: {len(lien6)}')
#     if len(lien6)>0:
#         lien = pd.concat([lien, lien6], ignore_index=True)
#         print('code pour ajouter lien6 à la table lien finale')

#         part7 = (part6.merge(lien[cols_part], how='outer', indicator=True)
#                     .query('_merge == "left_only"')
#                     .drop('_merge', axis=1))
#         df_app = lien[['project_id', 'generalPic', 'participant_pic_p', 'orderNumber_p']].rename(columns=({'orderNumber_p':'orderNumber', 'participant_pic_p':'participant_pic'}))
#         app7 = (app6.merge(df_app, how='outer', indicator=True)
#                 .query('_merge == "left_only"')
#                 .drop('_merge', axis=1))
#         print(f'reste à croiser -> app7: {len(app7)} part7: {len(part7)} faire codepour lien7')

#     elif (len(part6)>0)|(len(app6)>0):
#         p = pd.concat([part6, app6], ignore_index=True)
#         lien = pd.concat([lien, p], ignore_index=True)
#     # else:
#     #     '''jointure juste participant_pic'''
#     #     lien6 = part5.merge(app5, how='inner', on=['project_id', 'participant_pic'], suffixes=('', '_p'))
#     #     print(f'jointure avec seulement participant_pic -> lien6: {len(lien6)}')
#     #     if len(lien6)>0:
#     #         lien = pd.concat([lien, lien6], ignore_index=True)
#     #         print('code pour ajouter lien6 à la table lien finale')


#     # lien = pd.concat([lien1, lien2, lien3, lien4, app5, part5], ignore_index=True).drop_duplicates()
#     lien = lien.assign(inProposal=np.where(~lien['n_part'].isnull() & lien['n_app'].isnull(), False, True)).drop_duplicates()
#     lien = lien.assign(inProject=np.where(lien['n_part'].isnull() & ~lien['n_app'].isnull(), False, True))

#     lien['orderNumber_p'] = np.where((lien['orderNumber_p'].isnull()) & (lien['inProposal']==True), lien['orderNumber'], lien['orderNumber_p'])
#     lien['orderNumber'] = np.where(lien['inProject']==False, None, lien['orderNumber'])

#     lien['participant_pic_p'] = np.where((lien['participant_pic_p'].isnull()) & (lien['inProposal']==True), lien['participant_pic'], lien['participant_pic_p'])
#     lien['participant_pic'] = np.where(lien['inProject']==False, None, lien['participant_pic'])

#     for x in lien.columns:
#         if pd.api.types.infer_dtype(lien[x])=='string':
#             lien.loc[:,x]=np.where(lien.loc[:,x].isnull(), None, lien.loc[:,x])
        
#     lien.columns = ['applicant_'+k[0:-2] if k[-2:] == '_p' else k for k in list(lien.columns)]
#     lien['calculated_pic'] = np.where(~lien['participant_pic'].isnull(), lien['participant_pic'], lien['applicant_participant_pic'])

#     lien['projNlien'] = lien.groupby(['project_id', 'applicant_orderNumber', 'generalPic', 'calculated_pic'], dropna = False).pipe(lambda x: x.orderNumber.transform('nunique'))
#     lien['propNlien'] = lien.groupby(['project_id', 'orderNumber', 'generalPic', 'calculated_pic'], dropna = False).pipe(lambda x: x.applicant_orderNumber.transform('nunique'))
#     lien.loc[lien['projNlien']==0, 'projNlien']=1
#     lien.loc[lien['propNlien']==0, 'propNlien']=1

#     print(f'- size lien: {len(lien)}') 
#     length_lien=len(lien)
#     # print(lien[lien['n_lien']>1])

# #     lien.loc[lien.inProject==True, 'participation_linked'] = lien['project_id']+"-"+lien['orderNumber']
# #     lien.loc[lien.inProposal==True, 'participation_linked'] = lien['project_id']+"-"+lien['proposal_orderNumber']
    
#     # add countryCode
#     lien = (lien
#             .merge(part[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'country_code_source']],
#                    how='left', on=['project_id', 'orderNumber', 'generalPic', 'participant_pic']))

#     lien = (lien
#             .merge(app1[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'country_code_source']], 
#                    how='left', left_on=['project_id', 'applicant_orderNumber', 'generalPic', 'applicant_participant_pic'],
#                    right_on=['project_id', 'orderNumber', 'generalPic', 'participant_pic'],
#                    suffixes=[ '','.y'])
#             .drop(columns=[ 'participant_pic.y', 'orderNumber.y'])
#             .rename(columns={'country_code_source.y':'applicant_country_code_source'}))

#     lien.loc[lien.country_code_source.isnull(), 'country_code_source'] = lien.loc[lien.country_code_source.isnull(), 'applicant_country_code_source']

#     if any(lien.country_code_source.isnull()):
#         print(f"- ⚠️ {lien[lien.country_code_source.isnull()].generalPic.nunique()} countryCode missing {lien[lien.country_code_source.isnull()].generalPic.unique()}")


#     #add contribution 
#     rename_dict = {col: 'applicant_' + col for col in ['orderNumber', 'participant_pic', 'country_code_source', 'role', 'partnerType', 'erc_role']}

#     lien=(lien
#             .merge(app1[['project_id', 'generalPic', 'requestedGrant', 'orderNumber', 'participant_pic', 'country_code_source', 'role', 'partnerType', 'erc_role']]
#                         .rename(columns=rename_dict),
#             how='left', 
#             on=['project_id', 'applicant_orderNumber', 'generalPic', 'applicant_participant_pic', 'applicant_country_code_source']))

#     lien['app_fund'] = (np.where((lien['projNlien']>1.), lien['requestedGrant']/lien['projNlien'], lien['requestedGrant']))
   
#     if app1['requestedGrant'].sum()==lien['app_fund'].sum():
#         print("subventions app1/lien: ok")
#     else:
#         print(f"- check difference between requestGrant and app_fund: {'{:,.1f}'.format(app1['requestedGrant'].sum())}, {'{:,.1f}'.format(lien['app_fund'].sum())}")


#     lien=(lien.merge(part[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'country_code_source', 'role', 'partnerType', 'erc_role', 'euContribution', 'netEuContribution']],
#     how='left', 
#     on=['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'country_code_source']))

#     lien['beneficiary_fund'] = (np.where((lien['propNlien']>1.), lien['euContribution']/lien['propNlien'], lien['euContribution']))
#     if part['euContribution'].sum()==lien['beneficiary_fund'].sum():
#         print("subventions benef/lien: ok")
#     else:
#         print(f"- check difference between euContribution and benef_fund: {'{:,.1f}'.format(part['euContribution'].sum())}, {'{:,.1f}'.format(lien['beneficiary_fund'].sum())}")
    

#     lien['part_fund'] = (np.where((lien['propNlien']>1.), lien['netEuContribution']/lien['propNlien'], lien['netEuContribution']))
#     if part['netEuContribution'].sum()==lien['part_fund'].sum():
#         print("subventions part/lien: ok")
#     else:
#         print(f"- check difference between netEuContribution and part_fund: {'{:,.1f}'.format(part['netEuContribution'].sum())}, {'{:,.1f}'.format(lien['part_fund'].sum())}")



#     # verif que chaque obs contient un calculated pic
#     lien_no_pic=len(lien[lien['calculated_pic'].isnull()])
#     if lien_no_pic > 0:
#         print(f'1- ⚠️ {lien_no_pic} entités dans la table LIEN n\'ont pas de pic secondaire' )

#     del app2, app3, app4, app5, app6, part2, part3, part4, part5, part6, lien1, lien2, lien3, lien4, lien5, lien6
#     return lien
###########################################""


import pandas as pd, numpy as np
def _add_beneficiary_context(app1, part):
    """
    Reconstruit, pour chaque ligne, le PIC du beneficiary de rattachement.
    - app1 : le beneficiary d'une ligne 'affiliated partner'/'associated partner' est le
      generalPic de l'applicant qui la précède (bloc = applicant + ce qui suit, jusqu'au
      prochain applicant). orderNumber sert UNIQUEMENT à ordonner les lignes à l'intérieur
      d'un même project_id, jamais de clé de jointure entre app1 et part.
    - part : le beneficiary d'une ligne est le generalPic de la ligne 'beneficiary' qui
      partage le même orderNumber (orderNumber structure déjà les groupes côté part).
    """
    app1 = app1.sort_values(['project_id', 'orderNumber']).copy()
    is_new_block = app1['partnerType'].eq('applicant')
    app1['block_no'] = app1.groupby('project_id')['partnerType'].transform(lambda s: is_new_block.loc[s.index].cumsum())
    benef = app1.loc[is_new_block, ['project_id', 'block_no', 'generalPic']].rename(columns={'generalPic': 'benef_pic'})
    app1 = app1.merge(benef, on=['project_id', 'block_no'], how='left')
    app1.loc[app1['partnerType'] == 'applicant', 'benef_pic'] = app1['generalPic']
 
    part = part.copy()
    benef_part = part.loc[part['partnerType'] == 'beneficiary', ['project_id', 'orderNumber', 'generalPic']].rename(
        columns={'generalPic': 'benef_pic'})
    part = part.merge(benef_part, on=['project_id', 'orderNumber'], how='left')
    part.loc[part['partnerType'] == 'beneficiary', 'benef_pic'] = part['generalPic']
 
    return app1, part
 
 
def _cascade_lien(app1, part):
    """
    Construit le lien (project_id, orderNumber_app, orderNumber_part, generalPic,
    participant_pic app-side, participant_pic_p part-side, n_app, n_part, base_only)
    en cascade fine -> large. Retourne aussi les cas ambigus pour vérification manuelle.
    """
    app1c, partc = _add_beneficiary_context(app1, part)
 
    key_cols = ['project_id', 'generalPic', 'participant_pic', 'benef_pic']
    app2 = app1c[key_cols + ['orderNumber', 'n_app']].drop_duplicates().rename(columns={'orderNumber': 'orderNumber_app'})
    part2 = partc[key_cols + ['orderNumber', 'n_part']].drop_duplicates().rename(columns={'orderNumber': 'orderNumber_part'})
    print(f'app2: {len(app2)}  part2: {len(part2)}')
 
    lien = pd.DataFrame()
 
    # étape 1 : jointure fine (generalPic + participant_pic + beneficiary reconstruit)
    lien1 = app2.merge(part2, on=key_cols, how='inner')
    lien1['participant_pic_p'] = lien1['participant_pic']
    print(f'lien1 (fine): {len(lien1)}')
    lien = pd.concat([lien, lien1], ignore_index=True)
 
    app_rest = app2.merge(lien1[key_cols], on=key_cols, how='left', indicator=True).query('_merge=="left_only"').drop(columns='_merge')
    part_rest = part2.merge(lien1[key_cols], on=key_cols, how='left', indicator=True).query('_merge=="left_only"').drop(columns='_merge')
    print(f'reste -> app: {len(app_rest)}  part: {len(part_rest)}')
 
    # étape 2 : sans participant_pic (erreurs de saisie), toujours désambiguïsé par benef_pic
    key2 = ['project_id', 'generalPic', 'benef_pic']
    lien2 = app_rest.merge(part_rest, on=key2, how='inner', suffixes=('', '_p'))
    print(f'lien2 (sans participant_pic): {len(lien2)}')
    if len(lien2):
        lien = pd.concat([lien, lien2], ignore_index=True)
    app_rest = app_rest.merge(lien2[key2].drop_duplicates(), on=key2, how='left', indicator=True).query('_merge=="left_only"').drop(columns='_merge') if len(lien2) else app_rest
    part_rest = part_rest.merge(lien2[key2].drop_duplicates(), on=key2, how='left', indicator=True).query('_merge=="left_only"').drop(columns='_merge') if len(lien2) else part_rest
    print(f'reste -> app: {len(app_rest)}  part: {len(part_rest)}')
 
    # étape 3 : project_id + generalPic seul, uniquement si le PIC n'est plus dupliqué
    app_unique = app_rest[~app_rest.duplicated(['project_id', 'generalPic'], keep=False)]
    part_unique = part_rest[~part_rest.duplicated(['project_id', 'generalPic'], keep=False)]
    lien3 = app_unique.merge(part_unique, on=['project_id', 'generalPic'], how='inner', suffixes=('', '_p'))
    print(f'lien3 (generalPic seul, non dupliqué): {len(lien3)}')
    if len(lien3):
        lien = pd.concat([lien, lien3], ignore_index=True)
    app_rest = app_rest.merge(lien3[['project_id', 'generalPic']].drop_duplicates(), on=['project_id', 'generalPic'], how='left', indicator=True).query('_merge=="left_only"').drop(columns='_merge')
    part_rest = part_rest.merge(lien3[['project_id', 'generalPic']].drop_duplicates(), on=['project_id', 'generalPic'], how='left', indicator=True).query('_merge=="left_only"').drop(columns='_merge')
 
    # étape 4 : filet de sécurité orderNumber, uniquement sur les PIC encore dupliqués des deux côtés
    still_dup_app = app_rest[app_rest.duplicated(['project_id', 'generalPic'], keep=False)]
    still_dup_part = part_rest[part_rest.duplicated(['project_id', 'generalPic'], keep=False)]
    lien4 = still_dup_app.merge(still_dup_part, left_on=['project_id', 'generalPic', 'orderNumber_app'],
                                 right_on=['project_id', 'generalPic', 'orderNumber_part'], how='inner', suffixes=('', '_p'))
    print(f'lien4 (filet orderNumber, PIC encore ambigus): {len(lien4)}')
    if len(lien4):
        lien = pd.concat([lien, lien4], ignore_index=True)
        matched_app = lien4[['project_id', 'generalPic', 'orderNumber_app']].drop_duplicates()
        matched_part = lien4[['project_id', 'generalPic', 'orderNumber_part']].drop_duplicates()
        app_rest = app_rest.merge(matched_app, on=['project_id', 'generalPic', 'orderNumber_app'], how='left', indicator=True).query('_merge=="left_only"').drop(columns='_merge')
        part_rest = part_rest.merge(matched_part, on=['project_id', 'generalPic', 'orderNumber_part'], how='left', indicator=True).query('_merge=="left_only"').drop(columns='_merge')
 
    ambiguous_app = app_rest[app_rest.duplicated(['project_id', 'generalPic'], keep=False)]
    ambiguous_part = part_rest[part_rest.duplicated(['project_id', 'generalPic'], keep=False)]
    clean_app_only = app_rest[~app_rest.duplicated(['project_id', 'generalPic'], keep=False)]
    clean_part_only = part_rest[~part_rest.duplicated(['project_id', 'generalPic'], keep=False)]
    print(f'\napp1_only propre: {len(clean_app_only)}   part_only propre: {len(clean_part_only)}')
    print(f'⚠️  à vérifier manuellement -> app: {len(ambiguous_app)}  part: {len(ambiguous_part)}')
 
    lien = lien.assign(base_only='both')
    if len(clean_app_only):
        lien = pd.concat([lien, clean_app_only.assign(base_only='prop_only')], ignore_index=True)
    if len(clean_part_only):
        part_only_renamed = clean_part_only.rename(columns={'participant_pic': 'participant_pic_p', 'benef_pic': 'benef_pic_p'})
        lien = pd.concat([lien, part_only_renamed.assign(base_only='part_only')], ignore_index=True)
    if len(ambiguous_app):
        lien = pd.concat([lien, ambiguous_app.assign(base_only='AMBIGU_a_verifier')], ignore_index=True)
    if len(ambiguous_part):
        amb_part_renamed = ambiguous_part.rename(columns={'participant_pic': 'participant_pic_p', 'benef_pic': 'benef_pic_p'})
        lien = pd.concat([lien, amb_part_renamed.assign(base_only='AMBIGU_a_verifier')], ignore_index=True)
 
    print(f'\n- size lien (core): {len(lien)}')
    return lien
 
 
def merged_partApp(app1, part):
    """
    Reconstruit le lien avec exactement le même schéma final que l'ancienne fonction :
    ['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'n_app', 'base_only',
     'n_part', 'applicant_orderNumber', 'applicant_participant_pic', 'inProposal',
     'inProject', 'calculated_pic', 'projNlien', 'propNlien', 'country_code_source',
     'applicant_country_code_source', 'requestedGrant', 'applicant_role',
     'applicant_partnerType', 'applicant_erc_role', 'app_fund', 'role', 'partnerType',
     'erc_role', 'euContribution', 'netEuContribution', 'beneficiary_fund', 'part_fund']
    """
    core = _cascade_lien(app1, part)
 
    # --- renommage vers le schéma final ---
    lien = core.rename(columns={
        'orderNumber_app': 'applicant_orderNumber',
        'orderNumber_part': 'orderNumber',
        'participant_pic': 'applicant_participant_pic',
        'participant_pic_p': 'participant_pic',
    })
 
    # --- inProposal / inProject ---
    lien['inProposal'] = np.where((~lien['n_part'].isnull()) & (lien['n_app'].isnull()), False, True)
    lien['inProject'] = np.where((lien['n_part'].isnull()) & (~lien['n_app'].isnull()), False, True)
 
    # --- calculated_pic ---
    lien['calculated_pic'] = np.where(~lien['participant_pic'].isnull(), lien['participant_pic'], lien['applicant_participant_pic'])
 
    # --- projNlien / propNlien : nb de liens distincts de l'autre côté pour un même noeud ---
    lien['projNlien'] = lien.groupby(['project_id', 'applicant_orderNumber', 'generalPic', 'calculated_pic'], dropna=False)['orderNumber'].transform('nunique')
    lien['propNlien'] = lien.groupby(['project_id', 'orderNumber', 'generalPic', 'calculated_pic'], dropna=False)['applicant_orderNumber'].transform('nunique')
    lien.loc[lien['projNlien'] == 0, 'projNlien'] = 1
    lien.loc[lien['propNlien'] == 0, 'propNlien'] = 1
 
    # --- enrichissement app1 (colonnes côté proposal) ---
    app1_cols = ['project_id', 'orderNumber', 'generalPic', 'participant_pic',
                 'requestedGrant', 'role', 'partnerType', 'erc_role', 'country_code_source']
    app1_side = app1[app1_cols].rename(columns={
        'orderNumber': 'applicant_orderNumber',
        'participant_pic': 'applicant_participant_pic',
        'role': 'applicant_role',
        'partnerType': 'applicant_partnerType',
        'erc_role': 'applicant_erc_role',
        'country_code_source': 'applicant_country_code_source',
    })
    lien = lien.merge(app1_side, on=['project_id', 'applicant_orderNumber', 'generalPic', 'applicant_participant_pic'], how='left')
 
    # --- enrichissement part (colonnes côté grant agreement) ---
    part_cols = ['project_id', 'orderNumber', 'generalPic', 'participant_pic',
                 'role', 'partnerType', 'erc_role', 'euContribution', 'netEuContribution', 'country_code_source']
    part_side = part[part_cols]
    lien = lien.merge(part_side, on=['project_id', 'orderNumber', 'generalPic', 'participant_pic'], how='left')
 
    # --- country_code_source : priorité part, fallback app1 ---
    lien['country_code_source'] = lien['country_code_source'].where(~lien['country_code_source'].isnull(), lien['applicant_country_code_source'])
 
    # --- app_fund / beneficiary_fund / part_fund : répartition proportionnelle si plusieurs liens ---
    lien['app_fund'] = np.where(lien['projNlien'] > 1, lien['requestedGrant'] / lien['projNlien'], lien['requestedGrant'])
    lien['beneficiary_fund'] = np.where(lien['propNlien'] > 1, lien['euContribution'] / lien['propNlien'], lien['euContribution'])
    lien['part_fund'] = np.where(lien['propNlien'] > 1, lien['netEuContribution'] / lien['propNlien'], lien['netEuContribution'])
 
    # --- contrôle des sommes (réconciliation par table source, pas égalité croisée) ---
    if abs(app1['requestedGrant'].sum() - lien['app_fund'].sum()) < 1:
        print('subventions app1/lien: ok')
    else:
        print(f"- check difference requestedGrant/app_fund: {app1['requestedGrant'].sum():,.1f} vs {lien['app_fund'].sum():,.1f}")
 
    if abs(part['euContribution'].sum() - lien['beneficiary_fund'].sum()) < 1:
        print('subventions benef/lien: ok')
    else:
        print(f"- check difference euContribution/beneficiary_fund: {part['euContribution'].sum():,.1f} vs {lien['beneficiary_fund'].sum():,.1f}")
 
    if abs(part['netEuContribution'].sum() - lien['part_fund'].sum()) < 1:
        print('subventions part/lien: ok')
    else:
        print(f"- check difference netEuContribution/part_fund: {part['netEuContribution'].sum():,.1f} vs {lien['part_fund'].sum():,.1f}")
 
    final_cols = ['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'n_app', 'base_only',
                  'n_part', 'applicant_orderNumber', 'applicant_participant_pic', 'inProposal', 'inProject',
                  'calculated_pic', 'projNlien', 'propNlien', 'country_code_source',
                  'applicant_country_code_source', 'requestedGrant', 'applicant_role', 'applicant_partnerType',
                  'applicant_erc_role', 'app_fund', 'role', 'partnerType', 'erc_role', 'euContribution',
                  'netEuContribution', 'beneficiary_fund', 'part_fund']
 
    return lien[final_cols]


# def lien_create(app1, part):

#     lien, amb_a, amb_p = merged_partApp(app1, part)
 
#     # --- contrôle des sommes : la redistribution ne doit PAS changer le total de chaque
#     # table source. On ne compare jamais requestedGrant à netEuContribution entre eux
#     # (stades différents), seulement chaque colonne à elle-même avant/après fusion. ---
#     print('\n--- Contrôle des montants (réconciliation, pas égalité croisée) ---')
 
#     lien = lien.merge(
#         app1[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'requestedGrant']]
#             .rename(columns={'orderNumber': 'orderNumber_app'}),
#         on=['project_id', 'orderNumber_app', 'generalPic', 'participant_pic'], how='left')
#     lien = lien.merge(
#         part[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'netEuContribution']]
#             .rename(columns={'orderNumber': 'orderNumber_part', 'participant_pic': 'participant_pic_p'}),
#         on=['project_id', 'orderNumber_part', 'generalPic', 'participant_pic_p'], how='left')
 
#     n_dup_app = lien.groupby(['project_id', 'orderNumber_app', 'generalPic', 'participant_pic'])['orderNumber_part'].transform('nunique').replace(0, 1)
#     n_dup_part = lien.groupby(['project_id', 'orderNumber_part', 'generalPic', 'participant_pic_p'])['orderNumber_app'].transform('nunique').replace(0, 1)
#     lien['app_fund'] = np.where(n_dup_app > 1, lien['requestedGrant'] / n_dup_app, lien['requestedGrant'])
#     lien['part_fund'] = np.where(n_dup_part > 1, lien['netEuContribution'] / n_dup_part, lien['netEuContribution'])
 
#     print(f"requestedGrant  : app1={app1['requestedGrant'].sum():,.2f}   lien(app_fund)={lien['app_fund'].sum():,.2f}   "
#           f"{'OK' if abs(app1['requestedGrant'].sum() - lien['app_fund'].sum()) < 1 else '⚠️ ECART'}")
#     print(f"netEuContribution: part={part['netEuContribution'].sum():,.2f}   lien(part_fund)={lien['part_fund'].sum():,.2f}   "
#           f"{'OK' if abs(part['netEuContribution'].sum() - lien['part_fund'].sum()) < 1 else '⚠️ ECART'}")
#     return lien,  amb_a, amb_p