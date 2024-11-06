

part=participation[['stage', 'project_id','generalPic', 'role', 'participates_as', 'erc_role', 
                    'country_code', 'nuts_code', 'region_1_name', 'region_2_name', 'regional_unit_name',
                    'coordination_number', 'calculated_fund', 'beneficiary_subv']].assign(number_involved=1)

def ent_stage(df, stage_value:str):
    df=df[df.stage==stage_value].merge(entities_info, how='left', on=['generalPic','country_code'])
    
    if any(df.id.str.contains(';', na=False)):
        print(f"Attention multi id pour une participation, calculs sur les chiffres\n {df.loc[df.id.str.contains(';', na=False), 'id'].drop_duplicates()}")
        df['nb'] = df.id.str.split(';').str.len()
        for i in ['coordination_number', 'calculated_fund', 'beneficiary_subv', 'number_involved']:
            df[i] = np.where(df['nb']>1, df[i]/df['nb'], df[i])

    return df
    print(f"4 - size entities_{stage_value}={len(tmp)}, fund={'{:,.1f}'.format(tmp['calculated_fund'].sum())}")

entities_eval = ent_stage(part, 'evaluated')
entities_signed = ent_stage(part, 'successful')

entities_part = pd.concat([entities_eval, entities_signed], ignore_index=True)

mask=(entities_part.entities_id.str.contains('^gent', na=False))&(~entities_part.entities_acronym_source.isnull())
r=(entities_part.loc[mask ,['generalPic', 'entities_id','entities_name_source', 'entities_acronym_source']]
   .drop_duplicates())

r['entities_name_source'] = r.apply(lambda x: x['entities_name_source'] if  x["entities_acronym_source"].upper() in x["entities_name_source"].upper() else x['entities_name_source']+' '+x["entities_acronym_source"].lower(),axis=1)
entities_part = (entities_part.drop(columns=['entities_name_source', 'entities_acronym_source'])
                 .merge(r.drop(columns='entities_acronym_source'), how='left', on=['generalPic', 'entities_id'])
)

entities_part=(entities_part
                .drop(columns=['generalPic','businessName', 'legalName','generalState', 'street','postalCode','postalBox',
                               'nutsCode','webPage','naceCode','gps_loc', 'city', 'countryCode','isNonProfit',  'cat_an',
                               'isPublicBody', 'isInternationalOrganisation', 'isResearchOrganisation', 'isHigherEducation',
                              'legalType', 'vat', 'legalRegNumber', 'naceCode', 'gps_loc', 'id', 'id_m',  'siret_closeDate',
                               'siren', 'legalEntityTypeCode']))

entities_part=(entities_part
    .groupby(list(entities_part.columns.difference(['coordination_number', 'number_involved', 'calculated_fund', 'beneficiary_subv'])), dropna=False, as_index=False).sum()
    .drop_duplicates()
     )

# ope_cat=pd.read_pickle(f"{PATH_REF}operateurs_mires.pkl")
# entities_participation=entities_participation.merge(ope_cat[['entities_id','operateur_name']], how='left', on='entities_id')


entities_part = entities_part.applymap(lambda x: x.strip() if isinstance(x, str) else x)

print(f"1 - part={'{:,.1f}'.format(part.loc[part.stage=='evaluated', 'calculated_fund'].sum())},participation={'{:,.1f}'.format(participation.loc[participation.stage=='evaluated', 'calculated_fund'].sum())}")
print(f"2 - part={'{:,.1f}'.format(part.loc[part.stage=='successful', 'calculated_fund'].sum())},participation={'{:,.1f}'.format(participation.loc[participation.stage=='successful', 'calculated_fund'].sum())}")
print(f"3 - comparaison nb couple genpic + country (doit être égal) {len(part[['generalPic','country_code']].drop_duplicates())},{len(entities_info[['generalPic','country_code']].drop_duplicates())}")

proj=(projects[['project_id', 'call_id', 'stage', 'status_code', 'framework', 'ecorda_date',
                'call_year','topic_code', 'topic_name', 'action_code', 'action_name', 'action_code2', 'action_name2',
                'panel_code', 'panel_name', 'panel_regroupement_code', 'panel_regroupement_name',
               'pilier_code', 'pilier_name_en', 'pilier_name_fr', 'programme_code', 'programme_name_en','programme_name_fr',
                'thema_code', 'thema_name_fr', 'thema_name_en', 'destination_code','destination_lib', 'destination_name_en', 
               'destination_detail_code', 'destination_detail_name_en', 'free_keywords', 'abstract', 'acronym']]
      .drop_duplicates()
     )

entities_participation = entities_part.merge(proj, how='inner', on=['project_id', 'stage']).sort_values(['destination_name_en'], ascending=True)
entities_participation = entities_participation.assign(with_coord=np.where(entities_participation.destination_code.isin(['PF','ACCELERATOR','COST'])|(entities_participation.thema_code=='ERC'), False, True))
entities_participation.loc[entities_participation.destination_code=='SyG', 'with_coord'] = True 
       
entities_participation = entities_participation.reindex(sorted(entities_participation.columns), axis=1)
print(f"size de entities_participation : {len(entities_participation)}")