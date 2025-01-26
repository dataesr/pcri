
entities_all = pd.read_pickle(f'{PATH}participants/data_for_matching/entities_all.pkl')
entities_tmp=entities_all.loc[((entities_all.country_code=='FRA')&(entities_all.rnsr_merged.str.len()==0))|((entities_all.country_code!='FRA')&(entities_all.entities_id.str.contains('pic'))), ['project_id','generalPic','country_code', 'entities_id', 'entities_name']]