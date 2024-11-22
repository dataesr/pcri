import pandas as pd
def IDpic(entities_tmp):
    print("\n### traitement id_pic avec tiret")
    # IDENT with '-' : traitement des identifiants avec '-' pour regrouper multi-pic non identifiés 
    if not entities_tmp.loc[entities_tmp.id.str.contains('-', na=False)].empty:
        pic_dash = (entities_tmp.loc[entities_tmp.id.str.contains('-', na=False), ['generalPic', 'entities_id']]
        .drop_duplicates()
        .drop(columns='generalPic')
        .assign(pic_d = entities_tmp.entities_id.str.split('-').str[0]))
        print(f"- size entities pic_dash: {len(pic_dash)}")
        dash = (pic_dash.merge(entities_tmp, how='inner', left_on='pic_d', right_on='generalPic', suffixes=['_x',''])
                .drop(columns=['entities_id_x', 'pic_d'])).drop_duplicates()
        
        entities_tmp = entities_tmp.loc[~entities_tmp.entities_id.isin(dash.entities_id.unique())]
        entities_tmp = pd.concat([entities_tmp, dash], ignore_index=True)
        print(f"- size entities_tmp: {len(entities_tmp)}")

    # IDENT pic : corriger appliquer les lignes ci-dessous uniuqument sur entities_id est null ou commence par pic
    entities_tmp.loc[entities_tmp.entities_id.isnull(), 'entities_id'] = "pic"+entities_tmp.generalPic.map(str)
    print(f"- size entities_tmp: {len(entities_tmp)}")
    return entities_tmp