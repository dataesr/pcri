def proj_add_cols(prop1, proj):
    print('### ADD COLS TO PROJECTS FROM PROPOSALS')
    tmp=prop1[['project_id', 'eic_panels', 'panel_code']].drop_duplicates()
    proj=proj.merge(tmp, how='left', on='project_id')

    if proj.loc[proj.freekw.isnull()].empty:
        pass
    else:
        tmp=prop1.loc[prop1.project_id.isin(proj.loc[proj.freekw.isnull(), 'project_id'].unique()), ['project_id', 'freekw']].rename(columns={'freekw':'pf'})
        proj=proj.merge(tmp, how='left', on='project_id')
        proj.loc[~proj.pf.isnull(), 'freekw'] = proj.loc[~proj.pf.isnull(), 'pf']
        proj = proj.drop('pf', axis=1)

    l_prop=list(set(prop1.columns).difference(proj.columns))
    l_proj=list(set(proj.columns).difference(prop1.columns))
    print(f"1 - liste des variables PROJ en plus: {l_proj}\n2 - liste des variables PROP en plus: {l_prop}\n")
    return proj