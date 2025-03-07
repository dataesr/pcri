def dataset_decribe(df):
    import io
    buffer = io.StringIO()
    df.info(buf=buffer)
    s = buffer.getvalue()

    lines = s.splitlines()
    column_info = []
    # Analyser chaque ligne pour extraire les informations nécessaires
    for line in lines[5:]:  # Ignorer les premières lignes (généralement des en-têtes)
        parts = line.split()
        if len(parts) >= 5:
            col_order = int(parts[0])+1
            col_name = parts[1]
            col_nobs = parts[2]
            col_type = parts[4]
            column_info.append({'VARNUM':col_order,'code_champ_tech':col_name,'NOBS':col_nobs,'type':col_type})
    tmp=pd.DataFrame(column_info)
    tmp.loc[tmp.type=='object', 'type']='char'
    tmp.loc[tmp.type.str.startswith('int'), 'type']='num'

    tmp=tmp.assign(LENGTH=np.nan)
    for i in df.columns:
        var_char=tmp[tmp.type=='char'].code_champ_tech.to_list()
        var_num=tmp[tmp.type=='num'].code_champ_tech.to_list()
        if i in var_char:
            df[i]=df[i].astype(str)
            v=max(df[i].str.len())
            tmp.loc[tmp.code_champ_tech==i, 'LENGTH'] = v
        if i in var_num:
            df[i]=df[i].astype(int)
            tmp.loc[tmp.code_champ_tech==i, 'LENGTH'] = 8

    tmp['NPOS'] = tmp.LENGTH.cumsum()-tmp.LENGTH
    tmp[['LENGTH', 'NPOS']] = tmp[['LENGTH', 'NPOS']].astype(int)
    tmp=tmp.insert(loc=0, column='table', value='refext')
    print(tmp)
    return tmp