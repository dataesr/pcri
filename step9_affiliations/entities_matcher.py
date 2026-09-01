from remote_process.matcher import matcher

def run_rnsr_match(
    df,
    strategies,
    query_columns,
    year_column='call_year'
):
    """
    Lance affiliation-matcher sur un dataframe.

    Parameters
    ----------
    df : DataFrame
        Doit contenir au minimum :
        - call_year
        - labo
        - supervisor
        - city

    strategies : list
        Stratégies affiliation-matcher.

    query_columns : list
        Colonnes utilisées pour construire la requête.

    Returns
    -------
    DataFrame
        dataframe avec la colonne 'match'
    """

    df = df.copy()

    for col in ['labo', 'supervisor', 'city']:
        if col not in df.columns:
            df[col] = ''

        df[col] = (
            df[col]
            .fillna('')
            .astype(str)
            .str.strip()
        )

    df['match'] = None

    for i, row in df.iterrows():

        query = ' '.join(
            row[col]
            for col in query_columns
            if row[col]
        )

        matcher(
            df,
            i,
            typ='rnsr',
            query=query,
            strategies=strategies,
            year=row[year_column]
        )

    return df