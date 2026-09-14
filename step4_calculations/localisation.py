import pandas as pd

def geo_merge_codes(row):
    x_codes = {c.strip() for c in str(row['entities_geo_unit_code']).split(';')} if pd.notna(row['entities_geo_unit_code']) else set()
    y_codes = {c.strip() for c in str(row['geo_unit_code']).split(';')} if pd.notna(row['geo_unit_code']) else set()

    is_172 = '172' in {c.strip() for c in str(row['operateur_num']).split(';')}

    if is_172 and 'FR-75C' in x_codes:
        final_codes = y_codes
    else:
        final_codes = x_codes | y_codes

    final_codes.discard('nan')
    final_codes.discard('')  # sécurité si un split laisse une chaîne vide
    return ';'.join(sorted(final_codes))


def prepare_activity_loc(entities_part, participation):

    tmp = (
        entities_part.loc[entities_part['numero_national_de_structure'].notna(),
                        ['operateur_num', 'generalPic', 'country_code', 'entities_geo_unit_code', 'numero_national_de_structure']]
        .drop_duplicates()
        .merge(
            participation.loc[participation['numero_national_de_structure'].notna(),
                            ['numero_national_de_structure', 'geo_unit_code']]
            .drop_duplicates(),
            how='left', on='numero_national_de_structure'
        )
    )

    tmp['activity_geo_unit_code'] = tmp.apply(geo_merge_codes, axis=1)
    tmp = tmp[['generalPic', 'country_code', 'numero_national_de_structure', 'activity_geo_unit_code']].drop_duplicates()
    print(f"- size num_nat_ctruct + activity_geo: {len(tmp)}")

    entities_part = entities_part.merge(
        tmp,
        how='left',
        on=['generalPic', 'country_code', 'numero_national_de_structure']
    )

    test = (entities_part.loc[entities_part.activity_geo_unit_code.notna(),
            ['generalPic', 'country_code', 'numero_national_de_structure', 'activity_geo_unit_code']]
            .drop_duplicates())
    print(f"- size after merge (eq to tmp ?): {len(test)} ans size entities_part: {len(entities_part)}")

    return entities_part


def add_geo_subdivision(df):
    """
    Pour chaque famille (entities_/activity_) traitée INDEPENDAMMENT :
    explode les codes multi-valeurs, merge avec sub_div, ré-implode.
    Les deux familles ne se croisent jamais (pas de produit cartésien).
    """
    from functions_shared import build_geo_subdivision

    sub_div = build_geo_subdivision()

    df = df.copy()
    n_initial = len(df)
    df["_original_row_id"] = range(len(df))

    entities_cols = [
        "entities_geo_unit_code",
        "entities_geo_top_code",
        "entities_geo_top_name",
        "entities_geo_top_type",
        "entities_geo_top_latlng",
        "entities_geo_unit_name",
        "entities_geo_unit_type",
        "entities_geo_unit_latlng",
    ]

    activity_cols = [
        "activity_geo_unit_code",
        "activity_geo_top_code",
        "activity_geo_top_name",
        "activity_geo_top_type",
        "activity_geo_top_latlng",
        "activity_geo_unit_name",
        "activity_geo_unit_type",
        "activity_geo_unit_latlng",
    ]

    def concat_unique(values):
        values = values.dropna().astype(str).str.strip()
        values = values[values != ""]
        values = values.drop_duplicates()
        return ";".join(values)

    def process_family(code_col, family_cols, prefix):
        if code_col not in df.columns:
            print(f"⚠️ Colonne absente : {code_col}")
            return pd.DataFrame(index=pd.Index(df["_original_row_id"].unique(), name="_original_row_id"))

        base_sub = df[["_original_row_id", code_col]].copy()
        base_sub[code_col] = base_sub[code_col].astype("string")

        mask = base_sub[code_col].str.contains(r";|\s+", regex=True, na=False)
        n_multi = mask.sum()
        print(f"- {code_col} : {n_multi} lignes multi-valeurs sur {len(base_sub)}")

        # --- Partie 1 : lignes SANS multi-valeurs -> pas besoin de groupby ---
        single = base_sub.loc[~mask].copy()
        single[code_col] = single[code_col].str.strip()

        sub_div_pref = sub_div.add_prefix(prefix).drop_duplicates(subset=code_col)
        lookup_cols = [c for c in sub_div_pref.columns if c not in df.columns or c == code_col]

        single = single.merge(sub_div_pref[lookup_cols], how="left", on=code_col, sort=False)
        single = single.set_index("_original_row_id")
        family_cols_present = [c for c in family_cols if c in single.columns]
        single_result = single[family_cols_present]

        if n_multi == 0:
            return single_result

        # --- Partie 2 : lignes AVEC multi-valeurs -> explode + groupby uniquement ici ---
        multi = base_sub.loc[mask].copy()
        multi[code_col] = multi[code_col].str.split(r";|\s+", regex=True)
        multi = multi.explode(code_col).reset_index(drop=True)
        multi[code_col] = multi[code_col].astype("string").str.strip()
        multi = multi[multi[code_col].notna() & (multi[code_col] != "")]

        multi = multi.merge(sub_div_pref[lookup_cols], how="left", on=code_col, sort=False)
        multi = multi.sort_values(by=["_original_row_id", code_col], kind="stable", na_position="last")

        multi_result = multi.groupby("_original_row_id", sort=False)[family_cols_present].agg(concat_unique)

        # --- Fusion des deux résultats ---
        return pd.concat([single_result, multi_result])

    entities_imploded = process_family("entities_geo_unit_code", entities_cols, "entities_")
    activity_imploded = process_family("activity_geo_unit_code", activity_cols, "activity_")

    print("\nColonnes entities finales :", list(entities_imploded.columns))
    print("Colonnes activity finales :", list(activity_imploded.columns))

    # Colonnes non concernées par explode/implode
    all_implode_cols = entities_cols + activity_cols
    non_implode_cols = [c for c in df.columns if c not in all_implode_cols and c != "_original_row_id"]

    base = (
        df[["_original_row_id"] + non_implode_cols]
        .drop_duplicates(subset="_original_row_id", keep="first")
        .set_index("_original_row_id")
    )

    result = base.join(entities_imploded, how="left").join(activity_imploded, how="left")
    result = result.sort_index().reset_index(drop=True)

    print(f"\n- lignes initiales : {n_initial}")
    print(f"- lignes finales   : {len(result)}")
    print(f"- colonnes finales : {len(result.columns)}")

    #------ remplir activity null avec entities------

    activity_cols = [c for c in result.columns if c.startswith("activity_")]

    for activity_col in activity_cols:
        suffix = activity_col.removeprefix("activity_")
        entity_col = f"entities_{suffix}"

        if entity_col in result.columns:
            empty = (
                result[activity_col].isna()
                | result[activity_col].astype("string").str.strip().eq("")
            )
            result.loc[empty, activity_col] = result.loc[empty, entity_col]



    return result