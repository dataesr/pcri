# ==============================================================================
# AFFILIATION RNSR
# ==============================================================================

from step9_affiliations.entities_matcher import run_rnsr_match
from step9_affiliations.translate_eng_to_fr import translate_if_english

def identify_rnsr(
    keep,
    PATH_HARVEST,
    reload_matches=False
):
    """
    Identification des RNSR pour les structures/laboratoires.

    Passes :
        1. matching par code laboratoire
        2. matching par nom laboratoire
        3. matching par orcid_employer

    Les résultats intermédiaires sont sauvegardés afin de ne pas devoir
    relancer affiliation-matcher en cas de problème lors des merges.

    Parameters
    ----------
    keep : pandas.DataFrame
        Dataset principal.
    PATH_HARVEST : str
        Répertoire de sauvegarde.
    reload_matches : bool
        False : relance le matcher.
        True  : recharge les résultats déjà sauvegardés.

    Returns
    -------
    pandas.DataFrame
        keep enrichi avec les RNSR identifiés.
    """

    print("=" * 70)
    print("AFFILIATION RNSR")
    print("=" * 70)


    # ==========================================================================
    # 0. Fonctions utilitaires locales
    # ==========================================================================

    def safe_list(value):
        """
        Transforme proprement une valeur en liste.

        Gère notamment :
        - None
        - NaN
        - listes
        - tuples
        - chaînes séparées par ';'
        """

        if value is None:
            return []

        if isinstance(value, list):
            return value

        if isinstance(value, tuple):
            return list(value)

        if isinstance(value, str):
            if not value.strip():
                return []

            return [
                x.strip()
                for x in value.split(';')
                if x.strip()
            ]

        try:
            if pd.isna(value):
                return []
        except (TypeError, ValueError):
            pass

        return [value]


    def unique_ordered(*values):
        """
        Fusionne plusieurs listes en conservant l'ordre d'apparition.
        """

        result = []

        for value in values:
            for item in safe_list(value):

                if item is None:
                    continue

                if isinstance(item, float) and pd.isna(item):
                    continue

                if item not in result:
                    result.append(item)

        return result


    def save_match(df, filename):
        """
        Sauvegarde un résultat de matching.
        """

        path = f"{PATH_HARVEST}matcher/{filename}"

        df.to_pickle(
            path,
            compression='gzip'
        )

        print(f"  -> sauvegardé : {path}")


    def load_match(filename):
        """
        Recharge un résultat de matching.
        """

        path = f"{PATH_HARVEST}matcher/{filename}"

        print(f"  -> chargement : {path}")

        return pd.read_pickle(path)


    # ==========================================================================
    # 1. Sélection des lignes susceptibles d'avoir un laboratoire
    # ==========================================================================

    print("\n## 1. Préparation des lignes à identifier")

    labo = keep.loc[
        keep['numero_national_de_structure'].isna()
        &
        (
            keep['org_merged'].apply(safe_list).str.len().gt(0)
            |
            keep['operateur_num'].notna()
        ),
        [
            'call_year',
            'stage',
            'project_id',
            'generalPic',
            'entities_full_2',
            'department_dup_2',
            'orcid_employer',
            'org_type',
            'org_merged',
            'rnsr_merged',
            'lab_merged',
            'cp_back',
            'city_back',
            'operateur_num',
            'category_woven'
        ]
    ].copy()

    print(f"- size labo : {len(labo)}")


    # ==========================================================================
    # 2. On ne garde que les lignes sans RNSR
    # ==========================================================================

    labo['rnsr_merged'] = labo['rnsr_merged'].apply(safe_list)

    lab_a_ident = labo.loc[
        labo['rnsr_merged'].str.len().eq(0),
        [
            'project_id',
            'generalPic',
            'call_year',
            'department_dup_2',
            'entities_full_2',
            'orcid_employer',
            'lab_merged',
            'city_back'
        ]
    ].copy()

    print(f"- size lab_a_ident : {len(lab_a_ident)}")


    # ==========================================================================
    # PASSE 1
    # MATCHING PAR CODE LABORATOIRE
    # ==========================================================================

    print("\n" + "-" * 70)
    print("PASSE 1 — CODE LABORATOIRE")
    print("-" * 70)


    if reload_matches:

        lab_id = load_match('ident_lab1.pkl')

    else:

        # ----------------------------------------------------------------------
        # Important :
        # on conserve les clés d'origine AVANT de préparer les données
        # destinées au matcher.
        #
        # Le matcher n'a pas besoin de toutes ces clés.
        # ----------------------------------------------------------------------

        ident_by_id = lab_a_ident.loc[
            lab_a_ident['lab_merged'].apply(safe_list).str.len().gt(0),
            [
                'project_id',
                'generalPic',
                'call_year',
                'entities_full_2',
                'lab_merged',
                'city_back'
            ]
        ].copy()


        # ----------------------------------------------------------------------
        # Une ligne par combinaison de laboratoire / ville
        #
        # On explode uniquement ici, car le matcher travaille sur des
        # valeurs scalaires.
        # ----------------------------------------------------------------------

        ident_by_id['lab_merged'] = ident_by_id[
            'lab_merged'
        ].apply(safe_list)

        ident_by_id['city_back'] = ident_by_id[
            'city_back'
        ].apply(safe_list)

        ident_by_id = (
            ident_by_id
            .explode('lab_merged')
            .explode('city_back')
            .reset_index(drop=True)
        )


        # ----------------------------------------------------------------------
        # Dataset destiné au matcher
        # ----------------------------------------------------------------------

        org = ident_by_id.rename(
            columns={
                'city_back': 'city',
                'lab_merged': 'labo',
                'entities_full_2': 'supervisor'
            }
        ).copy()


        for col in ['supervisor', 'labo', 'city']:
            org[col] = org[col].fillna('').astype(str)


        strategies_id = [
            [
                [
                    'rnsr_code_number',
                    'rnsr_supervisor_name',
                    'rnsr_city'
                ]
            ],
            [
                [
                    'rnsr_code_number',
                    'rnsr_supervisor_name'
                ]
            ],
            [
                [
                    'rnsr_code_number',
                    'rnsr_city'
                ]
            ]
        ]


        df_id = run_rnsr_match(
            org,
            strategies=strategies_id,
            query_columns=['city', 'labo', 'supervisor']
        )


        lab_id = df_id.loc[
            df_id['match'].notna()
        ].copy()


        print(f"- RNSR trouvés : {len(lab_id)}")


        # ----------------------------------------------------------------------
        # Contrôle des ambiguïtés
        # ----------------------------------------------------------------------

        controle = (
            lab_id
            .explode('match')
            .dropna(subset=['match'])
            .groupby('labo')['match']
            .nunique()
        )

        ambigus = controle[controle > 1]

        if len(ambigus):
            print(
                f"⚠️ {len(ambigus)} laboratoires ont plusieurs RNSR"
            )
        else:
            print("✓ aucun laboratoire ambigu")


        lab_id = lab_id.drop(
            columns=['q'],
            errors='ignore'
        )

        lab_id.mask(lab_id == '', inplace=True)

        save_match(
            lab_id,
            'ident_lab1.pkl'
        )


    # ==========================================================================
    # 3. Préparation de la passe 2
    # ==========================================================================

    print("\n## préparation passe 2")


    # --------------------------------------------------------------------------
    # Pour faire la liaison avec lab_id, on reconstruit exactement les mêmes
    # valeurs scalaires que celles utilisées dans le matcher.
    # --------------------------------------------------------------------------

    lab_id_keys = lab_id[
        [
            'project_id',
            'generalPic',
            'call_year',
            'supervisor',
            'labo',
            'city',
            'match'
        ]
    ].copy()


    lab_id_keys['supervisor'] = (
        lab_id_keys['supervisor']
        .fillna('')
        .astype(str)
    )

    lab_id_keys['labo'] = (
        lab_id_keys['labo']
        .fillna('')
        .astype(str)
    )

    lab_id_keys['city'] = (
        lab_id_keys['city']
        .fillna('')
        .astype(str)
    )


    # --------------------------------------------------------------------------
    # Même préparation côté lab_a_ident
    # --------------------------------------------------------------------------

    lab_restants = lab_a_ident.copy()

    lab_restants['lab_merged'] = (
        lab_restants['lab_merged']
        .apply(safe_list)
    )

    lab_restants['city_back'] = (
        lab_restants['city_back']
        .apply(safe_list)
    )

    lab_restants = (
        lab_restants
        .explode('lab_merged')
        .explode('city_back')
        .reset_index(drop=True)
    )


    # --------------------------------------------------------------------------
    # Clés scalaires temporaires
    # --------------------------------------------------------------------------

    lab_restants['supervisor_key'] = (
        lab_restants['entities_full_2']
        .fillna('')
        .astype(str)
    )

    lab_restants['labo_key'] = (
        lab_restants['lab_merged']
        .fillna('')
        .astype(str)
    )

    lab_restants['city_key'] = (
        lab_restants['city_back']
        .fillna('')
        .astype(str)
    )


    # --------------------------------------------------------------------------
    # On retire les lignes déjà trouvées en passe 1.
    #
    # IMPORTANT :
    # aucune jointure sur une colonne contenant une liste.
    # --------------------------------------------------------------------------

    lab_restants = lab_restants.merge(
        lab_id_keys[
            [
                'project_id',
                'generalPic',
                'call_year',
                'supervisor',
                'labo',
                'city',
                'match'
            ]
        ],
        how='left',
        left_on=[
            'project_id',
            'generalPic',
            'call_year',
            'supervisor_key',
            'labo_key',
            'city_key'
        ],
        right_on=[
            'project_id',
            'generalPic',
            'call_year',
            'supervisor',
            'labo',
            'city'
        ],
        indicator=True
    )


    lab_restants = (
        lab_restants
        .loc[lab_restants['_merge'] == 'left_only']
        .drop(
            columns=[
                '_merge',
                'match',
                'supervisor',
                'labo',
                'city'
            ],
            errors='ignore'
        )
    )


    print(
        f"- lignes restantes après passe 1 : "
        f"{len(lab_restants)}"
    )


    # ==========================================================================
    # PASSE 2
    # MATCHING PAR NOM
    # ==========================================================================

    print("\n" + "-" * 70)
    print("PASSE 2 — NOM DU LABORATOIRE")
    print("-" * 70)


    if reload_matches:

        lab_name = load_match('ident_lab2.pkl')

    else:

        ident_by_name = lab_restants.loc[
            lab_restants['department_dup_2'].notna(),
            [
                'project_id',
                'generalPic',
                'call_year',
                'department_dup_2',
                'lab_merged',
                'entities_full_2',
                'city_back'
            ]
        ].copy()


        # ----------------------------------------------------------------------
        # Construction du nom du laboratoire
        # ----------------------------------------------------------------------

        ident_by_name['lab_merged'] = (
            ident_by_name['lab_merged']
            .apply(safe_list)
        )


        ident_by_name['lab_merged'] = (
            ident_by_name['lab_merged']
            .apply(lambda x: ' '.join(map(str, x)))
        )


        ident_by_name['labo'] = (
            ident_by_name[
                [
                    'department_dup_2',
                    'lab_merged'
                ]
            ]
            .fillna('')
            .astype(str)
            .agg(' '.join, axis=1)
            .str.replace(r'\s+', ' ', regex=True)
            .str.strip()
        )


        org = ident_by_name.rename(
            columns={
                'city_back': 'city',
                'entities_full_2': 'supervisor'
            }
        ).copy()


        # ----------------------------------------------------------------------
        # Le matcher travaille uniquement avec des valeurs scalaires
        # ----------------------------------------------------------------------

        for col in ['supervisor', 'labo', 'city']:
            org[col] = org[col].fillna('').astype(str)


        strategies_name = [
            [
                [
                    'rnsr_acronym',
                    'rnsr_name',
                    'rnsr_supervisor_name'
                ]
            ],
            [
                [
                    'rnsr_name',
                    'rnsr_supervisor_name'
                ]
            ],
            [
                [
                    'rnsr_acronym',
                    'rnsr_supervisor_name'
                ]
            ]
        ]


        df_name = run_rnsr_match(
            org,
            strategies=strategies_name,
            query_columns=['city', 'labo', 'supervisor']
        )


        lab_name = df_name.loc[
            df_name['match'].notna()
        ].copy()


        print(f"- RNSR trouvés : {len(lab_name)}")


        lab_name = lab_name.drop(
            columns=['q'],
            errors='ignore'
        )

        lab_name.mask(
            lab_name == '',
            inplace=True
        )

        save_match(
            lab_name,
            'ident_lab2.pkl'
        )


    # ==========================================================================
    # 4. Préparation des résultats des deux passes
    # ==========================================================================

    print("\n## fusion passe 1 + passe 2")


    print("LAB_ID")
    print(lab_id.columns.tolist())
    print(f"LAB_ID shape : {lab_id.shape}")

    print("\nLAB_NAME")
    print(lab_name.columns.tolist())
    print(f"LAB_NAME shape : {lab_name.shape}")


    # --------------------------------------------------------------------------
    # PASSE 1
    #
    # lab_id contient déjà les clés nécessaires.
    # --------------------------------------------------------------------------

    lab_id_full = lab_id[
        [
            'project_id',
            'generalPic',
            'call_year',
            'supervisor',
            'labo',
            'city',
            'match'
        ]
    ].copy()


    # --------------------------------------------------------------------------
    # PASSE 2
    #
    # lab_name ne possède pas nécessairement entities_full_2.
    # On le récupère depuis lab_restants en utilisant les clés scalaires
    # qui ont servi à construire labo.
    # --------------------------------------------------------------------------

    lab_name_full = lab_name.copy()


    # Clé du laboratoire utilisée dans le matcher
    lab_name_full['labo_key'] = (
        lab_name_full['labo']
        .fillna('')
        .astype(str)
    )

    lab_name_full['city_key'] = (
        lab_name_full['city']
        .fillna('')
        .astype(str)
    )


    # --------------------------------------------------------------------------
    # On construit une table de correspondance depuis lab_restants.
    #
    # PAS de drop_duplicates sur les listes :
    # toutes les colonnes de cette table sont scalaires.
    # --------------------------------------------------------------------------

    name_keys = lab_restants.copy()

    name_keys['labo_key'] = (
        name_keys['department_dup_2']
        .fillna('')
        .astype(str)
        + ' '
        + name_keys['lab_merged'].fillna('').astype(str)
    )

    name_keys['labo_key'] = (
        name_keys['labo_key']
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )

    name_keys['city_key'] = (
        name_keys['city_back']
        .fillna('')
        .astype(str)
    )


    name_keys = name_keys[
        [
            'project_id',
            'generalPic',
            'call_year',
            'department_dup_2',
            'entities_full_2',
            'labo_key',
            'city_key'
        ]
    ].copy()


    # --------------------------------------------------------------------------
    # Plusieurs lignes peuvent être parfaitement légitimes ici.
    #
    # On ne demande donc PAS validate='many_to_one'.
    # --------------------------------------------------------------------------

    name_keys = name_keys.drop_duplicates()


    lab_name_full = lab_name_full.merge(
        name_keys,
        how='left',
        on=[
            'project_id',
            'generalPic',
            'call_year',
            'labo_key',
            'city_key'
        ]
    )


    # --------------------------------------------------------------------------
    # On garde uniquement les lignes correctement documentées.
    # --------------------------------------------------------------------------

    lab_name_full = lab_name_full[
        [
            'project_id',
            'generalPic',
            'call_year',
            'department_dup_2',
            'entities_full_2',
            'labo',
            'city',
            'match'
        ]
    ].copy()


    # ==========================================================================
    # 5. Fusion des deux passes
    # ==========================================================================

    lab_matches = pd.concat(
        [
            lab_id_full,
            lab_name_full
        ],
        ignore_index=True,
        sort=False
    )


    lab_matches['match'] = (
        lab_matches['match']
        .apply(safe_list)
    )


    print(
        f"- total des correspondances : "
        f"{len(lab_matches)}"
    )


    # ==========================================================================
    # 6. Une ligne par entité
    #
    # Les clés sont uniquement scalaires.
    # ==========================================================================

    lab_matches['department_dup_2'] = (
        lab_matches['department_dup_2']
        .fillna('')
        .astype(str)
    )

    lab_matches['entities_full_2'] = (
        lab_matches['entities_full_2']
        .fillna('')
        .astype(str)
    )


    lab_ident = (
        lab_matches
        .groupby(
            [
                'call_year',
                'entities_full_2',
                'department_dup_2',
                'project_id',
                'generalPic'
            ],
            as_index=False
        )
        .agg(
            match=(
                'match',
                lambda x: unique_ordered(*x)
            )
        )
    )


    lab_ident.mask(
        lab_ident == '',
        inplace=True
    )


    save_match(
        lab_ident,
        'rnsr_detect.pkl'
    )


    # ==========================================================================
    # 7. Ajout des RNSR à keep
    # ==========================================================================

    print("\n## ajout des RNSR trouvés à keep")


    # --------------------------------------------------------------------------
    # On vérifie que lab_ident possède bien une seule ligne par clé.
    # --------------------------------------------------------------------------

    duplicate_keys = (
        lab_ident
        .duplicated(
            subset=[
                'call_year',
                'entities_full_2',
                'department_dup_2',
                'project_id',
                'generalPic'
            ],
            keep=False
        )
    )


    if duplicate_keys.any():

        print(
            "⚠️ lab_ident contient des clés de jointure dupliquées : "
            f"{duplicate_keys.sum()} lignes"
        )

        raise ValueError(
            "lab_ident n'est pas unique sur les clés de merge"
        )


    n_before = len(keep)


    keep = keep.merge(
        lab_ident,
        how='left',
        left_on=[
            'call_year',
            'entities_full_2',
            'department_dup_2',
            'project_id',
            'generalPic'
        ],
        right_on=[
            'call_year',
            'entities_full_2',
            'department_dup_2',
            'project_id',
            'generalPic'
        ],
        validate='many_to_one'
    )


    assert len(keep) == n_before, (
        "⚠️ Le merge des résultats RNSR a multiplié les lignes"
    )


    # --------------------------------------------------------------------------
    # Fusion des RNSR déjà présents et des nouveaux résultats
    # --------------------------------------------------------------------------

    keep['rnsr_merged'] = (
        keep['rnsr_merged']
        .apply(safe_list)
    )

    keep['match'] = (
        keep['match']
        .apply(safe_list)
    )


    keep['rnsr_merged'] = keep.apply(
        lambda x: unique_ordered(
            x['rnsr_merged'],
            x['match']
        ),
        axis=1
    )


    # ==========================================================================
    # PASSE 3
    # MATCHING PAR ORCID EMPLOYER
    # ==========================================================================

    print("\n" + "-" * 70)
    print("PASSE 3 — ORCID EMPLOYER")
    print("-" * 70)


    orphelins = keep.loc[
        keep['rnsr_merged'].str.len().eq(0)
        &
        keep['orcid_employer'].notna()
        &
        keep['orcid_employer'].astype(str).str.strip().ne(''),
        [
            'project_id',
            'generalPic',
            'call_year',
            'orcid_employer'
        ]
    ].drop_duplicates()


    print(f"- orphelins ORCID : {len(orphelins)}")


    if len(orphelins):

        if reload_matches:

            lab_orcid = load_match(
                'ident_lab_orcid.pkl'
            )

        else:

            org = (
                orphelins
                .rename(
                    columns={
                        'orcid_employer': 'labo'
                    }
                )
                .assign(
                    supervisor='',
                    city=''
                )
            )


            strategies_orcid = [
                [['rnsr_name']],
                [['rnsr_acronym']]
            ]


            df_orcid = run_rnsr_match(
                org,
                strategies=strategies_orcid,
                query_columns=['labo']
            )


            lab_orcid = df_orcid.loc[
                df_orcid['match'].notna()
            ].copy()


            print(
                f"- RNSR trouvés via ORCID : "
                f"{len(lab_orcid)}"
            )


            lab_orcid = lab_orcid.drop(
                columns=['q'],
                errors='ignore'
            )


            save_match(
                lab_orcid,
                'ident_lab_orcid.pkl'
            )


    else:

        lab_orcid = pd.DataFrame()


    # ==========================================================================
    # 8. Fusion des résultats ORCID
    # ==========================================================================

    if len(lab_orcid):

        lab_orcid['match'] = (
            lab_orcid['match']
            .apply(safe_list)
        )


        lab_orcid = (
            lab_orcid
            .groupby(
                [
                    'project_id',
                    'generalPic',
                    'call_year'
                ],
                as_index=False
            )['match']
            .agg(
                lambda x: unique_ordered(*x)
            )
            .rename(
                columns={
                    'match': 'rnsr_from_orcid'
                }
            )
        )


    else:

        lab_orcid = pd.DataFrame(
            columns=[
                'project_id',
                'generalPic',
                'call_year',
                'rnsr_from_orcid'
            ]
        )


    # --------------------------------------------------------------------------
    # Contrôle d'unicité
    # --------------------------------------------------------------------------

    duplicate_orcid = (
        lab_orcid
        .duplicated(
            subset=[
                'project_id',
                'generalPic',
                'call_year'
            ],
            keep=False
        )
    )


    assert not duplicate_orcid.any(), (
        "⚠️ Plusieurs résultats ORCID pour une même clé"
    )


    # ==========================================================================
    # 9. Ajout des RNSR ORCID à keep
    # ==========================================================================

    n_before = len(keep)


    keep = keep.merge(
        lab_orcid,
        how='left',
        on=[
            'project_id',
            'generalPic',
            'call_year'
        ],
        validate='many_to_one'
    )


    assert len(keep) == n_before, (
        "⚠️ Le merge ORCID a multiplié les lignes"
    )


    keep['rnsr_from_orcid'] = (
        keep['rnsr_from_orcid']
        .apply(safe_list)
    )


    # --------------------------------------------------------------------------
    # Les correspondances ORCID doivent être contrôlées
    # --------------------------------------------------------------------------

    mask_orcid = (
        keep['rnsr_from_orcid']
        .str.len()
        .gt(0)
    )

    keep.loc[
        mask_orcid,
        'resultat'
    ] = 'a controler'


    # --------------------------------------------------------------------------
    # Fusion finale
    # --------------------------------------------------------------------------

    keep['rnsr_merged'] = keep.apply(
        lambda x: unique_ordered(
            x['rnsr_merged'],
            x['rnsr_from_orcid']
        ),
        axis=1
    )


    # ==========================================================================
    # 10. Nettoyage
    # ==========================================================================

    keep.mask(
        keep == '',
        inplace=True
    )


    print("\n" + "=" * 70)
    print("FIN AFFILIATION RNSR")
    print("=" * 70)

    print(
        f"- lignes finales : {len(keep)}"
    )

    print(
        f"- lignes avec RNSR : "
        f"{keep['rnsr_merged'].str.len().gt(0).sum()}"
    )

    print(
        f"- lignes sans RNSR : "
        f"{keep['rnsr_merged'].str.len().eq(0).sum()}"
    )


    return keep