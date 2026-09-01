def entities_preparation(source_json, foreign_script=False):
    import pandas as pd, time, re, numpy as np, stopwordsiso, json
    pd.options.mode.copy_on_write = True
    from IPython.display import HTML
    from functions_shared import stop_word, unzip_zip, prep_str_col, work_csv, adr_tag
    from constant_vars import FRAMEWORK
    from paths import PATH_MATCH, PATH_HARVEST, PATH_CLEAN, PATH_ORG, PATH_WORK
    from remote_process.matcher import matcher
    from step9_affiliations.identify_rnsr import identify_rnsr
    


    # ================================================================
    # 0. HELPERS GENERIQUES (deplaces en tete de fonction)
    # ================================================================
    #
    # Deplaces ici car utilises tres tot dans le pipeline
    # (merge_lab_codes notamment). Auparavant definis tout en bas du
    # fichier, ce qui fonctionnait uniquement parce qu'ils etaient
    # dans le meme scope de fonction (resolution au moment de l'appel),
    # mais restait fragile/confus.
    # ================================================================

    def as_list(value):
        """
        Transforme différentes formes de données en liste propre.

        Exemples
        --------
        NaN / None / ""       -> []
        "abc;def"             -> ["abc", "def"]
        ["abc", "def"]        -> ["abc", "def"]
        np.array([...])       -> [...]
        tuple / set           -> [...]
        """

        # ----------------------------
        # None
        # ----------------------------

        if value is None:
            return []

        # ----------------------------
        # Listes / tuples / sets /
        # numpy arrays
        # ----------------------------

        if isinstance(
            value,
            (list, tuple, set, np.ndarray)
        ):
            values = value

        # ----------------------------
        # String
        # ----------------------------

        elif isinstance(value, str):

            values = value.split(';')

        # ----------------------------
        # Valeur scalaire
        # ----------------------------

        else:

            try:
                if pd.isna(value):
                    return []
            except (TypeError, ValueError):
                pass

            values = [value]

        # ----------------------------
        # Nettoyage
        # ----------------------------

        result = []

        for x in values:

            if x is None:
                continue

            try:
                if pd.isna(x):
                    continue
            except (TypeError, ValueError):
                pass

            x = str(x).strip()

            if not x:
                continue

            # Si une valeur contient encore un ;
            # on la découpe également.
            result.extend(
                part.strip()
                for part in x.split(';')
                if part.strip()
            )

        return result

    def unique_ordered(*values):
        """
        Fusionne plusieurs sources en conservant :

        - l'ordre d'apparition
        - une seule occurrence de chaque valeur
        """

        result = []

        for value in values:

            for item in as_list(value):

                if item not in result:
                    result.append(item)

        return result

    print(f"### IMPORT datasets")
    participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl")
    # participation = pd.read_pickle(f"{PATH_CLEAN}participation_complete.pkl")
    entities_info = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")
    # # entities = pd.read_pickle(f"{PATH_WORK}entities_participation_current.pkl")
    proj = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")
    nuts = pd.read_pickle("data_files/nuts_complet.pkl")

    countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl")
    lien = pd.read_pickle(f"{PATH_CLEAN}lien.pkl")
    perso = pd.read_pickle(f"{PATH_CLEAN}persons_current.pkl")

    pp_app = unzip_zip(source_json, 'proposals_applicants_departments.json', 'utf8')
    pp_app = pd.DataFrame(pp_app)
    pp_app = pp_app.rename(columns={'proposalNbr':'project_id', 'applicantPic':'pic','departmentApplicantName':'department'}).astype(str)
    pp_app = pp_app.replace({'None': np.nan})
    print(f"- size pp_app: {len(pp_app)}")

    pp_part = unzip_zip(source_json, 'projects_participants_departments.json', 'utf8')
    pp_part = pd.DataFrame(pp_part)
    pp_part = pp_part.rename(columns={'projectNbr':'project_id', 'participantPic':'pic','departmentParticipantName':'department'}).astype(str)
    pp_part = pp_part.replace({'None': np.nan})
    print(f"- size pp_part: {len(pp_part)}")


########

    def prep(stage, df, countries, lien):

        test = (
            df
            .merge(
                countries[['countryCode', 'countryCode_iso3', 'country_code']],
                how='left',
                on='countryCode'
            )
            .assign(stage=stage)
            .drop(
                columns=[
                    'countryCode',
                    'orderNumber',
                    'departmentUniqueId',
                    'framework',
                    'lastUpdateDate'
                ]
            )
            .drop_duplicates()
        )

        if stage == 'evaluated':
            tmp = (
                lien.loc[
                    lien.inProposal == True,
                    [
                        'project_id',
                        'generalPic',
                        'applicant_orderNumber',
                        'applicant_participant_pic',
                        'calculated_pic',
                        'applicant_nuts',
                        'n_app'
                    ]
                ]
                .rename(
                    columns={
                        'applicant_nuts': 'entities_nuts',
                        'applicant_participant_pic': 'pic',
                        'applicant_orderNumber': 'orderNumber',
                        'n_app': 'ent_nb'
                    }
                )
            )

        elif stage == 'successful':
            tmp = (
                lien.loc[
                    lien.inProject == True,
                    [
                        'project_id',
                        'generalPic',
                        'orderNumber',
                        'participant_pic',
                        'calculated_pic',
                        'participant_nuts',
                        'n_part'
                    ]
                ]
                .rename(
                    columns={
                        'participant_nuts': 'entities_nuts',
                        'participant_pic': 'pic',
                        'n_part': 'ent_nb'
                    }
                )
            )

        tmp = (
            tmp
            .merge(
                test,
                how='inner',
                on=['project_id', 'generalPic', 'pic']
            )
        )

        tmp['entities_nuts'] = tmp.apply(
            lambda x: ','.join(
                x.strip()
                for x in x.entities_nuts
                if x.strip()
            ),
            axis=1
        )

        return (
            tmp
            .rename(columns={'countryCode_iso3': 'country_code_source'})
            .sort_values('project_id')
            .drop_duplicates()
        )

    # ================================================================
    # 1. PREPARATION DES DEPARTEMENTS + 2. SUPPRESSION DOUBLONS
    #    evaluated/successful + 3. CONSTRUCTION DE STRUCT
    # ================================================================

    def build_struct():

        print("### departments datasets cleaning")

        app = prep('evaluated', pp_app, countries, lien)
        part_successful = prep('successful', pp_part, countries, lien)

        print(f"- app {len(app)}, part {len(part_successful)}")

        # ------------------------------------------------------------
        # SUPPRIMER DES evaluated LES PARTICIPATIONS DEJA successful
        # ------------------------------------------------------------

        lp = (
            part_successful[
                ['project_id', 'generalPic', 'pic', 'country_code_source']
            ]
            .drop_duplicates()
        )

        app_ = (
            app
            .merge(lp, how='left', indicator=True)
            .query('_merge == "left_only"')
            .drop(columns='_merge')
        )

        # ------------------------------------------------------------
        # CONSTRUCTION DE STRUCT
        # ------------------------------------------------------------

        print("\n## merge app+part -> struct")

        struct_ = pd.concat(
            [app_, part_successful],
            ignore_index=True
        )

        struct_['nb_stage'] = (
            struct_
            .groupby(
                [
                    'project_id',
                    'generalPic',
                    'country_code',
                    'orderNumber',
                    'calculated_pic',
                    'stage'
                ]
            )['department']
            .transform('count')
        )

        struct_ = struct_.rename(
            columns={
                'country_code_source': 'country_code_source_dept',
                'country_code': 'country_code_dept',
                'nutsCode': 'department_nuts'
            }
        )

        print(f"- size struct {len(struct_)}")

        return struct_

    struct = build_struct()

    # ================================================================
    # 4. PARTICIPATIONS + 5. PREPARATION STRUCT POUR MERGES
    #    + 6-10. MATCHING PROGRESSIF + 11-16. RESULTAT / CONTROLES
    # ================================================================

    def match_participation_to_struct(struct):

        subset_base = [
            'stage',
            'project_id',
            'generalPic',
            'orderNumber',
            'country_code',
            'country_code_source'
        ]

        subset_extended = subset_base + [
            'role',
            'participates_as'
        ]

        has_duplicates = (
            len(participation[subset_base].drop_duplicates())
            !=
            len(participation[subset_extended].drop_duplicates())
        )

        if has_duplicates:
            print(
                "- Attention : doublon d'une participation "
                "avec ajout de role + participates_as"
            )

        part = (
            participation[
                [
                    'project_id',
                    'generalPic',
                    'orderNumber',
                    'country_code',
                    'country_code_source',
                    'stage',
                    'numero_national_de_structure'
                ]
            ]
            .drop_duplicates()
        )

        print(f"- size participation : {len(part)}")

        # ============================================================
        # PREPARATION DE STRUCT POUR LES MERGES
        #
        # On remet les noms des colonnes de struct au même niveau que part.
        #
        # struct :
        #   country_code_dept
        #   country_code_source_dept
        #
        # deviennent :
        #   country_code
        #   country_code_source
        #
        # uniquement dans la copie utilisée pour le matching.
        # ============================================================

        struct_match = struct.rename(
            columns={
                'country_code_dept': 'country_code',
                'country_code_source_dept': 'country_code_source'
            }
        ).copy()

        # On ne veut surtout pas récupérer
        # numero_national_de_structure depuis struct.
        #
        # Cette colonne appartient à participation.
        struct_match = struct_match.drop(
            columns=['numero_national_de_structure'],
            errors='ignore'
        )

        # ============================================================
        # FONCTION DE MATCHING PROGRESSIF
        # ============================================================

        def match_level(remaining, struct_match, keys, level_name):

            matched = (
                remaining
                .merge(
                    struct_match,
                    how='inner',
                    on=keys,
                    suffixes=('', '_struct')
                )
                .drop_duplicates()
            )

            # Les clés qui ont trouvé au moins une correspondance.
            found = matched[keys].drop_duplicates()

            # On retire ces participations de remaining.
            remaining = (
                remaining
                .merge(
                    found,
                    how='left',
                    on=keys,
                    indicator=True
                )
                .query('_merge == "left_only"')
                .drop(columns='_merge')
            )

            matched['match_level'] = level_name

            print(
                f"- {level_name:<20} "
                f"matches={len(matched):>8} | "
                f"reste={len(remaining):>8}"
            )

            return matched, remaining

        # ============================================================
        # MATCHING NIVEAU 1
        #
        # Correspondance la plus stricte :
        #
        # stage
        # project_id
        # generalPic
        # orderNumber
        # country_code_source
        # ============================================================

        remaining = part.copy()
        matches = []

        keys1 = [
            'stage',
            'project_id',
            'generalPic',
            'orderNumber',
            'country_code_source'
        ]

        matched1, remaining = match_level(
            remaining,
            struct_match,
            keys1,
            'niveau_1_strict'
        )

        matches.append(matched1)

        # ============================================================
        # MATCHING NIVEAU 2
        #
        # On abandonne country_code_source.
        #
        # stage
        # project_id
        # generalPic
        # orderNumber
        # ============================================================

        keys2 = [
            'stage',
            'project_id',
            'generalPic',
            'orderNumber'
        ]

        matched2, remaining = match_level(
            remaining,
            struct_match,
            keys2,
            'niveau_2_sans_pays'
        )

        matches.append(matched2)

        # ============================================================
        # MATCHING NIVEAU 3
        #
        # On abandonne également generalPic.
        #
        # stage
        # project_id
        # orderNumber
        # ============================================================

        keys3 = [
            'stage',
            'project_id',
            'orderNumber'
        ]

        matched3, remaining = match_level(
            remaining,
            struct_match,
            keys3,
            'niveau_3_sans_generalPic'
        )

        matches.append(matched3)

        # ============================================================
        # MATCHING NIVEAU 4
        #
        # Ici on change de logique :
        #
        # - on réintroduit generalPic
        # - on réintroduit country_code
        # - on réintroduit country_code_source
        # - MAIS on abandonne stage
        #
        # Donc :
        #
        # project_id
        # generalPic
        # orderNumber
        # country_code
        # country_code_source
        # ============================================================

        keys4 = [
            'project_id',
            'generalPic',
            'orderNumber',
            'country_code',
            'country_code_source'
        ]

        # Pour le niveau 4, stage ne doit pas intervenir.
        struct_match4 = struct_match.drop(
            columns=['stage'],
            errors='ignore'
        )

        # Contrôle très utile :
        # combien de départements peuvent correspondre à une même clé ?
        print("\n### contrôle multiplicité niveau 4")

        multiplicity4 = (
            struct_match4
            .groupby(keys4)
            .size()
            .sort_values(ascending=False)
        )

        print(multiplicity4.head(20))

        matched4, remaining = match_level(
            remaining,
            struct_match4,
            keys4,
            'niveau_4_sans_stage'
        )

        matches.append(matched4)

        # ============================================================
        # PARTICIPATIONS SANS DEPARTEMENT
        # ============================================================

        remaining['match_level'] = 'non_apparie'

        print(
            f"- {'non_apparie':<20} "
            f"matches={0:>8} | "
            f"reste={len(remaining):>8}"
        )

        # ============================================================
        # RESULTAT FINAL
        # ============================================================

        part_final = pd.concat(
            matches + [remaining],
            ignore_index=True
        )

        # ============================================================
        # CONTROLES
        # ============================================================

        print("\n## résultat final")

        print(f"- participation initiale : {len(part)}")
        print(f"- résultat final          : {len(part_final)}")

        print("\n### répartition par niveau de matching")

        print(
            part_final['match_level']
            .value_counts(dropna=False)
        )

        # ============================================================
        # NB DE DEPARTEMENTS PAR PARTICIPATION
        # ============================================================

        part_final['nb'] = (
            part_final
            .groupby(
                [
                    'stage',
                    'project_id',
                    'generalPic',
                    'orderNumber'
                ]
            )['stage']
            .transform('count')
        )

        part_final['nb2'] = (
            part_final
            .groupby(
                [
                    'stage',
                    'project_id',
                    'generalPic',
                    'orderNumber',
                    'country_code_source'
                ]
            )['stage']
            .transform('count')
        )

        # ============================================================
        # PROPAGATION DES CODES PAYS
        # ============================================================

        part_final[
            ['country_code', 'country_code_source']
        ] = (
            part_final[
                ['country_code', 'country_code_source']
            ]
            .fillna(
                part_final
                .groupby(
                    [
                        'stage',
                        'project_id',
                        'generalPic',
                        'orderNumber'
                    ]
                )[
                    ['country_code', 'country_code_source']
                ]
                .ffill()
            )
        )

        print(f"- size part_final avant suppression : {len(part_final)}")

        # ============================================================
        # SUPPRESSION DES PARTICIPATIONS VIDES
        #
        # Si une participation possède plusieurs lignes mais qu'une ligne
        # n'a aucun département, on supprime cette ligne.
        # ============================================================

        part_final = part_final.loc[
            ~(
                (part_final['nb'] > 1)
                &
                (part_final['department'].isnull())
            )
        ]

        print(
            f"- end size with department -> part_final : "
            f"{len(part_final)}"
        )

        return part_final

    part_final = match_participation_to_struct(struct)

    # ================================================================
    # 17. MERGE PARTICIPATION + ENTITIES_INFO + PROJET
    # ================================================================

    def merge_entities_and_project(part_final):

        print("## merge participation + entities_info")

        entities_cols = [
            'generalPic',
            'entities_name_source',
            'entities_acronym_source',
            'category_woven',
            'city',
            'dep_code',
            'country_code_source',
            'country_code',
            'country_name_en',
            'id_secondaire',
            'entities_id',
            'entities_name',
            'entities_acronym',
            'operateur_num',
            'postalCode',
            'street',
            'webPage'
        ]

        structure_ = (
            part_final

            # --------------------------------------------------------
            # Ajout des informations de l'entité
            # --------------------------------------------------------
            .merge(
                entities_info[entities_cols],
                how='left',
                on=[
                    'generalPic',
                    'country_code_source',
                    'country_code'
                ]
            )

            # --------------------------------------------------------
            # Ajout de l'année du projet
            # --------------------------------------------------------
            .merge(
                proj[['project_id', 'call_year']].drop_duplicates(),
                how='inner',
                on=['project_id']
            )

            # --------------------------------------------------------
            # Renommage
            # --------------------------------------------------------
            .rename(
                columns={
                    'entities_name_source': 'leg',
                    'entities_acronym_source': 'acr',
                }
            )

            # --------------------------------------------------------
            # Colonnes temporaires de contrôle
            # --------------------------------------------------------
            .drop(
                columns=['nb_stage', 'nb', 'nb2'],
                errors='ignore'
            )

            .drop_duplicates()
        )

        # ============================================================
        # SUPPRESSION DES LIGNES SANS ENTITE
        # ============================================================

        structure_ = (
            structure_
            .loc[structure_.entities_name.notna()]
            .drop_duplicates()
        )

        print(f"- size structure + part_final : {len(structure_)}")

        return structure_

    structure = merge_entities_and_project(part_final)

    # ================================================================
    # 19. DUPLICATION DES VARIABLES POUR LE NETTOYAGE
    # ================================================================

    def duplicate_vars_for_cleaning(structure):

        print("## duplicate vars for cleaning")

        cols = [
            'department',
            'entities_acronym',
            'entities_name',
            'leg',
            'acr'
        ]

        for col in cols:
            structure[f'{col}_dup'] = structure[col]

        return structure

    structure = duplicate_vars_for_cleaning(structure)

    # ================================================================
    # 20. CONTROLE DE L'ANNEE DU PROJET
    # ================================================================

    if structure.call_year.isnull().any():

        print(
            "- vérification de l'année "
            "(corriger les nuls si existants):\n"
            f"{structure.call_year.value_counts(dropna=False)}"
        )

    # ================================================================
    # 21. ADD PERSONS DATA
    # ================================================================

    def add_persons_data(structure):

        print("## add PERSO")

        perso = pd.read_pickle(f"{PATH_WORK}perso_temp.pkl")

        perso_cols = [
            'project_id',
            'generalPic',
            'stage',
            'tel_clean',
            'email',
            'domaine_email',
            'contact',

            # infos employeur
            'employer_name',
            'employer_role',
            'employer_department',
            'employer_start_date',
            'employer_end_date',
            'employer_city',
            'employer_region',
            'employer_country',
            'employer_org_id',
            'employer_org_id_source',

            'orcid_unites_recherche',
            'country_match'
        ]

        perso = (
            perso[perso_cols]
            .drop_duplicates()
            .replace('', pd.NA)
        )

        # ============================================================
        # COUNTRY MATCH
        # ============================================================

        employer_cols = [
            'employer_name',
            'employer_role',
            'employer_department',
            'employer_start_date',
            'employer_end_date',
            'employer_city',
            'employer_region',
            'employer_country',
            'employer_org_id',
            'employer_org_id_source'
        ]

        # On conserve les informations employeur UNIQUEMENT lorsque
        # country_match == True.
        #
        # False ou NaN -> informations employeur supprimées.

        perso.loc[
            ~perso['country_match'].eq(True),
            employer_cols
        ] = pd.NA

        # ============================================================
        # NETTOYAGE
        # ============================================================

        cols = [
            'tel_clean',
            'email'
        ]

        perso[cols] = (
            perso[cols]
            .replace(r"\s+", "", regex=True)
        )

        perso[['contact', 'domaine_email']] = (
            perso[['contact', 'domaine_email']]
            .replace(r"\s+", "-", regex=True)
        )

        print(f"size perso before aggregation: {len(perso)}")

        # ============================================================
        # AGREGATION
        # ============================================================

        group_keys = [
            'project_id',
            'generalPic',
            'stage'
        ]

        # Variables pour lesquelles plusieurs valeurs peuvent exister
        # pour plusieurs chercheurs.
        #
        # Elles seront concaténées.
        concat_cols = [
            'tel_clean',
            'email',
            'domaine_email',
            'contact'
        ]

        # Informations employeur.
        #
        # Ici aussi plusieurs personnes peuvent avoir des informations
        # différentes. On conserve les valeurs uniques.
        #
        # On les concatène avec " | " afin de ne pas perdre l'information.
        employer_agg_cols = employer_cols

        def concat_unique(x):
            return " ".join(
                x.dropna()
                .astype(str)
                .loc[lambda s: s.ne('')]
                .unique()
            )

        perso_agg = (
            perso
            .groupby(group_keys, as_index=False)
            [concat_cols + employer_agg_cols]
            .agg(concat_unique)
        )

        # ============================================================
        # CONTROLE UNICITE APRES AGREGATION
        # ============================================================

        print(
            f"size perso after aggregation: "
            f"{len(perso_agg)}"
        )

        duplicates = perso_agg.duplicated(
            group_keys,
            keep=False
        )

        if duplicates.any():

            print(
                "🚨 ERREUR : doublons dans perso après agrégation"
            )

            print(
                perso_agg.loc[duplicates]
                .sort_values(group_keys)
            )

            raise ValueError(
                "perso_agg n'est pas unique sur "
                "project_id + generalPic + stage"
            )

        else:

            print(
                "✓ perso unique sur "
                "project_id + generalPic + stage"
            )

        perso = perso_agg

        # ============================================================
        # MERGE 1 :
        # project_id + generalPic + stage
        # ============================================================

        print(
            f"🔶 size structure before perso: "
            f"{len(structure)}"
        )

        tmp = (
            structure
            .drop(columns='_merge', errors='ignore')
            .merge(
                perso,
                how='left',
                on=group_keys,
                indicator=True
            )
        )

        print(
            f"🔶 size structure after merge stage: "
            f"{len(tmp)}"
        )

        # ============================================================
        # MATCHES DIRECTS
        # ============================================================

        tmp1 = (
            tmp[tmp['_merge'] == 'both']
            .drop(columns='_merge')
        )

        # ============================================================
        # RESTE À APPARIER
        # ============================================================

        tmp_left = (
            tmp[tmp['_merge'] == 'left_only']
            .drop(columns='_merge')
            .reset_index(drop=True)
        )

        tmp_left['_structure_id'] = tmp_left.index

        print(
            f"🔶 lignes restantes pour fallback contact: "
            f"{len(tmp_left)}"
        )

        # ============================================================
        # FALLBACK :
        #
        # project_id + generalPic + contact
        #
        # sans tenir compte du stage
        # ============================================================

        fallback_keys = [
            'project_id',
            'generalPic',
            'contact'
        ]

        # On enlève stage car justement on veut ignorer stage.
        perso_fallback = (
            perso
            .drop(columns='stage')
            .drop_duplicates()
        )

        # ============================================================
        # CONTROLE DE MULTIPLICITE DU FALLBACK
        # ============================================================

        fallback_duplicates = (
            perso_fallback
            .duplicated(fallback_keys, keep=False)
        )

        if fallback_duplicates.any():

            print(
                "⚠️ Attention : plusieurs lignes perso pour "
                "project_id + generalPic + contact"
            )

            print(
                perso_fallback.loc[fallback_duplicates]
                .sort_values(fallback_keys)
                .head(50)
            )

        # ============================================================
        # MERGE FALLBACK
        # ============================================================

        tmp2 = (
            tmp_left
            .merge(
                perso_fallback,
                how='inner',
                on=fallback_keys,
                suffixes=('', '_perso')
            )
        )

        print(
            f"🔶 lignes récupérées par fallback contact: "
            f"{len(tmp2)}"
        )

        # ============================================================
        # CONTROLE DE MULTIPLICATION
        # ============================================================

        if len(tmp2) > len(tmp_left):

            print(
                "🚨 ATTENTION : le fallback multiplie les lignes."
            )

            print(
                tmp2
                .groupby(fallback_keys)
                .size()
                .sort_values(ascending=False)
                .loc[lambda x: x > 1]
                .head(30)
            )

        # ============================================================
        # CONSTRUCTION DU RESULTAT
        # ============================================================

        if len(tmp2) > 0:

            matched_ids = tmp2['_structure_id'].unique()

            tmp_left_unmatched = tmp_left.loc[
                ~tmp_left['_structure_id'].isin(matched_ids)
            ]

            structure_ = pd.concat(
                [
                    tmp1,
                    tmp_left_unmatched,
                    tmp2
                ],
                ignore_index=True
            )

        else:

            structure_ = pd.concat(
                [
                    tmp1,
                    tmp_left
                ],
                ignore_index=True
            )

        # Nettoyage de l'identifiant technique
        structure_ = structure_.drop(
            columns='_structure_id',
            errors='ignore'
        )

        print(
            f"size structure after cleaning perso: "
            f"{len(structure_)}"
        )

        return structure_

    structure = add_persons_data(structure)

    # ================================================================
    # CLEAN text columns
    # ================================================================

    def clean_text_columns(structure):

        print("#### CLEANING")

        cols = [
            'department_dup',
            'leg_dup',
            'acr_dup',
            'entities_acronym_dup',
            'entities_name_dup',
            'employer_name',
            'employer_department',
            'street',
            'city'
        ]

        structure = prep_str_col(structure, cols)

        #-----------------------------
        # department

        print("# department")

        word_delete = (
            "department name|name of the department|same as legal name|"
            "not applicable|non applicable|n a|department if applicable"
        )

        structure.loc[
            structure.department.notna(),
            'department_dup'
        ] = (
            structure.loc[
                structure.department.notna(),
                'department_dup'
            ]
            .str.replace(word_delete, ' ', regex=True)
            .str.strip()
        )


        #-----------------------------
        # city, postal code

        print("# city, postalcode")

        cedex = (
            "cedax|cedrex|cdexe|cdex|credex|cedex|cedx|cede|"
            "ceddex|cdx|cex|cexex|edex"
        )

        structure.loc[
            structure.postalCode.isnull(),
            'postalCode'
        ] = (
            structure.loc[
                structure.postalCode.isnull(),
                'city'
            ]
            .str.extract(r"(\d+)", expand=False)
        )

        structure['city'] = (
            structure['city']
            .str.replace(r"\d+", ' ', regex=True)
            .str.strip()
        )

        structure.loc[
            structure.country_code == 'FRA',
            'city'
        ] = (
            structure.loc[
                structure.country_code == 'FRA',
                'city'
            ]
            .str.replace(cedex, ' ', regex=True)
            .str.strip()
        )

        structure.loc[
            structure.country_code == 'FRA',
            'city'
        ] = (
            structure.loc[
                structure.country_code == 'FRA',
                'city'
            ]
            .str.replace(
                r"^france$",
                '',
                regex=True
            )
            .str.strip()
        )

        return structure

    structure = clean_text_columns(structure)

    # ================================================================
    # CREATION entities_full
    # ================================================================

    def create_entities_full(structure):

        print("## creation entities_full")

        # Par défaut : pas de valeur
        structure['entities_full'] = pd.NA

        # ------------------------------------------------------------
        # 1. On peut construire entities_full avec leg + acr
        # ------------------------------------------------------------

        mask = (
            structure['leg_dup'].notna()
            & structure['acr_dup'].notna()
            & structure['leg_dup'].ne('')
            & structure['acr_dup'].ne('')
        )

        structure.loc[mask, 'entities_full'] = (
            structure.loc[mask, ['leg_dup', 'acr_dup']]
            .apply(
                lambda x:
                    x['leg_dup']
                    if x['acr_dup'] in x['leg_dup']
                    else f"{x['leg_dup']} {x['acr_dup']}",
                axis=1
            )
        )

        # ------------------------------------------------------------
        # 2. Fallback : entities_name_dup uniquement si entities_full
        #    n'a pas pu être construite
        # ------------------------------------------------------------

        mask_fallback = (
            structure['entities_full'].isna()
            & structure['entities_name_dup'].notna()
        )

        structure.loc[mask_fallback, 'entities_full'] = (
            structure.loc[mask_fallback, 'entities_name_dup']
        )

        # ------------------------------------------------------------
        # 3. Nettoyage final
        # ------------------------------------------------------------

        structure['entities_full'] = (
            structure['entities_full']
            .replace('', pd.NA)
            .str.strip()
        )

        print(
            f"- entities_full renseigné : "
            f"{structure['entities_full'].notna().sum()} / {len(structure)}"
        )

        print(
            f"- fallback entities_name_dup : "
            f"{mask_fallback.sum()}"
        )

        return structure

    structure = create_entities_full(structure)

    # ================================================================
    # ORGANISATION TYPE
    # ================================================================

    def classify_organisation_type(structure):

        print("## identification organisation type")

        # --------------------------------------------------------------
        # COMPANY / LEGAL FORMS
        # si besoin package CLEANCO
        # --------------------------------------------------------------

        with open(
            'data_files/organisation_type_terms.json',
            encoding='utf-8'
        ) as f:
            organisation_terms = json.load(f)

        company_terms = set(organisation_terms['company_terms'])
        company_words = set(organisation_terms['company_words'])
        association_terms = set(organisation_terms['association_terms'])

        def make_word_regex(words):
            words = sorted(
                map(re.escape, words),
                key=len,
                reverse=True
            )

            return re.compile(
                r'\b(?:' + '|'.join(words) + r')\b',
                flags=re.IGNORECASE
            )

        company_re = make_word_regex(
            company_terms | company_words
        )

        association_re = make_word_regex(
            association_terms
        )

        # --------------------------------------------------------------
        # CLASSIFICATION
        # --------------------------------------------------------------

        def classify_org(name, category=None):

            if pd.isna(name):
                return pd.Series({
                    'org_type': pd.NA,
                    'org_reason': pd.NA
                })

            name = str(name)

            # Association prioritaire
            # car certains termes peuvent également apparaître
            # dans des noms plus généraux.
            if association_re.search(name):
                return pd.Series({
                    'org_type': 'association',
                    'org_reason': 'association_term'
                })

            if company_re.search(name):
                return pd.Series({
                    'org_type': 'company',
                    'org_reason': 'company_term'
                })

            if category == 'Entreprise':
                return pd.Series({
                    'org_type': 'company',
                    'org_reason': 'category_woven'
                })

            if category == 'Institutions sans but lucratif (ISBL)':
                return pd.Series({
                    'org_type': 'association',
                    'org_reason': 'category_woven'
                })

            return pd.Series({
                'org_type': pd.NA,
                'org_reason': pd.NA
            })

        tmp_org = structure.apply(
            lambda x: classify_org(
                x['entities_full'],
                x['category_woven']
            ),
            axis=1
        )

        structure[['org_type', 'org_reason']] = tmp_org

        return structure

    structure = classify_organisation_type(structure)

    # ================================================================
    # STOPWORDS MULTILINGUES + ORCID EMPLOYER
    # ================================================================

    def apply_stopwords_cleaning(structure):

        print("## stop words")

        # ------------------------------------------------------------
        # Langues principales par pays
        #
        # L'anglais est ajouté systématiquement plus bas.
        # ------------------------------------------------------------

        COUNTRY_LANGUAGES = {
            'AUT': ['de'],
            'BEL': ['fr', 'nl', 'de'],
            'CHE': ['de', 'fr', 'it'],
            'DEU': ['de'],
            'DNK': ['da'],
            'ESP': ['es'],
            'FIN': ['fi'],
            'FRA': ['fr'],
            'GBR': ['en'],
            'IRL': ['en'],
            'ITA': ['it'],
            'NLD': ['nl'],
            'NOR': ['no'],
            'PRT': ['pt'],
            'SWE': ['sv'],
        }

        # ------------------------------------------------------------
        # Cache des stopwords
        #
        # Evite de recalculer les mêmes ensembles pour chaque ligne.
        # ------------------------------------------------------------

        _STOPWORDS_CACHE = {}

        def get_stopwords(country_code):
            """
            Retourne les stopwords correspondant au pays + anglais.

            Exemple :
                FRA -> français + anglais
                DEU -> allemand + anglais
                BEL -> français + néerlandais + allemand + anglais
                pays inconnu -> anglais
            """

            if pd.isna(country_code):
                country_code = None

            country_code = str(country_code).upper()

            if country_code in _STOPWORDS_CACHE:
                return _STOPWORDS_CACHE[country_code]

            languages = set(
                COUNTRY_LANGUAGES.get(country_code, [])
            )

            # Tous les pays peuvent potentiellement avoir
            # des noms d'organisation en anglais.
            languages.add('en')

            stopwords = set()

            for lang in languages:
                try:
                    stopwords.update(
                        stopwordsiso.stopwords(lang)
                    )
                except Exception:
                    print(
                        f"⚠️ langue inconnue pour stopwordsiso : {lang}"
                    )

            _STOPWORDS_CACHE[country_code] = stopwords

            return stopwords

        # ------------------------------------------------------------
        # Fonction de suppression
        # ------------------------------------------------------------

        def remove_stopwords(text, country_code):

            if pd.isna(text):
                return pd.NA

            text = str(text).strip()

            if not text:
                return pd.NA

            stopwords = get_stopwords(country_code)

            words = text.split()

            words = [
                word
                for word in words
                if word.lower() not in stopwords
            ]

            result = ' '.join(words).strip()

            return result if result else pd.NA

        # ------------------------------------------------------------
        # entities_full
        # ------------------------------------------------------------

        structure['entities_full_2'] = structure.apply(
            lambda x: remove_stopwords(
                x['entities_full'],
                x['country_code']
            ),
            axis=1
        )

        # ------------------------------------------------------------
        # department
        # ------------------------------------------------------------

        structure['department_dup_2'] = structure.apply(
            lambda x: remove_stopwords(
                x['department_dup'],
                x['country_code']
            ),
            axis=1
        )

        # ============================================================
        # ORCID EMPLOYER
        # ============================================================

        print("## cleaning ORCID employer")

        structure['employer_name_2'] = structure.apply(
            lambda x: remove_stopwords(
                x['employer_name'],
                x['country_code']
            ),
            axis=1
        )

        structure['employer_department_2'] = structure.apply(
            lambda x: remove_stopwords(
                x['employer_department'],
                x['country_code']
            ),
            axis=1
        )

        # ------------------------------------------------------------
        # Concaténation employer + department
        #
        # Exemple :
        #
        # employer_name       = University of Caen
        # employer_department = Department of Biology
        #
        # =>
        # orcid_employer = university caen biology
        # ------------------------------------------------------------

        structure['orcid_employer'] = (
            structure[
                ['employer_name_2', 'employer_department_2']
            ]
            .fillna('')
            .astype(str)
            .apply(
                lambda x: ' '.join(
                    value.strip()
                    for value in x
                    if value.strip()
                ),
                axis=1
            )
            .replace('', pd.NA)
        )

        # ============================================================
        # CONTROLES
        # ============================================================

        print(
            f"- entities_full renseigné  : "
            f"{structure['entities_full'].notna().sum()}"
        )

        print(
            f"- entities_full_2 renseigné: "
            f"{structure['entities_full_2'].notna().sum()}"
        )

        print(
            f"- department renseigné      : "
            f"{structure['department_dup'].notna().sum()}"
        )

        print(
            f"- department_dup_2 renseigné: "
            f"{structure['department_dup_2'].notna().sum()}"
        )

        print(
            f"- orcid_employer renseigné  : "
            f"{structure['orcid_employer'].notna().sum()}"
        )

        # ------------------------------------------------------------
        # Nettoyage final des chaînes vides
        # ------------------------------------------------------------

        structure = structure.mask(structure == '')

        return structure

    structure = apply_stopwords_cleaning(structure)

    # ================================================================
    # ================================================================
    # FRANCE
    # ================================================================

    def build_structure_fr(structure):

        print("## create dataset structure_fr")

        structure_fr = structure.loc[
            structure['country_code'].eq('FRA')
        ].copy()

        print(f"size structure_fr: {len(structure_fr)}")

        return structure_fr

    structure_fr = build_structure_fr(structure)

    # ================================================================
    # 1. IDENTIFICATION DES ORGANISMES
    # ================================================================

    def identify_organismes(structure_fr):

        print("## Identification organismes from patterns")

        def qualif_organisation(x):
            """
            Identifie les grands organismes français à partir du nom.
            Retourne une liste de tags.
            add insa, ecole nat
            """

            if not isinstance(x, str) or not x.strip():
                return []

            patterns = {
                'cnrs': [
                    r"\bcnrs\b",
                    r"ce.*na.*(de )?(la )?re.*sc",
                    r"fr.*na.*sc.*re.*ce"
                ],

                'inria': [
                    r"\binria\b",
                    r"in.*na.*(de )?re.*(en )?in.*(et )?(en )?au"
                ],

                'inrae': [
                    r"\binrae\b",
                    r"\binra\b",
                    r"\birstea\b",
                    r"in.*na.*(de )?re.*ag"
                ],

                'inserm': [
                    r"\binserm\b",
                    r"in.*na.*(de )?(la )?sa.*(et )?(de )?(la )?re.*me"
                ],

                'cea': [
                    r"\bcea\b",
                    r"co.*(a )?l?\'?en.*at"
                ],

                'ens': [
                    r"\bens\b",
                    r"ec.*no.*sup"
                ],

                'fnsp': [
                    r"\bfnsp\b",
                    r"fo.*na.*(des )?sc.*po",
                    r"\bsciences po\b"
                ],

                'cirad': [
                    r"\bcirad\b",
                    r"ce.*(de )?co.*in.*(en )?re.*ag.*(pour )?(le )?dev"
                ],

                'ird': [
                    r"\bird\b",
                    r"in.*(de )?re.*(pour )?(le )?dev",
                    r"\bi r d\b"
                ],

                'chu': [
                    r"\bchu\b",
                    r"\bchr\b",
                    r"\bchru\b",
                    r"hospice",
                    r"(ce.*|ctre|group.*) hos.*(univ)?",
                    r"univ.*hosp"
                ],

                'universite': [
                    r"\buniv(ersite|ersity|ersitaire)\b"
                ],

                'pasteur': [
                    r"\bpasteur\b",
                    r"ins.*pasteur",
                    r"pasteur inst"
                ],

                'curie': [
                    r"\bcurie\b",
                    r"inst.*curie",
                    r"curie inst"
                ],

                'irsn': [
                    r"\birsn\b",
                    r"in.*(de )?radio.*(et )?(de )?sur.*nuc"
                ],

                'onera': [
                    r"\bonera\b",
                    r"off.*na.*(d )?etu.*(et )?(de )?rech.*aero"
                ],

                'agrocampus': [
                    r"\bagrocampus\b"
                ],

                'ed': [
                    r"\bed\b",
                    r"doct.*sch",
                    r"ec.*doct"
                ],

                'ecole': [
                    r"\becole\b"
                ]
            }

            result = []

            for name, regex_list in patterns.items():
                if any(re.search(pattern, x, flags=re.IGNORECASE)
                    for pattern in regex_list):
                    result.append(name)

            return result

        # --------------------------------------------------
        # Identification sur les 3 champs
        # --------------------------------------------------

        structure_fr['org1'] = structure_fr['department_dup'].apply(
            qualif_organisation
        )

        structure_fr['org2'] = structure_fr['entities_full'].apply(
            qualif_organisation
        )

        structure_fr['org3'] = structure_fr['entities_name_dup'].apply(
            qualif_organisation
        )

        structure_fr['orc_employ'] = structure_fr['orcid_employer'].apply(
            qualif_organisation
        )

        # --------------------------------------------------
        # Fusion des résultats
        # --------------------------------------------------

        structure_fr['org_from_lib'] = (
            structure_fr[['org1', 'org2']]
            .apply(
                lambda x: list(dict.fromkeys(
                    x['org1'] + x['org2']
                )),
                axis=1
            )
        )

        structure_fr.drop(
            columns=['org1', 'org2'],
            inplace=True
        )

        return structure_fr

    structure_fr = identify_organismes(structure_fr)

    # ================================================================
    # 2. PRÉPARATION DES CHAMPS LABORATOIRE
    # 3. EXTRACTION DES SIGLES + NUMÉROS DE LABO
    # 4. NETTOYAGE
    # ================================================================

    def extract_labo_codes_section(structure_fr):

        print("## Identification labos from pattern -> lab_from_lib")

        structure_fr['dep_tag'] = structure_fr['department_dup']
        structure_fr['lab_tag'] = structure_fr['entities_full_2']
        structure_fr['orc_tag'] = structure_fr['orcid_employer']

        lab_cols = ['dep_tag', 'lab_tag', 'orc_tag']

        # --------------------------------------------------
        # Normalisation des libellés
        # --------------------------------------------------

        replacements = {
            'international research lab': 'irl',
            'joint research unit': 'jru',
            'equipe accueil': 'ea',
        }

        for col in lab_cols:

            for old, new in replacements.items():
                structure_fr[col] = structure_fr[col].str.replace(
                    old,
                    new,
                    regex=False
                )

        # --------------------------------------------------
        # UMR / INSERM / CNRS
        # --------------------------------------------------

        for col in lab_cols:

            # UMR / UMR S / unité / INSERM...
            structure_fr[col] = structure_fr[col].str.replace(
                r"""
                \bumr(\s?s\s?)?
                (u(\s?)|inserm(\s?))?
                (?=(\d+)?)
                |
                \bu\s?inserm(\s?)
                |
                \bunit(e?) (?=(\s?u?\s?\d+))
                |
                \binserm\s?(umr\s?(s?)|jru)\s?(u?)
                |
                \binserm(u?)\s?(?=\d+)
                |
                \binserm\s?un\s?umr\s?u?
                """,
                "u",
                regex=True
            )

            # suppression du nom intermédiaire entre le sigle et le numéro
            for sigle in [
                'umr', 'upr', 'uar', 'irl', 'emr',
                'umi', 'usr', 'fre', 'gdr', 'fr'
            ]:
                structure_fr[col] = structure_fr[col].str.replace(
                    rf"(?<=\b{sigle})\s?[a-z]+\s?(?=\d+)",
                    " ",
                    regex=True
                )

            # CNRS
            structure_fr[col] = structure_fr[col].str.replace(
                r"\bu\s?cnrs|\bum\s+r|\bcnrs\s?(?=\d+)|\bjru\s?(cnrs|umr)",
                "umr",
                regex=True
            )

            # JRU + UMI
            structure_fr[col] = structure_fr[col].str.replace(
                r"\bjru\s?(umi)",
                "umi",
                regex=True
            )

            # CIC
            structure_fr[col] = structure_fr[col].str.replace(
                r"""
                (\bce[a-z]*\s+inv[a-z]*\s+cl[a-z]*)
                |
                (\bcl[a-z]*\s+inv[a-z]*\s+ce[a-z]*)
                |
                (\bce[a-z]*\s+cl[a-z]*\s+inv[a-z]*)
                """,
                "cic",
                regex=True
            )

        # --------------------------------------------------
        # Règles dépendant de l'organisme
        # --------------------------------------------------

        mask_inserm = structure_fr['org_from_lib'].apply(
            lambda x: 'inserm' in x
        )

        mask_cnrs = structure_fr['org_from_lib'].apply(
            lambda x: 'cnrs' in x
        )

        structure_fr.loc[mask_inserm, lab_cols] = (
            structure_fr.loc[mask_inserm, lab_cols]
            .apply(
                lambda x: x.str.replace(
                    r"\bjru\b",
                    "u",
                    regex=True
                )
            )
        )

        structure_fr.loc[mask_cnrs, lab_cols] = (
            structure_fr.loc[mask_cnrs, lab_cols]
            .apply(
                lambda x: x.str.replace(
                    r"\bjru\b",
                    "umr",
                    regex=True
                )
            )
        )

        # ============================================================
        # EXTRACTION DES SIGLES + NUMÉROS DE LABO
        # ============================================================

        print("## Extract laboratory codes")

        # IMPORTANT :
        # L'ordre est volontaire.
        #
        # Exemple :
        #     UMR S 1234 -> u1234
        #     UMR 1234   -> umr1234
        #
        # Les formes spécifiques doivent donc être testées AVANT
        # les formes génériques.

        LAB_PATTERNS = [

            # --------------------------------------------------------
            # UMR spécialisées
            # --------------------------------------------------------

            (r"\bumr\s*s\s*(\d+)", "u"),
            (r"\bumr\s*a\s*(\d+)", "u"),
            (r"\bumr\s*m\s*(\d+)", "u"),
            (r"\bumr\s*t\s*(\d+)", "u"),
            (r"\bumr\s*d\s*(\d+)", "u"),

            # --------------------------------------------------------
            # formes particulières
            # --------------------------------------------------------

            (r"\bupesa\s*(\d+)", "upesa"),
            (r"\bumemi\s*(\d+)", "umemi"),
            (r"\bertint\s*(\d+)", "ertint"),

            # --------------------------------------------------------
            # formes classiques
            # --------------------------------------------------------

            (r"\bumrs?\s*(\d+)", "umr"),
            (r"\bua\s*(\d+)", "ua"),
            (r"\bea\s*(\d+)", "ea"),
            (r"\bgdr\s*(\d+)", "gdr"),
            (r"\bfre\s*(\d+)", "fre"),
            (r"\bfrc\s*(\d+)", "frc"),
            (r"\bfed\s*(\d+)", "fed"),
            (r"\bje\s*(\d+)", "je"),
            (r"\busr\s*(\d+)", "usr"),
            (r"\bums\s*(\d+)", "ums"),
            (r"\bupr\s*(\d+)", "upr"),
            (r"\bifr\s*(\d+)", "ifr"),
            (r"\bepi\s*(\d+)", "epi"),
            (r"\beac\s*(\d+)", "eac"),
            (r"\bert\s*(\d+)", "ert"),
            (r"\bur\s*(\d+)", "ur"),
            (r"\bups\s*(\d+)", "ups"),
            (r"\buar\s*(\d+)", "uar"),
            (r"\bura\s*(\d+)", "ura"),
            (r"\brtra\s*(\d+)", "rtra"),
            (r"\bue\s*(\d+)", "ue"),
            (r"\bers\s*(\d+)", "ers"),
            (r"\bcic\s*(\d+)", "cic"),
            (r"\bep\s*(\d+)", "ep"),
            (r"\bumi\s*(\d+)", "umi"),
            (r"\bunit\s*(\d+)", "unit"),
            (r"\bemr\s*(\d+)", "emr"),
            (r"\birl\s*(\d+)", "irl"),
            (r"\bjru\s*(\d+)", "jru"),

            # à mettre en dernier car très générique
            (r"\bu\s*(\d+)", "u"),
        ]

        def extract_lab_codes(text):
            """
            Extrait les codes de laboratoire en conservant
            l'ordre des règles et en supprimant les doublons.
            """

            if not isinstance(text, str):
                return []

            result = []

            for pattern, sigle in LAB_PATTERNS:

                for number in re.findall(
                    pattern,
                    text.lower()
                ):
                    code = f"{sigle}{number}"

                    if code not in result:
                        result.append(code)

            return result

        # --------------------------------------------------
        # Extraction depuis department / entities / ORCID
        # --------------------------------------------------

        structure_fr['org1'] = structure_fr['dep_tag'].apply(
            extract_lab_codes
        )

        structure_fr['org2'] = structure_fr['lab_tag'].apply(
            extract_lab_codes
        )

        structure_fr['org3'] = structure_fr['orc_tag'].apply(
            extract_lab_codes
        )

        # --------------------------------------------------
        # Fusion avec priorité :
        #
        # 1. department
        # 2. entities
        # 3. ORCID uniquement si department ET entities
        #    ne donnent aucun résultat
        # --------------------------------------------------

        def merge_lab_codes(org1, org2, org3):

            if org1:
                return unique_ordered(org1, org2)

            if org2:
                return unique_ordered(org2)

            return unique_ordered(org3)

        structure_fr['lab_from_lib'] = [
            merge_lab_codes(org1, org2, org3)
            for org1, org2, org3 in zip(
                structure_fr['org1'],
                structure_fr['org2'],
                structure_fr['org3']
            )
        ]

        # ============================================================
        # NETTOYAGE
        # ============================================================

        structure_fr.drop(
            columns=[
                'org1',
                'org2',
                'org3',
                'dep_tag',
                'lab_tag',
                'orc_tag'
            ],
            inplace=True
        )

        structure_fr = structure_fr.mask(
            structure_fr == ''
        )

        print(f"size structure_fr: {len(structure_fr)}")

        return structure_fr

    structure_fr = extract_labo_codes_section(structure_fr)

    # ================================================================
    # RETOUR ORGANISMES
    # ================================================================

    def load_and_prepare_organisme_back(structure_fr):

        print("### add data from organisme")

        # ------------------------------------------------
        # Import
        # ------------------------------------------------

        organisme_back = (
            pd.read_pickle(f"{PATH_ORG}organisme_back.pkl")
            .drop_duplicates()
            .drop(
                columns=[
                    'lib_back',
                    'location_back',
                    'role',
                    'participates_as'
                ],
                errors='ignore'
            )
            .drop_duplicates()
        )

        print(f"- size imported dataset: {len(organisme_back)}")

        # ============================================================
        # 1. STAGES DISPONIBLES DANS STRUCTURE
        # ============================================================

        stage_proj = (
            structure_fr[
                ['stage', 'project_id']
            ]
            .drop_duplicates()
        )

        # ============================================================
        # 2. SUCCESSFUL
        # ============================================================

        # Pour successful :
        # organisme_back.orderNumber = orderNumber
        #
        # On utilise directement orderNumber.
        # proposal_orderNumber n'est pas nécessaire.

        organisme1 = (
            organisme_back
            .merge(
                stage_proj,
                how='inner',
                on='project_id'
            )
            .query("stage == 'successful'")
            .drop(
                columns=['proposal_orderNumber'],
                errors='ignore'
            )
            .drop_duplicates()
        )

        print(
            f"- identification orga for successful: "
            f"{len(organisme1)}"
        )

        # ============================================================
        # 3. EVALUATED
        # ============================================================

        # Pour evaluated :
        #
        # priorité :
        #     proposal_orderNumber
        #
        # sinon :
        #     orderNumber
        #
        # On crée donc un orderNumber final avant le merge.

        organisme_back_eval = organisme_back.copy()

        organisme_back_eval['proposal_orderNumber'] = (
            organisme_back_eval['proposal_orderNumber']
            .fillna(organisme_back_eval['orderNumber'])
        )

        organisme2 = (
            organisme_back_eval
            .merge(
                stage_proj,
                how='inner',
                on='project_id'
            )
            .query("stage == 'evaluated'")
            .drop(
                columns=['orderNumber'],
                errors='ignore'
            )
            .rename(
                columns={
                    'proposal_orderNumber': 'orderNumber'
                }
            )
            .drop_duplicates()
        )

        print(
            f"- identification orga for evaluated: "
            f"{len(organisme2)}"
        )

        # ============================================================
        # 4. CONCATÉNATION
        # ============================================================

        oback = pd.concat(
            [organisme1, organisme2],
            ignore_index=True
        ).drop_duplicates()

        # ============================================================
        # 5. AGRÉGATION
        # ============================================================

        def concat_unique(values, sep=';'):
            """
            Concatène les valeurs non nulles et non vides,
            en supprimant les doublons tout en conservant leur ordre.
            """

            result = []

            for value in values:

                if pd.isna(value):
                    continue

                value = str(value).strip()

                if not value:
                    continue

                if value not in result:
                    result.append(value)

            return sep.join(result) if result else pd.NA

        group_cols = [
            'stage',
            'project_id',
            'generalPic',
            'pic',
            'orderNumber'
        ]

        oback = (
            oback
            .groupby(
                group_cols,
                dropna=False
            )
            .agg(concat_unique)
            .reset_index()
        )

        # ============================================================
        # 6. NORMALISATION DES ORGANISMES
        # ============================================================

        for col in ['labo_back', 'org_back']:

            if col in oback.columns:
                oback[col] = (
                    oback[col]
                    .astype('string')
                    .str.lower()
                    .str.strip()
                )

        # Remplace les chaînes vides par NA
        oback = oback.mask(
            oback == ''
        )

        print(f"- size oback: {len(oback)}")

        return oback

    oback = load_and_prepare_organisme_back(structure_fr)

    # ================================================================
    # MERGE ORGANISMES ET STRUCTURE
    # ================================================================

    def merge_organismes_et_structure(structure_fr, oback):

        print("### merge organismes + structure")

        # --------------------------------------------------
        # 1. MERGE
        # --------------------------------------------------
        # structure_fr reste le dataset maître.
        #
        # outer permet néanmoins de détecter les lignes présentes
        # uniquement dans oback.
        # --------------------------------------------------

        tmp = (
            structure_fr
            .merge(
                oback,
                how='outer',
                on=[
                    'stage',
                    'project_id',
                    'generalPic',
                    'pic'
                ],
                indicator=True,
                suffixes=('', '_back')
            )
        )

        # --------------------------------------------------
        # On conserve uniquement les lignes provenant
        # de structure_fr.
        #
        # Les lignes oback seules (right_only) sont ignorées.
        # --------------------------------------------------

        keep = (
            tmp.loc[tmp['_merge'].ne('right_only')]
            .drop(
                columns=[
                    '_merge',
                    'orderNumber_back'
                ],
                errors='ignore'
            )
        )

        print(f"- size keep after merge: {len(keep)}")

        # ============================================================
        # 3. NORMALISATION DES COLONNES OBACK
        # ============================================================

        for col in [
            'rnsr_back',
            'labo_back',
            'org_back',
            'city_back'
        ]:

            if col in keep.columns:
                keep[col] = keep[col].apply(as_list)

        # ============================================================
        # 4. ORGANISMES
        #
        # Source 1 : org_from_lib
        #     -> identification par tes règles / patterns
        #
        # Source 2 : org_back
        #     -> identification provenant de organisme_back
        #
        # On conserve l'ordre :
        #
        #       org_from_lib
        #       puis org_back
        #
        # et on supprime les doublons sans utiliser set().
        # ============================================================

        keep['org_merged'] = [
            unique_ordered(
                org_lib,
                org_back
            )
            for org_lib, org_back
            in zip(
                keep['org_from_lib'],
                keep['org_back']
            )
        ]

        # ============================================================
        # 5. LABORATOIRES
        #
        # Source 1 : lab_from_lib
        # Source 2 : labo_back
        # ============================================================

        keep['lab_merged'] = [
            unique_ordered(
                lab_lib,
                labo_back
            )
            for lab_lib, labo_back
            in zip(
                keep['lab_from_lib'],
                keep['labo_back']
            )
        ]

        # ============================================================
        # 6. RNSR
        #
        # Sources :
        #
        #   1. id_secondaire
        #   2. rnsr_back
        #   3. numero_national_de_structure
        #
        # On conserve cet ordre de priorité.
        # ============================================================

        # --------------------------------------------------
        # Nettoyage des id_secondaire
        #
        # Dans ton ancienne logique :
        # les valeurs contenant "0" étaient ignorées.
        # --------------------------------------------------

        keep.loc[
            keep['id_secondaire']
            .astype('string')
            .str.contains('0', na=True),
            'id_secondaire'
        ] = pd.NA

        # --------------------------------------------------
        # Passage en listes
        # --------------------------------------------------

        keep['id_secondaire'] = keep[
            'id_secondaire'
        ].apply(as_list)

        # --------------------------------------------------
        # Reconnaissance RNSR
        #
        # Format :
        #     9 chiffres + 1 lettre
        #
        # Exemple :
        #     123456789A
        # --------------------------------------------------

        RNSR_PATTERN = re.compile(
            r'^[0-9]{9}[A-Z]$',
            flags=re.IGNORECASE
        )

        keep['rnsr_from_id'] = keep[
            'id_secondaire'
        ].apply(
            lambda values: [
                value
                for value in values
                if RNSR_PATTERN.fullmatch(value)
            ]
        )

        # --------------------------------------------------
        # Fusion des trois sources
        #
        # Priorité :
        #
        # id_secondaire
        #       ↓
        # rnsr_back
        #       ↓
        # numero_national_de_structure
        # --------------------------------------------------

        keep['rnsr_merged'] = [
            unique_ordered(
                rnsr_id,
                rnsr_back,
                rnsr_structure
            )
            for rnsr_id,
                rnsr_back,
                rnsr_structure
            in zip(
                keep['rnsr_from_id'],
                keep['rnsr_back'],
                keep['numero_national_de_structure']
            )
        ]

        # ============================================================
        # 7. NETTOYAGE FINAL
        # ============================================================

        keep.drop(
            columns=['rnsr_from_id'],
            inplace=True
        )

        # Transforme les chaînes vides en NaN
        keep = keep.mask(
            keep == ''
        )

        print(f"- size keep final: {len(keep)}")

        return keep

    keep = merge_organismes_et_structure(structure_fr, oback)



    # ==============================================================================
    # AFFILIATION RNSR
    # ==============================================================================

    print("## identification des RNSR")

    keep = identify_rnsr(
        keep=keep,
        PATH_HARVEST=PATH_HARVEST,
        reload_matches=False
    )

    ##############################
    # etranger
    if foreign_script == True:
        struct_et = structure.loc[structure.country_code!='FRA']
        print(f"longueur struct etranger: {len(struct_et)}")
        df = (struct_et.loc[struct_et.entities_id.str.contains(r'^[^R0]', regex=True), 
                            ['entities_full_2', 'entities_id', 'country_code_source', 'country_code', 'city']]
            .drop_duplicates()
            .assign(match=None)
            )

        typ="ror"
        print(time.strftime("%H:%M:%S"))

        for i, row in df.iterrows():
            query="{} {} {}".format(row['city'], row['country_code'], row['entities_full_2'])

            strategies = [
                [['ror_name', 'ror_country']],
                [['ror_name', 'ror_acronym', 'ror_country', 'ror_city']],
                [['ror_name', 'ror_country', 'ror_city']]
            ]
            matcher(df, i, typ, query, strategies)
        print(time.strftime("%H:%M:%S"))
        
        df = df.loc[~df.match.isnull(), ['match']]
        df.to_pickle(f"{PATH_WORK}match_ror.pkl", compression='gzip')
        struct_et = pd.concat([struct_et, df], axis=1)
        struct_et.loc[struct_et.match.str.len()>1, 'resultat'] = 'a controler'
        struct_et.mask(struct_et=='', inplace=True)

        struct_et.to_pickle(f'{PATH_MATCH}struct_et.pkl')

        entities_all = pd.concat([keep,  struct_et], ignore_index=True, axis=0)
        print(f"size entities_all: {len(entities_all)}")

    else:
   
        entities_all = keep.copy()
        print(f"size entities_all: {len(entities_all)}")


    # ================================================================
    # FINALISATION DES ORGANISMES
    # ================================================================
    print("### FINALISATION DES ORGANISMES")

    entities_all = keep.copy()
    entities_all = entities_all.mask(entities_all == '')


    # ----------------------------------------------------------------
    # 1. Déterminer la méthode d'identification de l'organisme
    # ----------------------------------------------------------------
    #
    # Priorité :
    #   1. validated : numéro national de structure déjà présent
    #   2. orga      : RNSR provenant de organisme_back
    #   3. orcid     : RNSR provenant d'ORCID
    #   4. corda     : RNSR trouvé par affiliation-matcher
    #
    # Une méthode déjà définie n'est jamais écrasée.
    # ----------------------------------------------------------------

    entities_all.loc[
        entities_all['numero_national_de_structure'].notna(),
        'method'
    ] = 'validated'

    mask = entities_all['method'].isna()

    entities_all.loc[
        mask & entities_all['rnsr_back'].str.len().gt(0),
        'method'
    ] = 'orga'

    mask = entities_all['method'].isna()

    entities_all.loc[
        mask & entities_all['rnsr_from_orcid'].str.len().gt(0),
        'method'
    ] = 'orcid'

    mask = entities_all['method'].isna()

    entities_all.loc[
        mask & entities_all['rnsr_merged'].str.len().gt(0),
        'method'
    ] = 'corda'


    # ----------------------------------------------------------------
    # 2. Construire le numéro national de structure
    # ----------------------------------------------------------------
    #
    # Si un numéro officiel existe déjà :
    #     -> on le conserve
    #
    # Sinon, on utilise le/les RNSR identifiés.
    # ----------------------------------------------------------------

    mask = entities_all['numero_national_de_structure'].notna()

    entities_all.loc[
        mask,
        'num_nat_struct'
    ] = entities_all.loc[
        mask,
        'numero_national_de_structure'
    ]


    def join_rnsr(value):
        """Transforme une liste de RNSR en chaîne séparée par ';'."""
        if isinstance(value, (list, tuple, set)):
            values = [str(x) for x in value if pd.notna(x) and str(x).strip()]
            return ';'.join(dict.fromkeys(values)) if values else np.nan

        if pd.notna(value) and str(value).strip():
            return str(value)

        return np.nan


    mask = (
        entities_all['method'].ne('validated')
        & entities_all['rnsr_merged'].str.len().gt(0)
    )

    entities_all.loc[
        mask,
        'num_nat_struct'
    ] = entities_all.loc[
        mask,
        'rnsr_merged'
    ].apply(join_rnsr)


    # ================================================================
    # GEOLOCALISATION
    # ================================================================
    print("## geoloc cleaning")


    # ----------------------------------------------------------------
    # 3. Nettoyage de l'adresse
    # ----------------------------------------------------------------

    entities_all = stop_word(
        entities_all,
        'country_code',
        ['street']
    )


    # ----------------------------------------------------------------
    # 4. Code postal
    # ----------------------------------------------------------------

    # Pour la France, on extrait uniquement les chiffres du code postal.
    mask_fr = (
        entities_all['country_code'].eq('FRA')
        & entities_all['postalCode'].notna()
    )

    entities_all.loc[
        mask_fr,
        'code_postal'
    ] = (
        entities_all.loc[mask_fr, 'postalCode']
        .str.replace(r'\D*', '', regex=True)
        .str.strip()
    )


    # Un code postal français doit comporter 5 chiffres.
    mask = entities_all['code_postal'].notna()

    entities_all.loc[
        mask & entities_all['code_postal'].str.len().ne(5),
        'code_postal'
    ] = np.nan


    # Si aucun code postal nettoyé n'a été trouvé,
    # on conserve le postalCode original.
    mask = entities_all['code_postal'].isna()

    entities_all.loc[
        mask,
        'code_postal'
    ] = entities_all.loc[
        mask,
        'postalCode'
    ]


    # Département français = premier chiffre du code postal.
    mask_fr = (
        entities_all['country_code'].eq('FRA')
        & entities_all['code_postal'].notna()
    )

    entities_all.loc[
        mask_fr,
        'dep_code'
    ] = entities_all.loc[
        mask_fr,
        'code_postal'
    ].str[:1]


    # ----------------------------------------------------------------
    # 5. Normalisation de l'adresse
    # ----------------------------------------------------------------

    tmp = entities_all[['country_code', 'street_2']].copy()
    tmp = adr_tag(tmp, ['street_2'])
    tmp['street_2'] = (tmp['street_2']
        .apply(lambda x: ' '.join(x) if isinstance(x, list) else x)
        )

    # on retire les colonnes qu'on va recoller, pour éviter tout doublon
    entities_all = entities_all.drop(
        columns=['street_2', 'street_2_tag'],
        errors='ignore'
    )

    entities_all = pd.concat(
        [entities_all, tmp.drop(columns='country_code')],
        axis=1
    )


    # ----------------------------------------------------------------
    # 6. Normalisation des villes provenant de organisme_back
    # ----------------------------------------------------------------

    tmp = (
        entities_all[['city_back']]
        .explode('city_back')
    )

    tmp['city_back'] = (
        tmp['city_back']
        .str.replace(r'\bst\b', 'saint', regex=True)
        .str.replace(r'\bste\b', 'sainte', regex=True)
        .str.replace(r'\s+', '-', regex=True)
        .str.strip()
    )

    tmp = (
        tmp.groupby(level=0)['city_back']
        .agg(lambda x: ' '.join(x.dropna().unique()))
        .rename('city_back')
    )

    entities_all = (
        entities_all
        .drop(columns='city_back')
        .join(tmp)
    )


    # ----------------------------------------------------------------
    # 7. Normalisation de city
    # ----------------------------------------------------------------

    mask = (
        entities_all['country_code'].isin(['FRA', 'BEL', 'LUX'])
        & entities_all['city'].notna()
    )

    entities_all.loc[
        mask,
        'city'
    ] = (
        entities_all.loc[mask, 'city']
        .str.replace(r'\bst\b', 'saint', regex=True)
        .str.replace(r'\bste\b', 'sainte', regex=True)
        .str.strip()
    )


    # ----------------------------------------------------------------
    # 8. city_tag
    # ----------------------------------------------------------------

    mask = entities_all['city'].notna()

    entities_all.loc[
        mask,
        'city_tag'
    ] = (
        entities_all.loc[mask, 'city']
        .str.replace(r'\s+', '-', regex=True)
        .str.strip()
    )


    # ----------------------------------------------------------------
    # 8. country_code_dept
    # ----------------------------------------------------------------

    mask = entities_all['country_code_source_struct'].isna()

    entities_all.loc[
        mask,
        'country_code_source_struct'
    ] = (
        entities_all.loc[mask, 'country_code_source']
        .str.strip()
    )


    # ----------------------------------------------------------------
    # Contrôles
    # ----------------------------------------------------------------

    print(
        "Répartition des méthodes :\n",
        entities_all['method'].value_counts(dropna=False)
    )

    print(
        f"RNSR identifiés : "
        f"{entities_all['num_nat_struct'].notna().sum()} / "
        f"{len(entities_all)}"
    )

    entities_all.to_pickle(f'{PATH_MATCH}entities_all.pkl')