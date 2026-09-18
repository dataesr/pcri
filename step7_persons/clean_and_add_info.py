from functions_shared import country_iso_shift, prop_string
import pandas as pd, numpy as np

__all__ = ["clean_perso"]


def fix_string(s):
    """
    remove encoding prob like Pawe\005C0142

    """
    import re

    if not isinstance(s, str):
        return s

    prev = None
    while prev != s:
        prev = s
        s = s.replace("\\005C", "\\")
        s = re.sub(
            r"\\([0-9A-Fa-f]{4})", lambda m: chr(int(m.group(1), 16)), s
        )

    return s


def clean_part(df):
    """COUNTRY shift iso2 to iso3"""
    for el in [
        "birth_country_code",
        "nationality_country_code",
        "host_country_code",
        "sending_country_code",
    ]:
        df = country_iso_shift(df, el, iso2_to3=True)

    cols = ["first_name", "last_name"]
    for c in cols:
        df[c] = df[c].apply(fix_string)

    # role
    df.loc[df["role"].str.lower() == "principal investigator", "role"] = "PI"

    return df


def contact_name(df):

    for f in ["first_name", "last_name"]:

        df[f] = df[f].fillna("")
        df[f] = df[f].str.strip().str.replace(r"\s+", " ", regex=True)
        df[f] = df[f].str.strip().str.replace(r"-{2,}", "-", regex=True)
        df.loc[df[f].str.len() < 2, f] = ""

    df["contact"] = (
        df.first_name.astype(str).str.lower()
        + " "
        + df.last_name.astype(str).str.lower()
    )

    # -------------------------
    # words list to delete

    with open("data_files/persons_name_remove.txt",
              "r", encoding="utf-8") as f:
        pnr = f.read().splitlines()

    pattern = rf"\b({'|'.join(pnr)})s?\b"

    mask = (df["contact"]
            .astype(str)
            .str.strip()
            .str.contains(pattern, case=False, na=False)
            )

    # Remplace par une chaîne vide là où le motif est trouvé
    df.loc[mask, "contact"] = np.nan

    # Pour détecter les vides OU les cases contenant UNIQUEMENT des espaces
    mask = (df["contact"] == "") | (
            df["contact"].str.contains(r"^\s+$", na=False)
            )
    df.loc[mask, 'contact'] = np.nan

    return df


def check_role_type(df):
    """
    check if naw role in dataset
    if true add new role in list keep_order
    """
    keep_order = ["pi", "fellow", "main_contact"]
    if len(df.role.unique()) > len(keep_order):
        print(
            f"🚨 new role in df -> {set(df.role.unique())-set(keep_order)}\n",
            "- add it in keep_order list",
        )


def clean_mail(df):
    mail_del = [
        "gmail",
        "yahoo",
        "hotmail",
        "wanadoo",
        "aol",
        "free",
        "skynet",
        "outlook",
        "icloud",
        "googlemail",
    ]

    df["domaine"] = df.email.str.split("@").str[1]
    tmp = df.loc[df.domaine.notna(), ["domaine"]]

    for el in mail_del:
        m = r"^" + el

        mask = (tmp["domaine"]
                .str.contains(m, case=True, flags=0, na=None, regex=True)
                )
        tmp.loc[mask, "domain_email"] = ""

        tmp.loc[tmp["domain_email"].isnull(), "domain_email"] = tmp["domaine"]

    df = pd.concat([df, tmp], axis=1).drop(columns="domaine")

    df = df.mask(df == "")

    return df


def success_merge(mask, df, s):

    df = df.merge(
        s,
        how="left",
        left_on=["project_id", "role", "host_country_code_3"],
        right_on=["project_id", "role", "country_code"],
        suffixes=("", "_x"),
    )

    df.loc[mask, "generalPic"] = df.loc[mask, "generalPic_x"]

    return df.drop(columns=["generalPic_x", "country_code"])


def clean_pic(df, participation, stage):
    """
    fill missing pic
    """
    print(f"- size {stage} before filling generalPic: {len(df)}")

    mask = df.generalPic.isnull()

    if any(mask):
        print(f"🚨 size rows with generelPic null for {stage}: {len(df[mask])}")

        p = list(df.loc[mask, "project_id"].unique())
        sub = participation.loc[
            participation["project_id"].isin(p),
            ["stage", "project_id", "role", "generalPic", "country_code"],
        ]

        sub["role"] = sub["role"].str.lower()

        if stage == "successful":

            # merge with successful par and get missing generalPic

            s = sub[sub["stage"] == stage].drop(columns="stage")

            df = success_merge(mask, df, s)

            print(f"- size {stage} after filling generalPic: {len(df)}")

            if any(df.generalPic.isnull()):

                # merge with evaluated part and get missing generalPic
                print(
                    f"🚨 size rows with generelPic null for {stage}:",
                    f"{len(df[df.generalPic.isnull()])}\n",
                    "- test with evaluated generalPic",
                )

                s = sub[sub["stage"] == "evaluated"].drop(columns="stage")

                df = success_merge(df.generalPic.isnull(), df, s)

                if any(df.generalPic.isnull()):
                    print(
                        f"🚨 size rows with generelPic null for {stage}:",
                        f"{len(df[df.generalPic.isnull()])}\n",
                        "- RELOU !!!",
                    )
                else:
                    print("- missing pic resolved")

            return df

        else:
            print("🚨 missing generalPic in evaluated ; write script for")
            return df

    else:
        print("- No missing generalPic")
        return df


def merge_participation(df, participation, stage):
    """
    link with participation on project_id + generalPic
    remove rows only in participation
    rows only in persons without country_code -> part_merge = 'left_only'
    """

    df = (
        df.merge(
            participation.loc[
                participation.stage == stage,
                [
                    "project_id",
                    "generalPic",
                    "country_code",
                ],
            ],
            how="outer",
            on=["project_id", "generalPic"],
            indicator=True,
        ).query('_merge != "right_only"')
    )

    print(f"- size df after merge with participation: {len(df)}")

    # keep var _merge as part_merge if left_only maysbe relate to protability 
    df = df.rename(columns={'_merge': 'part_merge'})

    return df


def merge_entities(df, entities):
    ent = (
        entities[
            [
                'entities_id',
                'entities_name',
                'operateur_num',
                'generalPic',
                'country_code',
            ]
        ].drop_duplicates()
    )

    # merge with ent and country_code not null
    tmp = (df[df.country_code.notna()]
           .merge(ent,
                  how='left',
                  on=['generalPic', 'country_code']
                  )
           )

    # merge with previous affiliation ; portability erc
    mask = df.country_code.isnull()
    if any(mask):
        tmp1 = (df[mask]
                .drop(columns='country_code')
                .merge(ent,
                       how='left',
                       left_on=['generalPic', 'sending_country_code_3'],
                       right_on=['generalPic', 'country_code'],
                       indicator=True
                       )
                )

        mask = tmp1['_merge'] == 'left_only'
        if any(tmp1[mask]):
            print(f"🚨 still missing merge {len(tmp1[mask])}")

            # tmp2 = (tmp1[tmp1.country_code.isna()]
            #         .drop(columns='country_code')
            #         .merge(ent,
            #             how='left',
            #             on='generalPic'
            #             )


            #         )



    df = pd.concat([tmp, tmp1], ignore_index=True).drop(columns='_merge')

    return df



def clean_perso(df, participation, entities, stage):

    if stage == "successful":

        # ----------------------------------
        # country_code iso2 to iso3 ; replace Unicode escape sequence
        df = clean_part(df)

        # -----------------------------------
        # clean string
        cols = ["role", "first_name", "last_name", "gender"]
        df = prop_string(df, cols)

        # ---------------------------------
        # create column contact -> last_name + first_name
        df = contact_name(df)

        # ---------------------------
        # check role category
        check_role_type(df)

        # -------------------------------
        # create domaine_email only with domain and extension
        df = clean_mail(df)

        # --------------------------------
        # fill missing pic ; merge with part and entities
        df = clean_pic(df, participation, "successful")
        df = merge_participation(df, participation, 'successful')
        df = merge_entities(df, entities)

    else:

        # --------------------------------
        # same functions
        cols = ["role", "first_name", "last_name", "gender"]
        df = prop_string(df, cols)
        df = contact_name(df)
        check_role_type(df)
        df = clean_pic(df, participation, "evaluated")
        df = merge_participation(df, participation, 'evaluated')
        df = merge_entities(df, entities)
        df = clean_mail(df)

    print(f"- size {stage}: {len(df)}")
    return df
