
# # # GRIST database
# from remote_process.grist import grist_fetch_docs, load_grist_tables

# geoG = {}
# categoriesG = {}
# communesG = {}

# _initialized = False

# def init_referentiels():
#     global geoG, categoriesG, communesG, _initialized

#     if _initialized:
#         return

#     docs_dict = grist_fetch_docs('dataesr', ['pcri', 'nomenclatures'])

#     # Chaque doc correspond à un dict référentiel
#     # if 'pcri' in docs_dict:
#     #     print(docs_dict['pcri'])
#         # data = load_grist_tables(docs_dict['pcri'])
#     geoG.update(load_grist_tables(docs_dict['geo']))
#     categoriesG.update(load_grist_tables(docs_dict['categories']))
#     communesG.update(load_grist_tables(docs_dict['les_communes']))

#     _initialized = True