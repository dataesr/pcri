from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import os, pandas as pd

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            df[col] = df[col].astype(object).where(df[col].notna(), None)
            df[col] = df[col].apply(lambda x: int(x) if x is not None else None)

        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].apply(lambda x: float(x) if pd.notna(x) else None)

        elif pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].where(pd.notna(df[col]), None)

        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)

    return df

# --- Suppression ---
def mongo_delete_all(collection_name: str, database_name: str = "tableaux-staging") -> None:

    try:
        client = MongoClient(os.environ.get("mongoURI"))
        client.admin.command("ping")
        print("Successfully connected")

        db = client[database_name]

        if collection_name not in db.list_collection_names():
            print(f"⚠️ Collection '{collection_name}' introuvable dans '{database_name}' — suppression ignorée")
            return

        result = db[collection_name].delete_many({})
        print(f"✅ Deleted: {result.deleted_count} documents from '{collection_name}'")

    except Exception as e:
        print("Connection failed:", e)

    finally:
        client.close()

# --- Insertion ---
def mongo_bulk_insert_df(df: pd.DataFrame, collection_name: str, database_name: str = "tableaux-staging", batch_size: int = 10_000) -> None:

    try:
        client = MongoClient(os.environ.get("mongoURI"))
        client.admin.command("ping")
        print("Successfully connected")

        collection = client[database_name][collection_name]
        df = clean_df(df)

        total = len(df)
        inserted = 0

        for i in range(0, total, batch_size):
            batch = df.iloc[i : i + batch_size].to_dict(orient="records")

            try:
                result = collection.insert_many(batch, ordered=False)
                inserted += len(result.inserted_ids)
                print(f"Progress: {inserted}/{total}")

            except BulkWriteError as e:
                inserted += e.details["nInserted"]
                print(f"Batch error: {e.details}")

    except Exception as e:
        print("Connection failed:", e)

    client.close()




# from pymongo import MongoClient
# from pymongo.errors import BulkWriteError
# import os, pandas as pd

# def clean_df(df: pd.DataFrame) -> pd.DataFrame:
#     # Copie pour ne pas modifier l'original
#     df = df.copy()

#     for col in df.columns:
#         # numpy int64 → int natif Python
#         if pd.api.types.is_integer_dtype(df[col]):
#             df[col] = df[col].astype(object).where(df[col].notna(), None)
#             df[col] = df[col].apply(lambda x: int(x) if x is not None else None)

#         # numpy float64 → float natif Python
#         elif pd.api.types.is_float_dtype(df[col]):
#             df[col] = df[col].apply(lambda x: float(x) if pd.notna(x) else None)

#         # NaN dans colonnes object/string → None
#         elif pd.api.types.is_object_dtype(df[col]):
#             df[col] = df[col].where(pd.notna(df[col]), None)

#         # datetime → natif Python (MongoDB l'accepte directement)
#         elif pd.api.types.is_datetime64_any_dtype(df[col]):
#             df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)

#     return df

# # --- Insertion ---
# def mongo_bulk_insert_df(df: pd.DataFrame, batch_size: int = 10_000) -> None:

#     try:
#         client = MongoClient(os.environ.get("mongoURI"))
#         client.admin.command("ping")
#         print("Successfully connected")
        
#         database = client["tableaux-staging"]
#         collection = database["european-projects_projects-entities"]

#         df = clean_df(df)

#         total = len(df)
#         inserted = 0

#         for i in range(0, total, batch_size):
#             batch = df.iloc[i : i + batch_size].to_dict(orient="records")

#             try:
#                 result = collection.insert_many(batch, ordered=False)
#                 inserted += len(result.inserted_ids)
#                 print(f"Progress: {inserted}/{total}")

#             except BulkWriteError as e:
#                 inserted += e.details["nInserted"]
#                 print(f"Batch error: {e.details}")

#     except Exception as e:
#         print("Connection failed:", e)

#     client.close()