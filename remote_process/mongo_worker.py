# mongo_worker.py
import pickle
import sys
from pathlib import Path

# Ajoute le dossier parent de remote_process au path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from remote_process.mongo_runner import refresh_mongo_collection

PAYLOAD_PATH = Path(__file__).resolve().parent / "mongo_insert.pkl"

with open(PAYLOAD_PATH, "rb") as f:
    args = pickle.load(f)

print(f"Démarrage — FP={args['FP']}, collection=european-projects_{args['tab_mongo']}")
refresh_mongo_collection(args["FP"], args["df"], args["cols_select_xls"], args["tab_mongo"])
print("Terminé ✅")