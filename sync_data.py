import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import json_util # Handles MongoDB specific types like ObjectIDs

load_dotenv("security.env")

def run_sync():
    print("🚀 Connecting to Atlas...")
    client = MongoClient(os.getenv("MONGODB_URI"))
    db = client["CIS_360_Project"]
    
    # 1. Pull everything
    data = {
        "Papers": list(db["Papers"].find({})),
        "Datasets": list(db["Datasets"].find({})),
        "FusionMethods": list(db["FusionMethods"].find({}))
    }
    
    # 2. Convert MongoDB data to a standard JSON format
    # json_util.dumps handles the "ObjectId" errors that normally break JSON
    clean_data = json.loads(json_util.dumps(data))
    
    # 3. Write to the file
    with open("local_backup.json", "w") as f:
        json.dump(clean_data, f, indent=4)
    
    print(f"✅ Success! Saved {len(data['Papers'])} papers to local_backup.json")

if __name__ == "__main__":
    run_sync()