from openai import OpenAI
import os
import json

def sync_atlas_to_local():
    """Pulls everything from Atlas and saves it to a local JSON for backup/deep search."""
    try:
        client = MongoClient(os.getenv("MONGODB_URI"))
        db = client["CIS_360_Project"]
        
        data_to_backup = {
            "papers": list(db["Papers"].find({})),
            "datasets": list(db["Datasets"].find({})),
            "methods": list(db["FusionMethods"].find({}))
        }
        
        # Convert MongoDB ObjectIds to strings so they can be saved to JSON
        for category in data_to_backup:
            for item in data_to_backup[category]:
                if "_id" in item:
                    item["_id"] = str(item["_id"])

        with open("local_backup.json", "w") as f:
            json.dump(data_to_backup, f, indent=4)
            
        return "✅ Sync Complete! local_backup.json updated."
    except Exception as e:
        return f"❌ Sync Failed: {e}"

def ai_deep_search(user_query):
    """Uses OpenAI to read the local JSON backup for a deep conceptual search."""
    if not os.path.exists("local_backup.json"):
        return "No local backup found. Please sync first."

    with open("local_backup.json", "r") as f:
        raw_data = f.read()

    client_ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    prompt = f"""
    You are performing a DEEP SEARCH on a research database. 
    The user is looking for: "{user_query}"
    
    Here is the raw data from the database:
    {raw_data[:25000]} 

    Instructions:
    1. Identify papers, datasets, or methods that are conceptually related to the query.
    2. Explain why they are relevant even if they didn't show up in a standard keyword search.
    3. If nothing is even remotely related, let the user know.
    """
    
    try:
        response = client_ai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"Deep Search Error: {e}"

def modify_database(action_type, collection_name, data):
    """Handles Insert, Update, and Delete actions."""
    mapping = {
        "Datasets": datasets_col,
        "Papers": papers_col,
        "FusionMethods": methods_col
    }
    col = mapping.get(collection_name)
    if not col: return False
    
    try:
        if action_type == "INSERT":
            col.insert_one(data)
        elif action_type == "DELETE":
            # Data would be the filter, e.g., {"_id": "DOI-123"}
            col.delete_one(data)
        elif action_type == "UPDATE":
            # Data would be [filter, new_values]
            col.update_one(data[0], {"$set": data[1]})
        return True
    except Exception as e:
        st.error(f"DB Action Error: {e}")
        return False