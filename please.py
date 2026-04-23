import streamlit as st
from pymongo import MongoClient
from openai import OpenAI
import json
import os
import re
from dotenv import load_dotenv
from utils_syncOPENAI import ai_deep_search, sync_atlas_to_local
from utils_voice import handle_voice_input

# --- INITIALIZATION ---
load_dotenv("security.env")

# Initialize OpenAI Client
client_ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

@st.cache_resource
def init_connection():
    return MongoClient(os.getenv("MONGODB_URI"))

client = init_connection()
db = client["CIS_360_Project"]
datasets_col = db["Datasets"]
papers_col = db["Papers"]
methods_col = db["FusionMethods"]

st.set_page_config(
    page_title="ResearchLens AI",
    page_icon="🔬",
    layout="wide"
)

if "messages" not in st.session_state:
    st.session_state.messages = []
if "last_query_log" not in st.session_state:
    st.session_state.last_query_log = None

# --- UTILS ---
def clean_json_string(text):
    text = re.sub(r"```json|```", "", text)
    return text.strip()

SCHEMA_INFO = """
Collections:
- Datasets: {data_name, data_type (array), paper_doi, uncertainty, format}
- Papers: {_id (DOI), title, authors (array), abstract, keywords (array), field_of_study (array), is_data_fusion (bool)}
- FusionMethods: {method_name, paper_doi, description, uncertainty, dataset_ids}
"""

def generate_mongodb_query(user_question: str):
    prompt = f"""
    You are a MongoDB expert. Generate a JSON search plan for: "{user_question}"
    Schema: {SCHEMA_INFO}

    CRITICAL RULES:
    1. Return ONLY a JSON object with a "queries" key.
    2. Use ONLY simple query objects for .find(). 
    3. To find 'data fusion', search: {{"$or": [{{"is_data_fusion": true}}, {{"title": {{"$regex": "fusion", "$options": "i"}}}}]}}
    """
    try:
        response = client_ai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": "You output only valid JSON."},
                      {"role": "user", "content": prompt}],
            response_format={ "type": "json_object" }
        )
        query_data = json.loads(response.choices[0].message.content)
        st.session_state.last_query_log = query_data
        return query_data
    except Exception as e:
        st.sidebar.error(f"Brain Error: {e}")
        return {"queries": []}

def execute_queries(query_plan):
    results = {"datasets": [], "papers": [], "methods": []}
    mapping = {"Datasets": datasets_col, "Papers": papers_col, "FusionMethods": methods_col}
    
    for q_obj in query_plan.get("queries", []):
        col_name = q_obj.get("collection")
        raw_query = q_obj.get("query", {})
        
        if col_name in mapping:
            try:
                cursor = mapping[col_name].find(raw_query).limit(10)
                for doc in cursor:
                    if "_id" in doc and col_name != "Papers":
                        doc.pop("_id")
                    if col_name == "Datasets": results["datasets"].append(doc)
                    elif col_name == "Papers": results["papers"].append(doc)
                    elif col_name == "FusionMethods": results["methods"].append(doc)
            except Exception as e:
                st.sidebar.warning(f"DB Error in {col_name}: {e}")
    return results

def get_conversational_summary(user_question, results):
    if not any(results.values()):
        return "NO_RESULTS_FOUND"
    
    prompt = f"User: {user_question}\nResults: {json.dumps(results, default=str)}\nSummarize these findings in 2 sentences."
    try:
        response = client_ai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except:
        return "I found the following records in the database:"

# --- UI SIDEBAR ---
with st.sidebar:
    st.title("🔬 ResearchLens")
    
    # VOICE INPUT (Moved to sidebar to keep bottom clear)
    st.subheader("🎙️ Voice Search")
    voice_query = handle_voice_input()
    if voice_query:
        st.info(f"Detected: {voice_query}")

    st.divider()
    st.subheader("📡 Connection Health")
    try:
        client.admin.command('ping')
        st.success("Connected to Atlas Cloud")
        st.metric("Papers", papers_col.count_documents({}))
        st.metric("Datasets", datasets_col.count_documents({}))
    except Exception as e:
        st.error("Connection Failed")

    st.write("---")
    if st.button("🔄 Sync Local Backup"):
        with st.spinner("Syncing..."):
            status = sync_atlas_to_local()
            st.info(status)

    if st.session_state.last_query_log:
        with st.expander("🛠️ Query Debugger"):
            st.json(st.session_state.last_query_log)
            
    if st.button("Clear History"):
        st.session_state.messages = []
        st.rerun()

# --- INPUT HANDLING ---
# We check if voice was used, otherwise we show the single chat input
final_prompt = None

if voice_query:
    final_prompt = voice_query
else:
    # This is the ONLY chat_input in the file now
    typed_query = st.chat_input("Search papers, datasets, or fusion methods...", key="main_chat_input")
    if typed_query:
        final_prompt = typed_query

# --- EXECUTION LOGIC ---
if final_prompt:
    st.session_state.messages.append({"role": "user", "content": final_prompt})
    
    with st.spinner("Analyzing..."):
        query_plan = generate_mongodb_query(final_prompt)
        db_results = execute_queries(query_plan)
        summary = get_conversational_summary(final_prompt, db_results)
        
        st.session_state.messages.append({
            "role": "assistant", 
            "content": summary, 
            "data": db_results,
            "query_text": final_prompt
        })
    st.rerun()

# --- RENDER CHAT ---
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        if msg["content"] == "NO_RESULTS_FOUND":
            st.warning("🔍 No direct matches found in the database index.")
            st.write("Would you like to perform an **AI Deep Search**?")
            # We use a hash of the query text to keep the button key unique
            btn_key = f"deep_{hash(msg['query_text'])}"
            if st.button("🚀 Run Deep Search", key=btn_key):
                with   st.spinner("OpenAI is reading the local archives..."):
                    deep_result = ai_deep_search(msg["query_text"])
                    st.markdown("### 🔬 Deep Search Analysis")
                    st.write(deep_result)
        else:
            st.write(msg["content"])
            
        if "data" in msg and any(msg["data"].values()):
            t1, t2, t3 = st.tabs(["📄 Papers", "📊 Datasets", "🧪 Methods"])
            with t1:
                if msg["data"]["papers"]:
                    for p in msg["data"]["papers"]:
                        st.markdown(f"**{p.get('title')}**")
                        st.caption(f"DOI: {p.get('_id')}")
                else: st.info("None found.")
            with t2:
                if msg["data"]["datasets"]: st.dataframe(msg["data"]["datasets"])
                else: st.info("None found.")
            with t3:
                if msg["data"]["methods"]: st.write(msg["data"]["methods"])
                else: st.info("None found.")