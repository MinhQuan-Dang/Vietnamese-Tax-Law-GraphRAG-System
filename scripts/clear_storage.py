import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env", override=False)

WORKING_DIR = "./rag_storage"

FILES_TO_DELETE = [
    "graph_chunk_entity_relation.graphml",
    "kv_store_doc_status.json",
    "kv_store_full_docs.json",
    "kv_store_text_chunks.json",
    "vdb_chunks.json",
    "vdb_entities.json",
    "vdb_relationships.json",
    "kv_store_entity_chunks.json",
    "kv_store_full_entities.json",
    "kv_store_full_relations.json",
    "kv_store_llm_response_cache.json",
    "kv_store_relation_chunks.json"
    # nếu muốn giữ cache LLM :contentReference[oaicite:21]{index=21}"kv_store_llm_response_cache.json",
]

def main():
    if not os.path.exists(WORKING_DIR):
        print("No storage dir to clear.")
        return

    for fn in FILES_TO_DELETE:
        p = os.path.join(WORKING_DIR, fn)
        if os.path.exists(p):
            os.remove(p)
            print(f"Deleted: {p}")

if __name__ == "__main__":
    main()