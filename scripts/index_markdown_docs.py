import os
import asyncio
from dotenv import load_dotenv

from config_logging import configure_logging
from init_rag import initialize_rag


load_dotenv(dotenv_path=".env", override=False)

MARKDOWN_DIR = "./markdown_docs"

def read_all_markdown_files():
    docs = []
    file_paths = []
    for name in sorted(os.listdir(MARKDOWN_DIR)):
        if name.lower().endswith(".md"):
            path = os.path.join(MARKDOWN_DIR, name)
            with open(path, "r", encoding="utf-8") as f:
                docs.append(f.read())
            file_paths.append(path)
    return docs, file_paths

async def main():
    configure_logging()
    rag = None
    try:
        rag = await initialize_rag()

        docs, file_paths = read_all_markdown_files()
        if not docs:
            raise RuntimeError(f"No markdown files found in {MARKDOWN_DIR}")

        # LightRAG core có hỗ trợ file_paths để citation tracking (xem mô tả ainsert/enqueue trong code)
        # -> ưu tiên truyền file_paths để khi query có reference_id/file_path rõ ràng
        await rag.ainsert(docs, file_paths=file_paths)

        print(f"Indexed {len(docs)} markdown documents.")
    finally:
        if rag:
            await rag.finalize_storages()

if __name__ == "__main__":
    asyncio.run(main())