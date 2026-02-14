import asyncio
import json
from dotenv import load_dotenv
from lightrag import QueryParam

from config_logging import configure_logging
from init_rag import initialize_rag

load_dotenv(dotenv_path=".env", override=False)


DEBUG_QUERIES = [
    "Điều kiện khấu trừ thuế TNCN là gì?",
    "Doanh nghiệp được miễn thuế TNDN trong trường hợp nào?",
    "Các khoản thu nhập chịu thuế TNCN theo luật hiện hành"
]

DEBUG_MODE = "hybrid"

async def main():
    configure_logging()
    rag = None
    try:
        rag = await initialize_rag()

        for query in DEBUG_QUERIES:
            print(f"DEBUG QUERY: {query}")

            data = await rag.aquery_data(
                query,
                param=QueryParam(mode=DEBUG_MODE)
            )

            print(json.dumps(data, ensure_ascii=False, indent=2))

    finally:
        if rag:
            await rag.finalize_storages()

if __name__ == "__main__":
    asyncio.run(main())