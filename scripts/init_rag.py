import os
import asyncio
import numpy as np
from dotenv import load_dotenv

from openai import AsyncOpenAI
from sentence_transformers import SentenceTransformer

from lightrag import LightRAG
from lightrag.utils import EmbeddingFunc

from config_logging import configure_logging

WORKING_DIR = "./rag_storage"

load_dotenv(".env", override=False)

# Helpers
def normalize_base_url(raw: str | None) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return "https://" + raw


def _merge_messages(system_prompt, prompt, history_messages):
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})

    if history_messages:
        for m in history_messages:
            role = m.get("role", "user")
            content = m.get("content", "")
            if role not in ("system", "user", "assistant"):
                role = "user"
            messages.append({"role": role, "content": content})

    messages.append({"role": "user", "content": prompt})
    return messages


# OpenAI LLM (gpt-4.1-mini)
openai_client = AsyncOpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    # base_url=normalize_base_url(os.getenv("BASE_URL")),
    timeout=float(os.getenv("OPENAI_TIMEOUT", "60")),
)

LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4.1-mini")


async def llm_model_func(
    prompt,
    system_prompt=None,
    history_messages=[],
    keyword_extraction=False,
    **kwargs,
) -> str:
    messages = _merge_messages(system_prompt, prompt, history_messages)

    resp = await openai_client.chat.completions.create(
        model=LLM_MODEL,
        messages=messages,
    )
    return (resp.choices[0].message.content or "").strip()


# Local Embeddings (bge-m3)
# LOCAL_EMBED_MODEL = os.getenv("LOCAL_EMBED_MODEL", "BAAI/bge-m3")

# # Local Embeddings (intfloat/multilingual-e5-base)
LOCAL_EMBED_MODEL = os.getenv("LOCAL_EMBED_MODEL", "intfloat/multilingual-e5-base")
_embed_model = SentenceTransformer(LOCAL_EMBED_MODEL)

EMBEDDING_MAX_TOKEN_SIZE = int(os.getenv("EMBEDDING_MAX_TOKEN_SIZE", "8192"))

def _e5_prefix(text: str) -> str:
    t = (text or "").strip()
    if t.startswith("query:") or t.startswith("passage:"):
        return t

    # Heuristic: query thường ngắn, 1 dòng; passage thường dài/nhiều dòng
    is_query = (len(t) <= 256) and ("\n" not in t)
    return f"query: {t}" if is_query else f"passage: {t}"


async def embedding_func(texts: list[str]) -> np.ndarray:
    loop = asyncio.get_running_loop()

    def _encode():
        # E5 prefixes
        prefixed = [_e5_prefix(x) for x in texts]

        # Ép sentence-transformers trả numpy
        arr = _embed_model.encode(
            prefixed,
            normalize_embeddings=True,
            show_progress_bar=False,
            convert_to_numpy=True,   # <<< QUAN TRỌNG
        )
        return arr

    arr = await loop.run_in_executor(None, _encode)

    # Ép kiểu + shape chắc chắn
    arr = np.asarray(arr, dtype=np.float32)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    return arr

async def create_embedding_function_instance() -> EmbeddingFunc:
    test_vec = (await embedding_func(["test"]))[0]
    return EmbeddingFunc(
        embedding_dim=len(test_vec),
        max_token_size=EMBEDDING_MAX_TOKEN_SIZE,
        func=embedding_func,
    )


async def initialize_rag() -> LightRAG:
    if not os.path.exists(WORKING_DIR):
        os.mkdir(WORKING_DIR)

    emb = await create_embedding_function_instance()

    rag = LightRAG(
        working_dir=WORKING_DIR,
        llm_model_func=llm_model_func,
        embedding_func=emb,
    )

    await rag.initialize_storages()
    return rag


if __name__ == "__main__":
    configure_logging()
    asyncio.run(initialize_rag())
    # print("Initialized LightRAG (OpenAI LLM + Local bge-m3 embeddings).")
    print("Initialized LightRAG (OpenAI LLM + Local intfloat/multilingual-e5-base embeddings).")