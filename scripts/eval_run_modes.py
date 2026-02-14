import os, json, time, asyncio
from lightrag import QueryParam

# Import initializer của project bạn
from init_rag import initialize_rag

DATASET = os.path.join("scripts", "datasets", "tax_questions_120.jsonl")
OUT_DIR = os.path.join("scripts", "runs")
MODES = ["naive", "local", "global", "hybrid"]

async def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(OUT_DIR, f"run_{ts}.jsonl")

    rag = await initialize_rag()
    try:
        with open(DATASET, "r", encoding="utf-8") as f_in, open(out_path, "w", encoding="utf-8") as f_out:
            for line in f_in:
                item = json.loads(line)
                qid, qtext = item["id"], item["q"]

                for mode in MODES:
                    t0 = time.perf_counter()

                    # Lấy cả answer + retrieval data để sau judge chấm groundedness tốt hơn
                    data = await rag.aquery_data(qtext, param=QueryParam(mode=mode))
                    # data thường chứa answer trong cấu trúc; nếu không có thì fallback sang aquery
                    answer = data.get("response") or data.get("answer")
                    if not answer:
                        answer = await rag.aquery(qtext, param=QueryParam(mode=mode))

                    dt = time.perf_counter() - t0

                    rec = {
                        **item,
                        "mode": mode,
                        "latency_s": dt,
                        "answer": answer,
                        "retrieval": data,   # giữ nguyên để judge dùng
                    }
                    f_out.write(json.dumps(rec, ensure_ascii=False) + "\n")

                    print(f"{qid} | {mode} | {dt:.2f}s")
        print("Wrote:", out_path)
    finally:
        await rag.finalize_storages()

if __name__ == "__main__":
    asyncio.run(main())
