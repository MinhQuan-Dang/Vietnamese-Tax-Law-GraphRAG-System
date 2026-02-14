import os, json, re, time
from openai import OpenAI

RUN_FILE = os.environ.get("RUN_FILE", "")  # set env RUN_FILE=... hoặc để trống sẽ pick file mới nhất
RUN_DIR = os.path.join("scripts", "runs")
OUT_DIR = os.path.join("scripts", "scored")
os.makedirs(OUT_DIR, exist_ok=True)


JUDGE_MODEL = os.getenv("JUDGE_MODEL", "gpt-4.1-mini")

client = OpenAI()

RUBRIC = """Bạn là giám khảo chuyên đánh giá chất lượng câu trả lời về thuế Thu nhập cá nhân và Thu nhập doanh nghiệp Việt Nam.

Bạn được cung cấp:
- Câu hỏi của người dùng
- Câu trả lời do hệ thống sinh ra
- Dữ liệu truy hồi (retrieved contexts): các đoạn văn, chunk, entity, relation được lấy về

Hãy đánh giá từng tiêu chí theo thang điểm từ 1.0 đến 10.0 (có thể dùng số thập phân như 7.5, 8.0, 9.2, ...). Hãy chấm điểm một cách công bằng.

Định nghĩa thang điểm cho từng tiêu chí:

correctness (tính đúng đắn bản chất):
- 1.0  = Hoàn toàn sai, trả lời ngược với quy định pháp luật hiện hành
- 10.0 = Hoàn toàn chính xác về mặt pháp lý, đúng trọng tâm câu hỏi, không sai sót nào đáng kể

completeness (tính đầy đủ):
- 1.0  = Thiếu hầu hết các ý chính, điều kiện, ngoại lệ quan trọng → trả lời rất nghèo nàn
- 10.0 = Bao quát đầy đủ và cân đối: các quy định chính, điều kiện áp dụng, trường hợp ngoại lệ, giới hạn thời gian (nếu có), cách tính (nếu liên quan), ví dụ minh họa cần thiết

groundedness (mức độ bám sát dữ liệu truy hồi):
- 1.0  = Hầu như không dựa vào retrieval, phần lớn nội dung là kiến thức chung hoặc tự suy diễn
- 10.0 = Mọi thông tin quan trọng trong câu trả lời đều được hỗ trợ trực tiếp rõ ràng bởi các đoạn retrieval

hallucination (mức độ bịa đặt / khẳng định vô căn cứ):
- 1.0 = Không hề có hallucination: mọi khẳng định đều có căn cứ trong retrieval hoặc là kiến thức nền tảng cơ bản không thể tranh cãi (ví dụ: "Thuế TNCN là thuế trực thu")
- 10.0  = Có nhiều thông tin bịa đặt, tự suy ra quy định không tồn tại, hoặc khẳng định điều retrieval không hề đề cập

Trả về JSON thuần túy, KHÔNG được viết thêm bất kỳ chữ nào bên ngoài JSON:

"""

def _load_latest_run():
    files = [f for f in os.listdir(RUN_DIR) if f.startswith("run_") and f.endswith(".jsonl")]
    if not files:
        raise RuntimeError("No run_*.jsonl found in evaluation_tax/runs. Run 01_run_modes.py first.")
    files.sort()
    return os.path.join(RUN_DIR, files[-1])

def _safe_json(txt: str):
    # cố gắng lấy JSON object đầu tiên
    m = re.search(r"\{.*\}", txt, flags=re.S)
    if not m:
        raise ValueError("No JSON object found in judge output")
    return json.loads(m.group(0))

def judge_one(question, answer, retrieval):
    user_payload = {
        "question": question,
        "answer": answer,
        "retrieval": retrieval,
    }
    resp = client.chat.completions.create(
        model=JUDGE_MODEL,
        messages=[
            {"role": "system", "content": RUBRIC},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
    )
    txt = (resp.choices[0].message.content or "").strip()
    return _safe_json(txt)

def main():
    run_path = RUN_FILE.strip() or _load_latest_run()
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(OUT_DIR, f"scored_{ts}.jsonl")

    with open(run_path, "r", encoding="utf-8") as f_in, open(out_path, "w", encoding="utf-8") as f_out:
        for i, line in enumerate(f_in, 1):
            rec = json.loads(line)
            scores = judge_one(rec["q"], rec["answer"], rec.get("retrieval", {}))
            rec.update(scores)
            f_out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            if i % 20 == 0:
                print(f"Judged {i} rows...")

    print("Run:", run_path)
    print("Wrote:", out_path)

if __name__ == "__main__":
    main()
