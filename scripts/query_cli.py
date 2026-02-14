import asyncio
from dotenv import load_dotenv
from lightrag import QueryParam

from config_logging import configure_logging
from init_rag import initialize_rag

load_dotenv(dotenv_path=".env", override=False)

# For reference only
SAMPLE_QUERIES = [
    "Điều 46 Nghị Định Số 164/2003/NĐ-CP quy định điều gì?",
    "Tóm tắt các điểm chính về thuế thu nhập doanh nghiệp.",
    "Thuế thu nhập cá nhân áp dụng cho những loại thu nhập nào?",
    "Sự khác nhau giữa thuế TNDN và thuế TNCN là gì?"
    "Tôi có đẹp trai không?",
    "Lương của tôi là 30 triệu VNĐ 1 tháng, thì sẽ phải đóng thuế bao nhiêu? Tôi được cầm về nhà bao nhiêu tiền?"
]


async def main():
    configure_logging()
    rag = None
    try:
        rag = await initialize_rag()
        while True:
            initiate_program = input("Do you want to quit the program (Y/N): ").lower()
            if initiate_program == "y":
                break
            else:
                query = input(f"Please ask a question: ")
                mode = input("Please enter the query mode (naive, local, global, hybrid): ")
                response = await rag.aquery(
                    query,
                    param=QueryParam(mode=mode.lower(), enable_rerank=False)
                )
                print("="*80)
                print("Final Answer:")
                print(response)
    finally:
        if rag:
            await rag.finalize_storages()   

if __name__ == "__main__":
    asyncio.run(main())