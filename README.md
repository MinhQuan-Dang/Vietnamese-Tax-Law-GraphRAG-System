# Vietnamese Tax Law GraphRAG System

### Multi-Hop Legal Reasoning over Personal & Corporate Income Tax Documents

---

## 📌 Overview

This project builds an end-to-end **GraphRAG pipeline** over Vietnamese tax law documents related to:

* **Personal Income Tax (Thuế Thu Nhập Cá Nhân – TNCN)**
* **Corporate Income Tax (Thuế Thu Nhập Doanh Nghiệp – TNDN)**

The system:

1. Crawls official legal documents from the **National Legal Document Database of Vietnam**
2. Converts raw Word/PDF files into LLM-ready Markdown
3. Builds a **Knowledge Graph–enhanced RAG pipeline using LightRAG**
4. Evaluates Traditional RAG vs GraphRAG using a structured benchmarking framework

The goal is to enable **multi-hop regulatory reasoning**, improve groundedness, and reduce hallucination in complex cross-document tax queries.

---

# 🏗 Project Architecture

The project consists of **4 main stages**:

---

## 1️⃣ Data Collection (Web Scraping)

Script: `vbpl_scraper.py`

* Scrapes official Vietnamese tax law documents
* Source: *National Legal Document Database of Vietnam vbpl.vn*
* Collects ~200 legal documents related to PIT & CIT

After running:

```
output/
 ├── thue_thu_nhap_ca_nhan/
 ├── thue_thu_nhap_doanh_nghiep/
 └── logs/
```

---

## 2️⃣ Raw Data Organization

Script: `organize_files.py`

* Extracts all Word and PDF files from `output/`
* Stores them in structured raw format

```
raw_data/
 ├── pdfs_docs/
 └── word_docs/
```

---

## 3️⃣ Document Conversion (LLM-Ready Markdown)

Script: `markitdown_convert.py`

* Converts all Word & PDF files into Markdown
* Saves Markdown files to:

```
raw_data/markdown_docs/
```

⚠️ Only Markdown files are used in the RAG pipeline.
Original Word/PDF files are preserved but not processed by LightRAG.

---

## 4️⃣ GraphRAG Pipeline (LightRAG)

Directory: `hybrid_workspace/`

### Structure:

```
hybrid_workspace/
 ├── markdown_docs/       # LLM-ready legal documents
 ├── rag_storage/         # Vector DB + Knowledge Graph storage
 ├── logs/
 ├── knowledge_graph.html # Graph visualization
 ├── .env                 # API keys
 └── scripts/
```

---

# 🧠 LightRAG Pipeline Components

Located in: `hybrid_workspace/scripts/`

### Core Scripts

* `init_rag.py` — Initialize LightRAG with:

  * GPT-4.1-mini (OpenAI API)
  * Local embeddings (SentenceTransformers multilingual-e5-base)
* `index_markdown_docs.py` — Chunking, embedding, entity–relation extraction
* `query_cli.py` — Query in different modes:

  * `naive` (Traditional RAG)
  * `local`
  * `global`
  * `hybrid`
* `debug_retrieval_data.py` — Inspect retrieved chunks/entities/relations
* `clear_storage.py` — Reset vector DB & graph

---

# 🔬 Evaluation Framework

A structured benchmark was implemented to compare:

> Traditional RAG (vector search) vs GraphRAG (entity–relation + vector hybrid)

---

## 📊 Dataset

* 120 labeled queries:

  * 60 real-use tax queries
  * 60 stress-test multi-hop queries

Categories:

* Multi-hop conditional reasoning
* Cross-document aggregation
* Concept comparison
* Legal source tracing

---

## 🤖 LLM-as-a-Judge Rubric (1.0–10.0 Interval Scale)

Each answer is scored on:

* **Correctness**
* **Completeness**
* **Groundedness**
* **Hallucination**

Evaluation scripts:

* `eval_questions_tax.py`
* `eval_run_modes.py`
* `eval_judge_llm.py`
* `eval_analyze_plot.py`

Generated outputs:

* Histograms
* Box plots
* Radar charts
* Correlation matrices
* Effect size comparisons
* Mean & median per retrieval mode

---


# 🛠 Tech Stack

**Core AI Stack**

* Python
* LightRAG (GraphRAG framework)
* GPT-4.1-mini (OpenAI API)
* SentenceTransformers (intfloat/multilingual-e5-base)
* Knowledge Graph construction
* NanoVectorDB (vector similarity search)

**Data & Evaluation**

* AsyncIO
* JSONL pipelines
* Pandas
* NumPy
* Matplotlib
* Seaborn
* LLM-as-a-Judge evaluation

---

# 🚀 How to Run

## 1️⃣ Scrape data

```bash
python vbpl_scraper.py
```

## 2️⃣ Organize files

```bash
python organize_files.py
```

## 3️⃣ Convert to Markdown

```bash
python markitdown_convert.py
```

## 4️⃣ Initialize GraphRAG

```bash
cd hybrid_workspace
python scripts/init_rag.py
```

## 5️⃣ Index documents

```bash
python scripts/index_markdown_docs.py
```

## 6️⃣ Query system

```bash
python scripts/query_cli.py
```

## 7️⃣ Run evaluation

```bash
python scripts/eval_run_modes.py
python scripts/eval_judge_llm.py
python scripts/eval_analyze_plot.py
```

---

# 📊 Knowledge Graph Visualization

After indexing, an interactive graph is generated:

```
hybrid_workspace/knowledge_graph.html
```

This visualizes:

* Entities
* Relationships
* Multi-hop connections between tax regulations

---

# 📌 Project Goals

* Build a production-ready GraphRAG system for legal intelligence
* Quantitatively compare RAG vs GraphRAG
* Improve groundedness and reduce hallucination in regulatory AI systems
* Create reproducible evaluation framework for legal-domain LLM systems

---

# 📎 Future Improvements

* Add citation highlighting
* Implement retrieval confidence scoring
* Extend to other regulatory domains (VAT, investment law)
* Deploy as web-based tax assistant
