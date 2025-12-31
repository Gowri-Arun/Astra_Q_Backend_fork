"""
Phase 5: Build FAISS vector index from docs_parsed.

- Input:  ../static_pipeline/output/docs_parsed/*.json
  Each JSON should contain:
    {
      "text": "...",
      "filename": "O3_SST_L3_Daily_0p25_MOSDAC.json",
      ...
    }

- Output: ./faiss_store/  (FAISS index + docstore)

Keeps:
- RecursiveCharacterTextSplitter (chunk_size=500, overlap=100)
- HuggingFaceEmbeddings with all-MiniLM-L6-v2
- Metadata: {"source": filename, "satellite": ..., "parameter": ..., "region": ... (if present)}
"""

import os
import json
from pathlib import Path
from typing import List

from sentence_transformers import SentenceTransformer
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS as LC_FAISS
from tqdm import tqdm


# --------- Paths & Config ---------

CURRENT_DIR = Path(__file__).resolve().parent          # rag_pipeline/
BASE_DIR = CURRENT_DIR.parent                          # repo root (one level up)
JSON_FOLDER = BASE_DIR / "static_pipeline" / "output" / "docs_parsed"
FAISS_FOLDER = CURRENT_DIR / "faiss_store"

EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
CHUNK_SIZE = 500
CHUNK_OVERLAP = 100
BATCH_SIZE = 64


# --------- Core Helpers ---------

def load_docs_parsed(json_folder: Path) -> List[Document]:
    """Load docs_parsed/*.json and convert into chunked Documents."""
    if not json_folder.exists():
        raise FileNotFoundError(f"Path does not exist: {json_folder}")
    print(f"[build_vector_index] Using JSON folder: {json_folder}")

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", ".", " ", ""],
    )

    all_docs: List[Document] = []

    for filename in sorted(os.listdir(json_folder)):
        if not filename.endswith(".json"):
            continue
        file_path = json_folder / filename
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            continue

        text = (data.get("text") or "").strip()
        if not text:
            continue

        # Core metadata: always set a usable source
        source = data.get("url") or data.get("filename") or filename

        # Optional “friends” if present in JSON
        metadata = {
            "source": source,
        }
        if "satellite" in data:
            metadata["satellite"] = data["satellite"]
        if "parameter" in data:
            metadata["parameter"] = data["parameter"]
        if "region" in data:
            metadata["region"] = data["region"]

        chunks = text_splitter.split_text(text)
        for chunk in chunks:
            all_docs.append(
                Document(
                    page_content=chunk,
                    metadata=metadata,
                )
            )

    print(f"[build_vector_index] Total chunks after splitting: {len(all_docs)}")
    return all_docs


def build_embeddings(texts: List[str]):
    """Embed texts in batches using HuggingFaceEmbeddings (all-MiniLM-L6-v2)."""
    print("[build_vector_index] Initializing embedding model:", EMBEDDING_MODEL_NAME)
    _ = SentenceTransformer(EMBEDDING_MODEL_NAME)  # kept for parity with old script
    embedding_wrapper = HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL_NAME)

    embeddings = []
    print("[build_vector_index] Embedding documents...")
    for i in tqdm(range(0, len(texts), BATCH_SIZE), desc="Embedding Batches"):
        batch = texts[i : i + BATCH_SIZE]
        batch_embeds = embedding_wrapper.embed_documents(batch)
        embeddings.extend(batch_embeds)

    return embeddings, embedding_wrapper


def build_faiss_index(docs: List[Document], faiss_folder: Path) -> None:
    """Create FAISS index from Documents and save to disk."""
    if not docs:
        raise ValueError("No documents provided to build FAISS index.")

    texts = [doc.page_content for doc in docs]
    embeddings, embedding_wrapper = build_embeddings(texts)

    # Use the original docs to keep metadata
    vectorstore = LC_FAISS.from_documents(
        documents=docs,
        embedding=embedding_wrapper,
    )

    faiss_folder.mkdir(parents=True, exist_ok=True)
    vectorstore.save_local(str(faiss_folder))
    print(f"[build_vector_index] FAISS index saved to: {faiss_folder}")



# --------- Entry Point ---------

def main():
    print("[build_vector_index] Phase 5: building vector index for RAG backend")
    docs = load_docs_parsed(JSON_FOLDER)
    build_faiss_index(docs, FAISS_FOLDER)
    print("[build_vector_index] Done.")


if __name__ == "__main__":
    main()
