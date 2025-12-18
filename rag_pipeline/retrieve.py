import os
import json
import numpy as np
from sentence_transformers import SentenceTransformer
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_classic.chains import ConversationalRetrievalChain
from langchain_openai import ChatOpenAI
from langchain_classic.memory import ConversationBufferMemory
from tqdm import tqdm
from langchain_community.vectorstores import FAISS as LC_FAISS
from langchain_core.documents import Document
import time
import spacy
import google.generativeai as genai
from dotenv import load_dotenv

# ---------------- ENV + MODEL SETUP ----------------

load_dotenv(dotenv_path="config/.env")
api_key = os.getenv("GEMINI_API_KEY")

genai.configure(api_key=api_key)
model = genai.GenerativeModel("models/gemini-2.0-flash")

embedding_model_name = "sentence-transformers/all-MiniLM-L6-v2"
sentence_model = SentenceTransformer(embedding_model_name)
embedding_wrapper = HuggingFaceEmbeddings(model_name=embedding_model_name)

CURRENT_DIR = os.path.dirname(__file__)  # rag_pipeline/
faiss_folder = os.path.join(CURRENT_DIR, "faiss_store")

vectorstore = LC_FAISS.load_local(
    folder_path=faiss_folder,
    embeddings=embedding_wrapper,
    allow_dangerous_deserialization=True
)

retriever = vectorstore.as_retriever(search_kwargs={"k": 5})


# ---------------- RAG CORE FUNCTION ----------------

def rag_qa(query: str, use_fallback: bool = False, fallback_keyword: str = None):
    # Main retrieval call
    retrieved_docs = retriever.invoke(query)

    # Fallback keyword logic
    if use_fallback and fallback_keyword:
        fallback_lower = fallback_keyword.lower()
        found = any(fallback_lower in doc.page_content.lower() for doc in retrieved_docs)
    else:
        found = bool(retrieved_docs)

    # Fallback search if needed
    if use_fallback and not found and fallback_keyword:
        print(" Semantic search failed. Using keyword fallback...")
        fallback_lower = fallback_keyword.lower()
        retrieved_docs = [
            doc for doc in vectorstore.docstore._dict.values()
            if fallback_lower in doc.page_content.lower()
        ]
        if not retrieved_docs:
            return f" Could not find any relevant information for '{fallback_keyword}'."

    # Reorder results to prioritize keyword-matching docs
    if use_fallback and fallback_keyword:
        contains_keyword, others = [], []
        fallback_lower = fallback_keyword.lower()

        for doc in retrieved_docs:
            if fallback_lower in doc.page_content.lower():
                contains_keyword.append(doc)
            else:
                others.append(doc)

        ordered_docs = contains_keyword + others
    else:
        ordered_docs = retrieved_docs

    # Build context
    context = "\n\n".join([doc.page_content for doc in ordered_docs])

    # Construct prompt
    prompt = f"""Answer the question using the context below. 
If the answer is present, answer it fully. 
Only say you don't know if the context has no relevant info.

Context:
{context}

Question:
{query}
"""

    print(" Prompt sent to Gemini:\n", prompt)

    # Generate answer
    response = model.generate_content(prompt)
    return response.text


# ---------------- FALLBACK KEYWORD EXTRACTION ----------------

nlp = spacy.load("en_core_web_sm")

def extract_fallback_keyword(query: str) -> str:
    doc = nlp(query)
    entities = [ent.text for ent in doc.ents]
    return entities[0] if entities else None


# ---------------- RAG PIPELINE WRAPPER ----------------

def run_rag_pipeline(query, use_fallback=True, fallback_keyword=None):
    # Auto-extract fallback keyword if not provided
    if not fallback_keyword:
        fallback_keyword = extract_fallback_keyword(query)

    # Retrieve docs BEFORE Gemini call for source listing
    retrieved_docs = retriever.invoke(query)

    # Extract top 3 sources
    sources = [
        {
            "source": doc.metadata.get("source", "Unknown"),
            "content_preview": doc.page_content[:200] + "..."
        }
        for doc in retrieved_docs[:3]
    ]

    # Get final answer from Gemini
    answer = rag_qa(query, use_fallback=True, fallback_keyword=fallback_keyword)

    return {
        "answer": answer,
        "sources": sources
    }
