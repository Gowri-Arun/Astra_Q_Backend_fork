import os
import json
import numpy as np
from sentence_transformers import SentenceTransformer
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS as LC_FAISS
from langchain_classic.chains import ConversationalRetrievalChain
from langchain_openai import ChatOpenAI
from langchain_classic.memory import ConversationBufferMemory
from tqdm import tqdm
from langchain_community.vectorstores import FAISS as LC_FAISS
from langchain_core.documents import Document
import time


json_folder = r"../static_pipeline/output/docs_parsed"
faiss_folder = "../faiss_store"
embedding_model_name = "sentence-transformers/all-MiniLM-L6-v2"

if not os.path.exists(json_folder):
    raise FileNotFoundError(f" Path does not exist: {json_folder}")
else:
    print(" Path found:", json_folder)


sentence_model = SentenceTransformer(embedding_model_name)
embedding_wrapper = HuggingFaceEmbeddings(model_name=embedding_model_name)


text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=500,
    chunk_overlap=100,
    separators=["\n\n", "\n", ".", " ", ""]
)

all_docs = []

for filename in os.listdir(json_folder):
    if filename.endswith('.json'):
        file_path = os.path.join(json_folder, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict):
                text = data.get("text", "").strip()
                source = data.get("filename", filename)
                if text:
                    chunks = text_splitter.split_text(text)
                    for chunk in chunks:
                        all_docs.append(Document(page_content=chunk, metadata={"source": source}))

print(f" Total chunks after smart splitting: {len(all_docs)}")


texts = [doc.page_content for doc in all_docs]
metadatas = [doc.metadata for doc in all_docs]


print("Embedding documents...")
batch_size = 64
embeddings = []
for i in tqdm(range(0, len(texts), batch_size), desc="Embedding Batches"):
    batch = texts[i:i+batch_size]
    batch_embeds = embedding_wrapper.embed_documents(batch)
    embeddings.extend(batch_embeds)

# Convert embeddings + docs into vectorstore
# print(" Creating vectorstore...")
# docs = [Document(page_content=text, metadata=meta) for text, meta in zip(texts, metadatas)]
# vectorstore = LC_FAISS(embeddings, docs)

text_embedding_pairs = list(zip(texts, embeddings))
vectorstore = LC_FAISS.from_embeddings(text_embedding_pairs, embedding_wrapper)


faiss_folder = "faiss_store"
vectorstore.save_local(faiss_folder)
print(f" FAISS index saved to: {faiss_folder}")