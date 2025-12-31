import os
from typing import Any, Dict
from pathlib import Path

from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_neo4j import Neo4jGraph, GraphCypherQAChain
from langchain_core.prompts import PromptTemplate


# ---------------- ENV + GRAPH + LLM SETUP ----------------

CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = CURRENT_DIR.parent  # adjust if kg_pipeline is nested differently
ENV_PATH = BASE_DIR / "config" / ".env"

load_dotenv(ENV_PATH)


CYPHER_GENERATION_TEMPLATE = """Task: Generate a Cypher statement to query a Neo4j database.

Use ONLY the provided schema and follow these rules:
- Satellites are connected to products via :PRODUCES.
- Products are connected to parameters via :OBSERVES.
- Products are connected to regions via :COVERS.
- Ocean-related products are identified by parameters where par.category = "ocean".
- Rainfall-related products should be found via the Parameter node, using name/type contains "rain".

Schema:
{schema}

Examples:
# Which products are ocean-related from Oceansat-3?
MATCH (s:Satellite {{name: "Oceansat-3"}})-[:PRODUCES]->(p:Product)-[:OBSERVES]->(par:Parameter)
WHERE par.category = "ocean"
RETURN DISTINCT p.name, p.display_name

# What are the rainfall products from INSAT-3D and where are they available?
MATCH (s:Satellite {{name: "INSAT-3D"}})-[:PRODUCES]->(p:Product)
MATCH (p)-[:OBSERVES]->(par:Parameter)
MATCH (p)-[:COVERS]->(r:Region)
WHERE toLower(par.display_name) CONTAINS "rain"
   OR toLower(par.type) CONTAINS "rain"
RETURN DISTINCT
  p.name AS ProductName,
  p.display_name AS ProductDisplayName,
  r.name AS RegionAvailable

The question is:
{question}"""
QA_TEMPLATE = """You are an assistant answering questions about satellite data products.

You are given:
- A natural language question.
- The Cypher query used to answer it.
- The query results as a list of rows (context). Each row may have fields like:
  - ProductName
  - ProductDisplayName
  - RegionAvailable

Follow these rules:
- If the context list is NOT empty, you should use the context list to answer the question in 1–2 concise sentences.
- If there are multiple products, you may summarize them.
- Do not mention Cypher, Neo4j, or databases in your answer.

Question:
{question}

Query results (context):
{context}

Answer:"""

QA_PROMPT = PromptTemplate(
    input_variables=["question", "context"],
    template=QA_TEMPLATE,
)

CYPHER_GENERATION_PROMPT = PromptTemplate(
    input_variables=["schema", "question"],
    template=CYPHER_GENERATION_TEMPLATE,
)

graph = Neo4jGraph(
    url=os.getenv("NEO4J_URI"),
    username=os.getenv("NEO4J_USER"),
    password=os.getenv("NEO4J_PASSWORD"),
)

llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash-lite",
    temperature=0,
)

chain = GraphCypherQAChain.from_llm(
    llm=llm,
    graph=graph,
    verbose=True,
    allow_dangerous_requests=True,
    return_intermediate_steps=True,
    cypher_prompt=CYPHER_GENERATION_PROMPT,
    qa_prompt=QA_PROMPT,
)


# ---------------- PUBLIC HELPER ----------------

def ask_kg(question: str) -> Dict[str, Any]:
    result = chain.invoke({"query": question})
    steps = result.get("intermediate_steps", [])

    cypher = ""
    rows: Any = []

    if steps:
        cypher = steps[0].get("query", "")
        if len(steps) > 1:
            rows = steps[1].get("context", [])

    return {
        "answer": result.get("result", ""),
        "cypher": cypher,
        "rows": rows,
    }



# ---------------- DEV MAIN ----------------

if __name__ == "__main__":
    for q in [
        "Where is INSAT-3D rainfall data?",
        "What are the rainfall products from INSAT-3D and where are they available?",
        "Which products are ocean-related from Oceansat-3?",
    ]:
        out = ask_kg(q)
        print("\nQ:", q)
        print("Answer:", out["answer"])
        print("Cypher:", out["cypher"])
        print("Rows:", out["rows"])
