import os
from dotenv import load_dotenv

load_dotenv()

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_neo4j import Neo4jGraph, GraphCypherQAChain
from langchain_core.prompts import PromptTemplate

CYPHER_GENERATION_TEMPLATE = """Task: Generate a Cypher statement to query a Neo4j database.

Use ONLY the provided schema and follow these rules:

- Satellites are connected to products via :PRODUCES.
- Products are connected to parameters via :OBSERVES.
- Products are connected to regions via :COVERS.
- Ocean-related products are identified by parameters where par.category = "ocean".
- Each Product has:
  - p.product_type: "doc" or "site_doc" (documentation pages and generic site pages; no real data products yet).
  - p.display_name: a cleaner, human-readable name.
  - p.doc_section: section of the documentation (e.g., "Introduction", "Payloads", "References").
  - p.keywords: list of tokens summarizing the page.

Generation rules:

- If the question is about:
  - Datasets or data products (e.g., "download", "data", "grid", "time series"):
    - At present there are NO true data products tagged in the graph, so return a helpful message explaining that only documentation nodes (product_type="doc"/"site_doc") exist.
  - Satellite descriptions, introductions, payloads, objectives, references:
    - Use Product nodes with p.product_type = "doc".
    - Filter by p.doc_section where relevant (e.g., doc_section = "Payloads" for payload questions).
  - Site policies, access, or general site info:
    - Use Product nodes with p.product_type = "site_doc".

- Prefer returning p.display_name instead of the raw p.name when listing products.
- When filtering by satellite, match on s:Satellite with s.name (e.g., "Oceansat-3", "INSAT-3D").
- When filtering by parameters (rainfall, ocean, etc.), join via :OBSERVES and use par.category or par.display_name.

Schema:
{schema}

Examples:

# Which products are ocean-related from Oceansat-3?
MATCH (s:Satellite {{name: "Oceansat-3"}})-[:PRODUCES]->(p:Product)-[:OBSERVES]->(par:Parameter)
WHERE par.category = "ocean"
RETURN DISTINCT p.display_name, p.product_type, par.display_name

# What are the payload pages for INSAT-3D?
MATCH (s:Satellite {{name: "INSAT-3D"}})-[:PRODUCES]->(p:Product)
WHERE p.product_type = "doc" AND p.doc_section = "Payloads"
RETURN p.display_name, p.doc_section

# Show documentation pages for Oceansat-3
MATCH (s:Satellite {{name: "Oceansat-3"}})-[:PRODUCES]->(p:Product)
WHERE p.product_type = "doc"
RETURN p.display_name, p.doc_section

The question is:
{question}"""

CYPHER_GENERATION_PROMPT = PromptTemplate(
    input_variables=["schema", "question"],
    template=CYPHER_GENERATION_TEMPLATE,
)

# Connect to Neo4j Aura (or local)
graph = Neo4jGraph(
    url=os.getenv("NEO4J_URI"),
    username=os.getenv("NEO4J_USER"),
    password=os.getenv("NEO4J_PASSWORD"),
)

# Gemini LLM
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0,
)

# NL -> Cypher -> KG -> answer chain
chain = GraphCypherQAChain.from_llm(
    llm=llm,
    graph=graph,
    verbose=True,
    allow_dangerous_requests=True,
    cypher_prompt=CYPHER_GENERATION_PROMPT,
)

def ask(q: str):
    print(f"\nQ: {q}")
    result = chain.invoke({"query": q})
    print("A:", result["result"])

if __name__ == "__main__":
    ask("What are the payload pages for INSAT-3D?")
    ask("Which documentation pages are ocean-related from Oceansat-3?")
