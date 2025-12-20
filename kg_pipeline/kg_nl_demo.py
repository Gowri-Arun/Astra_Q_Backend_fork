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

Schema:
{schema}

Examples:
# Which products are ocean-related from Oceansat-3?
MATCH (s:Satellite {{name: "Oceansat-3"}})-[:PRODUCES]->(p:Product)-[:OBSERVES]->(par:Parameter)
WHERE par.category = "ocean"
RETURN DISTINCT p.name, p.display_name

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
    ask("Where is INSAT-3D rainfall data?")
    ask("Which products are ocean-related from Oceansat-3?")
